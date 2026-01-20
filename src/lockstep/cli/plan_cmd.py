"""Plan command for showing what changes would be made."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from lockstep.cli.common import (
    AuthTypeArg,
    CatalogOverrideArg,
    ClientIdArg,
    ClientSecretArg,
    FormatArg,
    HostArg,
    HttpPathArg,
    OutputArg,
    ProfileArg,
    QuietArg,
    SchemaOverrideArg,
    TablePrefixArg,
    TokenArg,
    VerboseArg,
    console,
    ensure_databricks_config,
    error_console,
    load_contracts,
    path_argument,
    setup_logging,
    validate_output_format,
)
from lockstep.cli.formatters import format_plan
from lockstep.cli.junit_reporter import generate_junit_xml
from lockstep.databricks import DatabricksConnector
from lockstep.databricks.connector import DatabricksConnectionError
from lockstep.models.catalog_state import SavedPlan
from lockstep.services import ContractLoader, SyncService
from lockstep.services.sync import SyncOptions

# Create the plan command app
plan_app = typer.Typer()


@plan_app.callback(invoke_without_command=True)
def plan_changes(
    path: Annotated[Path, path_argument],
    profile: ProfileArg = None,
    host: HostArg = None,
    http_path: HttpPathArg = None,
    auth_type: AuthTypeArg = None,
    token: TokenArg = None,
    client_id: ClientIdArg = None,
    client_secret: ClientSecretArg = None,
    ignore_tags: Annotated[
        bool,
        typer.Option(
            "--ignore-tags",
            help="Exclude tag changes from the plan.",
        ),
    ] = False,
    ignore_columns: Annotated[
        bool,
        typer.Option(
            "--ignore-columns",
            help="Exclude column changes from the plan.",
        ),
    ] = False,
    ignore_descriptions: Annotated[
        bool,
        typer.Option(
            "--ignore-descriptions",
            help="Exclude description changes from the plan.",
        ),
    ] = False,
    ignore_constraints: Annotated[
        bool,
        typer.Option(
            "--ignore-constraints",
            help="Exclude constraint changes from the plan.",
        ),
    ] = False,
    ignore_permissions: Annotated[
        bool,
        typer.Option(
            "--ignore-permissions",
            help="Exclude permission (grant/revoke) changes from the plan.",
        ),
    ] = False,
    catalog_override: CatalogOverrideArg = None,
    schema_override: SchemaOverrideArg = None,
    table_prefix: TablePrefixArg = None,
    plan_out: Annotated[
        Path | None,
        typer.Option(
            "--plan-out",
            help="Save the plan to a JSON file for later apply.",
        ),
    ] = None,
    format: FormatArg = None,
    out: OutputArg = None,
    verbose: VerboseArg = False,
    quiet: QuietArg = False,
) -> None:
    """Show what changes would be made without applying them.

    Compares contracts against Unity Catalog and displays the differences.
    No changes are made to Unity Catalog.

    Use --profile to authenticate via Databricks CLI profile (~/.databrickscfg).
    Use --ignore-* flags to exclude certain change types from the plan output.
    Use --plan-out to save the plan to a file for later apply.
    Use --format to specify output format (table, json, junit).
    Use --out to write output to a file (in addition to displaying).

    Authentication types:
    - profile: Use Databricks CLI profile from ~/.databrickscfg (recommended)
    - oauth: Interactive OAuth via Databricks CLI, Azure CLI, etc. (default)
    - pat: Personal Access Token (requires --token)
    - sp: Service Principal / OAuth M2M (requires --client-id and --client-secret)

    Exit codes:
    - 0: No changes needed (in sync)
    - 1: Error occurred
    - 2: Changes detected (drift)

    Examples:

        # Use Databricks CLI profile (recommended)
        $ lockstep plan contracts/ --profile my-workspace --sql-endpoint abc123

        # Show all changes (default table format)
        $ lockstep plan contracts/

        # Save plan to file for later apply
        $ lockstep plan contracts/ --plan-out plan.json

        # Output as JSON
        $ lockstep plan contracts/ --format json

        # Output as JUnit XML to file
        $ lockstep plan contracts/ --format junit --out results.xml

        # Ignore tag changes
        $ lockstep plan contracts/ --ignore-tags
    """
    setup_logging(verbose, quiet)
    output_format = validate_output_format(format)

    loader = ContractLoader()
    contracts = load_contracts(path, loader)

    config = ensure_databricks_config(
        host, http_path, auth_type, token, client_id, client_secret, profile
    )

    sync_options = SyncOptions(
        dry_run=True,
        catalog_override=catalog_override,
        schema_override=schema_override,
        table_prefix=table_prefix,
        # Show changes based on ignore flags
        add_tags=not ignore_tags,
        add_columns=not ignore_columns,
        add_descriptions=not ignore_descriptions,
        add_constraints=not ignore_constraints,
        add_permissions=not ignore_permissions,
        remove_columns=not ignore_columns,
        remove_tags=not ignore_tags,
        remove_constraints=not ignore_constraints,
        remove_permissions=not ignore_permissions,  # Show revokes in plan too
    )

    try:
        with DatabricksConnector(config) as connector:
            sync_service = SyncService(connector)
            results = sync_service.sync_contracts(contracts, sync_options)

            has_changes = False
            plans_to_save = []

            # Generate output based on format
            if output_format == "junit":
                output_content = generate_junit_xml(
                    results=results,
                    check_mode=True,
                    output_path=None,  # Return string instead of writing
                )
                console.print(output_content)
                if out:
                    out.write_text(output_content)
                    console.print(f"\n[green]✓[/green] JUnit XML written to: {out}")

            elif output_format == "json":
                output_data = {
                    "command": "plan",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "results": [
                        {
                            "contract_name": r.contract_name,
                            "table_name": r.table_name,
                            "has_changes": r.plan.has_changes if r.plan else False,
                            "actions": [
                                {
                                    "type": a.action_type.value,
                                    "target": a.target,
                                    "details": a.details,
                                    "sql": a.sql,
                                }
                                for a in (r.plan.actions if r.plan else [])
                            ],
                        }
                        for r in results
                    ],
                }
                output_content = json.dumps(output_data, indent=2)
                console.print(output_content)
                if out:
                    out.write_text(output_content)
                    console.print(f"\n[green]✓[/green] JSON written to: {out}")

            else:  # table format (default)
                output_lines = []
                for result in results:
                    if result.plan and result.plan.has_changes:
                        formatted = format_plan(result.plan)
                        console.print(formatted)
                        output_lines.append(str(formatted))
                        has_changes = True
                        plans_to_save.append(result.plan)
                    else:
                        msg = f"[dim]No changes needed for {result.table_name}[/dim]"
                        console.print(msg)
                        output_lines.append(f"No changes needed for {result.table_name}")

                if out:
                    out.write_text("\n".join(output_lines))
                    console.print(f"\n[green]✓[/green] Output written to: {out}")

            # Check for changes in all formats
            for result in results:
                if result.plan and result.plan.has_changes:
                    has_changes = True
                    if result.plan not in plans_to_save:
                        plans_to_save.append(result.plan)

            # Save plan to file if requested
            if plan_out and plans_to_save:
                saved_plan = SavedPlan(
                    version="1.0",
                    created_at=datetime.now(UTC).isoformat(),
                    host=config.host,
                    plans=plans_to_save,
                )
                with open(plan_out, "w") as f:
                    json.dump(saved_plan.to_dict(), f, indent=2)
                console.print(f"\n[green]✓[/green] Plan saved to: {plan_out}")
                console.print(f"[dim]Apply this plan with: lockstep apply {plan_out}[/dim]")

            if has_changes:
                console.print(
                    "\n[yellow]⚠️  Changes detected. Run 'lockstep apply' to apply changes.[/yellow]"
                )
                raise typer.Exit(2)
            else:
                console.print("\n[green]✓ All contracts are in sync with Unity Catalog.[/green]")

    except DatabricksConnectionError as e:
        error_console.print(f"\n[red]❌ Connection error:[/red] {e}")
        raise typer.Exit(1) from None

