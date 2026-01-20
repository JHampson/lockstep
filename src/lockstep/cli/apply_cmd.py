"""Apply command for applying contract changes to Unity Catalog."""

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
from lockstep.cli.formatters import format_sync_results
from lockstep.cli.junit_reporter import generate_junit_xml
from lockstep.databricks import DatabricksConnector
from lockstep.databricks.connector import DatabricksConnectionError
from lockstep.models.catalog_state import SavedPlan
from lockstep.services import ContractLoader, SyncService
from lockstep.services.sync import SyncOptions, SyncResult

# Create the apply command app
apply_app = typer.Typer()


def _apply_saved_plan(
    plan_path: Path,
    host: str | None,
    http_path: str | None,
    auth_type: str | None,
    token: str | None,
    client_id: str | None,
    client_secret: str | None,
    verbose: bool,  # noqa: ARG001
    quiet: bool,  # noqa: ARG001
    output_format: str,
    out: Path | None,
    profile: str | None = None,
) -> None:
    """Apply a saved plan file to Unity Catalog."""
    console.print(f"\n[bold]Loading plan from:[/bold] {plan_path}")

    try:
        with open(plan_path) as f:
            plan_data = json.load(f)
        saved_plan = SavedPlan.from_dict(plan_data)
    except (json.JSONDecodeError, KeyError) as e:
        error_console.print(f"[red]❌ Invalid plan file:[/red] {e}")
        raise typer.Exit(1) from None

    console.print(f"[dim]Plan created: {saved_plan.created_at}[/dim]")
    console.print(f"[dim]Original host: {saved_plan.host}[/dim]")
    console.print(f"[dim]Plans: {len(saved_plan.plans)}, Actions: {saved_plan.total_actions}[/dim]")

    if not saved_plan.has_changes:
        console.print("\n[green]✓ Plan has no changes to apply.[/green]")
        return

    # Use host from plan if not overridden, unless profile is specified
    effective_host = host or saved_plan.host
    if not effective_host and not profile:
        error_console.print(
            "[red]❌ No host specified. Use --host, --profile, or ensure the plan contains a host.[/red]"
        )
        raise typer.Exit(1) from None

    config = ensure_databricks_config(
        effective_host, http_path, auth_type, token, client_id, client_secret, profile
    )

    try:
        with DatabricksConnector(config) as connector:
            total_applied = 0
            total_failed = 0
            results_for_junit = []

            for plan in saved_plan.plans:
                console.print(f"\n[bold]Applying plan for:[/bold] {plan.table_name}")

                for action in plan.actions:
                    if action.sql:
                        try:
                            connector.execute(action.sql)
                            console.print(f"  [green]✓[/green] {action.description}")
                            total_applied += 1
                        except Exception as e:
                            console.print(f"  [red]✗[/red] {action.description}: {e}")
                            total_failed += 1
                    else:
                        console.print(f"  [yellow]⚠[/yellow] {action.description} (no SQL)")

                # Create a mock result for JUnit reporting
                results_for_junit.append(
                    SyncResult(
                        contract_name=plan.contract_name,
                        table_name=plan.table_name,
                        success=total_failed == 0,
                        actions_applied=total_applied,
                        plan=plan,
                    )
                )

            # Summary
            summary_msg = f"\n[bold]Summary:[/bold] {total_applied} applied, {total_failed} failed"
            console.print(summary_msg)

            # Generate output based on format
            if output_format == "junit":
                output_content = generate_junit_xml(
                    results=results_for_junit,
                    check_mode=False,
                    output_path=None,
                )
                console.print(output_content)
                if out:
                    out.write_text(output_content)
                    console.print(f"[green]✓[/green] JUnit XML written to: {out}")

            elif output_format == "json":
                output_data = {
                    "command": "apply",
                    "plan_file": str(plan_path),
                    "timestamp": datetime.now(UTC).isoformat(),
                    "summary": {
                        "applied": total_applied,
                        "failed": total_failed,
                    },
                    "results": [
                        {
                            "contract_name": r.contract_name,
                            "table_name": r.table_name,
                            "success": r.success,
                            "actions_applied": r.actions_applied,
                        }
                        for r in results_for_junit
                    ],
                }
                output_content = json.dumps(output_data, indent=2)
                console.print(output_content)
                if out:
                    out.write_text(output_content)
                    console.print(f"[green]✓[/green] JSON written to: {out}")

            elif out:
                # Table format - write plain text summary to file
                out.write_text(f"Summary: {total_applied} applied, {total_failed} failed")
                console.print(f"[green]✓[/green] Output written to: {out}")

            if total_failed > 0:
                raise typer.Exit(1) from None

    except DatabricksConnectionError as e:
        error_console.print(f"\n[red]❌ Connection error:[/red] {e}")
        raise typer.Exit(1) from None


@apply_app.callback(invoke_without_command=True)
def apply_contracts(
    path: Annotated[Path, path_argument],
    profile: ProfileArg = None,
    host: HostArg = None,
    http_path: HttpPathArg = None,
    auth_type: AuthTypeArg = None,
    token: TokenArg = None,
    client_id: ClientIdArg = None,
    client_secret: ClientSecretArg = None,
    add_tags: Annotated[
        bool,
        typer.Option(
            "--add-tags/--no-add-tags",
            help="Add/update tags from contract (default: enabled).",
        ),
    ] = True,
    add_columns: Annotated[
        bool,
        typer.Option(
            "--add-columns/--no-add-columns",
            help="Add missing columns from contract (default: enabled).",
        ),
    ] = True,
    add_descriptions: Annotated[
        bool,
        typer.Option(
            "--add-descriptions/--no-add-descriptions",
            help="Update descriptions from contract (default: enabled).",
        ),
    ] = True,
    add_constraints: Annotated[
        bool,
        typer.Option(
            "--add-constraints/--no-add-constraints",
            help="Add constraints (PK, NOT NULL) from contract (default: enabled).",
        ),
    ] = True,
    add_permissions: Annotated[
        bool,
        typer.Option(
            "--add-permissions/--no-add-permissions",
            help="Grant permissions from contract roles (default: enabled).",
        ),
    ] = True,
    remove_columns: Annotated[
        bool,
        typer.Option(
            "--remove-columns/--no-remove-columns",
            help="Remove columns not in contract (default: disabled).",
        ),
    ] = False,
    remove_tags: Annotated[
        bool,
        typer.Option(
            "--remove-tags/--no-remove-tags",
            help="Remove tags not in contract (default: disabled).",
        ),
    ] = False,
    remove_constraints: Annotated[
        bool,
        typer.Option(
            "--remove-constraints/--no-remove-constraints",
            help="Remove constraints not in contract (default: disabled).",
        ),
    ] = False,
    remove_permissions: Annotated[
        bool,
        typer.Option(
            "--remove-permissions/--no-remove-permissions",
            help="Revoke permissions not in contract roles (default: disabled).",
        ),
    ] = False,
    catalog_override: CatalogOverrideArg = None,
    schema_override: SchemaOverrideArg = None,
    table_prefix: TablePrefixArg = None,
    format: FormatArg = None,
    out: OutputArg = None,
    verbose: VerboseArg = False,
    quiet: QuietArg = False,
) -> None:
    """Apply ODCS contract(s) or a saved plan to Unity Catalog.

    PATH can be:
    - A YAML contract file or directory of contracts
    - A JSON plan file created by 'lockstep plan --plan-out'

    When applying contracts, by default this command will ADD elements but will NOT
    remove anything from Unity Catalog (safe mode).

    ADD operations (enabled by default):
    - Add missing columns (--add-columns)
    - Update descriptions (--add-descriptions)
    - Add/update tags (--add-tags)
    - Add constraints like PK and NOT NULL (--add-constraints)

    REMOVE operations (disabled by default for safety):
    - Remove columns not in contract (--remove-columns)
    - Remove tags not in contract (--remove-tags)
    - Remove constraints not in contract (--remove-constraints)

    Note: When applying a saved plan file, --add-* and --remove-* flags are ignored
    as the plan already contains the specific actions to apply.

    Use --format to specify output format (table, json, junit).
    Use --out to write output to a file (in addition to displaying).

    Examples:

        # Apply contracts (default safe mode)
        $ lockstep apply contracts/

        # Apply a saved plan
        $ lockstep apply plan.json

        # Output as JSON
        $ lockstep apply contracts/ --format json

        # Output as JUnit XML to file
        $ lockstep apply contracts/ --format junit --out results.xml

        # Full sync - remove everything not in contract
        $ lockstep apply contracts/ --remove-columns --remove-tags --remove-constraints
    """
    setup_logging(verbose, quiet)
    output_format = validate_output_format(format)

    # Check if path is a saved plan file (JSON)
    if path.suffix.lower() == ".json" and path.is_file():
        _apply_saved_plan(
            path,
            host,
            http_path,
            auth_type,
            token,
            client_id,
            client_secret,
            verbose,
            quiet,
            output_format,
            out,
            profile,
        )
        return

    loader = ContractLoader()
    contracts = load_contracts(path, loader)

    config = ensure_databricks_config(
        host, http_path, auth_type, token, client_id, client_secret, profile
    )

    sync_options = SyncOptions(
        dry_run=False,
        catalog_override=catalog_override,
        schema_override=schema_override,
        table_prefix=table_prefix,
        add_tags=add_tags,
        add_columns=add_columns,
        add_descriptions=add_descriptions,
        add_constraints=add_constraints,
        add_permissions=add_permissions,
        remove_columns=remove_columns,
        remove_tags=remove_tags,
        remove_constraints=remove_constraints,
        remove_permissions=remove_permissions,
    )

    try:
        with DatabricksConnector(config) as connector:
            sync_service = SyncService(connector)
            results = sync_service.sync_contracts(contracts, sync_options)

            # Generate output based on format
            if output_format == "junit":
                output_content = generate_junit_xml(
                    results=results,
                    check_mode=False,
                    output_path=None,
                )
                console.print(output_content)
                if out:
                    out.write_text(output_content)
                    console.print(f"\n[green]✓[/green] JUnit XML written to: {out}")

            elif output_format == "json":
                output_data = {
                    "command": "apply",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "results": [
                        {
                            "contract_name": r.contract_name,
                            "table_name": r.table_name,
                            "success": r.success,
                            "actions_applied": r.actions_applied,
                            "errors": r.errors,
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
                formatted_results = format_sync_results(results)
                console.print(formatted_results)
                if out:
                    out.write_text(str(formatted_results))
                    console.print(f"\n[green]✓[/green] Output written to: {out}")

            # Exit with error if any sync failed
            if any(not r.success for r in results):
                raise typer.Exit(1)

    except DatabricksConnectionError as e:
        error_console.print(f"\n[red]❌ Connection error:[/red] {e}")
        raise typer.Exit(1) from None

