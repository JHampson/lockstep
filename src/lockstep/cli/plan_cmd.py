"""Plan command for showing what changes would be made."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from lockstep.cli.actions import execute_plan
from lockstep.cli.common import (
    AuthTypeArg,
    CatalogOverrideArg,
    ClientIdArg,
    ClientSecretArg,
    ConnectionOptions,
    ContractLoadingError,
    FormatArg,
    HostArg,
    HttpPathArg,
    InvalidAuthTypeError,
    InvalidFormatError,
    MissingConfigurationError,
    OutputArg,
    ProfileArg,
    QuietArg,
    SchemaOverrideArg,
    TablePrefixArg,
    TokenArg,
    VerboseArg,
    build_databricks_config,
    load_contracts_from_path,
    path_argument,
    setup_logging,
    validate_databricks_config,
    validate_output_format,
)
from lockstep.cli.output import (
    OutputOptions,
    present_config_error,
    present_contract_load_error,
    present_error,
    present_info,
    present_plan_result,
    present_plan_summary,
)
from lockstep.models.catalog_state import SavedPlan
from lockstep.services import ContractLoader
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
    ignore_column_types: Annotated[
        bool,
        typer.Option(
            "--ignore-column-types",
            help="Exclude column type changes from the plan.",
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
    Use --verbose to show SQL statements.
    Use --quiet to suppress informational messages.

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

        # Verbose output with SQL
        $ lockstep plan contracts/ --verbose

        # Quiet mode (only show changes, errors)
        $ lockstep plan contracts/ --quiet
    """
    setup_logging(verbose, quiet)

    # Validate output format
    try:
        output_format = validate_output_format(format)
    except InvalidFormatError as e:
        present_error(str(e))
        raise typer.Exit(1) from None

    # Create grouped options
    conn_options = ConnectionOptions(
        profile=profile,
        host=host,
        http_path=http_path,
        auth_type=auth_type,
        token=token,
        client_id=client_id,
        client_secret=client_secret,
    )

    output_options = OutputOptions(
        format=output_format,
        out_path=out,
        quiet=quiet,
        verbose=verbose,
    )

    # Load contracts
    present_info(f"\n[bold]Loading contracts from:[/bold] {path}", quiet=quiet)
    loader = ContractLoader()

    try:
        result = load_contracts_from_path(path, loader)
    except ContractLoadingError as e:
        present_contract_load_error(e)
        raise typer.Exit(1) from None

    if result.has_validation_errors:
        present_contract_load_error(
            ContractLoadingError(
                "Some contracts failed validation", validation_errors=result.validation_errors
            )
        )
        if not result.contracts:
            raise typer.Exit(1)
        present_info(
            f"\n[yellow]⚠️  Continuing with {len(result.contracts)} valid contract(s)[/yellow]",
            quiet=quiet,
        )

    if not result.contracts:
        present_info("[yellow]No contracts found.[/yellow]", quiet=quiet)
        raise typer.Exit(0)

    present_info(f"[green]✓[/green] Loaded {len(result.contracts)} contract(s)\n", quiet=quiet)

    # Build configuration
    try:
        config = build_databricks_config(conn_options)
        validate_databricks_config(config)
    except InvalidAuthTypeError as e:
        present_error(str(e))
        raise typer.Exit(1) from None
    except MissingConfigurationError as e:
        present_config_error(e)
        raise typer.Exit(1) from None

    # Build sync options
    sync_options = SyncOptions(
        dry_run=True,
        catalog_override=catalog_override,
        schema_override=schema_override,
        table_prefix=table_prefix,
        add_tags=not ignore_tags,
        add_columns=not ignore_columns,
        add_descriptions=not ignore_descriptions,
        add_constraints=not ignore_constraints,
        add_permissions=not ignore_permissions,
        remove_columns=not ignore_columns,
        remove_tags=not ignore_tags,
        remove_constraints=not ignore_constraints,
        remove_permissions=not ignore_permissions,
        alter_column_types=not ignore_column_types,
    )

    # Execute the plan action
    plan_result = execute_plan(result.contracts, config, sync_options)

    # Handle errors
    if not plan_result.success:
        present_error(f"Connection error: {plan_result.error}")
        raise typer.Exit(1)

    # Present the results
    present_plan_result(plan_result, output_options)

    # Save plan to file if requested
    if plan_out and plan_result.plans_to_save:
        saved_plan = SavedPlan(
            version="1.0",
            created_at=datetime.now(UTC).isoformat(),
            host=config.host,
            plans=plan_result.plans_to_save,
        )
        with open(plan_out, "w") as f:
            json.dump(saved_plan.to_dict(), f, indent=2)
        present_info(f"\n[green]✓[/green] Plan saved to: {plan_out}", quiet=quiet)
        present_info(f"[dim]Apply this plan with: lockstep apply {plan_out}[/dim]", quiet=quiet)

    # Present summary and exit with appropriate code
    present_plan_summary(plan_result, quiet=quiet)

    if plan_result.has_changes:
        raise typer.Exit(2)
