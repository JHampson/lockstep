"""Apply command for applying contract changes to Unity Catalog."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from lockstep.cli.actions import execute_apply, execute_apply_saved_plan
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
    present_apply_progress,
    present_apply_result,
    present_config_error,
    present_contract_load_error,
    present_error,
    present_info,
)
from lockstep.databricks import DatabricksConnector
from lockstep.models.catalog_state import SavedPlan
from lockstep.services import ContractLoader
from lockstep.services.sync import SyncOptions

# Create the apply command app
apply_app = typer.Typer()


def _apply_saved_plan(
    plan_path: Path,
    conn_options: ConnectionOptions,
    output_options: OutputOptions,
) -> None:
    """Apply a saved plan file to Unity Catalog.

    Args:
        plan_path: Path to the saved plan JSON file.
        conn_options: Connection options for Databricks.
        output_options: Output formatting options.
    """
    quiet = output_options.quiet
    verbose = output_options.verbose

    present_info(f"\n[bold]Loading plan from:[/bold] {plan_path}", quiet=quiet)

    # Load the plan file
    try:
        with open(plan_path) as f:
            plan_data = json.load(f)
        saved_plan = SavedPlan.from_dict(plan_data)
    except (json.JSONDecodeError, KeyError) as e:
        present_error(f"Invalid plan file: {e}")
        raise typer.Exit(1) from None

    present_info(f"[dim]Plan created: {saved_plan.created_at}[/dim]", quiet=quiet)
    present_info(f"[dim]Original host: {saved_plan.host}[/dim]", quiet=quiet)
    present_info(
        f"[dim]Plans: {len(saved_plan.plans)}, Actions: {saved_plan.total_actions}[/dim]",
        quiet=quiet,
    )

    if not saved_plan.has_changes:
        present_info("\n[green]✓ Plan has no changes to apply.[/green]", quiet=quiet)
        return

    # Build configuration - use host from plan if not provided
    effective_conn = ConnectionOptions(
        profile=conn_options.profile,
        host=conn_options.host or saved_plan.host,
        http_path=conn_options.http_path,
        auth_type=conn_options.auth_type,
        token=conn_options.token,
        client_id=conn_options.client_id,
        client_secret=conn_options.client_secret,
    )

    if not effective_conn.host and not effective_conn.profile:
        present_error("No host specified. Use --host, --profile, or ensure the plan contains a host.")
        raise typer.Exit(1) from None

    try:
        config = build_databricks_config(effective_conn)
        validate_databricks_config(config)
    except InvalidAuthTypeError as e:
        present_error(str(e))
        raise typer.Exit(1) from None
    except MissingConfigurationError as e:
        present_config_error(e)
        raise typer.Exit(1) from None

    # Show progress during execution (for saved plans we show each action)
    try:
        with DatabricksConnector(config) as connector:
            for plan in saved_plan.plans:
                present_info(f"\n[bold]Applying plan for:[/bold] {plan.table_name}", quiet=quiet)

                for action in plan.actions:
                    if action.sql:
                        try:
                            connector.execute(action.sql)
                            present_apply_progress(
                                action.description,
                                success=True,
                                quiet=quiet,
                                verbose=verbose,
                                sql=action.sql,
                            )
                        except Exception as e:
                            present_apply_progress(
                                action.description,
                                success=False,
                                error=str(e),
                                quiet=quiet,
                            )
                    else:
                        present_apply_progress(
                            action.description,
                            success=True,
                            quiet=quiet,
                        )

        # Execute the action to get structured results
        result = execute_apply_saved_plan(saved_plan, config, str(plan_path))

    except Exception as e:
        present_error(f"Connection error: {e}")
        raise typer.Exit(1) from None

    # Present the results
    present_apply_result(result, output_options)

    # Exit with error if any failures
    if not result.success:
        raise typer.Exit(1)


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
    Use --verbose to show detailed progress.
    Use --quiet to suppress informational messages.

    Examples:

        # Apply contracts (default safe mode)
        $ lockstep apply contracts/

        # Apply a saved plan
        $ lockstep apply plan.json

        # Output as JSON
        $ lockstep apply contracts/ --format json

        # Output as JUnit XML to file
        $ lockstep apply contracts/ --format junit --out results.xml

        # Quiet mode (only show errors)
        $ lockstep apply contracts/ --quiet

        # Full sync - remove everything not in contract
        $ lockstep apply contracts/ --remove-columns --remove-tags --remove-constraints
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

    # Check if path is a saved plan file (JSON)
    if path.suffix.lower() == ".json" and path.is_file():
        _apply_saved_plan(plan_path=path, conn_options=conn_options, output_options=output_options)
        return

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
            ContractLoadingError("Some contracts failed validation", validation_errors=result.validation_errors)
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

    # Execute the apply action
    apply_result = execute_apply(result.contracts, config, sync_options)

    # Handle errors
    if apply_result.error:
        present_error(f"Connection error: {apply_result.error}")
        raise typer.Exit(1)

    # Present the results
    present_apply_result(apply_result, output_options)

    # Exit with error if any sync failed
    if not apply_result.success:
        raise typer.Exit(1)
