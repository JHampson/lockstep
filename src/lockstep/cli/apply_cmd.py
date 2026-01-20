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
    ensure_databricks_config,
    error_console,
    load_contracts,
    path_argument,
    setup_logging,
    validate_output_format,
)
from lockstep.cli.output import (
    OutputOptions,
    present_apply_progress,
    present_apply_result,
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
    host: str | None,
    http_path: str | None,
    auth_type: str | None,
    token: str | None,
    client_id: str | None,
    client_secret: str | None,
    verbose: bool,
    quiet: bool,
    output_format: str,
    out: Path | None,
    profile: str | None = None,
) -> None:
    """Apply a saved plan file to Unity Catalog."""
    output_options = OutputOptions(
        format=output_format,
        out_path=out,
        quiet=quiet,
        verbose=verbose,
    )

    present_info(f"\n[bold]Loading plan from:[/bold] {plan_path}", quiet=quiet)

    # Load the plan file
    try:
        with open(plan_path) as f:
            plan_data = json.load(f)
        saved_plan = SavedPlan.from_dict(plan_data)
    except (json.JSONDecodeError, KeyError) as e:
        error_console.print(f"[red]❌ Invalid plan file:[/red] {e}")
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

    # Build configuration
    effective_host = host or saved_plan.host
    if not effective_host and not profile:
        error_console.print(
            "[red]❌ No host specified. Use --host, --profile, or ensure the plan contains a host.[/red]"
        )
        raise typer.Exit(1) from None

    config = ensure_databricks_config(
        effective_host, http_path, auth_type, token, client_id, client_secret, profile
    )

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
    output_format = validate_output_format(format)

    # Create output options
    output_options = OutputOptions(
        format=output_format,
        out_path=out,
        quiet=quiet,
        verbose=verbose,
    )

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

    # Load contracts
    loader = ContractLoader()
    contracts = load_contracts(path, loader, quiet=quiet)

    # Build configuration
    config = ensure_databricks_config(
        host, http_path, auth_type, token, client_id, client_secret, profile
    )

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
    result = execute_apply(contracts, config, sync_options)

    # Handle errors
    if result.error:
        present_error(f"Connection error: {result.error}")
        raise typer.Exit(1)

    # Present the results
    present_apply_result(result, output_options)

    # Exit with error if any sync failed
    if not result.success:
        raise typer.Exit(1)
