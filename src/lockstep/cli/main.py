"""Main CLI entry point for Lockstep.

Provides commands for synchronizing data contracts to Databricks Unity Catalog.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table

from lockstep import __version__
from lockstep.cli.formatters import format_plan, format_sync_results, format_validation_report
from lockstep.cli.junit_reporter import generate_junit_xml, generate_validation_junit_xml
from lockstep.databricks import DatabricksConfig, DatabricksConnector
from lockstep.databricks.config import AuthType
from lockstep.databricks.connector import DatabricksConnectionError
from lockstep.models.contract import Contract
from lockstep.services import ContractLoader, ContractLoadError, SyncService
from lockstep.services.sync import SyncOptions

# Initialize CLI app
app = typer.Typer(
    name="lockstep",
    help="Synchronize data contracts to Databricks Unity Catalog.",
    add_completion=True,
    rich_markup_mode="rich",
)

console = Console()
error_console = Console(stderr=True)


def _setup_logging(verbose: bool, quiet: bool) -> None:
    """Configure logging based on verbosity flags."""
    if quiet:
        level = logging.ERROR
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=error_console, rich_tracebacks=True, show_path=False)],
    )

    # Reduce noise from libraries
    logging.getLogger("databricks").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def _get_databricks_config(
    host: str | None,
    http_path: str | None,
    auth_type: str | None,
    token: str | None,
    client_id: str | None,
    client_secret: str | None,
) -> DatabricksConfig:
    """Build Databricks configuration from CLI options and environment.

    Precedence: CLI parameters > environment variables > config file
    """
    # Determine auth type
    resolved_auth_type = AuthType.OAUTH  # Default
    if auth_type:
        try:
            resolved_auth_type = AuthType(auth_type.lower())
        except ValueError:
            error_console.print(
                f"[red]Invalid auth-type '{auth_type}'. Must be one of: oauth, pat, sp[/red]"
            )
            raise typer.Exit(1) from None

    return DatabricksConfig(
        host=host or "",
        http_path=http_path or "",
        auth_type=resolved_auth_type,
        token=token,
        client_id=client_id,
        client_secret=client_secret,
    )


def _load_contracts(path: Path, loader: ContractLoader) -> list[Contract]:
    """Load contracts from a file or directory."""
    console.print(f"\n[bold]Loading contracts from:[/bold] {path}")

    if path.is_file():
        try:
            contracts = [loader.load_one(path)]
        except ContractLoadError as e:
            error_console.print(f"\n[red]❌ Failed to load contract:[/red] {e}")
            if e.errors:
                for err in e.errors:
                    error_console.print(f"   • {err}")
            raise typer.Exit(1) from None
    else:
        contracts = loader.load_many(path)
        if loader.validation_errors:
            error_console.print(format_validation_report(loader.validation_errors))
            if not contracts:
                raise typer.Exit(1)
            console.print(
                f"\n[yellow]⚠️  Continuing with {len(contracts)} valid contract(s)[/yellow]"
            )

    if not contracts:
        console.print("[yellow]No contracts found.[/yellow]")
        raise typer.Exit(0)

    console.print(f"[green]✓[/green] Loaded {len(contracts)} contract(s)\n")
    return contracts


def _ensure_databricks_config(
    host: str | None,
    http_path: str | None,
    auth_type: str | None,
    token: str | None,
    client_id: str | None,
    client_secret: str | None,
) -> DatabricksConfig:
    """Build and validate Databricks configuration."""
    try:
        config = _get_databricks_config(host, http_path, auth_type, token, client_id, client_secret)
        if not config.is_configured():
            auth_help = ""
            if config.auth_type == AuthType.PAT:
                auth_help = "\n  • --token required for auth-type=pat"
            elif config.auth_type == AuthType.SP:
                auth_help = "\n  • --client-id and --client-secret required for auth-type=sp"

            error_console.print(
                "\n[red]❌ Databricks connection not configured.[/red]\n"
                "Please provide connection details via:\n"
                "  • CLI options: --host, --sql-endpoint\n"
                "  • Environment variables: DATABRICKS_HOST, DATABRICKS_HTTP_PATH\n"
                f"  • Config file: ~/.lockstep.yaml{auth_help}\n"
            )
            raise typer.Exit(1)
        return config
    except typer.Exit:
        raise
    except Exception as e:
        error_console.print(f"\n[red]❌ Configuration error:[/red] {e}")
        raise typer.Exit(1) from None


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"lockstep version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=version_callback,
            is_eager=True,
        ),
    ] = None,
) -> None:
    """Lockstep - Synchronize data contracts to Unity Catalog."""
    pass


# Common options for plan and apply commands
_path_argument = typer.Argument(
    help="Path to ODCS YAML file or directory containing YAML files.",
    exists=True,
    resolve_path=True,
)

_host_option = typer.Option(
    "--host",
    envvar="DATABRICKS_HOST",
    help="Databricks workspace host URL.",
)

_http_path_option = typer.Option(
    "--sql-endpoint",
    "--http-path",
    envvar="DATABRICKS_HTTP_PATH",
    help="SQL warehouse endpoint path (e.g., /sql/1.0/warehouses/abc123).",
)

_auth_type_option = typer.Option(
    "--auth-type",
    envvar="DATABRICKS_AUTH_TYPE",
    help="Authentication type: oauth (default), pat (Personal Access Token), or sp (Service Principal).",
)

_token_option = typer.Option(
    "--token",
    envvar="DATABRICKS_TOKEN",
    help="Personal Access Token (required when auth-type=pat).",
)

_client_id_option = typer.Option(
    "--client-id",
    envvar="DATABRICKS_CLIENT_ID",
    help="OAuth client ID (required when auth-type=sp).",
)

_client_secret_option = typer.Option(
    "--client-secret",
    envvar="DATABRICKS_CLIENT_SECRET",
    help="OAuth client secret (required when auth-type=sp).",
)

_catalog_override_option = typer.Option(
    "--catalog-override",
    help="Override catalog name from contracts.",
)

_schema_override_option = typer.Option(
    "--schema-override",
    help="Override schema name from contracts.",
)

_table_prefix_option = typer.Option(
    "--table-prefix",
    help="Prefix to add to table names.",
)

_verbose_option = typer.Option(
    "--verbose",
    "-v",
    help="Enable verbose output (debug logging).",
)

_quiet_option = typer.Option(
    "--quiet",
    "-q",
    help="Suppress non-error output.",
)

_junit_xml_option = typer.Option(
    "--junit-xml",
    help="Output results in JUnit XML format to specified file (for CI/CD).",
)


@app.command("plan")
def plan_changes(
    path: Annotated[Path, _path_argument],
    host: Annotated[str | None, _host_option] = None,
    http_path: Annotated[str | None, _http_path_option] = None,
    auth_type: Annotated[str | None, _auth_type_option] = None,
    token: Annotated[str | None, _token_option] = None,
    client_id: Annotated[str | None, _client_id_option] = None,
    client_secret: Annotated[str | None, _client_secret_option] = None,
    catalog_override: Annotated[str | None, _catalog_override_option] = None,
    schema_override: Annotated[str | None, _schema_override_option] = None,
    table_prefix: Annotated[str | None, _table_prefix_option] = None,
    verbose: Annotated[bool, _verbose_option] = False,
    quiet: Annotated[bool, _quiet_option] = False,
    junit_xml: Annotated[Path | None, _junit_xml_option] = None,
) -> None:
    """Show what changes would be made without applying them.

    Compares contracts against Unity Catalog and displays the differences.
    No changes are made to Unity Catalog.

    Authentication types:
    - oauth: Interactive OAuth via Databricks CLI, Azure CLI, etc. (default)
    - pat: Personal Access Token (requires --token)
    - sp: Service Principal / OAuth M2M (requires --client-id and --client-secret)

    Exit codes:
    - 0: No changes needed (in sync)
    - 1: Error occurred
    - 2: Changes detected (drift)

    Examples:

        # Using OAuth (default)
        $ lockstep plan contracts/

        # Using Personal Access Token
        $ lockstep plan contracts/ --auth-type pat --token "dapi..."

        # Using Service Principal
        $ lockstep plan contracts/ --auth-type sp --client-id "..." --client-secret "..."
    """
    _setup_logging(verbose, quiet)

    loader = ContractLoader()
    contracts = _load_contracts(path, loader)

    config = _ensure_databricks_config(host, http_path, auth_type, token, client_id, client_secret)

    sync_options = SyncOptions(
        dry_run=True,
        allow_destructive=True,  # Show all changes in plan
        preserve_extra_tags=False,
        catalog_override=catalog_override,
        schema_override=schema_override,
        table_prefix=table_prefix,
    )

    try:
        with DatabricksConnector(config) as connector:
            sync_service = SyncService(connector)
            results = sync_service.sync_contracts(contracts, sync_options)

            has_changes = False
            for result in results:
                if result.plan and result.plan.has_changes:
                    console.print(format_plan(result.plan))
                    has_changes = True
                else:
                    console.print(f"[dim]No changes needed for {result.table_name}[/dim]")

            # Generate JUnit XML if requested
            if junit_xml:
                generate_junit_xml(
                    results=results,
                    check_mode=True,
                    output_path=junit_xml,
                )
                console.print(f"\n[green]✓[/green] JUnit XML report written to: {junit_xml}")

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


@app.command("apply")
def apply_contracts(
    path: Annotated[Path, _path_argument],
    host: Annotated[str | None, _host_option] = None,
    http_path: Annotated[str | None, _http_path_option] = None,
    auth_type: Annotated[str | None, _auth_type_option] = None,
    token: Annotated[str | None, _token_option] = None,
    client_id: Annotated[str | None, _client_id_option] = None,
    client_secret: Annotated[str | None, _client_secret_option] = None,
    allow_destructive: Annotated[
        bool,
        typer.Option(
            "--allow-destructive",
            help="Allow destructive operations (drop columns, remove tags).",
        ),
    ] = False,
    preserve_extra_tags: Annotated[
        bool,
        typer.Option(
            "--preserve-extra-tags",
            help="Don't remove tags that exist in catalog but not in contract.",
        ),
    ] = False,
    catalog_override: Annotated[str | None, _catalog_override_option] = None,
    schema_override: Annotated[str | None, _schema_override_option] = None,
    table_prefix: Annotated[str | None, _table_prefix_option] = None,
    verbose: Annotated[bool, _verbose_option] = False,
    quiet: Annotated[bool, _quiet_option] = False,
    junit_xml: Annotated[Path | None, _junit_xml_option] = None,
) -> None:
    """Apply ODCS contract(s) to Unity Catalog.

    This command will:
    - Create tables if they don't exist
    - Add missing columns to existing tables
    - Update descriptions for tables and columns
    - Manage tags (add, update, remove)
    - Set primary key and not null constraints

    Use 'lockstep plan' first to preview changes.

    Authentication types:
    - oauth: Interactive OAuth via Databricks CLI, Azure CLI, etc. (default)
    - pat: Personal Access Token (requires --token)
    - sp: Service Principal / OAuth M2M (requires --client-id and --client-secret)

    Examples:

        # Apply using OAuth (default)
        $ lockstep apply contracts/

        # Apply using Service Principal
        $ lockstep apply contracts/ --auth-type sp --client-id "..." --client-secret "..."

        # Allow destructive changes
        $ lockstep apply contracts/ --allow-destructive
    """
    _setup_logging(verbose, quiet)

    loader = ContractLoader()
    contracts = _load_contracts(path, loader)

    config = _ensure_databricks_config(host, http_path, auth_type, token, client_id, client_secret)

    sync_options = SyncOptions(
        dry_run=False,
        allow_destructive=allow_destructive,
        preserve_extra_tags=preserve_extra_tags,
        catalog_override=catalog_override,
        schema_override=schema_override,
        table_prefix=table_prefix,
    )

    try:
        with DatabricksConnector(config) as connector:
            sync_service = SyncService(connector)
            results = sync_service.sync_contracts(contracts, sync_options)

            console.print(format_sync_results(results))

            # Generate JUnit XML if requested
            if junit_xml:
                generate_junit_xml(
                    results=results,
                    check_mode=False,
                    output_path=junit_xml,
                )
                console.print(f"\n[green]✓[/green] JUnit XML report written to: {junit_xml}")

            # Exit with error if any sync failed
            if any(not r.success for r in results):
                raise typer.Exit(1)

    except DatabricksConnectionError as e:
        error_console.print(f"\n[red]❌ Connection error:[/red] {e}")
        raise typer.Exit(1) from None


@app.command("validate")
def validate(
    path: Annotated[
        Path,
        typer.Argument(
            help="Path to ODCS YAML file or directory to validate.",
            exists=True,
            resolve_path=True,
        ),
    ],
    verbose: Annotated[bool, _verbose_option] = False,
    junit_xml: Annotated[Path | None, _junit_xml_option] = None,
) -> None:
    """Validate ODCS contract YAML files without connecting to Databricks.

    Checks that contracts:
    - Are valid YAML
    - Conform to the expected ODCS structure
    - Have all required fields

    Examples:

        # Validate a single file
        $ lockstep validate contracts/customer.yaml

        # Validate all files in a directory
        $ lockstep validate contracts/

        # Output results as JUnit XML
        $ lockstep validate contracts/ --junit-xml reports/validation.xml
    """
    _setup_logging(verbose, quiet=False)

    loader = ContractLoader()
    console.print(f"\n[bold]Validating contracts in:[/bold] {path}\n")

    # Collect files to validate
    yaml_files: list[Path] = []
    if path.is_file():
        yaml_files = [path]
    else:
        yaml_files = list(path.glob("**/*.yaml")) + list(path.glob("**/*.yml"))

    if not yaml_files:
        console.print("[yellow]No YAML files found.[/yellow]")
        raise typer.Exit(0)

    # Validate each file
    valid_count = 0
    invalid_count = 0
    valid_files: list[Path] = []
    invalid_files: list[tuple[Path, list[str]]] = []
    results_table = Table(title="Validation Results")
    results_table.add_column("File", style="cyan")
    results_table.add_column("Status")
    results_table.add_column("Details")

    for yaml_file in sorted(yaml_files):
        is_valid, errors = loader.validate_file(yaml_file)
        rel_path = yaml_file.relative_to(path) if path.is_dir() else yaml_file.name

        if is_valid:
            valid_count += 1
            valid_files.append(yaml_file)
            results_table.add_row(
                str(rel_path),
                "[green]✓ Valid[/green]",
                "",
            )
        else:
            invalid_count += 1
            invalid_files.append((yaml_file, errors))
            error_details = "\n".join(errors[:3])  # Show first 3 errors
            if len(errors) > 3:
                error_details += f"\n... and {len(errors) - 3} more"
            results_table.add_row(
                str(rel_path),
                "[red]✗ Invalid[/red]",
                error_details,
            )

    console.print(results_table)
    console.print()

    # Generate JUnit XML if requested
    if junit_xml:
        generate_validation_junit_xml(
            valid_files=valid_files,
            invalid_files=invalid_files,
            output_path=junit_xml,
        )
        console.print(f"[green]✓[/green] JUnit XML report written to: {junit_xml}\n")

    # Summary
    total = valid_count + invalid_count
    if invalid_count == 0:
        console.print(
            Panel(
                f"[green]All {total} contract(s) are valid! ✓[/green]",
                border_style="green",
            )
        )
    else:
        console.print(
            Panel(
                f"[red]{invalid_count} of {total} contract(s) failed validation[/red]",
                border_style="red",
            )
        )
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
