"""Main CLI entry point for ODCS Sync.

Provides commands for synchronizing ODCS contracts to Databricks Unity Catalog.
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

from odcs_sync import __version__
from odcs_sync.cli.formatters import format_plan, format_sync_results, format_validation_report
from odcs_sync.databricks import DatabricksConfig, DatabricksConnector
from odcs_sync.databricks.connector import DatabricksConnectionError
from odcs_sync.services import ContractLoader, ContractLoadError, SyncService
from odcs_sync.services.sync import SyncOptions

# Initialize CLI app
app = typer.Typer(
    name="odcs-sync",
    help="Synchronize Open Data Contract Standard (ODCS) YAML to Databricks Unity Catalog.",
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
    token: str | None,
    use_oauth: bool,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> DatabricksConfig:
    """Build Databricks configuration from CLI options and environment."""
    kwargs: dict[str, str | bool | None] = {}
    if host:
        kwargs["host"] = host
    if http_path:
        kwargs["http_path"] = http_path
    if token:
        kwargs["token"] = token
    kwargs["use_oauth"] = use_oauth
    # Service Principal / OAuth M2M auth
    if client_id:
        kwargs["client_id"] = client_id
    if client_secret:
        kwargs["client_secret"] = client_secret

    return DatabricksConfig(**kwargs)


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"odcs-sync version {__version__}")
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
    """ODCS Sync - Synchronize data contracts to Unity Catalog."""
    pass


@app.command("from-file")
def sync_from_file(
    path: Annotated[
        Path,
        typer.Argument(
            help="Path to ODCS YAML file or directory containing YAML files.",
            exists=True,
            resolve_path=True,
        ),
    ],
    # Databricks connection options
    host: Annotated[
        str | None,
        typer.Option(
            "--host",
            envvar="DATABRICKS_HOST",
            help="Databricks workspace host URL.",
        ),
    ] = None,
    http_path: Annotated[
        str | None,
        typer.Option(
            "--http-path",
            envvar="DATABRICKS_HTTP_PATH",
            help="HTTP path for SQL warehouse.",
        ),
    ] = None,
    token: Annotated[
        str | None,
        typer.Option(
            "--token",
            envvar="DATABRICKS_TOKEN",
            help="Personal Access Token (if not using OAuth).",
        ),
    ] = None,
    use_oauth: Annotated[
        bool,
        typer.Option(
            "--oauth/--no-oauth",
            help="Use OAuth authentication (default). Disable with --no-oauth to use token.",
        ),
    ] = True,
    # Service Principal / OAuth M2M authentication (works on AWS and Azure)
    client_id: Annotated[
        str | None,
        typer.Option(
            "--client-id",
            envvar="DATABRICKS_CLIENT_ID",
            help="OAuth client ID for service principal / M2M auth.",
        ),
    ] = None,
    client_secret: Annotated[
        str | None,
        typer.Option(
            "--client-secret",
            envvar="DATABRICKS_CLIENT_SECRET",
            help="OAuth client secret for service principal / M2M auth.",
        ),
    ] = None,
    # Sync options
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-n",
            help="Show planned changes without executing them.",
        ),
    ] = False,
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
    # Override options
    catalog_override: Annotated[
        str | None,
        typer.Option(
            "--catalog-override",
            help="Override catalog name from contracts.",
        ),
    ] = None,
    schema_override: Annotated[
        str | None,
        typer.Option(
            "--schema-override",
            help="Override schema name from contracts.",
        ),
    ] = None,
    table_prefix: Annotated[
        str | None,
        typer.Option(
            "--table-prefix",
            help="Prefix to add to table names.",
        ),
    ] = None,
    # Output options
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Enable verbose output (debug logging).",
        ),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Suppress non-error output.",
        ),
    ] = False,
) -> None:
    """Synchronize ODCS contract(s) from a YAML file or directory to Unity Catalog.

    This command will:
    - Create tables if they don't exist
    - Add missing columns to existing tables
    - Update descriptions for tables and columns
    - Manage tags (add, update, remove)
    - Set primary key and not null constraints
    - Manage certification status

    Examples:

        # Sync a single contract file
        $ odcs-sync from-file contracts/customer.yaml

        # Sync all contracts in a directory (dry run)
        $ odcs-sync from-file contracts/ --dry-run

        # Sync with catalog override
        $ odcs-sync from-file contracts/ --catalog-override dev_catalog

        # Allow destructive changes (column drops)
        $ odcs-sync from-file contracts/ --allow-destructive
    """
    _setup_logging(verbose, quiet)

    # Load contracts
    loader = ContractLoader()
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

    # Build configuration
    try:
        config = _get_databricks_config(
            host, http_path, token, use_oauth, client_id, client_secret
        )
        if not config.is_configured():
            error_console.print(
                "\n[red]❌ Databricks connection not configured.[/red]\n"
                "Please provide connection details via:\n"
                "  • Environment variables: DATABRICKS_HOST, DATABRICKS_HTTP_PATH\n"
                "  • CLI options: --host, --http-path\n"
                "  • Config file: ~/.odcs_sync.yaml\n"
            )
            raise typer.Exit(1)
    except Exception as e:
        error_console.print(f"\n[red]❌ Configuration error:[/red] {e}")
        raise typer.Exit(1) from None

    # Build sync options
    sync_options = SyncOptions(
        dry_run=dry_run,
        allow_destructive=allow_destructive,
        preserve_extra_tags=preserve_extra_tags,
        catalog_override=catalog_override,
        schema_override=schema_override,
        table_prefix=table_prefix,
    )

    # Connect and sync
    try:
        with DatabricksConnector(config) as connector:
            sync_service = SyncService(connector)

            if dry_run:
                console.print("[bold cyan]🔍 DRY RUN MODE[/bold cyan] - No changes will be made\n")

            results = sync_service.sync_contracts(contracts, sync_options)

            # Display results
            if dry_run:
                for result in results:
                    if result.plan and result.plan.has_changes:
                        console.print(format_plan(result.plan))
                    else:
                        console.print(f"[dim]No changes needed for {result.table_name}[/dim]")
            else:
                console.print(format_sync_results(results))

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
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Show detailed validation output.",
        ),
    ] = False,
) -> None:
    """Validate ODCS contract YAML files without syncing.

    Checks that contracts:
    - Are valid YAML
    - Conform to the expected ODCS structure
    - Have all required fields

    Examples:

        # Validate a single file
        $ odcs-sync validate contracts/customer.yaml

        # Validate all files in a directory
        $ odcs-sync validate contracts/
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
    results_table = Table(title="Validation Results")
    results_table.add_column("File", style="cyan")
    results_table.add_column("Status")
    results_table.add_column("Details")

    for yaml_file in sorted(yaml_files):
        is_valid, errors = loader.validate_file(yaml_file)
        rel_path = yaml_file.relative_to(path) if path.is_dir() else yaml_file.name

        if is_valid:
            valid_count += 1
            results_table.add_row(
                str(rel_path),
                "[green]✓ Valid[/green]",
                "",
            )
        else:
            invalid_count += 1
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
