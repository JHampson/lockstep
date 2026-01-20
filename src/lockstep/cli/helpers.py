"""CLI helper functions for command implementations."""

from __future__ import annotations

import logging
from pathlib import Path

import typer
from rich.console import Console
from rich.logging import RichHandler

from lockstep import __version__
from lockstep.cli.formatters import format_validation_report
from lockstep.databricks import DatabricksConfig
from lockstep.databricks.config import AuthType
from lockstep.models.contract import Contract
from lockstep.services import ContractLoader, ContractLoadError

# Shared console instances
console = Console()
error_console = Console(stderr=True)


def setup_logging(verbose: bool, quiet: bool) -> None:
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


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"lockstep version {__version__}")
        raise typer.Exit()


def validate_output_format(format_str: str | None) -> str:
    """Validate and normalize the output format option."""
    output_format = (format_str or "table").lower()
    if output_format not in ("table", "json", "junit"):
        error_console.print(
            f"[red]Invalid format '{format_str}'. Must be: table, json, or junit[/red]"
        )
        raise typer.Exit(1)
    return output_format


def get_databricks_config(
    host: str | None,
    http_path: str | None,
    auth_type: str | None,
    token: str | None,
    client_id: str | None,
    client_secret: str | None,
    profile: str | None = None,
) -> DatabricksConfig:
    """Build Databricks configuration from CLI options and environment.

    Precedence: CLI parameters > Databricks CLI profile > environment variables > config file
    """
    # Determine auth type
    resolved_auth_type = AuthType.OAUTH  # Default
    if auth_type:
        try:
            resolved_auth_type = AuthType(auth_type.lower())
        except ValueError:
            error_console.print(
                f"[red]Invalid auth-type '{auth_type}'. Must be one of: oauth, pat, sp, profile[/red]"
            )
            raise typer.Exit(1) from None

    return DatabricksConfig(
        profile=profile,
        host=host or "",
        http_path=http_path or "",
        auth_type=resolved_auth_type,
        token=token,
        client_id=client_id,
        client_secret=client_secret,
    )


def ensure_databricks_config(
    host: str | None,
    http_path: str | None,
    auth_type: str | None,
    token: str | None,
    client_id: str | None,
    client_secret: str | None,
    profile: str | None = None,
) -> DatabricksConfig:
    """Build and validate Databricks configuration, exiting on failure."""
    try:
        config = get_databricks_config(
            host, http_path, auth_type, token, client_id, client_secret, profile
        )
        if not config.is_configured():
            auth_help = ""
            if config.auth_type == AuthType.PAT:
                auth_help = "\n  • --token required for auth-type=pat"
            elif config.auth_type == AuthType.SP:
                auth_help = "\n  • --client-id and --client-secret required for auth-type=sp"

            error_console.print(
                "\n[red]❌ Databricks connection not configured.[/red]\n"
                "Please provide connection details via:\n"
                "  • CLI options: --profile OR --host, --sql-endpoint\n"
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


def load_contracts(path: Path, loader: ContractLoader) -> list[Contract]:
    """Load contracts from a file or directory, with CLI output and error handling.

    Args:
        path: Path to YAML file or directory.
        loader: ContractLoader instance.

    Returns:
        List of loaded contracts.

    Raises:
        typer.Exit: On load failure or no contracts found.
    """
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

