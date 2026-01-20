"""Main CLI entry point for Lockstep.

Provides commands for synchronizing data contracts to Databricks Unity Catalog.
"""

from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console

from lockstep import __version__

# Import command modules
from lockstep.cli.apply_cmd import apply_contracts
from lockstep.cli.plan_cmd import plan_changes
from lockstep.cli.validate_cmd import validate

# Initialize CLI app
app = typer.Typer(
    name="lockstep",
    help="Synchronize data contracts to Databricks Unity Catalog.",
    add_completion=True,
    rich_markup_mode="rich",
)

_console = Console()


def _version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        _console.print(f"lockstep version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = None,
) -> None:
    """Lockstep - Synchronize data contracts to Unity Catalog."""
    pass


# Register commands
app.command("plan")(plan_changes)
app.command("apply")(apply_contracts)
app.command("validate")(validate)


if __name__ == "__main__":
    app()
