"""Main CLI entry point for Lockstep.

Provides commands for synchronizing data contracts to Databricks Unity Catalog.
"""

from __future__ import annotations

from typing import Annotated

import typer

# Import command modules
from lockstep.cli.apply_cmd import apply_contracts
from lockstep.cli.common import version_callback
from lockstep.cli.plan_cmd import plan_changes
from lockstep.cli.validate_cmd import validate

# Initialize CLI app
app = typer.Typer(
    name="lockstep",
    help="Synchronize data contracts to Databricks Unity Catalog.",
    add_completion=True,
    rich_markup_mode="rich",
)


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


# Register commands
app.command("plan")(plan_changes)
app.command("apply")(apply_contracts)
app.command("validate")(validate)


# Try to import and register DQX commands if available
try:
    from lockstep.cli.dqx_commands import dqx_app
    app.add_typer(dqx_app, name="dqx")
except ImportError:
    # DQX module not available
    pass


if __name__ == "__main__":
    app()
