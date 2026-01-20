"""Validate command for validating contract YAML files."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from lockstep.cli.actions import execute_validate
from lockstep.cli.common import (
    FormatArg,
    OutputArg,
    VerboseArg,
    console,
    setup_logging,
    validate_output_format,
)
from lockstep.cli.output import present_validate_result, present_validate_summary
from lockstep.services import ContractLoader

# Create the validate command app
validate_app = typer.Typer()


@validate_app.callback(invoke_without_command=True)
def validate(
    path: Annotated[
        Path,
        typer.Argument(
            help="Path to ODCS YAML file or directory to validate.",
            exists=True,
            resolve_path=True,
        ),
    ],
    format: FormatArg = None,
    out: OutputArg = None,
    verbose: VerboseArg = False,
) -> None:
    """Validate ODCS contract YAML files without connecting to Databricks.

    Checks that contracts:
    - Are valid YAML
    - Conform to the expected ODCS structure
    - Have all required fields

    Use --format to specify output format (table, json, junit).
    Use --out to write output to a file (in addition to displaying).

    Examples:

        # Validate a single file
        $ lockstep validate contracts/customer.yaml

        # Validate all files in a directory
        $ lockstep validate contracts/

        # Output as JSON
        $ lockstep validate contracts/ --format json

        # Output as JUnit XML to file
        $ lockstep validate contracts/ --format junit --out validation.xml
    """
    setup_logging(verbose, quiet=False)
    output_format = validate_output_format(format)

    console.print(f"\n[bold]Validating contracts in:[/bold] {path}\n")

    # Execute the validate action
    loader = ContractLoader()
    result = execute_validate(path, loader)

    # Handle empty results
    if result.total == 0:
        console.print("[yellow]No YAML files found.[/yellow]")
        raise typer.Exit(0)

    # Present the results
    present_validate_result(result, output_format, out)

    # Present summary
    present_validate_summary(result)

    # Exit with error if any validation failed
    if not result.success:
        raise typer.Exit(1)
