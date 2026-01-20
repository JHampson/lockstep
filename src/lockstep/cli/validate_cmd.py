"""Validate command for validating contract YAML files."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from lockstep.cli.actions import execute_validate
from lockstep.cli.common import (
    FormatArg,
    OutputArg,
    QuietArg,
    VerboseArg,
    setup_logging,
    validate_output_format,
)
from lockstep.cli.output import (
    OutputOptions,
    present_info,
    present_validate_result,
    present_validate_summary,
)
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
    quiet: QuietArg = False,
) -> None:
    """Validate ODCS contract YAML files without connecting to Databricks.

    Checks that contracts:
    - Are valid YAML
    - Conform to the expected ODCS structure
    - Have all required fields

    Use --format to specify output format (table, json, junit).
    Use --out to write output to a file (in addition to displaying).
    Use --verbose to show all error details.
    Use --quiet to suppress informational messages (errors still shown).

    Examples:

        # Validate a single file
        $ lockstep validate contracts/customer.yaml

        # Validate all files in a directory
        $ lockstep validate contracts/

        # Output as JSON
        $ lockstep validate contracts/ --format json

        # Output as JUnit XML to file
        $ lockstep validate contracts/ --format junit --out validation.xml

        # Quiet mode (only show failures)
        $ lockstep validate contracts/ --quiet
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

    present_info(f"\n[bold]Validating contracts in:[/bold] {path}\n", quiet=quiet)

    # Execute the validate action
    loader = ContractLoader()
    result = execute_validate(path, loader)

    # Handle empty results
    if result.total == 0:
        present_info("[yellow]No YAML files found.[/yellow]", quiet=quiet)
        raise typer.Exit(0)

    # Present the results
    present_validate_result(result, output_options)

    # Present summary
    present_validate_summary(result, quiet=quiet)

    # Exit with error if any validation failed
    if not result.success:
        raise typer.Exit(1)
