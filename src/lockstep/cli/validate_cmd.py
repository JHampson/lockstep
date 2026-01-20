"""Validate command for validating contract YAML files."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.panel import Panel
from rich.table import Table

from lockstep.cli.common import (
    FormatArg,
    OutputArg,
    VerboseArg,
    console,
    setup_logging,
    validate_output_format,
)
from lockstep.cli.junit_reporter import generate_validation_junit_xml
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

    for yaml_file in sorted(yaml_files):
        is_valid, errors = loader.validate_file(yaml_file)

        if is_valid:
            valid_count += 1
            valid_files.append(yaml_file)
        else:
            invalid_count += 1
            invalid_files.append((yaml_file, errors))

    total = valid_count + invalid_count

    # Generate output based on format
    if output_format == "junit":
        output_content = generate_validation_junit_xml(
            valid_files=valid_files,
            invalid_files=invalid_files,
            output_path=None,  # Return string instead of writing
        )
        console.print(output_content)
        if out:
            out.write_text(output_content)
            console.print(f"\n[green]✓[/green] JUnit XML written to: {out}")

    elif output_format == "json":
        output_data = {
            "command": "validate",
            "timestamp": datetime.now(UTC).isoformat(),
            "summary": {
                "total": total,
                "valid": valid_count,
                "invalid": invalid_count,
            },
            "results": [
                {
                    "file": str(f.relative_to(path) if path.is_dir() else f.name),
                    "valid": True,
                    "errors": [],
                }
                for f in valid_files
            ] + [
                {
                    "file": str(f.relative_to(path) if path.is_dir() else f.name),
                    "valid": False,
                    "errors": errors,
                }
                for f, errors in invalid_files
            ],
        }
        output_content = json.dumps(output_data, indent=2)
        console.print(output_content)
        if out:
            out.write_text(output_content)
            console.print(f"\n[green]✓[/green] JSON written to: {out}")

    else:  # table format (default)
        results_table = Table(title="Validation Results")
        results_table.add_column("File", style="cyan")
        results_table.add_column("Status")
        results_table.add_column("Details")

        for yaml_file in valid_files:
            rel_path = yaml_file.relative_to(path) if path.is_dir() else yaml_file.name
            results_table.add_row(str(rel_path), "[green]✓ Valid[/green]", "")

        for yaml_file, errors in invalid_files:
            rel_path = yaml_file.relative_to(path) if path.is_dir() else yaml_file.name
            error_details = "\n".join(errors[:3])  # Show first 3 errors
            if len(errors) > 3:
                error_details += f"\n... and {len(errors) - 3} more"
            results_table.add_row(str(rel_path), "[red]✗ Invalid[/red]", error_details)

        console.print(results_table)
        console.print()

        if out:
            # Write plain text version to file
            lines = ["Validation Results", "=" * 40]
            for yaml_file in valid_files:
                rel_path = yaml_file.relative_to(path) if path.is_dir() else yaml_file.name
                lines.append(f"✓ {rel_path}: Valid")
            for yaml_file, errors in invalid_files:
                rel_path = yaml_file.relative_to(path) if path.is_dir() else yaml_file.name
                lines.append(f"✗ {rel_path}: Invalid")
                for err in errors[:3]:
                    lines.append(f"  - {err}")
            out.write_text("\n".join(lines))
            console.print(f"[green]✓[/green] Output written to: {out}\n")

    # Summary
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

