"""Result presentation functions that format and display action results.

These functions handle the output formatting (table, json, junit) and
file writing based on user preferences.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from lockstep.cli.actions import ApplyResult, PlanResult, ValidateResult
from lockstep.cli.formatters import format_plan, format_sync_results
from lockstep.cli.junit_reporter import generate_junit_xml, generate_validation_junit_xml

# Use shared console instances
console = Console()
error_console = Console(stderr=True)


class OutputFormat(Enum):
    """Output format options for CLI commands."""

    TABLE = "table"
    JSON = "json"
    JUNIT = "junit"


@dataclass
class OutputOptions:
    """Options controlling output verbosity and format."""

    format: OutputFormat = OutputFormat.TABLE
    out_path: Path | None = None
    quiet: bool = False
    verbose: bool = False


def present_plan_result(
    result: PlanResult,
    options: OutputOptions,
) -> None:
    """Present plan results to the user.

    Args:
        result: The plan result to present.
        options: Output options controlling format and verbosity.
    """
    output_format = options.format
    out_path = options.out_path

    if output_format == OutputFormat.JUNIT:
        output_content = generate_junit_xml(
            results=result.results,
            check_mode=True,
            output_path=None,
        )
        console.print(output_content)
        if out_path:
            out_path.write_text(output_content)
            if not options.quiet:
                console.print(f"\n[green]✓[/green] JUnit XML written to: {out_path}")

    elif output_format == OutputFormat.JSON:
        output_data: dict[str, object] = {
            "command": "plan",
            "timestamp": result.timestamp,
            "results": [
                {
                    "contract_name": r.contract_name,
                    "table_name": r.table_name,
                    "has_changes": r.plan.has_changes if r.plan else False,
                    "actions": [
                        {
                            "type": a.action_type.value,
                            "target": a.target,
                            "details": a.details,
                            **({"sql": a.sql} if options.verbose else {}),
                        }
                        for a in (r.plan.actions if r.plan else [])
                    ],
                }
                for r in result.results
            ],
        }
        output_content = json.dumps(output_data, indent=2)
        console.print(output_content)
        if out_path:
            out_path.write_text(output_content)
            if not options.quiet:
                console.print(f"\n[green]✓[/green] JSON written to: {out_path}")

    else:  # table format (default)
        output_lines = []
        for sync_result in result.results:
            if sync_result.plan and sync_result.plan.has_changes:
                formatted = format_plan(sync_result.plan, show_sql=options.verbose)
                console.print(formatted)
                output_lines.append(str(formatted))
            elif not options.quiet:
                msg = f"[dim]No changes needed for {sync_result.table_name}[/dim]"
                console.print(msg)
                output_lines.append(f"No changes needed for {sync_result.table_name}")

        if out_path:
            out_path.write_text("\n".join(output_lines))
            if not options.quiet:
                console.print(f"\n[green]✓[/green] Output written to: {out_path}")


def present_plan_summary(result: PlanResult, quiet: bool = False) -> None:
    """Present the final summary for a plan result.

    Args:
        result: The plan result.
        quiet: If True, suppress informational messages.
    """
    if quiet:
        return

    if result.has_changes:
        console.print(
            "\n[yellow]⚠️  Changes detected. Run 'lockstep apply' to apply changes.[/yellow]"
        )
    else:
        console.print("\n[green]✓ All contracts are in sync with Unity Catalog.[/green]")


def present_apply_result(
    result: ApplyResult,
    options: OutputOptions,
) -> None:
    """Present apply results to the user.

    Args:
        result: The apply result to present.
        options: Output options controlling format and verbosity.
    """
    output_format = options.format
    out_path = options.out_path

    if output_format == OutputFormat.JUNIT:
        output_content = generate_junit_xml(
            results=result.results,
            check_mode=False,
            output_path=None,
        )
        console.print(output_content)
        if out_path:
            out_path.write_text(output_content)
            if not options.quiet:
                console.print(f"\n[green]✓[/green] JUnit XML written to: {out_path}")

    elif output_format == OutputFormat.JSON:
        output_data: dict[str, object] = {
            "command": "apply",
            "timestamp": result.timestamp,
            "results": [
                {
                    "contract_name": r.contract_name,
                    "table_name": r.table_name,
                    "success": r.success,
                    "actions_applied": r.actions_applied,
                    "errors": r.errors,
                }
                for r in result.results
            ],
        }
        if result.plan_file:
            output_data["plan_file"] = result.plan_file
            output_data["summary"] = {
                "applied": result.total_applied,
                "failed": result.total_failed,
            }
        output_content = json.dumps(output_data, indent=2)
        console.print(output_content)
        if out_path:
            out_path.write_text(output_content)
            if not options.quiet:
                console.print(f"\n[green]✓[/green] JSON written to: {out_path}")

    else:  # table format (default)
        if result.plan_file:
            # For saved plan applies, show summary
            if not options.quiet:
                summary_msg = (
                    f"\n[bold]Summary:[/bold] {result.total_applied} applied, "
                    f"{result.total_failed} failed"
                )
                console.print(summary_msg)
            if out_path:
                out_path.write_text(
                    f"Summary: {result.total_applied} applied, {result.total_failed} failed"
                )
                if not options.quiet:
                    console.print(f"[green]✓[/green] Output written to: {out_path}")
        else:
            formatted_results = format_sync_results(result.results)
            console.print(formatted_results)
            if out_path:
                out_path.write_text(str(formatted_results))
                if not options.quiet:
                    console.print(f"\n[green]✓[/green] Output written to: {out_path}")


def present_apply_progress(
    action_desc: str,
    success: bool,
    error: str | None = None,
    quiet: bool = False,
    verbose: bool = False,
    sql: str | None = None,
) -> None:
    """Present progress for a single action during apply.

    Args:
        action_desc: Description of the action.
        success: Whether the action succeeded.
        error: Error message if failed.
        quiet: If True, only show errors.
        verbose: If True, show SQL statements.
        sql: SQL statement (shown in verbose mode).
    """
    if quiet and success:
        return

    if success:
        console.print(f"  [green]✓[/green] {action_desc}")
        if verbose and sql:
            console.print(f"    [dim]{sql}[/dim]")
    elif error:
        console.print(f"  [red]✗[/red] {action_desc}: {error}")
    else:
        console.print(f"  [yellow]⚠[/yellow] {action_desc} (no SQL)")


def present_validate_result(
    result: ValidateResult,
    options: OutputOptions,
) -> None:
    """Present validate results to the user.

    Args:
        result: The validate result to present.
        options: Output options controlling format and verbosity.
    """
    output_format = options.format
    out_path = options.out_path

    valid_files = [r.file_path for r in result.results if r.valid]
    invalid_files = [(r.file_path, r.errors) for r in result.results if not r.valid]

    if output_format == OutputFormat.JUNIT:
        output_content = generate_validation_junit_xml(
            valid_files=valid_files,
            invalid_files=invalid_files,
            output_path=None,
        )
        console.print(output_content)
        if out_path:
            out_path.write_text(output_content)
            if not options.quiet:
                console.print(f"\n[green]✓[/green] JUnit XML written to: {out_path}")

    elif output_format == OutputFormat.JSON:
        output_data = {
            "command": "validate",
            "timestamp": result.timestamp,
            "summary": {
                "total": result.total,
                "valid": result.valid_count,
                "invalid": result.invalid_count,
            },
            "results": [
                {
                    "file": r.relative_path,
                    "valid": r.valid,
                    "errors": r.errors,
                }
                for r in result.results
            ],
        }
        output_content = json.dumps(output_data, indent=2)
        console.print(output_content)
        if out_path:
            out_path.write_text(output_content)
            if not options.quiet:
                console.print(f"\n[green]✓[/green] JSON written to: {out_path}")

    else:  # table format (default)
        results_table = Table(title="Validation Results")
        results_table.add_column("File", style="cyan")
        results_table.add_column("Status")
        if options.verbose:
            results_table.add_column("Details")

        for file_result in result.results:
            if file_result.valid:
                if options.verbose:
                    results_table.add_row(file_result.relative_path, "[green]✓ Valid[/green]", "")
                elif not options.quiet:
                    results_table.add_row(file_result.relative_path, "[green]✓ Valid[/green]")
            else:
                error_details = "\n".join(file_result.errors[:3])
                if len(file_result.errors) > 3:
                    error_details += f"\n... and {len(file_result.errors) - 3} more"
                if options.verbose:
                    # Show all errors in verbose mode
                    error_details = "\n".join(file_result.errors)
                    results_table.add_row(
                        file_result.relative_path, "[red]✗ Invalid[/red]", error_details
                    )
                else:
                    results_table.add_row(
                        file_result.relative_path, "[red]✗ Invalid[/red]", error_details
                    )

        console.print(results_table)
        if not options.quiet:
            console.print()

        if out_path:
            lines = ["Validation Results", "=" * 40]
            for file_result in result.results:
                if file_result.valid:
                    lines.append(f"✓ {file_result.relative_path}: Valid")
                else:
                    lines.append(f"✗ {file_result.relative_path}: Invalid")
                    errors_to_show = (
                        file_result.errors if options.verbose else file_result.errors[:3]
                    )
                    for err in errors_to_show:
                        lines.append(f"  - {err}")
            out_path.write_text("\n".join(lines))
            if not options.quiet:
                console.print(f"[green]✓[/green] Output written to: {out_path}\n")


def present_validate_summary(result: ValidateResult, quiet: bool = False) -> None:
    """Present the final summary for a validate result.

    Args:
        result: The validate result.
        quiet: If True, suppress informational messages (errors still shown).
    """
    if result.invalid_count == 0:
        if not quiet:
            console.print(
                Panel(
                    f"[green]All {result.total} contract(s) are valid! ✓[/green]",
                    border_style="green",
                )
            )
    else:
        # Always show failure summary, even in quiet mode
        console.print(
            Panel(
                f"[red]{result.invalid_count} of {result.total} contract(s) failed validation[/red]",
                border_style="red",
            )
        )


def present_error(message: str) -> None:
    """Present an error message.

    Args:
        message: The error message to display.
    """
    error_console.print(f"\n[red]❌ {message}[/red]")


def present_info(message: str, quiet: bool = False) -> None:
    """Present an informational message.

    Args:
        message: The message to display.
        quiet: If True, suppress the message.
    """
    if not quiet:
        console.print(message)


def present_config_error(error: Exception) -> None:
    """Present a configuration error with helpful guidance.

    Args:
        error: The MissingConfigurationError exception.
    """
    from lockstep.cli.exceptions import MissingConfigurationError

    if isinstance(error, MissingConfigurationError):
        auth_help = f"\n  • {error.missing}" if error.missing else ""
        error_console.print(
            "\n[red]❌ Databricks connection not configured.[/red]\n"
            "Please provide connection details via:\n"
            "  • CLI options: --profile OR --host, --sql-endpoint\n"
            "  • Environment variables: DATABRICKS_HOST, DATABRICKS_HTTP_PATH\n"
            f"  • Config file: ~/.lockstep.yaml{auth_help}\n"
        )
    else:
        error_console.print(f"\n[red]❌ Configuration error:[/red] {error}")


def present_contract_load_error(error: Exception) -> None:
    """Present a contract loading error.

    Args:
        error: The ContractLoadingError exception.
    """
    from lockstep.cli.exceptions import ContractLoadingError
    from lockstep.cli.formatters import format_validation_report

    if isinstance(error, ContractLoadingError):
        if error.validation_errors:
            error_console.print(format_validation_report(error.validation_errors))
        else:
            error_console.print(f"\n[red]❌ {error}[/red]")
            for err in error.errors:
                error_console.print(f"   • {err}")
    else:
        error_console.print(f"\n[red]❌ Failed to load contracts:[/red] {error}")
