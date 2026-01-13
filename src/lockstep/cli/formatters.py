"""Output formatters for CLI display.

Provides rich formatting for plans, results, and validation reports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
    from lockstep.models.catalog_state import SyncPlan
    from lockstep.services.contract_loader import ContractLoadError
    from lockstep.services.sync import SyncResult


# Action type icons and colors
ACTION_STYLES = {
    "create_table": ("🆕", "green"),
    "add_column": ("➕", "green"),
    "drop_column": ("🗑️", "red"),
    "update_table_description": ("📝", "blue"),
    "update_column_description": ("📝", "blue"),
    "update_column_type": ("🔄", "yellow"),
    "add_table_tag": ("🏷️", "green"),
    "update_table_tag": ("🏷️", "yellow"),
    "remove_table_tag": ("🏷️", "red"),
    "add_column_tag": ("🏷️", "green"),
    "update_column_tag": ("🏷️", "yellow"),
    "remove_column_tag": ("🏷️", "red"),
    "add_primary_key": ("🔑", "green"),
    "drop_primary_key": ("🔑", "red"),
    "add_not_null": ("❗", "green"),
    "drop_not_null": ("❗", "red"),
}


def format_plan(plan: SyncPlan) -> Panel:
    """Format a sync plan for display.

    Args:
        plan: The sync plan to format.

    Returns:
        Rich Panel containing the formatted plan.
    """
    if not plan.has_changes:
        return Panel(
            f"[dim]No changes for {plan.table_name}[/dim]",
            title=f"📋 {plan.contract_name}",
            border_style="dim",
        )

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    table.add_column("", width=3)
    table.add_column("Action", style="cyan")
    table.add_column("Target")
    table.add_column("Details", style="dim")

    for action in plan.actions:
        icon, color = ACTION_STYLES.get(action.action_type.value, ("•", "white"))
        action_text = Text(action.action_type.value.replace("_", " ").title())
        action_text.stylize(color)

        # Format details
        details = ""
        if action.details:
            detail_parts = [f"{k}={v}" for k, v in action.details.items()]
            details = ", ".join(detail_parts)

        table.add_row(icon, action_text, action.target, details)

    # Add summary
    summary = plan.get_summary()
    summary_parts = [f"{count} {action_type}" for action_type, count in summary.items()]
    summary_text = f"\n[bold]Summary:[/bold] {', '.join(summary_parts)}"

    if plan.has_destructive_changes:
        summary_text += "\n[yellow]⚠️  Plan contains destructive changes[/yellow]"

    content = Group(table, Text(summary_text, style="dim"))

    return Panel(
        content,
        title=f"📋 [bold]{plan.contract_name}[/bold] → {plan.table_name}",
        border_style="blue",
    )


def format_sync_results(results: list[SyncResult]) -> Panel:
    """Format sync results for display.

    Args:
        results: List of sync results.

    Returns:
        Rich Panel containing the formatted results.
    """
    table = Table(show_header=True, header_style="bold")
    table.add_column("Contract", style="cyan")
    table.add_column("Table")
    table.add_column("Status")
    table.add_column("Applied")
    table.add_column("Skipped")

    total_applied = 0
    total_skipped = 0
    total_failed = 0

    for result in results:
        if result.success:
            status = "[green]✓ Success[/green]"
        else:
            status = "[red]✗ Failed[/red]"
            total_failed += 1

        table.add_row(
            result.contract_name,
            result.table_name,
            status,
            str(result.actions_applied),
            str(result.actions_skipped),
        )

        total_applied += result.actions_applied
        total_skipped += result.actions_skipped

        # Show errors if any
        if result.errors:
            for error in result.errors:
                table.add_row("", "", f"[red]  └─ {error}[/red]", "", "")

    # Summary row
    table.add_section()
    if total_failed > 0:
        summary_status = f"[red]{total_failed} failed[/red]"
    else:
        summary_status = "[green]All succeeded[/green]"

    table.add_row(
        f"[bold]Total ({len(results)} contracts)[/bold]",
        "",
        summary_status,
        f"[bold]{total_applied}[/bold]",
        f"[bold]{total_skipped}[/bold]",
    )

    border_style = "red" if total_failed > 0 else "green"
    title = "❌ Sync Results" if total_failed > 0 else "✅ Sync Results"

    return Panel(table, title=title, border_style=border_style)


def format_validation_report(errors: list[ContractLoadError]) -> Panel:
    """Format validation errors for display.

    Args:
        errors: List of contract load errors.

    Returns:
        Rich Panel containing the formatted errors.
    """
    content_parts = []

    for error in errors:
        file_text = Text()
        file_text.append("📄 ", style="bold")
        file_text.append(str(error.path) if error.path else "Unknown file", style="cyan")
        content_parts.append(file_text)

        error_text = Text(f"   Error: {error}", style="red")
        content_parts.append(error_text)

        if error.errors:
            for err_msg in error.errors[:5]:  # Limit to first 5 errors
                detail_text = Text(f"   • {err_msg}", style="dim red")
                content_parts.append(detail_text)
            if len(error.errors) > 5:
                more_text = Text(f"   ... and {len(error.errors) - 5} more errors", style="dim")
                content_parts.append(more_text)

        content_parts.append(Text(""))  # Blank line between files

    content = Group(*content_parts)

    return Panel(
        content,
        title=f"❌ Validation Errors ({len(errors)} file(s))",
        border_style="red",
    )
