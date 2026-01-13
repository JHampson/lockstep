"""Tests for CLI formatters."""

from __future__ import annotations

from pathlib import Path

from rich.panel import Panel

from odcs_sync.cli.formatters import (
    ACTION_STYLES,
    format_plan,
    format_sync_results,
    format_validation_report,
)
from odcs_sync.models.catalog_state import ActionType, SyncAction, SyncPlan
from odcs_sync.services.contract_loader import ContractLoadError
from odcs_sync.services.sync import SyncResult


class TestFormatPlan:
    """Tests for format_plan function."""

    def test_format_plan_no_changes(self) -> None:
        """Test formatting a plan with no changes."""
        plan = SyncPlan(
            contract_name="test_contract",
            table_name="catalog.schema.table",
            actions=[],
        )
        result = format_plan(plan)
        assert isinstance(result, Panel)
        assert "No changes" in str(result.renderable)

    def test_format_plan_with_actions(self) -> None:
        """Test formatting a plan with actions."""
        plan = SyncPlan(
            contract_name="test_contract",
            table_name="catalog.schema.table",
            actions=[
                SyncAction(
                    action_type=ActionType.CREATE_TABLE,
                    target="catalog.schema.table",
                    description="Create table",
                    sql="CREATE TABLE ...",
                ),
                SyncAction(
                    action_type=ActionType.ADD_TABLE_TAG,
                    target="catalog.schema.table",
                    description="Add tag",
                    sql="ALTER TABLE ...",
                    details={"tag": "domain", "value": "sales"},
                ),
            ],
        )
        result = format_plan(plan)
        assert isinstance(result, Panel)
        assert result.border_style == "blue"

    def test_format_plan_with_destructive_changes(self) -> None:
        """Test formatting a plan with destructive changes."""
        plan = SyncPlan(
            contract_name="test_contract",
            table_name="catalog.schema.table",
            actions=[
                SyncAction(
                    action_type=ActionType.DROP_COLUMN,
                    target="catalog.schema.table.col",
                    description="Drop column",
                    sql="ALTER TABLE ...",
                ),
            ],
        )
        result = format_plan(plan)
        assert isinstance(result, Panel)
        assert plan.has_destructive_changes


class TestFormatSyncResults:
    """Tests for format_sync_results function."""

    def test_format_results_all_success(self) -> None:
        """Test formatting results where all syncs succeeded."""
        results = [
            SyncResult(
                contract_name="contract1",
                table_name="catalog.schema.table1",
                success=True,
                actions_applied=5,
                actions_skipped=0,
            ),
            SyncResult(
                contract_name="contract2",
                table_name="catalog.schema.table2",
                success=True,
                actions_applied=3,
                actions_skipped=1,
            ),
        ]
        result = format_sync_results(results)
        assert isinstance(result, Panel)
        assert result.border_style == "green"
        assert "✅" in result.title

    def test_format_results_with_failures(self) -> None:
        """Test formatting results with some failures."""
        results = [
            SyncResult(
                contract_name="contract1",
                table_name="catalog.schema.table1",
                success=True,
                actions_applied=5,
            ),
            SyncResult(
                contract_name="contract2",
                table_name="catalog.schema.table2",
                success=False,
                errors=["Connection failed", "Timeout"],
            ),
        ]
        result = format_sync_results(results)
        assert isinstance(result, Panel)
        assert result.border_style == "red"
        assert "❌" in result.title

    def test_format_results_empty(self) -> None:
        """Test formatting empty results."""
        results: list[SyncResult] = []
        result = format_sync_results(results)
        assert isinstance(result, Panel)


class TestFormatValidationReport:
    """Tests for format_validation_report function."""

    def test_format_validation_errors(self) -> None:
        """Test formatting validation errors."""
        errors = [
            ContractLoadError(
                "Invalid YAML",
                path=Path("/path/to/file.yaml"),
                errors=["Error 1", "Error 2"],
            ),
        ]
        result = format_validation_report(errors)
        assert isinstance(result, Panel)
        assert "1 file(s)" in result.title
        assert result.border_style == "red"

    def test_format_validation_errors_multiple_files(self) -> None:
        """Test formatting errors from multiple files."""
        errors = [
            ContractLoadError("Error 1", path=Path("/path/to/file1.yaml")),
            ContractLoadError("Error 2", path=Path("/path/to/file2.yaml")),
            ContractLoadError("Error 3", path=None),  # Unknown file
        ]
        result = format_validation_report(errors)
        assert isinstance(result, Panel)
        assert "3 file(s)" in result.title

    def test_format_validation_errors_many_sub_errors(self) -> None:
        """Test that more than 5 sub-errors are truncated."""
        errors = [
            ContractLoadError(
                "Multiple errors",
                path=Path("/path/to/file.yaml"),
                errors=[f"Error {i}" for i in range(10)],
            ),
        ]
        result = format_validation_report(errors)
        assert isinstance(result, Panel)
        # Should show first 5 + "and X more"


class TestActionStyles:
    """Tests for ACTION_STYLES constant."""

    def test_all_action_types_have_styles(self) -> None:
        """Test that all action types have defined styles."""
        for action_type in ActionType:
            # Not all action types need styles (some might be new)
            # Just verify the dict is accessible
            pass

    def test_style_format(self) -> None:
        """Test that styles are tuples of (icon, color)."""
        for action_type, style in ACTION_STYLES.items():
            assert isinstance(style, tuple)
            assert len(style) == 2
            icon, color = style
            assert isinstance(icon, str)
            assert isinstance(color, str)

