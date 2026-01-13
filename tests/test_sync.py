"""Tests for sync service."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lockstep.models.catalog_state import ActionType, CatalogTable, SyncAction, SyncPlan
from lockstep.models.contract import Column, Contract, ContractSchema, TableInfo
from lockstep.services.sync import SyncOptions, SyncResult, SyncService


@pytest.fixture
def mock_connector() -> MagicMock:
    """Create a mock Databricks connector."""
    return MagicMock()


@pytest.fixture
def sync_service(mock_connector: MagicMock) -> SyncService:
    """Create a sync service with a mock connector."""
    return SyncService(mock_connector)


@pytest.fixture
def sample_contract() -> Contract:
    """Create a sample contract for testing."""
    return Contract(
        name="test_contract",
        table_info=TableInfo(catalog="catalog", schema="schema", table="table"),
        schema_def=ContractSchema(
            columns=[
                Column(name="id", logicalType="string", required=True, primaryKey=True),
                Column(name="name", logicalType="string", required=False),
            ]
        ),
        tags={"domain": "test"},
    )


class TestSyncOptions:
    """Tests for SyncOptions."""

    def test_default_options(self) -> None:
        """Test default sync options."""
        options = SyncOptions()
        assert options.dry_run is False
        assert options.allow_destructive is False
        assert options.preserve_extra_tags is False
        assert options.catalog_override is None
        assert options.schema_override is None
        assert options.table_prefix is None

    def test_custom_options(self) -> None:
        """Test custom sync options."""
        options = SyncOptions(
            dry_run=True,
            allow_destructive=True,
            preserve_extra_tags=True,
            catalog_override="dev_catalog",
            schema_override="dev_schema",
            table_prefix="test_",
        )
        assert options.dry_run is True
        assert options.catalog_override == "dev_catalog"
        assert options.table_prefix == "test_"


class TestSyncResult:
    """Tests for SyncResult."""

    def test_default_result(self) -> None:
        """Test default sync result."""
        result = SyncResult(
            contract_name="test",
            table_name="catalog.schema.table",
        )
        assert result.success is True
        assert result.actions_applied == 0
        assert result.actions_skipped == 0
        assert result.errors == []
        assert result.plan is None

    def test_failed_result(self) -> None:
        """Test failed sync result."""
        result = SyncResult(
            contract_name="test",
            table_name="catalog.schema.table",
            success=False,
            errors=["Connection failed"],
        )
        assert result.success is False
        assert "Connection failed" in result.errors


class TestSyncService:
    """Tests for SyncService."""

    def test_sync_contract_new_table(
        self,
        sync_service: SyncService,
        mock_connector: MagicMock,
        sample_contract: Contract,
    ) -> None:
        """Test syncing a contract when table doesn't exist."""
        # Mock introspection to return None (table doesn't exist)
        with (
            patch.object(sync_service.introspection, "get_table", return_value=None),
            patch.object(
                sync_service.diff_service,
                "compute_diff",
                return_value=SyncPlan(
                    contract_name="test_contract",
                    table_name="catalog.schema.table",
                    actions=[
                        SyncAction(
                            action_type=ActionType.CREATE_TABLE,
                            target="catalog.schema.table",
                            description="Create table",
                            sql="CREATE TABLE catalog.schema.table ...",
                        )
                    ],
                ),
            ),
        ):
            result = sync_service.sync_contract(sample_contract)

            assert result.success is True
            assert result.actions_applied == 1
            mock_connector.execute.assert_called_once()

    def test_sync_contract_dry_run(
        self,
        sync_service: SyncService,
        mock_connector: MagicMock,
        sample_contract: Contract,
    ) -> None:
        """Test dry run doesn't execute SQL."""
        with (
            patch.object(sync_service.introspection, "get_table", return_value=None),
            patch.object(
                sync_service.diff_service,
                "compute_diff",
                return_value=SyncPlan(
                    contract_name="test_contract",
                    table_name="catalog.schema.table",
                    actions=[
                        SyncAction(
                            action_type=ActionType.CREATE_TABLE,
                            target="catalog.schema.table",
                            description="Create table",
                            sql="CREATE TABLE ...",
                        )
                    ],
                ),
            ),
        ):
            options = SyncOptions(dry_run=True)
            result = sync_service.sync_contract(sample_contract, options)

            assert result.success is True
            assert result.actions_applied == 0
            assert result.actions_skipped == 1
            mock_connector.execute.assert_not_called()

    def test_sync_contract_no_changes(
        self,
        sync_service: SyncService,
        mock_connector: MagicMock,
        sample_contract: Contract,
    ) -> None:
        """Test syncing when no changes are needed."""
        with (
            patch.object(
                sync_service.introspection,
                "get_table",
                return_value=CatalogTable(
                    catalog="catalog",
                    schema_name="schema",
                    table_name="table",
                ),
            ),
            patch.object(
                sync_service.diff_service,
                "compute_diff",
                return_value=SyncPlan(
                    contract_name="test_contract",
                    table_name="catalog.schema.table",
                    actions=[],
                ),
            ),
        ):
            result = sync_service.sync_contract(sample_contract)

            assert result.success is True
            assert result.actions_applied == 0
            mock_connector.execute.assert_not_called()

    def test_sync_contract_filters_destructive(
        self,
        sync_service: SyncService,
        mock_connector: MagicMock,  # noqa: ARG002
        sample_contract: Contract,
    ) -> None:
        """Test that destructive actions are filtered by default."""
        plan_with_destructive = SyncPlan(
            contract_name="test_contract",
            table_name="catalog.schema.table",
            actions=[
                SyncAction(
                    action_type=ActionType.ADD_COLUMN,
                    target="catalog.schema.table.new_col",
                    description="Add column",
                    sql="ALTER TABLE ADD COLUMN ...",
                ),
                SyncAction(
                    action_type=ActionType.DROP_COLUMN,
                    target="catalog.schema.table.old_col",
                    description="Drop column",
                    sql="ALTER TABLE DROP COLUMN ...",
                ),
            ],
        )

        with (
            patch.object(sync_service.introspection, "get_table", return_value=None),
            patch.object(
                sync_service.diff_service,
                "compute_diff",
                return_value=plan_with_destructive,
            ),
        ):
            # Default options - destructive not allowed
            result = sync_service.sync_contract(sample_contract)

            # Only the non-destructive action should be applied
            assert result.actions_applied == 1

    def test_sync_contract_allows_destructive(
        self,
        sync_service: SyncService,
        mock_connector: MagicMock,  # noqa: ARG002
        sample_contract: Contract,
    ) -> None:
        """Test that destructive actions are allowed when enabled."""
        plan_with_destructive = SyncPlan(
            contract_name="test_contract",
            table_name="catalog.schema.table",
            actions=[
                SyncAction(
                    action_type=ActionType.DROP_COLUMN,
                    target="catalog.schema.table.old_col",
                    description="Drop column",
                    sql="ALTER TABLE DROP COLUMN ...",
                ),
            ],
        )

        with (
            patch.object(sync_service.introspection, "get_table", return_value=None),
            patch.object(
                sync_service.diff_service,
                "compute_diff",
                return_value=plan_with_destructive,
            ),
        ):
            options = SyncOptions(allow_destructive=True)
            result = sync_service.sync_contract(sample_contract, options)

            assert result.actions_applied == 1

    def test_sync_contract_handles_sql_error(
        self,
        sync_service: SyncService,
        mock_connector: MagicMock,
        sample_contract: Contract,
    ) -> None:
        """Test that SQL errors are captured but don't stop sync."""
        mock_connector.execute.side_effect = Exception("SQL Error")

        with (
            patch.object(sync_service.introspection, "get_table", return_value=None),
            patch.object(
                sync_service.diff_service,
                "compute_diff",
                return_value=SyncPlan(
                    contract_name="test_contract",
                    table_name="catalog.schema.table",
                    actions=[
                        SyncAction(
                            action_type=ActionType.CREATE_TABLE,
                            target="catalog.schema.table",
                            description="Create table",
                            sql="CREATE TABLE ...",
                        )
                    ],
                ),
            ),
        ):
            result = sync_service.sync_contract(sample_contract)

            assert result.success is False
            assert len(result.errors) > 0

    def test_sync_contracts_multiple(
        self,
        sync_service: SyncService,
        mock_connector: MagicMock,  # noqa: ARG002
    ) -> None:
        """Test syncing multiple contracts."""
        contracts = [
            Contract(
                name="contract1",
                table_info=TableInfo(catalog="cat", schema="sch", table="table1"),
                schema_def=ContractSchema(columns=[Column(name="id", logicalType="string")]),
            ),
            Contract(
                name="contract2",
                table_info=TableInfo(catalog="cat", schema="sch", table="table2"),
                schema_def=ContractSchema(columns=[Column(name="id", logicalType="string")]),
            ),
        ]

        with (
            patch.object(sync_service.introspection, "get_table", return_value=None),
            patch.object(
                sync_service.diff_service,
                "compute_diff",
                return_value=SyncPlan(
                    contract_name="test",
                    table_name="cat.sch.table",
                    actions=[],
                ),
            ),
        ):
            results = sync_service.sync_contracts(contracts)

            assert len(results) == 2
            assert all(r.success for r in results)

    def test_sync_contract_with_overrides(
        self,
        sync_service: SyncService,
        mock_connector: MagicMock,  # noqa: ARG002
        sample_contract: Contract,
    ) -> None:
        """Test syncing with catalog/schema overrides."""
        with (
            patch.object(sync_service.introspection, "get_table", return_value=None),
            patch.object(
                sync_service.diff_service,
                "compute_diff",
                return_value=SyncPlan(
                    contract_name="test_contract",
                    table_name="override_cat.override_sch.table",
                    actions=[],
                ),
            ) as mock_diff,
        ):
            options = SyncOptions(
                catalog_override="override_cat",
                schema_override="override_sch",
            )
            sync_service.sync_contract(sample_contract, options)

            # Verify overrides were passed to diff
            mock_diff.assert_called_once()
            call_kwargs = mock_diff.call_args.kwargs
            assert call_kwargs["catalog_override"] == "override_cat"
            assert call_kwargs["schema_override"] == "override_sch"
