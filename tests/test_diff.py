"""Tests for diff service."""

from __future__ import annotations

import pytest

from lockstep.models.catalog_state import (
    ActionType,
    CatalogColumn,
    CatalogConstraint,
    CatalogTable,
)
from lockstep.models.contract import Contract
from lockstep.services.diff import DiffService


class TestDiffService:
    """Tests for DiffService."""

    @pytest.fixture
    def diff_service(self) -> DiffService:
        """Create a diff service instance."""
        return DiffService()

    def test_diff_new_table(self, diff_service: DiffService, sample_contract: Contract) -> None:
        """Test diff when table doesn't exist."""
        plan = diff_service.compute_diff(sample_contract, None)

        assert plan.has_changes is True
        assert plan.contract_name == "customer_contract"
        assert plan.table_name == "main.sales.customers"

        # Should have create_table action
        action_types = [a.action_type for a in plan.actions]
        assert ActionType.CREATE_TABLE in action_types

        # Should have tag actions for new table
        assert ActionType.ADD_TABLE_TAG in action_types
        assert ActionType.ADD_COLUMN_TAG in action_types

    def test_diff_no_changes(self, diff_service: DiffService, sample_contract: Contract) -> None:
        """Test diff when catalog matches contract exactly."""
        # Create catalog state that matches contract
        catalog_state = CatalogTable(
            catalog="main",
            schema_name="sales",
            table_name="customers",
            columns=[
                CatalogColumn(
                    name="customer_id",
                    data_type="STRING",
                    nullable=False,
                    description="Unique customer identifier",
                    tags={"pii": "false"},
                ),
                CatalogColumn(
                    name="email",
                    data_type="STRING",
                    nullable=False,
                    description="Customer email address",
                    tags={"pii": "true", "classification": "sensitive"},
                ),
                CatalogColumn(
                    name="created_at",
                    data_type="TIMESTAMP",
                    nullable=True,
                    description="Account creation timestamp",
                ),
                CatalogColumn(
                    name="total_orders",
                    data_type="INT",
                    nullable=True,
                    description="Total number of orders",
                ),
            ],
            description="Customer data contract",
            tags={
                "domain": "sales",
                "team": "customer-success",
                "system.certification_status": "certified",
            },
            constraints=[
                CatalogConstraint(
                    name="pk_customers",
                    constraint_type="PRIMARY_KEY",
                    columns=["customer_id"],
                )
            ],
        )

        plan = diff_service.compute_diff(sample_contract, catalog_state)
        assert plan.has_changes is False

    def test_diff_add_column(self, diff_service: DiffService, sample_contract: Contract) -> None:
        """Test diff detects missing columns."""
        # Catalog state missing some columns
        catalog_state = CatalogTable(
            catalog="main",
            schema_name="sales",
            table_name="customers",
            columns=[
                CatalogColumn(name="customer_id", data_type="STRING", nullable=False),
                CatalogColumn(name="email", data_type="STRING", nullable=False),
            ],
            description="Customer data contract",
            tags={"system.certification_status": "certified"},
            constraints=[
                CatalogConstraint(
                    name="pk",
                    constraint_type="PRIMARY_KEY",
                    columns=["customer_id"],
                )
            ],
        )

        plan = diff_service.compute_diff(sample_contract, catalog_state)
        add_column_actions = [a for a in plan.actions if a.action_type == ActionType.ADD_COLUMN]

        # Should add created_at and total_orders
        assert len(add_column_actions) == 2
        added_columns = {a.target.split(".")[-1] for a in add_column_actions}
        assert "created_at" in added_columns
        assert "total_orders" in added_columns

    def test_diff_drop_column(
        self,
        diff_service: DiffService,
        sample_contract: Contract,
        sample_catalog_table: CatalogTable,
    ) -> None:
        """Test diff detects extra columns to drop."""
        plan = diff_service.compute_diff(sample_contract, sample_catalog_table)

        drop_column_actions = [a for a in plan.actions if a.action_type == ActionType.DROP_COLUMN]

        # Should drop old_column
        assert len(drop_column_actions) == 1
        assert "old_column" in drop_column_actions[0].target

    def test_diff_update_description(
        self,
        diff_service: DiffService,
        sample_contract: Contract,
        sample_catalog_table: CatalogTable,
    ) -> None:
        """Test diff detects description changes."""
        plan = diff_service.compute_diff(sample_contract, sample_catalog_table)

        desc_actions = [
            a
            for a in plan.actions
            if a.action_type
            in (
                ActionType.UPDATE_TABLE_DESCRIPTION,
                ActionType.UPDATE_COLUMN_DESCRIPTION,
            )
        ]

        # Should update table description and email column description
        assert len(desc_actions) >= 2

    def test_diff_tags(
        self,
        diff_service: DiffService,
        sample_contract: Contract,
        sample_catalog_table: CatalogTable,
    ) -> None:
        """Test diff detects tag changes."""
        plan = diff_service.compute_diff(sample_contract, sample_catalog_table)

        # Should add missing team tag
        add_tag_actions = [a for a in plan.actions if a.action_type == ActionType.ADD_TABLE_TAG]
        assert any(a.details.get("tag") == "team" for a in add_tag_actions)

        # Should remove extra legacy tag
        remove_tag_actions = [
            a for a in plan.actions if a.action_type == ActionType.REMOVE_TABLE_TAG
        ]
        assert any(a.details.get("tag") == "legacy" for a in remove_tag_actions)

    def test_diff_column_tags(
        self,
        diff_service: DiffService,
        sample_contract: Contract,
        sample_catalog_table: CatalogTable,
    ) -> None:
        """Test diff detects column tag changes."""
        plan = diff_service.compute_diff(sample_contract, sample_catalog_table)

        # Should add classification tag to email column
        add_col_tag_actions = [
            a for a in plan.actions if a.action_type == ActionType.ADD_COLUMN_TAG
        ]
        assert any(
            a.details.get("column") == "email" and a.details.get("tag") == "classification"
            for a in add_col_tag_actions
        )

    def test_diff_certification_via_tag(self, diff_service: DiffService) -> None:
        """Test diff detects certification changes via system tag."""
        from lockstep.models.contract import ContractSchema, TableInfo

        # Contract wants certified (via tag)
        contract = Contract(
            name="test",
            dataset=TableInfo(catalog="main", schema="default", table="test"),
            schema_def=ContractSchema(columns=[]),
            tags={"system.certification_status": "certified"},
        )

        # Catalog is not certified (no tag)
        catalog_state = CatalogTable(
            catalog="main",
            schema_name="default",
            table_name="test",
            tags={},
        )

        plan = diff_service.compute_diff(contract, catalog_state)
        tag_actions = [a for a in plan.actions if a.action_type == ActionType.ADD_TABLE_TAG]
        cert_actions = [
            a for a in tag_actions if a.details.get("tag") == "system.certification_status"
        ]
        assert len(cert_actions) == 1
        assert cert_actions[0].details.get("value") == "certified"

    def test_diff_clear_certification_via_tag(self, diff_service: DiffService) -> None:
        """Test diff detects certification removal via tag."""
        from lockstep.models.contract import ContractSchema, TableInfo

        # Contract wants no certification (no tag)
        contract = Contract(
            name="test",
            dataset=TableInfo(catalog="main", schema="default", table="test"),
            schema_def=ContractSchema(columns=[]),
            tags={},
        )

        # Catalog is certified (has tag)
        catalog_state = CatalogTable(
            catalog="main",
            schema_name="default",
            table_name="test",
            tags={"system.certification_status": "certified"},
        )

        plan = diff_service.compute_diff(contract, catalog_state)
        remove_actions = [a for a in plan.actions if a.action_type == ActionType.REMOVE_TABLE_TAG]
        cert_actions = [
            a for a in remove_actions if a.details.get("tag") == "system.certification_status"
        ]
        assert len(cert_actions) == 1

    def test_diff_with_overrides(
        self, diff_service: DiffService, sample_contract: Contract
    ) -> None:
        """Test diff with catalog/schema overrides."""
        plan = diff_service.compute_diff(
            sample_contract,
            None,
            catalog_override="dev",
            schema_override="staging",
            table_prefix="test_",
        )

        assert plan.table_name == "dev.staging.test_customers"

    def test_diff_type_mismatch(self, diff_service: DiffService) -> None:
        """Test diff detects column type mismatches."""
        # Contract defines customer_id as STRING, amount as DOUBLE
        contract = Contract.model_validate(
            {
                "name": "test",
                "dataset": {"catalog": "main", "schema": "default", "table": "test"},
                "schema": {
                    "properties": [
                        {"name": "customer_id", "logicalType": "string"},
                        {"name": "amount", "logicalType": "double"},
                    ]
                },
            }
        )

        # Catalog has customer_id as INT and amount as STRING (mismatches)
        catalog_state = CatalogTable(
            catalog="main",
            schema_name="default",
            table_name="test",
            columns=[
                CatalogColumn(name="customer_id", data_type="INT"),
                CatalogColumn(name="amount", data_type="STRING"),
            ],
        )

        plan = diff_service.compute_diff(contract, catalog_state)

        # Should have type mismatch warnings
        type_mismatch_actions = [
            a for a in plan.actions if a.action_type == ActionType.TYPE_MISMATCH
        ]
        assert len(type_mismatch_actions) == 2

        # Check details are captured
        customer_mismatch = next(
            a for a in type_mismatch_actions if a.details.get("column") == "customer_id"
        )
        assert customer_mismatch.details["contract_type"] == "STRING"
        assert customer_mismatch.details["catalog_type"] == "INT"
        assert customer_mismatch.sql is None  # No SQL for warnings

        # Plan should report warnings
        assert plan.has_warnings is True

    def test_diff_type_aliases_match(self, diff_service: DiffService) -> None:
        """Test that non-parameterized type aliases are treated as equivalent."""
        # Contract uses INT, BIGINT, STRING via logical types
        contract = Contract.model_validate(
            {
                "name": "test",
                "dataset": {"catalog": "main", "schema": "default", "table": "test"},
                "schema": {
                    "properties": [
                        {"name": "count", "logicalType": "integer"},  # Maps to INT
                        {"name": "big_count", "logicalType": "long"},  # Maps to BIGINT
                        {"name": "name", "logicalType": "string"},  # Maps to STRING
                    ]
                },
            }
        )

        # Catalog uses non-parameterized aliases (no length/precision specified)
        catalog_state = CatalogTable(
            catalog="main",
            schema_name="default",
            table_name="test",
            columns=[
                CatalogColumn(name="count", data_type="INTEGER"),  # Alias for INT
                CatalogColumn(name="big_count", data_type="BIGINT"),  # Alias for LONG
                CatalogColumn(name="name", data_type="VARCHAR"),  # Alias for STRING (no length)
            ],
        )

        plan = diff_service.compute_diff(contract, catalog_state)

        # Should NOT have type mismatch warnings (non-parameterized aliases are equivalent)
        type_mismatch_actions = [
            a for a in plan.actions if a.action_type == ActionType.TYPE_MISMATCH
        ]
        assert len(type_mismatch_actions) == 0
        assert plan.has_warnings is False

    def test_diff_type_warns_on_parameterized(self, diff_service: DiffService) -> None:
        """Test that parameterized types trigger warnings even when base types match."""
        # Contract uses STRING
        contract = Contract.model_validate(
            {
                "name": "test",
                "dataset": {"catalog": "main", "schema": "default", "table": "test"},
                "schema": {
                    "properties": [
                        {"name": "name", "logicalType": "string"},  # Maps to STRING
                    ]
                },
            }
        )

        # Catalog uses VARCHAR(100) - parameterized version
        catalog_state = CatalogTable(
            catalog="main",
            schema_name="default",
            table_name="test",
            columns=[
                CatalogColumn(name="name", data_type="VARCHAR(100)"),  # Has length constraint
            ],
        )

        plan = diff_service.compute_diff(contract, catalog_state)

        # SHOULD have type mismatch warning (parameterized type differs)
        type_mismatch_actions = [
            a for a in plan.actions if a.action_type == ActionType.TYPE_MISMATCH
        ]
        assert len(type_mismatch_actions) == 1
        assert plan.has_warnings is True
        assert type_mismatch_actions[0].details["contract_type"] == "STRING"
        assert type_mismatch_actions[0].details["catalog_type"] == "VARCHAR(100)"

    def test_types_match_direct(self, diff_service: DiffService) -> None:
        """Test _types_match for direct type equality."""
        assert diff_service._types_match("STRING", "STRING") is True
        assert diff_service._types_match("INT", "INT") is True
        assert diff_service._types_match("TIMESTAMP", "TIMESTAMP") is True

    def test_types_match_aliases(self, diff_service: DiffService) -> None:
        """Test _types_match for type aliases."""
        # String aliases
        assert diff_service._types_match("STRING", "VARCHAR") is True
        assert diff_service._types_match("VARCHAR", "STRING") is True
        assert diff_service._types_match("TEXT", "STRING") is True

        # Integer aliases
        assert diff_service._types_match("INT", "INTEGER") is True
        assert diff_service._types_match("INTEGER", "INT") is True

        # Long aliases
        assert diff_service._types_match("LONG", "BIGINT") is True
        assert diff_service._types_match("BIGINT", "LONG") is True

        # Boolean aliases
        assert diff_service._types_match("BOOLEAN", "BOOL") is True
        assert diff_service._types_match("BOOL", "BOOLEAN") is True

    def test_types_match_mismatches(self, diff_service: DiffService) -> None:
        """Test _types_match for actual mismatches."""
        assert diff_service._types_match("STRING", "INT") is False
        assert diff_service._types_match("DOUBLE", "STRING") is False
        assert diff_service._types_match("TIMESTAMP", "DATE") is False
        assert diff_service._types_match("INT", "BIGINT") is False  # Different sizes

    def test_types_match_parameterized(self, diff_service: DiffService) -> None:
        """Test _types_match warns when parameterized types differ."""
        # Exact match with same parameters - no warning
        assert diff_service._types_match("VARCHAR(100)", "VARCHAR(100)") is True
        assert diff_service._types_match("DECIMAL(10,2)", "DECIMAL(10,2)") is True

        # Different parameters - WARN (even if base type is same/alias)
        assert diff_service._types_match("VARCHAR(100)", "VARCHAR(200)") is False
        assert diff_service._types_match("DECIMAL(10,2)", "DECIMAL(18,4)") is False

        # Parameterized vs non-parameterized - WARN
        assert diff_service._types_match("VARCHAR", "VARCHAR(100)") is False
        assert diff_service._types_match("STRING", "VARCHAR(100)") is False
        assert diff_service._types_match("DECIMAL", "DECIMAL(10,2)") is False

        # Non-parameterized aliases still match
        assert diff_service._types_match("STRING", "VARCHAR") is True
        assert diff_service._types_match("INT", "INTEGER") is True

        # Different base types - always warn
        assert diff_service._types_match("VARCHAR(100)", "INT") is False
        assert diff_service._types_match("STRING", "INT(11)") is False


class TestSyncPlan:
    """Tests for SyncPlan filtering."""

    def test_get_summary(
        self, sample_contract: Contract, sample_catalog_table: CatalogTable
    ) -> None:
        """Test plan summary generation."""
        diff_service = DiffService()
        plan = diff_service.compute_diff(sample_contract, sample_catalog_table)

        summary = plan.get_summary()
        assert isinstance(summary, dict)
        # Should have counts for various action types
        assert sum(summary.values()) == len(plan.actions)

    def test_filter_no_add_tags(self) -> None:
        """Test filtering out tag add/update actions."""
        from lockstep.models.catalog_state import SyncAction, SyncPlan

        plan = SyncPlan(
            contract_name="test",
            table_name="cat.sch.tbl",
            actions=[
                SyncAction(ActionType.ADD_TABLE_TAG, "cat.sch.tbl", "Add tag"),
                SyncAction(ActionType.UPDATE_TABLE_TAG, "cat.sch.tbl", "Update tag"),
                SyncAction(ActionType.ADD_COLUMN_TAG, "cat.sch.tbl.col", "Add col tag"),
                SyncAction(ActionType.UPDATE_COLUMN_DESCRIPTION, "cat.sch.tbl.col", "Update desc"),
                SyncAction(ActionType.ADD_COLUMN, "cat.sch.tbl.col", "Add column"),
            ],
        )

        filtered = plan.filter_no_add_tags()
        assert len(filtered.actions) == 2
        for action in filtered.actions:
            assert action.action_type not in (
                ActionType.ADD_TABLE_TAG,
                ActionType.UPDATE_TABLE_TAG,
                ActionType.ADD_COLUMN_TAG,
                ActionType.UPDATE_COLUMN_TAG,
            )

    def test_filter_no_add_columns(self) -> None:
        """Test filtering out add column actions."""
        from lockstep.models.catalog_state import SyncAction, SyncPlan

        plan = SyncPlan(
            contract_name="test",
            table_name="cat.sch.tbl",
            actions=[
                SyncAction(ActionType.ADD_COLUMN, "cat.sch.tbl.col1", "Add column 1"),
                SyncAction(ActionType.ADD_COLUMN, "cat.sch.tbl.col2", "Add column 2"),
                SyncAction(ActionType.UPDATE_COLUMN_DESCRIPTION, "cat.sch.tbl.col", "Update desc"),
                SyncAction(ActionType.ADD_TABLE_TAG, "cat.sch.tbl", "Add tag"),
            ],
        )

        filtered = plan.filter_no_add_columns()
        assert len(filtered.actions) == 2
        for action in filtered.actions:
            assert action.action_type != ActionType.ADD_COLUMN

    def test_filter_no_add_descriptions(self) -> None:
        """Test filtering out description update actions."""
        from lockstep.models.catalog_state import SyncAction, SyncPlan

        plan = SyncPlan(
            contract_name="test",
            table_name="cat.sch.tbl",
            actions=[
                SyncAction(ActionType.UPDATE_TABLE_DESCRIPTION, "cat.sch.tbl", "Update table desc"),
                SyncAction(
                    ActionType.UPDATE_COLUMN_DESCRIPTION, "cat.sch.tbl.col", "Update col desc"
                ),
                SyncAction(ActionType.ADD_COLUMN, "cat.sch.tbl.col", "Add column"),
                SyncAction(ActionType.ADD_TABLE_TAG, "cat.sch.tbl", "Add tag"),
            ],
        )

        filtered = plan.filter_no_add_descriptions()
        assert len(filtered.actions) == 2
        for action in filtered.actions:
            assert action.action_type not in (
                ActionType.UPDATE_TABLE_DESCRIPTION,
                ActionType.UPDATE_COLUMN_DESCRIPTION,
            )

    def test_filter_no_add_constraints(self) -> None:
        """Test filtering out constraint add actions."""
        from lockstep.models.catalog_state import SyncAction, SyncPlan

        plan = SyncPlan(
            contract_name="test",
            table_name="cat.sch.tbl",
            actions=[
                SyncAction(ActionType.ADD_PRIMARY_KEY, "cat.sch.tbl", "Add PK"),
                SyncAction(ActionType.ADD_NOT_NULL, "cat.sch.tbl.col", "Add NOT NULL"),
                SyncAction(ActionType.ADD_COLUMN, "cat.sch.tbl.col", "Add column"),
                SyncAction(ActionType.ADD_TABLE_TAG, "cat.sch.tbl", "Add tag"),
            ],
        )

        filtered = plan.filter_no_add_constraints()
        assert len(filtered.actions) == 2
        for action in filtered.actions:
            assert action.action_type not in (
                ActionType.ADD_PRIMARY_KEY,
                ActionType.ADD_NOT_NULL,
            )

    def test_filter_no_remove_columns(self) -> None:
        """Test filtering out column drop actions."""
        from lockstep.models.catalog_state import SyncAction, SyncPlan

        plan = SyncPlan(
            contract_name="test",
            table_name="cat.sch.tbl",
            actions=[
                SyncAction(ActionType.DROP_COLUMN, "cat.sch.tbl.col1", "Drop column"),
                SyncAction(ActionType.ADD_COLUMN, "cat.sch.tbl.col2", "Add column"),
                SyncAction(ActionType.ADD_TABLE_TAG, "cat.sch.tbl", "Add tag"),
            ],
        )

        filtered = plan.filter_no_remove_columns()
        assert len(filtered.actions) == 2
        for action in filtered.actions:
            assert action.action_type != ActionType.DROP_COLUMN

    def test_filter_no_remove_tags(self) -> None:
        """Test filtering out tag removal actions."""
        from lockstep.models.catalog_state import SyncAction, SyncPlan

        plan = SyncPlan(
            contract_name="test",
            table_name="cat.sch.tbl",
            actions=[
                SyncAction(ActionType.REMOVE_TABLE_TAG, "cat.sch.tbl", "Remove tag"),
                SyncAction(ActionType.REMOVE_COLUMN_TAG, "cat.sch.tbl.col", "Remove col tag"),
                SyncAction(ActionType.ADD_TABLE_TAG, "cat.sch.tbl", "Add tag"),
                SyncAction(ActionType.ADD_COLUMN, "cat.sch.tbl.col", "Add column"),
            ],
        )

        filtered = plan.filter_no_remove_tags()
        assert len(filtered.actions) == 2
        for action in filtered.actions:
            assert action.action_type not in (
                ActionType.REMOVE_TABLE_TAG,
                ActionType.REMOVE_COLUMN_TAG,
            )

    def test_filter_no_remove_constraints(self) -> None:
        """Test filtering out constraint drop actions."""
        from lockstep.models.catalog_state import SyncAction, SyncPlan

        plan = SyncPlan(
            contract_name="test",
            table_name="cat.sch.tbl",
            actions=[
                SyncAction(ActionType.DROP_PRIMARY_KEY, "cat.sch.tbl", "Drop PK"),
                SyncAction(ActionType.DROP_NOT_NULL, "cat.sch.tbl.col", "Drop NOT NULL"),
                SyncAction(ActionType.ADD_COLUMN, "cat.sch.tbl.col", "Add column"),
                SyncAction(ActionType.ADD_TABLE_TAG, "cat.sch.tbl", "Add tag"),
            ],
        )

        filtered = plan.filter_no_remove_constraints()
        assert len(filtered.actions) == 2
        for action in filtered.actions:
            assert action.action_type not in (
                ActionType.DROP_PRIMARY_KEY,
                ActionType.DROP_NOT_NULL,
            )
