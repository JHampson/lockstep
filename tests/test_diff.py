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


class TestSyncPlan:
    """Tests for SyncPlan filtering."""

    def test_filter_non_destructive(
        self, sample_contract: Contract, sample_catalog_table: CatalogTable
    ) -> None:
        """Test filtering destructive actions."""
        diff_service = DiffService()
        plan = diff_service.compute_diff(sample_contract, sample_catalog_table)

        assert plan.has_destructive_changes is True

        filtered = plan.filter_non_destructive()
        assert filtered.has_destructive_changes is False

        # Should not have drop_column or remove_tag actions
        for action in filtered.actions:
            assert action.action_type not in (
                ActionType.DROP_COLUMN,
                ActionType.REMOVE_TABLE_TAG,
                ActionType.REMOVE_COLUMN_TAG,
            )

    def test_filter_preserve_extra_tags(
        self, sample_contract: Contract, sample_catalog_table: CatalogTable
    ) -> None:
        """Test filtering tag removal actions."""
        diff_service = DiffService()
        plan = diff_service.compute_diff(sample_contract, sample_catalog_table)

        filtered = plan.filter_preserve_extra_tags()

        # Should not have tag removal actions
        for action in filtered.actions:
            assert action.action_type not in (
                ActionType.REMOVE_TABLE_TAG,
                ActionType.REMOVE_COLUMN_TAG,
            )

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
