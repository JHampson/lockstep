"""Diff service for comparing contracts with Unity Catalog state.

Produces a structured plan of actions to synchronize a contract.
"""

from __future__ import annotations

import logging

from lockstep.models.catalog_state import (
    ActionType,
    CatalogGrant,
    CatalogTable,
    SyncAction,
    SyncPlan,
)
from lockstep.models.contract import Contract
from lockstep.services.sql_generator import SQLGenerator

logger = logging.getLogger(__name__)


class DiffService:
    """Service for computing differences between contracts and catalog state."""

    def __init__(self) -> None:
        """Initialize the diff service."""
        self.sql_gen = SQLGenerator()

    def compute_diff(
        self,
        contract: Contract,
        current_state: CatalogTable | None,
        catalog_override: str | None = None,
        schema_override: str | None = None,
        table_prefix: str | None = None,
    ) -> SyncPlan:
        """Compute the diff between a contract and current catalog state.

        Args:
            contract: The desired contract state.
            current_state: Current table state in Unity Catalog (None if doesn't exist).
            catalog_override: Override catalog name.
            schema_override: Override schema name.
            table_prefix: Prefix for table name.

        Returns:
            SyncPlan with actions to apply.
        """
        full_table_name = contract.get_full_table_name(
            catalog_override=catalog_override,
            schema_override=schema_override,
            table_prefix=table_prefix,
        )

        plan = SyncPlan(contract_name=contract.name, table_name=full_table_name)

        if current_state is None:
            # Table doesn't exist - create it
            self._plan_create_table(plan, contract, full_table_name)
            # Also plan permissions for new table
            self._plan_permission_changes(plan, contract, None, full_table_name)
        else:
            # Table exists - compute incremental changes
            self._plan_column_changes(plan, contract, current_state, full_table_name)
            self._plan_description_changes(plan, contract, current_state, full_table_name)
            self._plan_constraint_changes(plan, contract, current_state, full_table_name)
            self._plan_tag_changes(plan, contract, current_state, full_table_name)
            self._plan_permission_changes(plan, contract, current_state, full_table_name)

        return plan

    def _plan_create_table(self, plan: SyncPlan, contract: Contract, full_table_name: str) -> None:
        """Plan actions to create a new table."""
        sql = self.sql_gen.create_table(
            full_table_name=full_table_name,
            columns=contract.columns,
            description=contract.description,
            primary_key_columns=contract.primary_key_columns,
        )

        plan.actions.append(
            SyncAction(
                action_type=ActionType.CREATE_TABLE,
                target=full_table_name,
                description=f"Create table {full_table_name} with {len(contract.columns)} columns",
                sql=sql,
            )
        )

        # Plan tags for the new table
        for tag_name, tag_value in contract.tags.items():
            tag_sql = self.sql_gen.set_table_tag(full_table_name, tag_name, tag_value)
            plan.actions.append(
                SyncAction(
                    action_type=ActionType.ADD_TABLE_TAG,
                    target=full_table_name,
                    description=f"Add table tag {tag_name}={tag_value}",
                    sql=tag_sql,
                    details={"tag": tag_name, "value": tag_value},
                )
            )

        # Plan column tags for the new table
        for col in contract.columns:
            for tag_name, tag_value in col.tags.items():
                tag_sql = self.sql_gen.set_column_tag(
                    full_table_name, col.name, tag_name, tag_value
                )
                plan.actions.append(
                    SyncAction(
                        action_type=ActionType.ADD_COLUMN_TAG,
                        target=f"{full_table_name}.{col.name}",
                        description=f"Add column tag {col.name}.{tag_name}={tag_value}",
                        sql=tag_sql,
                        details={"column": col.name, "tag": tag_name, "value": tag_value},
                    )
                )

    def _plan_column_changes(
        self,
        plan: SyncPlan,
        contract: Contract,
        current: CatalogTable,
        full_table_name: str,
    ) -> None:
        """Plan column additions, type mismatch warnings, and removals."""
        current_columns = {col.name.lower(): col for col in current.columns}
        contract_columns = {col.name.lower(): col for col in contract.columns}

        # Add missing columns or detect type mismatches
        for col in contract.columns:
            col_lower = col.name.lower()
            if col_lower not in current_columns:
                # Column doesn't exist - add it
                sql = self.sql_gen.add_column(
                    full_table_name=full_table_name,
                    column_name=col.name,
                    data_type=col.get_databricks_type(),
                    nullable=not col.required,
                    description=col.description,
                )
                plan.actions.append(
                    SyncAction(
                        action_type=ActionType.ADD_COLUMN,
                        target=f"{full_table_name}.{col.name}",
                        description=f"Add column {col.name} ({col.get_databricks_type()})",
                        sql=sql,
                    )
                )
            else:
                # Column exists - check for type mismatch
                current_col = current_columns[col_lower]
                contract_type = col.get_databricks_type().upper()
                current_type = current_col.data_type.upper()

                # Normalize common type aliases for comparison
                if not self._types_match(contract_type, current_type):
                    plan.actions.append(
                        SyncAction(
                            action_type=ActionType.TYPE_MISMATCH,
                            target=f"{full_table_name}.{col.name}",
                            description=f"Type mismatch for column {col.name}: contract={contract_type}, catalog={current_type}",
                            sql=None,  # No SQL - manual intervention required
                            details={
                                "column": col.name,
                                "contract_type": contract_type,
                                "catalog_type": current_type,
                            },
                        )
                    )

        # Drop columns not in contract (destructive)
        for col_name_lower, current_col in current_columns.items():
            if col_name_lower not in contract_columns:
                sql = self.sql_gen.drop_column(full_table_name, current_col.name)
                plan.actions.append(
                    SyncAction(
                        action_type=ActionType.DROP_COLUMN,
                        target=f"{full_table_name}.{current_col.name}",
                        description=f"Drop column {current_col.name}",
                        sql=sql,
                    )
                )

    def _types_match(self, contract_type: str, catalog_type: str) -> bool:
        """Check if two data types are equivalent, accounting for aliases.

        Args:
            contract_type: Type from the contract (normalized to uppercase).
            catalog_type: Type from the catalog (normalized to uppercase).

        Returns:
            True if the types are considered equivalent.
        """
        # Direct match (exact strings including any parameters)
        if contract_type == catalog_type:
            return True

        # Extract base types and check for parameters
        contract_base = contract_type.split("(")[0].strip()
        catalog_base = catalog_type.split("(")[0].strip()
        contract_has_params = "(" in contract_type
        catalog_has_params = "(" in catalog_type

        # If either type has parameters and they differ, warn (even if base types match)
        # e.g., VARCHAR(100) vs VARCHAR(200), or STRING vs VARCHAR(100)
        if contract_has_params or catalog_has_params:
            # Parameters present and full types don't match - warn
            return False

        # Neither has parameters - check if base types are in the same equivalence group
        equivalence_groups: list[set[str]] = [
            {"STRING", "VARCHAR", "TEXT"},
            {"INT", "INTEGER"},
            {"LONG", "BIGINT"},
            {"SHORT", "SMALLINT"},
            {"BYTE", "TINYINT"},
            {"FLOAT", "REAL"},
            {"DOUBLE", "DOUBLE PRECISION"},
            {"BOOLEAN", "BOOL"},
        ]

        # Check if both base types belong to the same equivalence group
        return any(contract_base in group and catalog_base in group for group in equivalence_groups)

    def _plan_description_changes(
        self,
        plan: SyncPlan,
        contract: Contract,
        current: CatalogTable,
        full_table_name: str,
    ) -> None:
        """Plan description updates for table and columns."""
        # Table description
        if contract.description and contract.description != current.description:
            sql = self.sql_gen.update_table_description(full_table_name, contract.description)
            plan.actions.append(
                SyncAction(
                    action_type=ActionType.UPDATE_TABLE_DESCRIPTION,
                    target=full_table_name,
                    description="Update table description",
                    sql=sql,
                )
            )

        # Column descriptions
        current_columns = {col.name.lower(): col for col in current.columns}

        for col in contract.columns:
            col_lower = col.name.lower()
            if col_lower in current_columns:
                current_col = current_columns[col_lower]
                if col.description and col.description != current_col.description:
                    sql = self.sql_gen.update_column_description(
                        full_table_name, col.name, col.description
                    )
                    plan.actions.append(
                        SyncAction(
                            action_type=ActionType.UPDATE_COLUMN_DESCRIPTION,
                            target=f"{full_table_name}.{col.name}",
                            description=f"Update description for column {col.name}",
                            sql=sql,
                        )
                    )

    def _plan_constraint_changes(
        self,
        plan: SyncPlan,
        contract: Contract,
        current: CatalogTable,
        full_table_name: str,
    ) -> None:
        """Plan constraint changes (primary key, not null)."""
        # Primary key
        contract_pk = set(contract.primary_key_columns)
        current_pk_constraint = current.get_primary_key_constraint()
        current_pk = set(current_pk_constraint.columns) if current_pk_constraint else set()

        if contract_pk != current_pk:
            if current_pk_constraint and current_pk:
                # Drop existing PK first
                sql = self.sql_gen.drop_primary_key(full_table_name, current_pk_constraint.name)
                plan.actions.append(
                    SyncAction(
                        action_type=ActionType.DROP_PRIMARY_KEY,
                        target=full_table_name,
                        description=f"Drop existing primary key constraint {current_pk_constraint.name}",
                        sql=sql,
                    )
                )

            if contract_pk:
                # Add new PK
                pk_cols = list(contract_pk)
                sql = self.sql_gen.add_primary_key(full_table_name, pk_cols)
                plan.actions.append(
                    SyncAction(
                        action_type=ActionType.ADD_PRIMARY_KEY,
                        target=full_table_name,
                        description=f"Add primary key on columns: {', '.join(pk_cols)}",
                        sql=sql,
                    )
                )

        # NOT NULL constraints
        current_columns = {col.name.lower(): col for col in current.columns}

        for col in contract.columns:
            col_lower = col.name.lower()
            if col_lower in current_columns:
                current_col = current_columns[col_lower]
                contract_not_null = col.required
                current_nullable = current_col.nullable

                if contract_not_null and current_nullable:
                    # Need to add NOT NULL
                    sql = self.sql_gen.add_not_null(full_table_name, col.name)
                    plan.actions.append(
                        SyncAction(
                            action_type=ActionType.ADD_NOT_NULL,
                            target=f"{full_table_name}.{col.name}",
                            description=f"Add NOT NULL constraint on column {col.name}",
                            sql=sql,
                        )
                    )
                elif not contract_not_null and not current_nullable:
                    # Need to drop NOT NULL (destructive)
                    sql = self.sql_gen.drop_not_null(full_table_name, col.name)
                    plan.actions.append(
                        SyncAction(
                            action_type=ActionType.DROP_NOT_NULL,
                            target=f"{full_table_name}.{col.name}",
                            description=f"Drop NOT NULL constraint on column {col.name}",
                            sql=sql,
                        )
                    )

    def _plan_tag_changes(
        self,
        plan: SyncPlan,
        contract: Contract,
        current: CatalogTable,
        full_table_name: str,
    ) -> None:
        """Plan tag additions, updates, and removals.

        Note: Unity Catalog certification is handled via the 'system.certification_status' tag.
        """
        # Table-level tags
        contract_tags = dict(contract.tags)
        current_tags = dict(current.tags)

        # Add or update tags
        for tag_name, tag_value in contract_tags.items():
            if tag_name not in current_tags:
                sql = self.sql_gen.set_table_tag(full_table_name, tag_name, tag_value)
                plan.actions.append(
                    SyncAction(
                        action_type=ActionType.ADD_TABLE_TAG,
                        target=full_table_name,
                        description=f"Add table tag {tag_name}={tag_value}",
                        sql=sql,
                        details={"tag": tag_name, "value": tag_value},
                    )
                )
            elif current_tags[tag_name] != tag_value:
                sql = self.sql_gen.set_table_tag(full_table_name, tag_name, tag_value)
                plan.actions.append(
                    SyncAction(
                        action_type=ActionType.UPDATE_TABLE_TAG,
                        target=full_table_name,
                        description=f"Update table tag {tag_name}: {current_tags[tag_name]} -> {tag_value}",
                        sql=sql,
                        details={
                            "tag": tag_name,
                            "old_value": current_tags[tag_name],
                            "new_value": tag_value,
                        },
                    )
                )

        # Remove tags not in contract
        for tag_name in current_tags:
            if tag_name not in contract_tags:
                sql = self.sql_gen.remove_table_tag(full_table_name, tag_name)
                plan.actions.append(
                    SyncAction(
                        action_type=ActionType.REMOVE_TABLE_TAG,
                        target=full_table_name,
                        description=f"Remove table tag {tag_name}",
                        sql=sql,
                        details={"tag": tag_name},
                    )
                )

        # Column-level tags
        current_columns = {col.name.lower(): col for col in current.columns}

        for col in contract.columns:
            col_lower = col.name.lower()
            if col_lower not in current_columns:
                continue  # New column - tags handled in create

            current_col = current_columns[col_lower]
            contract_col_tags = col.tags
            current_col_tags = current_col.tags

            # Add or update column tags
            for tag_name, tag_value in contract_col_tags.items():
                if tag_name not in current_col_tags:
                    sql = self.sql_gen.set_column_tag(
                        full_table_name, col.name, tag_name, tag_value
                    )
                    plan.actions.append(
                        SyncAction(
                            action_type=ActionType.ADD_COLUMN_TAG,
                            target=f"{full_table_name}.{col.name}",
                            description=f"Add column tag {col.name}.{tag_name}={tag_value}",
                            sql=sql,
                            details={"column": col.name, "tag": tag_name, "value": tag_value},
                        )
                    )
                elif current_col_tags[tag_name] != tag_value:
                    sql = self.sql_gen.set_column_tag(
                        full_table_name, col.name, tag_name, tag_value
                    )
                    plan.actions.append(
                        SyncAction(
                            action_type=ActionType.UPDATE_COLUMN_TAG,
                            target=f"{full_table_name}.{col.name}",
                            description=f"Update column tag {col.name}.{tag_name}: {current_col_tags[tag_name]} -> {tag_value}",
                            sql=sql,
                            details={
                                "column": col.name,
                                "tag": tag_name,
                                "old_value": current_col_tags[tag_name],
                                "new_value": tag_value,
                            },
                        )
                    )

            # Remove column tags not in contract
            for tag_name in current_col_tags:
                if tag_name not in contract_col_tags:
                    sql = self.sql_gen.remove_column_tag(full_table_name, col.name, tag_name)
                    plan.actions.append(
                        SyncAction(
                            action_type=ActionType.REMOVE_COLUMN_TAG,
                            target=f"{full_table_name}.{col.name}",
                            description=f"Remove column tag {col.name}.{tag_name}",
                            sql=sql,
                            details={"column": col.name, "tag": tag_name},
                        )
                    )

    def _plan_permission_changes(
        self,
        plan: SyncPlan,
        contract: Contract,
        current: CatalogTable | None,
        full_table_name: str,
    ) -> None:
        """Plan permission grant and revoke actions.

        Args:
            plan: SyncPlan to append actions to.
            contract: Contract with expected permission grants.
            current: Current table state (None if new table).
            full_table_name: Fully qualified table name.
        """
        # Build set of expected grants from contract permission_grants (from customProperties)
        expected_grants: set[CatalogGrant] = set()
        for perm_grant in contract.permission_grants:
            for privilege in perm_grant.privileges:
                expected_grants.add(
                    CatalogGrant(
                        principal=perm_grant.principal,
                        privilege=privilege.upper(),
                    )
                )

        # Get current grants (empty if table doesn't exist yet)
        current_grants: set[CatalogGrant] = set()
        if current is not None:
            current_grants = set(current.grants)

        # Plan grants to add (in contract but not in catalog)
        for grant in expected_grants - current_grants:
            sql = self.sql_gen.grant_permission(
                full_table_name=full_table_name,
                principal=grant.principal,
                privilege=grant.privilege,
            )
            plan.actions.append(
                SyncAction(
                    action_type=ActionType.GRANT_PERMISSION,
                    target=full_table_name,
                    description=f"Grant {grant.privilege} to {grant.principal}",
                    sql=sql,
                    details={
                        "principal": grant.principal,
                        "privilege": grant.privilege,
                    },
                )
            )

        # Plan revokes (in catalog but not in contract) - destructive
        for grant in current_grants - expected_grants:
            sql = self.sql_gen.revoke_permission(
                full_table_name=full_table_name,
                principal=grant.principal,
                privilege=grant.privilege,
            )
            plan.actions.append(
                SyncAction(
                    action_type=ActionType.REVOKE_PERMISSION,
                    target=full_table_name,
                    description=f"Revoke {grant.privilege} from {grant.principal}",
                    sql=sql,
                    details={
                        "principal": grant.principal,
                        "privilege": grant.privilege,
                    },
                )
            )
