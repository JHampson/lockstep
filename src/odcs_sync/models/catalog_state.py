"""Models representing the current state of Unity Catalog objects.

These models are used to represent the introspected state from Databricks
and for diffing against the desired contract state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ActionType(str, Enum):
    """Types of sync actions that can be performed."""

    CREATE_TABLE = "create_table"
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    UPDATE_TABLE_DESCRIPTION = "update_table_description"
    UPDATE_COLUMN_DESCRIPTION = "update_column_description"
    UPDATE_COLUMN_TYPE = "update_column_type"
    ADD_TABLE_TAG = "add_table_tag"
    UPDATE_TABLE_TAG = "update_table_tag"
    REMOVE_TABLE_TAG = "remove_table_tag"
    ADD_COLUMN_TAG = "add_column_tag"
    UPDATE_COLUMN_TAG = "update_column_tag"
    REMOVE_COLUMN_TAG = "remove_column_tag"
    ADD_PRIMARY_KEY = "add_primary_key"
    DROP_PRIMARY_KEY = "drop_primary_key"
    ADD_NOT_NULL = "add_not_null"
    DROP_NOT_NULL = "drop_not_null"
    SET_CERTIFICATION = "set_certification"
    CLEAR_CERTIFICATION = "clear_certification"


@dataclass
class CatalogColumn:
    """Represents a column as it exists in Unity Catalog."""

    name: str
    data_type: str
    nullable: bool = True
    description: str | None = None
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class CatalogConstraint:
    """Represents a constraint in Unity Catalog."""

    name: str
    constraint_type: str  # PRIMARY_KEY, NOT_NULL
    columns: list[str] = field(default_factory=list)


@dataclass
class CatalogTable:
    """Represents a table as it exists in Unity Catalog."""

    catalog: str
    schema_name: str
    table_name: str
    columns: list[CatalogColumn] = field(default_factory=list)
    description: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    constraints: list[CatalogConstraint] = field(default_factory=list)
    certification_status: str | None = None  # 'certified', 'deprecated', or None

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.schema_name}.{self.table_name}"

    def get_column(self, name: str) -> CatalogColumn | None:
        """Get a column by name (case-insensitive)."""
        name_lower = name.lower()
        for col in self.columns:
            if col.name.lower() == name_lower:
                return col
        return None

    def get_primary_key_constraint(self) -> CatalogConstraint | None:
        """Get the primary key constraint if it exists."""
        for constraint in self.constraints:
            if constraint.constraint_type == "PRIMARY_KEY":
                return constraint
        return None


@dataclass
class SyncAction:
    """Represents a single synchronization action to be performed."""

    action_type: ActionType
    target: str  # Full table name or column identifier
    description: str  # Human-readable description
    sql: str | None = None  # SQL to execute (for display/execution)
    details: dict[str, str] = field(default_factory=dict)  # Additional context

    def __str__(self) -> str:
        return f"[{self.action_type.value}] {self.description}"


@dataclass
class SyncPlan:
    """A complete plan of actions to synchronize a contract to Unity Catalog."""

    contract_name: str
    table_name: str
    actions: list[SyncAction] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Check if there are any changes to apply."""
        return len(self.actions) > 0

    @property
    def has_destructive_changes(self) -> bool:
        """Check if the plan contains destructive changes."""
        destructive_types = {
            ActionType.DROP_COLUMN,
            ActionType.REMOVE_TABLE_TAG,
            ActionType.REMOVE_COLUMN_TAG,
            ActionType.DROP_PRIMARY_KEY,
            ActionType.DROP_NOT_NULL,
        }
        return any(action.action_type in destructive_types for action in self.actions)

    def filter_non_destructive(self) -> SyncPlan:
        """Return a new plan with only non-destructive actions."""
        destructive_types = {
            ActionType.DROP_COLUMN,
            ActionType.REMOVE_TABLE_TAG,
            ActionType.REMOVE_COLUMN_TAG,
            ActionType.DROP_PRIMARY_KEY,
            ActionType.DROP_NOT_NULL,
        }
        filtered_actions = [
            action for action in self.actions if action.action_type not in destructive_types
        ]
        return SyncPlan(
            contract_name=self.contract_name,
            table_name=self.table_name,
            actions=filtered_actions,
        )

    def filter_preserve_extra_tags(self) -> SyncPlan:
        """Return a new plan without tag removal actions."""
        tag_removal_types = {
            ActionType.REMOVE_TABLE_TAG,
            ActionType.REMOVE_COLUMN_TAG,
        }
        filtered_actions = [
            action for action in self.actions if action.action_type not in tag_removal_types
        ]
        return SyncPlan(
            contract_name=self.contract_name,
            table_name=self.table_name,
            actions=filtered_actions,
        )

    def get_summary(self) -> dict[str, int]:
        """Get a summary count of actions by type."""
        summary: dict[str, int] = {}
        for action in self.actions:
            key = action.action_type.value
            summary[key] = summary.get(key, 0) + 1
        return summary
