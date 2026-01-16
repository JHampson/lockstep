"""Models representing the current state of Unity Catalog objects.

These models are used to represent the introspected state from Databricks
and for diffing against the desired contract state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


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

    def to_dict(self) -> dict[str, Any]:
        """Serialize action to dictionary."""
        return {
            "action_type": self.action_type.value,
            "target": self.target,
            "description": self.description,
            "sql": self.sql,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SyncAction:
        """Deserialize action from dictionary."""
        return cls(
            action_type=ActionType(data["action_type"]),
            target=data["target"],
            description=data["description"],
            sql=data.get("sql"),
            details=data.get("details", {}),
        )


# Action types that are considered destructive (data loss or breaking changes)
DESTRUCTIVE_ACTION_TYPES: frozenset[ActionType] = frozenset(
    {
        ActionType.DROP_COLUMN,
        ActionType.REMOVE_TABLE_TAG,
        ActionType.REMOVE_COLUMN_TAG,
        ActionType.DROP_PRIMARY_KEY,
        ActionType.DROP_NOT_NULL,
    }
)

# Action types for tag removal (subset of destructive)
TAG_REMOVAL_ACTION_TYPES: frozenset[ActionType] = frozenset(
    {
        ActionType.REMOVE_TABLE_TAG,
        ActionType.REMOVE_COLUMN_TAG,
    }
)

# Action types for adding tags
TAG_ADD_ACTION_TYPES: frozenset[ActionType] = frozenset(
    {
        ActionType.ADD_TABLE_TAG,
        ActionType.UPDATE_TABLE_TAG,
        ActionType.ADD_COLUMN_TAG,
        ActionType.UPDATE_COLUMN_TAG,
    }
)

# Action types for adding columns
COLUMN_ADD_ACTION_TYPES: frozenset[ActionType] = frozenset(
    {
        ActionType.ADD_COLUMN,
    }
)

# Action types for description updates
DESCRIPTION_ACTION_TYPES: frozenset[ActionType] = frozenset(
    {
        ActionType.UPDATE_TABLE_DESCRIPTION,
        ActionType.UPDATE_COLUMN_DESCRIPTION,
    }
)

# Action types for constraint changes
CONSTRAINT_ADD_ACTION_TYPES: frozenset[ActionType] = frozenset(
    {
        ActionType.ADD_PRIMARY_KEY,
        ActionType.ADD_NOT_NULL,
    }
)

# Action types for removing columns
COLUMN_REMOVE_ACTION_TYPES: frozenset[ActionType] = frozenset(
    {
        ActionType.DROP_COLUMN,
    }
)

# Action types for removing constraints
CONSTRAINT_REMOVE_ACTION_TYPES: frozenset[ActionType] = frozenset(
    {
        ActionType.DROP_PRIMARY_KEY,
        ActionType.DROP_NOT_NULL,
    }
)


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
        return any(action.action_type in DESTRUCTIVE_ACTION_TYPES for action in self.actions)

    def filter_no_add_tags(self) -> SyncPlan:
        """Return a new plan without tag add/update actions."""
        filtered_actions = [
            action for action in self.actions if action.action_type not in TAG_ADD_ACTION_TYPES
        ]
        return SyncPlan(
            contract_name=self.contract_name,
            table_name=self.table_name,
            actions=filtered_actions,
        )

    def filter_no_add_columns(self) -> SyncPlan:
        """Return a new plan without add column actions."""
        filtered_actions = [
            action for action in self.actions if action.action_type not in COLUMN_ADD_ACTION_TYPES
        ]
        return SyncPlan(
            contract_name=self.contract_name,
            table_name=self.table_name,
            actions=filtered_actions,
        )

    def filter_no_add_descriptions(self) -> SyncPlan:
        """Return a new plan without description update actions."""
        filtered_actions = [
            action for action in self.actions if action.action_type not in DESCRIPTION_ACTION_TYPES
        ]
        return SyncPlan(
            contract_name=self.contract_name,
            table_name=self.table_name,
            actions=filtered_actions,
        )

    def filter_no_add_constraints(self) -> SyncPlan:
        """Return a new plan without constraint add actions."""
        filtered_actions = [
            action
            for action in self.actions
            if action.action_type not in CONSTRAINT_ADD_ACTION_TYPES
        ]
        return SyncPlan(
            contract_name=self.contract_name,
            table_name=self.table_name,
            actions=filtered_actions,
        )

    def filter_no_remove_columns(self) -> SyncPlan:
        """Return a new plan without column drop actions."""
        filtered_actions = [
            action
            for action in self.actions
            if action.action_type not in COLUMN_REMOVE_ACTION_TYPES
        ]
        return SyncPlan(
            contract_name=self.contract_name,
            table_name=self.table_name,
            actions=filtered_actions,
        )

    def filter_no_remove_tags(self) -> SyncPlan:
        """Return a new plan without tag removal actions."""
        filtered_actions = [
            action for action in self.actions if action.action_type not in TAG_REMOVAL_ACTION_TYPES
        ]
        return SyncPlan(
            contract_name=self.contract_name,
            table_name=self.table_name,
            actions=filtered_actions,
        )

    def filter_no_remove_constraints(self) -> SyncPlan:
        """Return a new plan without constraint drop actions."""
        filtered_actions = [
            action
            for action in self.actions
            if action.action_type not in CONSTRAINT_REMOVE_ACTION_TYPES
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

    def to_dict(self) -> dict[str, Any]:
        """Serialize plan to dictionary."""
        return {
            "contract_name": self.contract_name,
            "table_name": self.table_name,
            "actions": [action.to_dict() for action in self.actions],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SyncPlan:
        """Deserialize plan from dictionary."""
        return cls(
            contract_name=data["contract_name"],
            table_name=data["table_name"],
            actions=[SyncAction.from_dict(a) for a in data.get("actions", [])],
        )


@dataclass
class SavedPlan:
    """A saved plan file containing one or more sync plans."""

    version: str = "1.0"
    created_at: str = ""
    host: str = ""
    plans: list[SyncPlan] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize saved plan to dictionary."""
        return {
            "version": self.version,
            "created_at": self.created_at,
            "host": self.host,
            "plans": [plan.to_dict() for plan in self.plans],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SavedPlan:
        """Deserialize saved plan from dictionary."""
        return cls(
            version=data.get("version", "1.0"),
            created_at=data.get("created_at", ""),
            host=data.get("host", ""),
            plans=[SyncPlan.from_dict(p) for p in data.get("plans", [])],
        )

    @property
    def total_actions(self) -> int:
        """Total number of actions across all plans."""
        return sum(len(plan.actions) for plan in self.plans)

    @property
    def has_changes(self) -> bool:
        """Check if there are any changes in any plan."""
        return any(plan.has_changes for plan in self.plans)
