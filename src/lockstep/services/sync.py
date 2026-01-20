"""Sync service for applying changes to Unity Catalog.

Orchestrates the synchronization process from contract to catalog.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from lockstep.models.catalog_state import SyncPlan
from lockstep.models.contract import Contract
from lockstep.services.diff import DiffService
from lockstep.services.introspection import IntrospectionService

if TYPE_CHECKING:
    from lockstep.databricks.connector import DatabricksConnector

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    """Result of a sync operation."""

    contract_name: str
    table_name: str
    success: bool = True
    actions_applied: int = 0
    actions_skipped: int = 0
    errors: list[str] = field(default_factory=list)
    plan: SyncPlan | None = None


@dataclass
class SyncOptions:
    """Options for sync operation."""

    dry_run: bool = False
    catalog_override: str | None = None
    schema_override: str | None = None
    table_prefix: str | None = None
    # Selective sync options - what to ADD (all default to True)
    add_tags: bool = True
    add_columns: bool = True
    add_descriptions: bool = True
    add_constraints: bool = True
    add_permissions: bool = True
    # Selective sync options - what to REMOVE (all default to False for safety)
    remove_columns: bool = False
    remove_tags: bool = False
    remove_constraints: bool = False
    remove_permissions: bool = False  # Destructive - revokes permissions
    # Destructive type changes (default False - may cause data loss)
    alter_column_types: bool = False


class SyncService:
    """Service for synchronizing contracts to Unity Catalog."""

    def __init__(self, connector: DatabricksConnector) -> None:
        """Initialize the sync service.

        Args:
            connector: Databricks SQL connector.
        """
        self.connector = connector
        self.introspection = IntrospectionService(connector)
        self.diff_service = DiffService()

    def sync_contract(self, contract: Contract, options: SyncOptions | None = None) -> SyncResult:
        """Synchronize a single contract to Unity Catalog.

        Args:
            contract: The contract to synchronize.
            options: Sync options (dry_run, allow_destructive, etc.).

        Returns:
            SyncResult with details of the operation.
        """
        if options is None:
            options = SyncOptions()

        full_table_name = contract.get_full_table_name(
            catalog_override=options.catalog_override,
            schema_override=options.schema_override,
            table_prefix=options.table_prefix,
        )

        result = SyncResult(
            contract_name=contract.name,
            table_name=full_table_name,
        )

        try:
            # Get current state
            logger.info(f"Introspecting current state of {full_table_name}")
            current_state = self.introspection.get_table(full_table_name)

            # Compute diff
            logger.info(f"Computing diff for {full_table_name}")
            plan = self.diff_service.compute_diff(
                contract=contract,
                current_state=current_state,
                catalog_override=options.catalog_override,
                schema_override=options.schema_override,
                table_prefix=options.table_prefix,
            )

            # Apply filters based on options
            # Filter out ADD operations if disabled
            if not options.add_tags:
                plan = plan.filter_no_add_tags()

            if not options.add_columns:
                plan = plan.filter_no_add_columns()

            if not options.add_descriptions:
                plan = plan.filter_no_add_descriptions()

            if not options.add_constraints:
                plan = plan.filter_no_add_constraints()

            if not options.add_permissions:
                plan = plan.filter_no_add_permissions()

            # Filter out REMOVE operations if disabled (default)
            if not options.remove_columns:
                plan = plan.filter_no_remove_columns()

            if not options.remove_tags:
                plan = plan.filter_no_remove_tags()

            if not options.remove_constraints:
                plan = plan.filter_no_remove_constraints()

            if not options.remove_permissions:
                plan = plan.filter_no_remove_permissions()

            # Filter out type changes if disabled (default)
            if not options.alter_column_types:
                plan = plan.filter_no_alter_column_types()

            result.plan = plan

            if not plan.has_changes:
                logger.info(f"No changes needed for {full_table_name}")
                return result

            # Apply or preview changes
            if options.dry_run:
                logger.info(f"Dry run - would apply {len(plan.actions)} action(s)")
                result.actions_skipped = len(plan.actions)
            else:
                self._apply_plan(plan, result)

        except Exception as e:
            logger.exception(f"Error syncing contract {contract.name}")
            result.success = False
            result.errors.append(str(e))

        return result

    def sync_contracts(
        self, contracts: list[Contract], options: SyncOptions | None = None
    ) -> list[SyncResult]:
        """Synchronize multiple contracts to Unity Catalog.

        Args:
            contracts: List of contracts to synchronize.
            options: Sync options.

        Returns:
            List of SyncResults.
        """
        results = []
        for contract in contracts:
            logger.info(f"Processing contract: {contract.name}")
            result = self.sync_contract(contract, options)
            results.append(result)

        return results

    def _apply_plan(self, plan: SyncPlan, result: SyncResult) -> None:
        """Apply a sync plan to Unity Catalog.

        Args:
            plan: The plan to apply.
            result: SyncResult to update with progress.
        """
        logger.info(f"Applying {len(plan.actions)} action(s) to {plan.table_name}")

        for action in plan.actions:
            if not action.sql:
                logger.warning(f"Skipping action without SQL: {action}")
                result.actions_skipped += 1
                continue

            try:
                logger.info(f"Executing: {action}")
                logger.debug(f"SQL: {action.sql}")
                self.connector.execute(action.sql)
                result.actions_applied += 1
            except Exception as e:
                error_msg = f"Failed to execute {action.action_type}: {e}"
                logger.error(error_msg)
                result.errors.append(error_msg)
                # Continue with remaining actions instead of failing entirely

        if result.errors:
            result.success = False
