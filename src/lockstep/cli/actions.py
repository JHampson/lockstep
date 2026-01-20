"""Action execution functions that perform business logic and return structured results.

These functions separate the core action logic from CLI presentation,
making the code more testable and reusable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from lockstep.databricks import DatabricksConfig, DatabricksConnector
from lockstep.databricks.connector import DatabricksConnectionError
from lockstep.models.catalog_state import SavedPlan, SyncPlan
from lockstep.models.contract import Contract
from lockstep.services import ContractLoader, SyncService
from lockstep.services.sync import SyncOptions, SyncResult

# ============================================================================
# Result Data Classes
# ============================================================================


@dataclass
class PlanResult:
    """Result of a plan action."""

    success: bool
    timestamp: str
    host: str
    results: list[SyncResult]
    has_changes: bool
    plans_to_save: list[SyncPlan] = field(default_factory=list)
    error: str | None = None


@dataclass
class ApplyResult:
    """Result of an apply action."""

    success: bool
    timestamp: str
    results: list[SyncResult]
    total_applied: int = 0
    total_failed: int = 0
    error: str | None = None
    # For applying saved plans
    plan_file: str | None = None


@dataclass
class FileValidationResult:
    """Result of validating a single file."""

    file_path: Path
    relative_path: str
    valid: bool
    errors: list[str] = field(default_factory=list)


@dataclass
class ValidateResult:
    """Result of a validate action."""

    success: bool
    timestamp: str
    base_path: Path
    results: list[FileValidationResult]
    total: int
    valid_count: int
    invalid_count: int
    error: str | None = None


# ============================================================================
# Action Functions
# ============================================================================


def execute_plan(
    contracts: list[Contract],
    config: DatabricksConfig,
    sync_options: SyncOptions,
) -> PlanResult:
    """Execute a plan action to show what changes would be made.

    Args:
        contracts: List of contracts to plan.
        config: Databricks connection configuration.
        sync_options: Options controlling what to sync.

    Returns:
        PlanResult with the sync results and change information.
    """
    timestamp = datetime.now(UTC).isoformat()

    try:
        with DatabricksConnector(config) as connector:
            sync_service = SyncService(connector)
            results = sync_service.sync_contracts(contracts, sync_options)

            has_changes = False
            plans_to_save = []

            for result in results:
                if result.plan and result.plan.has_changes:
                    has_changes = True
                    plans_to_save.append(result.plan)

            return PlanResult(
                success=True,
                timestamp=timestamp,
                host=config.host,
                results=results,
                has_changes=has_changes,
                plans_to_save=plans_to_save,
            )

    except DatabricksConnectionError as e:
        return PlanResult(
            success=False,
            timestamp=timestamp,
            host=config.host,
            results=[],
            has_changes=False,
            error=str(e),
        )


def execute_apply(
    contracts: list[Contract],
    config: DatabricksConfig,
    sync_options: SyncOptions,
) -> ApplyResult:
    """Execute an apply action to sync contracts to Unity Catalog.

    Args:
        contracts: List of contracts to apply.
        config: Databricks connection configuration.
        sync_options: Options controlling what to sync.

    Returns:
        ApplyResult with the sync results.
    """
    timestamp = datetime.now(UTC).isoformat()

    try:
        with DatabricksConnector(config) as connector:
            sync_service = SyncService(connector)
            results = sync_service.sync_contracts(contracts, sync_options)

            total_applied = sum(r.actions_applied for r in results)
            total_failed = sum(len(r.errors) for r in results)

            return ApplyResult(
                success=all(r.success for r in results),
                timestamp=timestamp,
                results=results,
                total_applied=total_applied,
                total_failed=total_failed,
            )

    except DatabricksConnectionError as e:
        return ApplyResult(
            success=False,
            timestamp=timestamp,
            results=[],
            error=str(e),
        )


def execute_apply_saved_plan(
    saved_plan: SavedPlan,
    config: DatabricksConfig,
    plan_file: str,
) -> ApplyResult:
    """Execute an apply action from a saved plan file.

    Args:
        saved_plan: The loaded saved plan.
        config: Databricks connection configuration.
        plan_file: Path to the plan file (for reporting).

    Returns:
        ApplyResult with the application results.
    """
    timestamp = datetime.now(UTC).isoformat()

    if not saved_plan.has_changes:
        return ApplyResult(
            success=True,
            timestamp=timestamp,
            results=[],
            plan_file=plan_file,
        )

    try:
        with DatabricksConnector(config) as connector:
            total_applied = 0
            total_failed = 0
            results = []

            for plan in saved_plan.plans:
                plan_applied = 0
                plan_errors = []

                for action in plan.actions:
                    if action.sql:
                        try:
                            connector.execute(action.sql)
                            plan_applied += 1
                            total_applied += 1
                        except Exception as e:
                            plan_errors.append(f"{action.description}: {e}")
                            total_failed += 1

                results.append(
                    SyncResult(
                        contract_name=plan.contract_name,
                        table_name=plan.table_name,
                        success=len(plan_errors) == 0,
                        actions_applied=plan_applied,
                        errors=plan_errors,
                        plan=plan,
                    )
                )

            return ApplyResult(
                success=total_failed == 0,
                timestamp=timestamp,
                results=results,
                total_applied=total_applied,
                total_failed=total_failed,
                plan_file=plan_file,
            )

    except DatabricksConnectionError as e:
        return ApplyResult(
            success=False,
            timestamp=timestamp,
            results=[],
            error=str(e),
            plan_file=plan_file,
        )


def execute_validate(
    path: Path,
    loader: ContractLoader,
) -> ValidateResult:
    """Execute a validate action to check contract YAML files.

    Args:
        path: Path to YAML file or directory.
        loader: ContractLoader instance for validation.

    Returns:
        ValidateResult with validation results for each file.
    """
    timestamp = datetime.now(UTC).isoformat()

    # Find all YAML files using the loader's internal method
    files = list(loader._find_yaml_files(path))

    if not files:
        return ValidateResult(
            success=True,
            timestamp=timestamp,
            base_path=path,
            results=[],
            total=0,
            valid_count=0,
            invalid_count=0,
        )

    # Validate each file
    results = []
    valid_count = 0
    invalid_count = 0

    for yaml_file in files:
        is_valid, errors = loader.validate_file(yaml_file)
        rel_path = str(yaml_file.relative_to(path) if path.is_dir() else yaml_file.name)

        results.append(
            FileValidationResult(
                file_path=yaml_file,
                relative_path=rel_path,
                valid=is_valid,
                errors=errors,
            )
        )

        if is_valid:
            valid_count += 1
        else:
            invalid_count += 1

    total = valid_count + invalid_count

    return ValidateResult(
        success=invalid_count == 0,
        timestamp=timestamp,
        base_path=path,
        results=results,
        total=total,
        valid_count=valid_count,
        invalid_count=invalid_count,
    )
