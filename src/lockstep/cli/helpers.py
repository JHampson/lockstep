"""CLI helper functions for command implementations.

This module contains pure helper functions that do NOT write to console.
They return data or raise exceptions. The CLI commands handle presentation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from lockstep import __version__
from lockstep.cli.exceptions import (
    ContractLoadingError,
    InvalidAuthTypeError,
    InvalidFormatError,
    MissingConfigurationError,
)
from lockstep.databricks import DatabricksConfig
from lockstep.databricks.config import AuthType
from lockstep.models.contract import Contract
from lockstep.services import ContractLoader, ContractLoadError

if TYPE_CHECKING:
    from lockstep.cli.output import OutputFormat

# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ConnectionOptions:
    """Options for connecting to Databricks."""

    profile: str | None = None
    host: str | None = None
    http_path: str | None = None
    auth_type: str | None = None
    token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None


@dataclass
class LoadContractsResult:
    """Result of loading contracts."""

    contracts: list[Contract]
    validation_errors: list[ContractLoadError] = field(default_factory=list)

    @property
    def has_validation_errors(self) -> bool:
        """Return True if there were validation errors."""
        return len(self.validation_errors) > 0


# =============================================================================
# Pure Helper Functions (no console output)
# =============================================================================


def get_version() -> str:
    """Return the lockstep version string."""
    return __version__


def validate_output_format(format_str: str | None) -> OutputFormat:
    """Validate and normalize the output format option.

    Args:
        format_str: The format string to validate.

    Returns:
        OutputFormat enum value.

    Raises:
        InvalidFormatError: If format is not valid.
    """
    # Import here to avoid circular import
    from lockstep.cli.output import OutputFormat

    normalized = (format_str or "table").lower()
    try:
        return OutputFormat(normalized)
    except ValueError:
        raise InvalidFormatError(format_str or "") from None


def build_databricks_config(conn: ConnectionOptions) -> DatabricksConfig:
    """Build Databricks configuration from connection options.

    Args:
        conn: Connection options.

    Returns:
        DatabricksConfig instance.

    Raises:
        InvalidAuthTypeError: If auth_type is invalid.
    """
    # Determine auth type
    resolved_auth_type = AuthType.OAUTH  # Default
    if conn.auth_type:
        try:
            resolved_auth_type = AuthType(conn.auth_type.lower())
        except ValueError as e:
            raise InvalidAuthTypeError(conn.auth_type) from e

    return DatabricksConfig(
        profile=conn.profile,
        host=conn.host or "",
        http_path=conn.http_path or "",
        auth_type=resolved_auth_type,
        token=conn.token,
        client_id=conn.client_id,
        client_secret=conn.client_secret,
    )


def validate_databricks_config(config: DatabricksConfig) -> None:
    """Validate that a Databricks configuration is complete.

    Args:
        config: The configuration to validate.

    Raises:
        MissingConfigurationError: If required configuration is missing.
    """
    if not config.is_configured():
        raise MissingConfigurationError(config.auth_type)


def find_yaml_files(path: Path) -> list[Path]:
    """Find all YAML files in a path.

    Args:
        path: Path to YAML file or directory.

    Returns:
        List of YAML file paths, sorted alphabetically.
    """
    if path.is_file():
        return [path]
    return sorted(list(path.glob("**/*.yaml")) + list(path.glob("**/*.yml")))


def load_contracts_from_path(
    path: Path,
    loader: ContractLoader,
) -> LoadContractsResult:
    """Load contracts from a file or directory.

    Args:
        path: Path to YAML file or directory.
        loader: ContractLoader instance.

    Returns:
        LoadContractsResult with contracts and any validation errors.

    Raises:
        ContractLoadingError: If a single file fails to load.
    """
    if path.is_file():
        try:
            contracts = [loader.load_one(path)]
            return LoadContractsResult(contracts=contracts)
        except ContractLoadError as e:
            raise ContractLoadingError(
                f"Failed to load contract: {e}",
                errors=e.errors,
            ) from e
    else:
        contracts = loader.load_many(path)
        return LoadContractsResult(
            contracts=contracts,
            validation_errors=loader.validation_errors,
        )
