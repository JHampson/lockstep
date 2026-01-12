"""Configuration for Databricks connectivity.

Supports configuration via:
- Environment variables
- CLI parameters
- Config file (~/.odcs_sync.toml or ~/.odcs_sync.yaml)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _load_config_file() -> dict[str, Any]:
    """Load configuration from file if it exists."""
    config_paths = [
        Path.home() / ".odcs_sync.yaml",
        Path.home() / ".odcs_sync.yml",
        Path.home() / ".odcs_sync.toml",
    ]

    for config_path in config_paths:
        if config_path.exists():
            if config_path.suffix == ".toml":
                try:
                    import tomllib
                except ImportError:
                    import tomli as tomllib  # type: ignore[import-not-found, no-redef]
                with open(config_path, "rb") as f:
                    return tomllib.load(f)
            else:
                with open(config_path) as f:
                    return yaml.safe_load(f) or {}

    return {}


class DatabricksConfig(BaseSettings):
    """Databricks connection configuration.

    Configuration is loaded from (in order of precedence):
    1. CLI parameters (passed directly)
    2. Environment variables (DATABRICKS_*)
    3. Config file (~/.odcs_sync.yaml or ~/.odcs_sync.toml)
    """

    model_config = SettingsConfigDict(
        env_prefix="DATABRICKS_",
        env_file=".env",
        extra="ignore",
    )

    # Required connection parameters
    host: str = Field(
        default="",
        description="Databricks workspace host (e.g., https://dbc-xxx.cloud.databricks.com)",
    )
    http_path: str = Field(
        default="",
        description="HTTP path for SQL warehouse (e.g., /sql/1.0/warehouses/xxx)",
    )

    # Authentication - prefer OAuth, fallback to PAT
    token: str | None = Field(
        default=None,
        description="Personal Access Token (fallback if OAuth not available)",
    )
    use_oauth: bool = Field(
        default=True,
        description="Use OAuth for authentication (requires databricks-sdk)",
    )

    # Service Principal / OAuth M2M authentication
    # Works on both AWS (OAuth M2M) and Azure (Service Principal)
    client_id: str | None = Field(
        default=None,
        description="OAuth client ID for service principal / M2M auth",
    )
    client_secret: str | None = Field(
        default=None,
        description="OAuth client secret for service principal / M2M auth",
    )

    # Optional defaults
    catalog_default: str | None = Field(
        default=None,
        description="Default catalog to use if not specified in contract",
    )
    schema_default: str | None = Field(
        default=None,
        description="Default schema to use if not specified in contract",
    )

    # Connection settings
    timeout_seconds: int = Field(
        default=300,
        description="Query timeout in seconds",
    )
    retry_count: int = Field(
        default=3,
        description="Number of retries for failed queries",
    )

    @model_validator(mode="before")
    @classmethod
    def load_from_config_file(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Load values from config file for any missing settings."""
        file_config = _load_config_file()

        # Map config file keys to model fields
        key_mapping = {
            "host": "host",
            "http_path": "http_path",
            "token": "token",
            "use_oauth": "use_oauth",
            "client_id": "client_id",
            "client_secret": "client_secret",
            "catalog_default": "catalog_default",
            "schema_default": "schema_default",
            "timeout_seconds": "timeout_seconds",
            "retry_count": "retry_count",
        }

        for file_key, model_key in key_mapping.items():
            if (
                model_key not in values or values.get(model_key) is None
            ) and file_key in file_config:
                values[model_key] = file_config[file_key]

        return values

    @model_validator(mode="after")
    def validate_connection(self) -> DatabricksConfig:
        """Validate that required connection parameters are provided."""
        if not self.host:
            # Try to get from environment one more time
            self.host = os.getenv("DATABRICKS_HOST", "")

        if not self.http_path:
            self.http_path = os.getenv("DATABRICKS_HTTP_PATH", "")

        return self

    def is_configured(self) -> bool:
        """Check if the configuration has required values."""
        return bool(self.host and self.http_path)

    def has_service_principal(self) -> bool:
        """Check if service principal credentials are configured."""
        return bool(self.client_id and self.client_secret)

    def get_auth_type(self) -> str:
        """Return the authentication type being used."""
        if self.has_service_principal():
            return "service_principal"
        if self.use_oauth:
            return "oauth"
        if self.token:
            return "token"
        return "none"
