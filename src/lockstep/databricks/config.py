"""Configuration for Databricks connectivity.

Supports configuration via:
- CLI parameters (highest precedence)
- Environment variables
- Config file (~/.lockstep.toml or ~/.lockstep.yaml) (lowest precedence)
"""

from __future__ import annotations

import contextlib
import os
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuthType(str, Enum):
    """Authentication type for Databricks connection."""

    OAUTH = "oauth"  # Interactive OAuth / Databricks CLI / Azure CLI
    PAT = "pat"  # Personal Access Token
    SP = "sp"  # Service Principal (OAuth M2M)


def _load_config_file() -> dict[str, Any]:
    """Load configuration from file if it exists."""
    config_paths = [
        Path.home() / ".lockstep.yaml",
        Path.home() / ".lockstep.yml",
        Path.home() / ".lockstep.toml",
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
    3. Config file (~/.lockstep.yaml or ~/.lockstep.toml)
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
        description="SQL warehouse endpoint path (e.g., /sql/1.0/warehouses/xxx)",
    )

    # Authentication type
    auth_type: AuthType = Field(
        default=AuthType.OAUTH,
        description="Authentication type: oauth, pat, or sp",
    )

    # Token for PAT authentication
    token: str | None = Field(
        default=None,
        description="Personal Access Token (required for auth_type=pat)",
    )

    # Service Principal credentials (for auth_type=sp)
    client_id: str | None = Field(
        default=None,
        description="OAuth client ID for service principal auth (required for auth_type=sp)",
    )
    client_secret: str | None = Field(
        default=None,
        description="OAuth client secret for service principal auth (required for auth_type=sp)",
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
            "auth_type": "auth_type",
            "token": "token",
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

        # Check auth_type from environment if not set
        env_auth_type = os.getenv("DATABRICKS_AUTH_TYPE")
        if env_auth_type:
            with contextlib.suppress(ValueError):
                self.auth_type = AuthType(env_auth_type.lower())

        return self

    def is_configured(self) -> bool:
        """Check if the configuration has required values."""
        if not self.host or not self.http_path:
            return False

        # Check auth-specific requirements
        if self.auth_type == AuthType.PAT and not self.token:
            return False

        return not (self.auth_type == AuthType.SP and not (self.client_id and self.client_secret))

    def get_auth_description(self) -> str:
        """Return a human-readable description of the authentication being used."""
        if self.auth_type == AuthType.SP:
            return "Service Principal"
        if self.auth_type == AuthType.PAT:
            return "Personal Access Token"
        return "OAuth"
