"""Configuration for Databricks connectivity.

Supports configuration via:
- CLI parameters (highest precedence)
- Databricks CLI profile (~/.databrickscfg)
- Environment variables
- Config file (~/.lockstep.toml or ~/.lockstep.yaml) (lowest precedence)
"""

from __future__ import annotations

import configparser
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
    PROFILE = "profile"  # Use Databricks CLI profile


def _load_databricks_profile(profile_name: str | None = None) -> dict[str, Any]:
    """Load configuration from Databricks CLI profile (~/.databrickscfg).

    Args:
        profile_name: Profile name to load. If None, uses DEFAULT.

    Returns:
        Dict with host, token, and auth_type from the profile.
    """
    config_path = Path.home() / ".databrickscfg"
    if not config_path.exists():
        return {}

    config = configparser.ConfigParser()
    config.read(config_path)

    section = profile_name or "DEFAULT"
    if section not in config:
        return {}

    result: dict[str, Any] = {}
    if config.has_option(section, "host"):
        host = config.get(section, "host")
        # Ensure host has https:// prefix
        if host and not host.startswith(("http://", "https://")):
            host = f"https://{host}"
        result["host"] = host

    if config.has_option(section, "token"):
        result["token"] = config.get(section, "token")

    # Check for auth_type in the profile
    if config.has_option(section, "auth_type"):
        result["auth_type"] = config.get(section, "auth_type")

    return result


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
                    import tomli as tomllib  # type: ignore[no-redef]
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
    2. Databricks CLI profile (~/.databrickscfg) if --profile specified
    3. Environment variables (DATABRICKS_*)
    4. Config file (~/.lockstep.yaml or ~/.lockstep.toml)
    """

    model_config = SettingsConfigDict(
        env_prefix="DATABRICKS_",
        env_file=".env",
        extra="ignore",
    )

    # Databricks CLI profile (alternative to explicit host/token)
    profile: str | None = Field(
        default=None,
        description="Databricks CLI profile name from ~/.databrickscfg",
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
        description="Authentication type: oauth, pat, sp, or profile",
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
        """Load values from config file and Databricks profile for any missing settings."""
        # First, load from Databricks CLI profile if specified
        profile_name = values.get("profile")
        if profile_name:
            profile_config = _load_databricks_profile(profile_name)
            # Load host from profile if not explicitly provided
            if (not values.get("host")) and profile_config.get("host"):
                values["host"] = profile_config["host"]
            # Determine auth type from profile
            if not values.get("auth_type"):
                if profile_config.get("token"):
                    # Profile has a token, use PAT auth
                    values["token"] = profile_config["token"]
                    values["auth_type"] = AuthType.PAT
                elif profile_config.get("auth_type") in ("databricks-cli", "oauth-m2m"):
                    # Profile uses Databricks CLI OAuth or M2M OAuth
                    values["auth_type"] = AuthType.OAUTH
                else:
                    # Default to OAuth for profiles without explicit token
                    values["auth_type"] = AuthType.OAUTH

        # Then load from lockstep config file
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
            "profile": "profile",
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

        # Normalize http_path - accept warehouse ID or full path
        if self.http_path:
            self.http_path = self._normalize_http_path(self.http_path)

        # Check auth_type from environment if not set
        env_auth_type = os.getenv("DATABRICKS_AUTH_TYPE")
        if env_auth_type:
            with contextlib.suppress(ValueError):
                self.auth_type = AuthType(env_auth_type.lower())

        return self

    @staticmethod
    def _normalize_http_path(http_path: str) -> str:
        """Normalize http_path to full path format.

        Accepts:
        - Warehouse ID: d46d90fb53d76376
        - Full path: /sql/1.0/warehouses/d46d90fb53d76376

        Returns:
            Full path format: /sql/1.0/warehouses/<id>
        """
        http_path = http_path.strip()

        # If it already looks like a path, return as-is
        if "/" in http_path:
            return http_path

        # Otherwise, assume it's a warehouse ID and construct the path
        return f"/sql/1.0/warehouses/{http_path}"

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
        if self.profile:
            return f"Databricks CLI profile '{self.profile}'"
        if self.auth_type == AuthType.SP:
            return "Service Principal"
        if self.auth_type == AuthType.PAT:
            return "Personal Access Token"
        return "OAuth"
