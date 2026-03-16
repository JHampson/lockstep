"""CLI exceptions for Lockstep commands.

These exceptions are raised by helper functions and should be caught
by CLI commands to handle presentation (error messages, exit codes).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lockstep.databricks.config import AuthType
    from lockstep.services import ContractLoadError


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""

    pass


class InvalidAuthTypeError(ConfigurationError):
    """Raised when an invalid auth type is provided."""

    def __init__(self, auth_type: str) -> None:
        self.auth_type = auth_type
        super().__init__(
            f"Invalid auth-type '{auth_type}'. Must be one of: oauth, pat, sp, runtime"
        )


class MissingConfigurationError(ConfigurationError):
    """Raised when required configuration is missing."""

    def __init__(self, auth_type: AuthType) -> None:
        from lockstep.databricks.config import AuthType as AuthTypeEnum

        self.auth_type = auth_type
        if auth_type == AuthTypeEnum.PAT:
            self.missing = "--token required for auth-type=pat"
        elif auth_type == AuthTypeEnum.SP:
            self.missing = "--client-id and --client-secret required for auth-type=sp"
        else:
            self.missing = ""
        super().__init__("Databricks connection not configured")


class InvalidFormatError(Exception):
    """Raised when an invalid output format is specified."""

    def __init__(self, format_str: str) -> None:
        self.format_str = format_str
        super().__init__(f"Invalid format '{format_str}'. Must be: table, json, or junit")


class ContractLoadingError(Exception):
    """Raised when contracts fail to load."""

    def __init__(
        self,
        message: str,
        errors: list[str] | None = None,
        validation_errors: list[ContractLoadError] | None = None,
    ) -> None:
        self.errors = errors or []
        self.validation_errors = validation_errors or []
        super().__init__(message)
