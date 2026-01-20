"""Common CLI utilities - re-exports from options and helpers modules.

This module provides backwards compatibility by re-exporting all symbols
from the more focused options.py, helpers.py, exceptions.py, and logging_config.py modules.
"""

from __future__ import annotations

# Re-export exceptions
from lockstep.cli.exceptions import (
    ConfigurationError,
    ContractLoadingError,
    InvalidAuthTypeError,
    InvalidFormatError,
    MissingConfigurationError,
)

# Re-export helpers
from lockstep.cli.helpers import (
    # Data classes
    ConnectionOptions,
    LoadContractsResult,
    # Pure functions
    build_databricks_config,
    find_yaml_files,
    get_version,
    load_contracts_from_path,
    validate_databricks_config,
    validate_output_format,
)

# Re-export logging
from lockstep.cli.logging_config import setup_logging

# Re-export options
from lockstep.cli.options import (
    AlterColumnTypesArg,
    AuthTypeArg,
    CatalogOverrideArg,
    ClientIdArg,
    ClientSecretArg,
    FormatArg,
    HostArg,
    HttpPathArg,
    OutputArg,
    ProfileArg,
    QuietArg,
    SchemaOverrideArg,
    TablePrefixArg,
    TokenArg,
    VerboseArg,
    alter_column_types_option,
    auth_type_option,
    catalog_override_option,
    client_id_option,
    client_secret_option,
    format_option,
    host_option,
    http_path_option,
    output_option,
    path_argument,
    profile_option,
    quiet_option,
    schema_override_option,
    table_prefix_option,
    token_option,
    verbose_option,
)

# Re-export output types
from lockstep.cli.output import OutputFormat

__all__ = [
    # Exceptions
    "ConfigurationError",
    "InvalidAuthTypeError",
    "MissingConfigurationError",
    "InvalidFormatError",
    "ContractLoadingError",
    # Output types
    "OutputFormat",
    # Data classes
    "ConnectionOptions",
    "LoadContractsResult",
    # Logging
    "setup_logging",
    # Pure functions
    "get_version",
    "validate_output_format",
    "build_databricks_config",
    "validate_databricks_config",
    "find_yaml_files",
    "load_contracts_from_path",
    # Options
    "path_argument",
    "profile_option",
    "host_option",
    "http_path_option",
    "auth_type_option",
    "token_option",
    "client_id_option",
    "client_secret_option",
    "catalog_override_option",
    "schema_override_option",
    "table_prefix_option",
    "alter_column_types_option",
    "verbose_option",
    "quiet_option",
    "format_option",
    "output_option",
    # Type aliases
    "ProfileArg",
    "HostArg",
    "HttpPathArg",
    "AuthTypeArg",
    "TokenArg",
    "ClientIdArg",
    "ClientSecretArg",
    "CatalogOverrideArg",
    "SchemaOverrideArg",
    "TablePrefixArg",
    "AlterColumnTypesArg",
    "VerboseArg",
    "QuietArg",
    "FormatArg",
    "OutputArg",
]
