"""Common CLI utilities - re-exports from options and helpers modules.

This module provides backwards compatibility by re-exporting all symbols
from the more focused options.py and helpers.py modules.
"""

from __future__ import annotations

# Re-export helpers
from lockstep.cli.helpers import (
    console,
    ensure_databricks_config,
    error_console,
    get_databricks_config,
    load_contracts,
    setup_logging,
    validate_output_format,
    version_callback,
)

# Re-export options
from lockstep.cli.options import (
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

__all__ = [
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
    "VerboseArg",
    "QuietArg",
    "FormatArg",
    "OutputArg",
    # Helpers
    "console",
    "error_console",
    "setup_logging",
    "version_callback",
    "validate_output_format",
    "get_databricks_config",
    "ensure_databricks_config",
    "load_contracts",
]
