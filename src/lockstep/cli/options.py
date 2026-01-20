"""CLI option definitions shared across commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

# ============================================================================
# Common CLI Options
# ============================================================================

# Path argument for commands that take a contract path
path_argument = typer.Argument(
    help="Path to ODCS YAML file or directory containing YAML files.",
    exists=True,
    resolve_path=True,
)

# Connection options
profile_option = typer.Option(
    "--profile",
    "-p",
    envvar="DATABRICKS_CONFIG_PROFILE",
    help="Databricks CLI profile name from ~/.databrickscfg (alternative to --host).",
)

host_option = typer.Option(
    "--host",
    envvar="DATABRICKS_HOST",
    help="Databricks workspace host URL (not required if --profile is set).",
)

http_path_option = typer.Option(
    "--sql-endpoint",
    "--http-path",
    envvar="DATABRICKS_HTTP_PATH",
    help="SQL warehouse endpoint path (e.g., /sql/1.0/warehouses/abc123).",
)

# Authentication options
auth_type_option = typer.Option(
    "--auth-type",
    envvar="DATABRICKS_AUTH_TYPE",
    help="Authentication type: oauth (default), pat (Personal Access Token), or sp (Service Principal).",
)

token_option = typer.Option(
    "--token",
    envvar="DATABRICKS_TOKEN",
    help="Personal Access Token (required when auth-type=pat).",
)

client_id_option = typer.Option(
    "--client-id",
    envvar="DATABRICKS_CLIENT_ID",
    help="OAuth client ID (required when auth-type=sp).",
)

client_secret_option = typer.Option(
    "--client-secret",
    envvar="DATABRICKS_CLIENT_SECRET",
    help="OAuth client secret (required when auth-type=sp).",
)

# Override options
catalog_override_option = typer.Option(
    "--catalog-override",
    help="Override catalog name from contracts.",
)

schema_override_option = typer.Option(
    "--schema-override",
    help="Override schema name from contracts.",
)

table_prefix_option = typer.Option(
    "--table-prefix",
    help="Prefix to add to table names.",
)

# Output options
verbose_option = typer.Option(
    "--verbose",
    "-v",
    help="Enable verbose output (debug logging).",
)

quiet_option = typer.Option(
    "--quiet",
    "-q",
    help="Suppress non-error output.",
)

format_option = typer.Option(
    "--format",
    "-f",
    help="Output format: table (default), json, or junit.",
)

output_option = typer.Option(
    "--out",
    "-o",
    help="Write output to file (in addition to displaying).",
)


# ============================================================================
# Type Aliases for Command Signatures
# ============================================================================

ProfileArg = Annotated[str | None, profile_option]
HostArg = Annotated[str | None, host_option]
HttpPathArg = Annotated[str | None, http_path_option]
AuthTypeArg = Annotated[str | None, auth_type_option]
TokenArg = Annotated[str | None, token_option]
ClientIdArg = Annotated[str | None, client_id_option]
ClientSecretArg = Annotated[str | None, client_secret_option]
CatalogOverrideArg = Annotated[str | None, catalog_override_option]
SchemaOverrideArg = Annotated[str | None, schema_override_option]
TablePrefixArg = Annotated[str | None, table_prefix_option]
VerboseArg = Annotated[bool, verbose_option]
QuietArg = Annotated[bool, quiet_option]
FormatArg = Annotated[str | None, format_option]
OutputArg = Annotated[Path | None, output_option]

