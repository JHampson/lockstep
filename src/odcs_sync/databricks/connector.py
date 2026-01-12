"""Databricks SQL connector with context manager support.

Provides a clean interface for executing SQL against Databricks SQL endpoints.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from databricks import sql as databricks_sql

from odcs_sync.databricks.config import DatabricksConfig

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor

logger = logging.getLogger(__name__)


class DatabricksConnectionError(Exception):
    """Raised when there's an error connecting to Databricks."""


class DatabricksQueryError(Exception):
    """Raised when there's an error executing a query."""


class DatabricksConnector:
    """Databricks SQL connector with connection pooling and retry logic."""

    def __init__(self, config: DatabricksConfig) -> None:
        """Initialize the connector with configuration.

        Args:
            config: Databricks connection configuration.

        Raises:
            DatabricksConnectionError: If configuration is invalid.
        """
        if not config.is_configured():
            raise DatabricksConnectionError(
                "Databricks configuration is incomplete. "
                "Please set DATABRICKS_HOST and DATABRICKS_HTTP_PATH "
                "environment variables or provide them via config file."
            )

        self.config = config
        self._connection: Connection | None = None

    def _get_connection_kwargs(self) -> dict[str, Any]:
        """Build connection keyword arguments based on config."""
        kwargs: dict[str, Any] = {
            "server_hostname": self.config.host.replace("https://", "").replace("http://", ""),
            "http_path": self.config.http_path,
        }

        # Authentication - check service principal first, then OAuth, then token
        if self.config.has_service_principal():
            # Service Principal / OAuth M2M authentication
            # Works on both AWS (OAuth M2M) and Azure (Service Principal)
            try:
                from databricks.sdk.core import Config

                sp_config = Config(
                    host=self.config.host,
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret,
                )

                # Get the auth headers and extract the token
                auth_headers = sp_config.authenticate()
                auth_header = auth_headers.get("Authorization", "")
                if auth_header.startswith("Bearer "):
                    token = auth_header[7:]  # Remove "Bearer " prefix
                    kwargs["access_token"] = token
                    logger.debug("Using Service Principal / OAuth M2M authentication")
                else:
                    raise DatabricksConnectionError(
                        "Service Principal authentication returned unexpected format. "
                        f"Got: {list(auth_headers.keys())}"
                    )
            except ImportError as e:
                raise DatabricksConnectionError(
                    "Service Principal authentication requires databricks-sdk. "
                    "Please install it with: pip install databricks-sdk"
                ) from e
            except Exception as e:
                raise DatabricksConnectionError(
                    f"Service Principal / OAuth M2M authentication failed: {e}"
                ) from e

        elif self.config.use_oauth:
            # Use Databricks SDK for OAuth credential chain
            try:
                from databricks.sdk.core import Config

                # Create SDK config - it will use Azure CLI, env vars, etc.
                sdk_config = Config(host=self.config.host)

                # Get the auth headers and extract the token
                auth_headers = sdk_config.authenticate()
                auth_header = auth_headers.get("Authorization", "")
                if auth_header.startswith("Bearer "):
                    token = auth_header[7:]  # Remove "Bearer " prefix
                    kwargs["access_token"] = token
                    logger.debug("Using OAuth token from Databricks SDK")
                else:
                    raise DatabricksConnectionError(
                        "OAuth authentication returned unexpected format. "
                        f"Got: {list(auth_headers.keys())}"
                    )
            except ImportError as e:
                logger.warning(f"Databricks SDK not available ({e}), falling back to token auth")
                if self.config.token:
                    kwargs["access_token"] = self.config.token
                else:
                    raise DatabricksConnectionError(
                        "OAuth authentication failed (databricks-sdk not installed) "
                        "and no access token provided. "
                        "Please install databricks-sdk or set DATABRICKS_TOKEN."
                    ) from e
            except DatabricksConnectionError:
                raise
            except Exception as e:
                logger.warning(f"OAuth setup failed ({e}), falling back to token auth")
                if self.config.token:
                    kwargs["access_token"] = self.config.token
                else:
                    raise DatabricksConnectionError(
                        "OAuth authentication failed and no access token provided. "
                        "Please set DATABRICKS_TOKEN or configure OAuth credentials."
                    ) from e
        elif self.config.token:
            kwargs["access_token"] = self.config.token
            logger.debug("Using token authentication")
        else:
            raise DatabricksConnectionError(
                "No authentication method available. "
                "Please enable OAuth or provide DATABRICKS_TOKEN."
            )

        return kwargs

    def connect(self) -> Connection:
        """Establish a connection to Databricks SQL endpoint.

        Returns:
            Active database connection.

        Raises:
            DatabricksConnectionError: If connection fails.
        """
        if self._connection is not None:
            return self._connection

        try:
            kwargs = self._get_connection_kwargs()
            self._connection = databricks_sql.connect(**kwargs)
            logger.info(f"Connected to Databricks at {self.config.host}")
            return self._connection
        except Exception as e:
            raise DatabricksConnectionError(f"Failed to connect to Databricks: {e}") from e

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            try:
                self._connection.close()
                logger.debug("Databricks connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None

    @contextmanager
    def cursor(self) -> Generator[Cursor, None, None]:
        """Get a cursor for executing queries.

        Yields:
            Database cursor.
        """
        conn = self.connect()
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        """Execute a SQL statement that doesn't return results.

        Args:
            sql: SQL statement to execute.
            params: Optional parameters for parameterized queries.

        Raises:
            DatabricksQueryError: If query execution fails.
        """
        logger.debug(f"Executing SQL: {sql[:200]}...")
        try:
            with self.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
        except Exception as e:
            raise DatabricksQueryError(f"Query execution failed: {e}\nSQL: {sql}") from e

    def fetchone(self, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Execute a query and fetch one result row as a dict.

        Args:
            sql: SQL query to execute.
            params: Optional parameters for parameterized queries.

        Returns:
            Single row as dict, or None if no results.

        Raises:
            DatabricksQueryError: If query execution fails.
        """
        logger.debug(f"Fetching one: {sql[:200]}...")
        try:
            with self.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                row = cur.fetchone()
                if row is None:
                    return None
                # Convert to dict using column descriptions
                columns = [desc[0] for desc in cur.description] if cur.description else []
                return dict(zip(columns, row, strict=False))
        except Exception as e:
            raise DatabricksQueryError(f"Query execution failed: {e}\nSQL: {sql}") from e

    def fetchall(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a query and fetch all result rows as dicts.

        Args:
            sql: SQL query to execute.
            params: Optional parameters for parameterized queries.

        Returns:
            List of rows as dicts.

        Raises:
            DatabricksQueryError: If query execution fails.
        """
        logger.debug(f"Fetching all: {sql[:200]}...")
        try:
            with self.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                rows = cur.fetchall()
                if not rows:
                    return []
                # Convert to dicts using column descriptions
                columns = [desc[0] for desc in cur.description] if cur.description else []
                return [dict(zip(columns, row, strict=False)) for row in rows]
        except Exception as e:
            raise DatabricksQueryError(f"Query execution failed: {e}\nSQL: {sql}") from e

    def __enter__(self) -> DatabricksConnector:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
