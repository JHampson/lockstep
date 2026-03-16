"""Databricks SQL connector with context manager support.

Provides a clean interface for executing SQL against Databricks SQL endpoints.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from databricks import sql as databricks_sql

from lockstep.databricks.config import AuthType, DatabricksConfig, is_databricks_runtime

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor

logger = logging.getLogger(__name__)

# Default connection timeout in seconds
CONNECTION_TIMEOUT = 30


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
            missing = []
            if not config.host:
                missing.append("host")
            if not config.http_path:
                missing.append("sql-endpoint")
            if config.auth_type == AuthType.PAT and not config.token:
                missing.append("token (required for auth-type=pat)")
            if config.auth_type == AuthType.SP and not config.client_id:
                missing.append("client-id (required for auth-type=sp)")
            if config.auth_type == AuthType.SP and not config.client_secret:
                missing.append("client-secret (required for auth-type=sp)")

            raise DatabricksConnectionError(
                f"Databricks configuration is incomplete. Missing: {', '.join(missing)}"
            )

        self.config = config
        self._connection: Connection | None = None

    def _get_connection_kwargs(self) -> dict[str, Any]:
        """Build connection keyword arguments based on config."""
        kwargs: dict[str, Any] = {
            "server_hostname": self.config.host.replace("https://", "").replace("http://", ""),
            "http_path": self.config.http_path,
            "_socket_timeout": 30,  # Connection timeout in seconds
        }

        auth_type = self.config.auth_type

        if auth_type == AuthType.RUNTIME:
            # Databricks runtime authentication (Job or Notebook)
            self._authenticate_runtime(kwargs)
        elif auth_type == AuthType.SP:
            # Service Principal / OAuth M2M authentication
            self._authenticate_service_principal(kwargs)
        elif auth_type == AuthType.PAT:
            # Personal Access Token authentication
            self._authenticate_token(kwargs)
        else:  # AuthType.OAUTH
            # OAuth authentication (Databricks CLI, Azure CLI, etc.)
            self._authenticate_oauth(kwargs)

        return kwargs

    def _authenticate_runtime(self, kwargs: dict[str, Any]) -> None:
        """Authenticate using Databricks runtime native credentials.

        When running inside a Databricks Job or Notebook, the SDK can resolve
        credentials automatically via environment variables set by the runtime
        (DATABRICKS_HOST and DATABRICKS_TOKEN).
        """
        import os

        token = os.getenv("DATABRICKS_TOKEN")
        if token:
            kwargs["access_token"] = token
            logger.debug("Using Databricks runtime authentication (env token)")
            return

        # Fallback: use the SDK Config with no host to let it auto-detect runtime
        try:
            from databricks.sdk.core import Config

            sdk_config = Config()
            auth_headers = sdk_config.authenticate()
            auth_header = auth_headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                kwargs["access_token"] = auth_header[7:]
                logger.debug("Using Databricks runtime authentication (SDK auto-detect)")
            else:
                raise DatabricksConnectionError(
                    "Databricks runtime authentication returned unexpected format. "
                    f"Got: {list(auth_headers.keys())}"
                )
        except DatabricksConnectionError:
            raise
        except Exception as e:
            raise DatabricksConnectionError(
                f"Databricks runtime authentication failed: {e}. "
                "Ensure DATABRICKS_TOKEN is set or the runtime provides credentials."
            ) from e

    def _authenticate_service_principal(self, kwargs: dict[str, Any]) -> None:
        """Authenticate using Service Principal (OAuth M2M)."""
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
                logger.debug("Using Service Principal authentication")
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
        except DatabricksConnectionError:
            raise
        except Exception as e:
            raise DatabricksConnectionError(f"Service Principal authentication failed: {e}") from e

    def _authenticate_token(self, kwargs: dict[str, Any]) -> None:
        """Authenticate using Personal Access Token."""
        if not self.config.token:
            raise DatabricksConnectionError(
                "Personal Access Token required for auth-type=pat. "
                "Please set DATABRICKS_TOKEN or use --token."
            )
        kwargs["access_token"] = self.config.token
        logger.debug("Using Personal Access Token authentication")

    def _authenticate_oauth(self, kwargs: dict[str, Any]) -> None:
        """Authenticate using OAuth (Databricks CLI, Azure CLI, etc.)."""
        try:
            from databricks.sdk.core import Config

            # Create SDK config - it will use Azure CLI, Databricks CLI, env vars, etc.
            sdk_config = Config(host=self.config.host)

            # Get the auth headers and extract the token
            auth_headers = sdk_config.authenticate()
            auth_header = auth_headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]  # Remove "Bearer " prefix
                kwargs["access_token"] = token
                logger.debug("Using OAuth authentication")
            else:
                raise DatabricksConnectionError(
                    "OAuth authentication returned unexpected format. "
                    f"Got: {list(auth_headers.keys())}"
                )
        except ImportError as e:
            raise DatabricksConnectionError(
                "OAuth authentication requires databricks-sdk. "
                "Please install it with: pip install databricks-sdk"
            ) from e
        except DatabricksConnectionError:
            raise
        except Exception as e:
            raise DatabricksConnectionError(f"OAuth authentication failed: {e}") from e

    def connect(self) -> Connection:
        """Establish a connection to Databricks SQL endpoint.

        Returns:
            Active database connection.

        Raises:
            DatabricksConnectionError: If connection fails or times out.
        """
        if self._connection is not None:
            return self._connection

        try:
            kwargs = self._get_connection_kwargs()
            self._connection = self._connect_with_timeout(kwargs)
            logger.info(f"Connected to Databricks at {self.config.host}")
            return self._connection
        except DatabricksConnectionError:
            raise
        except Exception as e:
            raise DatabricksConnectionError(f"Failed to connect to Databricks: {e}") from e

    def _connect_with_timeout(self, kwargs: dict[str, Any]) -> Connection:
        """Connect with a timeout to prevent hanging on invalid endpoints.

        Uses signal-based timeout on Unix, threading-based on Windows.
        """
        timeout = (
            self.config.timeout_seconds if self.config.timeout_seconds < 60 else CONNECTION_TIMEOUT
        )

        # Use threading-based timeout (works on all platforms)
        result: list[Connection | Exception] = []

        def connect_thread() -> None:
            try:
                conn = databricks_sql.connect(**kwargs)
                result.append(conn)
            except Exception as e:
                result.append(e)

        thread = threading.Thread(target=connect_thread, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            # Connection is still trying - it's hanging
            raise DatabricksConnectionError(
                f"Connection timed out after {timeout} seconds. "
                f"Please verify the SQL endpoint ID is correct: {self.config.http_path}"
            )

        if not result:
            raise DatabricksConnectionError("Connection failed with no response")

        if isinstance(result[0], Exception):
            raise result[0]

        return result[0]

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
