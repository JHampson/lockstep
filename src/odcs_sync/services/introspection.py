"""Unity Catalog introspection service.

Fetches current state of tables, columns, constraints, and tags from Unity Catalog.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from odcs_sync.models.catalog_state import CatalogColumn, CatalogConstraint, CatalogTable

if TYPE_CHECKING:
    from odcs_sync.databricks.connector import DatabricksConnector

logger = logging.getLogger(__name__)


class IntrospectionService:
    """Service for introspecting Unity Catalog metadata."""

    # System tag for certification status
    CERTIFICATION_TAG = "system.certification_status"

    def __init__(self, connector: DatabricksConnector) -> None:
        """Initialize the introspection service.

        Args:
            connector: Databricks SQL connector.
        """
        self.connector = connector

    def table_exists(self, full_table_name: str) -> bool:
        """Check if a table exists in Unity Catalog.

        Args:
            full_table_name: Fully qualified table name (catalog.schema.table).

        Returns:
            True if the table exists.
        """
        catalog, schema, table = self._parse_table_name(full_table_name)
        sql = """
            SELECT 1 FROM system.information_schema.tables
            WHERE table_catalog = %(catalog)s
              AND table_schema = %(schema)s
              AND table_name = %(table)s
        """
        result = self.connector.fetchone(
            sql, {"catalog": catalog, "schema": schema, "table": table}
        )
        return result is not None

    def get_table(self, full_table_name: str) -> CatalogTable | None:
        """Get complete table metadata from Unity Catalog.

        Args:
            full_table_name: Fully qualified table name.

        Returns:
            CatalogTable with all metadata, or None if not found.
        """
        catalog, schema, table = self._parse_table_name(full_table_name)

        if not self.table_exists(full_table_name):
            logger.debug(f"Table {full_table_name} does not exist")
            return None

        # Get basic table info
        table_info = self._get_table_info(catalog, schema, table)
        if table_info is None:
            return None

        # Get columns
        columns = self._get_columns(catalog, schema, table)

        # Get constraints
        constraints = self._get_constraints(catalog, schema, table)

        # Get tags
        table_tags, column_tags = self._get_tags(catalog, schema, table)

        # Apply column tags
        for col in columns:
            if col.name in column_tags:
                col.tags = column_tags[col.name]

        # Extract certification status from tags
        certification = table_tags.pop(self.CERTIFICATION_TAG, None)

        return CatalogTable(
            catalog=catalog,
            schema_name=schema,
            table_name=table,
            columns=columns,
            description=table_info.get("comment"),
            tags=table_tags,
            constraints=constraints,
            certification_status=certification,
        )

    def _parse_table_name(self, full_name: str) -> tuple[str, str, str]:
        """Parse a fully qualified table name into components."""
        parts = full_name.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid table name '{full_name}'. Expected format: catalog.schema.table"
            )
        return parts[0], parts[1], parts[2]

    def _get_table_info(
        self, catalog: str, schema: str, table: str
    ) -> dict[str, str | None] | None:
        """Get basic table information."""
        sql = """
            SELECT table_name, comment
            FROM system.information_schema.tables
            WHERE table_catalog = %(catalog)s
              AND table_schema = %(schema)s
              AND table_name = %(table)s
        """
        return self.connector.fetchone(sql, {"catalog": catalog, "schema": schema, "table": table})

    def _get_columns(self, catalog: str, schema: str, table: str) -> list[CatalogColumn]:
        """Get column metadata for a table."""
        sql = """
            SELECT
                column_name,
                full_data_type,
                is_nullable,
                comment
            FROM system.information_schema.columns
            WHERE table_catalog = %(catalog)s
              AND table_schema = %(schema)s
              AND table_name = %(table)s
            ORDER BY ordinal_position
        """
        rows = self.connector.fetchall(sql, {"catalog": catalog, "schema": schema, "table": table})

        columns = []
        for row in rows:
            columns.append(
                CatalogColumn(
                    name=row["column_name"],
                    data_type=row["full_data_type"],
                    nullable=row["is_nullable"] == "YES",
                    description=row.get("comment"),
                )
            )
        return columns

    def _get_constraints(self, catalog: str, schema: str, table: str) -> list[CatalogConstraint]:
        """Get constraints for a table."""
        constraints: list[CatalogConstraint] = []

        # Get primary key constraints
        pk_sql = """
            SELECT
                tc.constraint_name,
                kcu.column_name
            FROM system.information_schema.table_constraints tc
            JOIN system.information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_catalog = kcu.table_catalog
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.table_catalog = %(catalog)s
              AND tc.table_schema = %(schema)s
              AND tc.table_name = %(table)s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        try:
            pk_rows = self.connector.fetchall(
                pk_sql, {"catalog": catalog, "schema": schema, "table": table}
            )

            if pk_rows:
                pk_columns = [row["column_name"] for row in pk_rows]
                constraint_name = pk_rows[0]["constraint_name"]
                constraints.append(
                    CatalogConstraint(
                        name=constraint_name,
                        constraint_type="PRIMARY_KEY",
                        columns=pk_columns,
                    )
                )
        except Exception as e:
            logger.warning(f"Could not fetch primary key constraints: {e}")

        return constraints

    def _get_tags(
        self, catalog: str, schema: str, table: str
    ) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
        """Get tags for a table and its columns.

        Returns:
            Tuple of (table_tags, column_tags).
            column_tags is a dict mapping column name to its tags.
        """
        table_tags: dict[str, str] = {}
        column_tags: dict[str, dict[str, str]] = {}

        full_name = f"{catalog}.{schema}.{table}"

        # Try to get table tags using SHOW TAGS
        try:
            # This query format may vary by Databricks runtime version
            tag_sql = f"SHOW TAGS ON TABLE {full_name}"
            tag_rows = self.connector.fetchall(tag_sql)

            for row in tag_rows:
                tag_name = row.get("tag_name") or row.get("name")
                tag_value = row.get("tag_value") or row.get("value")
                if tag_name and tag_value is not None:
                    table_tags[tag_name] = str(tag_value)

        except Exception as e:
            logger.debug(f"Could not fetch table tags via SHOW TAGS: {e}")
            # Fall back to information_schema if available
            self._get_tags_from_information_schema(catalog, schema, table, table_tags, column_tags)

        # Get column tags
        try:
            for col in self._get_columns(catalog, schema, table):
                col_tag_sql = f"SHOW TAGS ON {full_name}.{col.name}"
                try:
                    col_tag_rows = self.connector.fetchall(col_tag_sql)
                    col_tags: dict[str, str] = {}
                    for row in col_tag_rows:
                        tag_name = row.get("tag_name") or row.get("name")
                        tag_value = row.get("tag_value") or row.get("value")
                        if tag_name and tag_value is not None:
                            col_tags[tag_name] = str(tag_value)
                    if col_tags:
                        column_tags[col.name] = col_tags
                except Exception as e:
                    # Column might not have tags or SHOW TAGS syntax not supported
                    logger.debug(f"Could not fetch tags for column {col.name}: {e}")
        except Exception as e:
            logger.debug(f"Could not fetch column tags: {e}")

        return table_tags, column_tags

    def _get_tags_from_information_schema(
        self,
        catalog: str,
        schema: str,
        table: str,
        table_tags: dict[str, str],
        column_tags: dict[str, dict[str, str]],
    ) -> None:
        """Try to get tags from information_schema (newer Unity Catalog versions)."""
        try:
            sql = """
                SELECT
                    tag_name,
                    tag_value,
                    column_name
                FROM system.information_schema.column_tags
                WHERE catalog_name = %(catalog)s
                  AND schema_name = %(schema)s
                  AND table_name = %(table)s
            """
            rows = self.connector.fetchall(
                sql, {"catalog": catalog, "schema": schema, "table": table}
            )

            for row in rows:
                col_name = row.get("column_name")
                tag_name = row.get("tag_name")
                tag_value = row.get("tag_value")

                if col_name and tag_name:
                    if col_name not in column_tags:
                        column_tags[col_name] = {}
                    column_tags[col_name][tag_name] = str(tag_value)

        except Exception as e:
            # information_schema.column_tags might not exist in older Unity Catalog versions
            logger.debug(f"Could not fetch column tags from information_schema: {e}")

        try:
            sql = """
                SELECT tag_name, tag_value
                FROM system.information_schema.table_tags
                WHERE catalog_name = %(catalog)s
                  AND schema_name = %(schema)s
                  AND table_name = %(table)s
            """
            rows = self.connector.fetchall(
                sql, {"catalog": catalog, "schema": schema, "table": table}
            )

            for row in rows:
                tag_name = row.get("tag_name")
                tag_value = row.get("tag_value")
                if tag_name:
                    table_tags[tag_name] = str(tag_value)

        except Exception as e:
            # information_schema.table_tags might not exist in older Unity Catalog versions
            logger.debug(f"Could not fetch table tags from information_schema: {e}")

    def get_not_null_columns(self, full_table_name: str) -> set[str]:
        """Get columns that have NOT NULL constraints.

        This is tricky in Databricks as NOT NULL is part of column definition,
        not a separate constraint. We check the is_nullable field.

        Args:
            full_table_name: Fully qualified table name.

        Returns:
            Set of column names that are NOT NULL.
        """
        catalog, schema, table = self._parse_table_name(full_table_name)

        sql = """
            SELECT column_name
            FROM system.information_schema.columns
            WHERE table_catalog = %(catalog)s
              AND table_schema = %(schema)s
              AND table_name = %(table)s
              AND is_nullable = 'NO'
        """
        rows = self.connector.fetchall(sql, {"catalog": catalog, "schema": schema, "table": table})
        return {row["column_name"] for row in rows}
