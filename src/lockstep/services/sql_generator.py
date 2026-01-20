"""SQL generation for Unity Catalog operations.

Generates Databricks-compatible SQL for DDL and metadata operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lockstep.models.contract import Column


class SQLGenerator:
    """Generates SQL statements for Unity Catalog operations."""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape an identifier for use in SQL.

        Uses backticks for Databricks SQL.
        """
        # Replace backticks with escaped backticks
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"

    def _escape_string(self, value: str) -> str:
        """Escape a string value for use in SQL."""
        return value.replace("'", "''")

    def _format_column_def(
        self,
        column: Column,
        include_not_null: bool = True,
        include_comment: bool = True,
    ) -> str:
        """Format a column definition for CREATE TABLE or ALTER TABLE."""
        parts = [self._escape_identifier(column.name), column.get_databricks_type()]

        if include_not_null and column.required:
            parts.append("NOT NULL")

        if include_comment and column.description:
            parts.append(f"COMMENT '{self._escape_string(column.description)}'")

        return " ".join(parts)

    def create_table(
        self,
        full_table_name: str,
        columns: list[Column],
        description: str | None = None,
        primary_key_columns: list[str] | None = None,
    ) -> str:
        """Generate CREATE TABLE statement.

        Args:
            full_table_name: Fully qualified table name.
            columns: List of column definitions.
            description: Optional table description.
            primary_key_columns: Optional list of primary key column names.

        Returns:
            CREATE TABLE SQL statement.
        """
        # Format column definitions
        col_defs = [self._format_column_def(col) for col in columns]

        # Add primary key constraint if specified
        if primary_key_columns:
            pk_cols = ", ".join(self._escape_identifier(c) for c in primary_key_columns)
            col_defs.append(
                f"CONSTRAINT pk_{full_table_name.replace('.', '_')} PRIMARY KEY ({pk_cols})"
            )

        columns_sql = ",\n  ".join(col_defs)

        sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n  {columns_sql}\n)"

        if description:
            sql += f"\nCOMMENT '{self._escape_string(description)}'"

        return sql

    def add_column(
        self,
        full_table_name: str,
        column_name: str,
        data_type: str,
        nullable: bool = True,
        description: str | None = None,
    ) -> str:
        """Generate ALTER TABLE ADD COLUMN statement.

        Args:
            full_table_name: Fully qualified table name.
            column_name: Name of the column to add.
            data_type: Databricks SQL data type.
            nullable: Whether the column is nullable.
            description: Optional column description.

        Returns:
            ALTER TABLE ADD COLUMN SQL statement.
        """
        col_def = f"{self._escape_identifier(column_name)} {data_type}"

        if not nullable:
            col_def += " NOT NULL"

        if description:
            col_def += f" COMMENT '{self._escape_string(description)}'"

        return f"ALTER TABLE {full_table_name} ADD COLUMN {col_def}"

    def drop_column(self, full_table_name: str, column_name: str) -> str:
        """Generate ALTER TABLE DROP COLUMN statement."""
        return f"ALTER TABLE {full_table_name} DROP COLUMN {self._escape_identifier(column_name)}"

    def alter_column_type(self, full_table_name: str, column_name: str, new_type: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN TYPE statement.

        Note: Not all type changes are safe. Narrowing types or incompatible
        conversions may fail or cause data loss.

        Args:
            full_table_name: Fully qualified table name.
            column_name: Name of the column to alter.
            new_type: New Databricks SQL data type.

        Returns:
            ALTER COLUMN TYPE SQL statement.
        """
        return (
            f"ALTER TABLE {full_table_name} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} TYPE {new_type}"
        )

    def update_table_description(self, full_table_name: str, description: str) -> str:
        """Generate ALTER TABLE SET COMMENT statement."""
        return f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '{self._escape_string(description)}')"

    def update_column_description(
        self, full_table_name: str, column_name: str, description: str
    ) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET COMMENT statement."""
        return (
            f"ALTER TABLE {full_table_name} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} "
            f"COMMENT '{self._escape_string(description)}'"
        )

    def add_primary_key(
        self, full_table_name: str, columns: list[str], constraint_name: str | None = None
    ) -> str:
        """Generate ALTER TABLE ADD CONSTRAINT PRIMARY KEY statement.

        Note: Databricks Unity Catalog primary keys are informational (not enforced).
        """
        if not constraint_name:
            constraint_name = f"pk_{full_table_name.replace('.', '_')}"

        col_list = ", ".join(self._escape_identifier(c) for c in columns)
        return (
            f"ALTER TABLE {full_table_name} "
            f"ADD CONSTRAINT {self._escape_identifier(constraint_name)} "
            f"PRIMARY KEY ({col_list})"
        )

    def drop_primary_key(self, full_table_name: str, constraint_name: str) -> str:
        """Generate ALTER TABLE DROP CONSTRAINT statement."""
        return f"ALTER TABLE {full_table_name} DROP CONSTRAINT {self._escape_identifier(constraint_name)}"

    def add_not_null(self, full_table_name: str, column_name: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET NOT NULL statement."""
        return (
            f"ALTER TABLE {full_table_name} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} SET NOT NULL"
        )

    def drop_not_null(self, full_table_name: str, column_name: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN DROP NOT NULL statement."""
        return (
            f"ALTER TABLE {full_table_name} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} DROP NOT NULL"
        )

    def set_table_tag(self, full_table_name: str, tag_name: str, tag_value: str) -> str:
        """Generate ALTER TABLE SET TAGS statement."""
        return (
            f"ALTER TABLE {full_table_name} "
            f"SET TAGS ('{self._escape_string(tag_name)}' = '{self._escape_string(tag_value)}')"
        )

    def remove_table_tag(self, full_table_name: str, tag_name: str) -> str:
        """Generate ALTER TABLE UNSET TAGS statement."""
        return f"ALTER TABLE {full_table_name} UNSET TAGS ('{self._escape_string(tag_name)}')"

    def set_column_tag(
        self, full_table_name: str, column_name: str, tag_name: str, tag_value: str
    ) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET TAGS statement."""
        return (
            f"ALTER TABLE {full_table_name} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} "
            f"SET TAGS ('{self._escape_string(tag_name)}' = '{self._escape_string(tag_value)}')"
        )

    def remove_column_tag(self, full_table_name: str, column_name: str, tag_name: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN UNSET TAGS statement."""
        return (
            f"ALTER TABLE {full_table_name} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} "
            f"UNSET TAGS ('{self._escape_string(tag_name)}')"
        )

    def grant_permission(
        self,
        full_table_name: str,
        principal: str,
        privilege: str,
    ) -> str:
        """Generate GRANT statement for table permission.

        Args:
            full_table_name: Fully qualified table name.
            principal: User email or group name.
            privilege: Permission to grant (SELECT, MODIFY, ALL PRIVILEGES, etc.).

        Returns:
            GRANT SQL statement.
        """
        principal_sql = f"`{self._escape_string(principal)}`"
        return f"GRANT {privilege} ON TABLE {full_table_name} TO {principal_sql}"

    def revoke_permission(
        self,
        full_table_name: str,
        principal: str,
        privilege: str,
    ) -> str:
        """Generate REVOKE statement for table permission.

        Args:
            full_table_name: Fully qualified table name.
            principal: User email or group name.
            privilege: Permission to revoke (SELECT, MODIFY, ALL PRIVILEGES, etc.).

        Returns:
            REVOKE SQL statement.
        """
        principal_sql = f"`{self._escape_string(principal)}`"
        return f"REVOKE {privilege} ON TABLE {full_table_name} FROM {principal_sql}"
