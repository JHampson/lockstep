"""SQL generation for Unity Catalog operations.

Generates Databricks-compatible SQL for DDL and metadata operations.
Includes SQL injection protection for all user-provided values.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lockstep.models.contract import Column

# Valid Databricks SQL data types (base types without parameters)
VALID_BASE_TYPES: frozenset[str] = frozenset(
    {
        "BIGINT",
        "BINARY",
        "BOOLEAN",
        "BOOL",
        "BYTE",
        "DATE",
        "DECIMAL",
        "DOUBLE",
        "FLOAT",
        "INT",
        "INTEGER",
        "INTERVAL",
        "LONG",
        "MAP",
        "NULL",
        "SHORT",
        "SMALLINT",
        "STRING",
        "STRUCT",
        "TEXT",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
        "TINYINT",
        "VARCHAR",
        "VOID",
        "ARRAY",
    }
)

# Valid privileges for GRANT/REVOKE
VALID_PRIVILEGES: frozenset[str] = frozenset(
    {
        "SELECT",
        "MODIFY",
        "ALL PRIVILEGES",
        "APPLY TAG",
        "READ_METADATA",
        "CREATE",
        "USAGE",
        "EXECUTE",
        "READ FILES",
        "WRITE FILES",
        "MANAGE",
    }
)


class SQLInjectionError(ValueError):
    """Raised when a potential SQL injection is detected."""

    pass


class SQLGenerator:
    """Generates SQL statements for Unity Catalog operations.

    All methods include SQL injection protection:
    - Identifiers are escaped with backticks
    - String values have single quotes escaped
    - Table names are validated and each part escaped
    - Data types are validated against an allowlist
    - Privileges are validated against an allowlist
    """

    # Patterns that indicate potential SQL injection attempts
    _SUSPICIOUS_PATTERNS = re.compile(
        r"(--|;|/\*|\*/|\'\'|\"\"|\bDROP\b|\bDELETE\b|\bINSERT\b|\bUPDATE\b|\bEXEC\b|\bUNION\b)",
        re.IGNORECASE,
    )

    def _validate_identifier(self, identifier: str, identifier_type: str = "identifier") -> None:
        """Validate an identifier for suspicious patterns.

        Args:
            identifier: The identifier to validate.
            identifier_type: Type of identifier for error messages (e.g., "table name", "column").

        Raises:
            SQLInjectionError: If suspicious patterns are detected.
        """
        if self._SUSPICIOUS_PATTERNS.search(identifier):
            raise SQLInjectionError(
                f"Suspicious pattern detected in {identifier_type}: '{identifier}'. "
                "Identifiers cannot contain SQL keywords or comment sequences."
            )

    def _escape_identifier(self, identifier: str) -> str:
        """Escape an identifier for use in SQL.

        Uses backticks for Databricks SQL. Handles embedded backticks
        by doubling them. Also validates for suspicious patterns.

        Args:
            identifier: The identifier to escape (column name, table name part, etc.)

        Returns:
            Escaped identifier wrapped in backticks.

        Raises:
            SQLInjectionError: If suspicious patterns are detected.
        """
        self._validate_identifier(identifier)
        # Replace backticks with escaped backticks
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"

    def _escape_string(self, value: str) -> str:
        """Escape a string value for use in SQL.

        Handles single quotes by doubling them.

        Args:
            value: The string value to escape.

        Returns:
            Escaped string (without surrounding quotes).
        """
        return value.replace("'", "''")

    def _escape_table_name(self, full_table_name: str) -> str:
        """Escape a fully qualified table name (catalog.schema.table).

        Splits the name into parts, validates each for suspicious patterns,
        and escapes each identifier separately to prevent SQL injection.

        Args:
            full_table_name: Fully qualified table name (e.g., "catalog.schema.table").

        Returns:
            Escaped table name with each part in backticks.

        Raises:
            SQLInjectionError: If the table name format is invalid or contains
                suspicious patterns.
        """
        parts = full_table_name.split(".")
        if len(parts) != 3:
            raise SQLInjectionError(
                f"Invalid table name format: '{full_table_name}'. "
                "Expected 'catalog.schema.table' format."
            )

        # Validate and escape each part individually
        part_names = ["catalog", "schema", "table"]
        escaped_parts = []
        for part, part_name in zip(parts, part_names, strict=True):
            self._validate_identifier(part, f"{part_name} name")
            escaped_parts.append(self._escape_identifier(part))
        return ".".join(escaped_parts)

    def _validate_data_type(self, data_type: str) -> str:
        """Validate and return a data type string.

        Checks that the base type is in the allowlist of valid Databricks types.
        Allows parameterized types like DECIMAL(10,2), VARCHAR(100), ARRAY<STRING>.

        Args:
            data_type: The data type to validate.

        Returns:
            The validated data type (unchanged if valid).

        Raises:
            SQLInjectionError: If the data type is not valid.
        """
        # Extract base type (before any parentheses or angle brackets)
        base_type_match = re.match(r"^([A-Za-z_]+)", data_type.strip())
        if not base_type_match:
            raise SQLInjectionError(f"Invalid data type format: '{data_type}'")

        base_type = base_type_match.group(1).upper()

        if base_type not in VALID_BASE_TYPES:
            raise SQLInjectionError(
                f"Invalid data type: '{data_type}'. "
                f"Base type '{base_type}' is not a valid Databricks SQL type."
            )

        # Validate that the rest only contains safe characters for type parameters
        # Allow: digits, commas, spaces, colons, parentheses, angle brackets, and type names
        remainder = data_type[len(base_type) :].strip()
        if remainder and not re.match(r"^[\d\s,:.<>()\[\]A-Za-z_]+$", remainder):
            raise SQLInjectionError(
                f"Invalid data type: '{data_type}'. Contains invalid characters."
            )

        return data_type

    def _validate_privilege(self, privilege: str) -> str:
        """Validate a privilege string.

        Args:
            privilege: The privilege to validate.

        Returns:
            The validated privilege (unchanged if valid).

        Raises:
            SQLInjectionError: If the privilege is not valid.
        """
        if privilege.upper() not in VALID_PRIVILEGES:
            raise SQLInjectionError(
                f"Invalid privilege: '{privilege}'. "
                f"Must be one of: {', '.join(sorted(VALID_PRIVILEGES))}"
            )
        return privilege

    def _format_column_def(
        self,
        column: Column,
        include_not_null: bool = True,
        include_comment: bool = True,
    ) -> str:
        """Format a column definition for CREATE TABLE or ALTER TABLE."""
        data_type = self._validate_data_type(column.get_databricks_type())
        parts = [self._escape_identifier(column.name), data_type]

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

        Raises:
            SQLInjectionError: If table name or data types are invalid.
        """
        escaped_table = self._escape_table_name(full_table_name)

        # Format column definitions
        col_defs = [self._format_column_def(col) for col in columns]

        # Add primary key constraint if specified
        if primary_key_columns:
            pk_cols = ", ".join(self._escape_identifier(c) for c in primary_key_columns)
            # Use escaped parts for constraint name
            safe_name = "_".join(part.replace("`", "") for part in escaped_table.split("."))
            col_defs.append(f"CONSTRAINT pk_{safe_name} PRIMARY KEY ({pk_cols})")

        columns_sql = ",\n  ".join(col_defs)

        sql = f"CREATE TABLE IF NOT EXISTS {escaped_table} (\n  {columns_sql}\n)"

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

        Raises:
            SQLInjectionError: If table name or data type is invalid.
        """
        escaped_table = self._escape_table_name(full_table_name)
        validated_type = self._validate_data_type(data_type)
        col_def = f"{self._escape_identifier(column_name)} {validated_type}"

        if not nullable:
            col_def += " NOT NULL"

        if description:
            col_def += f" COMMENT '{self._escape_string(description)}'"

        return f"ALTER TABLE {escaped_table} ADD COLUMN {col_def}"

    def drop_column(self, full_table_name: str, column_name: str) -> str:
        """Generate ALTER TABLE DROP COLUMN statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return f"ALTER TABLE {escaped_table} DROP COLUMN {self._escape_identifier(column_name)}"

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

        Raises:
            SQLInjectionError: If table name or data type is invalid.
        """
        escaped_table = self._escape_table_name(full_table_name)
        validated_type = self._validate_data_type(new_type)
        return (
            f"ALTER TABLE {escaped_table} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} TYPE {validated_type}"
        )

    def update_table_description(self, full_table_name: str, description: str) -> str:
        """Generate ALTER TABLE SET COMMENT statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return f"ALTER TABLE {escaped_table} SET TBLPROPERTIES ('comment' = '{self._escape_string(description)}')"

    def update_column_description(
        self, full_table_name: str, column_name: str, description: str
    ) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET COMMENT statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return (
            f"ALTER TABLE {escaped_table} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} "
            f"COMMENT '{self._escape_string(description)}'"
        )

    def add_primary_key(
        self, full_table_name: str, columns: list[str], constraint_name: str | None = None
    ) -> str:
        """Generate ALTER TABLE ADD CONSTRAINT PRIMARY KEY statement.

        Note: Databricks Unity Catalog primary keys are informational (not enforced).
        """
        escaped_table = self._escape_table_name(full_table_name)
        if not constraint_name:
            # Generate safe constraint name from escaped table parts
            safe_name = "_".join(part.replace("`", "") for part in escaped_table.split("."))
            constraint_name = f"pk_{safe_name}"

        col_list = ", ".join(self._escape_identifier(c) for c in columns)
        return (
            f"ALTER TABLE {escaped_table} "
            f"ADD CONSTRAINT {self._escape_identifier(constraint_name)} "
            f"PRIMARY KEY ({col_list})"
        )

    def drop_primary_key(self, full_table_name: str, constraint_name: str) -> str:
        """Generate ALTER TABLE DROP CONSTRAINT statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return f"ALTER TABLE {escaped_table} DROP CONSTRAINT {self._escape_identifier(constraint_name)}"

    def add_not_null(self, full_table_name: str, column_name: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET NOT NULL statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return (
            f"ALTER TABLE {escaped_table} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} SET NOT NULL"
        )

    def drop_not_null(self, full_table_name: str, column_name: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN DROP NOT NULL statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return (
            f"ALTER TABLE {escaped_table} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} DROP NOT NULL"
        )

    def set_table_tag(self, full_table_name: str, tag_name: str, tag_value: str) -> str:
        """Generate ALTER TABLE SET TAGS statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return (
            f"ALTER TABLE {escaped_table} "
            f"SET TAGS ('{self._escape_string(tag_name)}' = '{self._escape_string(tag_value)}')"
        )

    def remove_table_tag(self, full_table_name: str, tag_name: str) -> str:
        """Generate ALTER TABLE UNSET TAGS statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return f"ALTER TABLE {escaped_table} UNSET TAGS ('{self._escape_string(tag_name)}')"

    def set_column_tag(
        self, full_table_name: str, column_name: str, tag_name: str, tag_value: str
    ) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET TAGS statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return (
            f"ALTER TABLE {escaped_table} "
            f"ALTER COLUMN {self._escape_identifier(column_name)} "
            f"SET TAGS ('{self._escape_string(tag_name)}' = '{self._escape_string(tag_value)}')"
        )

    def remove_column_tag(self, full_table_name: str, column_name: str, tag_name: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN UNSET TAGS statement."""
        escaped_table = self._escape_table_name(full_table_name)
        return (
            f"ALTER TABLE {escaped_table} "
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

        Raises:
            SQLInjectionError: If table name or privilege is invalid.
        """
        escaped_table = self._escape_table_name(full_table_name)
        validated_privilege = self._validate_privilege(privilege)
        # Principals use backticks for identifiers (handles special chars in emails/groups)
        principal_sql = self._escape_identifier(principal)
        return f"GRANT {validated_privilege} ON TABLE {escaped_table} TO {principal_sql}"

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

        Raises:
            SQLInjectionError: If table name or privilege is invalid.
        """
        escaped_table = self._escape_table_name(full_table_name)
        validated_privilege = self._validate_privilege(privilege)
        principal_sql = self._escape_identifier(principal)
        return f"REVOKE {validated_privilege} ON TABLE {escaped_table} FROM {principal_sql}"
