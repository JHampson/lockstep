"""Tests for SQL generator."""

from __future__ import annotations

import pytest

from lockstep.models.contract import Column
from lockstep.services.sql_generator import SQLGenerator, SQLInjectionError


class TestSQLGenerator:
    """Tests for SQLGenerator."""

    @pytest.fixture
    def sql_gen(self) -> SQLGenerator:
        """Create SQL generator instance."""
        return SQLGenerator()

    def test_create_table_basic(self, sql_gen: SQLGenerator) -> None:
        """Test basic CREATE TABLE statement."""
        columns = [
            Column(name="id", logicalType="string", required=True),
            Column(name="name", logicalType="string"),
        ]

        sql = sql_gen.create_table("main.default.test", columns)

        assert "CREATE TABLE IF NOT EXISTS `main`.`default`.`test`" in sql
        assert "`id` STRING NOT NULL" in sql
        assert "`name` STRING" in sql
        # name should not have NOT NULL since required=False by default

    def test_create_table_with_description(self, sql_gen: SQLGenerator) -> None:
        """Test CREATE TABLE with description."""
        columns = [
            Column(
                name="id",
                logicalType="string",
                description="Unique identifier",
            ),
        ]

        sql = sql_gen.create_table(
            "main.default.test",
            columns,
            description="Test table",
        )

        assert "COMMENT 'Test table'" in sql
        assert "COMMENT 'Unique identifier'" in sql

    def test_create_table_with_primary_key(self, sql_gen: SQLGenerator) -> None:
        """Test CREATE TABLE with primary key."""
        columns = [
            Column(name="id", logicalType="string", required=True, primaryKey=True),
            Column(name="name", logicalType="string"),
        ]

        sql = sql_gen.create_table(
            "main.default.test",
            columns,
            primary_key_columns=["id"],
        )

        assert "PRIMARY KEY" in sql
        assert "`id`" in sql

    def test_add_column(self, sql_gen: SQLGenerator) -> None:
        """Test ALTER TABLE ADD COLUMN."""
        sql = sql_gen.add_column(
            "main.default.test",
            "new_col",
            "STRING",
            nullable=True,
            description="New column",
        )

        assert "ALTER TABLE `main`.`default`.`test` ADD COLUMN" in sql
        assert "`new_col` STRING" in sql
        assert "COMMENT 'New column'" in sql

    def test_add_column_not_null(self, sql_gen: SQLGenerator) -> None:
        """Test ADD COLUMN with NOT NULL."""
        sql = sql_gen.add_column(
            "main.default.test",
            "required_col",
            "INT",
            nullable=False,
        )

        assert "NOT NULL" in sql

    def test_drop_column(self, sql_gen: SQLGenerator) -> None:
        """Test DROP COLUMN."""
        sql = sql_gen.drop_column("main.default.test", "old_col")
        assert sql == "ALTER TABLE `main`.`default`.`test` DROP COLUMN `old_col`"

    def test_alter_column_type(self, sql_gen: SQLGenerator) -> None:
        """Test ALTER COLUMN TYPE."""
        sql = sql_gen.alter_column_type("main.default.test", "my_col", "BIGINT")
        assert sql == "ALTER TABLE `main`.`default`.`test` ALTER COLUMN `my_col` TYPE BIGINT"

    def test_alter_column_type_with_parameters(self, sql_gen: SQLGenerator) -> None:
        """Test ALTER COLUMN TYPE with parameterized type."""
        sql = sql_gen.alter_column_type("main.default.test", "my_col", "DECIMAL(18,2)")
        assert sql == "ALTER TABLE `main`.`default`.`test` ALTER COLUMN `my_col` TYPE DECIMAL(18,2)"

    def test_update_table_description(self, sql_gen: SQLGenerator) -> None:
        """Test updating table description."""
        sql = sql_gen.update_table_description("main.default.test", "New description")
        assert "SET TBLPROPERTIES" in sql
        assert "'comment'" in sql
        assert "'New description'" in sql

    def test_update_column_description(self, sql_gen: SQLGenerator) -> None:
        """Test updating column description."""
        sql = sql_gen.update_column_description(
            "main.default.test",
            "my_col",
            "Updated description",
        )
        assert "ALTER TABLE `main`.`default`.`test`" in sql
        assert "ALTER COLUMN `my_col`" in sql
        assert "COMMENT 'Updated description'" in sql

    def test_add_primary_key(self, sql_gen: SQLGenerator) -> None:
        """Test adding primary key constraint."""
        sql = sql_gen.add_primary_key(
            "main.default.test",
            ["col1", "col2"],
            "pk_test",
        )
        assert "ADD CONSTRAINT `pk_test`" in sql
        assert "PRIMARY KEY (`col1`, `col2`)" in sql

    def test_drop_primary_key(self, sql_gen: SQLGenerator) -> None:
        """Test dropping primary key constraint."""
        sql = sql_gen.drop_primary_key("main.default.test", "pk_test")
        assert sql == "ALTER TABLE `main`.`default`.`test` DROP CONSTRAINT `pk_test`"

    def test_add_not_null(self, sql_gen: SQLGenerator) -> None:
        """Test adding NOT NULL constraint."""
        sql = sql_gen.add_not_null("main.default.test", "my_col")
        assert "ALTER COLUMN `my_col` SET NOT NULL" in sql

    def test_drop_not_null(self, sql_gen: SQLGenerator) -> None:
        """Test dropping NOT NULL constraint."""
        sql = sql_gen.drop_not_null("main.default.test", "my_col")
        assert "ALTER COLUMN `my_col` DROP NOT NULL" in sql

    def test_set_table_tag(self, sql_gen: SQLGenerator) -> None:
        """Test setting table tag."""
        sql = sql_gen.set_table_tag("main.default.test", "domain", "sales")
        assert "SET TAGS ('domain' = 'sales')" in sql

    def test_remove_table_tag(self, sql_gen: SQLGenerator) -> None:
        """Test removing table tag."""
        sql = sql_gen.remove_table_tag("main.default.test", "domain")
        assert "UNSET TAGS ('domain')" in sql

    def test_set_column_tag(self, sql_gen: SQLGenerator) -> None:
        """Test setting column tag."""
        sql = sql_gen.set_column_tag("main.default.test", "email", "pii", "true")
        assert "ALTER COLUMN `email`" in sql
        assert "SET TAGS ('pii' = 'true')" in sql

    def test_remove_column_tag(self, sql_gen: SQLGenerator) -> None:
        """Test removing column tag."""
        sql = sql_gen.remove_column_tag("main.default.test", "email", "pii")
        assert "ALTER COLUMN `email`" in sql
        assert "UNSET TAGS ('pii')" in sql

    def test_certification_via_tag(self, sql_gen: SQLGenerator) -> None:
        """Test certification is handled via standard tag methods."""
        # Certification is set using regular set_table_tag
        sql = sql_gen.set_table_tag("main.default.test", "system.certification_status", "certified")
        assert "SET TAGS ('system.certification_status' = 'certified')" in sql

        # Certification is cleared using regular remove_table_tag
        sql = sql_gen.remove_table_tag("main.default.test", "system.certification_status")
        assert "UNSET TAGS ('system.certification_status')" in sql

    def test_escape_special_characters(self, sql_gen: SQLGenerator) -> None:
        """Test escaping special characters in strings."""
        sql = sql_gen.update_table_description(
            "main.default.test",
            "Description with 'quotes' and special chars",
        )
        # Single quotes should be escaped
        assert "'Description with ''quotes'' and special chars'" in sql

    def test_escape_identifier_with_backticks(self, sql_gen: SQLGenerator) -> None:
        """Test escaping identifiers that contain backticks."""
        sql = sql_gen.drop_column("main.default.test", "col`name")
        # Backticks should be escaped
        assert "`col``name`" in sql

    def test_grant_permission(self, sql_gen: SQLGenerator) -> None:
        """Test GRANT permission statement."""
        sql = sql_gen.grant_permission("main.default.test", "data_engineers", "SELECT")
        assert sql == "GRANT SELECT ON TABLE `main`.`default`.`test` TO `data_engineers`"

    def test_revoke_permission(self, sql_gen: SQLGenerator) -> None:
        """Test REVOKE permission statement."""
        sql = sql_gen.revoke_permission("main.default.test", "data_engineers", "MODIFY")
        assert sql == "REVOKE MODIFY ON TABLE `main`.`default`.`test` FROM `data_engineers`"


class TestSQLInjectionProtection:
    """Tests for SQL injection protection."""

    @pytest.fixture
    def sql_gen(self) -> SQLGenerator:
        """Create SQL generator instance."""
        return SQLGenerator()

    def test_invalid_table_name_format(self, sql_gen: SQLGenerator) -> None:
        """Test that invalid table name format raises error."""
        with pytest.raises(SQLInjectionError, match="Invalid table name format"):
            sql_gen.drop_column("invalid_table", "col")

        with pytest.raises(SQLInjectionError, match="Invalid table name format"):
            sql_gen.drop_column("only.two", "col")

        with pytest.raises(SQLInjectionError, match="Invalid table name format"):
            sql_gen.drop_column("a.b.c.d", "col")

    def test_table_name_with_injection_attempt(self, sql_gen: SQLGenerator) -> None:
        """Test that injection attempts in table names are rejected."""
        # Semicolons are suspicious
        with pytest.raises(SQLInjectionError, match="Suspicious pattern"):
            sql_gen.drop_column("main.default.users; DROP TABLE x", "col")

        # SQL comment sequences are suspicious
        with pytest.raises(SQLInjectionError, match="Suspicious pattern"):
            sql_gen.drop_column("main.default.users--comment", "col")

        # SQL keywords in identifiers are suspicious
        with pytest.raises(SQLInjectionError, match="Suspicious pattern"):
            sql_gen.drop_column("main.default.DROP", "col")

    def test_column_name_with_injection_attempt(self, sql_gen: SQLGenerator) -> None:
        """Test that injection attempts in column names are rejected."""
        with pytest.raises(SQLInjectionError, match="Suspicious pattern"):
            sql_gen.drop_column("main.default.test", "col; DROP TABLE x")

        with pytest.raises(SQLInjectionError, match="Suspicious pattern"):
            sql_gen.drop_column("main.default.test", "col--comment")

    def test_invalid_data_type(self, sql_gen: SQLGenerator) -> None:
        """Test that invalid data types raise error."""
        with pytest.raises(SQLInjectionError, match="Invalid data type"):
            sql_gen.add_column("main.default.test", "col", "INVALID_TYPE")

        # Injection attempt with semicolons should fail validation
        with pytest.raises(SQLInjectionError, match="Invalid data type"):
            sql_gen.add_column("main.default.test", "col", "STRING; DROP TABLE x; --")

    def test_valid_parameterized_types(self, sql_gen: SQLGenerator) -> None:
        """Test that valid parameterized types work."""
        # These should all work
        sql_gen.add_column("main.default.test", "col", "DECIMAL(10,2)")
        sql_gen.add_column("main.default.test", "col", "VARCHAR(100)")
        sql_gen.add_column("main.default.test", "col", "ARRAY<STRING>")
        sql_gen.add_column("main.default.test", "col", "MAP<STRING, INT>")
        sql_gen.add_column("main.default.test", "col", "STRUCT<name: STRING, age: INT>")

    def test_invalid_privilege(self, sql_gen: SQLGenerator) -> None:
        """Test that invalid privileges raise error."""
        with pytest.raises(SQLInjectionError, match="Invalid privilege"):
            sql_gen.grant_permission("main.default.test", "user", "INVALID")

        with pytest.raises(SQLInjectionError, match="Invalid privilege"):
            sql_gen.grant_permission("main.default.test", "user", "SELECT; DROP TABLE x; --")

    def test_valid_privileges(self, sql_gen: SQLGenerator) -> None:
        """Test that all valid privileges work."""
        valid_privileges = [
            "SELECT",
            "MODIFY",
            "ALL PRIVILEGES",
            "APPLY TAG",
            "READ_METADATA",
        ]
        for priv in valid_privileges:
            sql = sql_gen.grant_permission("main.default.test", "user", priv)
            assert priv in sql

    def test_principal_with_special_characters(self, sql_gen: SQLGenerator) -> None:
        """Test that principals with special characters are escaped."""
        # Email addresses with special chars should be escaped
        sql = sql_gen.grant_permission("main.default.test", "user@example.com", "SELECT")
        assert "`user@example.com`" in sql

        # Group names with backticks should be escaped
        sql = sql_gen.grant_permission("main.default.test", "group`name", "SELECT")
        assert "`group``name`" in sql

    def test_description_injection_escaped(self, sql_gen: SQLGenerator) -> None:
        """Test that injection attempts in descriptions are escaped."""
        sql = sql_gen.update_table_description("main.default.test", "test'; DROP TABLE x; --")
        # Single quotes should be doubled (escaped)
        assert "test''; DROP TABLE x; --" in sql
