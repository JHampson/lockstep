"""Tests for SQL generator."""

from __future__ import annotations

import pytest

from odcs_sync.models.contract import Column
from odcs_sync.services.sql_generator import SQLGenerator


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

        assert "CREATE TABLE IF NOT EXISTS main.default.test" in sql
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

        assert "ALTER TABLE main.default.test ADD COLUMN" in sql
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
        assert sql == "ALTER TABLE main.default.test DROP COLUMN `old_col`"

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
        assert "ALTER TABLE main.default.test" in sql
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
        assert sql == "ALTER TABLE main.default.test DROP CONSTRAINT `pk_test`"

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

    def test_set_certification(self, sql_gen: SQLGenerator) -> None:
        """Test setting certification status."""
        sql = sql_gen.set_certification("main.default.test", "certified")
        assert "SET TAGS ('system.certification_status' = 'certified')" in sql

    def test_clear_certification(self, sql_gen: SQLGenerator) -> None:
        """Test clearing certification status."""
        sql = sql_gen.clear_certification("main.default.test")
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
