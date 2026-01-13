"""Tests for introspection service."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lockstep.models.catalog_state import CatalogTable
from lockstep.services.introspection import IntrospectionService


@pytest.fixture
def mock_connector() -> MagicMock:
    """Create a mock Databricks connector."""
    return MagicMock()


@pytest.fixture
def introspection_service(mock_connector: MagicMock) -> IntrospectionService:
    """Create an introspection service with a mock connector."""
    return IntrospectionService(mock_connector)


class TestIntrospectionService:
    """Tests for IntrospectionService."""

    def test_table_exists_true(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test table_exists returns True when table exists."""
        mock_connector.fetchone.return_value = {"1": 1}

        result = introspection_service.table_exists("catalog.schema.table")

        assert result is True
        mock_connector.fetchone.assert_called_once()

    def test_table_exists_false(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test table_exists returns False when table doesn't exist."""
        mock_connector.fetchone.return_value = None

        result = introspection_service.table_exists("catalog.schema.table")

        assert result is False

    def test_get_table_returns_none_when_not_exists(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test get_table returns None when table doesn't exist."""
        mock_connector.fetchone.return_value = None

        result = introspection_service.get_table("catalog.schema.table")

        assert result is None

    def test_get_table_returns_catalog_table(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test get_table returns CatalogTable with metadata."""
        # Mock table exists
        mock_connector.fetchone.side_effect = [
            {"1": 1},  # table_exists check
            {"table_name": "table", "comment": "Test table description"},  # _get_table_info
        ]

        # Mock columns
        mock_connector.fetchall.side_effect = [
            # _get_columns
            [
                {
                    "column_name": "id",
                    "full_data_type": "STRING",
                    "is_nullable": "NO",
                    "comment": "Primary key",
                },
                {
                    "column_name": "name",
                    "full_data_type": "STRING",
                    "is_nullable": "YES",
                    "comment": None,
                },
            ],
            # _get_constraints (PK query)
            [{"constraint_name": "pk_table", "column_name": "id"}],
            # _get_tags (SHOW TAGS ON TABLE) - simulate error fallback
            [],
            # _get_column_tags_from_information_schema
            [],
            # _get_table_tags_from_information_schema (fallback)
            [],
        ]

        result = introspection_service.get_table("catalog.schema.table")

        assert result is not None
        assert isinstance(result, CatalogTable)
        assert result.catalog == "catalog"
        assert result.schema_name == "schema"
        assert result.table_name == "table"
        assert result.description == "Test table description"
        assert len(result.columns) == 2
        assert result.columns[0].name == "id"
        assert result.columns[0].nullable is False
        assert result.columns[1].name == "name"
        assert result.columns[1].nullable is True

    def test_parse_table_name_valid(self, introspection_service: IntrospectionService) -> None:
        """Test _parse_table_name with valid format."""
        catalog, schema, table = introspection_service._parse_table_name(
            "my_catalog.my_schema.my_table"
        )
        assert catalog == "my_catalog"
        assert schema == "my_schema"
        assert table == "my_table"

    def test_parse_table_name_invalid(self, introspection_service: IntrospectionService) -> None:
        """Test _parse_table_name raises error for invalid format."""
        with pytest.raises(ValueError, match="Invalid table name"):
            introspection_service._parse_table_name("invalid_name")

    def test_parse_table_name_too_many_parts(
        self, introspection_service: IntrospectionService
    ) -> None:
        """Test _parse_table_name raises error for too many parts."""
        with pytest.raises(ValueError, match="Invalid table name"):
            introspection_service._parse_table_name("a.b.c.d")

    def test_get_columns(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test _get_columns returns list of CatalogColumn."""
        mock_connector.fetchall.return_value = [
            {
                "column_name": "col1",
                "full_data_type": "INT",
                "is_nullable": "YES",
                "comment": "Column 1",
            },
            {
                "column_name": "col2",
                "full_data_type": "VARCHAR(100)",
                "is_nullable": "NO",
                "comment": None,
            },
        ]

        columns = introspection_service._get_columns("catalog", "schema", "table")

        assert len(columns) == 2
        assert columns[0].name == "col1"
        assert columns[0].data_type == "INT"
        assert columns[0].nullable is True
        assert columns[0].description == "Column 1"
        assert columns[1].name == "col2"
        assert columns[1].nullable is False

    def test_get_constraints_with_primary_key(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test _get_constraints returns primary key constraint."""
        mock_connector.fetchall.return_value = [
            {"constraint_name": "pk_test", "column_name": "id"},
            {"constraint_name": "pk_test", "column_name": "version"},
        ]

        constraints = introspection_service._get_constraints("catalog", "schema", "table")

        assert len(constraints) == 1
        assert constraints[0].name == "pk_test"
        assert constraints[0].constraint_type == "PRIMARY_KEY"
        assert constraints[0].columns == ["id", "version"]

    def test_get_constraints_no_primary_key(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test _get_constraints returns empty list when no PK."""
        mock_connector.fetchall.return_value = []

        constraints = introspection_service._get_constraints("catalog", "schema", "table")

        assert len(constraints) == 0

    def test_get_not_null_columns(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test get_not_null_columns returns set of column names."""
        mock_connector.fetchall.return_value = [
            {"column_name": "id"},
            {"column_name": "created_at"},
        ]

        result = introspection_service.get_not_null_columns("catalog.schema.table")

        assert result == {"id", "created_at"}


class TestTagFetching:
    """Tests for tag fetching methods."""

    def test_get_column_tags_from_information_schema(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test fetching column tags from information_schema."""
        mock_connector.fetchall.return_value = [
            {"column_name": "email", "tag_name": "pii", "tag_value": "true"},
            {"column_name": "email", "tag_name": "classification", "tag_value": "sensitive"},
            {"column_name": "id", "tag_name": "pii", "tag_value": "false"},
        ]

        column_tags: dict[str, dict[str, str]] = {}
        introspection_service._get_column_tags_from_information_schema(
            "catalog", "schema", "table", column_tags
        )

        assert "email" in column_tags
        assert column_tags["email"]["pii"] == "true"
        assert column_tags["email"]["classification"] == "sensitive"
        assert "id" in column_tags
        assert column_tags["id"]["pii"] == "false"

    def test_get_table_tags_from_information_schema(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test fetching table tags from information_schema."""
        mock_connector.fetchall.return_value = [
            {"tag_name": "domain", "tag_value": "sales"},
            {"tag_name": "team", "tag_value": "analytics"},
        ]

        table_tags: dict[str, str] = {}
        introspection_service._get_table_tags_from_information_schema(
            "catalog", "schema", "table", table_tags
        )

        assert table_tags["domain"] == "sales"
        assert table_tags["team"] == "analytics"

    def test_tag_fetching_handles_errors_gracefully(
        self, introspection_service: IntrospectionService, mock_connector: MagicMock
    ) -> None:
        """Test that tag fetching handles errors without raising."""
        mock_connector.fetchall.side_effect = Exception("Query failed")

        column_tags: dict[str, dict[str, str]] = {}
        # Should not raise
        introspection_service._get_column_tags_from_information_schema(
            "catalog", "schema", "table", column_tags
        )

        assert column_tags == {}
