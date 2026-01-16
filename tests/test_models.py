"""Tests for Pydantic models."""

from __future__ import annotations

from typing import Any

import pytest

from lockstep.models.contract import (
    Column,
    Contract,
    ContractSchema,
    ContractStatus,
    TableInfo,
)


class TestColumn:
    """Tests for Column model."""

    def test_basic_column(self) -> None:
        """Test basic column creation."""
        col = Column(name="test", logicalType="string")
        assert col.name == "test"
        assert col.logical_type == "string"
        assert col.required is False
        assert col.primary_key is False
        assert col.tags == {}

    def test_column_with_all_fields(self) -> None:
        """Test column with all fields populated."""
        col = Column(
            name="customer_id",
            logicalType="STRING",  # Should be normalized to lowercase
            physicalType="VARCHAR(100)",
            description="Unique customer ID",
            required=True,
            primaryKey=True,
            tags={"pii": "false"},
        )
        assert col.name == "customer_id"
        assert col.logical_type == "string"
        assert col.physical_type == "VARCHAR(100)"
        assert col.required is True
        assert col.primary_key is True
        assert col.tags == {"pii": "false"}

    def test_get_databricks_type_logical(self) -> None:
        """Test Databricks type mapping from logical type."""
        test_cases = [
            ("string", "STRING"),
            ("integer", "INT"),
            ("long", "BIGINT"),
            ("double", "DOUBLE"),
            ("boolean", "BOOLEAN"),
            ("timestamp", "TIMESTAMP"),
            ("date", "DATE"),
        ]
        for logical_type, expected in test_cases:
            col = Column(name="test", logicalType=logical_type)
            assert col.get_databricks_type() == expected

    def test_get_databricks_type_physical_override(self) -> None:
        """Test that physical type overrides logical type."""
        col = Column(
            name="test",
            logicalType="string",
            physicalType="VARCHAR(255)",
        )
        assert col.get_databricks_type() == "VARCHAR(255)"


class TestContractSchema:
    """Tests for ContractSchema model."""

    def test_schema_with_list(self) -> None:
        """Test schema parsing with list of columns."""
        schema = ContractSchema(
            properties=[
                {"name": "col1", "logicalType": "string"},
                {"name": "col2", "logicalType": "integer"},
            ]
        )
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "col1"
        assert schema.columns[1].name == "col2"

    def test_schema_with_dict(self) -> None:
        """Test schema parsing with dict format (column name as key)."""
        schema = ContractSchema(
            properties={
                "col1": {"logicalType": "string"},
                "col2": {"logicalType": "integer"},
            }
        )
        assert len(schema.columns) == 2
        # Dict order is preserved in Python 3.7+
        col_names = {col.name for col in schema.columns}
        assert col_names == {"col1", "col2"}


class TestTableInfo:
    """Tests for TableInfo model."""

    def test_table_info(self) -> None:
        """Test table info creation."""
        info = TableInfo(catalog="main", schema="sales", table="customers")
        assert info.catalog == "main"
        assert info.schema_name == "sales"
        assert info.table == "customers"
        assert info.full_name == "main.sales.customers"


class TestContract:
    """Tests for Contract model."""

    def test_minimal_contract(self) -> None:
        """Test contract with minimal required fields."""
        data = {
            "name": "test_contract",
            "dataset": {
                "catalog": "main",
                "schema": "default",
                "table": "test",
            },
            "schema": {"properties": []},
        }
        contract = Contract.model_validate(data)
        assert contract.name == "test_contract"
        assert contract.status == ContractStatus.DRAFT
        assert contract.columns == []
        assert contract.tags == {}

    def test_full_contract(self, sample_contract_data: dict[str, Any]) -> None:
        """Test contract with all fields populated."""
        contract = Contract.model_validate(sample_contract_data)
        assert contract.name == "customer_contract"
        assert contract.version == "1.0.0"
        assert contract.status == ContractStatus.ACTIVE
        assert contract.description == "Customer data contract"
        assert len(contract.columns) == 4
        # Certification is now handled via tags
        assert contract.tags["system.certification_status"] == "certified"
        assert contract.tags["domain"] == "sales"

    def test_primary_key_columns(self, sample_contract_data: dict[str, Any]) -> None:
        """Test primary key column extraction."""
        contract = Contract.model_validate(sample_contract_data)
        assert contract.primary_key_columns == ["customer_id"]

    def test_get_full_table_name(self, sample_contract_data: dict[str, Any]) -> None:
        """Test full table name generation."""
        contract = Contract.model_validate(sample_contract_data)
        assert contract.get_full_table_name() == "main.sales.customers"

    def test_get_full_table_name_with_overrides(self, sample_contract_data: dict[str, Any]) -> None:
        """Test full table name with overrides."""
        contract = Contract.model_validate(sample_contract_data)
        assert (
            contract.get_full_table_name(
                catalog_override="dev",
                schema_override="staging",
                table_prefix="test_",
            )
            == "dev.staging.test_customers"
        )

    def test_missing_required_field(self) -> None:
        """Test that contract without table info fails at runtime."""
        # With ODCS v3 support, dataset is optional (can use servers instead)
        # But accessing table_info when neither dataset nor servers is set should fail
        contract = Contract.model_validate({"name": "test", "schema": {"properties": []}})
        with pytest.raises(ValueError, match="No table info available"):
            _ = contract.table_info

    def test_extra_fields_allowed(self) -> None:
        """Test that extra fields are allowed (for extensibility)."""
        data = {
            "name": "test",
            "dataset": {"catalog": "main", "schema": "default", "table": "test"},
            "schema": {"properties": []},
            "custom_field": "custom_value",
            "data_quality": {"rules": []},
        }
        contract = Contract.model_validate(data)
        assert contract.name == "test"
        # Extra fields should be accessible via model_extra

    def test_odcs_v3_format(self, sample_contract_data_v3: dict[str, Any]) -> None:
        """Test ODCS v3 format with servers and schema array."""
        contract = Contract.model_validate(sample_contract_data_v3)

        assert contract.name == "customer_contract"
        assert contract.id == "customer-contract"
        # Description should be extracted from object format
        assert contract.description == "Customer data contract for sales analytics"

        # Table info should be derived from servers + schema
        assert contract.table_info.catalog == "main"
        assert contract.table_info.schema_name == "sales"
        assert contract.table_info.table == "customers"
        assert contract.get_full_table_name() == "main.sales.customers"

        # Columns should be extracted from schema array's properties
        assert len(contract.columns) == 3
        assert contract.columns[0].name == "customer_id"
        assert contract.columns[2].name == "total_spent"
        assert contract.columns[2].logical_type == "number"

        # Tags should be parsed from array format
        assert contract.tags["domain"] == "sales"
        assert contract.tags["team"] == "customer-success"
        assert contract.tags["system.certification_status"] == "certified"

    def test_odcs_v3_number_type_mapping(self) -> None:
        """Test ODCS v3 'number' type maps to DOUBLE."""
        col = Column(name="amount", logicalType="number")
        assert col.get_databricks_type() == "DOUBLE"

    def test_odcs_v3_tags_array_format(self) -> None:
        """Test tags parsing from ODCS v3 array format."""
        from lockstep.models.contract import parse_tags

        tags = parse_tags(["domain:sales", "pii:true", "classification:internal"])
        assert tags == {
            "domain": "sales",
            "pii": "true",
            "classification": "internal",
        }

    def test_odcs_v3_tags_without_value(self) -> None:
        """Test tags array with tags that have no value."""
        from lockstep.models.contract import parse_tags

        tags = parse_tags(["archived", "domain:sales"])
        assert tags == {
            "archived": "",
            "domain": "sales",
        }

    def test_odcs_v3_description_object_format(self) -> None:
        """Test description parsing from ODCS v3 object format."""
        data = {
            "name": "test",
            "dataset": {"catalog": "main", "schema": "default", "table": "test"},
            "schema": {"properties": []},
            "description": {"usage": "This is the usage description"},
        }
        contract = Contract.model_validate(data)
        assert contract.description == "This is the usage description"

    def test_odcs_v3_logical_type_options(self) -> None:
        """Test logicalTypeOptions are preserved on columns."""
        col = Column(
            name="score",
            logicalType="number",
            logicalTypeOptions={"minimum": 0, "maximum": 100},
        )
        assert col.logical_type_options is not None
        assert col.logical_type_options["minimum"] == 0
        assert col.logical_type_options["maximum"] == 100
