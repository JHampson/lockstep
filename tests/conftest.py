"""Pytest configuration and fixtures."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from odcs_sync.models.catalog_state import CatalogColumn, CatalogConstraint, CatalogTable
from odcs_sync.models.contract import Contract


@pytest.fixture
def sample_contract_data() -> dict[str, Any]:
    """Sample ODCS contract data for testing."""
    return {
        "apiVersion": "v3.0.0",
        "kind": "DataContract",
        "name": "customer_contract",
        "version": "1.0.0",
        "status": "active",
        "description": "Customer data contract",
        "dataset": {
            "catalog": "main",
            "schema": "sales",
            "table": "customers",
        },
        "schema": {
            "properties": [
                {
                    "name": "customer_id",
                    "logicalType": "string",
                    "description": "Unique customer identifier",
                    "required": True,
                    "primaryKey": True,
                    "tags": {"pii": "false"},
                },
                {
                    "name": "email",
                    "logicalType": "string",
                    "description": "Customer email address",
                    "required": True,
                    "tags": {"pii": "true", "classification": "sensitive"},
                },
                {
                    "name": "created_at",
                    "logicalType": "timestamp",
                    "description": "Account creation timestamp",
                    "required": False,
                },
                {
                    "name": "total_orders",
                    "logicalType": "integer",
                    "description": "Total number of orders",
                    "required": False,
                },
            ]
        },
        "tags": {"domain": "sales", "team": "customer-success"},
        "certification": "certified",
    }


@pytest.fixture
def sample_contract(sample_contract_data: dict[str, Any]) -> Contract:
    """Sample parsed contract for testing."""
    return Contract.model_validate(sample_contract_data)


@pytest.fixture
def sample_catalog_table() -> CatalogTable:
    """Sample catalog table state for testing."""
    return CatalogTable(
        catalog="main",
        schema_name="sales",
        table_name="customers",
        columns=[
            CatalogColumn(
                name="customer_id",
                data_type="STRING",
                nullable=False,
                description="Unique customer identifier",
                tags={"pii": "false"},
            ),
            CatalogColumn(
                name="email",
                data_type="STRING",
                nullable=False,
                description="Customer email",  # Different from contract
                tags={"pii": "true"},  # Missing classification tag
            ),
            CatalogColumn(
                name="old_column",  # Column not in contract
                data_type="STRING",
                nullable=True,
            ),
        ],
        description="Customer data",  # Different from contract
        tags={"domain": "sales", "legacy": "true"},  # Has extra tag, missing team
        constraints=[
            CatalogConstraint(
                name="pk_customers",
                constraint_type="PRIMARY_KEY",
                columns=["customer_id"],
            )
        ],
        certification_status="certified",
    )


@pytest.fixture
def tmp_contract_file(tmp_path: Path, sample_contract_data: dict[str, Any]) -> Path:
    """Create a temporary contract YAML file."""
    contract_file = tmp_path / "contract.yaml"
    with open(contract_file, "w") as f:
        yaml.dump(sample_contract_data, f)
    return contract_file


@pytest.fixture
def tmp_contracts_dir(tmp_path: Path, sample_contract_data: dict[str, Any]) -> Path:
    """Create a temporary directory with multiple contract files."""
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()

    # Create valid contract
    with open(contracts_dir / "valid.yaml", "w") as f:
        yaml.dump(sample_contract_data, f)

    # Create another valid contract with different data
    contract2 = sample_contract_data.copy()
    contract2["name"] = "orders_contract"
    contract2["dataset"] = {
        "catalog": "main",
        "schema": "sales",
        "table": "orders",
    }
    with open(contracts_dir / "orders.yaml", "w") as f:
        yaml.dump(contract2, f)

    # Create invalid contract
    with open(contracts_dir / "invalid.yaml", "w") as f:
        yaml.dump({"name": "missing fields"}, f)

    return contracts_dir
