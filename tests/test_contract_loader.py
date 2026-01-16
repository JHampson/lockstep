"""Tests for contract loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from lockstep.services.contract_loader import ContractLoader, ContractLoadError


class TestContractLoader:
    """Tests for ContractLoader."""

    def test_load_one_valid(self, tmp_contract_file: Path) -> None:
        """Test loading a single valid contract."""
        loader = ContractLoader()
        contract = loader.load_one(tmp_contract_file)
        assert contract.name == "customer_contract"
        assert len(contract.columns) == 4

    def test_load_one_invalid_yaml(self, tmp_path: Path) -> None:
        """Test loading invalid YAML."""
        invalid_file = tmp_path / "invalid.yaml"
        with open(invalid_file, "w") as f:
            f.write("invalid: yaml: content:")

        loader = ContractLoader()
        with pytest.raises(ContractLoadError) as exc_info:
            loader.load_one(invalid_file)
        assert "parse YAML" in str(exc_info.value)

    def test_load_one_validation_error(self, tmp_path: Path) -> None:
        """Test loading contract with validation errors."""
        invalid_file = tmp_path / "invalid_contract.yaml"
        # Use an invalid status value to trigger validation error
        with open(invalid_file, "w") as f:
            yaml.dump({"name": "test", "status": "not_a_valid_status"}, f)

        loader = ContractLoader()
        with pytest.raises(ContractLoadError) as exc_info:
            loader.load_one(invalid_file)
        assert "validation failed" in str(exc_info.value)
        assert exc_info.value.errors is not None
        assert len(exc_info.value.errors) > 0

    def test_load_many_directory(self, tmp_contracts_dir: Path) -> None:
        """Test loading multiple contracts from directory."""
        loader = ContractLoader()
        contracts = loader.load_many(tmp_contracts_dir)

        # Should load valid contracts
        assert len(contracts) == 2
        contract_names = {c.name for c in contracts}
        assert "customer_contract" in contract_names
        assert "orders_contract" in contract_names

        # Should have validation errors for invalid file
        assert len(loader.validation_errors) == 1

    def test_load_many_single_file(self, tmp_contract_file: Path) -> None:
        """Test load_many with single file."""
        loader = ContractLoader()
        contracts = loader.load_many(tmp_contract_file)
        assert len(contracts) == 1
        assert contracts[0].name == "customer_contract"

    def test_load_multi_document_yaml(
        self, tmp_path: Path, sample_contract_data: dict[str, Any]
    ) -> None:
        """Test loading multi-document YAML."""
        multi_doc_file = tmp_path / "multi.yaml"

        contract1 = sample_contract_data.copy()
        contract2 = sample_contract_data.copy()
        contract2["name"] = "second_contract"
        contract2["dataset"]["table"] = "other_table"

        with open(multi_doc_file, "w") as f:
            yaml.dump_all([contract1, contract2], f)

        loader = ContractLoader()
        contracts = loader.load_many(multi_doc_file)
        assert len(contracts) == 2
        assert contracts[0].name == "customer_contract"
        assert contracts[1].name == "second_contract"

    def test_validate_file_valid(self, tmp_contract_file: Path) -> None:
        """Test validating a valid file."""
        loader = ContractLoader()
        is_valid, errors = loader.validate_file(tmp_contract_file)
        assert is_valid is True
        assert errors == []

    def test_validate_file_invalid(self, tmp_path: Path) -> None:
        """Test validating an invalid file."""
        invalid_file = tmp_path / "invalid.yaml"
        # Use an invalid status value to trigger validation error
        with open(invalid_file, "w") as f:
            yaml.dump({"name": "test", "status": "invalid_status"}, f)

        loader = ContractLoader()
        is_valid, errors = loader.validate_file(invalid_file)
        assert is_valid is False
        assert len(errors) > 0

    def test_format_validation_report_no_errors(self) -> None:
        """Test formatting report with no errors."""
        loader = ContractLoader()
        report = loader.format_validation_report()
        assert "successfully" in report

    def test_format_validation_report_with_errors(self, tmp_contracts_dir: Path) -> None:
        """Test formatting report with errors."""
        loader = ContractLoader()
        loader.load_many(tmp_contracts_dir)

        report = loader.format_validation_report()
        assert "failed" in report.lower() or "error" in report.lower()

    def test_empty_yaml_file(self, tmp_path: Path) -> None:
        """Test loading empty YAML file."""
        empty_file = tmp_path / "empty.yaml"
        with open(empty_file, "w") as f:
            f.write("")

        loader = ContractLoader()
        with pytest.raises(ContractLoadError) as exc_info:
            loader.load_one(empty_file)
        assert "empty" in str(exc_info.value).lower()

    def test_non_yaml_file_skipped(self, tmp_path: Path) -> None:
        """Test that non-YAML files are skipped."""
        txt_file = tmp_path / "readme.txt"
        with open(txt_file, "w") as f:
            f.write("not yaml")

        loader = ContractLoader()
        contracts = loader.load_many(tmp_path)
        assert contracts == []
