"""Tests for JUnit XML reporter."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from lockstep.cli.junit_reporter import (
    generate_junit_xml,
    generate_validation_junit_xml,
)
from lockstep.models.catalog_state import ActionType, SyncAction, SyncPlan
from lockstep.services.contract_loader import ContractLoadError
from lockstep.services.sync import SyncResult


class TestGenerateJunitXml:
    """Tests for generate_junit_xml function."""

    def test_empty_results(self) -> None:
        """Test XML generation with empty results."""
        xml_str = generate_junit_xml(results=[])
        root = ET.fromstring(xml_str)

        assert root.tag == "testsuites"
        assert root.get("name") == "lockstep"
        assert root.get("tests") == "0"
        assert root.get("failures") == "0"

    def test_successful_sync_results(self) -> None:
        """Test XML generation for successful sync."""
        results = [
            SyncResult(
                contract_name="test_contract",
                table_name="catalog.schema.table",
                success=True,
                actions_applied=3,
            ),
            SyncResult(
                contract_name="another_contract",
                table_name="catalog.schema.another",
                success=True,
                actions_applied=0,
            ),
        ]

        xml_str = generate_junit_xml(results=results)
        root = ET.fromstring(xml_str)

        assert root.get("tests") == "2"
        assert root.get("failures") == "0"
        assert root.get("errors") == "0"

        testsuite = root.find("testsuite")
        assert testsuite is not None
        assert testsuite.get("name") == "lockstep.sync"

        testcases = testsuite.findall("testcase")
        assert len(testcases) == 2

    def test_failed_sync_results(self) -> None:
        """Test XML generation for failed sync."""
        results = [
            SyncResult(
                contract_name="test_contract",
                table_name="catalog.schema.table",
                success=False,
                errors=["Connection failed", "SQL error"],
            ),
        ]

        xml_str = generate_junit_xml(results=results)
        root = ET.fromstring(xml_str)

        assert root.get("tests") == "1"
        assert root.get("errors") == "1"

        testcase = root.find(".//testcase")
        assert testcase is not None

        error = testcase.find("error")
        assert error is not None
        assert error.get("type") == "SyncError"
        assert "Connection failed" in (error.text or "")

    def test_check_mode_with_drift(self) -> None:
        """Test XML generation in check mode with drift detected."""
        plan = SyncPlan(
            contract_name="test_contract",
            table_name="catalog.schema.table",
            actions=[
                SyncAction(
                    action_type=ActionType.ADD_COLUMN,
                    target="catalog.schema.table.new_column",
                    description="Add new column",
                    details={"type": "STRING"},
                ),
            ],
        )
        results = [
            SyncResult(
                contract_name="test_contract",
                table_name="catalog.schema.table",
                success=True,
                plan=plan,
            ),
        ]

        xml_str = generate_junit_xml(results=results, check_mode=True)
        root = ET.fromstring(xml_str)

        assert root.get("tests") == "1"
        assert root.get("failures") == "1"  # Drift is a failure in check mode

        testcase = root.find(".//testcase")
        assert testcase is not None

        failure = testcase.find("failure")
        assert failure is not None
        assert failure.get("type") == "DriftDetected"

    def test_check_mode_no_drift(self) -> None:
        """Test XML generation in check mode with no drift."""
        plan = SyncPlan(
            contract_name="test_contract",
            table_name="catalog.schema.table",
            actions=[],  # No changes
        )
        results = [
            SyncResult(
                contract_name="test_contract",
                table_name="catalog.schema.table",
                success=True,
                plan=plan,
            ),
        ]

        xml_str = generate_junit_xml(results=results, check_mode=True)
        root = ET.fromstring(xml_str)

        assert root.get("tests") == "1"
        assert root.get("failures") == "0"
        assert root.get("errors") == "0"

    def test_writes_to_file(self, tmp_path: Path) -> None:
        """Test XML is written to file when path provided."""
        output_file = tmp_path / "report.xml"

        results = [
            SyncResult(
                contract_name="test",
                table_name="cat.sch.tbl",
                success=True,
            ),
        ]

        generate_junit_xml(results=results, output_path=output_file)

        assert output_file.exists()
        content = output_file.read_text()
        assert "testsuites" in content
        assert "lockstep" in content

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        """Test parent directories are created if needed."""
        output_file = tmp_path / "subdir" / "nested" / "report.xml"

        generate_junit_xml(results=[], output_path=output_file)

        assert output_file.exists()


class TestGenerateValidationJunitXml:
    """Tests for generate_validation_junit_xml function."""

    def test_all_valid(self, tmp_path: Path) -> None:
        """Test XML generation when all files are valid."""
        valid_files = [
            tmp_path / "contract1.yaml",
            tmp_path / "contract2.yaml",
        ]

        xml_str = generate_validation_junit_xml(
            valid_files=valid_files,
            invalid_files=[],
        )
        root = ET.fromstring(xml_str)

        assert root.get("tests") == "2"
        assert root.get("failures") == "0"

        testcases = root.findall(".//testcase")
        assert len(testcases) == 2

        # No failures should be present
        failures = root.findall(".//failure")
        assert len(failures) == 0

    def test_mixed_results(self, tmp_path: Path) -> None:
        """Test XML generation with mixed valid/invalid files."""
        valid_files = [tmp_path / "valid.yaml"]
        invalid_files = [
            (tmp_path / "invalid.yaml", ["Missing field: name", "Invalid status"]),
        ]

        xml_str = generate_validation_junit_xml(
            valid_files=valid_files,
            invalid_files=invalid_files,
        )
        root = ET.fromstring(xml_str)

        assert root.get("tests") == "2"
        assert root.get("failures") == "1"

        # Check failure details
        failure = root.find(".//failure")
        assert failure is not None
        assert failure.get("type") == "ValidationError"
        assert "Missing field: name" in (failure.text or "")

    def test_all_invalid(self, tmp_path: Path) -> None:
        """Test XML generation when all files are invalid."""
        invalid_files = [
            (tmp_path / "bad1.yaml", ["Error 1"]),
            (tmp_path / "bad2.yaml", ["Error 2", "Error 3"]),
        ]

        xml_str = generate_validation_junit_xml(
            valid_files=[],
            invalid_files=invalid_files,
        )
        root = ET.fromstring(xml_str)

        assert root.get("tests") == "2"
        assert root.get("failures") == "2"

    def test_writes_to_file(self, tmp_path: Path) -> None:
        """Test XML is written to file when path provided."""
        output_file = tmp_path / "validation.xml"

        generate_validation_junit_xml(
            valid_files=[tmp_path / "valid.yaml"],
            invalid_files=[],
            output_path=output_file,
        )

        assert output_file.exists()
        content = output_file.read_text()
        assert "testsuites" in content
        assert "lockstep.validation" in content


class TestValidationErrorsInJunit:
    """Tests for validation errors in JUnit output."""

    def test_contract_load_errors(self) -> None:
        """Test XML generation from ContractLoadError list."""
        errors = [
            ContractLoadError(
                "Validation failed",
                path=Path("contracts/bad.yaml"),
                errors=["Missing name", "Invalid schema"],
            ),
        ]

        xml_str = generate_junit_xml(validation_errors=errors)
        root = ET.fromstring(xml_str)

        testsuite = root.find("testsuite[@name='lockstep.validation']")
        assert testsuite is not None
        assert testsuite.get("tests") == "1"
        assert testsuite.get("failures") == "1"

        failure = testsuite.find(".//failure")
        assert failure is not None
        assert "Missing name" in (failure.text or "")
