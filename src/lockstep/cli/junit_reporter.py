"""JUnit XML report generator for CI/CD integration.

Generates JUnit XML format reports that can be consumed by CI/CD tools like:
- Jenkins
- Azure DevOps
- GitHub Actions
- GitLab CI
- CircleCI
"""

from __future__ import annotations

import socket
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from lockstep.models.catalog_state import DESTRUCTIVE_ACTION_TYPES

if TYPE_CHECKING:
    from lockstep.models.catalog_state import SyncPlan
    from lockstep.services.contract_loader import ContractLoadError
    from lockstep.services.sync import SyncResult


def generate_junit_xml(
    results: list[SyncResult] | None = None,
    validation_errors: list[ContractLoadError] | None = None,
    check_mode: bool = False,
    output_path: Path | None = None,
) -> str:
    """Generate JUnit XML report from sync results or validation errors.

    Args:
        results: List of sync results (from from-file command)
        validation_errors: List of validation errors (from validate command)
        check_mode: If True, treat drift as failures
        output_path: Optional path to write the XML file

    Returns:
        XML string content
    """
    # Create root element
    testsuites = ET.Element("testsuites")
    testsuites.set("name", "lockstep")
    testsuites.set("timestamp", datetime.now(UTC).isoformat())
    testsuites.set("hostname", socket.gethostname())

    total_tests = 0
    total_failures = 0
    total_errors = 0
    total_time = 0.0

    # Handle sync results
    if results:
        testsuite = ET.SubElement(testsuites, "testsuite")
        testsuite.set("name", "lockstep.sync")
        testsuite.set("timestamp", datetime.now(UTC).isoformat())

        suite_tests = 0
        suite_failures = 0
        suite_errors = 0

        for result in results:
            testcase = ET.SubElement(testsuite, "testcase")
            testcase.set("name", f"sync:{result.table_name}")
            testcase.set("classname", f"lockstep.contract.{result.contract_name}")
            testcase.set("time", "0.0")
            suite_tests += 1

            if not result.success:
                # Sync failed - this is an error
                suite_errors += 1
                error = ET.SubElement(testcase, "error")
                error.set("type", "SyncError")
                error.set("message", f"Sync failed for {result.table_name}")
                error.text = "\n".join(result.errors) if result.errors else "Unknown error"

            elif check_mode and result.plan and result.plan.has_changes:
                # In check mode, drift is a failure
                suite_failures += 1
                failure = ET.SubElement(testcase, "failure")
                failure.set("type", "DriftDetected")
                failure.set("message", f"Contract drift detected for {result.table_name}")
                failure.text = _format_plan_as_text(result.plan)

            else:
                # Success - add system-out with details if there was a plan
                if result.plan and result.plan.has_changes:
                    system_out = ET.SubElement(testcase, "system-out")
                    system_out.text = _format_plan_as_text(result.plan)

        testsuite.set("tests", str(suite_tests))
        testsuite.set("failures", str(suite_failures))
        testsuite.set("errors", str(suite_errors))
        testsuite.set("skipped", "0")

        total_tests += suite_tests
        total_failures += suite_failures
        total_errors += suite_errors

    # Handle validation errors
    if validation_errors:
        testsuite = ET.SubElement(testsuites, "testsuite")
        testsuite.set("name", "lockstep.validation")
        testsuite.set("timestamp", datetime.now(UTC).isoformat())

        suite_tests = len(validation_errors)
        suite_failures = len(validation_errors)

        for validation_error in validation_errors:
            testcase = ET.SubElement(testsuite, "testcase")
            file_name = validation_error.path.name if validation_error.path else "unknown"
            testcase.set("name", f"validate:{file_name}")
            testcase.set("classname", "lockstep.validation")
            testcase.set("time", "0.0")

            failure = ET.SubElement(testcase, "failure")
            failure.set("type", "ValidationError")
            failure.set("message", str(validation_error))
            if validation_error.errors:
                failure.text = "\n".join(validation_error.errors)

        testsuite.set("tests", str(suite_tests))
        testsuite.set("failures", str(suite_failures))
        testsuite.set("errors", "0")
        testsuite.set("skipped", "0")

        total_tests += suite_tests
        total_failures += suite_failures

    # Set totals on root
    testsuites.set("tests", str(total_tests))
    testsuites.set("failures", str(total_failures))
    testsuites.set("errors", str(total_errors))
    testsuites.set("time", str(total_time))

    # Generate XML string
    xml_str = ET.tostring(testsuites, encoding="unicode", xml_declaration=True)

    # Write to file if path provided
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(xml_str, encoding="utf-8")

    return xml_str


def _format_plan_as_text(plan: SyncPlan) -> str:
    """Format a sync plan as plain text for JUnit output."""
    lines = [f"Table: {plan.table_name}", f"Contract: {plan.contract_name}", ""]

    if not plan.actions:
        lines.append("No changes required.")
        return "\n".join(lines)

    lines.append("Planned changes:")
    for action in plan.actions:
        lines.append(f"  - {action.action_type.value}")
        if action.target:
            lines.append(f"    Target: {action.target}")
        if action.description:
            lines.append(f"    Description: {action.description}")
        for key, value in action.details.items():
            lines.append(f"    {key}: {value}")

    lines.append("")
    total_actions = len(plan.actions)
    lines.append(f"Summary: {total_actions} total changes")
    if plan.has_destructive_changes:
        destructive_count = len(
            [a for a in plan.actions if a.action_type in DESTRUCTIVE_ACTION_TYPES]
        )
        lines.append(f"  Warning: {destructive_count} destructive changes")

    return "\n".join(lines)


def generate_validation_junit_xml(
    valid_files: list[Path],
    invalid_files: list[tuple[Path, list[str]]],
    output_path: Path | None = None,
) -> str:
    """Generate JUnit XML report for validation results.

    Args:
        valid_files: List of paths to valid contract files
        invalid_files: List of (path, errors) tuples for invalid files
        output_path: Optional path to write the XML file

    Returns:
        XML string content
    """
    testsuites = ET.Element("testsuites")
    testsuites.set("name", "lockstep")
    testsuites.set("timestamp", datetime.now(UTC).isoformat())
    testsuites.set("hostname", socket.gethostname())

    testsuite = ET.SubElement(testsuites, "testsuite")
    testsuite.set("name", "lockstep.validation")
    testsuite.set("timestamp", datetime.now(UTC).isoformat())

    total_tests = len(valid_files) + len(invalid_files)
    failures = len(invalid_files)

    # Add successful validations
    for file_path in valid_files:
        testcase = ET.SubElement(testsuite, "testcase")
        testcase.set("name", f"validate:{file_path.name}")
        testcase.set("classname", "lockstep.validation")
        testcase.set("time", "0.0")

    # Add failed validations
    for file_path, errors in invalid_files:
        testcase = ET.SubElement(testsuite, "testcase")
        testcase.set("name", f"validate:{file_path.name}")
        testcase.set("classname", "lockstep.validation")
        testcase.set("time", "0.0")

        failure = ET.SubElement(testcase, "failure")
        failure.set("type", "ValidationError")
        failure.set("message", f"Contract validation failed: {file_path.name}")
        failure.text = "\n".join(errors)

    testsuite.set("tests", str(total_tests))
    testsuite.set("failures", str(failures))
    testsuite.set("errors", "0")
    testsuite.set("skipped", "0")

    testsuites.set("tests", str(total_tests))
    testsuites.set("failures", str(failures))
    testsuites.set("errors", "0")
    testsuites.set("time", "0.0")

    # Generate XML string
    xml_str = ET.tostring(testsuites, encoding="unicode", xml_declaration=True)

    # Write to file if path provided
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(xml_str, encoding="utf-8")

    return xml_str
