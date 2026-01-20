"""Tests for CLI modules: exceptions, output, actions, helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lockstep.cli.actions import (
    ApplyResult,
    FileValidationResult,
    PlanResult,
    ValidateResult,
    execute_validate,
)
from lockstep.cli.exceptions import (
    ConfigurationError,
    ContractLoadingError,
    InvalidAuthTypeError,
    InvalidFormatError,
    MissingConfigurationError,
)
from lockstep.cli.helpers import (
    ConnectionOptions,
    LoadContractsResult,
    build_databricks_config,
    validate_databricks_config,
    validate_output_format,
)
from lockstep.cli.output import (
    OutputFormat,
    OutputOptions,
    present_apply_progress,
    present_config_error,
    present_contract_load_error,
    present_error,
    present_info,
    present_plan_summary,
    present_validate_summary,
)
from lockstep.databricks.config import AuthType
from lockstep.services import ContractLoadError

# ============================================================================
# Tests for exceptions.py
# ============================================================================


class TestConfigurationError:
    """Tests for ConfigurationError exception."""

    def test_base_exception(self) -> None:
        """Test ConfigurationError is a base exception."""
        error = ConfigurationError("test error")
        assert str(error) == "test error"
        assert isinstance(error, Exception)


class TestInvalidAuthTypeError:
    """Tests for InvalidAuthTypeError exception."""

    def test_error_message(self) -> None:
        """Test error message includes auth type."""
        error = InvalidAuthTypeError("invalid")
        assert "invalid" in str(error)
        assert "oauth" in str(error)
        assert "pat" in str(error)
        assert "sp" in str(error)

    def test_stores_auth_type(self) -> None:
        """Test auth_type is stored."""
        error = InvalidAuthTypeError("bad_type")
        assert error.auth_type == "bad_type"

    def test_inherits_from_configuration_error(self) -> None:
        """Test inherits from ConfigurationError."""
        error = InvalidAuthTypeError("x")
        assert isinstance(error, ConfigurationError)


class TestMissingConfigurationError:
    """Tests for MissingConfigurationError exception."""

    def test_pat_missing_message(self) -> None:
        """Test error message for PAT auth type."""
        error = MissingConfigurationError(AuthType.PAT)
        assert error.auth_type == AuthType.PAT
        assert "--token" in error.missing

    def test_sp_missing_message(self) -> None:
        """Test error message for SP auth type."""
        error = MissingConfigurationError(AuthType.SP)
        assert error.auth_type == AuthType.SP
        assert "--client-id" in error.missing
        assert "--client-secret" in error.missing

    def test_oauth_no_missing(self) -> None:
        """Test OAuth has no specific missing message."""
        error = MissingConfigurationError(AuthType.OAUTH)
        assert error.missing == ""

    def test_inherits_from_configuration_error(self) -> None:
        """Test inherits from ConfigurationError."""
        error = MissingConfigurationError(AuthType.OAUTH)
        assert isinstance(error, ConfigurationError)


class TestInvalidFormatError:
    """Tests for InvalidFormatError exception."""

    def test_error_message(self) -> None:
        """Test error message includes format and valid options."""
        error = InvalidFormatError("bad_format")
        assert "bad_format" in str(error)
        assert "table" in str(error)
        assert "json" in str(error)
        assert "junit" in str(error)

    def test_stores_format(self) -> None:
        """Test format_str is stored."""
        error = InvalidFormatError("xml")
        assert error.format_str == "xml"


class TestContractLoadingError:
    """Tests for ContractLoadingError exception."""

    def test_basic_error(self) -> None:
        """Test basic error message."""
        error = ContractLoadingError("Failed to load")
        assert str(error) == "Failed to load"
        assert error.errors == []
        assert error.validation_errors == []

    def test_with_errors(self) -> None:
        """Test with error list."""
        error = ContractLoadingError("Failed", errors=["error1", "error2"])
        assert error.errors == ["error1", "error2"]

    def test_with_validation_errors(self) -> None:
        """Test with validation errors."""
        validation_err = ContractLoadError(Path("test.yaml"), ["validation failed"])
        error = ContractLoadingError("Failed", validation_errors=[validation_err])
        assert len(error.validation_errors) == 1


# ============================================================================
# Tests for helpers.py - Pure Functions
# ============================================================================


class TestValidateOutputFormat:
    """Tests for validate_output_format function."""

    def test_valid_table_format(self) -> None:
        """Test 'table' is valid."""
        assert validate_output_format("table") == OutputFormat.TABLE

    def test_valid_json_format(self) -> None:
        """Test 'json' is valid."""
        assert validate_output_format("json") == OutputFormat.JSON

    def test_valid_junit_format(self) -> None:
        """Test 'junit' is valid."""
        assert validate_output_format("junit") == OutputFormat.JUNIT

    def test_default_is_table(self) -> None:
        """Test None defaults to table."""
        assert validate_output_format(None) == OutputFormat.TABLE

    def test_case_insensitive(self) -> None:
        """Test format is case insensitive."""
        assert validate_output_format("JSON") == OutputFormat.JSON
        assert validate_output_format("TABLE") == OutputFormat.TABLE
        assert validate_output_format("JUnit") == OutputFormat.JUNIT

    def test_invalid_format_raises(self) -> None:
        """Test invalid format raises InvalidFormatError."""
        with pytest.raises(InvalidFormatError) as exc_info:
            validate_output_format("xml")
        assert "xml" in str(exc_info.value)


class TestBuildDatabricksConfig:
    """Tests for build_databricks_config function."""

    def test_default_oauth(self) -> None:
        """Test default auth type is OAuth."""
        conn = ConnectionOptions(host="https://test.com", http_path="/sql/test")
        config = build_databricks_config(conn)
        assert config.auth_type == AuthType.OAUTH

    def test_pat_auth(self) -> None:
        """Test PAT auth type."""
        conn = ConnectionOptions(
            host="https://test.com",
            http_path="/sql/test",
            auth_type="pat",
            token="token123",
        )
        config = build_databricks_config(conn)
        assert config.auth_type == AuthType.PAT
        assert config.token == "token123"

    def test_sp_auth(self) -> None:
        """Test Service Principal auth type."""
        conn = ConnectionOptions(
            host="https://test.com",
            http_path="/sql/test",
            auth_type="sp",
            client_id="client",
            client_secret="secret",
        )
        config = build_databricks_config(conn)
        assert config.auth_type == AuthType.SP
        assert config.client_id == "client"
        assert config.client_secret == "secret"

    def test_invalid_auth_type_raises(self) -> None:
        """Test invalid auth type raises InvalidAuthTypeError."""
        conn = ConnectionOptions(
            host="https://test.com",
            http_path="/sql/test",
            auth_type="invalid",
        )
        with pytest.raises(InvalidAuthTypeError):
            build_databricks_config(conn)

    def test_profile_included(self) -> None:
        """Test profile is included in config."""
        conn = ConnectionOptions(profile="my-profile")
        config = build_databricks_config(conn)
        assert config.profile == "my-profile"


class TestValidateDatabricksConfig:
    """Tests for validate_databricks_config function."""

    def test_valid_config_passes(self) -> None:
        """Test valid config does not raise."""
        conn = ConnectionOptions(
            host="https://test.com",
            http_path="/sql/test",
        )
        config = build_databricks_config(conn)
        # Should not raise
        validate_databricks_config(config)

    def test_missing_host_raises(self) -> None:
        """Test missing host raises MissingConfigurationError."""
        conn = ConnectionOptions(http_path="/sql/test")
        config = build_databricks_config(conn)
        with pytest.raises(MissingConfigurationError):
            validate_databricks_config(config)


class TestConnectionOptions:
    """Tests for ConnectionOptions dataclass."""

    def test_defaults_to_none(self) -> None:
        """Test all fields default to None."""
        opts = ConnectionOptions()
        assert opts.profile is None
        assert opts.host is None
        assert opts.http_path is None
        assert opts.auth_type is None
        assert opts.token is None
        assert opts.client_id is None
        assert opts.client_secret is None

    def test_with_values(self) -> None:
        """Test setting values."""
        opts = ConnectionOptions(
            profile="test",
            host="https://test.com",
            http_path="/sql/test",
        )
        assert opts.profile == "test"
        assert opts.host == "https://test.com"


class TestLoadContractsResult:
    """Tests for LoadContractsResult dataclass."""

    def test_no_validation_errors(self) -> None:
        """Test has_validation_errors is False when no errors."""
        result = LoadContractsResult(contracts=[])
        assert result.has_validation_errors is False

    def test_with_validation_errors(self) -> None:
        """Test has_validation_errors is True when errors exist."""
        err = ContractLoadError(Path("test.yaml"), ["error"])
        result = LoadContractsResult(contracts=[], validation_errors=[err])
        assert result.has_validation_errors is True


# ============================================================================
# Tests for actions.py - Result Dataclasses
# ============================================================================


class TestPlanResult:
    """Tests for PlanResult dataclass."""

    def test_successful_result(self) -> None:
        """Test successful plan result."""
        result = PlanResult(
            success=True,
            timestamp="2024-01-01T00:00:00Z",
            host="https://test.com",
            results=[],
            has_changes=False,
        )
        assert result.success is True
        assert result.error is None

    def test_failed_result(self) -> None:
        """Test failed plan result."""
        result = PlanResult(
            success=False,
            timestamp="2024-01-01T00:00:00Z",
            host="https://test.com",
            results=[],
            has_changes=False,
            error="Connection failed",
        )
        assert result.success is False
        assert result.error == "Connection failed"


class TestApplyResult:
    """Tests for ApplyResult dataclass."""

    def test_successful_result(self) -> None:
        """Test successful apply result."""
        result = ApplyResult(
            success=True,
            timestamp="2024-01-01T00:00:00Z",
            results=[],
            total_applied=5,
            total_failed=0,
        )
        assert result.success is True
        assert result.total_applied == 5

    def test_with_plan_file(self) -> None:
        """Test result from saved plan."""
        result = ApplyResult(
            success=True,
            timestamp="2024-01-01T00:00:00Z",
            results=[],
            plan_file="plan.json",
        )
        assert result.plan_file == "plan.json"


class TestValidateResult:
    """Tests for ValidateResult dataclass."""

    def test_all_valid(self) -> None:
        """Test when all files are valid."""
        result = ValidateResult(
            success=True,
            timestamp="2024-01-01T00:00:00Z",
            base_path=Path("/test"),
            results=[],
            total=2,
            valid_count=2,
            invalid_count=0,
        )
        assert result.success is True
        assert result.invalid_count == 0

    def test_some_invalid(self) -> None:
        """Test when some files are invalid."""
        result = ValidateResult(
            success=False,
            timestamp="2024-01-01T00:00:00Z",
            base_path=Path("/test"),
            results=[],
            total=3,
            valid_count=1,
            invalid_count=2,
        )
        assert result.success is False
        assert result.invalid_count == 2


class TestFileValidationResult:
    """Tests for FileValidationResult dataclass."""

    def test_valid_file(self) -> None:
        """Test valid file result."""
        result = FileValidationResult(
            file_path=Path("/test/file.yaml"),
            relative_path="file.yaml",
            valid=True,
        )
        assert result.valid is True
        assert result.errors == []

    def test_invalid_file(self) -> None:
        """Test invalid file result."""
        result = FileValidationResult(
            file_path=Path("/test/file.yaml"),
            relative_path="file.yaml",
            valid=False,
            errors=["Missing required field"],
        )
        assert result.valid is False
        assert len(result.errors) == 1


# ============================================================================
# Tests for output.py - Presentation Functions
# ============================================================================


class TestOutputOptions:
    """Tests for OutputOptions dataclass."""

    def test_defaults(self) -> None:
        """Test default values."""
        opts = OutputOptions()
        assert opts.format == OutputFormat.TABLE
        assert opts.out_path is None
        assert opts.quiet is False
        assert opts.verbose is False

    def test_with_values(self) -> None:
        """Test setting values."""
        opts = OutputOptions(
            format=OutputFormat.JSON,
            out_path=Path("/test/out.json"),
            quiet=True,
            verbose=True,
        )
        assert opts.format == OutputFormat.JSON
        assert opts.out_path == Path("/test/out.json")


class TestPresentError:
    """Tests for present_error function."""

    def test_prints_error_message(self) -> None:
        """Test error message is printed."""
        with patch("lockstep.cli.output.error_console") as mock_console:
            mock_console.print = MagicMock()
            present_error("Something went wrong")
            mock_console.print.assert_called_once()
            call_args = str(mock_console.print.call_args)
            assert "Something went wrong" in call_args


class TestPresentInfo:
    """Tests for present_info function."""

    def test_prints_when_not_quiet(self) -> None:
        """Test message is printed when not quiet."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            present_info("Info message", quiet=False)
            mock_console.print.assert_called_once()

    def test_silent_when_quiet(self) -> None:
        """Test message is suppressed when quiet."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            present_info("Info message", quiet=True)
            mock_console.print.assert_not_called()


class TestPresentApplyProgress:
    """Tests for present_apply_progress function."""

    def test_success_message(self) -> None:
        """Test success message is printed."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            present_apply_progress("Did something", success=True)
            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args)
            assert "Did something" in call_args

    def test_error_message(self) -> None:
        """Test error message is printed."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            present_apply_progress("Failed action", success=False, error="Oops")
            call_args = str(mock_console.print.call_args)
            assert "Failed action" in call_args
            assert "Oops" in call_args

    def test_quiet_suppresses_success(self) -> None:
        """Test quiet mode suppresses success messages."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            present_apply_progress("Success", success=True, quiet=True)
            mock_console.print.assert_not_called()

    def test_quiet_shows_errors(self) -> None:
        """Test quiet mode still shows errors."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            present_apply_progress("Failed", success=False, error="Error", quiet=True)
            mock_console.print.assert_called()

    def test_verbose_shows_sql(self) -> None:
        """Test verbose mode shows SQL."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            present_apply_progress(
                "Action",
                success=True,
                verbose=True,
                sql="SELECT 1",
            )
            # Should be called twice: once for action, once for SQL
            assert mock_console.print.call_count == 2


class TestPresentPlanSummary:
    """Tests for present_plan_summary function."""

    def test_shows_changes_warning(self) -> None:
        """Test shows warning when changes detected."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            result = PlanResult(
                success=True,
                timestamp="2024-01-01",
                host="test",
                results=[],
                has_changes=True,
            )
            present_plan_summary(result, quiet=False)
            call_args = str(mock_console.print.call_args)
            assert "Changes detected" in call_args

    def test_shows_in_sync(self) -> None:
        """Test shows in sync message when no changes."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            result = PlanResult(
                success=True,
                timestamp="2024-01-01",
                host="test",
                results=[],
                has_changes=False,
            )
            present_plan_summary(result, quiet=False)
            call_args = str(mock_console.print.call_args)
            assert "in sync" in call_args

    def test_quiet_suppresses_summary(self) -> None:
        """Test quiet mode suppresses summary."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            result = PlanResult(
                success=True,
                timestamp="2024-01-01",
                host="test",
                results=[],
                has_changes=True,
            )
            present_plan_summary(result, quiet=True)
            mock_console.print.assert_not_called()


class TestPresentValidateSummary:
    """Tests for present_validate_summary function."""

    def test_shows_all_valid(self) -> None:
        """Test shows success when all valid."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            result = ValidateResult(
                success=True,
                timestamp="2024-01-01",
                base_path=Path("/test"),
                results=[],
                total=3,
                valid_count=3,
                invalid_count=0,
            )
            present_validate_summary(result, quiet=False)
            mock_console.print.assert_called()

    def test_shows_invalid_count(self) -> None:
        """Test shows invalid count when failures."""
        with patch("lockstep.cli.output.console") as mock_console:
            mock_console.print = MagicMock()
            result = ValidateResult(
                success=False,
                timestamp="2024-01-01",
                base_path=Path("/test"),
                results=[],
                total=3,
                valid_count=1,
                invalid_count=2,
            )
            present_validate_summary(result, quiet=False)
            mock_console.print.assert_called()


class TestPresentConfigError:
    """Tests for present_config_error function."""

    def test_missing_config_error(self) -> None:
        """Test MissingConfigurationError is formatted."""
        with patch("lockstep.cli.output.error_console") as mock_console:
            mock_console.print = MagicMock()
            error = MissingConfigurationError(AuthType.PAT)
            present_config_error(error)
            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args)
            assert "not configured" in call_args

    def test_generic_error(self) -> None:
        """Test generic error is formatted."""
        with patch("lockstep.cli.output.error_console") as mock_console:
            mock_console.print = MagicMock()
            error = Exception("Generic error")
            present_config_error(error)
            mock_console.print.assert_called()


class TestPresentContractLoadError:
    """Tests for present_contract_load_error function."""

    def test_with_errors_list(self) -> None:
        """Test ContractLoadingError with errors list."""
        with patch("lockstep.cli.output.error_console") as mock_console:
            mock_console.print = MagicMock()
            error = ContractLoadingError("Failed", errors=["error1", "error2"])
            present_contract_load_error(error)
            assert mock_console.print.call_count >= 1

    def test_with_validation_errors(self) -> None:
        """Test ContractLoadingError with validation errors."""
        with patch("lockstep.cli.output.error_console") as mock_console:
            mock_console.print = MagicMock()
            val_err = ContractLoadError(Path("test.yaml"), ["validation error"])
            error = ContractLoadingError("Failed", validation_errors=[val_err])
            present_contract_load_error(error)
            mock_console.print.assert_called()


# ============================================================================
# Tests for actions.py - Execute Functions
# ============================================================================


class TestExecuteValidate:
    """Tests for execute_validate function."""

    def test_empty_directory(self, tmp_path: Path) -> None:
        """Test with no YAML files."""
        loader = MagicMock()
        loader._find_yaml_files.return_value = iter([])

        result = execute_validate(tmp_path, loader)
        assert result.success is True
        assert result.total == 0

    def test_valid_file(self, tmp_path: Path) -> None:
        """Test with a valid file."""
        file = tmp_path / "test.yaml"
        file.write_text("kind: DataContract\nid: test")

        loader = MagicMock()
        loader._find_yaml_files.return_value = iter([file])
        loader.validate_file.return_value = (True, [])

        result = execute_validate(tmp_path, loader)
        assert result.total == 1
        assert result.valid_count == 1
        assert result.invalid_count == 0

    def test_invalid_file(self, tmp_path: Path) -> None:
        """Test with an invalid file."""
        file = tmp_path / "test.yaml"
        file.write_text("invalid")

        loader = MagicMock()
        loader._find_yaml_files.return_value = iter([file])
        loader.validate_file.return_value = (False, ["Missing required field"])

        result = execute_validate(tmp_path, loader)
        assert result.total == 1
        assert result.valid_count == 0
        assert result.invalid_count == 1
        assert result.success is False

    def test_mixed_results(self, tmp_path: Path) -> None:
        """Test with mixed valid/invalid files."""
        valid_file = tmp_path / "valid.yaml"
        valid_file.write_text("valid: true")
        invalid_file = tmp_path / "invalid.yaml"
        invalid_file.write_text("invalid")

        loader = MagicMock()
        loader._find_yaml_files.return_value = iter([invalid_file, valid_file])
        loader.validate_file.side_effect = [
            (False, ["Error"]),  # invalid.yaml
            (True, []),  # valid.yaml
        ]

        result = execute_validate(tmp_path, loader)
        assert result.total == 2
        assert result.valid_count == 1
        assert result.invalid_count == 1
