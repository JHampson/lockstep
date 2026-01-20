"""Integration tests for CLI commands."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from lockstep.cli.helpers import get_databricks_config
from lockstep.cli.main import app
from lockstep.databricks.config import AuthType
from lockstep.models.catalog_state import ActionType, SyncAction, SyncPlan
from lockstep.services.sync import SyncResult

runner = CliRunner()


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


class TestVersionCommand:
    """Tests for --version flag."""

    def test_version_flag(self) -> None:
        """Test that --version shows version."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "lockstep version" in result.stdout

    def test_version_short_flag(self) -> None:
        """Test that -V shows version."""
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert "lockstep version" in result.stdout


class TestValidateCommand:
    """Tests for validate command."""

    def test_validate_valid_contract(self, tmp_contract_file: Path) -> None:
        """Test validating a valid contract file."""
        result = runner.invoke(app, ["validate", str(tmp_contract_file)])
        assert result.exit_code == 0
        assert "Valid" in result.stdout

    def test_validate_invalid_contract(self, tmp_path: Path) -> None:
        """Test validating an invalid contract file."""
        invalid_file = tmp_path / "invalid.yaml"
        # Use invalid status value to trigger validation error
        invalid_file.write_text("name: test_contract\nstatus: not_a_valid_status\n")

        result = runner.invoke(app, ["validate", str(invalid_file)])
        assert result.exit_code == 1
        assert "Invalid" in result.stdout

    def test_validate_directory(self, tmp_contracts_dir: Path) -> None:
        """Test validating a directory of contracts."""
        result = runner.invoke(app, ["validate", str(tmp_contracts_dir)])
        # Should have mixed results (some valid, some invalid)
        assert "Valid" in result.stdout
        assert "Invalid" in result.stdout

    def test_validate_nonexistent_file(self) -> None:
        """Test validating a file that doesn't exist."""
        result = runner.invoke(app, ["validate", "/nonexistent/path.yaml"])
        assert result.exit_code == 2  # Typer error for invalid path

    def test_validate_verbose(self, tmp_contract_file: Path) -> None:
        """Test validate with verbose flag."""
        result = runner.invoke(app, ["validate", str(tmp_contract_file), "--verbose"])
        assert result.exit_code == 0


class TestPlanCommand:
    """Tests for plan command."""

    def test_plan_no_changes(self, tmp_contract_file: Path) -> None:
        """Test plan when no changes needed."""
        with (
            patch("lockstep.cli.actions.DatabricksConnector") as mock_connector_cls,
            patch("lockstep.cli.actions.SyncService") as mock_sync_cls,
        ):
            mock_connector = MagicMock()
            mock_connector.__enter__ = MagicMock(return_value=mock_connector)
            mock_connector.__exit__ = MagicMock(return_value=None)
            mock_connector_cls.return_value = mock_connector

            mock_sync = MagicMock()
            mock_sync.sync_contracts.return_value = [
                SyncResult(
                    contract_name="test",
                    table_name="cat.sch.tbl",
                    success=True,
                    plan=SyncPlan(
                        contract_name="test",
                        table_name="cat.sch.tbl",
                        actions=[],
                    ),
                )
            ]
            mock_sync_cls.return_value = mock_sync

            result = runner.invoke(
                app,
                [
                    "plan",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--auth-type",
                    "pat",
                    "--token",
                    "test-token",
                ],
            )

            assert result.exit_code == 0
            assert "in sync" in result.stdout

    def test_plan_with_changes(self, tmp_contract_file: Path) -> None:
        """Test plan when changes are detected."""
        with (
            patch("lockstep.cli.actions.DatabricksConnector") as mock_connector_cls,
            patch("lockstep.cli.actions.SyncService") as mock_sync_cls,
        ):
            mock_connector = MagicMock()
            mock_connector.__enter__ = MagicMock(return_value=mock_connector)
            mock_connector.__exit__ = MagicMock(return_value=None)
            mock_connector_cls.return_value = mock_connector

            mock_sync = MagicMock()
            mock_sync.sync_contracts.return_value = [
                SyncResult(
                    contract_name="test",
                    table_name="cat.sch.tbl",
                    success=True,
                    plan=SyncPlan(
                        contract_name="test",
                        table_name="cat.sch.tbl",
                        actions=[
                            SyncAction(
                                action_type=ActionType.CREATE_TABLE,
                                target="cat.sch.tbl",
                                description="Create table",
                                sql="CREATE TABLE ...",
                            ),
                        ],
                    ),
                )
            ]
            mock_sync_cls.return_value = mock_sync

            result = runner.invoke(
                app,
                [
                    "plan",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--auth-type",
                    "pat",
                    "--token",
                    "test-token",
                ],
            )

            # Exit code 2 = drift detected
            assert result.exit_code == 2
            assert "Changes detected" in result.stdout

    def test_plan_invalid_contract(self, tmp_path: Path) -> None:
        """Test plan with invalid contract."""
        invalid_file = tmp_path / "invalid.yaml"
        invalid_file.write_text("name: test_contract\nstatus: not_a_valid_status\n")

        result = runner.invoke(
            app,
            [
                "plan",
                str(invalid_file),
                "--host",
                "https://test.databricks.com",
                "--sql-endpoint",
                "/sql/test",
            ],
        )
        assert result.exit_code == 1


class TestApplyCommand:
    """Tests for apply command."""

    def test_apply_invalid_contract(self, tmp_path: Path) -> None:
        """Test apply with invalid contract."""
        invalid_file = tmp_path / "invalid.yaml"
        # Use invalid status value to trigger validation error
        invalid_file.write_text("name: test_contract\nstatus: not_a_valid_status\n")

        result = runner.invoke(
            app,
            [
                "apply",
                str(invalid_file),
                "--host",
                "https://test.databricks.com",
                "--sql-endpoint",
                "/sql/test",
            ],
        )
        assert result.exit_code == 1

    def test_apply_success(self, tmp_contract_file: Path) -> None:
        """Test successful apply."""
        with (
            patch("lockstep.cli.actions.DatabricksConnector") as mock_connector_cls,
            patch("lockstep.cli.actions.SyncService") as mock_sync_cls,
        ):
            mock_connector = MagicMock()
            mock_connector.__enter__ = MagicMock(return_value=mock_connector)
            mock_connector.__exit__ = MagicMock(return_value=None)
            mock_connector_cls.return_value = mock_connector

            mock_sync = MagicMock()
            mock_sync.sync_contracts.return_value = [
                SyncResult(
                    contract_name="test",
                    table_name="cat.sch.tbl",
                    success=True,
                    actions_applied=3,
                )
            ]
            mock_sync_cls.return_value = mock_sync

            result = runner.invoke(
                app,
                [
                    "apply",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--auth-type",
                    "pat",
                    "--token",
                    "test-token",
                ],
            )

            assert result.exit_code == 0

    def test_apply_with_overrides(self, tmp_contract_file: Path) -> None:
        """Test apply with catalog/schema overrides."""
        with (
            patch("lockstep.cli.actions.DatabricksConnector") as mock_connector_cls,
            patch("lockstep.cli.actions.SyncService") as mock_sync_cls,
        ):
            mock_connector = MagicMock()
            mock_connector.__enter__ = MagicMock(return_value=mock_connector)
            mock_connector.__exit__ = MagicMock(return_value=None)
            mock_connector_cls.return_value = mock_connector

            mock_sync = MagicMock()
            mock_sync.sync_contracts.return_value = [
                SyncResult(contract_name="test", table_name="dev.test.tbl", success=True)
            ]
            mock_sync_cls.return_value = mock_sync

            result = runner.invoke(
                app,
                [
                    "apply",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--auth-type",
                    "pat",
                    "--token",
                    "test-token",
                    "--catalog-override",
                    "dev",
                    "--schema-override",
                    "test",
                    "--table-prefix",
                    "stg_",
                ],
            )

            assert result.exit_code == 0
            # Verify overrides were passed
            call_args = mock_sync.sync_contracts.call_args
            options = call_args[0][1]  # Second positional arg is options
            assert options.catalog_override == "dev"
            assert options.schema_override == "test"
            assert options.table_prefix == "stg_"

    def test_apply_remove_columns(self, tmp_contract_file: Path) -> None:
        """Test --remove-columns flag."""
        with (
            patch("lockstep.cli.actions.DatabricksConnector") as mock_connector_cls,
            patch("lockstep.cli.actions.SyncService") as mock_sync_cls,
        ):
            mock_connector = MagicMock()
            mock_connector.__enter__ = MagicMock(return_value=mock_connector)
            mock_connector.__exit__ = MagicMock(return_value=None)
            mock_connector_cls.return_value = mock_connector

            mock_sync = MagicMock()
            mock_sync.sync_contracts.return_value = [
                SyncResult(contract_name="test", table_name="cat.sch.tbl", success=True)
            ]
            mock_sync_cls.return_value = mock_sync

            result = runner.invoke(
                app,
                [
                    "apply",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--auth-type",
                    "pat",
                    "--token",
                    "test-token",
                    "--remove-columns",
                ],
            )

            assert result.exit_code == 0
            call_args = mock_sync.sync_contracts.call_args
            options = call_args[0][1]
            assert options.remove_columns is True

    def test_apply_remove_tags(self, tmp_contract_file: Path) -> None:
        """Test --remove-tags flag."""
        with (
            patch("lockstep.cli.actions.DatabricksConnector") as mock_connector_cls,
            patch("lockstep.cli.actions.SyncService") as mock_sync_cls,
        ):
            mock_connector = MagicMock()
            mock_connector.__enter__ = MagicMock(return_value=mock_connector)
            mock_connector.__exit__ = MagicMock(return_value=None)
            mock_connector_cls.return_value = mock_connector

            mock_sync = MagicMock()
            mock_sync.sync_contracts.return_value = [
                SyncResult(contract_name="test", table_name="cat.sch.tbl", success=True)
            ]
            mock_sync_cls.return_value = mock_sync

            result = runner.invoke(
                app,
                [
                    "apply",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--auth-type",
                    "pat",
                    "--token",
                    "test-token",
                    "--remove-tags",
                ],
            )

            assert result.exit_code == 0
            call_args = mock_sync.sync_contracts.call_args
            options = call_args[0][1]
            assert options.remove_tags is True

    def test_apply_no_add_columns(self, tmp_contract_file: Path) -> None:
        """Test --no-add-columns flag."""
        with (
            patch("lockstep.cli.actions.DatabricksConnector") as mock_connector_cls,
            patch("lockstep.cli.actions.SyncService") as mock_sync_cls,
        ):
            mock_connector = MagicMock()
            mock_connector.__enter__ = MagicMock(return_value=mock_connector)
            mock_connector.__exit__ = MagicMock(return_value=None)
            mock_connector_cls.return_value = mock_connector

            mock_sync = MagicMock()
            mock_sync.sync_contracts.return_value = [
                SyncResult(contract_name="test", table_name="cat.sch.tbl", success=True)
            ]
            mock_sync_cls.return_value = mock_sync

            result = runner.invoke(
                app,
                [
                    "apply",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--auth-type",
                    "pat",
                    "--token",
                    "test-token",
                    "--no-add-columns",
                ],
            )

            assert result.exit_code == 0
            call_args = mock_sync.sync_contracts.call_args
            options = call_args[0][1]
            assert options.add_columns is False


class TestGetDatabricksConfig:
    """Tests for get_databricks_config helper."""

    def test_default_auth_type_is_oauth(self) -> None:
        """Test that default auth type is OAuth."""
        config = get_databricks_config(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=None,
            token=None,
            client_id=None,
            client_secret=None,
        )
        assert config.auth_type == AuthType.OAUTH

    def test_pat_auth_type(self) -> None:
        """Test PAT authentication type."""
        config = get_databricks_config(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type="pat",
            token="my-token",
            client_id=None,
            client_secret=None,
        )
        assert config.auth_type == AuthType.PAT
        assert config.token == "my-token"

    def test_sp_auth_type(self) -> None:
        """Test Service Principal authentication type."""
        config = get_databricks_config(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type="sp",
            token=None,
            client_id="client-id",
            client_secret="client-secret",
        )
        assert config.auth_type == AuthType.SP
        assert config.client_id == "client-id"
        assert config.client_secret == "client-secret"

    def test_auth_type_case_insensitive(self) -> None:
        """Test that auth type is case insensitive."""
        config = get_databricks_config(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type="PAT",
            token="my-token",
            client_id=None,
            client_secret=None,
        )
        assert config.auth_type == AuthType.PAT


class TestHelpMessages:
    """Tests for help messages."""

    def test_main_help(self) -> None:
        """Test main help message."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        output = strip_ansi(result.stdout)
        assert "plan" in output
        assert "apply" in output
        assert "validate" in output

    def test_plan_help(self) -> None:
        """Test plan help message."""
        result = runner.invoke(app, ["plan", "--help"])
        assert result.exit_code == 0
        output = strip_ansi(result.stdout)
        assert "--format" in output
        assert "--out" in output
        assert "--catalog-override" in output
        assert "--auth-type" in output

    def test_apply_help(self) -> None:
        """Test apply help message."""
        result = runner.invoke(app, ["apply", "--help"])
        assert result.exit_code == 0
        output = strip_ansi(result.stdout)
        assert "--add-tags" in output
        assert "--add-columns" in output
        assert "--remove-columns" in output
        assert "--remove-tags" in output
        assert "--catalog-override" in output
        assert "--auth-type" in output
        assert "--token" in output
        assert "--client-id" in output

    def test_validate_help(self) -> None:
        """Test validate help message."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0
        output = strip_ansi(result.stdout)
        assert "--verbose" in output
