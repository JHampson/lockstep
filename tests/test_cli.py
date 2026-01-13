"""Integration tests for CLI commands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from odcs_sync.cli.main import _get_databricks_config, app
from odcs_sync.models.catalog_state import ActionType, SyncAction, SyncPlan
from odcs_sync.services.sync import SyncResult

runner = CliRunner()


class TestVersionCommand:
    """Tests for --version flag."""

    def test_version_flag(self) -> None:
        """Test that --version shows version."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "odcs-sync version" in result.stdout

    def test_version_short_flag(self) -> None:
        """Test that -V shows version."""
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert "odcs-sync version" in result.stdout


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
        invalid_file.write_text("name: missing_required_fields\n")

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


class TestFromFileCommand:
    """Tests for from-file command."""

    def test_from_file_invalid_contract(self, tmp_path: Path) -> None:
        """Test from-file with invalid contract."""
        invalid_file = tmp_path / "invalid.yaml"
        invalid_file.write_text("name: missing_required_fields\n")

        result = runner.invoke(app, ["from-file", str(invalid_file), "--no-oauth"])
        assert result.exit_code == 1

    def test_from_file_dry_run_no_changes(self, tmp_contract_file: Path) -> None:
        """Test dry run when no changes needed."""
        with (
            patch("odcs_sync.cli.main.DatabricksConnector") as mock_connector_cls,
            patch("odcs_sync.cli.main.SyncService") as mock_sync_cls,
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
                    "from-file",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--token",
                    "test-token",
                    "--dry-run",
                ],
            )

            assert result.exit_code == 0
            assert "No changes" in result.stdout or "DRY RUN" in result.stdout

    def test_from_file_dry_run_with_changes(self, tmp_contract_file: Path) -> None:
        """Test dry run when changes are detected."""
        with (
            patch("odcs_sync.cli.main.DatabricksConnector") as mock_connector_cls,
            patch("odcs_sync.cli.main.SyncService") as mock_sync_cls,
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
                    "from-file",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--token",
                    "test-token",
                    "--dry-run",
                ],
            )

            # Exit code 2 = changes detected
            assert result.exit_code == 2
            assert "Differences detected" in result.stdout

    def test_from_file_actual_sync(self, tmp_contract_file: Path) -> None:
        """Test actual sync (not dry run)."""
        with (
            patch("odcs_sync.cli.main.DatabricksConnector") as mock_connector_cls,
            patch("odcs_sync.cli.main.SyncService") as mock_sync_cls,
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
                    "from-file",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--token",
                    "test-token",
                ],
            )

            assert result.exit_code == 0

    def test_from_file_with_overrides(self, tmp_contract_file: Path) -> None:
        """Test from-file with catalog/schema overrides."""
        with (
            patch("odcs_sync.cli.main.DatabricksConnector") as mock_connector_cls,
            patch("odcs_sync.cli.main.SyncService") as mock_sync_cls,
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
                    "from-file",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
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

    def test_from_file_allow_destructive(self, tmp_contract_file: Path) -> None:
        """Test --allow-destructive flag."""
        with (
            patch("odcs_sync.cli.main.DatabricksConnector") as mock_connector_cls,
            patch("odcs_sync.cli.main.SyncService") as mock_sync_cls,
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
                    "from-file",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--token",
                    "test-token",
                    "--allow-destructive",
                ],
            )

            assert result.exit_code == 0
            call_args = mock_sync.sync_contracts.call_args
            options = call_args[0][1]
            assert options.allow_destructive is True

    def test_from_file_preserve_extra_tags(self, tmp_contract_file: Path) -> None:
        """Test --preserve-extra-tags flag."""
        with (
            patch("odcs_sync.cli.main.DatabricksConnector") as mock_connector_cls,
            patch("odcs_sync.cli.main.SyncService") as mock_sync_cls,
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
                    "from-file",
                    str(tmp_contract_file),
                    "--host",
                    "https://test.databricks.com",
                    "--sql-endpoint",
                    "/sql/test",
                    "--token",
                    "test-token",
                    "--preserve-extra-tags",
                ],
            )

            assert result.exit_code == 0
            call_args = mock_sync.sync_contracts.call_args
            options = call_args[0][1]
            assert options.preserve_extra_tags is True


class TestGetDatabricksConfig:
    """Tests for _get_databricks_config helper."""

    def test_token_disables_oauth(self) -> None:
        """Test that providing a token automatically disables OAuth."""
        config = _get_databricks_config(
            host="https://test.databricks.com",
            http_path="/sql/test",
            token="my-token",
            use_oauth=True,  # Should be overridden
        )
        assert config.token == "my-token"
        assert config.use_oauth is False

    def test_no_token_keeps_oauth(self) -> None:
        """Test that OAuth stays enabled when no token provided."""
        config = _get_databricks_config(
            host="https://test.databricks.com",
            http_path="/sql/test",
            token=None,
            use_oauth=True,
        )
        assert config.use_oauth is True

    def test_service_principal_config(self) -> None:
        """Test service principal configuration."""
        config = _get_databricks_config(
            host="https://test.databricks.com",
            http_path="/sql/test",
            token=None,
            use_oauth=True,
            client_id="client-id",
            client_secret="client-secret",
        )
        assert config.client_id == "client-id"
        assert config.client_secret == "client-secret"


class TestHelpMessages:
    """Tests for help messages."""

    def test_main_help(self) -> None:
        """Test main help message."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "from-file" in result.stdout
        assert "validate" in result.stdout

    def test_from_file_help(self) -> None:
        """Test from-file help message."""
        result = runner.invoke(app, ["from-file", "--help"])
        assert result.exit_code == 0
        assert "--dry-run" in result.stdout
        assert "--allow-destructive" in result.stdout
        assert "--catalog-override" in result.stdout
        assert "--token" in result.stdout
        assert "--client-id" in result.stdout

    def test_validate_help(self) -> None:
        """Test validate help message."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0
        assert "--verbose" in result.stdout
