"""Tests for Databricks configuration."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

from lockstep.databricks.config import AuthType, DatabricksConfig, _load_config_file, is_databricks_runtime


class TestDatabricksConfig:
    """Tests for DatabricksConfig."""

    def test_default_values(self) -> None:
        """Test that config has sensible defaults."""
        config = DatabricksConfig(host="https://test.databricks.com", http_path="/sql/test")
        assert config.host == "https://test.databricks.com"
        assert config.http_path == "/sql/test"
        assert config.auth_type == AuthType.OAUTH
        assert config.token is None
        assert config.client_id is None
        assert config.client_secret is None
        assert config.timeout_seconds == 300
        assert config.retry_count == 3

    def test_is_configured_true_oauth(self) -> None:
        """Test is_configured returns True for OAuth with host and http_path."""
        config = DatabricksConfig(host="https://test.databricks.com", http_path="/sql/test")
        assert config.is_configured() is True

    def test_is_configured_true_pat(self) -> None:
        """Test is_configured returns True for PAT with token."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.PAT,
            token="my-token",
        )
        assert config.is_configured() is True

    def test_is_configured_false_pat_no_token(self) -> None:
        """Test is_configured returns False for PAT without token."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.PAT,
        )
        assert config.is_configured() is False

    def test_is_configured_true_sp(self) -> None:
        """Test is_configured returns True for SP with credentials."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.SP,
            client_id="my-client-id",
            client_secret="my-secret",
        )
        assert config.is_configured() is True

    def test_is_configured_false_sp_no_credentials(self) -> None:
        """Test is_configured returns False for SP without credentials."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.SP,
        )
        assert config.is_configured() is False

    def test_is_configured_false_missing_host(self) -> None:
        """Test is_configured returns False when host is missing."""
        config = DatabricksConfig(host="", http_path="/sql/test")
        assert config.is_configured() is False

    def test_is_configured_false_missing_http_path(self) -> None:
        """Test is_configured returns False when http_path is missing."""
        config = DatabricksConfig(host="https://test.databricks.com", http_path="")
        assert config.is_configured() is False

    def test_get_auth_description_oauth(self) -> None:
        """Test get_auth_description returns OAuth."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.OAUTH,
        )
        assert config.get_auth_description() == "OAuth"

    def test_get_auth_description_pat(self) -> None:
        """Test get_auth_description returns Personal Access Token."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.PAT,
            token="my-token",
        )
        assert config.get_auth_description() == "Personal Access Token"

    def test_get_auth_description_sp(self) -> None:
        """Test get_auth_description returns Service Principal."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.SP,
            client_id="my-client-id",
            client_secret="my-secret",
        )
        assert config.get_auth_description() == "Service Principal"

    def test_load_from_environment_variables(self) -> None:
        """Test that config loads from environment variables."""
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_HOST": "https://env-test.databricks.com",
                "DATABRICKS_HTTP_PATH": "/sql/env-test",
                "DATABRICKS_TOKEN": "env-token",
            },
        ):
            config = DatabricksConfig()
            assert config.host == "https://env-test.databricks.com"
            assert config.http_path == "/sql/env-test"
            assert config.token == "env-token"

    def test_auth_type_from_environment(self) -> None:
        """Test that auth_type can be loaded from environment variable."""
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_HOST": "https://test.databricks.com",
                "DATABRICKS_HTTP_PATH": "/sql/test",
                "DATABRICKS_AUTH_TYPE": "pat",
                "DATABRICKS_TOKEN": "env-token",
            },
        ):
            config = DatabricksConfig()
            assert config.auth_type == AuthType.PAT


class TestLoadConfigFile:
    """Tests for _load_config_file function."""

    def test_returns_empty_dict_when_no_config_file(self, tmp_path: Path) -> None:
        """Test that empty dict is returned when no config file exists."""
        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result == {}

    def test_loads_yaml_config_file(self, tmp_path: Path) -> None:
        """Test that YAML config file is loaded."""
        config_file = tmp_path / ".lockstep.yaml"
        config_file.write_text("host: https://yaml-test.databricks.com\nhttp_path: /sql/yaml")

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result["host"] == "https://yaml-test.databricks.com"
            assert result["http_path"] == "/sql/yaml"

    def test_loads_yml_config_file(self, tmp_path: Path) -> None:
        """Test that .yml config file is loaded."""
        config_file = tmp_path / ".lockstep.yml"
        config_file.write_text("host: https://yml-test.databricks.com")

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result["host"] == "https://yml-test.databricks.com"

    def test_yaml_takes_precedence_over_yml(self, tmp_path: Path) -> None:
        """Test that .yaml file takes precedence over .yml."""
        yaml_file = tmp_path / ".lockstep.yaml"
        yaml_file.write_text("host: https://yaml-first.databricks.com")
        yml_file = tmp_path / ".lockstep.yml"
        yml_file.write_text("host: https://yml-second.databricks.com")

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result["host"] == "https://yaml-first.databricks.com"

    def test_handles_empty_yaml_file(self, tmp_path: Path) -> None:
        """Test that empty YAML file returns empty dict."""
        config_file = tmp_path / ".lockstep.yaml"
        config_file.write_text("")

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result == {}

    def test_loads_toml_config_file(self, tmp_path: Path) -> None:
        """Test that TOML config file is loaded."""
        config_file = tmp_path / ".lockstep.toml"
        config_file.write_text(
            'host = "https://toml-test.databricks.com"\n'
            'http_path = "/sql/toml"\n'
            "timeout_seconds = 600\n"
        )

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result["host"] == "https://toml-test.databricks.com"
            assert result["http_path"] == "/sql/toml"
            assert result["timeout_seconds"] == 600

    def test_yaml_takes_precedence_over_toml(self, tmp_path: Path) -> None:
        """Test that .yaml file takes precedence over .toml."""
        yaml_file = tmp_path / ".lockstep.yaml"
        yaml_file.write_text("host: https://yaml-first.databricks.com")
        toml_file = tmp_path / ".lockstep.toml"
        toml_file.write_text('host = "https://toml-second.databricks.com"')

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result["host"] == "https://yaml-first.databricks.com"

    def test_toml_config_with_service_principal(self, tmp_path: Path) -> None:
        """Test that TOML config loads service principal credentials."""
        config_file = tmp_path / ".lockstep.toml"
        config_file.write_text(
            'host = "https://toml-sp.databricks.com"\n'
            'http_path = "/sql/test"\n'
            'auth_type = "sp"\n'
            'client_id = "my-client-id"\n'
            'client_secret = "my-client-secret"\n'
        )

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result["host"] == "https://toml-sp.databricks.com"
            assert result["auth_type"] == "sp"
            assert result["client_id"] == "my-client-id"
            assert result["client_secret"] == "my-client-secret"

    def test_toml_config_integrates_with_databricks_config(self, tmp_path: Path) -> None:
        """Test that TOML config integrates with DatabricksConfig model."""
        config_file = tmp_path / ".lockstep.toml"
        config_file.write_text(
            'host = "https://toml-integration.databricks.com"\n'
            'http_path = "/sql/integration"\n'
            'auth_type = "pat"\n'
            'token = "toml-token"\n'
        )

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            config = DatabricksConfig()
            assert config.host == "https://toml-integration.databricks.com"
            assert config.http_path == "/sql/integration"
            assert config.auth_type == AuthType.PAT
            assert config.token == "toml-token"
            assert config.get_auth_description() == "Personal Access Token"


class TestRuntimeAuth:
    """Tests for Databricks runtime auto-detection."""

    def test_is_databricks_runtime_false_by_default(self) -> None:
        """Test that is_databricks_runtime returns False outside Databricks."""
        with patch.dict(os.environ, {}, clear=True):
            assert is_databricks_runtime() is False

    def test_is_databricks_runtime_true_when_env_set(self) -> None:
        """Test that is_databricks_runtime returns True when env var is set."""
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "15.4"}):
            assert is_databricks_runtime() is True

    def test_auto_detect_runtime_switches_oauth_to_runtime(self) -> None:
        """Test that OAuth auth auto-switches to runtime inside Databricks."""
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_RUNTIME_VERSION": "15.4",
                "DATABRICKS_HOST": "https://test.databricks.com",
                "DATABRICKS_HTTP_PATH": "/sql/test",
            },
        ):
            config = DatabricksConfig()
            assert config.auth_type == AuthType.RUNTIME

    def test_explicit_auth_type_not_overridden_by_runtime(self) -> None:
        """Test that explicit auth_type is not overridden by runtime detection."""
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_RUNTIME_VERSION": "15.4",
                "DATABRICKS_HOST": "https://test.databricks.com",
                "DATABRICKS_HTTP_PATH": "/sql/test",
                "DATABRICKS_TOKEN": "my-token",
            },
        ):
            config = DatabricksConfig(auth_type=AuthType.PAT, token="my-token")
            assert config.auth_type == AuthType.PAT

    def test_get_auth_description_runtime(self) -> None:
        """Test get_auth_description returns Databricks Runtime."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.RUNTIME,
        )
        assert config.get_auth_description() == "Databricks Runtime"

    def test_is_configured_true_runtime(self) -> None:
        """Test is_configured returns True for runtime auth with host and http_path."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            auth_type=AuthType.RUNTIME,
        )
        assert config.is_configured() is True
