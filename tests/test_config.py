"""Tests for Databricks configuration."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

from lockstep.databricks.config import DatabricksConfig, _load_config_file


class TestDatabricksConfig:
    """Tests for DatabricksConfig."""

    def test_default_values(self) -> None:
        """Test that config has sensible defaults."""
        config = DatabricksConfig(host="https://test.databricks.com", http_path="/sql/test")
        assert config.host == "https://test.databricks.com"
        assert config.http_path == "/sql/test"
        assert config.use_oauth is True
        assert config.token is None
        assert config.client_id is None
        assert config.client_secret is None
        assert config.timeout_seconds == 300
        assert config.retry_count == 3

    def test_is_configured_true(self) -> None:
        """Test is_configured returns True when host and http_path are set."""
        config = DatabricksConfig(host="https://test.databricks.com", http_path="/sql/test")
        assert config.is_configured() is True

    def test_is_configured_false_missing_host(self) -> None:
        """Test is_configured returns False when host is missing."""
        config = DatabricksConfig(host="", http_path="/sql/test")
        assert config.is_configured() is False

    def test_is_configured_false_missing_http_path(self) -> None:
        """Test is_configured returns False when http_path is missing."""
        config = DatabricksConfig(host="https://test.databricks.com", http_path="")
        assert config.is_configured() is False

    def test_has_service_principal_true(self) -> None:
        """Test has_service_principal returns True when both client_id and secret are set."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            client_id="test-client-id",
            client_secret="test-secret",
        )
        assert config.has_service_principal() is True

    def test_has_service_principal_false_missing_id(self) -> None:
        """Test has_service_principal returns False when client_id is missing."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            client_secret="test-secret",
        )
        assert config.has_service_principal() is False

    def test_has_service_principal_false_missing_secret(self) -> None:
        """Test has_service_principal returns False when client_secret is missing."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            client_id="test-client-id",
        )
        assert config.has_service_principal() is False

    def test_get_auth_type_service_principal(self) -> None:
        """Test get_auth_type returns service_principal when configured."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            client_id="test-client-id",
            client_secret="test-secret",
        )
        assert config.get_auth_type() == "service_principal"

    def test_get_auth_type_oauth(self) -> None:
        """Test get_auth_type returns oauth when use_oauth is True."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            use_oauth=True,
        )
        assert config.get_auth_type() == "oauth"

    def test_get_auth_type_token(self) -> None:
        """Test get_auth_type returns token when token is set and oauth is disabled."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            use_oauth=False,
            token="test-token",
        )
        assert config.get_auth_type() == "token"

    def test_get_auth_type_none(self) -> None:
        """Test get_auth_type returns none when no auth is configured."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            use_oauth=False,
        )
        assert config.get_auth_type() == "none"

    def test_service_principal_takes_precedence(self) -> None:
        """Test that service principal auth takes precedence over oauth."""
        config = DatabricksConfig(
            host="https://test.databricks.com",
            http_path="/sql/test",
            use_oauth=True,
            token="test-token",
            client_id="test-client-id",
            client_secret="test-secret",
        )
        assert config.get_auth_type() == "service_principal"

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
            'client_id = "my-client-id"\n'
            'client_secret = "my-client-secret"\n'
        )

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            result = _load_config_file()
            assert result["host"] == "https://toml-sp.databricks.com"
            assert result["client_id"] == "my-client-id"
            assert result["client_secret"] == "my-client-secret"

    def test_toml_config_integrates_with_databricks_config(self, tmp_path: Path) -> None:
        """Test that TOML config integrates with DatabricksConfig model."""
        config_file = tmp_path / ".lockstep.toml"
        config_file.write_text(
            'host = "https://toml-integration.databricks.com"\n'
            'http_path = "/sql/integration"\n'
            "use_oauth = false\n"
            'token = "toml-token"\n'
        )

        with patch("lockstep.databricks.config.Path.home", return_value=tmp_path):
            config = DatabricksConfig()
            assert config.host == "https://toml-integration.databricks.com"
            assert config.http_path == "/sql/integration"
            assert config.use_oauth is False
            assert config.token == "toml-token"
            assert config.get_auth_type() == "token"
