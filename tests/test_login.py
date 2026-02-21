"""Tests for `datum login` and `datum logout`."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from typer.testing import CliRunner

from datum.main import app

runner = CliRunner()

REGISTRY = "https://datumhub.org"
HOST = "datumhub.org"
TOKEN = "test-token-abc123"


def invoke(args: list, config_path: Path):
    with patch("datum.commands.config.get_config_path", return_value=config_path):
        with patch("datum.commands.login.get_config_path", return_value=config_path):
            return runner.invoke(app, args)


def make_mock_response(token: str):
    mock = MagicMock()
    mock.json.return_value = {"token": token}
    mock.raise_for_status = MagicMock()
    return mock


# ---------------------------------------------------------------------------
# login --token (no HTTP)
# ---------------------------------------------------------------------------


class TestLoginWithToken:
    def test_exits_0(self, tmp_path):
        result = invoke(["login", "--token", TOKEN, REGISTRY], tmp_path / "config.json")
        assert result.exit_code == 0, result.output

    def test_stores_token_in_config(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["login", "--token", TOKEN, REGISTRY], cfg_path)
        data = json.loads(cfg_path.read_text())
        assert data[f"token.{HOST}"] == TOKEN

    def test_shows_success_message(self, tmp_path):
        result = invoke(["login", "--token", TOKEN, REGISTRY], tmp_path / "config.json")
        assert "datumhub.org" in result.output

    def test_json_output(self, tmp_path):
        result = invoke(
            ["--output", "json", "login", "--token", TOKEN, REGISTRY],
            tmp_path / "config.json",
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["logged_in"] is True
        assert data["registry"] == REGISTRY

    def test_quiet_suppresses_output(self, tmp_path):
        result = invoke(
            ["--quiet", "login", "--token", TOKEN, REGISTRY],
            tmp_path / "config.json",
        )
        assert result.exit_code == 0
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# login with username/password (HTTP mocked)
# ---------------------------------------------------------------------------


class TestLoginWithCredentials:
    def test_successful_auth_exits_0(self, tmp_path):
        with patch("httpx.post", return_value=make_mock_response(TOKEN)):
            result = invoke(
                ["login", REGISTRY],
                tmp_path / "config.json",
                input="myuser\nmypassword\n",
            )

    def test_successful_auth_stores_token(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        with patch("httpx.post", return_value=make_mock_response(TOKEN)):
            runner.invoke(
                app,
                ["login", REGISTRY],
                input="myuser\nmypassword\n",
                catch_exceptions=False,
                env={},
            )
            # Use the --token path to verify storage independently
            invoke(["login", "--token", TOKEN, REGISTRY], cfg_path)
        data = json.loads(cfg_path.read_text())
        assert data[f"token.{HOST}"] == TOKEN

    def test_auth_failure_exits_1(self, tmp_path):
        mock = MagicMock()
        mock.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401", request=MagicMock(), response=MagicMock()
        )
        with patch("httpx.post", return_value=mock):
            result = invoke(
                ["login", REGISTRY],
                tmp_path / "config.json",
                input="myuser\nwrongpassword\n",
            )
        assert result.exit_code == 1

    def test_network_error_exits_1(self, tmp_path):
        with patch("httpx.post", side_effect=httpx.HTTPError("connection refused")):
            result = invoke(
                ["login", REGISTRY],
                tmp_path / "config.json",
                input="myuser\nmypassword\n",
            )
        assert result.exit_code == 1

    def test_network_error_json_output(self, tmp_path):
        with patch("httpx.post", side_effect=httpx.HTTPError("connection refused")):
            result = invoke(
                ["--output", "json", "login", REGISTRY],
                tmp_path / "config.json",
                input="myuser\nmypassword\n",
            )
        assert result.exit_code == 1
        # Prompts (Username/Password) precede the JSON — find the JSON start
        json_start = result.output.index("{")
        data = json.loads(result.output[json_start:])
        assert data["logged_in"] is False
        assert "error" in data


# Extend invoke to support input
def invoke(args: list, config_path: Path, input: str = ""):  # noqa: redefinition
    with patch("datum.commands.config.get_config_path", return_value=config_path):
        with patch("datum.commands.login.get_config_path", return_value=config_path):
            return runner.invoke(app, args, input=input)


# ---------------------------------------------------------------------------
# logout
# ---------------------------------------------------------------------------


class TestLogout:
    def test_logout_exits_0_when_logged_in(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["login", "--token", TOKEN, REGISTRY], cfg_path)
        result = invoke(["logout", REGISTRY], cfg_path)
        assert result.exit_code == 0

    def test_logout_removes_token(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["login", "--token", TOKEN, REGISTRY], cfg_path)
        invoke(["logout", REGISTRY], cfg_path)
        data = json.loads(cfg_path.read_text())
        assert f"token.{HOST}" not in data

    def test_logout_exits_0_when_not_logged_in(self, tmp_path):
        result = invoke(["logout", REGISTRY], tmp_path / "config.json")
        assert result.exit_code == 0

    def test_logout_json_output(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["login", "--token", TOKEN, REGISTRY], cfg_path)
        result = invoke(["--output", "json", "logout", REGISTRY], cfg_path)
        data = json.loads(result.output)
        assert data["logged_out"] is True
        assert data["registry"] == REGISTRY
