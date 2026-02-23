"""Tests for `datum whoami`."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from datum.main import app

runner = CliRunner()

REGISTRY = "https://datumhub.org"
HOST = "datumhub.org"


def invoke(args: list, config_path: Path):
    with patch("datum.commands.config.get_config_path", return_value=config_path):
        with patch("datum.commands.whoami.load_config", side_effect=lambda: (
            json.loads(config_path.read_text()) if config_path.exists() else {}
        )):
            return runner.invoke(app, args)


def _invoke_clean(args: list, tmp_path: Path):
    """Invoke with an empty (nonexistent) config to avoid picking up real user config."""
    cfg_path = tmp_path / "config.json"
    with patch("datum.commands.config.get_config_path", return_value=cfg_path):
        return runner.invoke(app, args)


class TestWhoamiLocal:
    def test_exits_0_local_registry(self, tmp_path):
        result = _invoke_clean(["whoami"], tmp_path)
        assert result.exit_code == 0

    def test_shows_local_mode(self, tmp_path):
        result = _invoke_clean(["whoami"], tmp_path)
        assert "local" in result.output.lower()

    def test_json_output_local(self, tmp_path):
        result = _invoke_clean(["--output", "json", "whoami"], tmp_path)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["is_remote"] is False
        assert data["authenticated"] is False
        assert data["username"] is None


class TestWhoamiRemote:
    def test_shows_not_authenticated_when_no_token(self, tmp_path):
        result = _invoke_clean(["--registry", REGISTRY, "whoami"], tmp_path)
        assert result.exit_code == 0
        assert "not authenticated" in result.output.lower() or "✗" in result.output

    def test_json_output_remote_no_token(self, tmp_path):
        result = _invoke_clean(["--registry", REGISTRY, "--output", "json", "whoami"], tmp_path)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["is_remote"] is True
        assert data["authenticated"] is False
        assert data["registry"] == REGISTRY

    def test_json_output_authenticated(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        cfg_path.write_text(json.dumps({f"token.{HOST}": "mytoken", f"username.{HOST}": "alice"}))
        with patch("datum.commands.config.get_config_path", return_value=cfg_path):
            result = runner.invoke(app, ["--registry", REGISTRY, "--output", "json", "whoami"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["authenticated"] is True
        assert data["username"] == "alice"

    def test_shows_username_when_logged_in(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        cfg_path.write_text(json.dumps({f"token.{HOST}": "mytoken", f"username.{HOST}": "alice"}))
        with patch("datum.commands.config.get_config_path", return_value=cfg_path):
            result = runner.invoke(app, ["--registry", REGISTRY, "whoami"])
        assert result.exit_code == 0
        assert "alice" in result.output
