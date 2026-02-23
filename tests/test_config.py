"""Tests for `datum config get/set/list/unset`."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from datum.main import app

runner = CliRunner()


def invoke(args: list, config_path: Path):
    with patch("datum.commands.config.get_config_path", return_value=config_path):
        return runner.invoke(app, args)


# ---------------------------------------------------------------------------
# config set
# ---------------------------------------------------------------------------


class TestConfigSet:
    def test_set_exits_0(self, tmp_path):
        result = invoke(["config", "set", "registry", "https://datumhub.org"], tmp_path / "config.json")
        assert result.exit_code == 0

    def test_set_writes_to_file(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        data = json.loads(cfg_path.read_text())
        assert data["registry"] == "https://datumhub.org"

    def test_set_shows_confirmation(self, tmp_path):
        result = invoke(["config", "set", "registry", "https://datumhub.org"], tmp_path / "config.json")
        assert "registry" in result.output

    def test_set_overwrites_existing_key(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "old"], cfg_path)
        invoke(["config", "set", "registry", "new"], cfg_path)
        data = json.loads(cfg_path.read_text())
        assert data["registry"] == "new"

    def test_set_preserves_other_keys(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        invoke(["config", "set", "output", "json"], cfg_path)
        data = json.loads(cfg_path.read_text())
        assert data["registry"] == "https://datumhub.org"
        assert data["output"] == "json"

    def test_set_json_output(self, tmp_path):
        result = invoke(["--output", "json", "config", "set", "registry", "https://datumhub.org"], tmp_path / "config.json")
        data = json.loads(result.output)
        assert data["key"] == "registry"
        assert data["value"] == "https://datumhub.org"

    def test_set_quiet_suppresses_output(self, tmp_path):
        result = invoke(["--quiet", "config", "set", "registry", "https://datumhub.org"], tmp_path / "config.json")
        assert result.exit_code == 0
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# config get
# ---------------------------------------------------------------------------


class TestConfigGet:
    def test_get_exits_0_for_set_key(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["config", "get", "registry"], cfg_path)
        assert result.exit_code == 0

    def test_get_prints_value(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["config", "get", "registry"], cfg_path)
        assert "https://datumhub.org" in result.output

    def test_get_missing_key_exits_1(self, tmp_path):
        result = invoke(["config", "get", "registry"], tmp_path / "config.json")
        assert result.exit_code == 1

    def test_get_json_output(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["--output", "json", "config", "get", "registry"], cfg_path)
        data = json.loads(result.output)
        assert data["key"] == "registry"
        assert data["value"] == "https://datumhub.org"

    def test_get_missing_key_json_output(self, tmp_path):
        result = invoke(["--output", "json", "config", "get", "registry"], tmp_path / "config.json")
        data = json.loads(result.output)
        assert data["value"] is None


# ---------------------------------------------------------------------------
# config list
# ---------------------------------------------------------------------------


class TestConfigList:
    def test_list_empty_exits_0(self, tmp_path):
        result = invoke(["config", "list"], tmp_path / "config.json")
        assert result.exit_code == 0

    def test_list_empty_shows_message(self, tmp_path):
        result = invoke(["config", "list"], tmp_path / "config.json")
        assert "no configuration" in result.output.lower()

    def test_list_shows_keys(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["config", "list"], cfg_path)
        assert "registry" in result.output
        assert "https://datumhub.org" in result.output

    def test_list_json_output(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["--output", "json", "config", "list"], cfg_path)
        data = json.loads(result.output)
        assert data["registry"] == "https://datumhub.org"

    def test_list_quiet_suppresses_output(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["--quiet", "config", "list"], cfg_path)
        assert result.exit_code == 0
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# config show
# ---------------------------------------------------------------------------


class TestConfigShow:
    def test_show_no_arg_lists_all(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["config", "show"], cfg_path)
        assert result.exit_code == 0
        assert "registry" in result.output
        assert "https://datumhub.org" in result.output

    def test_show_with_key_prints_value(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["config", "show", "registry"], cfg_path)
        assert result.exit_code == 0
        assert "https://datumhub.org" in result.output

    def test_show_with_missing_key_exits_1(self, tmp_path):
        result = invoke(["config", "show", "registry"], tmp_path / "config.json")
        assert result.exit_code == 1

    def test_show_no_arg_empty_config(self, tmp_path):
        result = invoke(["config", "show"], tmp_path / "config.json")
        assert result.exit_code == 0
        assert "no configuration" in result.output.lower()


# ---------------------------------------------------------------------------
# config unset
# ---------------------------------------------------------------------------


class TestConfigUnset:
    def test_unset_exits_0(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["config", "unset", "registry"], cfg_path)
        assert result.exit_code == 0

    def test_unset_removes_key(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        invoke(["config", "unset", "registry"], cfg_path)
        data = json.loads(cfg_path.read_text())
        assert "registry" not in data

    def test_unset_missing_key_exits_1(self, tmp_path):
        result = invoke(["config", "unset", "registry"], tmp_path / "config.json")
        assert result.exit_code == 1

    def test_unset_json_output(self, tmp_path):
        cfg_path = tmp_path / "config.json"
        invoke(["config", "set", "registry", "https://datumhub.org"], cfg_path)
        result = invoke(["--output", "json", "config", "unset", "registry"], cfg_path)
        data = json.loads(result.output)
        assert data["removed"] is True


# ---------------------------------------------------------------------------
# B1: Config V2 migration
# ---------------------------------------------------------------------------


class TestConfigMigration:
    """B1.13–B1.15: load_config() migrates v1 flat keys to v2 nested auth."""

    def test_v1_config_migrated_to_v2_on_load(self, tmp_path):
        """B1.13: v1 flat token.{host} keys are converted to auth.{host}.token."""
        from datum.commands.config import load_config

        cfg_path = tmp_path / "config.json"
        cfg_path.write_text(
            json.dumps({"token.datumhub.org": "tok123", "username.datumhub.org": "alice"})
        )
        with patch("datum.commands.config.get_config_path", return_value=cfg_path):
            cfg = load_config()

        assert cfg["auth"]["datumhub.org"]["token"] == "tok123"
        assert cfg["auth"]["datumhub.org"]["username"] == "alice"
        assert "_version" in cfg
        assert cfg["_version"] == 2

    def test_v2_config_loaded_without_migration(self, tmp_path):
        """B1.14: already-v2 config is returned unchanged."""
        from datum.commands.config import load_config

        cfg_path = tmp_path / "config.json"
        v2 = {"_version": 2, "auth": {"datumhub.org": {"token": "tok456"}}}
        cfg_path.write_text(json.dumps(v2))
        with patch("datum.commands.config.get_config_path", return_value=cfg_path):
            cfg = load_config()

        assert cfg == v2

    def test_migration_saves_updated_config_to_disk(self, tmp_path):
        """B1.15: when migration fires, the new v2 format is persisted to disk."""
        from datum.commands.config import load_config

        cfg_path = tmp_path / "config.json"
        cfg_path.write_text(json.dumps({"token.datumhub.org": "tok789"}))
        with patch("datum.commands.config.get_config_path", return_value=cfg_path):
            load_config()

        saved = json.loads(cfg_path.read_text())
        assert "token.datumhub.org" not in saved
        assert saved["auth"]["datumhub.org"]["token"] == "tok789"
        assert saved["_version"] == 2
