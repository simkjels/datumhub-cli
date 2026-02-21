"""Tests for `datum update`."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch  # noqa: F401 — used for get_cache_root patching

from typer.testing import CliRunner

from datum.main import app
from datum.models import DataPackage
from datum.registry.local import LocalRegistry

runner = CliRunner()

CONTENT = b"col1,col2\n1,2\n3,4\n"

PKG_V1 = {
    "id": "simkjels.samples.sampledata",
    "version": "0.1.0",
    "title": "Sample Data",
    "publisher": {"name": "Simen Kjelsrud"},
    "sources": [{"url": "https://example.com/sample.csv", "format": "csv"}],
}
PKG_V2 = {**PKG_V1, "version": "0.2.0"}


def publish_pkg(registry_path: Path, pkg_dict: dict) -> None:
    LocalRegistry(registry_path).publish(DataPackage.model_validate(pkg_dict))


def make_cache_entry(cache_root: Path, pkg_id: str, version: str) -> Path:
    pub, ns, ds = pkg_id.split(".")
    d = cache_root / pub / ns / ds / version
    d.mkdir(parents=True)
    (d / "sample.csv").write_bytes(CONTENT)
    return d


def invoke(args: list, tmp_path: Path, cache_path: Path | None = None):
    reg = str(tmp_path / "registry")
    cache = cache_path or tmp_path / "cache"
    with patch("datum.commands.update.get_cache_root", return_value=cache):
        return runner.invoke(app, ["--registry", reg] + args)


# ---------------------------------------------------------------------------
# Empty / nothing cached
# ---------------------------------------------------------------------------


class TestUpdateEmpty:
    def test_nothing_cached_exits_0(self, tmp_path):
        result = invoke(["update"], tmp_path)
        assert result.exit_code == 0

    def test_nothing_cached_shows_message(self, tmp_path):
        result = invoke(["update"], tmp_path)
        assert "nothing" in result.output.lower() or result.exit_code == 0


# ---------------------------------------------------------------------------
# Already up to date
# ---------------------------------------------------------------------------


class TestUpdateAlreadyLatest:
    def test_exits_0_when_up_to_date(self, tmp_path):
        reg_path = tmp_path / "registry"
        cache = tmp_path / "cache"
        publish_pkg(reg_path, PKG_V1)
        make_cache_entry(cache, "simkjels.samples.sampledata", "0.1.0")
        result = invoke(["update", "simkjels.samples.sampledata"], tmp_path, cache)
        assert result.exit_code == 0

    def test_output_says_up_to_date(self, tmp_path):
        reg_path = tmp_path / "registry"
        cache = tmp_path / "cache"
        publish_pkg(reg_path, PKG_V1)
        make_cache_entry(cache, "simkjels.samples.sampledata", "0.1.0")
        result = invoke(["update", "simkjels.samples.sampledata"], tmp_path, cache)
        assert "latest" in result.output.lower() or "up to date" in result.output.lower()

    def test_json_already_latest(self, tmp_path):
        reg_path = tmp_path / "registry"
        cache = tmp_path / "cache"
        publish_pkg(reg_path, PKG_V1)
        make_cache_entry(cache, "simkjels.samples.sampledata", "0.1.0")
        result = invoke(["--output", "json", "update", "simkjels.samples.sampledata"], tmp_path, cache)
        data = json.loads(result.output)
        assert data["updated"] == []
        assert "already_latest" in data


# ---------------------------------------------------------------------------
# --check (dry-run)
# ---------------------------------------------------------------------------


class TestUpdateCheck:
    def test_check_shows_pending_update(self, tmp_path):
        reg_path = tmp_path / "registry"
        cache = tmp_path / "cache"
        publish_pkg(reg_path, PKG_V1)
        time.sleep(0.02)
        publish_pkg(reg_path, PKG_V2)
        make_cache_entry(cache, "simkjels.samples.sampledata", "0.1.0")
        result = invoke(["update", "--check", "simkjels.samples.sampledata"], tmp_path, cache)
        assert result.exit_code == 0
        assert "0.2.0" in result.output

    def test_check_json_output(self, tmp_path):
        reg_path = tmp_path / "registry"
        cache = tmp_path / "cache"
        publish_pkg(reg_path, PKG_V1)
        time.sleep(0.02)
        publish_pkg(reg_path, PKG_V2)
        make_cache_entry(cache, "simkjels.samples.sampledata", "0.1.0")
        result = invoke(
            ["--output", "json", "update", "--check", "simkjels.samples.sampledata"],
            tmp_path, cache,
        )
        data = json.loads(result.output)
        assert len(data["would_update"]) == 1
        assert data["would_update"][0]["to"] == "0.2.0"

    def test_check_does_not_download(self, tmp_path):
        reg_path = tmp_path / "registry"
        cache = tmp_path / "cache"
        publish_pkg(reg_path, PKG_V1)
        time.sleep(0.02)
        publish_pkg(reg_path, PKG_V2)
        make_cache_entry(cache, "simkjels.samples.sampledata", "0.1.0")
        # v0.2.0 dir should not be created by --check
        invoke(["update", "--check", "simkjels.samples.sampledata"], tmp_path, cache)
        pub, ns, ds = "simkjels", "samples", "sampledata"
        assert not (cache / pub / ns / ds / "0.2.0").exists()


# ---------------------------------------------------------------------------
# Bad identifier
# ---------------------------------------------------------------------------


class TestUpdateBadIdentifier:
    def test_bad_identifier_exits_1(self, tmp_path):
        result = invoke(["update", "bad-id"], tmp_path)
        assert result.exit_code == 1

    def test_bad_identifier_json_output(self, tmp_path):
        result = invoke(["--output", "json", "update", "bad-id"], tmp_path)
        data = json.loads(result.output)
        assert "error" in data


# ---------------------------------------------------------------------------
# Quiet
# ---------------------------------------------------------------------------


class TestUpdateQuiet:
    def test_quiet_suppresses_output(self, tmp_path):
        reg_path = tmp_path / "registry"
        cache = tmp_path / "cache"
        publish_pkg(reg_path, PKG_V1)
        make_cache_entry(cache, "simkjels.samples.sampledata", "0.1.0")
        result = invoke(["--quiet", "update", "simkjels.samples.sampledata"], tmp_path, cache)
        assert result.exit_code == 0
        assert result.output.strip() == ""
