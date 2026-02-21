"""Tests for `datum search`."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from datum.main import app
from datum.models import DataPackage
from datum.registry.local import LocalRegistry

runner = CliRunner()

PKG_A = {
    "id": "met/no/oslo-hourly",
    "version": "1.0.0",
    "title": "Oslo Hourly Weather",
    "publisher": {"name": "Meteorologisk institutt"},
    "description": "Hourly weather observations for Oslo.",
    "tags": ["weather", "norway"],
    "sources": [{"url": "https://example.com/oslo.csv", "format": "csv"}],
}

PKG_B = {
    "id": "simkjels/samples/sampledata",
    "version": "0.1.0",
    "title": "Sample Data Text File",
    "publisher": {"name": "Simen Kjelsrud"},
    "sources": [{"url": "https://example.com/sample.csv", "format": "csv"}],
}


def publish_pkg(registry_path: Path, pkg_dict: dict) -> None:
    reg = LocalRegistry(registry_path)
    reg.publish(DataPackage.model_validate(pkg_dict))


def invoke(args: list, tmp_path: Path):
    reg = str(tmp_path / "registry")
    return runner.invoke(app, ["--registry", reg] + args)


# ---------------------------------------------------------------------------
# No matches
# ---------------------------------------------------------------------------


class TestSearchNoMatches:
    def test_exits_0_when_no_matches(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["search", "zzznomatch"], tmp_path)
        assert result.exit_code == 0

    def test_no_match_message_shown(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["search", "zzznomatch"], tmp_path)
        assert "zzznomatch" in result.output

    def test_exits_0_on_empty_registry(self, tmp_path):
        result = invoke(["search", "anything"], tmp_path)
        assert result.exit_code == 0

    def test_json_no_matches_returns_empty_array(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["--output", "json", "search", "zzznomatch"], tmp_path)
        assert result.exit_code == 0
        assert json.loads(result.output) == []


# ---------------------------------------------------------------------------
# Matching fields
# ---------------------------------------------------------------------------


class TestSearchMatching:
    def test_matches_id(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["search", "met/no"], tmp_path)
        assert result.exit_code == 0
        assert "met/no/oslo-hourly" in result.output

    def test_matches_title(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["search", "hourly weather"], tmp_path)
        assert "met/no/oslo-hourly" in result.output

    def test_matches_publisher(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["search", "meteorologisk"], tmp_path)
        assert "met/no/oslo-hourly" in result.output

    def test_matches_description(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["search", "observations"], tmp_path)
        assert "met/no/oslo-hourly" in result.output

    def test_matches_tag(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["search", "norway"], tmp_path)
        assert "met/no/oslo-hourly" in result.output

    def test_case_insensitive(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["search", "OSLO"], tmp_path)
        assert "met/no/oslo-hourly" in result.output


# ---------------------------------------------------------------------------
# Multiple results
# ---------------------------------------------------------------------------


class TestSearchMultiple:
    def test_returns_only_matching_packages(self, tmp_path):
        reg = tmp_path / "registry"
        publish_pkg(reg, PKG_A)
        publish_pkg(reg, PKG_B)
        result = invoke(["search", "oslo"], tmp_path)
        assert "met/no/oslo-hourly" in result.output
        assert "simkjels/samples/sampledata" not in result.output

    def test_returns_all_matching_packages(self, tmp_path):
        reg = tmp_path / "registry"
        publish_pkg(reg, PKG_A)
        publish_pkg(reg, PKG_B)
        result = invoke(["search", "sample"], tmp_path)
        assert "simkjels/samples/sampledata" in result.output
        assert "met/no/oslo-hourly" not in result.output

    def test_result_count_shown(self, tmp_path):
        reg = tmp_path / "registry"
        publish_pkg(reg, PKG_A)
        publish_pkg(reg, PKG_B)
        # Both have a source at example.com — but we search by metadata fields only
        result = invoke(["search", "oslo"], tmp_path)
        assert "1" in result.output


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


class TestSearchJson:
    def test_json_returns_array(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["--output", "json", "search", "oslo"], tmp_path)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == "met/no/oslo-hourly"

    def test_json_includes_full_package(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["--output", "json", "search", "oslo"], tmp_path)
        data = json.loads(result.output)
        assert data[0]["title"] == "Oslo Hourly Weather"
        assert isinstance(data[0]["sources"], list)


# ---------------------------------------------------------------------------
# Quiet mode
# ---------------------------------------------------------------------------


class TestSearchQuiet:
    def test_quiet_suppresses_output(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_A)
        result = invoke(["--quiet", "search", "oslo"], tmp_path)
        assert result.exit_code == 0
        assert result.output.strip() == ""
