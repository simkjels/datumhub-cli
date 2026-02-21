"""Tests for `datum check`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from datum.main import app
from datum.state import state, OutputFormat

runner = CliRunner()

VALID_PKG = {
    "id": "met.no.oslo-hourly",
    "version": "2024-01",
    "title": "Oslo Hourly Weather Data",
    "publisher": {"name": "Norwegian Meteorological Institute", "url": "https://met.no"},
    "sources": [{"url": "https://met.no/data/oslo.csv", "format": "csv"}],
}


def write_pkg(tmp_path: Path, data: dict) -> Path:
    f = tmp_path / "datapackage.json"
    f.write_text(json.dumps(data))
    return f


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestCheckValid:
    def test_exits_zero_for_valid_file(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        result = runner.invoke(app, ["check", str(f)])
        assert result.exit_code == 0, result.output

    def test_output_contains_identifier(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        result = runner.invoke(app, ["check", str(f)])
        assert "met.no.oslo-hourly" in result.output

    def test_json_output_valid_true(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        result = runner.invoke(app, ["--output", "json", "check", str(f)])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["valid"] is True
        assert data["errors"] == []
        assert "package" in data

    def test_json_output_includes_package(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        result = runner.invoke(app, ["--output", "json", "check", str(f)])
        data = json.loads(result.output)
        assert data["package"]["id"] == "met.no.oslo-hourly"

    def test_quiet_flag_suppresses_table(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        result = runner.invoke(app, ["--quiet", "check", str(f)])
        assert result.exit_code == 0
        # Quiet mode should produce no table output
        assert "met.no.oslo-hourly" not in result.output


# ---------------------------------------------------------------------------
# File errors
# ---------------------------------------------------------------------------


class TestCheckFileErrors:
    def test_missing_file_exits_2(self, tmp_path: Path):
        result = runner.invoke(app, ["check", str(tmp_path / "missing.json")])
        assert result.exit_code == 2

    def test_missing_file_json_output(self, tmp_path: Path):
        result = runner.invoke(app, ["--output", "json", "check", str(tmp_path / "missing.json")])
        assert result.exit_code == 2
        data = json.loads(result.output)
        assert data["valid"] is False
        assert any("not found" in e["message"].lower() for e in data["errors"])

    def test_invalid_json_exits_2(self, tmp_path: Path):
        f = tmp_path / "datapackage.json"
        f.write_text("{not valid json")
        result = runner.invoke(app, ["check", str(f)])
        assert result.exit_code == 2

    def test_invalid_json_output(self, tmp_path: Path):
        f = tmp_path / "datapackage.json"
        f.write_text("{bad}")
        result = runner.invoke(app, ["--output", "json", "check", str(f)])
        assert result.exit_code == 2
        data = json.loads(result.output)
        assert data["valid"] is False


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


class TestCheckInvalid:
    def test_bad_id_exits_1(self, tmp_path: Path):
        pkg = {**VALID_PKG, "id": "bad-id"}
        f = write_pkg(tmp_path, pkg)
        result = runner.invoke(app, ["check", str(f)])
        assert result.exit_code == 1

    def test_bad_id_json_output(self, tmp_path: Path):
        pkg = {**VALID_PKG, "id": "bad-id"}
        f = write_pkg(tmp_path, pkg)
        result = runner.invoke(app, ["--output", "json", "check", str(f)])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["valid"] is False
        assert len(data["errors"]) > 0
        assert any(e["field"] == "id" for e in data["errors"])

    def test_missing_required_field(self, tmp_path: Path):
        pkg = {k: v for k, v in VALID_PKG.items() if k != "title"}
        f = write_pkg(tmp_path, pkg)
        result = runner.invoke(app, ["check", str(f)])
        assert result.exit_code == 1

    def test_empty_sources(self, tmp_path: Path):
        pkg = {**VALID_PKG, "sources": []}
        f = write_pkg(tmp_path, pkg)
        result = runner.invoke(app, ["check", str(f)])
        assert result.exit_code == 1

    def test_bad_checksum_format(self, tmp_path: Path):
        pkg = {
            **VALID_PKG,
            "sources": [{"url": "https://x.com/f.csv", "format": "csv", "checksum": "notvalid"}],
        }
        f = write_pkg(tmp_path, pkg)
        result = runner.invoke(app, ["--output", "json", "check", str(f)])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert any("checksum" in e["field"] for e in data["errors"])

    def test_multiple_errors_all_reported(self, tmp_path: Path):
        pkg = {**VALID_PKG, "id": "BAD", "title": ""}
        f = write_pkg(tmp_path, pkg)
        result = runner.invoke(app, ["--output", "json", "check", str(f)])
        data = json.loads(result.output)
        assert len(data["errors"]) >= 2

    def test_default_file_path(self, tmp_path: Path):
        """datum check with no args looks for datapackage.json in cwd."""
        # No file → exit 2
        result = runner.invoke(app, ["check"], catch_exceptions=False)
        assert result.exit_code == 2
