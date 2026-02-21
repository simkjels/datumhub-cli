"""Tests for `datum publish`."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from datum.main import app
from datum.models import DataPackage
from datum.registry.local import LocalRegistry

runner = CliRunner()

VALID_PKG = {
    "id": "simkjels/samples/sampledata",
    "version": "0.1.0",
    "title": "Sample Data Text File",
    "publisher": {"name": "Simen Kjelsrud"},
    "sources": [{"url": "https://example.com/sample.csv", "format": "csv"}],
}


def write_pkg(tmp_path: Path, data: dict) -> Path:
    f = tmp_path / "datapackage.json"
    f.write_text(json.dumps(data))
    return f


def invoke(args: list, tmp_path: Path) -> object:
    """Helper: injects --registry pointing at tmp_path/registry."""
    reg = str(tmp_path / "registry")
    return runner.invoke(app, ["--registry", reg] + args)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestPublishValid:
    def test_valid_file_exits_zero(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        result = invoke(["publish", str(f)], tmp_path)
        assert result.exit_code == 0, result.output

    def test_valid_file_written_to_registry(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        invoke(["publish", str(f)], tmp_path)
        reg = LocalRegistry(tmp_path / "registry")
        pkg = reg.get("simkjels/samples/sampledata", "0.1.0")
        assert pkg is not None
        assert pkg.id == "simkjels/samples/sampledata"

    def test_json_output_published_true(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        result = invoke(["--output", "json", "publish", str(f)], tmp_path)
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["published"] is True
        assert data["id"] == "simkjels/samples/sampledata"
        assert data["version"] == "0.1.0"
        assert "path" in data


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestPublishErrors:
    def test_missing_file_exits_2(self, tmp_path: Path):
        result = invoke(["publish", str(tmp_path / "missing.json")], tmp_path)
        assert result.exit_code == 2

    def test_bad_json_exits_2(self, tmp_path: Path):
        f = tmp_path / "datapackage.json"
        f.write_text("{bad json")
        result = invoke(["publish", str(f)], tmp_path)
        assert result.exit_code == 2

    def test_invalid_schema_exits_1(self, tmp_path: Path):
        f = write_pkg(tmp_path, {**VALID_PKG, "id": "bad-id"})
        result = invoke(["publish", str(f)], tmp_path)
        assert result.exit_code == 1

    def test_duplicate_exits_1_without_force(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        invoke(["publish", str(f)], tmp_path)
        result = invoke(["publish", str(f)], tmp_path)
        assert result.exit_code == 1

    def test_duplicate_with_force_exits_0(self, tmp_path: Path):
        f = write_pkg(tmp_path, VALID_PKG)
        invoke(["publish", str(f)], tmp_path)
        result = invoke(["publish", "--force", str(f)], tmp_path)
        assert result.exit_code == 0, result.output

    def test_missing_file_json_output(self, tmp_path: Path):
        result = invoke(
            ["--output", "json", "publish", str(tmp_path / "missing.json")], tmp_path
        )
        assert result.exit_code == 2
        data = json.loads(result.output)
        assert data["published"] is False
        assert "error" in data
