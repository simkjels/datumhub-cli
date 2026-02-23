"""Tests for `datum list`."""

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


def invoke(args: list, tmp_path: Path) -> object:
    reg = str(tmp_path / "registry")
    return runner.invoke(app, ["--registry", reg] + args)


# ---------------------------------------------------------------------------
# Empty registry
# ---------------------------------------------------------------------------


class TestListEmpty:
    def test_empty_registry_exits_0(self, tmp_path: Path):
        result = invoke(["list"], tmp_path)
        assert result.exit_code == 0

    def test_empty_registry_shows_message(self, tmp_path: Path):
        result = invoke(["list"], tmp_path)
        assert "No datasets" in result.output


# ---------------------------------------------------------------------------
# Registry with data
# ---------------------------------------------------------------------------


class TestListWithData:
    def test_one_dataset_exits_0(self, tmp_path: Path):
        LocalRegistry(tmp_path / "registry").publish(DataPackage.model_validate(VALID_PKG))
        result = invoke(["list"], tmp_path)
        assert result.exit_code == 0

    def test_one_dataset_id_in_output(self, tmp_path: Path):
        LocalRegistry(tmp_path / "registry").publish(DataPackage.model_validate(VALID_PKG))
        result = invoke(["list"], tmp_path)
        assert "simkjels/samples/sampledata" in result.output

    def test_json_output_valid_array(self, tmp_path: Path):
        LocalRegistry(tmp_path / "registry").publish(DataPackage.model_validate(VALID_PKG))
        result = invoke(["--output", "json", "list"], tmp_path)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == "simkjels/samples/sampledata"


# ---------------------------------------------------------------------------
# Quiet flag
# ---------------------------------------------------------------------------


class TestListQuiet:
    def test_quiet_with_results_exits_0(self, tmp_path: Path):
        LocalRegistry(tmp_path / "registry").publish(DataPackage.model_validate(VALID_PKG))
        result = invoke(["--quiet", "list"], tmp_path)
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# Pattern filter
# ---------------------------------------------------------------------------


class TestListPattern:
    def _setup(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        reg.publish(DataPackage.model_validate(VALID_PKG))
        reg.publish(DataPackage.model_validate({
            **VALID_PKG,
            "id": "other/ns/dataset",
            "version": "1.0.0",
        }))

    def test_pattern_filters_by_publisher(self, tmp_path: Path):
        self._setup(tmp_path)
        result = invoke(["list", "simkjels/*"], tmp_path)
        assert result.exit_code == 0
        assert "simkjels/samples/sampledata" in result.output
        assert "other/ns/dataset" not in result.output

    def test_pattern_no_match_shows_empty(self, tmp_path: Path):
        self._setup(tmp_path)
        result = invoke(["list", "nobody/*"], tmp_path)
        assert result.exit_code == 0
        assert "No datasets" in result.output

    def test_pattern_json_output_filtered(self, tmp_path: Path):
        self._setup(tmp_path)
        result = invoke(["--output", "json", "list", "simkjels/*"], tmp_path)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["id"] == "simkjels/samples/sampledata"
