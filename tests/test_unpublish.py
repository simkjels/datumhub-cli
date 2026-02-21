"""Tests for `datum unpublish`."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from datum.main import app
from datum.models import DataPackage
from datum.registry.local import LocalRegistry

runner = CliRunner()

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


def invoke(args: list, tmp_path: Path):
    reg = str(tmp_path / "registry")
    return runner.invoke(app, ["--registry", reg] + args)


# ---------------------------------------------------------------------------
# Bad identifier
# ---------------------------------------------------------------------------


class TestUnpublishBadIdentifier:
    def test_bad_identifier_exits_1(self, tmp_path):
        result = invoke(["unpublish", "bad-id:1.0.0"], tmp_path)
        assert result.exit_code == 1

    def test_missing_version_exits_1(self, tmp_path):
        result = invoke(["unpublish", "simkjels.samples.sampledata"], tmp_path)
        assert result.exit_code == 1

    def test_bad_identifier_json_output(self, tmp_path):
        result = invoke(["--output", "json", "unpublish", "bad-id:1.0.0"], tmp_path)
        data = json.loads(result.output)
        assert data["unpublished"] is False
        assert "error" in data


# ---------------------------------------------------------------------------
# Not found
# ---------------------------------------------------------------------------


class TestUnpublishNotFound:
    def test_unknown_version_exits_1(self, tmp_path):
        result = invoke(["unpublish", "--yes", "simkjels.samples.sampledata:9.9.9"], tmp_path)
        assert result.exit_code == 1

    def test_unknown_version_json_output(self, tmp_path):
        result = invoke(
            ["--output", "json", "unpublish", "simkjels.samples.sampledata:9.9.9"], tmp_path
        )
        data = json.loads(result.output)
        assert data["unpublished"] is False

    def test_all_flag_unknown_dataset_exits_1(self, tmp_path):
        result = invoke(["unpublish", "--yes", "--all", "simkjels.samples.sampledata"], tmp_path)
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------


class TestUnpublishSuccess:
    def test_removes_version_exits_0(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_V1)
        result = invoke(["unpublish", "--yes", "simkjels.samples.sampledata:0.1.0"], tmp_path)
        assert result.exit_code == 0

    def test_removes_version_from_registry(self, tmp_path):
        reg_path = tmp_path / "registry"
        publish_pkg(reg_path, PKG_V1)
        invoke(["unpublish", "--yes", "simkjels.samples.sampledata:0.1.0"], tmp_path)
        assert LocalRegistry(reg_path).get("simkjels.samples.sampledata", "0.1.0") is None

    def test_output_contains_identifier(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_V1)
        result = invoke(["unpublish", "--yes", "simkjels.samples.sampledata:0.1.0"], tmp_path)
        assert "simkjels.samples.sampledata" in result.output

    def test_json_output(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_V1)
        result = invoke(
            ["--output", "json", "unpublish", "simkjels.samples.sampledata:0.1.0"], tmp_path
        )
        data = json.loads(result.output)
        assert data["unpublished"] is True
        assert data["id"] == "simkjels.samples.sampledata"
        assert "0.1.0" in data["versions"]

    def test_quiet_suppresses_output(self, tmp_path):
        publish_pkg(tmp_path / "registry", PKG_V1)
        result = invoke(
            ["--quiet", "unpublish", "--yes", "simkjels.samples.sampledata:0.1.0"], tmp_path
        )
        assert result.exit_code == 0
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# --all flag
# ---------------------------------------------------------------------------


class TestUnpublishAll:
    def test_removes_all_versions(self, tmp_path):
        reg_path = tmp_path / "registry"
        publish_pkg(reg_path, PKG_V1)
        publish_pkg(reg_path, PKG_V2)
        invoke(["unpublish", "--yes", "--all", "simkjels.samples.sampledata"], tmp_path)
        reg = LocalRegistry(reg_path)
        assert reg.get("simkjels.samples.sampledata", "0.1.0") is None
        assert reg.get("simkjels.samples.sampledata", "0.2.0") is None

    def test_all_json_output(self, tmp_path):
        reg_path = tmp_path / "registry"
        publish_pkg(reg_path, PKG_V1)
        publish_pkg(reg_path, PKG_V2)
        result = invoke(
            ["--output", "json", "unpublish", "--all", "simkjels.samples.sampledata"], tmp_path
        )
        data = json.loads(result.output)
        assert data["unpublished"] is True
        assert len(data["versions"]) == 2
