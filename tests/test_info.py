"""Tests for `datum info`."""

from __future__ import annotations

import json
import time
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
    "publisher": {"name": "Simen Kjelsrud", "url": "https://example.com"},
    "description": "A sample CSV dataset.",
    "license": "CC-BY-4.0",
    "tags": ["sample", "csv"],
    "sources": [
        {
            "url": "https://example.com/sample.csv",
            "format": "csv",
            "size": 2048,
            "checksum": "sha256:" + "a" * 64,
        }
    ],
}


def publish_pkg(registry_path: Path, pkg_dict: dict) -> None:
    reg = LocalRegistry(registry_path)
    pkg = DataPackage.model_validate(pkg_dict)
    reg.publish(pkg)


def invoke(args: list, tmp_path: Path):
    reg = str(tmp_path / "registry")
    return runner.invoke(app, ["--registry", reg] + args)


# ---------------------------------------------------------------------------
# Identifier validation
# ---------------------------------------------------------------------------


class TestInfoBadIdentifier:
    def test_two_part_identifier_exits_1(self, tmp_path):
        result = invoke(["info", "bad/identifier"], tmp_path)
        assert result.exit_code == 1

    def test_uppercase_slug_exits_1(self, tmp_path):
        result = invoke(["info", "Bad/x/y"], tmp_path)
        assert result.exit_code == 1

    def test_bad_identifier_json_output(self, tmp_path):
        result = invoke(["--output", "json", "info", "bad/id"], tmp_path)
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "error" in data


# ---------------------------------------------------------------------------
# Not found
# ---------------------------------------------------------------------------


class TestInfoNotFound:
    def test_unknown_package_exits_1(self, tmp_path):
        result = invoke(["info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert result.exit_code == 1

    def test_unknown_package_json_output(self, tmp_path):
        result = invoke(["--output", "json", "info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "error" in data


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------


class TestInfoSuccess:
    def test_exits_0_for_known_package(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert result.exit_code == 0, result.output

    def test_output_contains_id(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert "simkjels/samples/sampledata" in result.output

    def test_output_contains_title(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert "Sample Data Text File" in result.output

    def test_output_contains_publisher(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert "Simen Kjelsrud" in result.output

    def test_output_contains_source_url(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        # Rich may truncate long URLs in the table; check the visible prefix
        assert "https://example.com" in result.output

    def test_output_contains_optional_fields(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert "CC-BY-4.0" in result.output
        assert "sample" in result.output  # tag

    def test_quiet_suppresses_output(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["--quiet", "info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert result.exit_code == 0
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# Latest resolution
# ---------------------------------------------------------------------------


class TestInfoLatest:
    def test_latest_resolves_most_recent_version(self, tmp_path):
        reg_path = tmp_path / "registry"
        publish_pkg(reg_path, VALID_PKG)
        time.sleep(0.02)
        publish_pkg(reg_path, {**VALID_PKG, "version": "0.2.0"})
        result = invoke(["info", "simkjels/samples/sampledata"], tmp_path)
        assert result.exit_code == 0
        assert "0.2.0" in result.output

    def test_latest_not_found_exits_1(self, tmp_path):
        result = invoke(["info", "simkjels/samples/sampledata"], tmp_path)
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


class TestInfoJson:
    def test_json_output_is_valid(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["--output", "json", "info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["id"] == "simkjels/samples/sampledata"
        assert data["version"] == "0.1.0"
        assert data["title"] == "Sample Data Text File"
        assert isinstance(data["sources"], list)
        assert len(data["sources"]) == 1

    def test_json_includes_optional_fields(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        result = invoke(["--output", "json", "info", "simkjels/samples/sampledata:0.1.0"], tmp_path)
        data = json.loads(result.output)
        assert data["description"] == "A sample CSV dataset."
        assert data["license"] == "CC-BY-4.0"
        assert "sample" in data["tags"]
