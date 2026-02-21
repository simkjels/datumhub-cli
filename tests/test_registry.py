"""Tests for LocalRegistry."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datum.models import DataPackage
from datum.registry.local import LocalRegistry

VALID_PKG_1 = {
    "id": "met/no/oslo-hourly",
    "version": "2024-01",
    "title": "Oslo Hourly Weather Data",
    "publisher": {"name": "Norwegian Meteorological Institute", "url": "https://met.no"},
    "sources": [{"url": "https://met.no/data/oslo.csv", "format": "csv"}],
}

VALID_PKG_2 = {
    "id": "simkjels/samples/sampledata",
    "version": "0.1.0",
    "title": "Sample Data Text File",
    "publisher": {"name": "Simen Kjelsrud"},
    "sources": [{"url": "https://example.com/sample.csv", "format": "csv"}],
}


def make_pkg(data: dict) -> DataPackage:
    return DataPackage.model_validate(data)


# ---------------------------------------------------------------------------
# publish()
# ---------------------------------------------------------------------------


class TestLocalRegistryPublish:
    def test_publish_creates_correct_path(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        pkg = make_pkg(VALID_PKG_1)
        path = reg.publish(pkg)
        expected = tmp_path / "registry" / "met" / "no" / "oslo-hourly" / "2024-01.json"
        assert path == expected
        assert path.exists()

    def test_publish_writes_valid_json(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        pkg = make_pkg(VALID_PKG_1)
        path = reg.publish(pkg)
        data = json.loads(path.read_text())
        assert data["id"] == "met/no/oslo-hourly"
        assert data["version"] == "2024-01"

    def test_publish_raises_on_duplicate(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        pkg = make_pkg(VALID_PKG_1)
        reg.publish(pkg)
        with pytest.raises(FileExistsError):
            reg.publish(pkg)

    def test_publish_overwrite_passes(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        pkg = make_pkg(VALID_PKG_1)
        reg.publish(pkg)
        path = reg.publish(pkg, overwrite=True)
        assert path.exists()


# ---------------------------------------------------------------------------
# list()
# ---------------------------------------------------------------------------


class TestLocalRegistryList:
    def test_list_returns_empty_for_missing_root(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "nonexistent")
        assert reg.list() == []

    def test_list_finds_published_packages(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        reg.publish(make_pkg(VALID_PKG_1))
        reg.publish(make_pkg(VALID_PKG_2))
        results = reg.list()
        assert len(results) == 2
        ids = {p.id for p in results}
        assert "met/no/oslo-hourly" in ids
        assert "simkjels/samples/sampledata" in ids

    def test_list_skips_corrupt_files(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        reg.publish(make_pkg(VALID_PKG_1))
        corrupt = tmp_path / "registry" / "bad" / "bad" / "bad" / "corrupt.json"
        corrupt.parent.mkdir(parents=True)
        corrupt.write_text("{not valid json")
        results = reg.list()
        assert len(results) == 1


# ---------------------------------------------------------------------------
# get()
# ---------------------------------------------------------------------------


class TestLocalRegistryGet:
    def test_get_returns_none_for_missing(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        result = reg.get("met/no/oslo-hourly", "2024-01")
        assert result is None

    def test_get_returns_package(self, tmp_path: Path):
        reg = LocalRegistry(tmp_path / "registry")
        reg.publish(make_pkg(VALID_PKG_1))
        result = reg.get("met/no/oslo-hourly", "2024-01")
        assert result is not None
        assert result.id == "met/no/oslo-hourly"
        assert result.version == "2024-01"
