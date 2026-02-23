"""Tests for `datum cache list`, `datum cache size`, `datum cache clear`."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from datum.main import app

runner = CliRunner()

CONTENT = b"col1,col2\n1,2\n3,4\n"


def invoke(args: list, cache_path: Path):
    with patch("datum.commands.cache.get_cache_root", return_value=cache_path):
        return runner.invoke(app, args)


def make_cache_entry(cache_root: Path, publisher: str, ns: str, ds: str, version: str, files: dict):
    """Create a fake cache entry with given files {filename: bytes}."""
    d = cache_root / publisher / ns / ds / version
    d.mkdir(parents=True)
    for name, content in files.items():
        (d / name).write_bytes(content)
    return d


# ---------------------------------------------------------------------------
# cache list
# ---------------------------------------------------------------------------


class TestCacheList:
    def test_empty_cache_exits_0(self, tmp_path):
        result = invoke(["cache", "list"], tmp_path / "cache")
        assert result.exit_code == 0

    def test_empty_cache_shows_message(self, tmp_path):
        result = invoke(["cache", "list"], tmp_path / "cache")
        assert "empty" in result.output.lower()

    def test_lists_cached_entry(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        result = invoke(["cache", "list"], cache)
        assert result.exit_code == 0
        assert "met/no/oslo-hourly" in result.output
        assert "1.0.0" in result.output

    def test_lists_multiple_entries(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        make_cache_entry(cache, "simkjels", "samples", "sampledata", "0.1.0", {"sample.csv": CONTENT})
        result = invoke(["cache", "list"], cache)
        assert "met/no/oslo-hourly" in result.output
        assert "simkjels/samples/sampledata" in result.output

    def test_json_output(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        result = invoke(["--output", "json", "cache", "list"], cache)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == "met/no/oslo-hourly"
        assert data[0]["version"] == "1.0.0"
        assert data[0]["size"] == len(CONTENT)

    def test_quiet_suppresses_output(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        result = invoke(["--quiet", "cache", "list"], cache)
        assert result.exit_code == 0
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# cache size
# ---------------------------------------------------------------------------


class TestCacheSize:
    def test_empty_cache_exits_0(self, tmp_path):
        result = invoke(["cache", "size"], tmp_path / "cache")
        assert result.exit_code == 0

    def test_shows_size(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        result = invoke(["cache", "size"], cache)
        assert result.exit_code == 0
        assert "B" in result.output  # some size unit shown

    def test_json_output(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        result = invoke(["--output", "json", "cache", "size"], cache)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["size_bytes"] == len(CONTENT)
        assert data["files"] == 1

    def test_json_empty_cache(self, tmp_path):
        result = invoke(["--output", "json", "cache", "size"], tmp_path / "cache")
        data = json.loads(result.output)
        assert data["size_bytes"] == 0
        assert data["files"] == 0

    def test_quiet_suppresses_output(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        result = invoke(["--quiet", "cache", "size"], cache)
        assert result.exit_code == 0
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# cache clear
# ---------------------------------------------------------------------------


class TestCacheClear:
    def test_empty_cache_exits_0(self, tmp_path):
        result = invoke(["cache", "clear", "--yes"], tmp_path / "cache")
        assert result.exit_code == 0

    def test_clear_removes_files(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        invoke(["cache", "clear", "--yes"], cache)
        assert not cache.exists() or not any(cache.rglob("*"))

    def test_clear_shows_confirmation(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        result = invoke(["cache", "clear", "--yes"], cache)
        assert result.exit_code == 0
        assert "cleared" in result.output.lower()

    def test_abort_without_yes_keeps_files(self, tmp_path):
        cache = tmp_path / "cache"
        entry = make_cache_entry(
            cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT}
        )
        # Simulate user typing "n" at the prompt
        result = runner.invoke(
            app,
            ["cache", "clear"],
            input="n\n",
            catch_exceptions=False,
        )
        # Files should still be there (patching not used here — real cache not touched)
        assert (entry / "data.csv").exists()

    def test_clear_with_dataset_removes_only_that_dataset(self, tmp_path):
        cache = tmp_path / "cache"
        make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        entry2 = make_cache_entry(cache, "simkjels", "samples", "sampledata", "0.1.0", {"s.csv": CONTENT})
        result = invoke(["cache", "clear", "--yes", "--dataset", "met/no/oslo-hourly"], cache)
        assert result.exit_code == 0
        assert not (cache / "met" / "no" / "oslo-hourly").exists()
        assert (entry2 / "s.csv").exists()

    def test_clear_with_versioned_dataset(self, tmp_path):
        cache = tmp_path / "cache"
        v1 = make_cache_entry(cache, "met", "no", "oslo-hourly", "1.0.0", {"data.csv": CONTENT})
        v2 = make_cache_entry(cache, "met", "no", "oslo-hourly", "2.0.0", {"data.csv": CONTENT})
        result = invoke(["cache", "clear", "--yes", "--dataset", "met/no/oslo-hourly:1.0.0"], cache)
        assert result.exit_code == 0
        assert not v1.exists()
        assert v2.exists()

    def test_clear_dataset_invalid_identifier_exits_1(self, tmp_path):
        result = invoke(["cache", "clear", "--dataset", "bad/id"], tmp_path / "cache")
        assert result.exit_code == 1

    def test_clear_dataset_not_cached_shows_message(self, tmp_path):
        result = invoke(["cache", "clear", "--yes", "--dataset", "met/no/oslo-hourly"], tmp_path / "cache")
        assert result.exit_code == 0
        assert "No cached" in result.output


# ---------------------------------------------------------------------------
# cache path
# ---------------------------------------------------------------------------


class TestCachePath:
    def test_prints_path_for_valid_identifier(self, tmp_path):
        result = invoke(["cache", "path", "met/no/oslo-hourly"], tmp_path / "cache")
        assert result.exit_code == 0
        assert "met" in result.output
        assert "oslo-hourly" in result.output

    def test_exits_1_for_invalid_identifier(self, tmp_path):
        result = invoke(["cache", "path", "bad/id"], tmp_path / "cache")
        assert result.exit_code == 1

    def test_strips_version_from_identifier(self, tmp_path):
        result = invoke(["cache", "path", "met/no/oslo-hourly:1.0.0"], tmp_path / "cache")
        assert result.exit_code == 0
        # Should not include version in the path output (path is per-dataset, not version)
        assert "no/oslo-hourly" in result.output
