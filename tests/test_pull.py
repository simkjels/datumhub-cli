"""Tests for `datum pull`."""

from __future__ import annotations

import hashlib
import json
import time
from contextlib import contextmanager
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import httpx
import pytest
from typer.testing import CliRunner

from datum.main import app
from datum.models import DataPackage
from datum.registry.local import LocalRegistry

runner = CliRunner(mix_stderr=False)

VALID_PKG = {
    "id": "simkjels.samples.sampledata",
    "version": "0.1.0",
    "title": "Sample Data Text File",
    "publisher": {"name": "Simen Kjelsrud"},
    "sources": [{"url": "https://example.com/sample.csv", "format": "csv"}],
}

CONTENT = b"col1,col2\n1,2\n3,4\n"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_fake_stream(chunks: List[bytes]):
    """Return a patched httpx.stream that yields the given byte chunks."""

    @contextmanager
    def _stream(*args, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.iter_bytes.return_value = iter(chunks)
        mock_resp.raise_for_status = MagicMock()
        yield mock_resp

    return _stream


def make_error_stream():
    """Return a patched httpx.stream that raises a network error."""

    @contextmanager
    def _stream(*args, **kwargs):
        raise httpx.HTTPError("network error")
        yield  # noqa: unreachable — needed to make this a generator

    return _stream


def publish_pkg(registry_path: Path, pkg_dict: dict) -> None:
    reg = LocalRegistry(registry_path)
    pkg = DataPackage.model_validate(pkg_dict)
    reg.publish(pkg)


def invoke(args: list, tmp_path: Path, cache_path: Path, dest_root: Path | None = None):
    """Invoke the CLI with isolated registry, cache, and destination directories."""
    reg = str(tmp_path / "registry")
    if dest_root is None:
        dest_root = tmp_path / "dest"
    with (
        patch("datum.commands.pull.get_cache_root", return_value=cache_path),
        patch("datum.commands.pull.get_dest_root", return_value=dest_root),
    ):
        return runner.invoke(app, ["--registry", reg] + args)


# ---------------------------------------------------------------------------
# Identifier validation
# ---------------------------------------------------------------------------


class TestPullBadIdentifier:
    def test_two_part_identifier_exits_1(self, tmp_path):
        cache = tmp_path / "cache"
        result = invoke(["pull", "bad.identifier"], tmp_path, cache)
        assert result.exit_code == 1

    def test_uppercase_slug_exits_1(self, tmp_path):
        cache = tmp_path / "cache"
        result = invoke(["pull", "Bad.x.y:1.0"], tmp_path, cache)
        assert result.exit_code == 1

    def test_bad_identifier_json_output(self, tmp_path):
        cache = tmp_path / "cache"
        result = invoke(["--output", "json", "pull", "bad.id"], tmp_path, cache)
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["downloaded"] is False
        assert "error" in data


# ---------------------------------------------------------------------------
# Registry lookup
# ---------------------------------------------------------------------------


class TestPullNotFound:
    def test_unknown_package_exits_1(self, tmp_path):
        cache = tmp_path / "cache"
        result = invoke(["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache)
        assert result.exit_code == 1

    def test_unknown_package_json_output(self, tmp_path):
        cache = tmp_path / "cache"
        result = invoke(
            ["--output", "json", "pull", "simkjels.samples.sampledata:0.1.0"],
            tmp_path,
            cache,
        )
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["downloaded"] is False
        assert "error" in data


# ---------------------------------------------------------------------------
# Download success
# ---------------------------------------------------------------------------


class TestPullDownload:
    def test_download_success_exits_0(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        with patch("httpx.stream", make_fake_stream([CONTENT])):
            result = invoke(["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache)
        assert result.exit_code == 0, result.output

    def test_download_creates_file_in_dest(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        dest_root = tmp_path / "dest"
        with patch("httpx.stream", make_fake_stream([CONTENT])):
            invoke(["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache, dest_root)
        dest_dir = dest_root / "sampledata"
        files = list(dest_dir.iterdir())
        assert len(files) == 1
        assert files[0].name == "sample.csv"

    def test_download_also_writes_to_cache(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        with patch("httpx.stream", make_fake_stream([CONTENT])):
            invoke(["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache)
        cache_file = cache / "simkjels" / "samples" / "sampledata" / "0.1.0" / "sample.csv"
        assert cache_file.exists()

    def test_cached_skips_http(self, tmp_path):
        """File already in dest dir — no HTTP."""
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        dest_root = tmp_path / "dest"
        dest_dir = dest_root / "sampledata"
        dest_dir.mkdir(parents=True)
        (dest_dir / "sample.csv").write_bytes(CONTENT)

        mock_stream = MagicMock()
        with patch("httpx.stream", mock_stream):
            result = invoke(
                ["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache, dest_root
            )

        assert result.exit_code == 0
        mock_stream.assert_not_called()

    def test_uses_cache_avoids_http(self, tmp_path):
        """File absent from dest but present in cache — copy, no HTTP."""
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        dest_root = tmp_path / "dest"

        cache_dir = cache / "simkjels" / "samples" / "sampledata" / "0.1.0"
        cache_dir.mkdir(parents=True)
        (cache_dir / "sample.csv").write_bytes(CONTENT)

        mock_stream = MagicMock()
        with patch("httpx.stream", mock_stream):
            result = invoke(
                ["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache, dest_root
            )

        assert result.exit_code == 0
        mock_stream.assert_not_called()
        assert (dest_root / "sampledata" / "sample.csv").read_bytes() == CONTENT

    def test_force_redownloads_cached_file(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        dest_root = tmp_path / "dest"

        # Pre-populate both cache and dest with old content
        cache_dir = cache / "simkjels" / "samples" / "sampledata" / "0.1.0"
        cache_dir.mkdir(parents=True)
        (cache_dir / "sample.csv").write_bytes(b"old content")
        dest_dir = dest_root / "sampledata"
        dest_dir.mkdir(parents=True)
        (dest_dir / "sample.csv").write_bytes(b"old content")

        with patch("httpx.stream", make_fake_stream([CONTENT])):
            result = invoke(
                ["pull", "--force", "simkjels.samples.sampledata:0.1.0"],
                tmp_path,
                cache,
                dest_root,
            )

        assert result.exit_code == 0
        assert (dest_dir / "sample.csv").read_bytes() == CONTENT


# ---------------------------------------------------------------------------
# Checksum verification
# ---------------------------------------------------------------------------


class TestPullChecksum:
    def test_correct_checksum_exits_0(self, tmp_path):
        digest = hashlib.sha256(CONTENT).hexdigest()
        pkg = {
            **VALID_PKG,
            "sources": [
                {
                    "url": "https://example.com/sample.csv",
                    "format": "csv",
                    "checksum": f"sha256:{digest}",
                }
            ],
        }
        publish_pkg(tmp_path / "registry", pkg)
        cache = tmp_path / "cache"
        with patch("httpx.stream", make_fake_stream([CONTENT])):
            result = invoke(["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache)
        assert result.exit_code == 0

    def test_wrong_checksum_exits_1(self, tmp_path):
        pkg = {
            **VALID_PKG,
            "sources": [
                {
                    "url": "https://example.com/sample.csv",
                    "format": "csv",
                    "checksum": "sha256:" + "a" * 64,
                }
            ],
        }
        publish_pkg(tmp_path / "registry", pkg)
        cache = tmp_path / "cache"
        with patch("httpx.stream", make_fake_stream([CONTENT])):
            result = invoke(["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache)
        assert result.exit_code == 1

    def test_wrong_checksum_deletes_cache_file(self, tmp_path):
        pkg = {
            **VALID_PKG,
            "sources": [
                {
                    "url": "https://example.com/sample.csv",
                    "format": "csv",
                    "checksum": "sha256:" + "b" * 64,
                }
            ],
        }
        publish_pkg(tmp_path / "registry", pkg)
        cache = tmp_path / "cache"
        dest_root = tmp_path / "dest"
        with patch("httpx.stream", make_fake_stream([CONTENT])):
            invoke(["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache, dest_root)
        cache_file = cache / "simkjels" / "samples" / "sampledata" / "0.1.0" / "sample.csv"
        dest_file = dest_root / "sampledata" / "sample.csv"
        assert not cache_file.exists()
        assert not dest_file.exists()


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


class TestPullJsonOutput:
    def test_json_downloaded_true(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        dest_root = tmp_path / "dest"
        with patch("httpx.stream", make_fake_stream([CONTENT])):
            result = invoke(
                ["--output", "json", "pull", "simkjels.samples.sampledata:0.1.0"],
                tmp_path,
                cache,
                dest_root,
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["downloaded"] is True
        assert data["id"] == "simkjels.samples.sampledata"
        assert data["version"] == "0.1.0"
        assert isinstance(data["files"], list)
        assert len(data["files"]) == 1
        assert "sampledata" in data["files"][0]
        assert "sample.csv" in data["files"][0]

    def test_json_cached_file_included(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        dest_root = tmp_path / "dest"

        # File present in cache but not dest
        cache_dir = cache / "simkjels" / "samples" / "sampledata" / "0.1.0"
        cache_dir.mkdir(parents=True)
        (cache_dir / "sample.csv").write_bytes(CONTENT)

        result = invoke(
            ["--output", "json", "pull", "simkjels.samples.sampledata:0.1.0"],
            tmp_path,
            cache,
            dest_root,
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["downloaded"] is True
        assert len(data["files"]) == 1
        assert "sampledata" in data["files"][0]


# ---------------------------------------------------------------------------
# Network errors
# ---------------------------------------------------------------------------


class TestPullNetworkError:
    def test_network_error_exits_2(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        with patch("httpx.stream", make_error_stream()):
            result = invoke(["pull", "simkjels.samples.sampledata:0.1.0"], tmp_path, cache)
        assert result.exit_code == 2

    def test_network_error_json_output(self, tmp_path):
        publish_pkg(tmp_path / "registry", VALID_PKG)
        cache = tmp_path / "cache"
        with patch("httpx.stream", make_error_stream()):
            result = invoke(
                ["--output", "json", "pull", "simkjels.samples.sampledata:0.1.0"],
                tmp_path,
                cache,
            )
        assert result.exit_code == 2
        data = json.loads(result.output)
        assert data["downloaded"] is False
        assert "error" in data


# ---------------------------------------------------------------------------
# Latest resolution
# ---------------------------------------------------------------------------


class TestPullLatest:
    def test_latest_resolves_most_recent_version(self, tmp_path):
        reg_path = tmp_path / "registry"

        publish_pkg(reg_path, VALID_PKG)
        time.sleep(0.02)  # ensure different mtime
        publish_pkg(reg_path, {**VALID_PKG, "version": "0.2.0"})

        cache = tmp_path / "cache"
        dest_root = tmp_path / "dest"
        with patch("httpx.stream", make_fake_stream([CONTENT])):
            result = invoke(["pull", "simkjels.samples.sampledata"], tmp_path, cache, dest_root)

        assert result.exit_code == 0
        assert (dest_root / "sampledata" / "sample.csv").exists()

    def test_latest_not_in_registry_exits_1(self, tmp_path):
        cache = tmp_path / "cache"
        result = invoke(["pull", "simkjels.samples.sampledata"], tmp_path, cache)
        assert result.exit_code == 1
