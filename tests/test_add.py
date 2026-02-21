"""Tests for `datum add`."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from typer.testing import CliRunner

from datum.main import app

runner = CliRunner()

CONTENT = b"col1,col2\n1,2\n3,4\n"

VALID_PKG = {
    "id": "simkjels/samples/sampledata",
    "version": "0.1.0",
    "title": "Sample",
    "publisher": {"name": "Simen"},
    "sources": [],
}


def write_pkg(tmp_path: Path, data: dict) -> Path:
    f = tmp_path / "datapackage.json"
    f.write_text(json.dumps(data))
    return f


def invoke(args: list, cwd: Path | None = None):
    """Invoke datum add; optionally change the working directory."""
    if cwd:
        import os
        old = os.getcwd()
        os.chdir(cwd)
        try:
            return runner.invoke(app, args)
        finally:
            os.chdir(old)
    return runner.invoke(app, args)


def make_fake_stream(content: bytes):
    """Return a patched httpx.stream that yields content."""
    from contextlib import contextmanager

    @contextmanager
    def _stream(*args, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"content-length": str(len(content))}
        mock_resp.iter_bytes.return_value = iter([content])
        mock_resp.raise_for_status = MagicMock()
        yield mock_resp

    return _stream


def make_head_resp(content_length: int):
    resp = MagicMock()
    resp.headers = {"content-length": str(content_length)}
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# No datapackage.json
# ---------------------------------------------------------------------------


class TestAddNoPackage:
    def test_missing_pkg_exits_1(self, tmp_path):
        result = invoke(["add", "https://example.com/data.csv"], cwd=tmp_path)
        assert result.exit_code == 1

    def test_missing_pkg_json_output(self, tmp_path):
        result = invoke(["--output", "json", "add", "https://example.com/data.csv"], cwd=tmp_path)
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["added"] == 0
        assert "error" in data


# ---------------------------------------------------------------------------
# Single URL — checksum computed
# ---------------------------------------------------------------------------


class TestAddSingleUrl:
    def test_exits_0_on_success(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(["add", "https://example.com/data.csv"], cwd=tmp_path)
        assert result.exit_code == 0, result.output

    def test_source_written_to_file(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(["add", "https://example.com/data.csv"], cwd=tmp_path)
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert len(pkg["sources"]) == 1
        src = pkg["sources"][0]
        assert src["url"] == "https://example.com/data.csv"
        assert src["format"] == "csv"
        assert src["checksum"].startswith("sha256:")
        assert src["size"] == len(CONTENT)

    def test_checksum_is_correct(self, tmp_path):
        import hashlib
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(["add", "https://example.com/data.csv"], cwd=tmp_path)
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        expected = "sha256:" + hashlib.sha256(CONTENT).hexdigest()
        assert pkg["sources"][0]["checksum"] == expected

    def test_format_detected_from_extension(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(["add", "https://example.com/data.parquet"], cwd=tmp_path)
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["sources"][0]["format"] == "parquet"

    def test_format_override(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(["add", "--format", "tsv", "https://example.com/data.csv"], cwd=tmp_path)
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["sources"][0]["format"] == "tsv"

    def test_json_output(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(
                ["--output", "json", "add", "https://example.com/data.csv"], cwd=tmp_path
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["added"] == 1
        assert len(data["sources"]) == 1
        assert data["sources"][0]["checksum"].startswith("sha256:")


# ---------------------------------------------------------------------------
# --no-checksum
# ---------------------------------------------------------------------------


class TestAddNoChecksum:
    def test_no_checksum_skips_stream(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        mock_stream = MagicMock()
        with (
            patch("httpx.stream", mock_stream),
            patch("httpx.head", return_value=make_head_resp(len(CONTENT))),
        ):
            result = invoke(
                ["add", "--no-checksum", "https://example.com/data.csv"], cwd=tmp_path
            )
        assert result.exit_code == 0
        mock_stream.assert_not_called()

    def test_no_checksum_size_from_head(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with (
            patch("httpx.stream", MagicMock()),
            patch("httpx.head", return_value=make_head_resp(9999)),
        ):
            invoke(["add", "--no-checksum", "https://example.com/data.csv"], cwd=tmp_path)
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["sources"][0]["size"] == 9999
        assert "checksum" not in pkg["sources"][0]


# ---------------------------------------------------------------------------
# Multiple URLs
# ---------------------------------------------------------------------------


class TestAddMultipleUrls:
    def test_adds_all_urls(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(
                ["add", "https://example.com/a.csv", "https://example.com/b.csv"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert len(pkg["sources"]) == 2

    def test_skips_existing_url(self, tmp_path):
        existing = {
            **VALID_PKG,
            "sources": [{"url": "https://example.com/a.csv", "format": "csv"}],
        }
        write_pkg(tmp_path, existing)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(
                ["--output", "json", "add", "https://example.com/a.csv", "https://example.com/b.csv"],
                cwd=tmp_path,
            )
        data = json.loads(result.output)
        assert data["added"] == 1
        assert data["skipped"] == 1
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert len(pkg["sources"]) == 2  # original + 1 new


# ---------------------------------------------------------------------------
# --crawl (HTML listing)
# ---------------------------------------------------------------------------


class TestAddCrawlHtml:
    HTML_LISTING = """
    <html><body>
    <a href="data.csv">data.csv</a>
    <a href="data2.parquet">data2.parquet</a>
    <a href="readme.txt">readme.txt</a>
    <a href="?C=N&O=D">sort link</a>
    <a href="../">Parent</a>
    </body></html>
    """

    def _mock_get(self, url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"content-type": "text/html"}
        resp.text = self.HTML_LISTING
        resp.raise_for_status = MagicMock()
        return resp

    def test_crawl_discovers_data_files(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with (
            patch("httpx.get", side_effect=self._mock_get),
            patch("httpx.stream", make_fake_stream(CONTENT)),
        ):
            result = invoke(
                ["add", "--crawl", "https://example.com/data/"], cwd=tmp_path
            )
        assert result.exit_code == 0, result.output
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        urls = [s["url"] for s in pkg["sources"]]
        assert any("data.csv" in u for u in urls)
        assert any("data2.parquet" in u for u in urls)
        assert not any("readme.txt" in u for u in urls)

    def test_crawl_filter_pattern(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with (
            patch("httpx.get", side_effect=self._mock_get),
            patch("httpx.stream", make_fake_stream(CONTENT)),
        ):
            invoke(
                ["add", "--crawl", "--filter", "*.csv", "https://example.com/data/"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        urls = [s["url"] for s in pkg["sources"]]
        assert all("csv" in u for u in urls)
        assert not any("parquet" in u for u in urls)


# ---------------------------------------------------------------------------
# --crawl (S3 XML)
# ---------------------------------------------------------------------------


class TestAddCrawlS3:
    S3_XML = """<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult>
      <Key>datasets/oslo.csv</Key>
      <Key>datasets/bergen.csv</Key>
      <Key>datasets/README.md</Key>
    </ListBucketResult>
    """

    def _mock_get(self, url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"content-type": "application/xml"}
        resp.text = self.S3_XML
        resp.raise_for_status = MagicMock()
        return resp

    def test_s3_crawl_discovers_csv_files(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with (
            patch("httpx.get", side_effect=self._mock_get),
            patch("httpx.stream", make_fake_stream(CONTENT)),
        ):
            result = invoke(
                ["add", "--crawl", "https://mybucket.s3.amazonaws.com/"], cwd=tmp_path
            )
        assert result.exit_code == 0, result.output
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        urls = [s["url"] for s in pkg["sources"]]
        assert len(urls) == 2
        assert not any("README" in u for u in urls)


# ---------------------------------------------------------------------------
# Network errors
# ---------------------------------------------------------------------------


class TestAddNetworkError:
    def test_network_error_exits_2(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", side_effect=httpx.HTTPError("timeout")):
            result = invoke(["add", "https://example.com/data.csv"], cwd=tmp_path)
        assert result.exit_code == 2

    def test_no_sources_written_on_all_fail(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", side_effect=httpx.HTTPError("timeout")):
            invoke(["add", "https://example.com/data.csv"], cwd=tmp_path)
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["sources"] == []


# ---------------------------------------------------------------------------
# --file flag
# ---------------------------------------------------------------------------


class TestAddFileFlag:
    def test_explicit_file_path(self, tmp_path):
        pkg_file = write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(["add", "--file", str(pkg_file), "https://example.com/data.csv"])
        assert result.exit_code == 0, result.output
        pkg = json.loads(pkg_file.read_text())
        assert len(pkg["sources"]) == 1


# ---------------------------------------------------------------------------
# Metadata flags — create new datapackage.json
# ---------------------------------------------------------------------------


CREATE_FLAGS = [
    "--id", "simkjels/samples/newdata",
    "--title", "New Dataset",
    "--publisher", "Simen",
    "--version", "1.0.0",
]


class TestAddCreateNew:
    def test_creates_file_with_all_required_flags(self, tmp_path):
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(
                ["add"] + CREATE_FLAGS + ["https://example.com/data.csv"],
                cwd=tmp_path,
            )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "datapackage.json").exists()

    def test_created_file_has_correct_metadata(self, tmp_path):
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(
                ["add"] + CREATE_FLAGS + ["https://example.com/data.csv"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["id"] == "simkjels/samples/newdata"
        assert pkg["title"] == "New Dataset"
        assert pkg["publisher"]["name"] == "Simen"
        assert pkg["version"] == "1.0.0"
        assert len(pkg["sources"]) == 1

    def test_missing_id_flag_exits_1(self, tmp_path):
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(
                ["add", "--title", "T", "--publisher", "P", "--version", "1.0",
                 "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        assert result.exit_code == 1

    def test_missing_title_flag_exits_1(self, tmp_path):
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(
                ["add", "--id", "a/b/c", "--publisher", "P", "--version", "1.0",
                 "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        assert result.exit_code == 1

    def test_missing_publisher_flag_exits_1(self, tmp_path):
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(
                ["add", "--id", "a/b/c", "--title", "T", "--version", "1.0",
                 "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        assert result.exit_code == 1

    def test_missing_version_flag_exits_1(self, tmp_path):
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(
                ["add", "--id", "a/b/c", "--title", "T", "--publisher", "P",
                 "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        assert result.exit_code == 1

    def test_missing_flags_json_output_lists_missing(self, tmp_path):
        result = invoke(
            ["--output", "json", "add", "https://example.com/data.csv"],
            cwd=tmp_path,
        )
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["added"] == 0
        assert "error" in data

    def test_optional_flags_written(self, tmp_path):
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(
                ["add"] + CREATE_FLAGS
                + ["--description", "A desc", "--license", "CC-BY-4.0",
                   "--tags", "weather,norway",
                   "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["description"] == "A desc"
        assert pkg["license"] == "CC-BY-4.0"
        assert pkg["tags"] == ["weather", "norway"]


# ---------------------------------------------------------------------------
# Metadata flags — update existing datapackage.json
# ---------------------------------------------------------------------------


class TestAddUpdateMetadata:
    def test_title_updated_in_existing_file(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(
                ["add", "--title", "Updated Title", "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["title"] == "Updated Title"

    def test_version_updated_in_existing_file(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(
                ["add", "--version", "2.0.0", "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["version"] == "2.0.0"

    def test_publisher_updated_in_existing_file(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(
                ["add", "--publisher", "New Publisher", "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["publisher"]["name"] == "New Publisher"

    def test_metadata_written_even_when_no_new_sources(self, tmp_path):
        existing = {
            **VALID_PKG,
            "sources": [{"url": "https://example.com/data.csv", "format": "csv"}],
        }
        write_pkg(tmp_path, existing)
        # URL already present — but metadata flag should still apply
        invoke(
            ["add", "--title", "Refreshed Title", "https://example.com/data.csv"],
            cwd=tmp_path,
        )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["title"] == "Refreshed Title"

    def test_id_change_warns_user(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            result = invoke(
                ["add", "--id", "simkjels/samples/other",
                 "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        assert result.exit_code == 0, result.output
        # Warning about ID change should appear in output
        assert "simkjels/samples/other" in result.output

    def test_id_change_updates_file(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(
                ["add", "--id", "simkjels/samples/other",
                 "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["id"] == "simkjels/samples/other"

    def test_tags_updated_in_existing_file(self, tmp_path):
        write_pkg(tmp_path, VALID_PKG)
        with patch("httpx.stream", make_fake_stream(CONTENT)):
            invoke(
                ["add", "--tags", "a,b,c", "https://example.com/data.csv"],
                cwd=tmp_path,
            )
        pkg = json.loads((tmp_path / "datapackage.json").read_text())
        assert pkg["tags"] == ["a", "b", "c"]
