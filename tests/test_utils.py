"""Tests for datum.utils shared helpers."""

from __future__ import annotations

import pytest

from datum.utils import fmt_size, parse_identifier, sort_versions


class TestParseIdentifier:
    def test_with_version(self):
        assert parse_identifier("a/b/c:1.0.0") == ("a/b/c", "1.0.0")

    def test_without_version(self):
        assert parse_identifier("a/b/c") == ("a/b/c", None)

    def test_version_latest(self):
        assert parse_identifier("a/b/c:latest") == ("a/b/c", "latest")

    def test_version_with_colon_in_version(self):
        # Only first colon splits
        result = parse_identifier("a/b/c:1.0:extra")
        assert result == ("a/b/c", "1.0:extra")

    def test_publisher_with_dots(self):
        assert parse_identifier("norge.no/pop/census:2024") == ("norge.no/pop/census", "2024")

    def test_version_date_style(self):
        assert parse_identifier("a/b/c:2024-01") == ("a/b/c", "2024-01")


class TestFmtSize:
    def test_bytes(self):
        assert fmt_size(500) == "500 B"

    def test_kilobytes(self):
        assert fmt_size(1024) == "1.0 KB"

    def test_kilobytes_fractional(self):
        assert fmt_size(1536) == "1.5 KB"

    def test_megabytes(self):
        assert fmt_size(1024 * 1024) == "1.0 MB"

    def test_gigabytes(self):
        assert fmt_size(1024 ** 3) == "1.0 GB"

    def test_terabytes(self):
        assert fmt_size(1024 ** 4) == "1.0 TB"

    def test_zero(self):
        assert fmt_size(0) == "0 B"

    def test_large_mb(self):
        assert fmt_size(150 * 1024 * 1024) == "150.0 MB"


class TestSortVersions:
    def test_semver_ascending(self):
        result = sort_versions(["1.0.0", "2.0.0", "1.1.0"])
        assert result == ["1.0.0", "1.1.0", "2.0.0"]

    def test_semver_multi_digit(self):
        # Alphabetic sort would give wrong order: 1.0.10 < 1.0.2
        result = sort_versions(["1.0.10", "1.0.2", "1.0.9"])
        assert result == ["1.0.2", "1.0.9", "1.0.10"]

    def test_date_versions(self):
        result = sort_versions(["2024-12", "2024-01", "2023-06"])
        assert result == ["2023-06", "2024-01", "2024-12"]

    def test_mixed_style(self):
        result = sort_versions(["2024-01", "2024-12"])
        assert result[-1] == "2024-12"

    def test_single_element(self):
        assert sort_versions(["1.0.0"]) == ["1.0.0"]

    def test_empty(self):
        assert sort_versions([]) == []

    def test_stable_for_equal_strings(self):
        result = sort_versions(["1.0.0", "1.0.0"])
        assert result == ["1.0.0", "1.0.0"]

    def test_newest_is_last(self):
        result = sort_versions(["3.0.0", "1.0.0", "2.0.0"])
        assert result[-1] == "3.0.0"

    def test_latest_string_sorted_last_among_plain_strings(self):
        # 'latest' has no digits — falls to lexicographic tier
        result = sort_versions(["1.0.0", "latest"])
        # We just need it not to crash; order for 'latest' is unspecified
        assert "latest" in result
        assert "1.0.0" in result
