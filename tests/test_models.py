"""Tests for DataPackage and related Pydantic models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from datum.models import DataPackage, PublisherInfo, Source


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_SOURCE = {
    "url": "https://example.com/data.csv",
    "format": "csv",
    "size": 1024,
    "checksum": "sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abc123",
}

VALID_PKG = {
    "id": "met/no/oslo-hourly",
    "version": "2024-01",
    "title": "Oslo Hourly Weather Data",
    "description": "Hourly weather observations for Oslo.",
    "license": "CC-BY-4.0",
    "publisher": {"name": "Norwegian Meteorological Institute", "url": "https://met.no"},
    "sources": [VALID_SOURCE],
    "tags": ["weather", "norway"],
    "created": "2024-01-15",
    "updated": "2024-01-15",
}


def make_pkg(**overrides) -> dict:
    return {**VALID_PKG, **overrides}


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


class TestSource:
    def test_valid(self):
        s = Source(**VALID_SOURCE)
        assert s.url == "https://example.com/data.csv"
        assert s.format == "csv"

    def test_format_normalised_lowercase(self):
        s = Source(url="https://x.com/f.CSV", format="CSV")
        assert s.format == "csv"

    def test_url_requires_http_scheme(self):
        with pytest.raises(ValidationError, match="http"):
            Source(url="ftp://example.com/data.csv", format="csv")

    def test_url_rejects_bare_string(self):
        with pytest.raises(ValidationError):
            Source(url="example.com/data.csv", format="csv")

    def test_checksum_valid_sha256(self):
        s = Source(url="https://x.com/f", format="csv", checksum="sha256:deadbeef")
        assert s.checksum == "sha256:deadbeef"

    def test_checksum_valid_md5(self):
        s = Source(url="https://x.com/f", format="csv", checksum="md5:deadbeef")
        assert s.checksum == "md5:deadbeef"

    def test_checksum_invalid_algorithm(self):
        with pytest.raises(ValidationError, match="checksum"):
            Source(url="https://x.com/f", format="csv", checksum="crc32:deadbeef")

    def test_checksum_invalid_format(self):
        with pytest.raises(ValidationError, match="checksum"):
            Source(url="https://x.com/f", format="csv", checksum="deadbeef")

    def test_size_none_is_ok(self):
        s = Source(url="https://x.com/f", format="csv")
        assert s.size is None

    def test_size_negative_rejected(self):
        with pytest.raises(ValidationError):
            Source(url="https://x.com/f", format="csv", size=-1)

    def test_format_empty_rejected(self):
        with pytest.raises(ValidationError):
            Source(url="https://x.com/f", format="  ")


# ---------------------------------------------------------------------------
# PublisherInfo
# ---------------------------------------------------------------------------


class TestPublisherInfo:
    def test_valid_with_url(self):
        p = PublisherInfo(name="Acme", url="https://acme.com")
        assert p.url == "https://acme.com"

    def test_valid_without_url(self):
        p = PublisherInfo(name="Acme")
        assert p.url is None

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError):
            PublisherInfo(name="   ")

    def test_url_without_scheme_rejected(self):
        with pytest.raises(ValidationError, match="http"):
            PublisherInfo(name="Acme", url="acme.com")


# ---------------------------------------------------------------------------
# DataPackage — ID validation
# ---------------------------------------------------------------------------


class TestDataPackageId:
    def test_valid_id(self):
        pkg = DataPackage.model_validate(make_pkg(id="met/no/oslo-hourly"))
        assert pkg.id == "met/no/oslo-hourly"

    def test_id_with_numbers(self):
        pkg = DataPackage.model_validate(make_pkg(id="org2/ns3/dataset1"))
        assert pkg.publisher_slug == "org2"

    @pytest.mark.parametrize(
        "bad_id",
        [
            "only-two/parts",           # only two segments
            "four/parts/are/too-many",  # too many segments
            "Met/no/data",              # uppercase
            "met/no/data!",             # special char
            "-met/no/data",             # leading hyphen
            "met/no/data-",             # trailing hyphen
            "met//data",                # empty segment
            "",                         # empty
        ],
    )
    def test_invalid_id(self, bad_id: str):
        with pytest.raises(ValidationError, match="identifier|Invalid"):
            DataPackage.model_validate(make_pkg(id=bad_id))

    def test_slug_helpers(self):
        pkg = DataPackage.model_validate(VALID_PKG)
        assert pkg.publisher_slug == "met"
        assert pkg.namespace_slug == "no"
        assert pkg.dataset_slug == "oslo-hourly"


# ---------------------------------------------------------------------------
# DataPackage — full validation
# ---------------------------------------------------------------------------


class TestDataPackage:
    def test_valid_minimal(self):
        pkg = DataPackage.model_validate(
            {
                "id": "a/b/c",
                "version": "1",
                "title": "Test",
                "publisher": {"name": "Test Org"},
                "sources": [{"url": "https://x.com/f.csv", "format": "csv"}],
            }
        )
        assert pkg.title == "Test"

    def test_optional_fields_default_to_none(self):
        pkg = DataPackage.model_validate(
            {
                "id": "a/b/c",
                "version": "1",
                "title": "Test",
                "publisher": {"name": "Test Org"},
                "sources": [{"url": "https://x.com/f.csv", "format": "csv"}],
            }
        )
        assert pkg.description is None
        assert pkg.license is None
        assert pkg.tags is None
        assert pkg.created is None

    def test_empty_sources_rejected(self):
        with pytest.raises(ValidationError, match="source"):
            DataPackage.model_validate(make_pkg(sources=[]))

    def test_empty_title_rejected(self):
        with pytest.raises(ValidationError):
            DataPackage.model_validate(make_pkg(title=""))

    def test_empty_version_rejected(self):
        with pytest.raises(ValidationError):
            DataPackage.model_validate(make_pkg(version=""))

    def test_to_dict_excludes_none(self):
        pkg = DataPackage.model_validate(
            {
                "id": "a/b/c",
                "version": "1",
                "title": "Test",
                "publisher": {"name": "Test Org"},
                "sources": [{"url": "https://x.com/f.csv", "format": "csv"}],
            }
        )
        d = pkg.to_dict()
        assert "description" not in d
        assert "license" not in d
        assert d["id"] == "a/b/c"
