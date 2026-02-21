from __future__ import annotations

import re
from typing import List, Optional

from pydantic import BaseModel, field_validator, model_validator

# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

# Each slug: lowercase letters, digits, hyphens; must not start/end with hyphen.
_SLUG = r"[a-z0-9]([a-z0-9-]*[a-z0-9])?"
ID_PATTERN = re.compile(rf"^{_SLUG}\.{_SLUG}\.{_SLUG}$")
SLUG_PATTERN = re.compile(rf"^{_SLUG}$")
CHECKSUM_PATTERN = re.compile(r"^(sha256|sha512|md5):[a-f0-9]+$")

COMMON_FORMATS = {
    "csv", "json", "parquet", "geojson", "xlsx", "xls",
    "tsv", "xml", "zip", "gz", "bz2", "tar",
}


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class Source(BaseModel):
    """A single data file within a dataset."""

    url: str
    format: str
    size: Optional[int] = None
    checksum: Optional[str] = None

    @field_validator("url")
    @classmethod
    def _url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v

    @field_validator("format")
    @classmethod
    def _format(cls, v: str) -> str:
        v = v.lower().strip()
        if not v:
            raise ValueError("Format cannot be empty")
        return v

    @field_validator("size")
    @classmethod
    def _size(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError("Size must be a non-negative integer")
        return v

    @field_validator("checksum")
    @classmethod
    def _checksum(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not CHECKSUM_PATTERN.match(v):
            raise ValueError(
                "Invalid checksum format — expected sha256:<hex>, sha512:<hex>, or md5:<hex>"
            )
        return v


class PublisherInfo(BaseModel):
    """Metadata about the dataset's publisher."""

    name: str
    url: Optional[str] = None

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Publisher name cannot be empty")
        return v

    @field_validator("url")
    @classmethod
    def _url(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith(("http://", "https://")):
            raise ValueError("Publisher URL must start with http:// or https://")
        return v


# ---------------------------------------------------------------------------
# Root model
# ---------------------------------------------------------------------------


class DataPackage(BaseModel):
    """
    The datapackage.json schema.

    Identifiers follow the format:  publisher.namespace.dataset
    Each segment is a lowercase slug (letters, digits, hyphens).
    """

    id: str
    version: str
    title: str
    description: Optional[str] = None
    license: Optional[str] = None
    publisher: PublisherInfo
    sources: List[Source]
    tags: Optional[List[str]] = None
    created: Optional[str] = None
    updated: Optional[str] = None

    @field_validator("id")
    @classmethod
    def _id(cls, v: str) -> str:
        if not ID_PATTERN.match(v):
            raise ValueError(
                f"Invalid identifier format '{v}'. "
                "Expected publisher.namespace.dataset "
                "(three dot-separated slugs of lowercase letters, digits, and hyphens — "
                "e.g. met.no.oslo-hourly)"
            )
        return v

    @field_validator("version")
    @classmethod
    def _version(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Version cannot be empty")
        return v

    @field_validator("title")
    @classmethod
    def _title(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Title cannot be empty")
        return v

    @field_validator("sources")
    @classmethod
    def _sources(cls, v: List[Source]) -> List[Source]:
        if not v:
            raise ValueError("At least one source is required")
        return v

    # Convenience helpers --------------------------------------------------

    @property
    def publisher_slug(self) -> str:
        return self.id.split(".")[0]

    @property
    def namespace_slug(self) -> str:
        return self.id.split(".")[1]

    @property
    def dataset_slug(self) -> str:
        return self.id.split(".")[2]

    def to_dict(self) -> dict:
        return self.model_dump(exclude_none=True)
