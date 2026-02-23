"""Shared utility helpers for datum commands."""

from __future__ import annotations

import re
from typing import List, Optional, Tuple


def parse_identifier(s: str) -> Tuple[str, Optional[str]]:
    """Split 'publisher/ns/ds:version' into (id_part, version).

    Version is None if omitted — callers decide the default meaning
    ('latest' for pull/info, must-specify for unpublish).

    Examples:
        parse_identifier("a/b/c:1.0") -> ("a/b/c", "1.0")
        parse_identifier("a/b/c")     -> ("a/b/c", None)
    """
    if ":" in s:
        id_part, version = s.split(":", 1)
        return id_part, version
    return s, None


def fmt_size(n: int) -> str:
    """Return a human-readable byte count (e.g. '1.4 MB')."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def sort_versions(versions: List[str]) -> List[str]:
    """Sort version strings in ascending order, newest last.

    Strategy (in priority order):
    1. PEP 440 / semver  e.g. '1.0.0', '2.1.3'  — parsed by packaging if present
    2. Tuple of ints     e.g. '2024-01', '1.0.10' — handles most real version strings
    3. Fallback          plain lexicographic

    packaging is not declared as a dependency; if absent the int-tuple
    fallback handles the common cases correctly.
    """
    def _key(v: str):
        try:
            from packaging.version import Version
            return (0, Version(v), v)
        except Exception:
            pass
        nums = tuple(int(x) for x in re.findall(r"\d+", v))
        if nums:
            return (1, nums, v)
        return (2, (), v)

    return sorted(versions, key=_key)
