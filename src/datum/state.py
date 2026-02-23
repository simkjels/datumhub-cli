from __future__ import annotations

from enum import Enum


class OutputFormat(str, Enum):
    table = "table"
    json = "json"
    plain = "plain"


class AppState:
    """Global mutable state populated by the root CLI callback."""

    def __init__(self) -> None:
        self.registry: str = ""
        self.output: OutputFormat = OutputFormat.table
        self.quiet: bool = False
        self.verbose: bool = False

    @property
    def is_remote(self) -> bool:
        return bool(self.registry and self.registry.startswith(("http://", "https://")))


# Singleton accessed by every command module.
state = AppState()
