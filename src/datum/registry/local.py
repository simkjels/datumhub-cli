"""Local filesystem registry for datum."""

from __future__ import annotations

import difflib
import json
from pathlib import Path
from typing import List, Optional

from datum.models import DataPackage
from datum.state import state


class LocalRegistry:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _pkg_path(self, id: str, version: str) -> Path:
        pub, ns, ds = id.split(".")
        return self.root / pub / ns / ds / f"{version}.json"

    def publish(self, pkg: DataPackage, overwrite: bool = False) -> Path:
        path = self._pkg_path(pkg.id, pkg.version)
        if path.exists() and not overwrite:
            raise FileExistsError(
                f"{pkg.id}@{pkg.version} already exists in the registry. "
                "Use --force to overwrite."
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(pkg.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return path

    def list(self) -> List[DataPackage]:
        if not self.root.exists():
            return []
        results: List[DataPackage] = []
        for p in sorted(self.root.rglob("*.json")):
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                results.append(DataPackage.model_validate(raw))
            except Exception:
                continue
        return results

    def get(self, id: str, version: str) -> Optional[DataPackage]:
        path = self._pkg_path(id, version)
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return DataPackage.model_validate(raw)
        except Exception:
            return None

    def suggest(self, id_part: str, n: int = 3) -> List[str]:
        """Return up to n dataset IDs that closely match id_part."""
        all_ids = list({pkg.id for pkg in self.list()})
        if not all_ids:
            return []

        # If publisher.namespace prefix is known, suggest only within that namespace
        parts = id_part.split(".")
        if len(parts) == 3:
            prefix = f"{parts[0]}.{parts[1]}."
            scoped = [id for id in all_ids if id.startswith(prefix)]
            if scoped:
                return difflib.get_close_matches(id_part, scoped, n=n, cutoff=0.5)

        # Fall back to global match with a tighter cutoff
        return difflib.get_close_matches(id_part, all_ids, n=n, cutoff=0.7)

    def latest(self, id: str) -> Optional[DataPackage]:
        """Return the most recently published version for a dataset id."""
        pub, ns, ds = id.split(".")
        folder = self.root / pub / ns / ds
        if not folder.exists():
            return None
        candidates = sorted(
            folder.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        for p in candidates:
            try:
                return DataPackage.model_validate(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                continue
        return None


def get_local_registry() -> LocalRegistry:
    if state.registry and not state.registry.startswith(("http://", "https://")):
        return LocalRegistry(Path(state.registry).expanduser())
    return LocalRegistry(Path("~/.datum/registry").expanduser())
