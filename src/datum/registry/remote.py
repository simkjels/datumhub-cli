"""Remote HTTP registry for datum."""

from __future__ import annotations

import difflib
from typing import List, Optional
from urllib.parse import urlparse

import httpx

from datum.models import DataPackage


class RemoteRegistry:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")
        self._host = urlparse(url).netloc or url

    def _auth_headers(self) -> dict:
        from datum.commands.config import load_config

        cfg = load_config()
        token = cfg.get(f"token.{self._host}")
        return {"Authorization": f"Bearer {token}"} if token else {}

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list(self, q: str = "") -> List[DataPackage]:
        params: dict = {"limit": 500}
        if q:
            params["q"] = q
        try:
            resp = httpx.get(f"{self.url}/api/v1/packages", params=params, timeout=10)
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Registry unreachable: {exc}") from exc
        resp.raise_for_status()
        return [DataPackage.model_validate(item) for item in resp.json()["items"]]

    def get(self, id: str, version: str) -> Optional[DataPackage]:
        try:
            resp = httpx.get(f"{self.url}/api/v1/packages/{id}/{version}", timeout=10)
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Registry unreachable: {exc}") from exc
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return DataPackage.model_validate(resp.json())

    def latest(self, id: str) -> Optional[DataPackage]:
        try:
            resp = httpx.get(f"{self.url}/api/v1/packages/{id}/latest", timeout=10)
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Registry unreachable: {exc}") from exc
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return DataPackage.model_validate(resp.json())

    def suggest(self, id_part: str, n: int = 3) -> List[str]:
        try:
            all_ids = list({pkg.id for pkg in self.list()})
        except RuntimeError:
            return []
        parts = id_part.split("/")
        if len(parts) == 3:
            prefix = f"{parts[0]}/{parts[1]}/"
            scoped = [i for i in all_ids if i.startswith(prefix)]
            if scoped:
                return difflib.get_close_matches(id_part, scoped, n=n, cutoff=0.5)
        return difflib.get_close_matches(id_part, all_ids, n=n, cutoff=0.7)

    # ------------------------------------------------------------------
    # Write (authenticated)
    # ------------------------------------------------------------------

    def publish(self, pkg: DataPackage, overwrite: bool = False) -> None:
        path = "/api/v1/packages"
        if overwrite:
            path += "?force=true"
        try:
            resp = httpx.post(
                f"{self.url}{path}",
                json=pkg.to_dict(),
                headers=self._auth_headers(),
                timeout=10,
            )
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Registry unreachable: {exc}") from exc
        if resp.status_code == 401:
            raise PermissionError("Not authenticated. Run `datum login`.")
        if resp.status_code == 409:
            raise FileExistsError(
                f"{pkg.id}@{pkg.version} already exists. Use --force to overwrite."
            )
        resp.raise_for_status()

    def unpublish(self, id: str, version: str) -> bool:
        try:
            resp = httpx.delete(
                f"{self.url}/api/v1/packages/{id}/{version}",
                headers=self._auth_headers(),
                timeout=10,
            )
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Registry unreachable: {exc}") from exc
        if resp.status_code == 404:
            return False
        if resp.status_code in (401, 403):
            raise PermissionError(
                "Not authorised. Make sure you are logged in and own this package."
            )
        resp.raise_for_status()
        return True

    def versions(self, id: str) -> List[str]:
        """Return all published versions for a dataset id (fetches full list)."""
        try:
            pkgs = self.list()
        except RuntimeError:
            return []
        return sorted({p.version for p in pkgs if p.id == id})
