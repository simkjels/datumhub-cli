"""Remote HTTP registry for datum."""

from __future__ import annotations

import difflib
import time
from typing import List, Optional
from urllib.parse import urlparse

import httpx

from datum.models import DataPackage

# HTTP status codes that indicate a transient server-side problem worth retrying
_TRANSIENT_STATUS = {429, 500, 502, 503, 504}
_MAX_ATTEMPTS = 3


class RemoteRegistry:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")
        self._host = urlparse(url).netloc or url

    def _auth_headers(self) -> dict:
        from datum.commands.config import get_token, load_config

        cfg = load_config()
        token = get_token(cfg, self._host)
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _can_refresh(self) -> bool:
        """Return True if a stored token is available to attempt a refresh."""
        from datum.commands.config import get_token, load_config

        return bool(get_token(load_config(), self._host))

    def _do_refresh(self) -> None:
        """POST to /api/auth/refresh with the current token; update config on success."""
        from datum.commands.config import get_token, load_config, save_config, set_auth

        cfg = load_config()
        token = get_token(cfg, self._host)
        if not token:
            return
        try:
            resp = httpx.request(
                "POST",
                f"{self.url}/api/auth/refresh",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            if resp.status_code == 200:
                new_token = resp.json().get("token", "")
                if new_token:
                    set_auth(cfg, self._host, new_token)
                    save_config(cfg)
        except Exception:
            pass

    def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Make an HTTP request with transient retry + one-shot 401 auto-refresh."""
        resp = self._raw_request(method, url, **kwargs)
        if resp.status_code == 401 and self._can_refresh():
            self._do_refresh()
            headers = {**kwargs.pop("headers", {}), **self._auth_headers()}
            kwargs["headers"] = headers
            resp = self._raw_request(method, url, **kwargs)
        return resp

    def _raw_request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Inner request with exponential-backoff retry on transient errors.

        Retries up to _MAX_ATTEMPTS times on network-level errors (timeout,
        connect failure) and on transient 5xx / 429 responses.  All other
        HTTP errors propagate immediately as RuntimeError.
        """
        from datum.state import state

        last_exc: Optional[Exception] = None
        for attempt in range(_MAX_ATTEMPTS):
            if state.verbose:
                from datum.console import err_console
                err_console.print(f"  [muted]→ {method} {url}[/muted]")
            try:
                resp = httpx.request(method, url, **kwargs)
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_exc = exc
                if attempt < _MAX_ATTEMPTS - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f"Registry unreachable: {exc}") from exc
            except httpx.HTTPError as exc:
                raise RuntimeError(f"Registry unreachable: {exc}") from exc

            if resp.status_code in _TRANSIENT_STATUS and attempt < _MAX_ATTEMPTS - 1:
                time.sleep(2 ** attempt)
                continue

            return resp

        # Should be unreachable, but satisfies the type checker
        raise RuntimeError(f"Registry unreachable after {_MAX_ATTEMPTS} attempts: {last_exc}")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list(self, q: str = "") -> List[DataPackage]:
        results: List[DataPackage] = []
        offset = 0
        limit = 100
        while True:
            params: dict = {"limit": limit, "offset": offset}
            if q:
                params["q"] = q
            try:
                resp = self._request(
                    "GET", f"{self.url}/api/v1/packages", params=params, timeout=10
                )
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise RuntimeError(f"Registry unreachable: {exc}") from exc
            data = resp.json()
            results.extend(DataPackage.model_validate(item) for item in data["items"])
            if not data.get("has_next"):
                break
            offset += limit
        return results

    def get(self, id: str, version: str) -> Optional[DataPackage]:
        try:
            resp = self._request(
                "GET", f"{self.url}/api/v1/packages/{id}/{version}", timeout=10
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Registry unreachable: {exc}") from exc
        return DataPackage.model_validate(resp.json())

    def latest(self, id: str) -> Optional[DataPackage]:
        try:
            resp = self._request(
                "GET", f"{self.url}/api/v1/packages/{id}/latest", timeout=10
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Registry unreachable: {exc}") from exc
        return DataPackage.model_validate(resp.json())

    def suggest(self, id_part: str, n: int = 3) -> List[str]:
        """Return up to n close matches. Uses the server suggest endpoint with difflib fallback."""
        try:
            resp = self._request(
                "GET",
                f"{self.url}/api/v1/packages/suggest",
                params={"q": id_part, "n": n},
                timeout=10,
            )
            if resp.status_code == 200:
                return resp.json().get("suggestions", [])
            if resp.status_code != 404:
                resp.raise_for_status()
        except (RuntimeError, httpx.HTTPError):
            pass
        return self._suggest_fallback(id_part, n)

    def _suggest_fallback(self, id_part: str, n: int) -> List[str]:
        """Fallback: fetch full list and run difflib client-side."""
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
            resp = self._request(
                "POST",
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
            resp = self._request(
                "DELETE",
                f"{self.url}/api/v1/packages/{id}/{version}",
                headers=self._auth_headers(),
                timeout=10,
            )
            if resp.status_code == 404:
                return False
            if resp.status_code in (401, 403):
                raise PermissionError(
                    "Not authorised. Make sure you are logged in and own this package."
                )
            resp.raise_for_status()
        except PermissionError:
            raise
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Registry unreachable: {exc}") from exc
        return True

    def versions(self, id: str) -> List[str]:
        """Return all published versions for a dataset id (fetches full list)."""
        try:
            pkgs = self.list()
        except RuntimeError:
            return []
        return sorted({p.version for p in pkgs if p.id == id})
