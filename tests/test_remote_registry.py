"""Tests for RemoteRegistry."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from datum.registry.remote import RemoteRegistry

BASE_URL = "https://datumhub.fly.dev"

VALID_ITEM = {
    "id": "simkjels/samples/sampledata",
    "version": "0.1.0",
    "title": "Sample Data",
    "publisher": {"name": "Simen Kjelsrud"},
    "sources": [{"url": "https://example.com/data.csv", "format": "csv"}],
    # extra fields from API — should be ignored by DataPackage model
    "owner": "simkjels",
    "published_at": "2024-01-01T00:00:00",
}

LIST_RESPONSE = {
    "items": [VALID_ITEM],
    "total": 1,
    "limit": 500,
    "offset": 0,
}


def _mock_resp(status_code: int, body=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body or {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp
        )
    return resp


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


class TestRemoteList:
    def test_list_returns_packages(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", return_value=_mock_resp(200, LIST_RESPONSE)) as mock_get:
            pkgs = reg.list()
        assert len(pkgs) == 1
        assert pkgs[0].id == "simkjels/samples/sampledata"
        mock_get.assert_called_once()

    def test_list_passes_query_param(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", return_value=_mock_resp(200, LIST_RESPONSE)) as mock_get:
            reg.list(q="sample")
        call_kwargs = mock_get.call_args
        assert call_kwargs.kwargs["params"]["q"] == "sample"

    def test_list_network_error_raises_runtime(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", side_effect=httpx.HTTPError("timeout")):
            with pytest.raises(RuntimeError, match="Registry unreachable"):
                reg.list()

    def test_list_ignores_extra_api_fields(self):
        """DataPackage model should accept owner/published_at without error."""
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", return_value=_mock_resp(200, LIST_RESPONSE)):
            pkgs = reg.list()
        assert pkgs[0].id == "simkjels/samples/sampledata"


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


class TestRemoteGet:
    def test_get_returns_package(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", return_value=_mock_resp(200, VALID_ITEM)):
            pkg = reg.get("simkjels/samples/sampledata", "0.1.0")
        assert pkg is not None
        assert pkg.version == "0.1.0"

    def test_get_returns_none_on_404(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", return_value=_mock_resp(404)):
            pkg = reg.get("simkjels/samples/sampledata", "9.9.9")
        assert pkg is None

    def test_get_network_error_raises_runtime(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", side_effect=httpx.HTTPError("conn refused")):
            with pytest.raises(RuntimeError):
                reg.get("simkjels/samples/sampledata", "0.1.0")


# ---------------------------------------------------------------------------
# latest
# ---------------------------------------------------------------------------


class TestRemoteLatest:
    def test_latest_returns_package(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", return_value=_mock_resp(200, VALID_ITEM)):
            pkg = reg.latest("simkjels/samples/sampledata")
        assert pkg is not None
        assert pkg.id == "simkjels/samples/sampledata"

    def test_latest_returns_none_on_404(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", return_value=_mock_resp(404)):
            pkg = reg.latest("simkjels/samples/unknown")
        assert pkg is None


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------


class TestRemotePublish:
    def _make_pkg(self):
        from datum.models import DataPackage

        return DataPackage.model_validate(
            {
                "id": "simkjels/samples/sampledata",
                "version": "0.1.0",
                "title": "Sample",
                "publisher": {"name": "Simen"},
                "sources": [{"url": "https://example.com/data.csv", "format": "csv"}],
            }
        )

    def test_publish_success(self):
        reg = RemoteRegistry(BASE_URL)
        pkg = self._make_pkg()
        with patch("httpx.post", return_value=_mock_resp(201, VALID_ITEM)):
            reg.publish(pkg)  # no exception

    def test_publish_with_force_appends_query(self):
        reg = RemoteRegistry(BASE_URL)
        pkg = self._make_pkg()
        with patch("httpx.post", return_value=_mock_resp(201, VALID_ITEM)) as mock_post:
            reg.publish(pkg, overwrite=True)
        url_arg = mock_post.call_args.args[0]
        assert "force=true" in url_arg

    def test_publish_raises_permission_error_on_401(self):
        reg = RemoteRegistry(BASE_URL)
        pkg = self._make_pkg()
        with patch("httpx.post", return_value=_mock_resp(401)):
            with pytest.raises(PermissionError, match="Not authenticated"):
                reg.publish(pkg)

    def test_publish_raises_file_exists_error_on_409(self):
        reg = RemoteRegistry(BASE_URL)
        pkg = self._make_pkg()
        with patch("httpx.post", return_value=_mock_resp(409)):
            with pytest.raises(FileExistsError):
                reg.publish(pkg)

    def test_publish_sends_auth_header_when_token_stored(self, tmp_path):
        reg = RemoteRegistry(BASE_URL)
        pkg = self._make_pkg()
        cfg = {"token.datumhub.fly.dev": "mytoken123"}
        with (
            patch("httpx.post", return_value=_mock_resp(201, VALID_ITEM)) as mock_post,
            patch("datum.registry.remote.RemoteRegistry._auth_headers", return_value={"Authorization": "Bearer mytoken123"}),
        ):
            reg.publish(pkg)
        mock_post.assert_called_once()

    def test_publish_network_error_raises_runtime(self):
        reg = RemoteRegistry(BASE_URL)
        pkg = self._make_pkg()
        with patch("httpx.post", side_effect=httpx.HTTPError("timeout")):
            with pytest.raises(RuntimeError):
                reg.publish(pkg)


# ---------------------------------------------------------------------------
# unpublish
# ---------------------------------------------------------------------------


class TestRemoteUnpublish:
    def test_unpublish_success(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.delete", return_value=_mock_resp(204)):
            result = reg.unpublish("simkjels/samples/sampledata", "0.1.0")
        assert result is True

    def test_unpublish_returns_false_on_404(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.delete", return_value=_mock_resp(404)):
            result = reg.unpublish("simkjels/samples/sampledata", "9.9.9")
        assert result is False

    def test_unpublish_raises_permission_error_on_403(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.delete", return_value=_mock_resp(403)):
            with pytest.raises(PermissionError):
                reg.unpublish("simkjels/samples/sampledata", "0.1.0")

    def test_unpublish_network_error_raises_runtime(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.delete", side_effect=httpx.HTTPError("timeout")):
            with pytest.raises(RuntimeError):
                reg.unpublish("simkjels/samples/sampledata", "0.1.0")


# ---------------------------------------------------------------------------
# versions
# ---------------------------------------------------------------------------


class TestRemoteVersions:
    def test_versions_returns_sorted_list(self):
        reg = RemoteRegistry(BASE_URL)
        items = [
            {**VALID_ITEM, "version": "0.2.0"},
            {**VALID_ITEM, "version": "0.1.0"},
        ]
        body = {"items": items, "total": 2, "limit": 500, "offset": 0}
        with patch("httpx.get", return_value=_mock_resp(200, body)):
            versions = reg.versions("simkjels/samples/sampledata")
        assert versions == ["0.1.0", "0.2.0"]

    def test_versions_returns_empty_on_network_error(self):
        reg = RemoteRegistry(BASE_URL)
        with patch("httpx.get", side_effect=httpx.HTTPError("timeout")):
            versions = reg.versions("simkjels/samples/sampledata")
        assert versions == []
