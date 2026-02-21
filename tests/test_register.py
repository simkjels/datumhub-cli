"""Tests for `datum register`."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest
from typer.testing import CliRunner

from datum.main import app

runner = CliRunner()

BASE_URL = "https://datumhub.org"

REGISTER_OK = {"registered": True, "username": "testuser"}
TOKEN_OK = {"token": "abc123"}


def _mock_resp(status_code: int, body: dict):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.is_success = status_code < 400
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp
        )
    return resp


def invoke(*prompts, extra_args=None):
    """Invoke `datum register` with sequenced prompt inputs."""
    args = ["register", BASE_URL] + (extra_args or [])
    return runner.invoke(app, args, input="\n".join(prompts) + "\n")


def parse_json(output: str) -> dict:
    """Extract the JSON object from output that may contain prompt text before it."""
    return json.loads(output[output.index("{"):])


class TestRegisterSuccess:
    def test_exits_0(self):
        with (
            patch("httpx.post", side_effect=[
                _mock_resp(201, REGISTER_OK),
                _mock_resp(200, TOKEN_OK),
            ]),
            patch("datum.commands.register.save_config"),
            patch("datum.commands.register.load_config", return_value={}),
        ):
            result = invoke("testuser", "password1", "password1")
        assert result.exit_code == 0, result.output

    def test_output_confirms_registration(self):
        with (
            patch("httpx.post", side_effect=[
                _mock_resp(201, REGISTER_OK),
                _mock_resp(200, TOKEN_OK),
            ]),
            patch("datum.commands.register.save_config"),
            patch("datum.commands.register.load_config", return_value={}),
        ):
            result = invoke("testuser", "password1", "password1")
        assert "testuser" in result.output

    def test_json_output(self):
        with (
            patch("httpx.post", side_effect=[
                _mock_resp(201, REGISTER_OK),
                _mock_resp(200, TOKEN_OK),
            ]),
            patch("datum.commands.register.save_config"),
            patch("datum.commands.register.load_config", return_value={}),
        ):
            result = runner.invoke(
                app, ["--output", "json", "register", BASE_URL],
                input="testuser\npassword1\npassword1\n",
            )
        assert result.exit_code == 0
        data = parse_json(result.output)
        assert data["registered"] is True
        assert data["username"] == "testuser"
        assert data["logged_in"] is True


class TestRegisterValidation:
    def test_password_mismatch_exits_1(self):
        result = invoke("testuser", "password1", "different")
        assert result.exit_code == 1

    def test_password_mismatch_json(self):
        result = runner.invoke(
            app, ["--output", "json", "register", BASE_URL],
            input="testuser\npassword1\ndifferent\n",
        )
        data = parse_json(result.output)
        assert data["registered"] is False
        assert "match" in data["error"].lower()


class TestRegisterErrors:
    def test_username_taken_exits_1(self):
        with patch("httpx.post", return_value=_mock_resp(409, {"detail": "Username already taken"})):
            result = invoke("taken", "password1", "password1")
        assert result.exit_code == 1

    def test_username_taken_json(self):
        with patch("httpx.post", return_value=_mock_resp(409, {"detail": "Username already taken"})):
            result = runner.invoke(
                app, ["--output", "json", "register", BASE_URL],
                input="taken\npassword1\npassword1\n",
            )
        data = parse_json(result.output)
        assert data["registered"] is False

    def test_network_error_exits_2(self):
        with patch("httpx.post", side_effect=httpx.HTTPError("timeout")):
            result = invoke("testuser", "password1", "password1")
        assert result.exit_code == 2
