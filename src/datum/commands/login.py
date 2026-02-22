"""datum login — authenticate with a Datum registry."""

from __future__ import annotations

import json
from typing import Optional
from urllib.parse import urlparse

import httpx
import typer

from datum.commands.config import get_config_path, load_config, save_config
from datum.console import console, err_console
from datum.state import OutputFormat, state


def get_token_key(host: str) -> str:
    """Config key used to store the token for a given registry host."""
    return f"token.{host}"


def get_username_key(host: str) -> str:
    """Config key used to store the username for a given registry host."""
    return f"username.{host}"


def cmd_login(
    url: str = typer.Argument(
        "https://datumhub.org",
        help="Registry URL to authenticate with",
    ),
    token: Optional[str] = typer.Option(
        None, "--token", "-t",
        help="API token — skips the username/password prompt",
    ),
) -> None:
    """
    Authenticate with a Datum registry.

    Credentials are stored in [bold]~/.datum/config.json[/bold].
    Run [bold]datum logout[/bold] to remove them.
    """
    output_fmt = state.output
    quiet = state.quiet
    host = urlparse(url).netloc or url

    if not quiet and output_fmt != OutputFormat.json:
        console.print(f"\n  Logging in to [bold]{url}[/bold]\n")

    collected_username: Optional[str] = None
    if token is None:
        collected_username = typer.prompt("  Username")
        password = typer.prompt("  Password", hide_input=True)
        token = _fetch_token(url, collected_username, password, output_fmt)
        if token is None:
            raise typer.Exit(code=1)

    cfg = load_config()
    cfg[get_token_key(host)] = token
    if collected_username is not None:
        cfg[get_username_key(host)] = collected_username
    save_config(cfg)

    if output_fmt == OutputFormat.json:
        print(json.dumps({"logged_in": True, "registry": url}, indent=2))
    elif not quiet:
        console.print(f"\n  [success]✓[/success]  Logged in to [bold]{url}[/bold]\n")


def cmd_logout(
    url: str = typer.Argument(
        "https://datumhub.org",
        help="Registry URL to log out from",
    ),
) -> None:
    """
    Remove stored credentials for a registry.
    """
    output_fmt = state.output
    quiet = state.quiet
    host = urlparse(url).netloc or url
    key = get_token_key(host)

    cfg = load_config()
    if key in cfg:
        del cfg[key]
        cfg.pop(get_username_key(host), None)
        save_config(cfg)

    if output_fmt == OutputFormat.json:
        print(json.dumps({"logged_out": True, "registry": url}, indent=2))
    elif not quiet:
        console.print(f"\n  [success]✓[/success]  Logged out from [bold]{url}[/bold]\n")


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _fetch_token(url: str, username: str, password: str, output_fmt) -> Optional[str]:
    """POST credentials to the registry and return the token, or None on failure."""
    try:
        resp = httpx.post(
            f"{url.rstrip('/')}/api/auth/token",
            json={"username": username, "password": password},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["token"]
    except httpx.HTTPStatusError as exc:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"logged_in": False, "error": str(exc)}, indent=2))
        else:
            err_console.print(f"\n[error]✗[/error] Authentication failed: {exc}\n")
        return None
    except httpx.HTTPError as exc:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"logged_in": False, "error": str(exc)}, indent=2))
        else:
            err_console.print(
                f"\n[error]✗[/error] Could not reach [bold]{url}[/bold].\n"
                "  Check the registry URL and your network connection.\n"
            )
        return None
