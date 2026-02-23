"""datum login — authenticate with a Datum registry."""

from __future__ import annotations

import json
from typing import Optional
from urllib.parse import urlparse

import httpx
import typer

from datum.commands.config import (
    clear_auth,
    get_config_path,  # noqa: F401 — kept so callers can patch via this module
    get_token,
    load_config,
    save_config,
    set_auth,
)
from datum.console import console, err_console
from datum.state import OutputFormat, state


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
    set_auth(cfg, host, token, collected_username)
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

    cfg = load_config()
    was_logged_in = get_token(cfg, host) is not None
    if was_logged_in:
        clear_auth(cfg, host)
        save_config(cfg)

    if output_fmt == OutputFormat.json:
        print(json.dumps({"logged_out": was_logged_in, "registry": url}, indent=2))
    elif not quiet:
        if was_logged_in:
            console.print(f"\n  [success]✓[/success]  Logged out from [bold]{url}[/bold]\n")
        else:
            console.print(
                f"\n  [muted]Not logged in to [bold]{url}[/bold] — nothing to do.[/muted]\n"
            )


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
