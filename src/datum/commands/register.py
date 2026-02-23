"""datum register — create an account on a Datum registry."""

from __future__ import annotations

import json
from urllib.parse import urlparse

import httpx
import typer

from datum.commands.config import load_config, save_config, set_auth
from datum.console import console, err_console
from datum.state import OutputFormat, state

DEFAULT_REGISTRY = "https://datumhub.org"


def cmd_register(
    url: str = typer.Argument(
        DEFAULT_REGISTRY,
        help="Registry URL to register with",
    ),
) -> None:
    """
    Create a new account on a Datum registry.

    Prompts for a username and password, registers the account, then
    stores the API token so you are logged in immediately.

    Credentials are stored in [bold]~/.datum/config.json[/bold].
    """
    output_fmt = state.output
    quiet = state.quiet

    if not quiet and output_fmt != OutputFormat.json:
        console.print(f"\n  Creating account on [bold]{url}[/bold]\n")

    username = typer.prompt("  Username")
    password = typer.prompt("  Password", hide_input=True)
    password2 = typer.prompt("  Confirm password", hide_input=True)

    if password != password2:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"registered": False, "error": "Passwords do not match"}))
        else:
            err_console.print("\n[error]✗[/error] Passwords do not match.\n")
        raise typer.Exit(code=1)

    # Register
    try:
        resp = httpx.post(
            f"{url.rstrip('/')}/api/auth/register",
            json={"username": username, "password": password},
            timeout=10,
        )
    except httpx.HTTPError as exc:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"registered": False, "error": str(exc)}))
        else:
            err_console.print(
                f"\n[error]✗[/error] Could not reach [bold]{url}[/bold].\n"
                "  Check the registry URL and your network connection.\n"
            )
        raise typer.Exit(code=2)

    if resp.status_code == 422:
        detail = resp.json().get("detail", "Invalid input")
        if isinstance(detail, list):
            detail = "; ".join(e.get("msg", str(e)) for e in detail)
        if output_fmt == OutputFormat.json:
            print(json.dumps({"registered": False, "error": detail}))
        else:
            err_console.print(f"\n[error]✗[/error] {detail}\n")
        raise typer.Exit(code=1)

    if resp.status_code == 409:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"registered": False, "error": f"Username {username!r} is already taken"}))
        else:
            err_console.print(
                f"\n[error]✗[/error] Username [bold]{username}[/bold] is already taken.\n"
            )
        raise typer.Exit(code=1)

    if not resp.is_success:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"registered": False, "error": f"HTTP {resp.status_code}"}))
        else:
            err_console.print(f"\n[error]✗[/error] Registration failed (HTTP {resp.status_code}).\n")
        raise typer.Exit(code=1)

    # Auto-login: fetch a token immediately
    try:
        token_resp = httpx.post(
            f"{url.rstrip('/')}/api/auth/token",
            json={"username": username, "password": password},
            timeout=10,
        )
        token_resp.raise_for_status()
        token = token_resp.json()["token"]
        host = urlparse(url).netloc or url
        cfg = load_config()
        set_auth(cfg, host, token, username)
        save_config(cfg)
        logged_in = True
    except Exception:
        logged_in = False

    if output_fmt == OutputFormat.json:
        print(json.dumps({"registered": True, "username": username, "logged_in": logged_in}))
    elif not quiet:
        console.print(f"\n  [success]✓[/success]  Account [bold]{username}[/bold] created")
        if logged_in:
            console.print(f"  [success]✓[/success]  Logged in to [bold]{url}[/bold]")
        console.print()
