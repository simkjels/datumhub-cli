"""datum whoami — show active registry and login status."""

from __future__ import annotations

import json
from urllib.parse import urlparse

import typer

from datum.commands.config import get_token, get_username, load_config
from datum.console import console
from datum.state import OutputFormat, state


def cmd_whoami() -> None:
    """
    Show the active registry and your login status.

    Prints the configured registry, the stored username (if any),
    and whether a valid token is present.
    """
    output_fmt = state.output
    cfg = load_config()

    registry = state.registry or cfg.get("registry", "") or "~/.datum/registry (local)"

    username: str | None = None
    has_token: bool = False

    if state.is_remote:
        host = urlparse(registry).netloc
        username = get_username(cfg, host)
        has_token = get_token(cfg, host) is not None

    if output_fmt == OutputFormat.json:
        print(json.dumps({
            "registry": registry,
            "is_remote": state.is_remote,
            "username": username,
            "authenticated": has_token,
        }, indent=2))
        return

    console.print()
    console.print(f"  [bold]Registry:[/bold]  {registry}")
    if state.is_remote:
        if username and has_token:
            console.print(f"  [bold]Logged in:[/bold] [success]✓[/success]  {username}")
        elif has_token:
            console.print("  [bold]Logged in:[/bold] [success]✓[/success]  (username not stored)")
        else:
            console.print("  [bold]Logged in:[/bold] [error]✗[/error]  not authenticated")
            console.print(f"  Run [bold]datum login {registry}[/bold] to authenticate.")
    else:
        console.print("  [bold]Mode:[/bold]      local (no authentication required)")
    console.print()
