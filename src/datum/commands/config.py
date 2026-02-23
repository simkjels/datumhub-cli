"""datum config — manage local Datum configuration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import typer
from rich import box
from rich.table import Table

from datum.console import console, err_console
from datum.state import OutputFormat, state

config_app = typer.Typer(help="Manage local Datum configuration.")

# Keys with descriptions shown in `datum config list`
KNOWN_KEYS = {
    "registry": "Default registry URL or local path",
    "output":   "Default output format  (table | json | plain)",
}


def get_config_path() -> Path:
    return Path("~/.datum/config.json").expanduser()


def load_config() -> dict:
    p = get_config_path()
    if not p.exists():
        return {}
    try:
        cfg = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        # Import here to avoid circular import at module load time
        from datum.console import err_console
        err_console.print(
            f"\n[warning]⚠[/warning]  Config file is corrupted and cannot be parsed.\n"
            f"  Path:  [bold]{p}[/bold]\n"
            f"  Error: {exc}\n\n"
            f"  Fix or delete the file and try again.\n"
        )
        raise SystemExit(2)
    except OSError as exc:
        from datum.console import err_console
        err_console.print(f"\n[error]✗[/error]  Could not read config file: {exc}\n")
        raise SystemExit(2)
    if _migrate_config(cfg):
        save_config(cfg)
    return cfg


def save_config(cfg: dict) -> None:
    p = get_config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")


def _migrate_config(cfg: dict) -> bool:
    """Convert v1 flat token.{host}/username.{host} keys to v2 nested auth. Returns True if changed."""
    if cfg.get("_version", 1) >= 2:
        return False

    auth: dict = {}
    to_remove = []
    for key in list(cfg):
        if key.startswith("token."):
            host = key[len("token."):]
            auth.setdefault(host, {})["token"] = cfg[key]
            to_remove.append(key)
        elif key.startswith("username."):
            host = key[len("username."):]
            auth.setdefault(host, {})["username"] = cfg[key]
            to_remove.append(key)

    for key in to_remove:
        del cfg[key]

    if auth:
        existing = cfg.get("auth", {})
        for host, data in auth.items():
            existing.setdefault(host, {}).update(data)
        cfg["auth"] = existing

    cfg["_version"] = 2
    return True


def get_token(cfg: dict, host: str) -> Optional[str]:
    """Return the stored token for *host*, or None."""
    return cfg.get("auth", {}).get(host, {}).get("token")


def get_username(cfg: dict, host: str) -> Optional[str]:
    """Return the stored username for *host*, or None."""
    return cfg.get("auth", {}).get(host, {}).get("username")


def set_auth(cfg: dict, host: str, token: str, username: Optional[str] = None) -> None:
    """Store *token* (and optionally *username*) for *host*."""
    cfg.setdefault("auth", {}).setdefault(host, {})["token"] = token
    if username is not None:
        cfg["auth"][host]["username"] = username


def clear_auth(cfg: dict, host: str) -> None:
    """Remove all stored auth for *host*."""
    cfg.get("auth", {}).pop(host, None)


# Keep private aliases for backwards compat within this module
_load = load_config
_save = save_config


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


@config_app.command("get")
def config_get(
    key: str = typer.Argument(..., help="Configuration key"),
) -> None:
    """Print the value of a configuration key."""
    _show_one(key)


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Configuration key"),
    value: str = typer.Argument(..., help="Configuration value"),
) -> None:
    """Set a configuration value."""
    output_fmt = state.output
    quiet = state.quiet

    cfg = _load()
    cfg[key] = value
    _save(cfg)

    if output_fmt == OutputFormat.json:
        print(json.dumps({"key": key, "value": value}, indent=2))
    elif not quiet:
        console.print(f"  [success]✓[/success]  [bold]{key}[/bold] = {value}")


@config_app.command("show")
def config_show(
    key: Optional[str] = typer.Argument(None, help="Key to show (omit to show all)"),
) -> None:
    """Show one key or all configuration values."""
    if key is not None:
        return _show_one(key)
    _show_all()


@config_app.command("list")
def config_list() -> None:
    """List all configuration values."""
    _show_all()


def _show_one(key: str) -> None:
    output_fmt = state.output
    cfg = _load()

    if key not in cfg:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"key": key, "value": None}, indent=2))
        else:
            err_console.print(f"\n[error]✗[/error] Key [bold]{key}[/bold] is not set.\n")
        raise typer.Exit(code=1)

    value = cfg[key]
    if output_fmt == OutputFormat.json:
        print(json.dumps({"key": key, "value": value}, indent=2))
    else:
        console.print(value)


def _show_all() -> None:
    output_fmt = state.output
    quiet = state.quiet
    cfg = _load()

    if output_fmt == OutputFormat.json:
        print(json.dumps(cfg, indent=2, ensure_ascii=False))
        return

    if quiet:
        return

    console.print()

    if not cfg:
        console.print("  [muted]No configuration set.[/muted]")
        console.print(f"  [muted]{get_config_path()}[/muted]")
        console.print()
        return

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    table.add_column("Key", style="key", min_width=16)
    table.add_column("Value", min_width=20)
    table.add_column("Description", style="muted")

    for k, v in sorted(cfg.items()):
        desc = KNOWN_KEYS.get(k, "")
        table.add_row(k, str(v), desc)

    console.print(table)
    console.print()


@config_app.command("unset")
def config_unset(
    key: str = typer.Argument(..., help="Configuration key to remove"),
) -> None:
    """Remove a configuration key."""
    output_fmt = state.output
    quiet = state.quiet
    cfg = _load()

    if key not in cfg:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"key": key, "removed": False}, indent=2))
        else:
            err_console.print(f"\n[error]✗[/error] Key [bold]{key}[/bold] is not set.\n")
        raise typer.Exit(code=1)

    del cfg[key]
    _save(cfg)

    if output_fmt == OutputFormat.json:
        print(json.dumps({"key": key, "removed": True}, indent=2))
    elif not quiet:
        console.print(f"  [success]✓[/success]  [bold]{key}[/bold] removed")
