"""datum init — interactive wizard to create a datapackage.json."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import typer
from pydantic import ValidationError
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.text import Text

from datum.commands.config import load_config
from datum.commands.login import get_username_key
from datum.console import console, err_console
from datum.models import CHECKSUM_PATTERN, PUBLISHER_PATTERN, SLUG_PATTERN, DataPackage
from datum.state import state


# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------


def _prompt_slug(label: str, default: Optional[str] = None) -> str:
    """Ask for a slug, re-prompting on invalid input."""
    while True:
        kwargs: dict = {}
        if default:
            kwargs["default"] = default
        value = Prompt.ask(f"  {label}", **kwargs).strip()
        if not value:
            err_console.print("  [error]This field is required.[/error]")
            continue
        if not SLUG_PATTERN.match(value):
            err_console.print(
                "  [error]Must use only lowercase letters, digits, and hyphens, "
                "and must not start or end with a hyphen.[/error]"
            )
            continue
        return value


def _prompt_publisher_slug(label: str, default: Optional[str] = None) -> str:
    """Ask for a publisher slug (allows dots for domains), re-prompting on invalid input."""
    while True:
        kwargs: dict = {}
        if default:
            kwargs["default"] = default
        value = Prompt.ask(f"  {label}", **kwargs).strip()
        if not value:
            err_console.print("  [error]This field is required.[/error]")
            continue
        if not PUBLISHER_PATTERN.match(value):
            err_console.print(
                "  [error]Must use only lowercase letters, digits, hyphens, and dots, "
                "and must not start or end with a hyphen or dot.[/error]"
            )
            continue
        return value


def _prompt_required(label: str, default: Optional[str] = None) -> str:
    """Ask for any non-empty string."""
    while True:
        kwargs: dict = {}
        if default:
            kwargs["default"] = default
        value = Prompt.ask(f"  {label}", **kwargs).strip()
        if not value:
            err_console.print("  [error]This field is required.[/error]")
            continue
        return value


def _prompt_url(label: str, required: bool = True, default: Optional[str] = None) -> Optional[str]:
    """Ask for a URL, validating the scheme."""
    while True:
        kwargs: dict = {}
        if default:
            kwargs["default"] = default
        value = Prompt.ask(f"  {label}", **kwargs).strip()
        if not value:
            if required:
                err_console.print("  [error]This field is required.[/error]")
                continue
            return None
        if not value.startswith(("http://", "https://")):
            err_console.print("  [error]URL must start with http:// or https://[/error]")
            continue
        return value


def _prompt_optional(label: str, default: str = "") -> Optional[str]:
    """Ask for an optional string; returns None if empty."""
    value = Prompt.ask(f"  {label}", default=default).strip()
    return value if value else None


def _prompt_int(label: str) -> Optional[int]:
    """Ask for an optional integer."""
    while True:
        value = Prompt.ask(f"  {label}", default="").strip()
        if not value:
            return None
        try:
            n = int(value)
            if n < 0:
                raise ValueError
            return n
        except ValueError:
            err_console.print("  [error]Must be a non-negative integer.[/error]")


def _prompt_checksum(label: str) -> Optional[str]:
    """Ask for an optional checksum string."""
    while True:
        value = Prompt.ask(f"  {label}", default="").strip()
        if not value:
            return None
        if not CHECKSUM_PATTERN.match(value):
            err_console.print(
                "  [error]Expected format: sha256:<hex>, sha512:<hex>, or md5:<hex>[/error]"
            )
            continue
        return value


def _get_stored_username() -> Optional[str]:
    """Return the username stored for the active registry, if any.

    Checks the explicit registry from state first, then falls back to
    scanning config for any stored username (covers the common case where
    the user logged in but hasn't set a default registry in config).
    """
    cfg = load_config()

    # Prefer the explicitly active registry
    registry = state.registry or cfg.get("registry", "")
    if registry and registry.startswith(("http://", "https://")):
        host = urlparse(registry).netloc
        if host:
            value = cfg.get(get_username_key(host))
            if value:
                return value

    # Fall back: if exactly one username.* key exists, use it
    usernames = [v for k, v in cfg.items() if k.startswith("username.")]
    return usernames[0] if len(usernames) == 1 else None


def _guess_format(url: str) -> str:
    """Guess a file format from the URL path extension."""
    path = urlparse(url).path
    ext = os.path.splitext(path)[1].lstrip(".").lower()
    return ext if ext else "csv"


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


def cmd_init(
    output: Path = typer.Option(
        Path("datapackage.json"),
        "--output",
        "-o",
        help="Path to write the datapackage.json",
    ),
) -> None:
    """
    Interactively create a datapackage.json for your dataset.

    Guides you through the required and optional fields, validates every
    input, and writes the finished file to the current directory.
    """
    console.print()
    console.print(
        Panel(
            Text("Datum — Dataset Initialization Wizard", justify="center"),
            border_style="cyan",
            padding=(0, 2),
        )
    )
    console.print()

    # Warn if file already exists
    if output.exists():
        console.print(f"[warning]A file already exists at [bold]{output}[/bold].[/warning]")
        if not Confirm.ask("  Overwrite it?", default=False):
            console.print("[muted]Aborted.[/muted]")
            raise typer.Exit()
        console.print()

    # -----------------------------------------------------------------------
    # Step 1: Identifier
    # -----------------------------------------------------------------------
    console.print(Rule("[bold]Step 1 · Dataset Identifier[/bold]", style="cyan"))
    console.print(
        "  The identifier format is [bold cyan]publisher/namespace/dataset[/bold cyan]\n"
        "  Publisher may be a domain (e.g. [bold]norge.no[/bold]) or a simple slug.\n"
        "  Example: [identifier]norge.no/population/census[/identifier]  or  [identifier]acme/samples/demo[/identifier]"
    )
    console.print()

    stored_username = _get_stored_username()
    if stored_username:
        console.print(f"  [muted]Logged in as [bold]{stored_username}[/bold] — press Enter to use as publisher slug.[/muted]\n")

    publisher_slug = _prompt_publisher_slug("Publisher slug", default=stored_username)
    namespace_slug = _prompt_slug("Namespace slug  (e.g. population, weather)")
    dataset_slug = _prompt_slug("Dataset slug    (e.g. census, oslo-hourly)")
    identifier = f"{publisher_slug}/{namespace_slug}/{dataset_slug}"
    console.print(f"\n  [success]✓[/success] Identifier: [identifier]{identifier}[/identifier]\n")

    # -----------------------------------------------------------------------
    # Step 2: Version
    # -----------------------------------------------------------------------
    console.print(Rule("[bold]Step 2 · Version[/bold]", style="cyan"))
    console.print(
        "  Common formats: [bold]2024-01[/bold] (date), [bold]1.0.0[/bold] (semver), "
        "[bold]latest[/bold]"
    )
    console.print()
    version = _prompt_required("Version", default="latest")
    console.print()

    # -----------------------------------------------------------------------
    # Step 3: Metadata
    # -----------------------------------------------------------------------
    console.print(Rule("[bold]Step 3 · Metadata[/bold]", style="cyan"))
    console.print()
    title = _prompt_required("Title")
    description = _prompt_optional("Description          (optional)")
    license_val = _prompt_optional("License              (e.g. CC-BY-4.0, ODbL, MIT)", default="CC-BY-4.0")
    console.print()

    # -----------------------------------------------------------------------
    # Step 4: Publisher
    # -----------------------------------------------------------------------
    console.print(Rule("[bold]Step 4 · Publisher[/bold]", style="cyan"))
    console.print()
    publisher_name = _prompt_required("Publisher name")
    publisher_url = _prompt_url("Publisher URL        (optional)", required=False)
    console.print()

    # -----------------------------------------------------------------------
    # Step 5: Sources
    # -----------------------------------------------------------------------
    console.print(Rule("[bold]Step 5 · Data Sources[/bold]", style="cyan"))
    console.print("  Add at least one URL pointing to the actual data file(s).")
    console.print()

    sources: list[dict] = []
    source_num = 1
    while True:
        console.print(f"  [bold]Source {source_num}[/bold]")
        url = _prompt_url("URL")
        guessed_fmt = _guess_format(url)
        fmt = _prompt_required("Format               (e.g. csv, parquet, json)", default=guessed_fmt)
        size = _prompt_int("File size in bytes   (optional)")
        checksum = _prompt_checksum("Checksum             (optional, e.g. sha256:abc123…)")

        source: dict = {"url": url, "format": fmt}
        if size is not None:
            source["size"] = size
        if checksum:
            source["checksum"] = checksum
        sources.append(source)

        console.print()
        if not Confirm.ask("  Add another source?", default=False):
            break
        source_num += 1
        console.print()

    # -----------------------------------------------------------------------
    # Step 6: Tags
    # -----------------------------------------------------------------------
    console.print()
    console.print(Rule("[bold]Step 6 · Tags[/bold]", style="cyan"))
    console.print()
    raw_tags = _prompt_optional("Tags, comma-separated (optional)  e.g. weather, norway, oslo")
    tags: Optional[list[str]] = (
        [t.strip() for t in raw_tags.split(",") if t.strip()] if raw_tags else None
    )
    console.print()

    # -----------------------------------------------------------------------
    # Assemble & validate
    # -----------------------------------------------------------------------
    today = date.today().isoformat()
    payload: dict = {
        "id": identifier,
        "version": version,
        "title": title,
        "publisher": {
            "name": publisher_name,
            **({"url": publisher_url} if publisher_url else {}),
        },
        "sources": sources,
        "created": today,
        "updated": today,
    }
    if description:
        payload["description"] = description
    if license_val:
        payload["license"] = license_val
    if tags:
        payload["tags"] = tags

    # Run through Pydantic to catch anything we missed in the prompts.
    try:
        pkg = DataPackage.model_validate(payload)
    except ValidationError as exc:
        err_console.print("\n[error]Validation failed — the following errors were found:[/error]")
        for error in exc.errors():
            field = ".".join(str(p) for p in error["loc"])
            err_console.print(f"  [error]•[/error] [key]{field}[/key]: {error['msg']}")
        err_console.print("\nFix the errors above and run [bold]datum init[/bold] again.")
        raise typer.Exit(code=1)

    # Write
    output.write_text(json.dumps(pkg.to_dict(), indent=2, ensure_ascii=False) + "\n")

    console.print(Rule(style="green"))
    console.print(f"  [success]✓[/success] Created [bold]{output}[/bold]")
    console.print()
    console.print("  Next steps:")
    console.print("    [bold]datum check[/bold]    — validate the metadata")
    console.print("    [bold]datum publish[/bold]   — publish to the registry")
    console.print()
