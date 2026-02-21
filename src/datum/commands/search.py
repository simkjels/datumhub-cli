"""datum search — search the registry by keyword."""

from __future__ import annotations

import json
from typing import List

import typer
from rich import box
from rich.table import Table

from datum.console import console, err_console
from datum.models import DataPackage
from datum.registry.local import get_registry
from datum.state import OutputFormat, state


def cmd_search(
    query: str = typer.Argument(..., help="Keyword to search for"),
) -> None:
    """
    Search the local registry for datasets matching a keyword.

    Matches against [bold]id[/bold], [bold]title[/bold], [bold]description[/bold],
    [bold]tags[/bold], and [bold]publisher name[/bold]. Case-insensitive.

    Exit codes: 0 always (no matches is not an error)
    """
    output_fmt = state.output
    quiet = state.quiet

    registry = get_registry()
    is_remote = bool(state.registry and state.registry.startswith(("http://", "https://")))
    try:
        if is_remote:
            matches = registry.list(q=query)
        else:
            matches = _search(registry.list(), query)
    except RuntimeError as exc:
        if output_fmt == OutputFormat.json:
            print(json.dumps([], indent=2))
        else:
            err_console.print(f"\n[error]✗[/error] {exc}\n")
        raise typer.Exit(code=2)

    if output_fmt == OutputFormat.json:
        print(json.dumps([p.to_dict() for p in matches], indent=2, ensure_ascii=False))
        return

    if quiet:
        return

    if not matches:
        console.print()
        console.print(f"  [muted]No datasets found matching[/muted] [bold]{query!r}[/bold].")
        console.print()
        return

    console.print()
    console.print(
        f"  [bold]{len(matches)}[/bold] result(s) for [bold]{query!r}[/bold]\n"
    )

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    table.add_column("ID", style="identifier", min_width=30)
    table.add_column("Version", min_width=9)
    table.add_column("Title", min_width=20)
    table.add_column("Publisher", min_width=18)

    for pkg in matches:
        table.add_row(pkg.id, pkg.version, pkg.title, pkg.publisher.name)

    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------


def _search(packages: List[DataPackage], query: str) -> List[DataPackage]:
    q = query.lower()
    results = []
    for pkg in packages:
        if _matches(pkg, q):
            results.append(pkg)
    return results


def _matches(pkg: DataPackage, q: str) -> bool:
    fields = [
        pkg.id,
        pkg.title,
        pkg.publisher.name,
        pkg.description or "",
        " ".join(pkg.tags or []),
    ]
    return any(q in field.lower() for field in fields)
