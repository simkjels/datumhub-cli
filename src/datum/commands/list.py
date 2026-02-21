"""datum list / datum ls — list datasets in a registry."""

from __future__ import annotations

import json

import typer
from rich import box
from rich.table import Table

from datum.console import console, err_console
from datum.registry.local import get_registry
from datum.state import OutputFormat, state


def cmd_list() -> None:
    """
    List datasets in the local registry.

    Shows all datasets published to [bold]~/.datum/registry/[/bold].
    Exits with code 0 always (an empty registry is not an error).

    Use [bold]--output json[/bold] for machine-readable output:

        datum list --output json | jq .
    """
    output_fmt = state.output
    quiet = state.quiet

    registry = get_registry()
    try:
        packages = registry.list()
    except RuntimeError as exc:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            err_console.print(f"\n[error]✗[/error] {exc}\n")
        raise typer.Exit(code=2)

    if output_fmt == OutputFormat.json:
        print(json.dumps([p.to_dict() for p in packages], indent=2, ensure_ascii=False))
        return

    is_remote = bool(state.registry and state.registry.startswith(("http://", "https://")))

    if not packages:
        if not quiet:
            console.print()
            if is_remote:
                console.print("  [muted]No datasets found.[/muted]")
            else:
                console.print("  [muted]No datasets in local registry.[/muted]")
                console.print("  Run [bold]datum publish[/bold] to add one.")
            console.print()
        return

    if not quiet:
        console.print()
        location = state.registry if is_remote else "local registry"
        console.print(f"  [bold]{len(packages)}[/bold] dataset(s) in {location}\n")

        table = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
        table.add_column("ID", style="identifier", min_width=30)
        table.add_column("Version", min_width=9)
        table.add_column("Title", min_width=20)
        table.add_column("Publisher", min_width=18)
        table.add_column("Sources", justify="right")

        for pkg in packages:
            table.add_row(
                pkg.id,
                pkg.version,
                pkg.title,
                pkg.publisher.name,
                str(len(pkg.sources)),
            )

        console.print(table)
        console.print()
