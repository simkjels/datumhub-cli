"""datum info — show dataset metadata without downloading."""

from __future__ import annotations

import json

import typer
from rich import box
from rich.panel import Panel
from rich.table import Table

from datum.console import console, err_console
from datum.models import ID_PATTERN
from datum.registry.local import get_local_registry
from datum.state import OutputFormat, state


def cmd_info(
    identifier: str = typer.Argument(
        ..., help="Dataset identifier (publisher.namespace.dataset[:version])"
    ),
) -> None:
    """
    Show full metadata for a dataset without downloading.

    IDENTIFIER format: [bold]publisher.namespace.dataset[:version][/bold]

    Omit :version to show the latest published version.

    Exit codes: 0 success · 1 not found / bad identifier
    """
    output_fmt = state.output
    quiet = state.quiet

    # Parse identifier
    if ":" in identifier:
        id_part, version = identifier.split(":", 1)
    else:
        id_part = identifier
        version = "latest"

    # Validate
    if not ID_PATTERN.match(id_part):
        if output_fmt == OutputFormat.json:
            print(json.dumps({"error": f"Invalid identifier format: {id_part!r}"}, indent=2))
        else:
            err_console.print(
                f"\n[error]✗[/error] Invalid identifier: [bold]{id_part}[/bold]\n\n"
                "  Expected [bold]publisher.namespace.dataset[/bold] "
                "(three dot-separated slugs — e.g. met.no.oslo-hourly)"
            )
        raise typer.Exit(code=1)

    # Resolve
    registry = get_local_registry()
    pkg = registry.latest(id_part) if version == "latest" else registry.get(id_part, version)

    if pkg is None:
        label = f"{id_part}:{version}"
        if output_fmt == OutputFormat.json:
            print(json.dumps({"error": f"Not found: {label}"}, indent=2))
        else:
            msg = (
                f"\n[error]✗[/error] Dataset [bold]{label}[/bold] not found in the registry.\n"
            )
            suggestions = registry.suggest(id_part)
            if suggestions:
                msg += "\n  Did you mean?\n" + "".join(f"    {s}\n" for s in suggestions)
            err_console.print(msg)
        raise typer.Exit(code=1)

    if output_fmt == OutputFormat.json:
        print(json.dumps(pkg.to_dict(), indent=2, ensure_ascii=False))
        return

    if not quiet:
        _print_info(pkg)


# ---------------------------------------------------------------------------
# Output renderer
# ---------------------------------------------------------------------------


def _print_info(pkg) -> None:
    console.print()
    console.print(
        Panel(
            f"[identifier]{pkg.id}[/identifier]  [muted]@{pkg.version}[/muted]\n"
            f"[bold]{pkg.title}[/bold]",
            border_style="cyan",
            padding=(0, 2),
        )
    )

    meta = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
    meta.add_column("key", style="key", min_width=14)
    meta.add_column("value")

    publisher_str = pkg.publisher.name
    if pkg.publisher.url:
        publisher_str += f"  [muted]{pkg.publisher.url}[/muted]"
    meta.add_row("Publisher", publisher_str)

    if pkg.description:
        meta.add_row("Description", pkg.description)
    if pkg.license:
        meta.add_row("License", pkg.license)
    if pkg.tags:
        meta.add_row("Tags", "  ".join(pkg.tags))
    if pkg.created:
        meta.add_row("Created", pkg.created)
    if pkg.updated:
        meta.add_row("Updated", pkg.updated)

    console.print(meta)
    console.print()

    console.print(f"  [bold]Sources[/bold] ({len(pkg.sources)})\n")

    src_table = Table(box=box.SIMPLE, show_header=True, header_style="bold white", padding=(0, 1))
    src_table.add_column("URL")
    src_table.add_column("Format", min_width=8)
    src_table.add_column("Size", justify="right", min_width=8)
    src_table.add_column("Checksum")

    for source in pkg.sources:
        size_str = _fmt_size(source.size) if source.size else "[muted]—[/muted]"
        checksum_str = source.checksum if source.checksum else "[muted]—[/muted]"
        src_table.add_row(source.url, source.format, size_str, checksum_str)

    console.print(src_table)
    console.print()


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
