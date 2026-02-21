"""datum cache — manage the local dataset cache."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import List, NamedTuple

import typer
from rich import box
from rich.table import Table

from datum.console import console, err_console
from datum.state import OutputFormat, state

cache_app = typer.Typer(help="Manage the local dataset cache.")


def get_cache_root() -> Path:
    return Path("~/.datum/cache").expanduser()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class CacheEntry(NamedTuple):
    dataset_id: str   # publisher.namespace.dataset
    version: str
    files: List[Path]

    @property
    def size(self) -> int:
        return sum(f.stat().st_size for f in self.files)


def _scan_cache(root: Path) -> List[CacheEntry]:
    """Walk cache root and return one entry per dataset version."""
    entries: List[CacheEntry] = []
    if not root.exists():
        return entries
    for pub_dir in sorted(root.iterdir()):
        if not pub_dir.is_dir():
            continue
        for ns_dir in sorted(pub_dir.iterdir()):
            if not ns_dir.is_dir():
                continue
            for ds_dir in sorted(ns_dir.iterdir()):
                if not ds_dir.is_dir():
                    continue
                for ver_dir in sorted(ds_dir.iterdir()):
                    if not ver_dir.is_dir():
                        continue
                    files = [f for f in ver_dir.iterdir() if f.is_file()]
                    dataset_id = f"{pub_dir.name}.{ns_dir.name}.{ds_dir.name}"
                    entries.append(CacheEntry(dataset_id, ver_dir.name, files))
    return entries


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


@cache_app.command("list")
def cache_list() -> None:
    """List all cached datasets."""
    output_fmt = state.output
    quiet = state.quiet

    entries = _scan_cache(get_cache_root())

    if output_fmt == OutputFormat.json:
        import json
        payload = [
            {
                "id": e.dataset_id,
                "version": e.version,
                "files": [str(f) for f in e.files],
                "size": e.size,
            }
            for e in entries
        ]
        print(json.dumps(payload, indent=2))
        return

    if quiet:
        return

    if not entries:
        console.print()
        console.print("  [muted]Cache is empty.[/muted]")
        console.print(f"  [muted]{get_cache_root()}[/muted]")
        console.print()
        return

    total_size = sum(e.size for e in entries)
    total_files = sum(len(e.files) for e in entries)

    console.print()
    console.print(
        f"  [bold]{len(entries)}[/bold] cached version(s)  "
        f"[muted]·[/muted]  {_fmt_size(total_size)}  "
        f"[muted]·[/muted]  {get_cache_root()}\n"
    )

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold white")
    table.add_column("Dataset", style="identifier", min_width=30)
    table.add_column("Version", min_width=9)
    table.add_column("Files", justify="right", min_width=5)
    table.add_column("Size", justify="right", min_width=8)

    for entry in entries:
        table.add_row(
            entry.dataset_id,
            entry.version,
            str(len(entry.files)),
            _fmt_size(entry.size),
        )

    console.print(table)
    console.print()


@cache_app.command("size")
def cache_size() -> None:
    """Show total disk usage of the local cache."""
    output_fmt = state.output
    quiet = state.quiet

    entries = _scan_cache(get_cache_root())
    total_size = sum(e.size for e in entries)
    total_files = sum(len(e.files) for e in entries)

    if output_fmt == OutputFormat.json:
        import json
        print(json.dumps({"size_bytes": total_size, "files": total_files}, indent=2))
        return

    if quiet:
        return

    console.print()
    console.print(f"  [bold]Cache:[/bold]  {get_cache_root()}")
    console.print(f"  [bold]Total:[/bold]  {_fmt_size(total_size)}  [muted]({total_files} file(s))[/muted]")
    console.print()


@cache_app.command("clear")
def cache_clear(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Remove all cached datasets."""
    root = get_cache_root()
    entries = _scan_cache(root)

    if not entries:
        console.print()
        console.print("  [muted]Cache is already empty.[/muted]")
        console.print()
        return

    total_size = sum(e.size for e in entries)
    total_files = sum(len(e.files) for e in entries)

    if not yes:
        confirmed = typer.confirm(
            f"  Clear {_fmt_size(total_size)} ({total_files} file(s)) from cache?",
            default=False,
        )
        if not confirmed:
            console.print("  [muted]Aborted.[/muted]")
            return

    shutil.rmtree(root, ignore_errors=True)

    console.print()
    console.print(f"  [success]✓[/success] Cache cleared  [muted]({_fmt_size(total_size)} freed)[/muted]")
    console.print()
