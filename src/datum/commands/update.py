"""datum update — pull the latest version of one or all cached datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, Tuple

import typer

from datum.commands.cache import get_cache_root
from datum.console import console, err_console
from datum.models import ID_PATTERN
from datum.registry.local import get_registry
from datum.state import OutputFormat, state
from datum.utils import parse_identifier, sort_versions


def _cached_dataset_ids(cache_root: Path) -> List[str]:
    """Return all unique dataset IDs present in the cache."""
    if not cache_root.exists():
        return []
    ids = set()
    for version_dir in cache_root.glob("*/*/*/*/"):
        parts = version_dir.relative_to(cache_root).parts
        if len(parts) == 4 and version_dir.is_dir():
            pub, ns, ds, _ = parts
            ids.add(f"{pub}/{ns}/{ds}")
    return sorted(ids)


def _cached_versions(cache_root: Path, id_part: str) -> List[str]:
    """Return all cached versions for a dataset id, sorted ascending (newest last)."""
    pub, ns, ds = id_part.split("/")
    folder = cache_root / pub / ns / ds
    if not folder.exists():
        return []
    raw = [d.name for d in folder.iterdir() if d.is_dir()]
    return sort_versions(raw)


def cmd_update(
    identifier: Optional[str] = typer.Argument(
        None,
        help="Dataset identifier without version (omit to update all cached datasets)",
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Re-download even if already at latest version"
    ),
    check: bool = typer.Option(
        False, "--check", help="Show what would be updated without downloading"
    ),
) -> None:
    """
    Pull the latest version of one or all cached datasets.

    With no argument, checks every dataset in the local cache and pulls any
    that have a newer version published in the registry.

    Exit codes: 0 always (nothing to update is not an error)
    """
    output_fmt = state.output
    quiet = state.quiet

    cache_root = get_cache_root()
    registry = get_registry()

    # Determine which dataset IDs to check
    if identifier is not None:
        id_part, _ = parse_identifier(identifier)

        if not ID_PATTERN.match(id_part):
            if output_fmt == OutputFormat.json:
                print(json.dumps({"error": f"Invalid identifier: {id_part!r}"}))
            else:
                err_console.print(f"\n[error]✗[/error] Invalid identifier: [bold]{id_part}[/bold]\n")
            raise typer.Exit(code=1)

        ids_to_check = [id_part]
    else:
        ids_to_check = _cached_dataset_ids(cache_root)

    if not ids_to_check:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"updated": [], "message": "Nothing cached yet."}))
        elif not quiet:
            console.print("\n  [muted]Nothing cached yet.[/muted]\n")
        return

    # Check each dataset for updates
    # List of (id, cached_version, latest_version, needs_update)
    results: List[Tuple[str, Optional[str], Optional[str], bool]] = []

    for ds_id in ids_to_check:
        latest = registry.latest(ds_id)
        if latest is None:
            # Not in registry — skip silently
            continue

        cached = _cached_versions(cache_root, ds_id)
        current = cached[-1] if cached else None  # highest mtime approximation
        needs = force or (latest.version not in cached)
        results.append((ds_id, current, latest.version, needs))

    if not results:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"updated": [], "message": "No registry entries found for cached datasets."}))
        elif not quiet:
            console.print("\n  [muted]No registry entries found.[/muted]\n")
        return

    to_update = [(ds_id, cur, new) for ds_id, cur, new, needs in results if needs]
    up_to_date = [(ds_id, cur) for ds_id, cur, new, needs in results if not needs]

    if not to_update:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"updated": [], "already_latest": [r[0] for r in results]}))
        elif not quiet:
            console.print()
            if len(results) == 1:
                ds_id, _, latest_ver, _ = results[0]
                console.print(
                    f"  [success]✓[/success]  [bold]{ds_id}[/bold] is already at the latest version ([bold]{latest_ver}[/bold])."
                )
            else:
                console.print(f"  [success]✓[/success]  All {len(results)} datasets are up to date.")
            console.print()
        return

    if check:
        # Dry-run: just report
        if output_fmt == OutputFormat.json:
            print(json.dumps({
                "would_update": [
                    {"id": ds_id, "from": cur, "to": new}
                    for ds_id, cur, new in to_update
                ]
            }))
        elif not quiet:
            console.print()
            console.print(f"  [bold]{len(to_update)}[/bold] dataset(s) would be updated:\n")
            for ds_id, cur, new in to_update:
                arrow = f"{cur} → {new}" if cur and cur != new else new
                console.print(f"    [identifier]{ds_id}[/identifier]  [muted]{arrow}[/muted]")
            console.print()
        return

    # Pull updates — import here to avoid circular imports
    from datum.commands.pull import _pull_one  # noqa: PLC0415

    updated = []
    for ds_id, cur, new in to_update:
        pull_id = f"{ds_id}:{new}"
        if not quiet and output_fmt != OutputFormat.json:
            console.print()
            if cur and cur != new:
                console.print(f"  Updating [bold]{ds_id}[/bold]: {cur} → {new}")
            else:
                console.print(f"  Pulling [bold]{ds_id}:{new}[/bold]")
        ok = _pull_one(pull_id, force=force)
        if ok == 0:
            updated.append({"id": ds_id, "from": cur, "to": new})

    if output_fmt == OutputFormat.json:
        print(json.dumps({"updated": updated}))
