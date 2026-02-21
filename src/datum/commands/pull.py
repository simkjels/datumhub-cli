"""datum pull — resolve an identifier, download data, verify checksum."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import urlparse

import httpx
import typer
from rich.panel import Panel
from rich.progress import BarColumn, DownloadColumn, Progress, TextColumn, TimeRemainingColumn
from rich.table import Table

from datum.commands.cache import get_cache_root
from datum.console import console, err_console
from datum.models import ID_PATTERN
from datum.registry.local import get_registry
from datum.state import OutputFormat, state


def get_dest_root() -> Path:
    return Path.cwd()


def cmd_pull(
    identifier: str = typer.Argument(
        ..., help="Dataset identifier (publisher.namespace.dataset:version)"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Re-download even if already cached"
    ),
) -> None:
    """
    Download a dataset by identifier, verify checksums, and cache locally.

    IDENTIFIER format: [bold]publisher.namespace.dataset:version[/bold]

    Omit :version to resolve the latest published version.

    Exit codes: 0 success · 1 not found / bad identifier / checksum fail · 2 network error
    """
    output_fmt = state.output
    quiet = state.quiet

    # 1. Parse identifier
    if ":" in identifier:
        id_part, version = identifier.split(":", 1)
    else:
        id_part = identifier
        version = "latest"

    # 2. Validate id format
    if not ID_PATTERN.match(id_part):
        if output_fmt == OutputFormat.json:
            _emit_json(downloaded=False, error=f"Invalid identifier format: {id_part!r}")
        else:
            err_console.print(
                f"\n[error]✗[/error] Invalid identifier: [bold]{id_part}[/bold]\n\n"
                "  Expected [bold]publisher/namespace/dataset[/bold] "
                "(slash-separated — publisher may contain dots, "
                "e.g. norge.no/population/census or simkjels/samples/demo)"
            )
        raise typer.Exit(code=1)

    # 3. Resolve package from registry
    registry = get_registry()
    try:
        if version == "latest":
            pkg = registry.latest(id_part)
        else:
            pkg = registry.get(id_part, version)
    except RuntimeError as exc:
        if output_fmt == OutputFormat.json:
            _emit_json(downloaded=False, error=str(exc))
        else:
            err_console.print(f"\n[error]✗[/error] {exc}\n")
        raise typer.Exit(code=2)

    if pkg is None:
        label = f"{id_part}:{version}"
        if output_fmt == OutputFormat.json:
            _emit_json(downloaded=False, error=f"Not found: {label}")
        else:
            msg = (
                f"\n[error]✗[/error] Dataset [bold]{label}[/bold] not found in the registry.\n"
            )
            suggestions = registry.suggest(id_part)
            if suggestions:
                msg += "\n  Did you mean?\n" + "".join(f"    {s}\n" for s in suggestions)
            else:
                msg += "\n  Use [bold]datum publish[/bold] to add it first."
            err_console.print(msg)
        raise typer.Exit(code=1)

    # 4. Dirs
    pub, ns, ds = pkg.id.split("/")
    cache_dir = get_cache_root() / pub / ns / ds / pkg.version
    cache_dir.mkdir(parents=True, exist_ok=True)

    dest_dir = get_dest_root() / pkg.dataset_slug
    dest_dir.mkdir(parents=True, exist_ok=True)

    # 5. Download each source
    downloaded_files: List[Path] = []
    show_progress = not quiet and output_fmt != OutputFormat.json

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TimeRemainingColumn(),
        console=console,
        disable=not show_progress,
    ) as progress:
        for i, source in enumerate(pkg.sources):
            # Determine filename from URL path
            url_path = urlparse(source.url).path
            raw_name = url_path.rstrip("/").split("/")[-1] if url_path else ""
            filename = raw_name if raw_name else f"source_{i}.{source.format}"

            dest_file = dest_dir / filename
            cache_file = cache_dir / filename

            # Already in CWD — skip
            if dest_file.exists() and not force:
                if show_progress:
                    console.print(f"  [muted]skipped[/muted]  {filename}")
                downloaded_files.append(dest_file)
                continue

            # In cache but not CWD — copy, no HTTP
            if cache_file.exists() and not force:
                shutil.copy2(cache_file, dest_file)
                if show_progress:
                    console.print(f"  [muted]cached[/muted]  {filename}")
                downloaded_files.append(dest_file)
                continue

            task_id = progress.add_task(filename, total=source.size)

            # Prepare checksum state
            algo: Optional[str] = None
            checksum_expected: Optional[str] = None
            h: Optional[Any] = None
            if source.checksum:
                algo, checksum_expected = source.checksum.split(":", 1)
                h = hashlib.new(algo)

            # Download with streaming → write to cache, then copy to dest
            try:
                with httpx.stream("GET", source.url, follow_redirects=True) as response:
                    response.raise_for_status()
                    with cache_file.open("wb") as fh:
                        for chunk in response.iter_bytes(chunk_size=65536):
                            fh.write(chunk)
                            if h is not None:
                                h.update(chunk)
                            progress.update(task_id, advance=len(chunk))
            except httpx.HTTPError as exc:
                if output_fmt == OutputFormat.json:
                    _emit_json(
                        downloaded=False,
                        id=pkg.id,
                        version=pkg.version,
                        error=str(exc),
                    )
                else:
                    err_console.print(
                        f"\n[error]✗[/error] Network error downloading "
                        f"[bold]{source.url}[/bold]:\n  {exc}\n"
                    )
                raise typer.Exit(code=2)

            # Verify checksum
            if h is not None and checksum_expected is not None:
                actual = h.hexdigest()
                if actual != checksum_expected:
                    cache_file.unlink(missing_ok=True)
                    if output_fmt == OutputFormat.json:
                        _emit_json(
                            downloaded=False,
                            id=pkg.id,
                            version=pkg.version,
                            error=(
                                f"Checksum mismatch for {filename}: "
                                f"expected {checksum_expected}, got {actual}"
                            ),
                        )
                    else:
                        err_console.print(
                            f"\n[error]✗[/error] Checksum mismatch for [bold]{filename}[/bold].\n\n"
                            f"  Expected:  {checksum_expected}\n"
                            f"  Got:       {actual}\n"
                        )
                    raise typer.Exit(code=1)

            shutil.copy2(cache_file, dest_file)
            downloaded_files.append(dest_file)

    # 6. Success output
    if output_fmt == OutputFormat.json:
        _emit_json(
            downloaded=True,
            id=pkg.id,
            version=pkg.version,
            files=[str(f) for f in downloaded_files],
        )
    elif not quiet:
        _print_success(pkg=pkg, dest_dir=dest_dir, files=downloaded_files)


# ---------------------------------------------------------------------------
# Output renderers
# ---------------------------------------------------------------------------


def _emit_json(
    *,
    downloaded: bool,
    id: str = "",
    version: str = "",
    files: Optional[List[str]] = None,
    error: str = "",
) -> None:
    payload: dict[str, Any] = {"downloaded": downloaded}
    if id:
        payload["id"] = id
    if version:
        payload["version"] = version
    if files is not None:
        payload["files"] = files
    if error:
        payload["error"] = error
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _print_success(pkg: Any, dest_dir: Path, files: List[Path]) -> None:
    console.print()
    console.print(
        Panel(
            f"[success]✓ Downloaded[/success]  [muted]·[/muted]  "
            f"[bold]{pkg.id}@{pkg.version}[/bold]",
            border_style="green",
            padding=(0, 2),
        )
    )
    for f in files:
        console.print(f"  {f}")
    console.print()
