"""datum pull — resolve an identifier, download data, verify file integrity."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
import typer
from rich.panel import Panel
from rich.progress import BarColumn, DownloadColumn, Progress, TextColumn, TimeRemainingColumn

from datum.commands.cache import get_cache_root
from datum.console import console, err_console
from datum.models import ID_PATTERN
from datum.registry.local import get_registry
from datum.state import OutputFormat, state
from datum.utils import parse_identifier


def get_dest_root() -> Path:
    return Path.cwd()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _verify_checksum(path: Path, checksum: str) -> bool:
    """Return True if path's content matches the expected checksum string."""
    algo, expected = checksum.split(":", 1)
    h = hashlib.new(algo)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest() == expected


def _source_filename(source: Any, i: int) -> str:
    """Derive a filename for a source file from its URL."""
    url_path = urlparse(source.url).path
    raw_name = url_path.rstrip("/").split("/")[-1] if url_path else ""
    return raw_name if raw_name else f"source_{i}.{source.format}"


# ---------------------------------------------------------------------------
# Async parallel download helpers (B5)
# ---------------------------------------------------------------------------


async def _download_file_async(
    client: httpx.AsyncClient,
    source: Any,
    cache_file: Path,
    staging_file: Path,
    semaphore: asyncio.Semaphore,
) -> Tuple[bool, str]:
    """Download *source* to *cache_file*, verify checksum, copy to *staging_file*.

    Returns (success, error_message).
    """
    async with semaphore:
        algo: Optional[str] = None
        checksum_expected: Optional[str] = None
        h: Optional[Any] = None
        if source.checksum:
            algo, checksum_expected = source.checksum.split(":", 1)
            h = hashlib.new(algo)

        try:
            async with client.stream(
                "GET",
                source.url,
                follow_redirects=True,
                timeout=httpx.Timeout(connect=10.0, read=300.0, write=None, pool=10.0),
            ) as response:
                response.raise_for_status()
                with cache_file.open("wb") as fh:
                    async for chunk in response.aiter_bytes(65536):
                        fh.write(chunk)
                        if h is not None:
                            h.update(chunk)
        except httpx.HTTPError as exc:
            return False, f"Network error downloading {source.url}: {exc}"

        if h is not None and checksum_expected is not None:
            actual = h.hexdigest()
            if actual != checksum_expected:
                cache_file.unlink(missing_ok=True)
                return False, (
                    f"Integrity check failed for {cache_file.name} — "
                    f"the file may be corrupted or tampered with. Try pulling again."
                )

        shutil.copy2(cache_file, staging_file)
        return True, ""


async def _pull_sources_parallel(
    sources_info: List[Tuple[Any, int, str]],  # (source, i, filename)
    cache_dir: Path,
    staging_dir: Path,
    parallel: int,
) -> List[Tuple[bool, str, str]]:
    """Download sources in parallel. Returns list of (success, error, filename)."""
    semaphore = asyncio.Semaphore(parallel)
    async with httpx.AsyncClient() as client:
        tasks = []
        filenames: List[str] = []
        for source, _i, filename in sources_info:
            cache_file = cache_dir / filename
            staging_file = staging_dir / filename
            tasks.append(
                _download_file_async(client, source, cache_file, staging_file, semaphore)
            )
            filenames.append(filename)
        results = await asyncio.gather(*tasks)
    return [(success, error, fn) for (success, error), fn in zip(results, filenames)]


# ---------------------------------------------------------------------------
# Core download logic
# ---------------------------------------------------------------------------


def _pull_one(
    identifier: str,
    force: bool = False,
    dest: Optional[Path] = None,
    parallel: int = 1,
) -> int:
    """Pull a single dataset. Returns exit code: 0 success, 1 user error, 2 network error.

    Prints errors to err_console but does not raise typer.Exit —
    callers (cmd_pull, cmd_update) decide how to handle failure.
    """
    output_fmt = state.output
    quiet = state.quiet

    # 1. Parse identifier
    id_part, version = parse_identifier(identifier)
    version = version or "latest"

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
        return 1

    # 3. Resolve package from registry
    registry = get_registry()

    if state.verbose:
        reg_label = state.registry or "~/.datum/registry (local)"
        err_console.print(f"  [muted]registry: {reg_label}[/muted]")

    try:
        pkg = registry.latest(id_part) if version == "latest" else registry.get(id_part, version)
    except RuntimeError as exc:
        if output_fmt == OutputFormat.json:
            _emit_json(downloaded=False, error=str(exc))
        else:
            err_console.print(f"\n[error]✗[/error] {exc}\n")
        return 2

    if pkg is None:
        label = f"{id_part}:{version}"
        if output_fmt == OutputFormat.json:
            _emit_json(downloaded=False, error=f"Not found: {label}")
        else:
            msg = f"\n[error]✗[/error] Dataset [bold]{label}[/bold] not found in the registry.\n"
            suggestions = registry.suggest(id_part)
            if suggestions:
                msg += "\n  Did you mean?\n" + "".join(f"    {s}\n" for s in suggestions)
            else:
                msg += "\n  Use [bold]datum publish[/bold] to add it first."
            err_console.print(msg)
        return 1

    # 4. Set up directories
    pub, ns, ds = pkg.id.split("/")
    cache_dir = get_cache_root() / pub / ns / ds / pkg.version
    cache_dir.mkdir(parents=True, exist_ok=True)

    # dest_dir: explicit --dest overrides default ./<dataset-slug>
    if dest is not None:
        dest_dir = dest.resolve()
    else:
        dest_dir = get_dest_root() / pkg.dataset_slug

    # 5. Download each source via staging directory for atomicity
    downloaded_files: List[Path] = []
    show_progress = not quiet and output_fmt != OutputFormat.json

    with tempfile.TemporaryDirectory(prefix=".staging-", dir=cache_dir.parent) as tmp_str:
        staging = Path(tmp_str)

        if parallel > 1:
            # Parallel path: handle tier 1/2 synchronously, tier 3 in parallel
            sources_to_download: List[Tuple[Any, int, str]] = []

            for i, source in enumerate(pkg.sources):
                filename = _source_filename(source, i)
                dest_file = dest_dir / filename
                cache_file = cache_dir / filename

                # Tier 1: already in dest
                if dest_file.exists() and not force:
                    downloaded_files.append(dest_file)
                    continue

                # Tier 2: in cache (valid)
                if cache_file.exists() and not force:
                    if not source.checksum or _verify_checksum(cache_file, source.checksum):
                        shutil.copy2(cache_file, staging / filename)
                        continue
                    cache_file.unlink(missing_ok=True)

                sources_to_download.append((source, i, filename))

            if sources_to_download:
                results = asyncio.run(
                    _pull_sources_parallel(sources_to_download, cache_dir, staging, parallel)
                )
                for success, error, _fn in results:
                    if not success:
                        if output_fmt == OutputFormat.json:
                            _emit_json(
                                downloaded=False, id=pkg.id, version=pkg.version, error=error
                            )
                        else:
                            err_console.print(f"\n[error]✗[/error] {error}\n")
                        return 2
        else:
            # Serial path with staging
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TimeRemainingColumn(),
                console=console,
                disable=not show_progress,
            ) as progress:
                for i, source in enumerate(pkg.sources):
                    filename = _source_filename(source, i)
                    dest_file = dest_dir / filename
                    cache_file = cache_dir / filename
                    staging_file = staging / filename

                    # Tier 1: already in dest — skip
                    if dest_file.exists() and not force:
                        if show_progress:
                            console.print(f"  [muted]skipped[/muted]  {filename}")
                        downloaded_files.append(dest_file)
                        continue

                    # Tier 2: in cache — verify integrity if checksum present, then stage
                    if cache_file.exists() and not force:
                        if source.checksum and not _verify_checksum(cache_file, source.checksum):
                            cache_file.unlink(missing_ok=True)
                            if show_progress:
                                console.print(
                                    f"  [warning]⚠[/warning]  integrity check failed, re-downloading {filename}"
                                )
                        else:
                            shutil.copy2(cache_file, staging_file)
                            if show_progress:
                                console.print(f"  [muted]cached[/muted]  {filename}")
                            continue

                    # Tier 3: download from URL
                    task_id = progress.add_task(filename, total=source.size)

                    algo: Optional[str] = None
                    checksum_expected: Optional[str] = None
                    h_obj: Optional[Any] = None
                    if source.checksum:
                        algo, checksum_expected = source.checksum.split(":", 1)
                        h_obj = hashlib.new(algo)

                    try:
                        with httpx.stream(
                            "GET",
                            source.url,
                            follow_redirects=True,
                            timeout=httpx.Timeout(
                                connect=10.0, read=300.0, write=None, pool=10.0
                            ),
                        ) as response:
                            response.raise_for_status()
                            with cache_file.open("wb") as fh:
                                for chunk in response.iter_bytes(chunk_size=65536):
                                    fh.write(chunk)
                                    if h_obj is not None:
                                        h_obj.update(chunk)
                                    progress.update(task_id, advance=len(chunk))
                    except httpx.HTTPError as exc:
                        if output_fmt == OutputFormat.json:
                            _emit_json(
                                downloaded=False, id=pkg.id, version=pkg.version, error=str(exc)
                            )
                        else:
                            err_console.print(
                                f"\n[error]✗[/error] Network error downloading "
                                f"[bold]{source.url}[/bold]:\n  {exc}\n"
                            )
                        return 2  # staging auto-cleans on exit

                    # Verify checksum
                    if h_obj is not None and checksum_expected is not None:
                        actual = h_obj.hexdigest()
                        if actual != checksum_expected:
                            cache_file.unlink(missing_ok=True)
                            if output_fmt == OutputFormat.json:
                                _emit_json(
                                    downloaded=False,
                                    id=pkg.id,
                                    version=pkg.version,
                                    error=(
                                        f"Integrity check failed for {filename}: "
                                        f"expected {checksum_expected}, got {actual}"
                                    ),
                                )
                            else:
                                err_console.print(
                                    f"\n[error]✗[/error] Integrity check failed for "
                                    f"[bold]{filename}[/bold] — the file may be corrupted or tampered with.\n"
                                    f"  Try pulling again. If the problem persists, contact the dataset publisher.\n"
                                )
                            return 1  # staging auto-cleans on exit

                        if state.verbose:
                            err_console.print(
                                f"  [muted]verified  ✓  {filename}[/muted]"
                            )

                    shutil.copy2(cache_file, staging_file)

        # Atomically move all staged files to dest
        staged = list(staging.iterdir())
        if staged:
            dest_dir.mkdir(parents=True, exist_ok=True)
            for f in staged:
                dst = dest_dir / f.name
                shutil.move(str(f), dst)
                downloaded_files.append(dst)

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

    return 0


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------


def cmd_pull(
    identifiers: List[str] = typer.Argument(
        ..., help="One or more dataset identifiers (publisher/namespace/dataset[:version])"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Re-download even if already cached"
    ),
    dest: Optional[Path] = typer.Option(
        None,
        "--dest",
        "-d",
        help=(
            "Destination directory. Defaults to ./<dataset-slug>. "
            "Pass '.' to download into the current directory."
        ),
    ),
    parallel: int = typer.Option(
        1,
        "--parallel",
        "-p",
        min=1,
        max=8,
        help="Number of parallel downloads per dataset (1 = serial, default)",
    ),
) -> None:
    """
    Download one or more datasets by identifier, verify file integrity, and cache locally.

    IDENTIFIER format: [bold]publisher/namespace/dataset[:version][/bold]

    Omit :version to resolve the latest published version.

    Exit codes: 0 success · 1 not found / bad identifier / integrity fail · 2 network error
    """
    results: Dict[str, int] = {}
    for ident in identifiers:
        results[ident] = _pull_one(ident, force=force, dest=dest, parallel=parallel)

    if state.output == OutputFormat.json and len(identifiers) > 1:
        # Individual _pull_one calls already emitted per-dataset JSON.
        # For multi-pull, emit a summary.
        print(json.dumps(
            {"results": {k: {"downloaded": v == 0} for k, v in results.items()}},
            indent=2,
        ))

    failed = [k for k, v in results.items() if v != 0]
    if failed:
        raise typer.Exit(code=max(results[k] for k in failed))


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
