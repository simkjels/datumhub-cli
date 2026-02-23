"""datum add — append source URLs to a datapackage.json."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import httpx
import typer
from rich.progress import BarColumn, DownloadColumn, Progress, TextColumn, TimeRemainingColumn

from pydantic import ValidationError

from datum.console import console, err_console
from datum.models import COMMON_FORMATS, ID_PATTERN, DataPackage
from datum.state import OutputFormat, state

_DATA_EXTENSIONS: set = COMMON_FORMATS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_pkg_data(pkg_data: dict, output_fmt, quiet: bool) -> bool:
    """Validate pkg_data against the DataPackage schema.

    Prints errors and returns False if invalid; returns True if valid.
    Does not raise or exit — caller decides what to do.
    """
    try:
        DataPackage.model_validate(pkg_data)
        return True
    except ValidationError as exc:
        errors = [
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}"
            for e in exc.errors()
        ]
        if output_fmt == OutputFormat.json:
            print(json.dumps({"added": 0, "error": "; ".join(errors)}))
        else:
            err_console.print("\n[error]✗[/error] Package would be invalid after these changes:\n")
            for e in errors:
                err_console.print(f"  [error]•[/error] {e}")
            err_console.print()
        return False


def _find_datapackage(start: Optional[Path] = None) -> Optional[Path]:
    """Walk up from start (default CWD) to find the nearest datapackage.json."""
    p = (start or Path.cwd()).resolve()
    while True:
        candidate = p / "datapackage.json"
        if candidate.exists():
            return candidate
        if p.parent == p:
            return None
        p = p.parent


def _detect_format(url: str) -> str:
    """Guess file format from the URL path extension."""
    path = urlparse(url).path
    ext = os.path.splitext(path)[1].lstrip(".").lower()
    return ext if ext else "unknown"


def _crawl_urls(base_url: str, pattern: Optional[str]) -> List[str]:
    """Fetch base_url and return a list of data file URLs found in the listing."""
    resp = httpx.get(base_url, follow_redirects=True, timeout=15)
    resp.raise_for_status()
    content_type = resp.headers.get("content-type", "")

    if "xml" in content_type or "<ListBucketResult" in resp.text[:2000]:
        # S3-style XML listing — extract <Key> elements
        keys = re.findall(r"<Key>([^<]+)</Key>", resp.text)
        parsed = urlparse(base_url)
        bucket_base = f"{parsed.scheme}://{parsed.netloc}/"
        candidates = [(k.split("/")[-1], urljoin(bucket_base, k)) for k in keys]
    else:
        # HTML directory listing — extract href attributes
        hrefs = re.findall(r'href=["\']([^"\'?#]+)["\']', resp.text, re.IGNORECASE)
        candidates = []
        for href in hrefs:
            if href.startswith(("/", "http", "?")):
                continue
            if href.endswith("/"):
                continue
            full_url = urljoin(base_url.rstrip("/") + "/", href)
            candidates.append((href, full_url))

    results = []
    for name, url in candidates:
        ext = os.path.splitext(name)[1].lstrip(".").lower()
        if ext not in _DATA_EXTENSIONS:
            continue
        if pattern and not fnmatch.fnmatch(name, pattern):
            continue
        results.append(url)
    return results


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


def cmd_add(
    urls: List[str] = typer.Argument(..., help="URL(s) to add as data sources"),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Path to datapackage.json (defaults to nearest one found)"
    ),
    # Metadata flags — used when creating a new datapackage.json, or to update an existing one
    id_: Optional[str] = typer.Option(
        None, "--id", help="Package identifier: publisher/namespace/dataset"
    ),
    title: Optional[str] = typer.Option(None, "--title", help="Human-readable dataset title"),
    publisher: Optional[str] = typer.Option(None, "--publisher", help="Publisher name (e.g. 'Statistics Norway')"),
    version: Optional[str] = typer.Option(None, "--version", help="Dataset version (e.g. 2024-01, 1.0.0)"),
    description: Optional[str] = typer.Option(None, "--description", help="Short description of the dataset"),
    license_: Optional[str] = typer.Option(None, "--license", help="License identifier (e.g. CC-BY-4.0)"),
    tags: Optional[str] = typer.Option(
        None, "--tags", help="Comma-separated tags (e.g. 'weather,norway,oslo')"
    ),
    # Source flags
    fmt: Optional[str] = typer.Option(
        None, "--format", help="Override detected file format (e.g. csv, parquet)"
    ),
    no_checksum: bool = typer.Option(
        False,
        "--no-verify",
        "--no-checksum",  # deprecated alias, kept for backward compatibility
        help="Skip integrity verification (faster, but the file will not be checked)",
    ),
    crawl: bool = typer.Option(
        False, "--crawl", help="Treat the URL as a directory index and discover all data files"
    ),
    filter_: Optional[str] = typer.Option(
        None, "--filter", metavar="GLOB", help="Filter crawled files by glob pattern (e.g. '*.csv')"
    ),
) -> None:
    """
    Add one or more source URLs to a datapackage.json.

    Each file is streamed to automatically verify its integrity
    and record the byte size — no manual work required.

    When no datapackage.json exists yet, pass [bold]--id[/bold], [bold]--title[/bold],
    [bold]--publisher[/bold], and [bold]--version[/bold] to create one on the fly:

      datum add \\
        --id simkjels/samples/oslo-weather \\
        --title "Oslo Weather Observations" \\
        --publisher "Met Norway" \\
        --version 2024-01 \\
        https://met.no/oslo-2024.csv

    If a datapackage.json already exists, any metadata flags you pass
    will update the corresponding fields in the file.

    [bold]Other examples:[/bold]

      datum add https://example.com/data.csv
      datum add https://a.com/a.csv https://b.com/b.csv
      datum add --no-verify https://example.com/large.parquet
      datum add --crawl https://example.com/datasets/
      datum add --crawl --filter '*.csv' https://s3.amazonaws.com/my-bucket/
    """
    output_fmt = state.output
    quiet = state.quiet

    metadata_flags = {
        k: v for k, v in {
            "id": id_,
            "title": title,
            "version": version,
            "description": description,
            "license": license_,
            "tags": [t.strip() for t in tags.split(",") if t.strip()] if tags else None,
        }.items()
        if v is not None
    }
    if publisher is not None:
        metadata_flags["publisher"] = {"name": publisher}

    # Validate --id format early, before any network I/O
    if id_ is not None and not ID_PATTERN.match(id_):
        if output_fmt == OutputFormat.json:
            print(json.dumps({"added": 0, "error": f"Invalid --id: {id_!r}"}))
        else:
            err_console.print(
                f"\n[error]✗[/error] Invalid --id: [bold]{id_}[/bold]\n\n"
                "  Expected [bold]publisher/namespace/dataset[/bold] "
                "(e.g. simkjels/samples/demo)\n"
            )
        raise typer.Exit(code=1)

    # ------------------------------------------------------------------
    # Locate or create datapackage.json
    # ------------------------------------------------------------------
    pkg_path = file or _find_datapackage()

    if pkg_path is None:
        # No existing file — need enough metadata to create one
        missing = [f for f in ("id", "title", "version") if f not in metadata_flags] + (
            ["publisher"] if publisher is None else []
        )
        if missing:
            if output_fmt == OutputFormat.json:
                print(json.dumps({
                    "added": 0,
                    "error": (
                        f"No datapackage.json found. Pass {', '.join('--' + m for m in missing)} "
                        "to create one, or run 'datum init' for the interactive wizard."
                    ),
                }))
            else:
                err_console.print(
                    "\n[error]✗[/error] No [bold]datapackage.json[/bold] found.\n\n"
                    "  Pass the following flags to create one:\n"
                    + "".join(f"    [bold]--{m}[/bold]\n" for m in missing)
                    + "\n  Or run [bold]datum init[/bold] for the interactive wizard.\n"
                )
            raise typer.Exit(code=1)

        pkg_path = (file or Path.cwd()) / "datapackage.json"
        pkg_data: dict = {}
        if not quiet and output_fmt != OutputFormat.json:
            console.print(f"\n  Creating [bold]{pkg_path.name}[/bold]\n")
    else:
        # Load existing file
        try:
            pkg_data = json.loads(pkg_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            err_console.print(f"\n[error]✗[/error] Could not read {pkg_path}: {exc}\n")
            raise typer.Exit(code=2)

        # Warn if --id changes the existing id
        if id_ is not None and pkg_data.get("id") and pkg_data["id"] != id_:
            if output_fmt != OutputFormat.json:
                console.print(
                    f"\n  [warning]⚠[/warning]  Changing package ID: "
                    f"[bold]{pkg_data['id']}[/bold] → [bold]{id_}[/bold]\n"
                )

    # Apply metadata flags to pkg_data
    pkg_data.update(metadata_flags)

    # ------------------------------------------------------------------
    # Resolve target URLs
    # ------------------------------------------------------------------
    if crawl:
        if len(urls) != 1:
            err_console.print("\n[error]✗[/error] --crawl requires exactly one URL.\n")
            raise typer.Exit(code=1)
        base_url = urls[0]
        if not quiet and output_fmt != OutputFormat.json:
            console.print(f"\n  Crawling [bold]{base_url}[/bold] ...\n")
        try:
            target_urls = _crawl_urls(base_url, filter_)
        except httpx.HTTPError as exc:
            err_console.print(f"\n[error]✗[/error] Could not reach {base_url}: {exc}\n")
            raise typer.Exit(code=2)
        if not target_urls:
            if output_fmt == OutputFormat.json:
                print(json.dumps({"added": 0, "error": "No data files found at that URL."}))
            else:
                err_console.print(
                    "\n[warning]No data files found.[/warning] "
                    "Check the URL or use [bold]--filter '*.csv'[/bold] to narrow results.\n"
                )
            raise typer.Exit(code=1)
        if not quiet and output_fmt != OutputFormat.json:
            console.print(f"  Found [bold]{len(target_urls)}[/bold] file(s)\n")
    else:
        target_urls = list(urls)

    # Deduplicate against existing sources
    existing_sources: list = pkg_data.get("sources", [])
    existing_urls = {s.get("url") for s in existing_sources}
    new_urls = [u for u in target_urls if u not in existing_urls]
    skipped = len(target_urls) - len(new_urls)

    if not new_urls:
        # Still write any metadata updates even if no new sources
        if not _validate_pkg_data(pkg_data, output_fmt, quiet):
            raise typer.Exit(code=1)
        pkg_path.write_text(json.dumps(pkg_data, indent=2, ensure_ascii=False) + "\n")
        if output_fmt == OutputFormat.json:
            print(json.dumps({"added": 0, "skipped": skipped, "message": "All URLs already present."}))
        elif not quiet:
            console.print("\n  [muted]All URLs are already in sources — nothing to add.[/muted]\n")
        return

    # ------------------------------------------------------------------
    # Fetch / checksum each URL
    # ------------------------------------------------------------------
    added_sources: List[dict] = []
    failed: List[str] = []
    show_progress = not quiet and output_fmt != OutputFormat.json

    with Progress(
        TextColumn("  [cyan]{task.description}[/cyan]"),
        BarColumn(),
        DownloadColumn(),
        TimeRemainingColumn(),
        console=console,
        disable=not show_progress,
    ) as progress:
        for url in new_urls:
            filename = urlparse(url).path.rstrip("/").split("/")[-1] or "file"
            source: dict = {"url": url, "format": fmt.lower().strip() if fmt else _detect_format(url)}

            if no_checksum:
                try:
                    head = httpx.head(url, follow_redirects=True, timeout=15)
                    if cl := head.headers.get("content-length"):
                        source["size"] = int(cl)
                except httpx.HTTPError:
                    pass
                added_sources.append(source)
                if show_progress:
                    console.print(f"  [success]✓[/success] {filename}  [muted](integrity check skipped)[/muted]")
                continue

            task = progress.add_task(filename, total=None)
            hasher = hashlib.sha256()
            size = 0
            try:
                with httpx.stream("GET", url, follow_redirects=True, timeout=60) as resp:
                    resp.raise_for_status()
                    if cl := resp.headers.get("content-length"):
                        progress.update(task, total=int(cl))
                        source["size"] = int(cl)
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        hasher.update(chunk)
                        size += len(chunk)
                        progress.update(task, advance=len(chunk))
                source["checksum"] = f"sha256:{hasher.hexdigest()}"
                if "size" not in source:
                    source["size"] = size
                added_sources.append(source)
            except httpx.HTTPStatusError as exc:
                err_console.print(f"\n  [error]✗[/error] HTTP {exc.response.status_code}: {url}")
                failed.append(url)
                progress.remove_task(task)
            except httpx.HTTPError as exc:
                err_console.print(f"\n  [error]✗[/error] {exc}: {url}")
                failed.append(url)
                progress.remove_task(task)

    if not added_sources:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"added": 0, "failed": failed}))
        raise typer.Exit(code=2)

    # ------------------------------------------------------------------
    # Write updated datapackage.json
    # ------------------------------------------------------------------
    pkg_data["sources"] = existing_sources + added_sources
    if not _validate_pkg_data(pkg_data, output_fmt, quiet):
        raise typer.Exit(code=1)
    pkg_path.write_text(json.dumps(pkg_data, indent=2, ensure_ascii=False) + "\n")

    if output_fmt == OutputFormat.json:
        print(json.dumps({
            "added": len(added_sources),
            "skipped": skipped,
            "failed": len(failed),
            "sources": added_sources,
        }, indent=2))
    elif not quiet:
        console.print()
        console.print(
            f"  [success]✓[/success]  Added [bold]{len(added_sources)}[/bold] source(s) to "
            f"[bold]{pkg_path.name}[/bold]"
            + (f"  [muted]({skipped} already present)[/muted]" if skipped else "")
        )
        if failed:
            console.print(f"  [warning]⚠[/warning]  {len(failed)} URL(s) failed — check errors above")
        console.print()
