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

from datum.console import console, err_console
from datum.models import COMMON_FORMATS
from datum.state import OutputFormat, state

_DATA_EXTENSIONS: set = COMMON_FORMATS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
    fmt: Optional[str] = typer.Option(
        None, "--format", help="Override detected file format (e.g. csv, parquet)"
    ),
    no_checksum: bool = typer.Option(
        False, "--no-checksum", help="Skip checksum computation — faster, but less safe"
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

    Each file is streamed to automatically compute its SHA-256 checksum
    and byte size — no manual work required.

    [bold]Examples:[/bold]

      datum add https://example.com/data.csv
      datum add https://a.com/a.csv https://b.com/b.csv
      datum add --no-checksum https://example.com/large.parquet
      datum add --crawl https://example.com/datasets/
      datum add --crawl --filter '*.csv' https://s3.amazonaws.com/my-bucket/

    Run from the directory containing your datapackage.json, or use
    [bold]--file[/bold] to point to a specific one.
    """
    output_fmt = state.output
    quiet = state.quiet

    # Find datapackage.json
    pkg_path = file or _find_datapackage()
    if pkg_path is None:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"added": 0, "error": "No datapackage.json found. Run 'datum init' first."}))
        else:
            err_console.print(
                "\n[error]✗[/error] No [bold]datapackage.json[/bold] found in this directory or any parent.\n"
                "  Run [bold]datum init[/bold] to create one.\n"
            )
        raise typer.Exit(code=1)

    # Load existing package data
    try:
        pkg_data = json.loads(pkg_path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        err_console.print(f"\n[error]✗[/error] Could not read {pkg_path}: {exc}\n")
        raise typer.Exit(code=2)

    existing_sources: list = pkg_data.get("sources", [])
    existing_urls = {s.get("url") for s in existing_sources}

    # Resolve target URLs
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
    new_urls = [u for u in target_urls if u not in existing_urls]
    skipped = len(target_urls) - len(new_urls)

    if not new_urls:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"added": 0, "skipped": skipped, "message": "All URLs already present."}))
        elif not quiet:
            console.print("\n  [muted]All URLs are already in sources — nothing to add.[/muted]\n")
        return

    # Fetch each URL
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
                # HEAD-only: just grab file size if available
                try:
                    head = httpx.head(url, follow_redirects=True, timeout=15)
                    if cl := head.headers.get("content-length"):
                        source["size"] = int(cl)
                except httpx.HTTPError:
                    pass
                added_sources.append(source)
                if show_progress:
                    console.print(f"  [success]✓[/success] {filename}  [muted](no checksum)[/muted]")
                continue

            # Stream to compute sha256 + size
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

    # Write updated datapackage.json
    pkg_data["sources"] = existing_sources + added_sources
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
