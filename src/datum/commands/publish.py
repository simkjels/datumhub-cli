"""datum publish — publish dataset metadata to a registry."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer
from pydantic import ValidationError
from rich.panel import Panel
from rich.table import Table

from datum.console import console, err_console
from datum.models import DataPackage
from datum.registry.local import get_local_registry
from datum.state import OutputFormat, state


def cmd_publish(
    file: Path = typer.Argument(
        Path("datapackage.json"),
        help="Path to the datapackage.json to publish",
        show_default=True,
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Overwrite an existing version in the registry",
    ),
) -> None:
    """
    Publish dataset metadata to the local registry.

    Reads and validates a datapackage.json, then writes it to
    [bold]~/.datum/registry/[/bold]. Exits with code 0 on success,
    1 on duplicate or validation errors, and 2 on file or parse errors.

    Use [bold]--force[/bold] to overwrite an existing version.
    """
    output_fmt = state.output
    quiet = state.quiet

    if state.registry and state.registry.startswith(("http://", "https://")):
        if output_fmt == OutputFormat.json:
            _emit_json(published=False, error="Remote registry publishing is not yet implemented.")
        else:
            err_console.print(
                "\n[error]✗[/error] Remote registry publishing is not yet implemented.\n"
                "Remove [bold]--registry[/bold] to publish to the local registry."
            )
        raise typer.Exit(code=1)

    # 1. File existence
    if not file.exists():
        if output_fmt == OutputFormat.json:
            _emit_json(published=False, error=f"File not found: {file}")
        else:
            err_console.print(
                f"\n[error]✗[/error] File not found: [bold]{file}[/bold]\n\n"
                "Run [bold]datum init[/bold] to create a datapackage.json, "
                "or pass a path: [bold]datum publish path/to/datapackage.json[/bold]"
            )
        raise typer.Exit(code=2)

    # 2. JSON parse
    try:
        raw: dict[str, Any] = json.loads(file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        if output_fmt == OutputFormat.json:
            _emit_json(published=False, error=f"Invalid JSON: {exc}")
        else:
            err_console.print(
                f"\n[error]✗[/error] [bold]{file}[/bold] is not valid JSON.\n\n"
                f"  {exc}\n"
            )
        raise typer.Exit(code=2)

    # 3. Schema validation
    try:
        pkg = DataPackage.model_validate(raw)
    except ValidationError as exc:
        errors = [err["msg"] for err in exc.errors()]
        if output_fmt == OutputFormat.json:
            _emit_json(published=False, error="; ".join(errors))
        else:
            err_console.print(
                f"\n[error]✗[/error] [bold]{file}[/bold] has validation errors.\n"
                "Run [bold]datum check[/bold] for details."
            )
        raise typer.Exit(code=1)

    # 4. Publish to registry
    registry = get_local_registry()
    try:
        path = registry.publish(pkg, overwrite=force)
    except FileExistsError as exc:
        if output_fmt == OutputFormat.json:
            _emit_json(published=False, id=pkg.id, version=pkg.version, error=str(exc))
        else:
            err_console.print(
                f"\n[error]✗[/error] [bold]{pkg.id}@{pkg.version}[/bold] already exists "
                "in the registry.\n\n"
                "  Use [bold]datum publish --force[/bold] to overwrite."
            )
        raise typer.Exit(code=1)

    # 5. Success
    if output_fmt == OutputFormat.json:
        _emit_json(published=True, id=pkg.id, version=pkg.version, path=str(path))
    else:
        _print_success(pkg=pkg, path=path, registry=registry.root, quiet=quiet)


# ---------------------------------------------------------------------------
# Output renderers
# ---------------------------------------------------------------------------


def _emit_json(
    *,
    published: bool,
    id: str = "",
    version: str = "",
    path: str = "",
    error: str = "",
) -> None:
    payload: dict[str, Any] = {"published": published}
    if id:
        payload["id"] = id
    if version:
        payload["version"] = version
    if path:
        payload["path"] = path
    if error:
        payload["error"] = error
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _print_success(pkg: DataPackage, path: Path, registry: Path, quiet: bool) -> None:
    if quiet:
        return

    console.print()
    console.print(
        Panel(
            f"[success]✓ Published[/success]  [muted]·[/muted]  "
            f"[bold]{pkg.id}@{pkg.version}[/bold]",
            border_style="green",
            padding=(0, 2),
        )
    )

    table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
    table.add_column("key", style="key", min_width=10)
    table.add_column("value")
    table.add_row("Registry", str(registry))
    table.add_row("Path", str(path))

    console.print(table)
    console.print()
