"""datum check — validate a datapackage.json file."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import typer
from pydantic import ValidationError
from rich.panel import Panel
from rich.table import Table

from datum.console import console, err_console
from datum.models import DataPackage
from datum.state import OutputFormat, state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pydantic_errors(exc: ValidationError) -> list[dict[str, str]]:
    """Flatten Pydantic v2 errors into simple field/message dicts."""
    out: list[dict[str, str]] = []
    for err in exc.errors():
        loc_parts = []
        for part in err["loc"]:
            if isinstance(part, int):
                loc_parts.append(f"[{part}]")
            else:
                loc_parts.append(str(part))
        field = ".".join(loc_parts).replace(".[", "[")
        out.append({"field": field, "message": err["msg"]})
    return out


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


def cmd_check(
    file: Path = typer.Argument(
        Path("datapackage.json"),
        help="Path to the datapackage.json to validate",
        show_default=True,
    ),
) -> None:
    """
    Validate a datapackage.json file.

    Checks that the file exists, is valid JSON, and conforms to the
    Datum datapackage schema. Exits with code 0 on success, 1 on
    validation errors, and 2 on file or parse errors.

    Use [bold]--output json[/bold] for machine-readable output:

        datum check --output json | jq .
    """
    output_fmt = state.output
    quiet = state.quiet

    # -----------------------------------------------------------------------
    # 1. File existence
    # -----------------------------------------------------------------------
    if not file.exists():
        if output_fmt == OutputFormat.json:
            _emit_json(
                valid=False,
                file=str(file),
                errors=[{"field": "file", "message": f"File not found: {file}"}],
            )
        else:
            err_console.print(
                f"\n[error]✗[/error] File not found: [bold]{file}[/bold]\n\n"
                "Run [bold]datum init[/bold] to create a datapackage.json, "
                "or pass a path: [bold]datum check path/to/datapackage.json[/bold]"
            )
        raise typer.Exit(code=2)

    # -----------------------------------------------------------------------
    # 2. JSON parse
    # -----------------------------------------------------------------------
    try:
        raw: dict[str, Any] = json.loads(file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        if output_fmt == OutputFormat.json:
            _emit_json(
                valid=False,
                file=str(file),
                errors=[{"field": "json", "message": f"Invalid JSON: {exc}"}],
            )
        else:
            err_console.print(
                f"\n[error]✗[/error] [bold]{file}[/bold] is not valid JSON.\n\n"
                f"  {exc}\n"
            )
        raise typer.Exit(code=2)

    # -----------------------------------------------------------------------
    # 3. Schema validation
    # -----------------------------------------------------------------------
    try:
        pkg = DataPackage.model_validate(raw)
    except ValidationError as exc:
        errors = _pydantic_errors(exc)

        if output_fmt == OutputFormat.json:
            _emit_json(valid=False, file=str(file), errors=errors)
        else:
            _print_failure(file=file, errors=errors, quiet=quiet)

        raise typer.Exit(code=1)

    # -----------------------------------------------------------------------
    # 4. Success
    # -----------------------------------------------------------------------
    if output_fmt == OutputFormat.json:
        _emit_json(valid=True, file=str(file), errors=[], package=pkg.to_dict())
    else:
        _print_success(file=file, pkg=pkg, quiet=quiet)


# ---------------------------------------------------------------------------
# Output renderers
# ---------------------------------------------------------------------------


def _emit_json(
    *,
    valid: bool,
    file: str,
    errors: list[dict[str, str]],
    package: dict | None = None,
) -> None:
    payload: dict[str, Any] = {
        "valid": valid,
        "file": file,
        "errors": errors,
    }
    if package is not None:
        payload["package"] = package
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _print_success(file: Path, pkg: DataPackage, quiet: bool) -> None:
    if quiet:
        return

    console.print()
    console.print(
        Panel(
            f"[success]✓ Valid[/success]  [muted]·[/muted]  [bold]{file}[/bold]",
            border_style="green",
            padding=(0, 2),
        )
    )

    # Summary table
    table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
    table.add_column("key", style="key", min_width=14)
    table.add_column("value")

    table.add_row("Identifier", f"[identifier]{pkg.id}[/identifier]")
    table.add_row("Version", pkg.version)
    table.add_row("Title", pkg.title)
    table.add_row("Publisher", pkg.publisher.name)
    table.add_row(
        "Sources",
        f"{len(pkg.sources)} file(s)  "
        + "  ".join(
            f"[muted]{s.format.upper()}[/muted]" for s in pkg.sources
        ),
    )
    if pkg.license:
        table.add_row("License", pkg.license)
    if pkg.tags:
        table.add_row("Tags", ", ".join(pkg.tags))

    # Checksum coverage
    sources_with_checksum = sum(1 for s in pkg.sources if s.checksum)
    if sources_with_checksum == 0:
        table.add_row(
            "File integrity",
            "[warning]Not verified — run [bold]datum add[/bold] to enable integrity checks for your sources[/warning]",
        )
    elif sources_with_checksum < len(pkg.sources):
        missing = len(pkg.sources) - sources_with_checksum
        table.add_row(
            "File integrity",
            f"[warning]{missing} source(s) not verified — run [bold]datum add[/bold] to fix[/warning]",
        )
    else:
        table.add_row("File integrity", "[success]✓ All sources verified[/success]")

    console.print(table)
    console.print()


def _print_failure(file: Path, errors: list[dict[str, str]], quiet: bool) -> None:
    n = len(errors)

    if not quiet:
        console.print()
        console.print(
            Panel(
                f"[error]✗ Invalid[/error]  [muted]·[/muted]  [bold]{file}[/bold]  "
                f"[muted]·[/muted]  [error]{n} error{'s' if n != 1 else ''}[/error]",
                border_style="red",
                padding=(0, 2),
            )
        )

    table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
    table.add_column("field", style="key", min_width=20)
    table.add_column("message")

    for err in errors:
        table.add_row(
            f"[error]✗[/error] {err['field']}",
            err["message"],
        )

    if not quiet:
        console.print(table)
        console.print()
        console.print(
            "  Fix the errors above and run [bold]datum check[/bold] again."
        )
        console.print()
