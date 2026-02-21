"""datum unpublish — remove a dataset version from the local registry."""

from __future__ import annotations

import json
from typing import Optional

import typer

from datum.console import console, err_console
from datum.models import ID_PATTERN
from datum.registry.local import get_registry
from datum.state import OutputFormat, state


def cmd_unpublish(
    identifier: str = typer.Argument(
        ..., help="Dataset identifier (publisher.namespace.dataset:version)"
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Skip confirmation prompt"
    ),
    all_versions: bool = typer.Option(
        False, "--all", help="Remove all versions of this dataset"
    ),
) -> None:
    """
    Remove a dataset version from the local registry.

    IDENTIFIER format: [bold]publisher.namespace.dataset:version[/bold]

    Use [bold]--all[/bold] to remove every version of a dataset at once.

    Exit codes: 0 success · 1 not found / bad identifier
    """
    output_fmt = state.output
    quiet = state.quiet

    # Parse identifier
    if ":" in identifier:
        id_part, version = identifier.split(":", 1)
    else:
        id_part = identifier
        version = None

    if not ID_PATTERN.match(id_part):
        if output_fmt == OutputFormat.json:
            print(json.dumps({"unpublished": False, "error": f"Invalid identifier: {id_part!r}"}))
        else:
            err_console.print(
                f"\n[error]✗[/error] Invalid identifier: [bold]{id_part}[/bold]\n"
            )
        raise typer.Exit(code=1)

    if version is None and not all_versions:
        err_console.print(
            "\n[error]✗[/error] Specify a version ([bold]publisher.namespace.dataset:version[/bold]) "
            "or use [bold]--all[/bold] to remove all versions.\n"
        )
        raise typer.Exit(code=1)

    registry = get_registry()

    # Fetch from registry — may raise RuntimeError for remote registries
    try:
        if all_versions:
            versions_found = registry.versions(id_part)
        else:
            existing_pkg = registry.get(id_part, version)
    except RuntimeError as exc:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"unpublished": False, "error": str(exc)}))
        else:
            err_console.print(f"\n[error]✗[/error] {exc}\n")
        raise typer.Exit(code=2)

    # Determine versions_to_remove — no registry access beyond this point
    if all_versions:
        if not versions_found:
            if output_fmt == OutputFormat.json:
                print(json.dumps({"unpublished": False, "error": f"Not found: {id_part}"}))
            else:
                err_console.print(
                    f"\n[error]✗[/error] No versions of [bold]{id_part}[/bold] found.\n"
                )
            raise typer.Exit(code=1)
        versions_to_remove = versions_found
        label = f"{id_part} ({len(versions_to_remove)} version(s))"
    else:
        if existing_pkg is None:
            label = f"{id_part}:{version}"
            if output_fmt == OutputFormat.json:
                print(json.dumps({"unpublished": False, "error": f"Not found: {label}"}))
            else:
                err_console.print(
                    f"\n[error]✗[/error] [bold]{label}[/bold] not found in registry.\n"
                )
            raise typer.Exit(code=1)
        versions_to_remove = [version]
        label = f"{id_part}:{version}"

    # Confirm
    if not yes and output_fmt != OutputFormat.json:
        confirmed = typer.confirm(f"Remove {label} from the registry?", default=False)
        if not confirmed:
            console.print("\n  Aborted.\n")
            raise typer.Exit(code=0)

    # Remove
    try:
        removed = [v for v in versions_to_remove if registry.unpublish(id_part, v)]
    except PermissionError as exc:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"unpublished": False, "error": str(exc)}))
        else:
            err_console.print(f"\n[error]✗[/error] {exc}\n")
        raise typer.Exit(code=1)
    except RuntimeError as exc:
        if output_fmt == OutputFormat.json:
            print(json.dumps({"unpublished": False, "error": str(exc)}))
        else:
            err_console.print(f"\n[error]✗[/error] {exc}\n")
        raise typer.Exit(code=2)

    if output_fmt == OutputFormat.json:
        print(json.dumps({
            "unpublished": True,
            "id": id_part,
            "versions": removed,
        }))
    elif not quiet:
        console.print()
        for v in removed:
            console.print(f"  [success]✓[/success]  Unpublished [bold]{id_part}:{v}[/bold]")
        console.print()
