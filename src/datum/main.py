"""Datum CLI — open datasets, open source."""

from __future__ import annotations

from typing import Optional

import typer

from datum.__init__ import __version__
from datum.commands.cache import cache_app
from datum.commands.check import cmd_check
from datum.commands.config import config_app
from datum.commands.info import cmd_info
from datum.commands.init import cmd_init
from datum.commands.list import cmd_list
from datum.commands.login import cmd_login, cmd_logout
from datum.commands.register import cmd_register
from datum.commands.publish import cmd_publish
from datum.commands.pull import cmd_pull
from datum.commands.search import cmd_search
from datum.commands.unpublish import cmd_unpublish
from datum.commands.update import cmd_update
from datum.state import OutputFormat, state

# ---------------------------------------------------------------------------
# Root app
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="datum",
    help=(
        "[bold cyan]Datum[/bold cyan] — open datasets, open source.\n\n"
        "Publish and consume open datasets with a familiar, composable CLI.\n"
        "Datasets are identified as [bold]publisher.namespace.dataset:version[/bold].\n\n"
        "[bold]Note:[/bold] global flags ([bold]--output[/bold], [bold]--quiet[/bold], "
        "[bold]--registry[/bold]) must come [bold]before[/bold] the subcommand:\n\n"
        "  datum --output json info my.ns.dataset\n"
        "  datum --quiet pull my.ns.dataset:1.0.0"
    ),
    add_completion=True,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


@app.callback()
def _root(
    ctx: typer.Context,
    registry: Optional[str] = typer.Option(
        None,
        "--registry",
        help="Registry URL or local path (overrides config)",
        envvar="DATUM_REGISTRY",
    ),
    output: OutputFormat = typer.Option(
        OutputFormat.table,
        "--output",
        "-o",
        help="Output format: table | json | plain",
        envvar="DATUM_OUTPUT",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Suppress non-essential output (useful in scripts)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Emit additional diagnostic information",
    ),
    version: bool = typer.Option(
        False,
        "--version",
        help="Print version and exit",
        is_eager=True,
    ),
) -> None:
    """Global options that apply to every datum command."""
    if version:
        typer.echo(f"datum {__version__}")
        raise typer.Exit()

    state.registry = registry or ""
    state.output = output
    state.quiet = quiet
    state.verbose = verbose


# ---------------------------------------------------------------------------
# Register commands
# ---------------------------------------------------------------------------

app.command("init", help="Create a datapackage.json via an interactive wizard.")(cmd_init)
app.command("check", help="Validate a datapackage.json against the Datum schema.")(cmd_check)
app.command("publish", help="Publish dataset metadata to the registry.")(cmd_publish)
app.command("pull", help="Download a dataset by identifier and verify its checksum.")(cmd_pull)
app.command("info", help="Show dataset metadata without downloading data files.")(cmd_info)
app.command("search", help="Search the registry by keyword.")(cmd_search)
app.command("register", help="Create an account on a Datum registry.")(cmd_register)
app.command("login", help="Authenticate with a Datum registry.")(cmd_login)
app.command("logout", help="Remove stored credentials for a registry.")(cmd_logout)
app.command("unpublish", help="Remove a dataset version from the local registry.")(cmd_unpublish)
app.command("update", help="Pull the latest version of one or all cached datasets.")(cmd_update)

# list / ls alias
app.command("list", help="List datasets in the registry.")(cmd_list)
app.command("ls", help="Alias for [bold]datum list[/bold].", hidden=True)(cmd_list)

# Sub-command groups
app.add_typer(cache_app, name="cache")
app.add_typer(config_app, name="config")


if __name__ == "__main__":
    app()
