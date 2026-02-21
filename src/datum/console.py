from rich.console import Console
from rich.theme import Theme

_THEME = Theme(
    {
        "info": "dim cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "bold green",
        "muted": "dim white",
        "identifier": "bold cyan",
        "key": "bold white",
        "check.ok": "green",
        "check.fail": "red",
        "check.warn": "yellow",
    }
)

# Primary console — writes to stdout (for composable output).
console = Console(theme=_THEME)

# Error console — writes to stderr so stdout stays clean for piping.
err_console = Console(stderr=True, theme=_THEME)
