"""Logging configuration for Lockstep CLI."""

from __future__ import annotations

import logging

from rich.console import Console
from rich.logging import RichHandler

# Console for logging output (stderr)
_log_console = Console(stderr=True)


def setup_logging(verbose: bool, quiet: bool) -> None:
    """Configure logging based on verbosity flags.

    Args:
        verbose: Enable debug logging.
        quiet: Suppress all but error logging.
    """
    if quiet:
        level = logging.ERROR
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=_log_console, rich_tracebacks=True, show_path=False)],
    )

    # Reduce noise from libraries
    logging.getLogger("databricks").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

