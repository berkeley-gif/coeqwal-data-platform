"""commands.py - CLI subcommands imported by `etl/statistics/run_all.py`."""

from __future__ import annotations


def cmd_list_modules(args) -> int:
    """Print the module registry to stdout. Exit code 0 on success."""
    raise NotImplementedError


def cmd_run_all(args) -> int:
    """Dispatch one or more modules for one or more scenarios."""
    raise NotImplementedError
