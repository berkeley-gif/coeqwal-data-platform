"""module.py - Reservoir statistics module entry point.

`run()` is the contract called by `etl/statistics/lib/runner.dispatch_module`.
"""

from __future__ import annotations

from typing import Optional

from etl.statistics.lib.protocol import ModuleResult

MODULE_NAME = "reservoirs"


def run(
    scenario_short_code: str,
    conn,
    csv_path: Optional[str] = None,
) -> ModuleResult:
    """Calculate reservoir statistics for one scenario and write to DB.

    Args:
        scenario_short_code: e.g. "s0020"
        conn: live psycopg2 connection (caller manages lifecycle)
        csv_path: optional local CSV path. If None, fetched from S3

    Returns:
        ModuleResult summarizing what was written and how long it took
    """
    raise NotImplementedError
