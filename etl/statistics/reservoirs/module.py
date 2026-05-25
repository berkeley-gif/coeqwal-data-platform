"""module.py - Reservoir statistics module entry point.

`run()` is the contract called by `etl/statistics/lib/runner.dispatch_module`.

The work itself lives in `reservoirs/main.py` (`process_scenario`), which
loads reservoir metadata, calls the percentile and statistics calculators,
and writes results to PostgreSQL through psycopg2. This file is a thin
adapter that:

1. resolves the `reservoirs/` directory on sys.path so `main.py`'s
   sibling-flat imports work whether the caller is run_all.py, a test,
   or an interactive shell
2. invokes `process_scenario`
3. wraps the result in a `ModuleResult` for the runner
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from etl.statistics.lib.protocol import ModuleResult

_RESERVOIRS_DIR = Path(__file__).resolve().parent
if str(_RESERVOIRS_DIR) not in sys.path:
    sys.path.insert(0, str(_RESERVOIRS_DIR))

import main as _legacy_main  # noqa: E402

MODULE_NAME = "reservoirs"


def run(
    scenario_short_code: str,
    conn=None,
    csv_path: Optional[str] = None,
) -> ModuleResult:
    """Calculate reservoir statistics for one scenario and write to DB.

    Args:
        scenario_short_code: e.g. "s0020"
        conn: ignored. `process_scenario` opens its own psycopg2
            connection per writer call. Kept on the signature to match
            the shared `RunFn` protocol.
        csv_path: optional local CSV path. If None, the CSV is fetched
            from the configured S3 bucket.

    Returns:
        ModuleResult summarizing the rows written and wall-clock time.
    """
    started_perf = time.perf_counter()
    started_at = _utc_now()

    result = _legacy_main.process_scenario(
        scenario_id=scenario_short_code,
        write_to_db=True,
        csv_path=csv_path,
    )
    elapsed = time.perf_counter() - started_perf

    return ModuleResult(
        module_name=MODULE_NAME,
        scenario_short_code=scenario_short_code,
        rows_written=dict(result.get("counts", {})),
        wall_time_sec=elapsed,
        started_at_utc=started_at,
        finished_at_utc=_utc_now(),
        success=True,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
