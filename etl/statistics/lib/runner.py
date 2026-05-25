"""runner.py - In-process module dispatch.

`dispatch_module` imports a registered module and calls its
`run()` on a scenario and returns a `ModuleResult`. `write_audit_csv` summarizes a batch of
results into a `stats_audit_<timestamp>.csv` file.
"""

from __future__ import annotations

import csv
import importlib
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from .config import MODULE_REGISTRY
from .protocol import ModuleResult

log = logging.getLogger(__name__)


def dispatch_module(
    module_name: str,
    scenario_short_code: str,
    conn,
    csv_path: Optional[str] = None,
) -> ModuleResult:
    """Import the named module and call its `run()`.

    Wraps any exception into a `ModuleResult` with `success=False` so the
    caller never sees a bare traceback for a single module's failure.
    """
    if module_name not in MODULE_REGISTRY:
        raise KeyError(f"Unknown module: {module_name!r}")

    spec = MODULE_REGISTRY[module_name]
    started = time.perf_counter()
    started_at = _utc_now()

    try:
        mod = importlib.import_module(spec.import_path)
        result = mod.run(scenario_short_code, conn, csv_path)
    except Exception as exc:
        elapsed = time.perf_counter() - started
        log.exception("module %s failed for %s", module_name, scenario_short_code)
        return ModuleResult(
            module_name=module_name,
            scenario_short_code=scenario_short_code,
            wall_time_sec=elapsed,
            started_at_utc=started_at,
            finished_at_utc=_utc_now(),
            success=False,
            error=f"{type(exc).__name__}: {exc}",
        )

    return result


def write_audit_csv(
    results: Iterable[ModuleResult],
    audit_dir: Path,
) -> Path:
    """Write a `stats_audit_<timestamp>.csv` summarizing a batch of results."""
    audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = audit_dir / f"stats_audit_{stamp}.csv"
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["module", "scenario", "success", "wall_time_sec", "rows_written", "error"]
        )
        for r in results:
            writer.writerow(
                [
                    r.module_name,
                    r.scenario_short_code,
                    r.success,
                    f"{r.wall_time_sec:.3f}",
                    "|".join(f"{k}={v}" for k, v in r.rows_written.items()),
                    r.error or "",
                ]
            )
    return path


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
