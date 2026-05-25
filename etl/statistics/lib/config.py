"""config.py - Constants and module registry for the statistics ETL runner.

`MODULE_REGISTRY` lists each stats module's import path, output tables,
and the path to its legacy per-module CLI.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple


@dataclass(frozen=True)
class ModuleSpec:
    """Where a stats module lives and what it writes."""

    name: str
    import_path: str
    output_tables: Tuple[str, ...]
    legacy_cli_path: Path
    csv_arg_name: Optional[str] = None


_STATS_DIR = Path(__file__).resolve().parent.parent

DEFAULT_AUDIT_DIR = _STATS_DIR / "audit_reports"

MODULE_REGISTRY = {
    "reservoirs": ModuleSpec(
        name="Reservoir Statistics",
        import_path="etl.statistics.reservoirs.module",
        output_tables=(
            "reservoir_monthly_percentile",
            "reservoir_storage_monthly",
            "reservoir_spill_monthly",
            "reservoir_period_summary",
        ),
        legacy_cli_path=_STATS_DIR / "reservoirs" / "main.py",
    ),
    # More modules registered here as their module.py is added.
}
