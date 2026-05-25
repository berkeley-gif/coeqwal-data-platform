"""protocol.py - The contract every statistics module implements.

Each module exposes `run(scenario_short_code, conn=None, csv_path=None)`
and returns a `ModuleResult`. The runner and CLI commands work with this
contract, not with any specific module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Protocol


@dataclass(frozen=True)
class ModuleResult:
    """Outcome of one `run()` invocation."""

    module_name: str
    scenario_short_code: str
    rows_written: Dict[str, int] = field(default_factory=dict)
    wall_time_sec: float = 0.0
    started_at_utc: str = ""
    finished_at_utc: str = ""
    success: bool = True
    error: Optional[str] = None


class RunFn(Protocol):
    """Signature each module's `module.py` exposes as `run`."""

    def __call__(
        self,
        scenario_short_code: str,
        conn=None,
        csv_path: Optional[str] = None,
    ) -> ModuleResult: ...
