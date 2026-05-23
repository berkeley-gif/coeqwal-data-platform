"""scenarios.py - Shared scenario short-code helpers: argparse parsing and active-set override resolution.

`parse_scenarios` normalizes whatever `argparse` handed back for a `--scenarios`
or `--scenarios-override` flag into a clean set of short codes. Comma, whitespace,
and newline-pasted spreadsheet columns all work.

`resolve_active_scenarios` is the escape hatch for the three
consumers that gate on `ACTIVE_SCENARIOS` (tier loaders, API verification, tier
verification). When a developer passes `--scenarios-override sXXX`, the consumer
uses that set instead of the auto-generated `ACTIVE_SCENARIOS` for the duration
of that one run. To persistently change which scenarios are active on the public
website, use `etl/ingestion/tools/set_scenario_active.py` instead.
"""

from __future__ import annotations

import logging
from typing import Iterable, Union

from etl.common.active_scenarios import ACTIVE_SCENARIOS

log = logging.getLogger(__name__)


def parse_scenarios(values: Union[None, str, Iterable[str]]) -> set[str]:
    """Normalize a `--scenarios`-style CLI argument into a set of lowercase short codes.

    Accepts whatever the developer pasted into the shell. Splits on whitespace
    and commas in any combination. Useful when copying a column straight from a
    spreadsheet (newline-separated) or a comma-separated string from elsewhere.

    Examples (all yield {"s0070", "s0071", "s0072"}):
      ["s0070", "s0071", "s0072"]      # nargs="*" with spaces
      ["s0070,s0071,s0072"]            # comma-pasted into one shell token
      ["s0070, s0071, s0072"]          # comma-and-space
      ["s0070\\ns0071\\ns0072"]        # newline-pasted from a spreadsheet
    """
    if not values:
        return set()
    if isinstance(values, str):
        values = [values]
    out: set[str] = set()
    for v in values:
        if v is None:
            continue
        for token in str(v).replace(",", " ").split():
            t = token.strip().lower()
            if t:
                out.add(t)
    return out


def resolve_active_scenarios(
    override: Union[None, str, Iterable[str]] = None,
) -> frozenset[str]:
    """Return the active-scenario set for this invocation.

    If `override` is empty or None, returns `ACTIVE_SCENARIOS` (the auto-generated
    set from the live API). If `override` is non-empty, returns the parsed
    override as a frozenset and logs a WARNING so it is visible in pipeline logs.
    """
    parsed = parse_scenarios(override)
    if not parsed:
        return ACTIVE_SCENARIOS

    resolved = frozenset(parsed)
    log.warning(
        "--scenarios-override active. Replacing ACTIVE_SCENARIOS (%d codes) "
        "with override (%d codes): %s",
        len(ACTIVE_SCENARIOS), len(resolved), sorted(resolved),
    )
    return resolved


__all__ = ["parse_scenarios", "resolve_active_scenarios"]
