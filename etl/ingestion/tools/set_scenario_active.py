#!/usr/bin/env python3
"""
set_scenario_active.py - flips `scenario.is_active` in the database.

The supported way to promote a scenario onto the public website (or take
one off) once its DB row already exists. Runs a single
`UPDATE scenario SET is_active = ... WHERE short_code = ANY(...)` against
`DATABASE_URL`, then chains `refresh_active_scenarios.py` to regenerate
`etl/common/active_scenarios.py` and the README marker block.

Two and only two sources of truth for the project's scenario lists:

  - `etl/common/etl_scenarios.py` (`ETL_SCENARIOS`): project-wide intent,
    derived from the WAM scenario listing CSV by `refresh_etl_scenarios.py`.
  - `etl/common/active_scenarios.py` (`ACTIVE_SCENARIOS`): what the public
    website actually serves, derived from the DB by `refresh_active_scenarios.py`.

New scenario rows enter the DB via a hand-authored INSERT script committed
under `database/scripts/sql/` (see `add_s0107-s0156_scenarios.sql`),
inserted with `is_active = FALSE`. This script only flips that flag for rows
that already exist. It never inserts.

Usage:
  python etl/ingestion/tools/set_scenario_active.py --activate s0070,s0072
  python etl/ingestion/tools/set_scenario_active.py --deactivate s0036
  python etl/ingestion/tools/set_scenario_active.py --activate s0070 --deactivate s0036
  python etl/ingestion/tools/set_scenario_active.py --activate s0070 --dry-run
  python etl/ingestion/tools/set_scenario_active.py --activate s0070 --skip-refresh
"""

from __future__ import annotations

# Make `etl.common` and `etl.ingestion.lib` importable when this script is
# invoked directly with `python etl/ingestion/tools/set_scenario_active.py`.
# See etl/common/__init__.py for the rationale.
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import argparse  # noqa: E402
import logging  # noqa: E402
import subprocess  # noqa: E402
from typing import List, Set  # noqa: E402

from etl.common import get_db_connection, parse_scenarios  # noqa: E402

REFRESH_SCRIPT = Path(__file__).resolve().parent / "refresh_active_scenarios.py"

log = logging.getLogger("set_scenario_active")


def _fetch_current_state(conn, short_codes: List[str]) -> dict:
    """Return {short_code: is_active} for the requested codes that exist in the DB."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT short_code, is_active FROM scenario WHERE short_code = ANY(%s)",
            (short_codes,),
        )
        return {row[0]: bool(row[1]) for row in cur.fetchall()}


def _apply_updates(conn, activate: List[str], deactivate: List[str]) -> dict:
    """Run the two UPDATEs in one transaction. Returns rows-changed per group."""
    counts = {"activated": 0, "deactivated": 0}
    with conn.cursor() as cur:
        if activate:
            cur.execute(
                "UPDATE scenario SET is_active = TRUE WHERE short_code = ANY(%s)",
                (activate,),
            )
            counts["activated"] = cur.rowcount
        if deactivate:
            cur.execute(
                "UPDATE scenario SET is_active = FALSE WHERE short_code = ANY(%s)",
                (deactivate,),
            )
            counts["deactivated"] = cur.rowcount
    conn.commit()
    return counts


def _print_state_table(title: str, state: dict, requested: List[str]) -> None:
    """Pretty-print {short_code: is_active} as a small two-column table."""
    print(f"\n{title}:")
    print(f"  {'short_code':<12} {'is_active'}")
    print(f"  {'-' * 12} {'-' * 9}")
    for sc in sorted(requested):
        if sc in state:
            print(f"  {sc:<12} {state[sc]}")
        else:
            print(f"  {sc:<12} (not in scenario table)")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Flip scenario.is_active in the DB and regenerate "
            "etl/common/active_scenarios.py from the live API."
        )
    )
    parser.add_argument(
        "--activate", nargs="*", default=[],
        help="Short codes to set is_active=TRUE. Whitespace or comma-separated. "
             "Newline-pasted spreadsheet columns also work.",
    )
    parser.add_argument(
        "--deactivate", nargs="*", default=[],
        help="Short codes to set is_active=FALSE. Same parsing as --activate.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the planned UPDATE statements and exit. No DB write, no refresh.",
    )
    parser.add_argument(
        "--skip-refresh", action="store_true",
        help="Skip the chained `refresh_active_scenarios.py` call. Useful when "
             "you want to batch several flips and refresh at the end.",
    )
    args = parser.parse_args()

    activate: Set[str] = parse_scenarios(args.activate)
    deactivate: Set[str] = parse_scenarios(args.deactivate)

    if not activate and not deactivate:
        parser.error("Pass --activate, --deactivate, or both. Nothing to do otherwise.")

    overlap = activate & deactivate
    if overlap:
        parser.error(
            f"Short codes appear in both --activate and --deactivate: {sorted(overlap)}"
        )

    activate_list = sorted(activate)
    deactivate_list = sorted(deactivate)
    all_codes = sorted(activate | deactivate)

    if args.dry_run:
        print("DRY RUN. Would run:")
        if activate_list:
            print(f"  UPDATE scenario SET is_active = TRUE  WHERE short_code = ANY({activate_list})")
        if deactivate_list:
            print(f"  UPDATE scenario SET is_active = FALSE WHERE short_code = ANY({deactivate_list})")
        print("\nThen would invoke:")
        print(f"  {sys.executable} {REFRESH_SCRIPT}")
        print("\n(no DB connection opened, no refresh run)")
        return

    conn = get_db_connection(required=True)
    try:
        before = _fetch_current_state(conn, all_codes)

        missing = [sc for sc in all_codes if sc not in before]
        if missing:
            raise SystemExit(
                f"\nThese short codes are not in the scenario table: {missing}\n"
                f"Insert their identity rows first with a hand-authored SQL script "
                f"under database/scripts/sql/ (modeled on add_s0107-s0156_scenarios.sql), "
                f"then re-run this script.\n"
            )

        _print_state_table("Before", before, all_codes)

        counts = _apply_updates(conn, activate_list, deactivate_list)
        log.info(
            "Updated scenario.is_active: %d activated, %d deactivated",
            counts["activated"], counts["deactivated"],
        )

        after = _fetch_current_state(conn, all_codes)
        _print_state_table("After", after, all_codes)
    finally:
        conn.close()

    if args.skip_refresh:
        log.info("--skip-refresh set. Run `python %s` when ready.", REFRESH_SCRIPT)
        return

    log.info("Refreshing etl/common/active_scenarios.py from the API ...")
    result = subprocess.run(
        [sys.executable, str(REFRESH_SCRIPT)],
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"\nrefresh_active_scenarios.py exited with code {result.returncode}. "
            f"The DB write succeeded; re-run the refresh manually:\n"
            f"  python {REFRESH_SCRIPT}\n"
        )


if __name__ == "__main__":
    main()
