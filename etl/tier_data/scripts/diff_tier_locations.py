#!/usr/bin/env python3
"""
diff_tier_locations.py - report differences between tier staging CSVs and
the live `tier_location` catalog.

Read-only. For each tier with a staging CSV, prints:

  - location_ids in the staging CSV but not active in `tier_location`
    (tier team added a location since the last sync)
  - location_ids active in `tier_location` but not in the staging CSV
    (tier team dropped a location since the last sync)
  - the count of matching active rows

To reconcile, run:
    python etl/tier_data/scripts/sync_tier_locations_from_staging.py --dry-run
    python etl/tier_data/scripts/sync_tier_locations_from_staging.py

Usage:
    python etl/tier_data/scripts/diff_tier_locations.py
    python etl/tier_data/scripts/diff_tier_locations.py --tier RES_STOR
    python etl/tier_data/scripts/diff_tier_locations.py --staging /custom/path
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from etl.common import (  # noqa: E402
    assess_coverage,
    format_coverage_warnings,
    get_db_connection,
)
from etl.tier_data.staging_inventory import (  # noqa: E402
    StagingInventory,
    TIER_LOCATION_TYPE,
    build_inventory,
)

DEFAULT_STAGING_DIR = Path(__file__).parent.parent / "staging"


def _fetch_db_ids(conn) -> Dict[str, Set[str]]:
    """Return {tier_short_code: {location_id}} for active rows in tier_location."""
    out: Dict[str, Set[str]] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tier_short_code, location_id FROM tier_location WHERE is_active = TRUE"
        )
        for tier, lid in cur.fetchall():
            out.setdefault(tier, set()).add(lid)
    return out


def _report(
    tier: str,
    location_type: str,
    csv_ids: Set[str],
    db_ids: Set[str],
) -> Tuple[List[str], List[str], int]:
    only_in_csv = sorted(csv_ids - db_ids)
    only_in_db = sorted(db_ids - csv_ids)
    matching = len(csv_ids & db_ids)
    print(f"\n{tier} ({location_type}):")
    if not csv_ids:
        print("  (no staging CSV found; cannot diff)")
        return only_in_csv, only_in_db, matching
    print(f"  in CSV, not in DB: {only_in_csv if only_in_csv else 'none'}")
    print(f"  in DB, not in CSV: {only_in_db if only_in_db else 'none'}")
    print(f"  matching        : {matching}")
    return only_in_csv, only_in_db, matching


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Report location-id gaps between tier staging CSVs and the live "
            "tier_location catalog."
        ),
    )
    parser.add_argument(
        "--tier",
        choices=sorted(TIER_LOCATION_TYPE),
        help="Only diff this tier (default: all tiers with staging CSVs).",
    )
    parser.add_argument(
        "--staging",
        type=Path,
        default=DEFAULT_STAGING_DIR,
        help=f"Staging directory to read CSVs from (default: {DEFAULT_STAGING_DIR}).",
    )
    args = parser.parse_args()

    inventory: Dict[str, StagingInventory] = build_inventory(args.staging)
    if args.tier:
        inventory = {args.tier: inventory[args.tier]} if args.tier in inventory else {}

    # Coverage check uses every id the developer might care about: staging
    # ids (so newly-added rows get checked before sync runs) plus the
    # already-active catalog rows (so a tier with no staging change but a
    # GIS regression still surfaces). Triples drive tier-aware attribute
    # resolution so AG_REV ids hit `du_agriculture_entity`.
    tier_locations: List[Tuple[str, str, str]] = []
    for inv in inventory.values():
        for lid in inv.ids:
            tier_locations.append((inv.tier, inv.location_type, lid))

    conn = get_db_connection(required=True)
    try:
        db_ids = _fetch_db_ids(conn)
        for tier, lids in db_ids.items():
            loc_type = TIER_LOCATION_TYPE.get(tier)
            if loc_type is None:
                continue
            for lid in lids:
                tier_locations.append((tier, loc_type, lid))
        coverage_reports = assess_coverage(conn, tier_locations)
    finally:
        conn.close()

    print("Tier location diff")
    print(f"  staging  : {args.staging}")
    print("  catalog  : tier_location (live DB, is_active=TRUE)")

    tiers_to_show: List[str]
    if args.tier:
        tiers_to_show = [args.tier]
    else:
        tiers_to_show = sorted(set(inventory) | set(db_ids))

    any_gaps = False
    any_missing_staging = False
    for tier in tiers_to_show:
        loc_type = TIER_LOCATION_TYPE.get(tier, "?")
        inv = inventory.get(tier)
        csv_ids = set(inv.ids) if inv else set()
        only_csv, only_db, _ = _report(tier, loc_type, csv_ids, db_ids.get(tier, set()))
        if not csv_ids:
            any_missing_staging = True
            continue
        if only_csv or only_db:
            any_gaps = True

    # Coverage scorecard: per location_type attribute and geometry hits
    # across the union of staging + catalog ids. Always printed so a clean
    # diff still tells the developer whether the catalog has full GIS.
    print()
    print("Coverage scorecard (attribute + geometry vs entity tables):")
    if not coverage_reports:
        print("  (no location_types to score)")
    else:
        for loc_type in sorted(coverage_reports):
            report = coverage_reports[loc_type]
            attr_resolved = report.attribute_resolved
            total = len(report.ids_checked)
            geom_part = (
                f"geometry {report.geometry_resolved}/{total}"
                if report.geometry_supported
                else "geometry n/a"
            )
            print(
                f"  {loc_type:<20} attribute {attr_resolved}/{total}, {geom_part}"
            )

    warning_lines = format_coverage_warnings(tier_locations, coverage_reports)
    if warning_lines:
        print()
        for line in warning_lines:
            print(line)

    print()
    if any_gaps:
        print("Membership gaps detected. To reconcile:")
        print("  1. python etl/tier_data/scripts/sync_tier_locations_from_staging.py --dry-run")
        print("  2. python etl/tier_data/scripts/sync_tier_locations_from_staging.py")
    elif any_missing_staging:
        print("No membership gaps in tiers with staging CSVs. Tiers without staging were skipped.")
        print("Run `python etl/tier_data/scripts/stage_tier_results.py` first to populate staging.")
    else:
        print("No membership gaps. Staging CSVs and tier_location are in sync.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
