#!/usr/bin/env python3
"""
sync_tier_locations_from_staging.py - reconcile the `tier_location` catalog/table
with the tier teams' staging CSVs.

The staging CSVs in `etl/tier_data/staging/` are the source of truth for
which locations belong to each tier outcome. This script:

  1. Builds the per-tier location inventory from staging
     (`etl.tier_data.staging_inventory`).
  2. Validates every (`location_type`, `location_id`) against the entity
     tables named in `etl/common/tier_location_entities.py` and refuses
     to write unresolved ids unless `--allow-unresolved` is passed.
  3. Upserts active rows into `tier_location` (insert new, mark
     re-introduced rows `is_active = TRUE`, refresh `display_order`).
  4. Soft-deletes anything no longer in staging by flipping `is_active`
     to FALSE (preserving history; we never DELETE).

Run as a regular DB user. `DATABASE_URL` must be writable on
`tier_location`.

Usage:
    # Dry run, full diff scorecard:
    python etl/tier_data/sync_tier_locations_from_staging.py --dry-run

    # Apply changes in a single transaction:
    python etl/tier_data/sync_tier_locations_from_staging.py

    # Only sync a subset of tiers:
    python etl/tier_data/sync_tier_locations_from_staging.py --tier RES_STOR,GW_STOR

    # Skip the validation against entity tables (use during gap-fill only):
    python etl/tier_data/sync_tier_locations_from_staging.py --allow-unresolved
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from etl.common import (  # noqa: E402
    CoverageReport,
    assess_coverage,
    format_coverage_warnings,
    get_db_connection,
)
from etl.tier_data.staging_inventory import (  # noqa: E402
    StagingInventory,
    build_inventory,
)

DEFAULT_STAGING_DIR = Path(__file__).parent / "staging"


@dataclass
class TierPlan:
    """What the sync will do for one tier outcome."""

    tier: str
    location_type: str
    to_insert: List[Tuple[str, int]] = field(default_factory=list)   # (location_id, display_order)
    to_reactivate: List[Tuple[str, int]] = field(default_factory=list)
    to_update_order: List[Tuple[str, int]] = field(default_factory=list)
    to_deactivate: List[str] = field(default_factory=list)
    unresolved: List[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return not (
            self.to_insert
            or self.to_reactivate
            or self.to_update_order
            or self.to_deactivate
            or self.unresolved
        )


def _fetch_existing(conn) -> Dict[str, Dict[str, Tuple[int, bool]]]:
    """Return {tier: {location_id: (display_order, is_active)}}."""
    out: Dict[str, Dict[str, Tuple[int, bool]]] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tier_short_code, location_id, display_order, is_active FROM tier_location"
        )
        for tier, lid, order, active in cur.fetchall():
            out.setdefault(tier, {})[lid] = (order, active)
    return out


def _coverage_for_inventory(
    conn,
    inventories: Iterable[StagingInventory],
) -> Dict[str, CoverageReport]:
    """Run attribute + geometry coverage across every staging id at once.

    Passes `(tier, location_type, location_id)` triples so AG_REV ids
    resolve against `du_agriculture_entity` instead of `du_urban_entity`
    (see `attribute_resolver_for`).
    """
    triples = [
        (inv.tier, inv.location_type, lid)
        for inv in inventories
        for lid in inv.ids
    ]
    return assess_coverage(conn, triples)


def _unresolved_attributes(
    inventories: Iterable[StagingInventory],
    reports: Dict[str, CoverageReport],
) -> Dict[str, Set[str]]:
    """`{tier: {ids}}` for staging ids whose entity row does not exist."""
    out: Dict[str, Set[str]] = {}
    for inv in inventories:
        missing_for_type = set(reports.get(inv.location_type, CoverageReport(
            location_type=inv.location_type,
            ids_checked=[],
            attribute_missing=[],
            geometry_missing=[],
            geometry_supported=False,
        )).attribute_missing)
        missing = {i for i in inv.ids if i in missing_for_type}
        if missing:
            out[inv.tier] = missing
    return out


def _build_plan(
    inv: StagingInventory,
    existing: Dict[str, Tuple[int, bool]],
    unresolved_ids: Set[str],
) -> TierPlan:
    """Diff one tier's staging members against the live tier_location rows."""
    plan = TierPlan(tier=inv.tier, location_type=inv.location_type)
    staging_map = {m.location_id: m.display_order for m in inv.members}
    seen_in_staging: Set[str] = set()

    for lid, order in staging_map.items():
        seen_in_staging.add(lid)
        if lid in unresolved_ids:
            plan.unresolved.append(lid)
            continue
        if lid not in existing:
            plan.to_insert.append((lid, order))
            continue
        existing_order, existing_active = existing[lid]
        if not existing_active:
            plan.to_reactivate.append((lid, order))
        elif existing_order != order:
            plan.to_update_order.append((lid, order))

    for lid, (_, active) in existing.items():
        if active and lid not in seen_in_staging:
            plan.to_deactivate.append(lid)

    plan.to_insert.sort()
    plan.to_reactivate.sort()
    plan.to_update_order.sort()
    plan.to_deactivate.sort()
    plan.unresolved.sort()
    return plan


def _print_plan(
    plans: List[TierPlan],
    inventory: Dict[str, StagingInventory],
    reports: Dict[str, CoverageReport],
) -> None:
    print()
    print("=" * 72)
    print("TIER LOCATION SYNC PLAN")
    print("=" * 72)
    if not plans:
        print("No tiers with staging CSVs to sync.")
        return
    for plan in plans:
        clean = "(no changes)" if plan.is_clean else ""
        print(f"\n{plan.tier}  [{plan.location_type}]  {clean}")
        if plan.to_insert:
            print(f"  insert       ({len(plan.to_insert)}): {[i for i, _ in plan.to_insert]}")
        if plan.to_reactivate:
            print(f"  reactivate   ({len(plan.to_reactivate)}): {[i for i, _ in plan.to_reactivate]}")
        if plan.to_update_order:
            print(f"  reorder      ({len(plan.to_update_order)}): {plan.to_update_order}")
        if plan.to_deactivate:
            print(f"  deactivate   ({len(plan.to_deactivate)}): {plan.to_deactivate}")
        if plan.unresolved:
            print(f"  UNRESOLVED   ({len(plan.unresolved)}): {plan.unresolved}")
            print("    -> entity table has no matching row; run audit_tier_location_geometry.py for context")

        # Per-tier coverage line so the developer sees attribute + geometry
        # hits/misses for every tier in the plan, including tiers with no
        # other changes.
        inv = inventory.get(plan.tier)
        report = reports.get(plan.location_type)
        if inv is None or report is None:
            continue
        ids = set(inv.ids)
        attr_miss = len(ids & set(report.attribute_missing))
        attr_ok = len(ids) - attr_miss
        if report.geometry_supported:
            geom_miss = len(ids & set(report.geometry_missing))
            geom_ok = len(ids) - geom_miss
            print(
                f"  coverage     : attribute {attr_ok}/{len(ids)}, "
                f"geometry {geom_ok}/{len(ids)}"
            )
        else:
            print(
                f"  coverage     : attribute {attr_ok}/{len(ids)}, "
                f"geometry n/a (no registered geometry resolver)"
            )


def _tier_locations_for(
    inventory: Dict[str, StagingInventory],
) -> List[Tuple[str, str, str]]:
    """Flatten staging inventory into `(tier, location_type, location_id)` tuples."""
    out: List[Tuple[str, str, str]] = []
    for inv in inventory.values():
        for m in inv.members:
            out.append((inv.tier, inv.location_type, m.location_id))
    return out


def _apply(conn, plans: List[TierPlan]) -> None:
    with conn.cursor() as cur:
        for plan in plans:
            for lid, order in plan.to_insert:
                cur.execute(
                    "INSERT INTO tier_location "
                    "(tier_short_code, location_type, location_id, display_order, is_active) "
                    "VALUES (%s, %s, %s, %s, TRUE)",
                    (plan.tier, plan.location_type, lid, order),
                )
            for lid, order in plan.to_reactivate:
                cur.execute(
                    "UPDATE tier_location SET is_active = TRUE, display_order = %s, "
                    "updated_at = NOW(), updated_by = coeqwal_current_operator() "
                    "WHERE tier_short_code = %s AND location_id = %s",
                    (order, plan.tier, lid),
                )
            for lid, order in plan.to_update_order:
                cur.execute(
                    "UPDATE tier_location SET display_order = %s, "
                    "updated_at = NOW(), updated_by = coeqwal_current_operator() "
                    "WHERE tier_short_code = %s AND location_id = %s",
                    (order, plan.tier, lid),
                )
            for lid in plan.to_deactivate:
                cur.execute(
                    "UPDATE tier_location SET is_active = FALSE, "
                    "updated_at = NOW(), updated_by = coeqwal_current_operator() "
                    "WHERE tier_short_code = %s AND location_id = %s",
                    (plan.tier, lid),
                )
    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconcile tier_location with the tier-team staging CSVs.",
    )
    parser.add_argument(
        "--staging",
        type=Path,
        default=DEFAULT_STAGING_DIR,
        help=f"Staging dir (default: {DEFAULT_STAGING_DIR}).",
    )
    parser.add_argument(
        "--tier",
        type=str,
        help="Comma-separated list of tiers to sync (default: all with staging CSVs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan but do not commit any DB changes.",
    )
    parser.add_argument(
        "--allow-unresolved",
        action="store_true",
        help=(
            "Skip the entity-table membership check. Use only during a "
            "geometry / attribute gap-fill, after audit_tier_location_geometry.py "
            "has identified the missing rows."
        ),
    )
    args = parser.parse_args()

    inventory = build_inventory(args.staging)
    if args.tier:
        wanted = {t.strip() for t in args.tier.split(",") if t.strip()}
        unknown = wanted - set(inventory)
        if unknown:
            print(f"WARNING: requested tiers without staging CSVs: {sorted(unknown)}")
        inventory = {t: inv for t, inv in inventory.items() if t in wanted}

    if not inventory:
        print(f"No staging CSVs found in {args.staging}. Nothing to sync.")
        return 0

    conn = get_db_connection(required=True)
    try:
        existing = _fetch_existing(conn)

        # One batched coverage pass for everything in staging. Drives
        # both the attribute-block path (existing behavior) and the new
        # warn-only geometry alert.
        reports = _coverage_for_inventory(conn, inventory.values())

        unresolved_map: Dict[str, Set[str]] = {}
        if not args.allow_unresolved:
            unresolved_map = _unresolved_attributes(inventory.values(), reports)

        plans = [
            _build_plan(inventory[t], existing.get(t, {}), unresolved_map.get(t, set()))
            for t in sorted(inventory)
        ]
        _print_plan(plans, inventory, reports)

        warning_lines = format_coverage_warnings(
            _tier_locations_for(inventory), reports
        )
        if warning_lines:
            print()
            for line in warning_lines:
                print(line)

        unresolved_total = sum(len(p.unresolved) for p in plans)
        if unresolved_total:
            print(
                f"\n{unresolved_total} location_id(s) failed entity-table validation. "
                "Resolve in the entity table (or pass --allow-unresolved) and re-run."
            )
            return 2

        if args.dry_run:
            print("\n--dry-run: no changes committed.")
            return 0

        if all(p.is_clean for p in plans):
            print("\nNothing to do; tier_location already matches staging.")
            return 0

        _apply(conn, plans)
        print("\nSync applied. tier_location now reflects the staging CSVs.")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
