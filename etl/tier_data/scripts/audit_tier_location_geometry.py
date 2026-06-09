#!/usr/bin/env python3
"""
audit_tier_location_geometry.py - inventory PostGIS coverage for tier locations.

Read-only audit. For each tier `location_type` in
[`etl/common/tier_location_entities.py`](../common/tier_location_entities.py):

  1. Attribute scorecard: of the location_ids extracted from staging CSVs
     (or from the `tier_location` table when staging is empty), how many
     resolve in the attribute table named by the entity registry?
  2. Geometry scorecard: how many resolve in the geometry table with a
     non-NULL `geom`? Applies `SLUIS_CVP`/`SLUIS_SWP` -> `SLUIS` aliasing
     for `reservoir`.
  3. Schema drift pass: column inventory diff between the live RDS and
     [`database/schema/COEQWAL_SCENARIOS_DB_ERD.md`](../../database/schema/COEQWAL_SCENARIOS_DB_ERD.md).
     Flags columns the live DB has that the ERD does not document (and
     vice versa) for every table the entity registry references.

Requires `DATABASE_URL`. Reads, never writes. Designed to be re-runnable
to audit and fill geometry gaps related to tier locations and their updates.

Companion writer for demand-unit polygons:
[`database/scripts/data_processing/load_du_geometries.py`](../../database/scripts/data_processing/load_du_geometries.py)
(requires the [`56_add_du_geometry_columns.sql`](../../database/sql_archive/04_scenario/56_add_du_geometry_columns.sql)
migration). For data gaps see
[`docs/du_geometry_gap.md`](../../docs/du_geometry_gap.md).

Usage:
    python etl/tier_data/scripts/audit_tier_location_geometry.py
    python etl/tier_data/scripts/audit_tier_location_geometry.py --tier RES_STOR
    python etl/tier_data/scripts/audit_tier_location_geometry.py --staging /custom/path
    python etl/tier_data/scripts/audit_tier_location_geometry.py --skip-drift
    python etl/tier_data/scripts/audit_tier_location_geometry.py --json out/audit.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from etl.common import (  # noqa: E402
    LOCATION_ENTITY_MAP,
    CoverageReport,
    assess_coverage,
    get_db_connection,
)
from etl.tier_data.staging_inventory import (  # noqa: E402
    TIER_LOCATION_TYPE,
    build_inventory,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STAGING_DIR = Path(__file__).parent.parent / "staging"
ERD_PATH = REPO_ROOT / "database/schema/COEQWAL_SCENARIOS_DB_ERD.md"


@dataclass
class TierIds:
    """The canonical set of (location_id) we expect to resolve, per tier."""
    tier: str
    location_type: str
    ids: Set[str] = field(default_factory=set)
    source: str = ""


def _staging_ids(staging_dir: Path) -> Dict[str, TierIds]:
    """Per-tier location_id sets from the canonical staging CSVs.

    Thin adapter over `staging_inventory.build_inventory` so this script
    and `sync_tier_locations_from_staging.py` / `diff_tier_locations.py`
    all see the same membership extraction. Don't reintroduce a private
    parser here. Bugs found in one parser would otherwise not be fixed
    in the others (this happened: ENV_FLOWS was reading the scenario
    column as locations, fixed in staging_inventory but the audit's
    duplicate copy kept reporting 0/72).
    """
    out: Dict[str, TierIds] = {}
    for tier, inv in build_inventory(staging_dir).items():
        out[tier] = TierIds(
            tier=tier,
            location_type=inv.location_type,
            ids={m.location_id for m in inv.members if m.location_id},
            source=", ".join(inv.source_files),
        )
    return out


def _db_ids(conn) -> Dict[str, TierIds]:
    """Fallback: pull location_id sets from tier_location when staging is empty."""
    out: Dict[str, TierIds] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tier_short_code, location_type, location_id "
            "FROM tier_location WHERE is_active = TRUE"
        )
        for tier, loc_type, loc_id in cur.fetchall():
            entry = out.setdefault(tier, TierIds(tier=tier, location_type=loc_type, source="tier_location DB"))
            entry.ids.add(loc_id)
    return out


@dataclass
class TierAudit:
    tier: str
    location_type: str
    source: str
    total_ids: int
    attribute_resolved: int
    attribute_missing: List[str]
    geometry_resolved: int
    geometry_missing: List[str]
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "tier": self.tier,
            "location_type": self.location_type,
            "source": self.source,
            "total_ids": self.total_ids,
            "attribute_resolved": self.attribute_resolved,
            "attribute_missing": sorted(self.attribute_missing),
            "geometry_resolved": self.geometry_resolved,
            "geometry_missing": sorted(self.geometry_missing),
            "note": self.note,
        }


def _audit_tier(tids: TierIds, report: CoverageReport) -> TierAudit:
    """Project a per-`location_type` CoverageReport onto one tier's id set."""
    ids = set(tids.ids)
    entry = LOCATION_ENTITY_MAP.get(tids.location_type)
    if entry is None:
        return TierAudit(
            tier=tids.tier,
            location_type=tids.location_type,
            source=tids.source,
            total_ids=len(ids),
            attribute_resolved=0,
            attribute_missing=sorted(ids),
            geometry_resolved=0,
            geometry_missing=sorted(ids),
            note=f"No entity registry entry for location_type={tids.location_type!r}",
        )

    attr_missing = sorted(ids & set(report.attribute_missing))
    if report.geometry_supported:
        geom_missing = sorted(ids & set(report.geometry_missing))
        geom_resolved = len(ids) - len(geom_missing)
        note = entry.geometry.notes if entry.geometry else ""
    else:
        geom_missing = []
        geom_resolved = 0
        note = (entry.geometry.notes if entry.geometry else "") or \
            "No geometry path registered for this location_type."

    return TierAudit(
        tier=tids.tier,
        location_type=tids.location_type,
        source=tids.source,
        total_ids=len(ids),
        attribute_resolved=len(ids) - len(attr_missing),
        attribute_missing=attr_missing,
        geometry_resolved=geom_resolved,
        geometry_missing=geom_missing,
        note=note,
    )


# ---------------------------------------------------------------------------
# ERD vs live drift
# ---------------------------------------------------------------------------


_TABLE_HEADING_RE = re.compile(r"^###\s+\*\*(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\*\*")
# Lines inside an ERD column block look like:
#   id                             integer              [PK]
# We only want the column name (first token); the type metadata is too noisy
# to round-trip and not what drift hunting needs.
_COL_LINE_RE = re.compile(r"^\s{2,}(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\s+\S")


def _parse_erd_columns(erd_path: Path) -> Dict[str, Set[str]]:
    """Best-effort parse of the markdown ERD into {table: {column}}.

    The ERD is loosely structured prose with code blocks. This walks the
    file, tracks the current `### **table_name**` heading, and collects
    indented identifier-looking lines under the next ``` block. Good
    enough to flag drift; not a strict parser.
    """
    out: Dict[str, Set[str]] = {}
    current: Optional[str] = None
    in_code = False
    in_columns = False
    if not erd_path.exists():
        return out
    for raw in erd_path.read_text().splitlines():
        heading = _TABLE_HEADING_RE.match(raw)
        if heading:
            current = heading.group("name")
            in_code = False
            in_columns = False
            out.setdefault(current, set())
            continue
        stripped = raw.strip()
        if stripped.startswith("```"):
            in_code = not in_code
            in_columns = False
            continue
        if not in_code or current is None:
            continue
        if stripped.startswith("Columns:"):
            in_columns = True
            continue
        if not in_columns:
            continue
        m = _COL_LINE_RE.match(raw)
        if m:
            out[current].add(m.group("name"))
    return out


def _live_columns(conn, tables: Iterable[str]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = ANY(%s)",
            (sorted(set(tables)),),
        )
        for table, col in cur.fetchall():
            out.setdefault(table, set()).add(col)
    return out


def _drift_pass(conn) -> List[dict]:
    """For every table the entity registry touches, compare live vs ERD columns."""
    tables: Set[str] = set()
    for entry in LOCATION_ENTITY_MAP.values():
        tables.add(entry.attribute.table)
        if entry.geometry is not None:
            tables.add(entry.geometry.table)

    erd = _parse_erd_columns(ERD_PATH)
    live = _live_columns(conn, tables)
    rows: List[dict] = []
    for table in sorted(tables):
        live_cols = live.get(table, set())
        erd_cols = erd.get(table, set())
        in_live_not_erd = sorted(live_cols - erd_cols)
        in_erd_not_live = sorted(erd_cols - live_cols)
        rows.append({
            "table": table,
            "live_present": bool(live_cols),
            "erd_present": bool(erd_cols),
            "in_live_not_erd": in_live_not_erd,
            "in_erd_not_live": in_erd_not_live,
        })
    return rows


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _print_scorecard(audits: List[TierAudit]) -> None:
    print()
    print("=" * 78)
    print("TIER LOCATION GEOMETRY AUDIT")
    print("=" * 78)
    if not audits:
        print("No tiers audited (no staging CSVs and no tier_location DB rows).")
        return
    width = max(len(a.tier) for a in audits)
    for a in audits:
        attr_pct = (a.attribute_resolved / a.total_ids * 100) if a.total_ids else 0.0
        geom_pct = (a.geometry_resolved / a.total_ids * 100) if a.total_ids else 0.0
        print(
            f"\n{a.tier.ljust(width)}  [{a.location_type}]  source={a.source}\n"
            f"  attribute : {a.attribute_resolved:>3}/{a.total_ids:<3}  ({attr_pct:5.1f}%)"
        )
        if a.attribute_missing:
            preview = ", ".join(a.attribute_missing[:8])
            tail = "..." if len(a.attribute_missing) > 8 else ""
            print(f"              missing: {preview}{tail}")
        print(f"  geometry  : {a.geometry_resolved:>3}/{a.total_ids:<3}  ({geom_pct:5.1f}%)")
        if a.geometry_missing:
            preview = ", ".join(a.geometry_missing[:8])
            tail = "..." if len(a.geometry_missing) > 8 else ""
            print(f"              missing: {preview}{tail}")
        if a.note:
            print(f"  note      : {a.note}")


def _print_drift(rows: List[dict]) -> None:
    print()
    print("=" * 78)
    print("SCHEMA DRIFT (live RDS vs COEQWAL_SCENARIOS_DB_ERD.md)")
    print("=" * 78)
    for row in rows:
        table = row["table"]
        if not row["live_present"]:
            print(f"\n{table}: NOT FOUND in live DB (expected by entity registry)")
            continue
        if not row["erd_present"]:
            print(f"\n{table}: not documented in ERD")
        extras = row["in_live_not_erd"]
        missing = row["in_erd_not_live"]
        if not extras and not missing:
            print(f"\n{table}: in sync")
            continue
        print(f"\n{table}:")
        if extras:
            print(f"  columns in live not in ERD ({len(extras)}): {', '.join(extras)}")
        if missing:
            print(f"  columns in ERD not in live ({len(missing)}): {', '.join(missing)}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit PostGIS coverage for every tier location_type, and report "
            "ERD vs live schema drift for the tables the entity registry uses."
        ),
    )
    parser.add_argument(
        "--tier",
        choices=sorted(TIER_LOCATION_TYPE),
        help="Only audit this tier (default: all).",
    )
    parser.add_argument(
        "--staging",
        type=Path,
        default=DEFAULT_STAGING_DIR,
        help=f"Staging dir to read canonical flat CSVs from (default: {DEFAULT_STAGING_DIR}).",
    )
    parser.add_argument(
        "--use-db-ids",
        action="store_true",
        help="Ignore staging CSVs; audit the location_ids currently active in tier_location.",
    )
    parser.add_argument(
        "--skip-drift",
        action="store_true",
        help="Skip the ERD-vs-live schema drift pass.",
    )
    parser.add_argument(
        "--json",
        type=Path,
        help="Optional path to write a machine-readable JSON dump alongside the printed scorecard.",
    )
    args = parser.parse_args()

    conn = get_db_connection(required=True)
    try:
        if args.use_db_ids:
            by_tier = _db_ids(conn)
            if args.tier:
                by_tier = {args.tier: by_tier[args.tier]} if args.tier in by_tier else {}
        else:
            staging = _staging_ids(args.staging)
            if args.tier:
                staging = {args.tier: staging[args.tier]} if args.tier in staging else {}
            by_tier = staging
            # Backfill from DB only for tiers whose staging file is missing.
            if not args.tier:
                db_view = _db_ids(conn)
                for tier, ids in db_view.items():
                    if tier not in by_tier:
                        by_tier[tier] = ids

        # Pass `(tier, location_type, location_id)` triples so attribute
        # lookups stay tier-aware (AG_REV resolves against
        # du_agriculture_entity, CWS_DEL against du_urban_entity).
        # `assess_coverage` internally collapses tiers that share a
        # resolver into one query, so wba (GW_STOR + DELTA_ECO) still
        # hits the wba table only once.
        triples: List[Tuple[str, str, str]] = [
            (tids.tier, tids.location_type, lid)
            for tids in by_tier.values()
            for lid in tids.ids
        ]
        reports = assess_coverage(conn, triples)

        audits = [
            _audit_tier(by_tier[t], reports[by_tier[t].location_type])
            for t in sorted(by_tier)
        ]
        _print_scorecard(audits)

        drift: List[dict] = []
        if not args.skip_drift:
            drift = _drift_pass(conn)
            _print_drift(drift)

        if args.json:
            args.json.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "audits": [a.to_dict() for a in audits],
                "drift": drift,
            }
            args.json.write_text(json.dumps(payload, indent=2))
            print(f"\nJSON written to {args.json}")
    finally:
        conn.close()

    # Exit non-zero if any tier has gaps, so CI / wrappers can branch on it.
    has_gaps = any(a.attribute_missing or a.geometry_missing for a in audits)
    return 1 if has_gaps else 0


if __name__ == "__main__":
    sys.exit(main())
