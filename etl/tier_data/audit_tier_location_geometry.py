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
(requires the [`56_add_du_geometry_columns.sql`](../../database/scripts/sql/56_add_du_geometry_columns.sql)
migration). For the persistent gap roster see
[`docs/du_geometry_gap.md`](../../docs/du_geometry_gap.md).

Usage:
    python etl/tier_data/audit_tier_location_geometry.py
    python etl/tier_data/audit_tier_location_geometry.py --tier RES_STOR
    python etl/tier_data/audit_tier_location_geometry.py --staging /custom/path
    python etl/tier_data/audit_tier_location_geometry.py --skip-drift
    python etl/tier_data/audit_tier_location_geometry.py --json out/audit.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from etl.common import (  # noqa: E402
    LOCATION_ENTITY_MAP,
    CoverageReport,
    assess_coverage,
    get_db_connection,
)
from etl.tier_data.staging_inventory import TIER_LOCATION_TYPE  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STAGING_DIR = Path(__file__).parent / "staging"
ERD_PATH = REPO_ROOT / "database/schema/COEQWAL_SCENARIOS_DB_ERD.md"


_RES_STOR_COL_RE = re.compile(r"^S_(?P<short>[A-Z0-9_]+?)_Storage_Tier$")


def _res_stor_id(column: str) -> str:
    m = _RES_STOR_COL_RE.match(column)
    return m.group("short") if m else column


def _wba_id(col: str) -> str:
    """Mirror of load_all_tier_results.convert_wba_id_to_mapbox_format."""
    if col == "DETAW":
        return "DETAW"
    if col.startswith("WBA"):
        suffix = col[3:]
        if suffix and suffix[0].isdigit():
            if len(suffix) == 1 or (len(suffix) == 2 and suffix[1] in "NS"):
                return "0" + suffix
        return suffix
    return col


@dataclass
class TierIds:
    """The canonical set of (location_id) we expect to resolve, per tier."""
    tier: str
    location_type: str
    ids: Set[str] = field(default_factory=set)
    source: str = ""


def _staging_ids(staging_dir: Path) -> Dict[str, TierIds]:
    """Extract per-tier location_id sets from the canonical staging CSVs."""
    import pandas as pd

    out: Dict[str, TierIds] = {}

    def _add(tier: str, ids: Iterable[str], source: str) -> None:
        loc_type = TIER_LOCATION_TYPE[tier]
        entry = out.setdefault(tier, TierIds(tier=tier, location_type=loc_type, source=source))
        entry.ids.update(i for i in ids if i)

    # ENV_FLOWS: row index across legacy and split files
    env_files = []
    legacy = staging_dir / "ENV_FLOWS.csv"
    if legacy.exists():
        env_files.append(legacy)
    env_files.extend(sorted(staging_dir.glob("ENV_FLOWS_*.csv")))
    for path in env_files:
        df = pd.read_csv(path, index_col=0)
        _add("ENV_FLOWS", (str(s).strip() for s in df.index), source=str(path.name))

    # RES_STOR: column headers parsed via S_*_Storage_Tier
    p = staging_dir / "RES_STOR.csv"
    if p.exists():
        df = pd.read_csv(p)
        _add("RES_STOR", (_res_stor_id(c) for c in df.columns if c != "Scenario"), source=p.name)

    # GW_STOR: WBA columns converted to mapbox format
    p = staging_dir / "GW_STOR.csv"
    if p.exists():
        df = pd.read_csv(p)
        _add("GW_STOR", (_wba_id(c) for c in df.columns if c != "scenario"), source=p.name)

    # CWS_DEL: column headers are DU ids
    p = staging_dir / "CWS_DEL.csv"
    if p.exists():
        df = pd.read_csv(p)
        _add("CWS_DEL", (c for c in df.columns[1:]), source=p.name)

    # AG_REV: long or wide; both yield DU ids
    p = staging_dir / "AG_REV.csv"
    if p.exists():
        df = pd.read_csv(p)
        if "region" in df.columns and "tier" in df.columns:
            _add("AG_REV", (str(r) for r in df["region"].dropna().unique()), source=p.name)
        else:
            _add("AG_REV", (c for c in df.columns[1:]), source=p.name)

    # DELTA_ECO: fixed DETAW
    p = staging_dir / "DELTA_ECO.csv"
    if p.exists():
        _add("DELTA_ECO", ("DETAW",), source=p.name)

    # FW_DELTA_USES: fixed EM, JP
    p = staging_dir / "FW_DELTA_USES.csv"
    if p.exists():
        _add("FW_DELTA_USES", ("EM", "JP"), source=p.name)

    # FW_EXP: fixed Banks/Jones network nodes
    p = staging_dir / "FW_EXP.csv"
    if p.exists():
        _add("FW_EXP", ("CAA003", "DMC000"), source=p.name)

    # WRC_SALMON_AB: fixed SAC299
    p = staging_dir / "WRC_SALMON_AB.csv"
    if p.exists():
        _add("WRC_SALMON_AB", ("SAC299",), source=p.name)

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
