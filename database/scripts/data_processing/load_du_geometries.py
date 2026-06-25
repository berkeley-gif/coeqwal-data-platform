#!/usr/bin/env python3
"""
load_du_geometries.py - load dissolved demand-unit polygons into the
`du_*_entity` tables from `database/seed_tables/03_GIS/du_4326.gpkg`.

This is a one-shot bootstrap loader for reference data, paired with
`database/sql_archive/04_scenario/56_add_du_geometry_columns.sql`. It is not part
of the recurring ETL pipeline; rerun it only when new polygons land
in the source GeoPackage (or `--gpkg` points at a successor file).

Source layer: `demandunits` in the GeoPackage. One row per `DU_ID`,
already dissolved, EPSG:4326, MULTIPOLYGON.

GeoPackage Binary Format (GPB), per OGC 12-128r15:
  bytes  0-1  magic     "GP"
  byte   2    version
  byte   3    flags     bits 1-3 encode the envelope size:
                          0 -> no envelope (0 bytes)
                          1 -> 2D envelope (32 bytes)
                          2 -> 3D-XYZ      (48 bytes)
                          3 -> 3D-XYM      (48 bytes)
                          4 -> 4D-XYZM     (64 bytes)
  bytes  4-7  srs_id
  N bytes     envelope (length from flags above)
  remainder   standard ISO/OGC WKB

`_strip_gpb_header` decodes the flag byte rather than hardcoding the
length, and then asserts the resulting WKB starts with a valid
endianness byte (`0x00` or `0x01`) and a MultiPolygon type code (`6`).
A malformed strip surfaces immediately as a Python-side ValueError
instead of silently writing garbage. Once the WKB is verified the loader
hands it to PostGIS via `ST_GeomFromWKB(wkb, 4326)` (PostGIS will also
reject malformed WKB, so two independent checks must both pass before a
row is written).

After every batch of writes the loader runs an in-DB validation pass:

  - row count of `geom IS NOT NULL` equals the count of UPDATEs that
    reported a non-zero rowcount;
  - `ST_IsValid(geom)` is TRUE for every loaded row;
  - `ST_SRID(geom) = 4326`;
  - `ST_GeometryType(geom)` is `ST_MultiPolygon`;
  - bounding box `(xmin, ymin, xmax, ymax)` is within California
    (approximately `-125 to -114 deg lon, 32 to 43 deg lat`).

Any failure of any check aborts the loader with a non-zero exit code,
so the load is either valid in every row or rolled back loudly. No GDAL,
geopandas, or shapely is required at any step.

For each `DU_ID` in the geopackage, the loader writes the polygon to
every `du_*_entity` table that already contains a row with that `du_id`.
`26N_NA` is the only `DU_ID` present in two tables (`du_urban_entity`
and `du_agriculture_entity`); both rows get the same dissolved polygon.

After every update the loader sets:

    geom         = ST_Multi(ST_CollectionExtract(
                     ST_MakeValid(ST_GeomFromWKB(:wkb, 4326)),
                     3
                   ))
    geom_wkt     = ST_AsText(geom)
    srid         = 4326
    has_gis_data = TRUE

The wrap exists because the dissolved polygons in
`du_4326.gpkg` fail strict OGC `ST_IsValid` (ring
self-intersections, duplicated vertices, etc.):

  - `ST_MakeValid` repairs the validity violations in place.
  - `ST_CollectionExtract(..., 3)` keeps only polygon-typed parts of
    the result. `ST_MakeValid` can return a `GeometryCollection`
    containing dangling line segments alongside the repaired
    polygons. The type-3 filter drops those.
  - `ST_Multi(...)` guarantees the final geometry is a
    `MultiPolygon`, matching the column type
    `geometry(MultiPolygon, 4326)` and the `ST_GeometryType` check
    in `validate_writes`.

The migration `database/sql_archive/04_scenario/56_add_du_geometry_columns.sql`
has already been applied to RDS, so the `geom_wkt` / `srid` / `geom`
columns exist. Re-apply it from `database/sql_archive/` only if you are rebuilding
the DU entity tables on a fresh database.

Read-only companion (audit / drift scorecard):
`etl/tier_data/scripts/audit_tier_location_geometry.py`. Persistent roster of
missing polygons: `docs/du_geometry_gap.md`.

Usage:
    export DATABASE_URL="postgresql://USER:PASS@HOST:5432/coeqwal_scenario"
    python database/scripts/data_processing/load_du_geometries.py --dry-run
    python database/scripts/data_processing/load_du_geometries.py
    python database/scripts/data_processing/load_du_geometries.py --gpkg /alt/path/du_4326.gpkg

Exit codes:
    0  - load completed; every gpkg `DU_ID` either matched at least one
         entity row or was logged as a gpkg-only id, and every in-DB
         validation check passed
    1  - load completed but with structural problems (e.g. missing
         migration columns, no DATABASE_URL, or a validation check
         failed)
    2  - bad CLI arguments
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_GPKG = (
    REPO_ROOT / "database" / "seed_tables" / "03_GIS" / "du_4326.gpkg"
)
GAP_DOC = "docs/du_geometry_gap.md"
MIGRATION_SCRIPT = "database/sql_archive/04_scenario/56_add_du_geometry_columns.sql"

ENTITY_TABLES: Tuple[str, ...] = (
    "du_urban_entity",
    "du_agriculture_entity",
    "du_refuge_entity",
)


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------


def _connect_db():
    """Open a psycopg2 connection using `DATABASE_URL` from the environment.

    Mirrors the pattern in `database/run_local_audit.py` so this script
    has no `etl/` dependency. Exits the process with a clear message if
    the variable is unset.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        print(
            "ERROR: DATABASE_URL is not set. Export it in your shell, e.g.:\n"
            "  export DATABASE_URL='postgresql://USER:PASS@HOST:5432/coeqwal_scenario'\n"
            "Cloud9 typically sets this in ~/.bashrc.",
            file=sys.stderr,
        )
        sys.exit(1)
    return psycopg2.connect(url)


# ---------------------------------------------------------------------------
# GeoPackage reader
# ---------------------------------------------------------------------------


def _gpb_header_length(flags: int) -> int:
    """Return the GPB header byte length implied by the flag byte.

    GeoPackage WKB blobs start with 2 bytes magic (`'GP'`), 1 byte
    version, 1 byte flags, then an optional envelope. The envelope
    type lives in bits 1-3 of the flag byte; 0 means no envelope, 1
    means a 32-byte 2D envelope, others are 3D/4D variants. Every
    polygon in `du_4326.gpkg` uses envelope type 1, so the prefix is
    8 + 32 = 40 bytes.
    """
    envelope_type = (flags >> 1) & 0x07
    envelope_bytes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope_type, 0)
    return 8 + envelope_bytes


_WKB_MULTIPOLYGON = 6


def _strip_gpb_header(blob: bytes) -> bytes:
    """Return the plain WKB payload of a GeoPackage geometry blob.

    Validates:
      - GPB magic `'GP'` at bytes 0-1;
      - the byte at the computed offset is a legal WKB endianness
        marker (`0x00` big-endian, `0x01` little-endian);
      - the next 4 bytes decode to WKB type 6 (MultiPolygon).

    Raises `ValueError` on any check failure. This guards against a
    malformed header length: a wrong strip would either misalign the
    endianness byte or yield a nonsense geometry type, and both are
    caught here before the bytes ever reach PostGIS.
    """
    if len(blob) < 8 or blob[:2] != b"GP":
        raise ValueError("Not a GeoPackage geometry blob (missing 'GP' magic).")
    wkb = blob[_gpb_header_length(blob[3]):]
    if len(wkb) < 5:
        raise ValueError("Stripped WKB is too short to contain header.")
    endian = wkb[0]
    if endian not in (0, 1):
        raise ValueError(
            f"Stripped WKB has invalid endianness byte 0x{endian:02x}; "
            "GPB header length likely miscalculated."
        )
    byte_order = "little" if endian == 1 else "big"
    geom_type = int.from_bytes(wkb[1:5], byte_order)
    if geom_type != _WKB_MULTIPOLYGON:
        raise ValueError(
            f"Stripped WKB geometry type is {geom_type}, expected "
            f"{_WKB_MULTIPOLYGON} (MultiPolygon); GPB header strip likely wrong."
        )
    return wkb


def read_gpkg_polygons(gpkg_path: Path) -> Dict[str, bytes]:
    """Return {du_id: raw_wkb_bytes} for every non-null DU_ID in the gpkg."""
    out: Dict[str, bytes] = {}
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GeoPackage not found at {gpkg_path}")
    conn = sqlite3.connect(str(gpkg_path))
    try:
        cur = conn.execute(
            "SELECT DU_ID, geom FROM demandunits "
            "WHERE DU_ID IS NOT NULL AND TRIM(DU_ID) <> ''"
        )
        for du_id, blob in cur.fetchall():
            out[du_id] = _strip_gpb_header(blob)
    finally:
        conn.close()
    return out


# ---------------------------------------------------------------------------
# DB introspection
# ---------------------------------------------------------------------------


def _table_has_geom_columns(conn, table: str) -> bool:
    """Confirm the migration script ran (columns geom_wkt, srid, geom present)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s "
            "AND column_name IN ('geom', 'geom_wkt', 'srid')",
            (table,),
        )
        return {row[0] for row in cur.fetchall()} == {"geom", "geom_wkt", "srid"}


def _fetch_entity_du_ids(conn) -> Dict[str, Set[str]]:
    """Return {table: {du_id}} for the three demand-unit entity tables."""
    out: Dict[str, Set[str]] = {}
    with conn.cursor() as cur:
        for table in ENTITY_TABLES:
            cur.execute(f'SELECT du_id FROM "{table}"')
            out[table] = {row[0] for row in cur.fetchall()}
    return out


# ---------------------------------------------------------------------------
# Update plan
# ---------------------------------------------------------------------------


def plan_updates(
    gpkg_polygons: Dict[str, bytes],
    entity_ids: Dict[str, Set[str]],
) -> Tuple[Dict[str, List[str]], List[str], Dict[str, List[str]]]:
    """Decide which (table, du_id) rows to update.

    Returns:
        updates_by_table: {table: [du_id, ...]} - du_ids the loader will
            write polygons to, grouped by entity table.
        gpkg_only: du_ids present in the gpkg but absent from every
            entity table (logged, no DB write).
        missing_in_gpkg: {table: [du_id, ...]} - per-table list of
            du_ids that exist in the entity table but have no polygon
            in the gpkg (logged, no DB write).
    """
    updates_by_table: Dict[str, List[str]] = defaultdict(list)
    for du_id in sorted(gpkg_polygons):
        for table in ENTITY_TABLES:
            if du_id in entity_ids.get(table, set()):
                updates_by_table[table].append(du_id)
    gpkg_ids = set(gpkg_polygons)
    all_entity_ids: Set[str] = set().union(*entity_ids.values())
    gpkg_only = sorted(gpkg_ids - all_entity_ids)
    missing_in_gpkg: Dict[str, List[str]] = {
        table: sorted(ids - gpkg_ids) for table, ids in entity_ids.items()
    }
    return dict(updates_by_table), gpkg_only, missing_in_gpkg


# ---------------------------------------------------------------------------
# DB writer
# ---------------------------------------------------------------------------


def apply_updates(
    conn,
    updates_by_table: Dict[str, List[str]],
    gpkg_polygons: Dict[str, bytes],
    dry_run: bool,
) -> Dict[str, int]:
    """Write polygons to the entity tables. Returns {table: rows_updated}."""
    written: Dict[str, int] = {table: 0 for table in ENTITY_TABLES}
    if dry_run:
        for table, du_ids in updates_by_table.items():
            written[table] = len(du_ids)
        return written

    with conn.cursor() as cur:
        for table, du_ids in updates_by_table.items():
            for du_id in du_ids:
                wkb = gpkg_polygons[du_id]
                cur.execute(
                    f'UPDATE "{table}" SET '
                    f'  geom         = ST_Multi(ST_CollectionExtract('
                    f'                   ST_MakeValid(ST_GeomFromWKB(%s::bytea, 4326)),'
                    f'                   3'
                    f'                 )), '
                    f'  geom_wkt     = ST_AsText(ST_Multi(ST_CollectionExtract('
                    f'                   ST_MakeValid(ST_GeomFromWKB(%s::bytea, 4326)),'
                    f'                   3'
                    f'                 ))), '
                    f'  srid         = 4326, '
                    f'  has_gis_data = TRUE '
                    f'WHERE du_id = %s',
                    (wkb, wkb, du_id),
                )
                written[table] += cur.rowcount
    conn.commit()
    return written


# California bounding box, generously padded so a polygon on the very edge
# of the state still falls inside. Anything outside this box would indicate
# a CRS mix-up (e.g. accidentally loading EPSG:3857 web-mercator bytes).
_CA_LON_RANGE = (-125.0, -114.0)
_CA_LAT_RANGE = (32.0, 43.0)


def validate_writes(
    conn,
    updates_by_table: Dict[str, List[str]],
    written: Dict[str, int],
) -> List[str]:
    """Run server-side sanity checks on every row the loader just wrote.

    Returns a list of problem descriptions. Empty list means everything
    passed. Each check is independent so multiple failures surface at
    once instead of stopping at the first one.

    Checks per table:
      - count of `du_id IN (planned) AND geom IS NOT NULL` equals the
        UPDATE-reported rowcount (i.e. nothing was silently NULLed);
      - `ST_IsValid(geom)` is TRUE for every planned row;
      - `ST_SRID(geom) = 4326`;
      - `ST_GeometryType(geom) = 'ST_MultiPolygon'`;
      - the union bounding box of all planned rows in this table lies
        inside the California envelope (catches CRS mix-ups).
    """
    problems: List[str] = []
    with conn.cursor() as cur:
        for table, du_ids in updates_by_table.items():
            if not du_ids:
                continue

            cur.execute(
                f'SELECT COUNT(*) FROM "{table}" '
                f'WHERE du_id = ANY(%s) AND geom IS NOT NULL',
                (du_ids,),
            )
            non_null = cur.fetchone()[0]
            if non_null != written.get(table, 0):
                problems.append(
                    f"{table}: planned {written.get(table, 0)} writes but "
                    f"only {non_null} rows have non-null geom afterwards"
                )

            cur.execute(
                f'SELECT du_id FROM "{table}" '
                f'WHERE du_id = ANY(%s) AND geom IS NOT NULL '
                f'AND NOT ST_IsValid(geom) LIMIT 5',
                (du_ids,),
            )
            invalid = [r[0] for r in cur.fetchall()]
            if invalid:
                problems.append(f"{table}: invalid geometries (sample): {invalid}")

            cur.execute(
                f'SELECT DISTINCT ST_SRID(geom) FROM "{table}" '
                f'WHERE du_id = ANY(%s) AND geom IS NOT NULL',
                (du_ids,),
            )
            srids = {r[0] for r in cur.fetchall()}
            if srids - {4326}:
                problems.append(f"{table}: unexpected SRIDs: {sorted(srids)}")

            cur.execute(
                f"SELECT DISTINCT ST_GeometryType(geom) FROM \"{table}\" "
                f'WHERE du_id = ANY(%s) AND geom IS NOT NULL',
                (du_ids,),
            )
            types = {r[0] for r in cur.fetchall()}
            if types - {"ST_MultiPolygon"}:
                problems.append(
                    f"{table}: unexpected geometry types: {sorted(types)}"
                )

            cur.execute(
                f"SELECT "
                f"  ST_XMin(ST_Extent(geom)), ST_YMin(ST_Extent(geom)), "
                f"  ST_XMax(ST_Extent(geom)), ST_YMax(ST_Extent(geom)) "
                f'FROM "{table}" '
                f'WHERE du_id = ANY(%s) AND geom IS NOT NULL',
                (du_ids,),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                xmin, ymin, xmax, ymax = row
                in_box = (
                    _CA_LON_RANGE[0] <= xmin <= _CA_LON_RANGE[1]
                    and _CA_LON_RANGE[0] <= xmax <= _CA_LON_RANGE[1]
                    and _CA_LAT_RANGE[0] <= ymin <= _CA_LAT_RANGE[1]
                    and _CA_LAT_RANGE[0] <= ymax <= _CA_LAT_RANGE[1]
                )
                if not in_box:
                    problems.append(
                        f"{table}: union bbox ({xmin:.3f}, {ymin:.3f}) - "
                        f"({xmax:.3f}, {ymax:.3f}) falls outside California; "
                        f"likely a CRS / endian misparse"
                    )
    return problems


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _preview(ids: Iterable[str], cap: int = 6) -> str:
    seq = list(ids)
    head = ", ".join(seq[:cap])
    tail = f" (+{len(seq) - cap} more)" if len(seq) > cap else ""
    return head + tail if head else "(none)"


def report(
    gpkg_polygons: Dict[str, bytes],
    entity_ids: Dict[str, Set[str]],
    updates_by_table: Dict[str, List[str]],
    gpkg_only: List[str],
    missing_in_gpkg: Dict[str, List[str]],
    written: Dict[str, int],
    dry_run: bool,
) -> None:
    print()
    print("=" * 78)
    print("DEMAND-UNIT GEOMETRY LOAD")
    print("=" * 78)
    print(f"  GeoPackage features (non-null DU_ID): {len(gpkg_polygons)}")
    for table in ENTITY_TABLES:
        print(
            f"  {table:<23}  entity rows: {len(entity_ids.get(table, set())):>4}  "
            f"matched in gpkg: {len(updates_by_table.get(table, [])):>4}  "
            f"missing in gpkg: {len(missing_in_gpkg.get(table, [])):>4}"
        )
    label = "would update" if dry_run else "updated"
    total = sum(written.values())
    print(f"\n  rows {label}: {total}")

    if gpkg_only:
        print(
            f"\n  gpkg-only DU_IDs (not in any entity table; {len(gpkg_only)}): "
            f"{_preview(gpkg_only)}"
        )

    for table in ENTITY_TABLES:
        missing = missing_in_gpkg.get(table, [])
        if not missing:
            continue
        print(
            f"\n  {table}: {len(missing)} DU_IDs lack a polygon in {DEFAULT_GPKG.name}:"
        )
        print(f"    {_preview(missing, cap=20)}")

    print(
        f"\n  Reconcile against {GAP_DOC} when this list shrinks. "
        f"Polygons missing today should be sourced from the responsible "
        f"agencies and added to {DEFAULT_GPKG.name}."
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Load dissolved demand-unit polygons into the du_*_entity tables "
            f"from {DEFAULT_GPKG}. Requires the migration "
            f"{MIGRATION_SCRIPT} to have been applied first."
        ),
    )
    parser.add_argument(
        "--gpkg",
        type=Path,
        default=DEFAULT_GPKG,
        help=f"GeoPackage path (default: {DEFAULT_GPKG}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read the gpkg, plan and print the updates, but do not write to the DB.",
    )
    args = parser.parse_args()

    try:
        gpkg_polygons = read_gpkg_polygons(args.gpkg)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"ERROR reading {args.gpkg}: {e}", file=sys.stderr)
        return 1
    print(f"Read {len(gpkg_polygons)} polygons from {args.gpkg}.")

    conn = _connect_db()
    problems: List[str] = []
    try:
        for table in ENTITY_TABLES:
            if not _table_has_geom_columns(conn, table):
                print(
                    f"ERROR: {table} is missing one or more of (geom_wkt, srid, geom). "
                    f"Apply {MIGRATION_SCRIPT} first.",
                    file=sys.stderr,
                )
                return 1
        entity_ids = _fetch_entity_du_ids(conn)
        updates_by_table, gpkg_only, missing_in_gpkg = plan_updates(
            gpkg_polygons, entity_ids
        )
        written = apply_updates(conn, updates_by_table, gpkg_polygons, args.dry_run)
        report(
            gpkg_polygons,
            entity_ids,
            updates_by_table,
            gpkg_only,
            missing_in_gpkg,
            written,
            args.dry_run,
        )
        if not args.dry_run:
            print("\nRunning post-write validation pass...")
            problems = validate_writes(conn, updates_by_table, written)
            if problems:
                print("\nVALIDATION FAILED:")
                for p in problems:
                    print(f"  - {p}")
            else:
                print("  all checks passed (rowcount, ST_IsValid, SRID, "
                      "geometry type, California bbox).")
    finally:
        conn.close()
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
