#!/usr/bin/env python3
"""
Export all foundational schema tables (layers 00-08) to CSV.

Writes one CSV per table, organized into subfolders by layer number. 
Use this to verify that live database content matches the seed data 
in database/seed_tables/.

Usage
-----
    # Export all layers (run from repo root)
    python database/scripts/export_layer_tables.py

    # Custom output directory
    python database/scripts/export_layer_tables.py --output-dir /tmp/exports

    # Single layer only
    python database/scripts/export_layer_tables.py --layer 06

Output
------
    exports/layer_tables/          (default, at repo root)
    ├── 00_versioning/
    │   ├── developer.csv
    │   ├── version_family.csv
    │   ├── version.csv
    │   └── domain_family_map.csv
    ├── 01_lookup/
    │   └── <one csv per table>
    ├── ...
    ├── 08_theme/
    │   └── <one csv per table>
    └── summary.csv                row counts for all exported tables

Notes
-----
- PostGIS geometry columns are exported as WKT text via ST_AsText().
- The audit_log table is excluded (can be very large; run separately if needed).
- Connects as read-only; the script never writes to the database.
- Run from the repo root with DATABASE_URL set in your environment.
"""

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2

# ── Layer → table definitions ─────────────────────────────────────────────────
# Table order within each layer follows FK dependency (parents before children).
LAYERS: dict[str, list[str]] = {
    "00_versioning": [
        "developer",
        "version_family",
        "version",
        "domain_family_map",
    ],
    "01_lookup": [
        "hydrologic_region",
        "source",
        "model_source",
        "unit",
        "spatial_scale",
        "temporal_scale",
        "statistic_type",
        "geometry_type",
        "network_type",
        "network_subtype",
        "watershed",
        "wba",
    ],
    "02_network": [
        # network_entity_type is present in the live DB and verified by
        # 09_verify_level02.sql, but is not listed in the ERD architecture
        # overview — the ERD has no dedicated Layer 02 section. ERD gap,
        # not a script error.
        "network_entity_type",
        "network",
        "network_arc",
        "network_node",
        "network_gis",
    ],
    "03_entity": [
        "reservoir",
        "compliance_station",
        "du_agriculture_entity",
        "du_urban_entity",
        "du_refuge_entity",
        "reservoir_entity",
        "mi_contractor",
    ],
    "04_variable": [
        "calsim_model_variable_type",
        "derived_variable_type",
        "variable_type",
        "channel_variable",
        "reservoir_variable",
        "inflow_variable",
        "derived_variable",
    ],
    "05_assumptions_operations": [
        "assumption_category",
        "assumption_definition",
        "operation_category",
        "operation_definition",
        "scenario_key_assumption_link",
        "scenario_key_operation_link",
    ],
    "06_scenario": [
        "scenario",
        "scenario_author",
        "scenario_source",
        "scenario_source_link",
    ],
    "07_hydroclimate": [
        "hydroclimate",
        "slr",
    ],
    "08_theme": [
        "theme",
        "theme_scenario_link",
        "theme_source_link",
    ],
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def build_select(cur: psycopg2.extensions.cursor, table_name: str):
    """
    Return (sql, col_names) for the table.

    Geometry columns are wrapped in ST_AsText() so they export as WKT strings
    instead of binary blobs. Returns (None, []) if the table doesn't exist.
    """
    cur.execute(
        """
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    columns = cur.fetchall()
    if not columns:
        return None, []

    parts = []
    col_names = []
    for col_name, udt_name in columns:
        if udt_name == "geometry":
            parts.append(f'ST_AsText("{col_name}") AS "{col_name}"')
        else:
            parts.append(f'"{col_name}"')
        col_names.append(col_name)

    sql = f'SELECT {", ".join(parts)} FROM "{table_name}" ORDER BY 1'
    return sql, col_names


def export_table(
    cur: psycopg2.extensions.cursor,
    table_name: str,
    output_path: Path,
) -> int | None:
    """
    Export one table to a CSV file.

    Returns the row count on success, or None if the table was not found or
    the query failed.
    """
    sql, col_names = build_select(cur, table_name)
    if sql is None:
        print(f"  SKIP  {table_name} (table not found)")
        return None

    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except psycopg2.Error as exc:
        print(f"  ERROR {table_name}: {exc}")
        return None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(col_names)
        writer.writerows(rows)

    print(f"  OK    {table_name:<40} {len(rows):>6,} rows  →  {output_path}")
    return len(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export COEQWAL schema layers 00-08 to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output-dir",
        default="exports/layer_tables",
        help="Output directory (default: exports/layer_tables)",
    )
    parser.add_argument(
        "--layer",
        metavar="PREFIX",
        help=(
            "Export a single layer only, matched by prefix "
            "(e.g. --layer 06 exports '06_scenario')"
        ),
    )
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print(
            "Error: DATABASE_URL environment variable is not set.\n"
            '  export DATABASE_URL="postgresql://user:pass@host:5432/coeqwal_scenario"',
            file=sys.stderr,
        )
        sys.exit(1)

    # Resolve which layers to export
    layers_to_export = LAYERS
    if args.layer:
        matching = {k: v for k, v in LAYERS.items() if k.startswith(args.layer)}
        if not matching:
            print(
                f"Error: no layer matches prefix '{args.layer}'.\n"
                f"Available layers: {list(LAYERS.keys())}",
                file=sys.stderr,
            )
            sys.exit(1)
        layers_to_export = matching

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    started = datetime.now()
    print(f"\nCOEQWAL layer table export")
    print(f"Output:  {output_dir.resolve()}")
    print(f"Started: {started.strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        conn = psycopg2.connect(database_url)
    except psycopg2.OperationalError as exc:
        print(f"Error: could not connect to database.\n  {exc}", file=sys.stderr)
        sys.exit(1)

    conn.set_session(readonly=True)  # Safety — never write during export

    summary: list[dict] = []

    try:
        with conn.cursor() as cur:
            for layer_name, tables in layers_to_export.items():
                print(f"Layer {layer_name}")
                layer_dir = output_dir / layer_name
                for table_name in tables:
                    csv_path = layer_dir / f"{table_name}.csv"
                    row_count = export_table(cur, table_name, csv_path)
                    summary.append(
                        {
                            "layer": layer_name,
                            "table": table_name,
                            "row_count": row_count if row_count is not None else "ERROR",
                            "csv_path": str(csv_path),
                            "exported_at": started.isoformat(),
                        }
                    )
                print()
    finally:
        conn.close()

    # Write summary CSV
    summary_path = output_dir / "summary.csv"
    with open(summary_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["layer", "table", "row_count", "csv_path", "exported_at"]
        )
        writer.writeheader()
        writer.writerows(summary)

    n_ok = sum(1 for r in summary if isinstance(r["row_count"], int))
    n_total = len(summary)
    total_rows = sum(r["row_count"] for r in summary if isinstance(r["row_count"], int))
    elapsed = (datetime.now() - started).total_seconds()

    print("─" * 60)
    print(f"Export complete ({elapsed:.1f}s)")
    print(f"  Tables:     {n_ok}/{n_total} exported")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Summary:    {summary_path}")

    if n_ok < n_total:
        n_err = n_total - n_ok
        print(f"\n  WARNING: {n_err} table(s) had errors or were not found.")
        sys.exit(1)


if __name__ == "__main__":
    main()
