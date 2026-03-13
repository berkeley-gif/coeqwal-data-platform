#!/usr/bin/env python3
"""
COEQWAL monthly database audit.

One command, one report, all the CSVs. Can run from Cloud9 with DATABASE_URL set.

Usage
-----
    cd ~/environment/coeqwal-backend
    python database/audit/run_monthly_audit.py

    # Skip a section (health | cost | verification | content)
    python database/audit/run_monthly_audit.py --skip health

    # Custom output directory
    python database/audit/run_monthly_audit.py --output-dir /tmp/audit

Output
------
    audits/monthly_YYYYMMDD_HHMMSS/
    ├── report.md                       Markdown report
    ├── schema_snapshot.json            Full schema snapshot (same format as Lambda)
    ├── tables_summary.csv              Per-table row counts + audit field status
    ├── layer_exports/                  Full CSV exports, layers 00-08
    │   ├── 00_versioning/
    │   └── ...
    └── results_samples/                First/last 10 rows, layers 10+
        ├── {table}_head.csv
        └── {table}_tail.csv
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from textwrap import dedent

import pandas as pd
import psycopg2
import psycopg2.extras

# ── Module imports ──────────────────────────────────
_HERE = Path(__file__).resolve().parent
_DB_DIR = _HERE.parent
_REPO_ROOT = _DB_DIR.parent

sys.path.insert(0, str(_DB_DIR / "utils" / "db_audit_lambda"))
from db_audit_lambda import generate_audit_report  # noqa: E402

sys.path.insert(0, str(_HERE))
from verify_erd_against_audit import (  # noqa: E402
    compare_schemas,
    load_audit_data,
    parse_erd_tables,
)

sys.path.insert(0, str(_DB_DIR / "scripts"))
from export_layer_tables import LAYERS, build_select, export_table  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Expected row counts (layers 00-08) ───────────────────────────────────────
# Lower bounds: check passes if actual >= expected.
# None = report count only, no pass/fail.
EXPECTED_COUNTS: dict[str, int | None] = {
    "version_family": 13,
    "version": 13,
    "developer": 2,
    "domain_family_map": 70,
    "hydrologic_region": 6,
    "network_type": 21,
    "network_subtype": 26,
    "unit": None,
    "source": None,
    "model_source": None,
    "watershed": None,
    "wba": 42,
    "network_entity_type": 4,
    "network": 6908,
    "network_arc": 2610,
    "network_node": 1544,
    "network_gis": 4154,
    "reservoir": 7,
    "compliance_station": 2,
    "du_agriculture_entity": 144,
    "du_urban_entity": 145,
    "du_refuge_entity": 18,
    "reservoir_entity": 92,
    "mi_contractor": 30,
    "scenario": None,
    "theme": None,
    "theme_scenario_link": None,
}


# ── Layer 10+ results tables to sample ────────────────────────────────────────
RESULTS_TABLES = [
    "tier_definition",
    "tier_result",
    "tier_location_result",
    "reservoir_storage_monthly",
    "reservoir_spill_monthly",
    "reservoir_period_summary",
    "reservoir_monthly_percentile",
    "du_delivery_monthly",
    "du_shortage_monthly",
    "du_period_summary",
    "mi_delivery_monthly",
    "mi_shortage_monthly",
    "mi_contractor_period_summary",
    "cws_aggregate_monthly",
    "cws_aggregate_period_summary",
    "ag_du_delivery_monthly",
    "ag_du_shortage_monthly",
    "ag_du_period_summary",
    "ag_aggregate_period_summary",
    "env_flow_season",
    "env_flow_channel_monthly",
    "env_flow_channel_seasonal",
    "env_flow_channel_period_summary",
]


# ── SQL queries ───────────────────────────────────────────────────────────────

SQL_DB_SIZE = "SELECT pg_size_pretty(pg_database_size(current_database()))"

SQL_TABLE_SIZES = """
SELECT relname AS table_name,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size,
       pg_size_pretty(pg_relation_size(oid))       AS data_size,
       pg_size_pretty(pg_total_relation_size(oid) - pg_relation_size(oid)) AS index_size,
       pg_total_relation_size(oid) AS bytes
FROM pg_class
WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace
ORDER BY bytes DESC LIMIT 25
"""

SQL_CACHE_HIT = """
SELECT blks_hit AS cache_hits, blks_read AS disk_reads,
       CASE WHEN blks_hit + blks_read = 0 THEN NULL
            ELSE round(blks_hit * 100.0 / (blks_hit + blks_read), 2)
       END AS cache_hit_pct
FROM pg_stat_database WHERE datname = current_database()
"""

SQL_CONNECTIONS = """
SELECT count(*) AS active_connections,
       (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections,
       round(count(*) * 100.0 /
             (SELECT setting::int FROM pg_settings WHERE name = 'max_connections'), 1) AS pct_used,
       count(*) FILTER (WHERE state = 'idle') AS idle,
       count(*) FILTER (WHERE state = 'active') AS active,
       count(*) FILTER (WHERE wait_event IS NOT NULL) AS waiting
FROM pg_stat_activity WHERE datname = current_database()
"""

SQL_DEAD_TUPLES = """
SELECT relname AS table_name, n_live_tup AS live_rows, n_dead_tup AS dead_rows,
       CASE WHEN n_live_tup + n_dead_tup = 0 THEN 0
            ELSE round(n_dead_tup * 100.0 / (n_live_tup + n_dead_tup), 1)
       END AS dead_pct,
       to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI') AS last_autovacuum,
       to_char(last_autoanalyze, 'YYYY-MM-DD HH24:MI') AS last_autoanalyze
FROM pg_stat_user_tables WHERE n_dead_tup > 100
ORDER BY n_dead_tup DESC LIMIT 15
"""

SQL_BLOAT = """
SELECT relname AS table_name,
       pg_size_pretty(pg_relation_size(oid)) AS current_size,
       n_dead_tup AS dead_rows, n_live_tup AS live_rows,
       CASE WHEN n_live_tup + n_dead_tup = 0 THEN 0
            ELSE round(n_dead_tup * 100.0 / (n_live_tup + n_dead_tup), 1)
       END AS bloat_pct
FROM pg_stat_user_tables
JOIN pg_class ON pg_class.relname = pg_stat_user_tables.relname
WHERE n_dead_tup > 1000 ORDER BY n_dead_tup DESC
"""

SQL_UNUSED_INDEXES = """
SELECT relname AS table_name, indexrelname AS index_name,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       idx_scan AS times_used
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND indexrelname NOT LIKE '%%_pkey'
  AND indexrelname NOT LIKE '%%_key'
ORDER BY pg_relation_size(indexrelid) DESC
"""

SQL_SCENARIO_COVERAGE = """
SELECT s.short_code, s.is_active,
       (SELECT COUNT(*) FROM reservoir_storage_monthly     WHERE scenario_id = s.id) AS reservoir,
       (SELECT COUNT(*) FROM du_delivery_monthly           WHERE scenario_id = s.id) AS du_delivery,
       (SELECT COUNT(*) FROM ag_du_delivery_monthly        WHERE scenario_id = s.id) AS ag_delivery,
       (SELECT COUNT(*) FROM mi_contractor_period_summary  WHERE scenario_id = s.id) AS mi_summary,
       (SELECT COUNT(*) FROM tier_result                   WHERE scenario_id = s.id) AS tiers
FROM scenario s ORDER BY s.short_code
"""

SQL_NULL_AUDIT_FIELDS = """
SELECT table_name, COUNT(*) AS rows_missing_created_by FROM (
    SELECT 'du_urban_entity'        AS table_name, created_by FROM du_urban_entity
    UNION ALL SELECT 'du_agriculture_entity', created_by FROM du_agriculture_entity
    UNION ALL SELECT 'du_refuge_entity',      created_by FROM du_refuge_entity
    UNION ALL SELECT 'reservoir_entity',      created_by FROM reservoir_entity
    UNION ALL SELECT 'mi_contractor',         created_by FROM mi_contractor
    UNION ALL SELECT 'scenario',              created_by FROM scenario
    UNION ALL SELECT 'theme',                 created_by FROM theme
) t WHERE created_by IS NULL GROUP BY table_name HAVING COUNT(*) > 0
"""

SQL_INVALID_WATER_MONTHS = """
SELECT 'du_delivery_monthly' AS table_name,
       COUNT(*) FILTER (WHERE water_month NOT BETWEEN 1 AND 12) AS invalid_count
FROM du_delivery_monthly
UNION ALL
SELECT 'ag_du_delivery_monthly',
       COUNT(*) FILTER (WHERE water_month NOT BETWEEN 1 AND 12)
FROM ag_du_delivery_monthly
"""

SQL_ORPHANED_STATS = """
SELECT 'reservoir_period_summary' AS table_name, COUNT(*) AS orphan_rows
FROM reservoir_period_summary WHERE scenario_id NOT IN (SELECT id FROM scenario)
UNION ALL SELECT 'mi_contractor_period_summary', COUNT(*)
FROM mi_contractor_period_summary WHERE scenario_id NOT IN (SELECT id FROM scenario)
UNION ALL SELECT 'ag_aggregate_period_summary', COUNT(*)
FROM ag_aggregate_period_summary WHERE scenario_id NOT IN (SELECT id FROM scenario)
"""

SQL_ROW_COUNTS = """
SELECT relname AS table_name, reltuples::bigint AS estimated_rows,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size
FROM pg_class
WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace
ORDER BY relname
"""


# ── Markdown helpers ──────────────────────────────────────────────────────────

def md_table(rows: list[dict], columns: list[str] | None = None) -> str:
    if not rows:
        return "_No rows returned._\n"
    cols = columns or list(rows[0].keys())
    header = "| " + " | ".join(str(c) for c in cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    body = "\n".join(
        "| " + " | ".join(str(r.get(c, "")) for c in cols) + " |"
        for r in rows
    )
    return f"{header}\n{sep}\n{body}\n"


def run_query(cur, sql: str) -> list[dict]:
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        return [{"error": str(exc)}]


def json_serial(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "__float__") or str(type(obj)) == "<class 'decimal.Decimal'>":
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


# ── Sample export (head/tail for results tables) ─────────────────────────────

def export_sample(cur, table_name: str, output_dir: Path, n: int = 10) -> dict:
    """Export first N and last N rows of a table to CSV. Returns row counts."""
    sql, col_names = build_select(cur, table_name)
    if sql is None:
        return {"table": table_name, "head": 0, "tail": 0, "status": "NOT FOUND"}

    output_dir.mkdir(parents=True, exist_ok=True)
    counts = {"table": table_name, "status": "OK"}

    for suffix, order in [("head", "ASC"), ("tail", "DESC")]:
        sample_sql = sql.replace("ORDER BY 1", f"ORDER BY 1 {order}") + f" LIMIT {n}"
        try:
            cur.execute(sample_sql)
            rows = cur.fetchall()
        except psycopg2.Error:
            counts[suffix] = 0
            continue

        csv_path = output_dir / f"{table_name}_{suffix}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(col_names)
            writer.writerows(rows)
        counts[suffix] = len(rows)

    return counts


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the COEQWAL monthly database audit.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output-dir", default="audits",
        help="Parent directory for output (default: audits/)",
    )
    parser.add_argument(
        "--skip", metavar="SECTION", action="append", default=[],
        help="Skip a section: content | verification | health | cost (repeatable)",
    )
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print(
            'Error: DATABASE_URL is not set.\n'
            '  export DATABASE_URL="postgresql://user:pass@host:5432/coeqwal_scenario"',
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        conn = psycopg2.connect(database_url)
    except psycopg2.OperationalError as exc:
        print(f"Error: could not connect — {exc}", file=sys.stderr)
        sys.exit(1)

    started = datetime.now()
    ts = started.strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.output_dir) / f"monthly_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    def h(text: str, level: int = 2) -> None:
        lines.append(f"\n{'#' * level} {text}\n")
    def p(text: str) -> None:
        lines.append(text + "\n")
    def section(title: str, cur, sql: str, note: str = "") -> list[dict]:
        rows = run_query(cur, sql)
        h(title, 3)
        if note:
            p(f"_{note}_")
        lines.append(md_table(rows))
        return rows

    # ── Header ────────────────────────────────────────────────────────────
    lines.append("# COEQWAL Monthly Database Audit\n")
    lines.append(f"**Generated:** {started.strftime('%Y-%m-%d %H:%M:%S')}  \n")

    conn.set_session(readonly=True)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT current_database() AS db, current_user AS usr, "
            "version() AS ver"
        )
        info = cur.fetchone()
        cur.execute(SQL_DB_SIZE)
        db_size = cur.fetchone()[0]
        p(
            f"**Database:** {info['db']}  \n"
            f"**Connected as:** {info['usr']}  \n"
            f"**PostgreSQL:** {str(info['ver'])[:60]}  \n"
            f"**Total DB size:** {db_size}  \n"
        )

    # ══════════════════════════════════════════════════════════════════════
    # 1. DATABASE CONTENT AUDIT
    # ══════════════════════════════════════════════════════════════════════
    if "content" not in args.skip:
        h("1. DATABASE CONTENT AUDIT")

        # ── 1a. Schema snapshot + table inventory ─────────────────────────
        h("1a. Table inventory", 3)
        logger.info("Running schema snapshot (same logic as Lambda)...")

        snapshot_conn = psycopg2.connect(database_url)
        try:
            audit_report = generate_audit_report(snapshot_conn)
        finally:
            snapshot_conn.close()

        snapshot_path = run_dir / "schema_snapshot.json"
        snapshot_path.write_text(
            json.dumps(audit_report, indent=2, default=json_serial)
        )
        logger.info(f"Schema snapshot → {snapshot_path}")

        csv_summary_path = run_dir / "tables_summary.csv"
        summary_data = [
            {
                "table": t["table"],
                "schema": t["schema"],
                "records": t["record_count"],
                "columns": t["column_count"],
                "has_audit_trigger": t["has_audit_trigger"],
                "created_by_values": ",".join(
                    map(str, t["audit_fields"]["created_by_values"])
                ),
            }
            for t in audit_report["tables"]
        ]
        pd.DataFrame(summary_data).to_csv(csv_summary_path, index=False)
        logger.info(f"Tables summary  → {csv_summary_path}")

        # Build layer-grouped inventory for the report
        layer_assignment = {}
        for layer_name, tables in LAYERS.items():
            for tbl in tables:
                layer_assignment[tbl] = layer_name
        for tbl in RESULTS_TABLES:
            layer_assignment[tbl] = "10+_results"

        inventory = []
        for t in sorted(audit_report["tables"], key=lambda x: x["table"]):
            inventory.append({
                "table": t["table"],
                "layer": layer_assignment.get(t["table"], "other"),
                "columns": t["column_count"],
                "rows": f'{t["record_count"]:,}',
                "audit_trigger": "yes" if t["has_audit_trigger"] else "no",
            })
        lines.append(md_table(inventory, ["table", "layer", "columns", "rows", "audit_trigger"]))
        p(f"_Schema snapshot saved to `{snapshot_path.name}`. "
          f"Tables summary saved to `{csv_summary_path.name}`._")

        # ── 1b. Schema vs. ERD comparison ─────────────────────────────────
        h("1b. Schema vs. ERD comparison", 3)
        erd_path = _DB_DIR / "schema" / "COEQWAL_SCENARIOS_DB_ERD.md"
        if erd_path.exists():
            erd_tables = parse_erd_tables(erd_path)
            audit_data = load_audit_data(snapshot_path)
            erd_result = compare_schemas(erd_tables, audit_data, quiet=True)

            if erd_result["is_synchronized"]:
                p("**STATUS: ERD is fully synchronized with the live database.**")
            else:
                if erd_result["missing_from_erd"]:
                    p(f"**Tables in DB but missing from ERD:** "
                      f"{', '.join(erd_result['missing_from_erd'])}")
                if erd_result["missing_from_db"]:
                    planned = [t for t, info in erd_result["missing_from_db"].items()
                               if info.get("is_planned")]
                    real_missing = [t for t in erd_result["missing_from_db"]
                                    if t not in (planned or [])]
                    if planned:
                        p(f"**Tables in ERD marked PLANNED (not yet built):** "
                          f"{', '.join(planned)}")
                    if real_missing:
                        p(f"**Tables in ERD but NOT in DB (unexpected):** "
                          f"{', '.join(real_missing)}")
                if erd_result["column_mismatches"]:
                    p(f"**Tables with column mismatches:** "
                      f"{len(erd_result['column_mismatches'])}")
                    for tbl, info in sorted(erd_result["column_mismatches"].items()):
                        extra = info.get("extra_in_db", [])
                        missing = info.get("missing_from_db", [])
                        if extra:
                            p(f"  - `{tbl}`: extra in DB: {extra}")
                        if missing:
                            p(f"  - `{tbl}`: missing from DB: {missing}")
                p(f"\n_Correct tables: {erd_result['correct_count']}_")
        else:
            p(f"_ERD file not found at {erd_path}. Skipping comparison._")

        # ── 1c. Row counts vs. expected ───────────────────────────────────
        h("1c. Row counts vs. expected (layers 00-08)", 3)
        conn_rw = psycopg2.connect(database_url)
        conn_rw.set_session(readonly=True)
        with conn_rw.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            count_rows = run_query(cur, SQL_ROW_COUNTS)
        count_map = {r["table_name"]: r for r in count_rows}

        check_results = []
        for tbl, expected in sorted(EXPECTED_COUNTS.items()):
            info = count_map.get(tbl)
            if info is None:
                check_results.append({
                    "table": tbl, "actual": "NOT FOUND",
                    "expected": str(expected) if expected else "—",
                    "status": "MISSING",
                })
                continue
            actual = info["estimated_rows"]
            if expected is None:
                status = "OK (no target)"
            elif actual >= expected:
                status = "PASS"
            else:
                status = f"FAIL (short by {expected - actual})"
            check_results.append({
                "table": tbl, "actual": f"{actual:,}",
                "expected": f"{expected:,}" if expected else "—",
                "status": status,
            })
        lines.append(md_table(check_results, ["table", "actual", "expected", "status"]))

        # ── 1d. Reference data downloads (layers 00-08) ──────────────────
        h("1d. Reference data downloads (layers 00-08)", 3)
        export_dir = run_dir / "layer_exports"
        export_summary = []
        with conn_rw.cursor() as cur:
            for layer_name, tables in LAYERS.items():
                layer_dir = export_dir / layer_name
                for table_name in tables:
                    csv_path = layer_dir / f"{table_name}.csv"
                    row_count = export_table(cur, table_name, csv_path)
                    export_summary.append({
                        "layer": layer_name, "table": table_name,
                        "rows": row_count if row_count is not None else "ERROR",
                    })
        lines.append(md_table(export_summary, ["layer", "table", "rows"]))
        p(f"_CSVs written to `{export_dir.relative_to(run_dir)}/`._")

        # ── 1e. Results data samples (layers 10+) ────────────────────────
        h("1e. Results data samples (layers 10+)", 3)
        p("_First 10 and last 10 rows per table._")
        samples_dir = run_dir / "results_samples"
        sample_results = []
        with conn_rw.cursor() as cur:
            for tbl in RESULTS_TABLES:
                result = export_sample(cur, tbl, samples_dir)
                sample_results.append(result)
        lines.append(md_table(sample_results, ["table", "head", "tail", "status"]))
        p(f"_CSVs written to `{samples_dir.relative_to(run_dir)}/`._")

    # ══════════════════════════════════════════════════════════════════════
    # 2. DATABASE CONTENT VERIFICATION
    # ══════════════════════════════════════════════════════════════════════
    if "verification" not in args.skip:
        h("2. DATABASE CONTENT VERIFICATION")

        if "conn_rw" not in dir():
            conn_rw = psycopg2.connect(database_url)
            conn_rw.set_session(readonly=True)
        with conn_rw.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # ── 2a. Data integrity checks ─────────────────────────────────
            section("2a. NULL audit fields", cur, SQL_NULL_AUDIT_FIELDS,
                    "Rows with created_by = NULL — trigger was not active during insert. "
                    "Should return no rows.")
            section("2a. Orphaned statistics rows", cur, SQL_ORPHANED_STATS,
                    "Results rows referencing non-existent scenarios. Should all be 0.")
            section("2a. Invalid water_month values", cur, SQL_INVALID_WATER_MONTHS,
                    "water_month must be 1-12. Non-zero = data integrity error.")

            # Schema snapshot validation (from section 1a if available)
            if "audit_report" in dir():
                val = audit_report.get("validation", {})
                missing_trigger = val.get("tables_missing_audit_trigger", [])
                system_only = val.get("tables_attributed_to_system_only", [])
                if missing_trigger:
                    h("2a. Tables missing audit trigger", 3)
                    p(f"These tables have audit columns but no `set_audit_fields()` trigger: "
                      f"{', '.join(missing_trigger)}")
                if system_only:
                    h("2a. Tables attributed to system account only", 3)
                    p(f"Every row has `created_by = 1` (system). Likely mis-attributed bulk loads: "
                      f"{', '.join(system_only)}")

            # ── 2b. Per-scenario ETL coverage ─────────────────────────────
            section("2b. Per-scenario ETL coverage", cur, SQL_SCENARIO_COVERAGE,
                    "Every active scenario should have non-zero rows in each results table. "
                    "Zeros indicate a missed ETL run.")

        # ── 2c. ETL accuracy status summary ───────────────────────────────
        h("2c. ETL accuracy status summary", 3)
        reports_dir = _REPO_ROOT / "audits" / "verification_reports"
        if reports_dir.exists():
            report_files = sorted(reports_dir.glob("*_layer*.json"))
            if report_files:
                accuracy_summary = []
                for rf in report_files:
                    try:
                        data = json.loads(rf.read_text())
                        total = data.get("total_checks", 0)
                        passed = data.get("passed", data.get("pass", 0))
                        failed = data.get("failed", data.get("fail", 0))
                        skipped = data.get("skipped", data.get("skip", 0))
                        accuracy_summary.append({
                            "report": rf.name,
                            "total": total, "pass": passed,
                            "fail": failed, "skip": skipped,
                        })
                    except (json.JSONDecodeError, KeyError):
                        accuracy_summary.append({
                            "report": rf.name, "total": "?",
                            "pass": "?", "fail": "?", "skip": "parse error",
                        })
                lines.append(md_table(accuracy_summary,
                                      ["report", "total", "pass", "fail", "skip"]))
            else:
                p("_No verification reports found. Run `verify_all_sections.py` to generate them._")
        else:
            p(f"_Reports directory not found at `{reports_dir}`. "
              f"Run `verify_all_sections.py` to generate verification reports._")

    # ══════════════════════════════════════════════════════════════════════
    # 3. DATABASE HEALTH
    # ══════════════════════════════════════════════════════════════════════
    if "health" not in args.skip:
        h("3. DATABASE HEALTH")

        if "conn_rw" not in dir():
            conn_rw = psycopg2.connect(database_url)
            conn_rw.set_session(readonly=True)
        with conn_rw.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            section("3a. Cache hit ratio", cur, SQL_CACHE_HIT,
                    "Should be > 99%. Below that = too many disk reads.")
            section("3b. Connection utilization", cur, SQL_CONNECTIONS,
                    "Watch for pct_used > 80%. Many idle = connection leak.")
            section("3c. Dead tuple accumulation", cur, SQL_DEAD_TUPLES,
                    "Dead tuples are old row versions left by UPDATE/DELETE. "
                    "High counts after ETL = autovacuum is behind.")
            p(
                "> **What is a dead tuple?** PostgreSQL never overwrites a row in-place. "
                "UPDATE/DELETE marks the old version 'dead'; it stays on disk until VACUUM "
                "reclaims it. High dead_pct wastes storage and slows scans. Run "
                "`VACUUM ANALYZE <table>` after large ETL loads if autovacuum hasn't caught up."
            )
            section("3d. Table bloat estimate", cur, SQL_BLOAT,
                    "bloat_pct > 20% = significant wasted space. VACUUM ANALYZE recommended.")

    # ══════════════════════════════════════════════════════════════════════
    # 4. DATABASE COST
    # ══════════════════════════════════════════════════════════════════════
    if "cost" not in args.skip:
        h("4. DATABASE COST")

        if "conn_rw" not in dir():
            conn_rw = psycopg2.connect(database_url)
            conn_rw.set_session(readonly=True)
        with conn_rw.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            section("4a. Table sizes (top 25)", cur, SQL_TABLE_SIZES,
                    "Total = data + indexes.")
            section("4b. Unused indexes", cur, SQL_UNUSED_INDEXES,
                    "idx_scan = 0 since last stats reset. PKs and UNIQUE constraints excluded. "
                    "Each unused index adds write overhead with no read benefit.")
            h("4c. Total storage", 3)
            p(f"**Total database size:** {db_size}")

    # ══════════════════════════════════════════════════════════════════════
    # 5. NEXT STEPS CHECKLIST
    # ══════════════════════════════════════════════════════════════════════
    h("5. NEXT STEPS CHECKLIST")
    lines.append(dedent("""\
    - [ ] **Health**: Cache hit ratio > 99%?
    - [ ] **Health**: No connections approaching max_connections?
    - [ ] **Health**: Any tables with dead_pct > 20%? Run `VACUUM ANALYZE <table>`
    - [ ] **Cost**: Any large unused indexes worth dropping?
    - [ ] **Content**: All expected row counts PASS?
    - [ ] **Content**: ERD fully synchronized with live schema?
    - [ ] **Verification**: All active scenarios have ETL coverage (non-zero rows)?
    - [ ] **Verification**: Zero NULL audit fields?
    - [ ] **Verification**: Zero orphaned statistics rows?
    - [ ] **Verification**: Any scenarios never verified? Run `verify_all_sections.py`
    """))

    # ── Close connections ─────────────────────────────────────────────────
    conn.close()
    if "conn_rw" in dir():
        conn_rw.close()

    # ── Write report ──────────────────────────────────────────────────────
    elapsed = (datetime.now() - started).total_seconds()
    lines.append(f"\n---\n")
    lines.append(
        f"_Report generated in {elapsed:.1f}s by "
        f"`database/audit/run_monthly_audit.py`_\n"
    )

    report_path = run_dir / "report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")

    print()
    print("=" * 60)
    print("MONTHLY AUDIT COMPLETE")
    print("=" * 60)
    print(f"  Time:    {elapsed:.1f}s")
    print(f"  Output:  {run_dir.resolve()}")
    print(f"  Report:  {report_path.name}")
    print(f"  Snapshot:{(run_dir / 'schema_snapshot.json').name}")
    print(f"  Exports: layer_exports/ + results_samples/")
    print("=" * 60)


if __name__ == "__main__":
    main()
