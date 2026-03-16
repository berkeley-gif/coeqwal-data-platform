#!/usr/bin/env python3
"""
COEQWAL monthly database audit.

One command, one report, all the CSVs. Run from Cloud9 with DATABASE_URL set.

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
    ├── schema_snapshot.json            Full schema snapshot
    ├── tables_summary.csv              Per-table row counts + audit field status
    ├── layer_exports/                  Full CSV exports, layers 00-08
    │   ├── 00_versioning/
    │   └── ...
    └── results_samples/                First/last 10 rows, layers 10+
        ├── {table}_head.csv
        └── {table}_tail.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

# verify_erd_against_audit is a sibling module in database/audit/
from verify_erd_against_audit import (
    compare_schemas,
    load_audit_data,
    parse_erd_tables,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_HERE = Path(__file__).resolve().parent
_DB_DIR = _HERE.parent
_REPO_ROOT = _DB_DIR.parent


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA SNAPSHOT
# ═══════════════════════════════════════════════════════════════════════════════

_NON_DOMAIN_TABLES = {
    "spatial_ref_sys",
    "audit_log",
    "developer",
    "domain_family_map",
    "version",
    "version_family",
}


def _rows(cursor) -> list[dict]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _get_all_tables(cursor) -> list[dict]:
    cursor.execute("""
        SELECT schemaname, tablename, tableowner, hasindexes, hasrules, hastriggers
        FROM pg_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, tablename;
    """)
    return _rows(cursor)


def _get_table_structure(cursor, schema: str, table: str) -> list[dict]:
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default,
               character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """, (schema, table))
    return _rows(cursor)


def _get_record_count(cursor, schema: str, table: str) -> int:
    try:
        cursor.execute("SAVEPOINT _count")
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
        result = cursor.fetchone()[0]
        cursor.execute("RELEASE SAVEPOINT _count")
        return result
    except Exception as exc:
        logger.warning("Could not count %s.%s: %s", schema, table, exc)
        cursor.execute("ROLLBACK TO SAVEPOINT _count")
        cursor.execute("RELEASE SAVEPOINT _count")
        return -1


def _get_audit_field_info(cursor, schema: str, table: str, structure: list[dict]) -> dict:
    col_names = {col["column_name"] for col in structure}
    has_created_by = "created_by" in col_names
    has_created_at = "created_at" in col_names
    has_updated_by = "updated_by" in col_names
    has_updated_at = "updated_at" in col_names

    result = {
        "has_created_by": has_created_by,
        "has_created_at": has_created_at,
        "has_updated_by": has_updated_by,
        "has_updated_at": has_updated_at,
        "created_by_values": [],
        "created_at_range": None,
        "sample_records": [],
    }

    if has_created_by:
        try:
            cursor.execute("SAVEPOINT _audit_fields")
            cursor.execute(
                f'SELECT DISTINCT created_by FROM "{schema}"."{table}" '
                f"WHERE created_by IS NOT NULL ORDER BY created_by;"
            )
            result["created_by_values"] = [row[0] for row in cursor.fetchall()]

            if has_created_at:
                cursor.execute(
                    f'SELECT MIN(created_at), MAX(created_at) FROM "{schema}"."{table}" '
                    f"WHERE created_at IS NOT NULL;"
                )
                min_date, max_date = cursor.fetchone()
                if min_date and max_date:
                    result["created_at_range"] = {
                        "min": min_date.isoformat(),
                        "max": max_date.isoformat(),
                    }

            cursor.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT 3;')
            cols = [desc[0] for desc in cursor.description]
            result["sample_records"] = [dict(zip(cols, row)) for row in cursor.fetchall()]
            cursor.execute("RELEASE SAVEPOINT _audit_fields")
        except Exception as exc:
            result["error"] = str(exc)
            cursor.execute("ROLLBACK TO SAVEPOINT _audit_fields")
            cursor.execute("RELEASE SAVEPOINT _audit_fields")

    return result


def _get_indexes(cursor) -> list[dict]:
    cursor.execute("""
        SELECT
            ix.tablename                                              AS table_name,
            ix.indexname                                              AS index_name,
            i.indisunique                                             AS is_unique,
            i.indisprimary                                            AS is_primary,
            string_agg(a.attname, ', ' ORDER BY k.ordinality)        AS columns,
            ix.indexdef                                               AS definition
        FROM pg_indexes                       ix
        JOIN pg_class                         c  ON c.relname   = ix.indexname
        JOIN pg_index                         i  ON i.indexrelid = c.oid
        JOIN pg_class                         t  ON t.oid        = i.indrelid
        JOIN LATERAL unnest(i.indkey)
             WITH ORDINALITY AS k(attnum, ordinality)
                                              ON TRUE
        JOIN pg_attribute                     a  ON a.attrelid   = t.oid
                                                 AND a.attnum    = k.attnum
        WHERE ix.schemaname = 'public'
        GROUP BY ix.tablename, ix.indexname, i.indisunique, i.indisprimary, ix.indexdef
        ORDER BY ix.tablename, ix.indexname;
    """)
    return _rows(cursor)


def _get_foreign_keys(cursor) -> list[dict]:
    cursor.execute("""
        SELECT
            c.conname                    AS constraint_name,
            c.conrelid::regclass::text   AS table_name,
            a.attname                    AS column_name,
            c.confrelid::regclass::text  AS ref_table,
            f.attname                    AS ref_column,
            CASE c.confdeltype
                WHEN 'a' THEN 'NO ACTION'   WHEN 'r' THEN 'RESTRICT'
                WHEN 'c' THEN 'CASCADE'     WHEN 'n' THEN 'SET NULL'
                WHEN 'd' THEN 'SET DEFAULT' END AS delete_rule,
            CASE c.confupdtype
                WHEN 'a' THEN 'NO ACTION'   WHEN 'r' THEN 'RESTRICT'
                WHEN 'c' THEN 'CASCADE'     WHEN 'n' THEN 'SET NULL'
                WHEN 'd' THEN 'SET DEFAULT' END AS update_rule
        FROM pg_constraint c
        JOIN pg_class      t ON t.oid = c.conrelid
        JOIN pg_namespace  n ON n.oid = t.relnamespace
        JOIN pg_attribute  a ON a.attrelid = c.conrelid  AND a.attnum = ANY(c.conkey)
        JOIN pg_attribute  f ON f.attrelid = c.confrelid AND f.attnum = ANY(c.confkey)
        WHERE c.contype = 'f'
          AND n.nspname  = 'public'
        ORDER BY table_name, constraint_name, column_name;
    """)
    return _rows(cursor)


def _get_check_constraints(cursor) -> list[dict]:
    cursor.execute("""
        SELECT tc.table_name, tc.constraint_name, cc.check_clause
        FROM information_schema.table_constraints  tc
        JOIN information_schema.check_constraints  cc
             USING (constraint_name, constraint_schema)
        WHERE tc.constraint_type = 'CHECK'
          AND tc.table_schema    = 'public'
          AND cc.check_clause NOT LIKE '%%IS NOT NULL'
        ORDER BY tc.table_name, tc.constraint_name;
    """)
    return _rows(cursor)


def _get_unique_constraints(cursor) -> list[dict]:
    cursor.execute("""
        SELECT
            tc.table_name,
            tc.constraint_name,
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS columns
        FROM information_schema.table_constraints  tc
        JOIN information_schema.key_column_usage   kcu
             USING (constraint_name, constraint_schema)
        WHERE tc.constraint_type = 'UNIQUE'
          AND tc.table_schema    = 'public'
        GROUP BY tc.table_name, tc.constraint_name
        ORDER BY tc.table_name, tc.constraint_name;
    """)
    return _rows(cursor)


def _get_trigger_details(cursor) -> list[dict]:
    cursor.execute("""
        SELECT
            c.relname                           AS table_name,
            t.tgname                            AS trigger_name,
            CASE WHEN (t.tgtype & 2) > 0
                 THEN 'BEFORE' ELSE 'AFTER' END AS timing,
            ARRAY_REMOVE(ARRAY[
                CASE WHEN (t.tgtype & 4)  > 0 THEN 'INSERT'   END,
                CASE WHEN (t.tgtype & 8)  > 0 THEN 'DELETE'   END,
                CASE WHEN (t.tgtype & 16) > 0 THEN 'UPDATE'   END,
                CASE WHEN (t.tgtype & 32) > 0 THEN 'TRUNCATE' END
            ], NULL)                             AS events,
            p.proname                           AS function_name,
            t.tgenabled != 'D'                  AS is_enabled
        FROM pg_trigger    t
        JOIN pg_class      c ON c.oid = t.tgrelid
        JOIN pg_proc       p ON p.oid = t.tgfoid
        JOIN pg_namespace  n ON n.oid = c.relnamespace
        WHERE n.nspname    = 'public'
          AND NOT t.tgisinternal
        ORDER BY c.relname, t.tgname;
    """)
    rows = []
    cols = [d[0] for d in cursor.description]
    for row in cursor.fetchall():
        d = dict(zip(cols, row))
        if hasattr(d["events"], "__iter__") and not isinstance(d["events"], (str, list)):
            d["events"] = list(d["events"])
        rows.append(d)
    return rows


def _get_functions(cursor) -> list[dict]:
    cursor.execute("""
        SELECT
            p.proname                               AS function_name,
            pg_get_function_arguments(p.oid)        AS argument_types,
            pg_get_function_result(p.oid)           AS return_type,
            l.lanname                               AS language,
            p.prosecdef                             AS security_definer,
            r.rolname                               AS owner
        FROM pg_proc       p
        JOIN pg_namespace  n ON n.oid  = p.pronamespace
        JOIN pg_language   l ON l.oid  = p.prolang
        JOIN pg_roles      r ON r.oid  = p.proowner
        WHERE n.nspname    = 'public'
          AND p.prokind    = 'f'
        ORDER BY p.proname;
    """)
    return _rows(cursor)


def _check_versioning_system(cursor) -> dict:
    versioning_tables = ["version_family", "version", "domain_family_map", "developer"]
    result = {
        "versioning_tables_exist": {},
        "version_families": [],
        "active_versions": [],
        "versions": [],
        "domain_mappings": [],
        "developers": [],
        "validation": {
            "families_without_active_version": [],
            "families_with_multiple_active_versions": [],
            "domain_map_missing_tables": [],
            "domain_map_unexpected_tables": [],
        },
    }

    for table in versioning_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM public.{table};")
            result["versioning_tables_exist"][table] = {
                "exists": True, "count": cursor.fetchone()[0],
            }
        except Exception as exc:
            cursor.connection.rollback()
            result["versioning_tables_exist"][table] = {"exists": False, "error": str(exc)}

    try:
        cursor.execute("SELECT * FROM public.version_family ORDER BY id;")
        result["version_families"] = _rows(cursor)
    except Exception as exc:
        cursor.connection.rollback()
        result["version_families_error"] = str(exc)

    try:
        cursor.execute("SELECT * FROM public.version ORDER BY id;")
        result["versions"] = _rows(cursor)

        active_by_family: dict[int, list] = defaultdict(list)
        for v in result["versions"]:
            if v.get("is_active"):
                active_by_family[v["version_family_id"]].append(v["id"])

        result["active_versions"] = [
            {"version_family_id": fid, "active_version_ids": vids}
            for fid, vids in active_by_family.items()
        ]
        family_ids = {vf["id"] for vf in result["version_families"]}
        result["validation"]["families_without_active_version"] = sorted(
            family_ids - set(active_by_family.keys())
        )
        result["validation"]["families_with_multiple_active_versions"] = [
            {"version_family_id": fid, "active_version_ids": vids}
            for fid, vids in active_by_family.items()
            if len(vids) > 1
        ]
    except Exception as exc:
        cursor.connection.rollback()
        result["versions_error"] = str(exc)

    try:
        cursor.execute(
            "SELECT * FROM public.domain_family_map ORDER BY schema_name, table_name;"
        )
        result["domain_mappings"] = _rows(cursor)

        mapped_tables = {row["table_name"] for row in result["domain_mappings"]} - _NON_DOMAIN_TABLES
        cursor.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' ORDER BY tablename;
        """)
        all_db_tables = {row[0] for row in cursor.fetchall()} - _NON_DOMAIN_TABLES
        result["validation"]["domain_map_missing_tables"] = sorted(all_db_tables - mapped_tables)
        result["validation"]["domain_map_unexpected_tables"] = sorted(mapped_tables - all_db_tables)
    except Exception as exc:
        cursor.connection.rollback()
        result["domain_mappings_error"] = str(exc)

    try:
        cursor.execute("SELECT * FROM public.developer ORDER BY id;")
        result["developers"] = _rows(cursor)
    except Exception as exc:
        cursor.connection.rollback()
        result["developers_error"] = str(exc)

    return result


def generate_schema_snapshot(conn) -> dict:
    """Build a full schema snapshot. Pure Python, no AWS dependencies."""
    cursor = conn.cursor()
    logger.info("Starting schema snapshot...")

    cursor.execute("SELECT current_database(), current_user, session_user, version();")
    db_name, db_user, db_session_user, db_version = cursor.fetchone()

    report = {
        "audit_timestamp": datetime.now().isoformat(),
        "database_info": {
            "database_name": db_name,
            "current_user": db_user,
            "session_user": db_session_user,
            "postgresql_version": db_version,
        },
        "versioning_system": _check_versioning_system(cursor),
        "indexes": [],
        "foreign_keys": [],
        "check_constraints": [],
        "unique_constraints": [],
        "triggers": [],
        "functions": [],
        "tables": [],
        "validation": {
            "tables_missing_audit_trigger": [],
            "tables_attributed_to_system_only": [],
        },
    }

    logger.info("Collecting indexes...")
    report["indexes"] = _get_indexes(cursor)
    logger.info("Collecting foreign keys...")
    report["foreign_keys"] = _get_foreign_keys(cursor)
    logger.info("Collecting CHECK constraints...")
    report["check_constraints"] = _get_check_constraints(cursor)
    logger.info("Collecting UNIQUE constraints...")
    report["unique_constraints"] = _get_unique_constraints(cursor)
    logger.info("Collecting triggers...")
    report["triggers"] = _get_trigger_details(cursor)
    logger.info("Collecting functions...")
    report["functions"] = _get_functions(cursor)

    tables_with_triggers = {t["table_name"] for t in report["triggers"]}  # noqa: F841

    tables = _get_all_tables(cursor)
    logger.info("Auditing %d tables...", len(tables))

    for i, table_info in enumerate(tables, 1):
        schema = table_info["schemaname"]
        table = table_info["tablename"]
        logger.info("  [%d/%d] %s.%s", i, len(tables), schema, table)

        structure = _get_table_structure(cursor, schema, table)
        record_count = _get_record_count(cursor, schema, table)
        audit_fields = _get_audit_field_info(cursor, schema, table, structure)

        has_audit_trigger = any(
            t["function_name"] == "set_audit_fields" and t["table_name"] == table
            for t in report["triggers"]
        )

        report["tables"].append({
            "schema": schema,
            "table": table,
            "owner": table_info["tableowner"],
            "has_indexes": table_info["hasindexes"],
            "has_rules": table_info["hasrules"],
            "has_triggers": table_info["hastriggers"],
            "has_audit_trigger": has_audit_trigger,
            "record_count": record_count,
            "column_count": len(structure),
            "columns": structure,
            "audit_fields": audit_fields,
        })

    report["validation"]["tables_missing_audit_trigger"] = sorted([
        t["table"] for t in report["tables"]
        if t["audit_fields"]["has_created_by"] and not t["has_audit_trigger"]
    ])
    report["validation"]["tables_attributed_to_system_only"] = sorted([
        t["table"] for t in report["tables"]
        if t["audit_fields"]["has_created_by"]
        and t["record_count"] > 0
        and t["audit_fields"]["created_by_values"] == [1]
        and t["table"] != "developer"
    ])

    cursor.close()
    return report


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER TABLE EXPORT — replaces export_layer_tables.py dependency
# ═══════════════════════════════════════════════════════════════════════════════

LAYERS: dict[str, list[str]] = {
    "00_versioning": [
        "developer", "version_family", "version", "domain_family_map", "audit_log",
    ],
    "01_lookup": [
        "hydrologic_region", "source", "model_source", "unit",
        "spatial_scale", "temporal_scale", "statistic_category", "statistic_type",
        "geometry_type",
        "network_entity_type", "network_type", "network_subtype", "watershed",
    ],
    "02_network": [
        "network", "network_arc", "network_node", "network_gis",
    ],
    "03_entity": [
        "reservoir", "compliance_station",
        "du_agriculture_entity", "du_urban_entity", "du_refuge_entity",
        "reservoir_entity", "mi_contractor", "wba",
        "channel_entity",
        "ag_aggregate_entity", "cws_aggregate_entity",
        "du_urban_group", "du_urban_group_member",
        "du_urban_delivery_arc", "mi_contractor_delivery_arc",
        "mi_contractor_group", "mi_contractor_group_member",
        "reservoir_group", "reservoir_group_member",
    ],
    "04_variable": [
        "calsim_model_variable_type", "derived_variable_type", "variable_type",
        "channel_variable", "du_urban_variable",
    ],
    "05_assumptions_operations": [
        "assumption_category", "assumption_definition",
        "operation_category", "operation_definition",
        "scenario_key_assumption_link", "scenario_key_operation_link",
    ],
    "06_scenario": [
        "scenario", "scenario_author",
        "scenario_tag", "scenario_tag_link",
    ],
    "07_hydroclimate": ["hydroclimate", "slr"],
    "08_theme": ["theme", "theme_scenario_link"],
}

RESULTS_TABLES = [
    "tier_definition", "tier_result", "tier_location_result",
    "reservoir_storage_monthly", "reservoir_spill_monthly",
    "reservoir_period_summary", "reservoir_monthly_percentile",
    "du_delivery_monthly", "du_shortage_monthly", "du_period_summary",
    "mi_delivery_monthly", "mi_shortage_monthly", "mi_contractor_period_summary",
    "cws_aggregate_monthly", "cws_aggregate_period_summary",
    "ag_du_demand_monthly", "ag_du_gw_pumping_monthly", "ag_du_sw_delivery_monthly",
    "ag_du_shortage_monthly",
    "ag_du_period_summary",
    "ag_aggregate_monthly", "ag_aggregate_period_summary",
    "refuge_du_delivery_monthly", "refuge_du_shortage_monthly", "refuge_du_period_summary",
    "env_flow_season", "env_flow_channel_monthly",
    "env_flow_channel_seasonal", "env_flow_channel_period_summary",
    "delta_monthly", "delta_period_summary",
]

EXPECTED_COUNTS: dict[str, int | None] = {
    "version_family": 14, "version": 14, "developer": 2,
    "domain_family_map": None, "audit_log": None,
    "hydrologic_region": 7, "source": 12, "model_source": 1,
    "unit": None, "watershed": None,
    "spatial_scale": None, "temporal_scale": None,
    "statistic_category": 3, "statistic_type": 20, "geometry_type": 4,
    "network_entity_type": 4, "network_type": 21, "network_subtype": 28,
    "network": 6908, "network_arc": 2610, "network_node": 1544, "network_gis": 4154,
    "reservoir": 7, "compliance_station": 2, "wba": 42,
    "du_agriculture_entity": 144, "du_urban_entity": 145,
    "du_refuge_entity": 18, "reservoir_entity": 92, "mi_contractor": 30,
    "scenario": None, "scenario_tag": 10, "scenario_tag_link": None,
    "theme": None, "theme_scenario_link": None,
}


def _build_select(cur, table_name: str) -> tuple[str | None, list[str]]:
    """Return (sql, col_names). Geometry columns wrapped in ST_AsText()."""
    cur.execute("""
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = cur.fetchall()
    if not columns:
        return None, []

    parts, col_names = [], []
    for col_name, udt_name in columns:
        if udt_name == "geometry":
            parts.append(f'ST_AsText("{col_name}") AS "{col_name}"')
        else:
            parts.append(f'"{col_name}"')
        col_names.append(col_name)

    sql = f'SELECT {", ".join(parts)} FROM "{table_name}" ORDER BY 1'
    return sql, col_names


def _export_table(cur, table_name: str, output_path: Path) -> int | None:
    """Export one table to CSV. Returns row count or None on failure."""
    sql, col_names = _build_select(cur, table_name)
    if sql is None:
        logger.warning("SKIP  %s (table not found)", table_name)
        return None
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except psycopg2.Error as exc:
        logger.warning("ERROR %s: %s", table_name, exc)
        return None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(col_names)
        writer.writerows(rows)
    return len(rows)


def _export_sample(cur, table_name: str, output_dir: Path, n: int = 10) -> dict:
    """Export first N and last N rows of a table to CSV."""
    sql, col_names = _build_select(cur, table_name)
    if sql is None:
        return {"table": table_name, "head": 0, "tail": 0, "status": "NOT FOUND"}

    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict = {"table": table_name, "status": "OK"}

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


# ═══════════════════════════════════════════════════════════════════════════════
# SQL QUERIES — health, cost, verification
# ═══════════════════════════════════════════════════════════════════════════════

SQL_DB_SIZE = "SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size"

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
SELECT s.relname AS table_name,
       pg_size_pretty(pg_relation_size(c.oid)) AS current_size,
       s.n_dead_tup AS dead_rows, s.n_live_tup AS live_rows,
       CASE WHEN s.n_live_tup + s.n_dead_tup = 0 THEN 0
            ELSE round(s.n_dead_tup * 100.0 / (s.n_live_tup + s.n_dead_tup), 1)
       END AS bloat_pct
FROM pg_stat_user_tables s
JOIN pg_class c ON c.relname = s.relname AND c.relnamespace = 'public'::regnamespace
WHERE s.n_dead_tup > 1000 ORDER BY s.n_dead_tup DESC
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
       (SELECT COUNT(*) FROM reservoir_storage_monthly     WHERE scenario_short_code = s.short_code) AS reservoir,
       (SELECT COUNT(*) FROM du_delivery_monthly           WHERE scenario_short_code = s.short_code) AS du_delivery,
       (SELECT COUNT(*) FROM ag_du_demand_monthly          WHERE scenario_short_code = s.short_code) AS ag_delivery,
       (SELECT COUNT(*) FROM mi_contractor_period_summary  WHERE scenario_short_code = s.short_code) AS mi_summary,
       (SELECT COUNT(*) FROM tier_result                   WHERE scenario_short_code = s.short_code) AS tiers
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
SELECT 'ag_du_demand_monthly',
       COUNT(*) FILTER (WHERE water_month NOT BETWEEN 1 AND 12)
FROM ag_du_demand_monthly
UNION ALL
SELECT 'mi_delivery_monthly',
       COUNT(*) FILTER (WHERE water_month NOT BETWEEN 1 AND 12)
FROM mi_delivery_monthly
"""

SQL_ORPHANED_STATS = """
SELECT 'reservoir_period_summary' AS table_name, COUNT(*) AS orphan_rows
FROM reservoir_period_summary WHERE scenario_short_code NOT IN (SELECT short_code FROM scenario)
UNION ALL SELECT 'mi_contractor_period_summary', COUNT(*)
FROM mi_contractor_period_summary WHERE scenario_short_code NOT IN (SELECT short_code FROM scenario)
UNION ALL SELECT 'ag_aggregate_period_summary', COUNT(*)
FROM ag_aggregate_period_summary WHERE scenario_short_code NOT IN (SELECT short_code FROM scenario)
"""



# ═══════════════════════════════════════════════════════════════════════════════
# REPORT HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

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
        rows = cur.fetchall()
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return [dict(r) for r in rows]
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in rows]
    except psycopg2.Error as exc:
        cur.connection.rollback()
        return [{"error": str(exc)}]


def json_serial(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "__float__") or str(type(obj)) == "<class 'decimal.Decimal'>":
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

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
    findings: dict[str, object] = {}
    conn_rw = None
    audit_report = None

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
        db_size = cur.fetchone()["db_size"]
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
        logger.info("Running schema snapshot...")

        snapshot_conn = psycopg2.connect(database_url)
        try:
            audit_report = generate_schema_snapshot(snapshot_conn)
        finally:
            snapshot_conn.close()

        snapshot_path = run_dir / "schema_snapshot.json"
        snapshot_path.write_text(
            json.dumps(audit_report, indent=2, default=json_serial)
        )
        logger.info("Schema snapshot → %s", snapshot_path)

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
        logger.info("Tables summary  → %s", csv_summary_path)

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

            findings["erd_synced"] = erd_result["is_synchronized"]
            findings["erd_result"] = erd_result
            if erd_result["is_synchronized"]:
                p("**STATUS: ERD is fully synchronized with the live database.**")
            else:
                if erd_result["missing_from_erd"]:
                    p(f"**Tables in DB but missing from ERD:** "
                      f"{', '.join(erd_result['missing_from_erd'])}")
                if erd_result["missing_from_db"]:
                    p(f"**Tables in ERD but NOT in DB:** "
                      f"{', '.join(erd_result['missing_from_db'])}")
                if erd_result["column_mismatches"]:
                    p(f"**Tables with column mismatches:** "
                      f"{len(erd_result['column_mismatches'])}")
                    for m in erd_result["column_mismatches"]:
                        extra = m.get("missing_in_erd", [])
                        missing = m.get("extra_in_erd", [])
                        if extra:
                            p(f"  - `{m['table']}`: in DB but not ERD: {extra}")
                        if missing:
                            p(f"  - `{m['table']}`: in ERD but not DB: {missing}")
                p(f"\n_Correct tables: {erd_result['correct_count']}_")
        else:
            p(f"_ERD file not found at {erd_path}. Skipping comparison._")

        # ── 1c. Row counts vs. expected ───────────────────────────────────
        h("1c. Row counts vs. expected (layers 00-08)", 3)
        exact_count_map = {}
        if audit_report is not None:
            for t in audit_report.get("tables", []):
                exact_count_map[t["table"]] = t.get("record_count", 0)

        check_results = []
        for tbl, expected in sorted(EXPECTED_COUNTS.items()):
            actual = exact_count_map.get(tbl)
            if actual is None:
                check_results.append({
                    "table": tbl, "actual": "NOT FOUND",
                    "expected": str(expected) if expected else "—",
                    "status": "MISSING",
                })
                continue
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
        failed_counts = [r for r in check_results if r["status"].startswith("FAIL")]
        missing_counts = [r for r in check_results if r["status"] == "MISSING"]
        findings["row_count_failures"] = failed_counts
        findings["row_count_missing"] = missing_counts

        # ── 1d. Reference data downloads (layers 00-08) ──────────────────
        h("1d. Reference data downloads (layers 00-08)", 3)
        if conn_rw is None:
            conn_rw = psycopg2.connect(database_url)
            conn_rw.set_session(readonly=True)
        export_dir = run_dir / "layer_exports"
        export_summary = []
        with conn_rw.cursor() as cur:
            for layer_name, tables in LAYERS.items():
                layer_dir = export_dir / layer_name
                for table_name in tables:
                    csv_path = layer_dir / f"{table_name}.csv"
                    row_count = _export_table(cur, table_name, csv_path)
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
                result = _export_sample(cur, tbl, samples_dir)
                sample_results.append(result)
        lines.append(md_table(sample_results, ["table", "head", "tail", "status"]))
        p(f"_CSVs written to `{samples_dir.relative_to(run_dir)}/`._")

    # ══════════════════════════════════════════════════════════════════════
    # 2. DATABASE CONTENT VERIFICATION
    # ══════════════════════════════════════════════════════════════════════
    if "verification" not in args.skip:
        h("2. DATABASE CONTENT VERIFICATION")

        if conn_rw is None:
            conn_rw = psycopg2.connect(database_url)
            conn_rw.set_session(readonly=True)
        with conn_rw.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            null_audit_rows = section(
                "2a. NULL audit fields", cur, SQL_NULL_AUDIT_FIELDS,
                "Rows with created_by = NULL — trigger was not active during insert. "
                "Should return no rows.")
            orphan_rows = section(
                "2a. Orphaned statistics rows", cur, SQL_ORPHANED_STATS,
                "Results rows referencing non-existent scenarios. Should all be 0.")
            invalid_wm_rows = section(
                "2a. Invalid water_month values", cur, SQL_INVALID_WATER_MONTHS,
                "water_month must be 1-12. Non-zero = data integrity error.")

            findings["null_audit"] = null_audit_rows
            findings["orphans"] = orphan_rows
            findings["invalid_water_month"] = invalid_wm_rows

            if audit_report is not None:
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

            coverage_rows = section(
                "2b. Per-scenario ETL coverage", cur, SQL_SCENARIO_COVERAGE,
                "Every active scenario should have non-zero rows in each results table. "
                "Zeros indicate a missed ETL run.")
            gaps = []
            for row in coverage_rows:
                if row.get("is_active") and not row.get("error"):
                    for col in ("reservoir", "du_delivery", "ag_delivery",
                                "mi_summary", "tiers"):
                        if row.get(col, 0) == 0:
                            gaps.append(f"{row.get('short_code', '?')}/{col}")
            findings["etl_coverage_gaps"] = gaps

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

        if conn_rw is None:
            conn_rw = psycopg2.connect(database_url)
            conn_rw.set_session(readonly=True)
        with conn_rw.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cache_rows = section("3a. Cache hit ratio", cur, SQL_CACHE_HIT,
                    "Should be > 99%. Below that = too many disk reads.")
            conn_rows = section("3b. Connection utilization", cur, SQL_CONNECTIONS,
                    "Watch for pct_used > 80%. Many idle = connection leak.")
            dead_rows = section("3c. Dead tuple accumulation", cur, SQL_DEAD_TUPLES,
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

            cache_pct = None
            if cache_rows and not cache_rows[0].get("error"):
                cache_pct = cache_rows[0].get("cache_hit_pct")
            findings["cache_hit_pct"] = float(cache_pct) if cache_pct is not None else None

            conn_pct = None
            if conn_rows and not conn_rows[0].get("error"):
                conn_pct = conn_rows[0].get("pct_used")
            findings["conn_pct_used"] = float(conn_pct) if conn_pct is not None else None

            bloated = [r["table_name"] for r in (dead_rows or [])
                       if not r.get("error") and float(r.get("dead_pct", 0)) > 20]
            findings["bloated_tables"] = bloated

    # ══════════════════════════════════════════════════════════════════════
    # 4. DATABASE COST
    # ══════════════════════════════════════════════════════════════════════
    if "cost" not in args.skip:
        h("4. DATABASE COST")

        if conn_rw is None:
            conn_rw = psycopg2.connect(database_url)
            conn_rw.set_session(readonly=True)
        with conn_rw.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            section("4a. Table sizes (top 25)", cur, SQL_TABLE_SIZES,
                    "Total = data + indexes.")
            unused_idx_rows = section("4b. Unused indexes", cur, SQL_UNUSED_INDEXES,
                    "idx_scan = 0 since last stats reset. PKs and UNIQUE constraints excluded. "
                    "Each unused index adds write overhead with no read benefit.")
            findings["unused_indexes"] = [
                r.get("index_name", "?") for r in (unused_idx_rows or [])
                if not r.get("error")
            ]
            h("4c. Total storage", 3)
            p(f"**Total database size:** {db_size}")

    # ══════════════════════════════════════════════════════════════════════
    # 5. AUDIT SUMMARY
    # ══════════════════════════════════════════════════════════════════════
    h("5. AUDIT SUMMARY")

    def check(key: str, label: str, pass_test, fail_msg_fn=None) -> None:
        val = findings.get(key)
        if val is None:
            p(f"- **—** **{label}**: _skipped_")
        elif pass_test(val):
            p(f"- **PASS** **{label}**")
        else:
            detail = fail_msg_fn(val) if fail_msg_fn else ""
            p(f"- **FAIL** **{label}**{': ' + detail if detail else ''}")

    check("cache_hit_pct", "Health: Cache hit ratio > 99%",
          lambda v: v >= 99.0,
          lambda v: f"currently {v}% — too many disk reads")
    check("conn_pct_used", "Health: Connections below 80% of max",
          lambda v: v < 80.0,
          lambda v: f"currently {v}% — approaching limit")
    check("bloated_tables", "Health: No tables with dead_pct > 20%",
          lambda v: len(v) == 0,
          lambda v: f"run `VACUUM ANALYZE` on: {', '.join(v)}")
    check("unused_indexes", "Cost: No large unused indexes",
          lambda v: len(v) == 0,
          lambda v: f"{len(v)} unused index(es) adding write overhead")
    check("row_count_failures", "Content: All expected row counts pass",
          lambda v: len(v) == 0,
          lambda v: f"{len(v)} table(s) below target: "
                    + ", ".join(r["table"] for r in v))
    check("row_count_missing", "Content: No expected tables missing from DB",
          lambda v: len(v) == 0,
          lambda v: f"{len(v)} table(s) not found: "
                    + ", ".join(r["table"] for r in v))
    check("erd_synced", "Content: ERD synchronized with live schema",
          lambda v: v is True,
          lambda _: "see section 1b for details")
    check("etl_coverage_gaps", "Verification: All active scenarios have ETL coverage",
          lambda v: len(v) == 0,
          lambda v: f"{len(v)} gap(s): " + ", ".join(v[:10])
                    + ("..." if len(v) > 10 else ""))
    check("null_audit", "Verification: Zero NULL audit fields",
          lambda v: len(v) == 0 or all(r.get("error") for r in v),
          lambda v: f"{len(v)} table(s) with NULL created_by")
    check("orphans", "Verification: Zero orphaned statistics rows",
          lambda v: all(r.get("orphan_rows", 0) == 0 for r in v if not r.get("error")),
          lambda v: ", ".join(
              f"{r['table_name']}={r['orphan_rows']}"
              for r in v if r.get("orphan_rows", 0) > 0))
    check("invalid_water_month", "Verification: Zero invalid water_month values",
          lambda v: all(r.get("invalid_count", 0) == 0 for r in v if not r.get("error")),
          lambda v: ", ".join(
              f"{r['table_name']}={r['invalid_count']}"
              for r in v if r.get("invalid_count", 0) > 0))

    # ── Close connections ─────────────────────────────────────────────────
    conn.close()
    if conn_rw is not None:
        conn_rw.close()

    # ── Write report ──────────────────────────────────────────────────────
    elapsed = (datetime.now() - started).total_seconds()
    lines.append("\n---\n")
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
    print("  Exports: layer_exports/ + results_samples/")
    print("=" * 60)


if __name__ == "__main__":
    main()
