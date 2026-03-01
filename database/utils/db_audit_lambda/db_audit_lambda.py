#!/usr/bin/env python3
"""
AWS Lambda database audit function
==================================
PostgreSQL database audit that saves results to S3.

Captures per-table: columns, record counts, audit field presence, sample rows.
Captures database-wide: indexes, foreign keys, CHECK constraints, unique constraints,
and versioning system completeness.
"""

import json
import boto3
import psycopg2
import pandas as pd
from datetime import datetime
import os
from typing import Dict, List, Any
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_database_connection():
    """Get database connection from environment variables."""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    try:
        conn = psycopg2.connect(database_url)
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


# ---------------------------------------------------------------------------
# Per-table queries
# ---------------------------------------------------------------------------

def get_all_tables(cursor) -> List[Dict[str, Any]]:
    """Return all user tables with basic metadata from pg_tables."""
    cursor.execute("""
        SELECT schemaname, tablename, tableowner, hasindexes, hasrules, hastriggers
        FROM pg_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, tablename;
    """)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_table_structure(cursor, schema: str, table: str) -> List[Dict[str, Any]]:
    """Return ordered column definitions for a table."""
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default,
               character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """, (schema, table))
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_record_count(cursor, schema: str, table: str) -> int:
    """Return exact row count; -1 on error. Uses a savepoint so a permission
    error on one table does not abort the surrounding transaction."""
    try:
        cursor.execute("SAVEPOINT _count")
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
        result = cursor.fetchone()[0]
        cursor.execute("RELEASE SAVEPOINT _count")
        return result
    except Exception as e:
        logger.warning(f"Could not count {schema}.{table}: {e}")
        cursor.execute("ROLLBACK TO SAVEPOINT _count")
        cursor.execute("RELEASE SAVEPOINT _count")
        return -1


def get_audit_field_info(cursor, schema: str, table: str, structure: List[Dict]) -> Dict[str, Any]:
    """
    Check whether the standard audit columns exist and are populated.
    Returns presence flags, distinct created_by values, created_at range, and 3 sample rows.
    """
    col_names = {col['column_name'] for col in structure}
    has_created_by = 'created_by' in col_names
    has_created_at = 'created_at' in col_names
    has_updated_by = 'updated_by' in col_names
    has_updated_at = 'updated_at' in col_names

    result = {
        'has_created_by': has_created_by,
        'has_created_at': has_created_at,
        'has_updated_by': has_updated_by,
        'has_updated_at': has_updated_at,
        'created_by_values': [],
        'created_at_range': None,
        'sample_records': [],
    }

    if has_created_by:
        try:
            cursor.execute("SAVEPOINT _audit_fields")
            cursor.execute(
                f'SELECT DISTINCT created_by FROM "{schema}"."{table}" '
                f'WHERE created_by IS NOT NULL ORDER BY created_by;'
            )
            result['created_by_values'] = [row[0] for row in cursor.fetchall()]

            if has_created_at:
                cursor.execute(
                    f'SELECT MIN(created_at), MAX(created_at) FROM "{schema}"."{table}" '
                    f'WHERE created_at IS NOT NULL;'
                )
                min_date, max_date = cursor.fetchone()
                if min_date and max_date:
                    result['created_at_range'] = {
                        'min': min_date.isoformat(),
                        'max': max_date.isoformat(),
                    }

            cursor.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT 3;')
            cols = [desc[0] for desc in cursor.description]
            result['sample_records'] = [dict(zip(cols, row)) for row in cursor.fetchall()]
            cursor.execute("RELEASE SAVEPOINT _audit_fields")

        except Exception as e:
            result['error'] = str(e)
            cursor.execute("ROLLBACK TO SAVEPOINT _audit_fields")
            cursor.execute("RELEASE SAVEPOINT _audit_fields")

    return result


# ---------------------------------------------------------------------------
# Database-wide structural queries (NEW)
# ---------------------------------------------------------------------------

def get_indexes(cursor) -> List[Dict[str, Any]]:
    """
    Return structured index data for all user tables.
    Captures is_unique and column list separately so consumers can do
    programmatic comparisons without parsing the definition string.
    """
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
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_foreign_keys(cursor) -> List[Dict[str, Any]]:
    """
    Return all foreign key constraints with source column and referenced table/column.

    Uses pg_constraint instead of information_schema.constraint_column_usage.
    The information_schema view filters results by table ownership, so non-superuser
    connections return 0 rows for tables they don't own. pg_constraint is readable
    by any user with CONNECT privilege.
    """
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
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_check_constraints(cursor) -> List[Dict[str, Any]]:
    """
    Return CHECK constraints, excluding the auto-generated NOT NULL checks
    that PostgreSQL creates internally.
    """
    cursor.execute("""
        SELECT tc.table_name, tc.constraint_name, cc.check_clause
        FROM information_schema.table_constraints  tc
        JOIN information_schema.check_constraints  cc
             USING (constraint_name, constraint_schema)
        WHERE tc.constraint_type = 'CHECK'
          AND tc.table_schema    = 'public'
          AND cc.check_clause NOT LIKE '%IS NOT NULL'
        ORDER BY tc.table_name, tc.constraint_name;
    """)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_unique_constraints(cursor) -> List[Dict[str, Any]]:
    """
    Return named UNIQUE constraints with their constituent columns.
    Note: unique indexes created via CREATE UNIQUE INDEX appear in pg_indexes, not here.
    """
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
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Trigger and function inventory
# ---------------------------------------------------------------------------

def get_trigger_details(cursor) -> List[Dict[str, Any]]:
    """
    Return per-trigger detail for all user tables.
    Replaces the per-table has_triggers boolean with actionable information:
    trigger name, timing (BEFORE/AFTER), events (INSERT/UPDATE/DELETE), and
    the function it calls.

    tgtype is a bitmask: bit 1=ROW, bit 2=BEFORE, bit 3=INSERT, bit 4=DELETE,
    bit 5=UPDATE, bit 6=TRUNCATE.
    """
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
    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        d = dict(zip(columns, row))
        # Convert PostgreSQL array to Python list if needed
        if hasattr(d['events'], '__iter__') and not isinstance(d['events'], (str, list)):
            d['events'] = list(d['events'])
        rows.append(d)
    return rows


def get_functions(cursor) -> List[Dict[str, Any]]:
    """
    Inventory all user-defined functions (excludes built-ins and aggregates).
    Captures security_definer flag — a SECURITY DEFINER function runs as its
    owner, not the caller, which affects session_user vs current_user behaviour
    (see coeqwal_current_operator).
    """
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
          AND p.prokind    = 'f'          -- functions only (not aggregates/procedures)
        ORDER BY p.proname;
    """)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Versioning system check
# ---------------------------------------------------------------------------

# Infrastructure/system tables that are not domain data tables and should not
# be required in domain_family_map. Excluded from the "missing from map" check.
_NON_DOMAIN_TABLES = {
    'spatial_ref_sys',         # PostGIS extension table
    'audit_log',               # Audit infrastructure
    'developer',               # Versioning infrastructure
    'domain_family_map',       # Self-referential
    'version',                 # Versioning infrastructure
    'version_family',          # Versioning infrastructure
}


def check_versioning_system(cursor) -> Dict[str, Any]:
    """
    Audit the versioning system tables:
    - Existence and row counts of the four core versioning tables
    - Full contents of version_family, version, domain_family_map, developer
    - Validation: each version family should have exactly one active version
    - Validation: domain_family_map should cover all expected domain tables
    """
    versioning_tables = ['version_family', 'version', 'domain_family_map', 'developer']

    result = {
        'versioning_tables_exist': {},
        'version_families': [],
        'active_versions': [],
        'versions': [],
        'domain_mappings': [],
        'developers': [],
        'validation': {
            'families_without_active_version': [],
            'families_with_multiple_active_versions': [],
            'domain_map_missing_tables': [],
            'domain_map_unexpected_tables': [],
        },
    }

    for table in versioning_tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM public.{table};')
            result['versioning_tables_exist'][table] = {
                'exists': True,
                'count': cursor.fetchone()[0],
            }
        except Exception as e:
            result['versioning_tables_exist'][table] = {'exists': False, 'error': str(e)}

    try:
        cursor.execute('SELECT * FROM public.version_family ORDER BY id;')
        cols = [desc[0] for desc in cursor.description]
        result['version_families'] = [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        result['version_families_error'] = str(e)

    try:
        cursor.execute('SELECT * FROM public.version ORDER BY id;')
        cols = [desc[0] for desc in cursor.description]
        result['versions'] = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # Validate: exactly one active version per family
        from collections import defaultdict
        active_by_family: Dict[int, list] = defaultdict(list)
        for v in result['versions']:
            if v.get('is_active'):
                active_by_family[v['version_family_id']].append(v['id'])

        result['active_versions'] = [
            {'version_family_id': fid, 'active_version_ids': vids}
            for fid, vids in active_by_family.items()
        ]

        family_ids = {vf['id'] for vf in result['version_families']}
        result['validation']['families_without_active_version'] = sorted(
            family_ids - set(active_by_family.keys())
        )
        result['validation']['families_with_multiple_active_versions'] = [
            {'version_family_id': fid, 'active_version_ids': vids}
            for fid, vids in active_by_family.items()
            if len(vids) > 1
        ]
    except Exception as e:
        result['versions_error'] = str(e)

    try:
        cursor.execute(
            'SELECT * FROM public.domain_family_map ORDER BY schema_name, table_name;'
        )
        cols = [desc[0] for desc in cursor.description]
        result['domain_mappings'] = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # Validate dynamically against actual DB tables:
        # - "missing": tables that exist in the DB but have no domain_family_map entry
        # - "phantom": entries in domain_family_map for tables that don't exist in the DB
        # Infrastructure tables are excluded from both sides of the comparison.
        mapped_tables = {row['table_name'] for row in result['domain_mappings']} - _NON_DOMAIN_TABLES
        cursor.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """)
        all_db_tables = {row[0] for row in cursor.fetchall()} - _NON_DOMAIN_TABLES
        result['validation']['domain_map_missing_tables'] = sorted(
            all_db_tables - mapped_tables
        )
        result['validation']['domain_map_unexpected_tables'] = sorted(
            mapped_tables - all_db_tables
        )
    except Exception as e:
        result['domain_mappings_error'] = str(e)

    try:
        cursor.execute('SELECT * FROM public.developer ORDER BY id;')
        cols = [desc[0] for desc in cursor.description]
        result['developers'] = [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        result['developers_error'] = str(e)

    return result


# ---------------------------------------------------------------------------
# Main audit report builder
# ---------------------------------------------------------------------------

def generate_audit_report(conn) -> Dict[str, Any]:
    """Build the full audit report. Called by the Lambda handler and the local runner."""
    cursor = conn.cursor()
    logger.info("Starting comprehensive database audit...")

    # Capture both current_user and session_user.
    # current_user changes inside SECURITY DEFINER functions (returns function owner),
    # so session_user is the reliable record of who actually ran the audit.
    cursor.execute("SELECT current_database(), current_user, session_user, version();")
    db_name, db_user, db_session_user, db_version = cursor.fetchone()

    audit_report = {
        'audit_timestamp': datetime.now().isoformat(),
        'database_info': {
            'database_name': db_name,
            'current_user': db_user,
            'session_user': db_session_user,
            'postgresql_version': db_version,
        },
        'versioning_system': check_versioning_system(cursor),
        'indexes': [],
        'foreign_keys': [],
        'check_constraints': [],
        'unique_constraints': [],
        'triggers': [],
        'functions': [],
        'tables': [],
        'validation': {
            'tables_missing_audit_trigger': [],
            'tables_attributed_to_system_only': [],
        },
    }

    # Database-wide structural data
    logger.info("Collecting indexes...")
    audit_report['indexes'] = get_indexes(cursor)

    logger.info("Collecting foreign key constraints...")
    audit_report['foreign_keys'] = get_foreign_keys(cursor)

    logger.info("Collecting CHECK constraints...")
    audit_report['check_constraints'] = get_check_constraints(cursor)

    logger.info("Collecting UNIQUE constraints...")
    audit_report['unique_constraints'] = get_unique_constraints(cursor)

    logger.info("Collecting trigger details...")
    audit_report['triggers'] = get_trigger_details(cursor)

    logger.info("Collecting function inventory...")
    audit_report['functions'] = get_functions(cursor)

    # Build a set of table names that have at least one trigger for fast lookup
    tables_with_triggers = {t['table_name'] for t in audit_report['triggers']}

    # Per-table detail
    tables = get_all_tables(cursor)
    logger.info(f"Auditing {len(tables)} tables...")

    for i, table_info in enumerate(tables, 1):
        schema = table_info['schemaname']
        table = table_info['tablename']
        logger.info(f"  [{i}/{len(tables)}] {schema}.{table}")

        structure = get_table_structure(cursor, schema, table)
        record_count = get_record_count(cursor, schema, table)
        audit_fields = get_audit_field_info(cursor, schema, table, structure)

        # Derive has_audit_trigger from the detailed trigger list rather than
        # the coarse pg_tables boolean, which is true for ANY trigger.
        has_audit_trigger = any(
            t['function_name'] == 'set_audit_fields' and t['table_name'] == table
            for t in audit_report['triggers']
        )

        audit_report['tables'].append({
            'schema': schema,
            'table': table,
            'owner': table_info['tableowner'],
            'has_indexes': table_info['hasindexes'],
            'has_rules': table_info['hasrules'],
            'has_triggers': table_info['hastriggers'],
            'has_audit_trigger': has_audit_trigger,
            'record_count': record_count,
            'column_count': len(structure),
            'columns': structure,
            'audit_fields': audit_fields,
        })

    # ---------------------------------------------------------------------------
    # Cross-table validation
    # ---------------------------------------------------------------------------

    # 1. Tables that have audit columns but no set_audit_fields() trigger applied.
    #    These rows will not get automatic created_by/updated_by population.
    audit_report['validation']['tables_missing_audit_trigger'] = sorted([
        t['table'] for t in audit_report['tables']
        if t['audit_fields']['has_created_by']
        and not t['has_audit_trigger']
    ])

    # 2. Tables where every row is attributed to developer id=1 (system account)
    #    and the table is non-empty. These are likely mis-attributed.
    #    Excludes the developer table itself (system user legitimately has id=1 there).
    audit_report['validation']['tables_attributed_to_system_only'] = sorted([
        t['table'] for t in audit_report['tables']
        if t['audit_fields']['has_created_by']
        and t['record_count'] > 0
        and t['audit_fields']['created_by_values'] == [1]
        and t['table'] != 'developer'
    ])

    cursor.close()
    return audit_report


# ---------------------------------------------------------------------------
# S3 upload helper
# ---------------------------------------------------------------------------

def upload_to_s3(content: str, key: str, bucket: str, content_type: str = 'application/json'):
    """Upload string content to S3."""
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=content, ContentType=content_type)
        logger.info(f"Uploaded to s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """Main Lambda handler — runs audit and writes JSON + CSV summary to S3."""
    try:
        bucket = event.get('bucket', os.environ.get('S3_BUCKET'))
        if not bucket:
            raise ValueError("S3_BUCKET not specified in environment or event")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        conn = get_database_connection()

        try:
            audit_report = generate_audit_report(conn)

            def json_serial(obj):
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                if hasattr(obj, '__float__') or str(type(obj)) == "<class 'decimal.Decimal'>":
                    return float(obj)
                raise TypeError(f"Type {type(obj)} not serializable")

            json_content = json.dumps(audit_report, indent=2, default=json_serial)
            json_key = f"database_audits/audit_{timestamp}.json"
            json_s3_path = upload_to_s3(json_content, json_key, bucket, 'application/json')

            # CSV summary (per-table overview)
            tables_data = [
                {
                    'schema': t['schema'],
                    'table': t['table'],
                    'records': t['record_count'],
                    'columns': t['column_count'],
                    'has_created_by': t['audit_fields']['has_created_by'],
                    'has_created_at': t['audit_fields']['has_created_at'],
                    'has_updated_by': t['audit_fields']['has_updated_by'],
                    'has_updated_at': t['audit_fields']['has_updated_at'],
                    'has_audit_trigger': t['has_audit_trigger'],
                    'created_by_values': ','.join(map(str, t['audit_fields']['created_by_values'])),
                    'owner': t['owner'],
                }
                for t in audit_report['tables']
            ]
            csv_content = pd.DataFrame(tables_data).to_csv(index=False)
            csv_key = f"database_audits/tables_summary_{timestamp}.csv"
            csv_s3_path = upload_to_s3(csv_content, csv_key, bucket, 'text/csv')

            vs = audit_report['versioning_system']
            val = audit_report['validation']
            return {
                'statusCode': 200,
                'body': {
                    'message': 'Database audit completed successfully',
                    'timestamp': audit_report['audit_timestamp'],
                    'database': audit_report['database_info']['database_name'],
                    'session_user': audit_report['database_info']['session_user'],
                    'summary': {
                        'total_tables': len(audit_report['tables']),
                        'total_records': sum(
                            t['record_count'] for t in audit_report['tables'] if t['record_count'] > 0
                        ),
                        'total_indexes': len(audit_report['indexes']),
                        'total_foreign_keys': len(audit_report['foreign_keys']),
                        'total_check_constraints': len(audit_report['check_constraints']),
                        'total_triggers': len(audit_report['triggers']),
                        'total_functions': len(audit_report['functions']),
                        'tables_with_audit_fields': sum(
                            1 for t in audit_report['tables'] if t['audit_fields']['has_created_by']
                        ),
                        'tables_with_audit_trigger': sum(
                            1 for t in audit_report['tables'] if t['has_audit_trigger']
                        ),
                        'version_families': len(vs.get('version_families', [])),
                        'developers': len(vs.get('developers', [])),
                        'domain_map_missing': len(
                            vs.get('validation', {}).get('domain_map_missing_tables', [])
                        ),
                    },
                    'validation': {
                        'tables_missing_audit_trigger': val['tables_missing_audit_trigger'],
                        'tables_attributed_to_system_only': val['tables_attributed_to_system_only'],
                        'domain_map_missing_tables': vs.get('validation', {}).get('domain_map_missing_tables', []),
                        'domain_map_unexpected_tables': vs.get('validation', {}).get('domain_map_unexpected_tables', []),
                        'families_without_active_version': vs.get('validation', {}).get('families_without_active_version', []),
                        'families_with_multiple_active_versions': vs.get('validation', {}).get('families_with_multiple_active_versions', []),
                    },
                    'reports': {
                        'detailed_json': json_s3_path,
                        'tables_csv': csv_s3_path,
                    },
                },
            }

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Audit failed: {e}")
        return {
            'statusCode': 500,
            'body': {'error': str(e), 'message': 'Database audit failed'},
        }
