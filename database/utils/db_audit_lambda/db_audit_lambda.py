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
    """Return exact row count; -1 on error."""
    try:
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
        return cursor.fetchone()[0]
    except Exception as e:
        logger.warning(f"Could not count {schema}.{table}: {e}")
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

        except Exception as e:
            result['error'] = str(e)

    return result


# ---------------------------------------------------------------------------
# Database-wide structural queries (NEW)
# ---------------------------------------------------------------------------

def get_indexes(cursor) -> List[Dict[str, Any]]:
    """
    Return full index definitions for all user tables.
    Replaces the per-table has_indexes boolean with actionable detail.
    """
    cursor.execute("""
        SELECT tablename AS table_name, indexname AS index_name, indexdef AS definition
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname;
    """)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_foreign_keys(cursor) -> List[Dict[str, Any]]:
    """
    Return all foreign key constraints with source column and referenced table/column.
    """
    cursor.execute("""
        SELECT
            tc.table_name,
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name  AS ref_table,
            ccu.column_name AS ref_column,
            rc.delete_rule,
            rc.update_rule
        FROM information_schema.table_constraints        tc
        JOIN information_schema.key_column_usage         kcu USING (constraint_name, constraint_schema)
        JOIN information_schema.referential_constraints  rc  USING (constraint_name, constraint_schema)
        JOIN information_schema.constraint_column_usage  ccu USING (constraint_name, constraint_schema)
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema    = 'public'
        ORDER BY tc.table_name, tc.constraint_name;
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
# Versioning system check
# ---------------------------------------------------------------------------

# Tables that MUST appear in domain_family_map based on the seed CSV.
# Update this list whenever a new versioned domain table is added.
EXPECTED_DOMAIN_MAPPINGS = {
    'theme', 'scenario',
    'scenario_variable_statistic', 'scenario_measure_statistic',
    'scenario_outcome_statistic', 'scenario_category_statistic',
    'scenario_tier_value', 'scenario_metadata', 'scenario_ancillary_output',
    'assumption_definition', 'operation_definition',
    'hydroclimate',
    'variable_group', 'model_variable', 'derived_variable',
    'hydroclimate_variable_summary',
    'network_node', 'network_arc',
    'reservoir_entity', 'inflow_entity', 'channel_entity',
    'reservoir_variable', 'inflow_variable', 'channel_variable',
    'outcome_category', 'outcome_measure',
    'tier_definition', 'tier_level',
    'geometry',
    'analysis', 'key_concept', 'chart_type', 'ancillary_data',
    'constant', 'model_value',
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

        # Validate against expected seed tables
        mapped_tables = {row['table_name'] for row in result['domain_mappings']}
        result['validation']['domain_map_missing_tables'] = sorted(
            EXPECTED_DOMAIN_MAPPINGS - mapped_tables
        )
        result['validation']['domain_map_unexpected_tables'] = sorted(
            mapped_tables - EXPECTED_DOMAIN_MAPPINGS
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

    cursor.execute("SELECT current_database(), current_user, version();")
    db_name, db_user, db_version = cursor.fetchone()

    audit_report = {
        'audit_timestamp': datetime.now().isoformat(),
        'database_info': {
            'database_name': db_name,
            'current_user': db_user,
            'postgresql_version': db_version,
        },
        'versioning_system': check_versioning_system(cursor),
        'indexes': [],
        'foreign_keys': [],
        'check_constraints': [],
        'unique_constraints': [],
        'tables': [],
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

        audit_report['tables'].append({
            'schema': schema,
            'table': table,
            'owner': table_info['tableowner'],
            'has_indexes': table_info['hasindexes'],
            'has_rules': table_info['hasrules'],
            'has_triggers': table_info['hastriggers'],
            'record_count': record_count,
            'column_count': len(structure),
            'columns': structure,
            'audit_fields': audit_fields,
        })

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
                    'created_by_values': ','.join(map(str, t['audit_fields']['created_by_values'])),
                    'owner': t['owner'],
                }
                for t in audit_report['tables']
            ]
            csv_content = pd.DataFrame(tables_data).to_csv(index=False)
            csv_key = f"database_audits/tables_summary_{timestamp}.csv"
            csv_s3_path = upload_to_s3(csv_content, csv_key, bucket, 'text/csv')

            vs = audit_report['versioning_system']
            return {
                'statusCode': 200,
                'body': {
                    'message': 'Database audit completed successfully',
                    'timestamp': audit_report['audit_timestamp'],
                    'database': audit_report['database_info']['database_name'],
                    'summary': {
                        'total_tables': len(audit_report['tables']),
                        'total_records': sum(
                            t['record_count'] for t in audit_report['tables'] if t['record_count'] > 0
                        ),
                        'total_indexes': len(audit_report['indexes']),
                        'total_foreign_keys': len(audit_report['foreign_keys']),
                        'total_check_constraints': len(audit_report['check_constraints']),
                        'tables_with_audit_fields': sum(
                            1 for t in audit_report['tables'] if t['audit_fields']['has_created_by']
                        ),
                        'version_families': len(vs.get('version_families', [])),
                        'developers': len(vs.get('developers', [])),
                        'domain_map_missing': len(
                            vs.get('validation', {}).get('domain_map_missing_tables', [])
                        ),
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
