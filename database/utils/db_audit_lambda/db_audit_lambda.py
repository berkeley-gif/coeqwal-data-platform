#!/usr/bin/env python3
"""
AWS Lambda Database Audit Function
==================================
PostgreSQL database audit that saves results to S3
"""

import json
import boto3
import psycopg2
import pandas as pd
from datetime import datetime
import os
from typing import Dict, List, Any
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 client
s3_client = boto3.client('s3')

def get_database_connection():
    """Get database connection from environment variables"""
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

def get_all_tables(cursor) -> List[Dict[str, Any]]:
    """Get all tables with basic info"""
    query = """
    SELECT 
        schemaname,
        tablename,
        tableowner,
        hasindexes,
        hasrules,
        hastriggers
    FROM pg_tables 
    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
    ORDER BY schemaname, tablename;
    """
    
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def get_table_structure(cursor, schema: str, table: str) -> List[Dict[str, Any]]:
    """Get detailed structure for a specific table"""
    query = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position;
    """
    
    cursor.execute(query, (schema, table))
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def get_record_counts(cursor, schema: str, table: str) -> int:
    """Get record count for a table"""
    try:
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
        return cursor.fetchone()[0]
    except Exception as e:
        logger.warning(f"Could not count records in {schema}.{table}: {e}")
        return -1

def check_created_by_field(cursor, schema: str, table: str) -> Dict[str, Any]:
    """Check if created_by field exists and has data"""
    structure = get_table_structure(cursor, schema, table)
    
    has_created_by = any(col['column_name'] == 'created_by' for col in structure)
    has_created_at = any(col['column_name'] == 'created_at' for col in structure)
    has_updated_by = any(col['column_name'] == 'updated_by' for col in structure)
    has_updated_at = any(col['column_name'] == 'updated_at' for col in structure)
    
    result = {
        'has_created_by': has_created_by,
        'has_created_at': has_created_at,
        'has_updated_by': has_updated_by,
        'has_updated_at': has_updated_at,
        'created_by_values': [],
        'created_at_range': None,
        'sample_records': []
    }
    
    if has_created_by:
        try:
            # Get unique created_by values
            cursor.execute(f'SELECT DISTINCT created_by FROM "{schema}"."{table}" WHERE created_by IS NOT NULL ORDER BY created_by;')
            result['created_by_values'] = [row[0] for row in cursor.fetchall()]
            
            # Get created_at range if available
            if has_created_at:
                cursor.execute(f'SELECT MIN(created_at), MAX(created_at) FROM "{schema}"."{table}" WHERE created_at IS NOT NULL;')
                min_date, max_date = cursor.fetchone()
                if min_date and max_date:
                    result['created_at_range'] = {
                        'min': min_date.isoformat(),
                        'max': max_date.isoformat()
                    }
            
            # Get sample records
            sample_query = f'SELECT * FROM "{schema}"."{table}" LIMIT 3;'
            cursor.execute(sample_query)
            columns = [desc[0] for desc in cursor.description]
            result['sample_records'] = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
        except Exception as e:
            result['error'] = str(e)
    
    return result

def check_versioning_system(cursor) -> Dict[str, Any]:
    """Check the versioning system tables and functionality"""
    versioning_tables = ['version_family', 'version', 'domain_family_map', 'developer']
    
    result = {
        'versioning_tables_exist': {},
        'version_families': [],
        'versions': [],
        'domain_mappings': [],
        'developers': []
    }
    
    # Check if versioning tables exist
    for table in versioning_tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM public.{table};')
            count = cursor.fetchone()[0]
            result['versioning_tables_exist'][table] = {
                'exists': True,
                'count': count
            }
        except Exception as e:
            result['versioning_tables_exist'][table] = {
                'exists': False,
                'error': str(e)
            }
    
    # Get version families
    try:
        cursor.execute('SELECT * FROM public.version_family ORDER BY id;')
        columns = [desc[0] for desc in cursor.description]
        result['version_families'] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        result['version_families_error'] = str(e)
    
    # Get versions
    try:
        cursor.execute('SELECT * FROM public.version ORDER BY id;')
        columns = [desc[0] for desc in cursor.description]
        result['versions'] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        result['versions_error'] = str(e)
    
    # Get domain mappings
    try:
        cursor.execute('SELECT * FROM public.domain_family_map ORDER BY schema_name, table_name;')
        columns = [desc[0] for desc in cursor.description]
        result['domain_mappings'] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        result['domain_mappings_error'] = str(e)
    
    # Get developers
    try:
        cursor.execute('SELECT * FROM public.developer ORDER BY id;')
        columns = [desc[0] for desc in cursor.description]
        result['developers'] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        result['developers_error'] = str(e)
    
    return result


def generate_audit_report(conn) -> Dict[str, Any]:
    """Generate comprehensive audit report"""
    cursor = conn.cursor()
    
    logger.info("Starting comprehensive database audit...")
    
    # Basic database info
    cursor.execute("SELECT current_database(), current_user, version();")
    db_name, db_user, db_version = cursor.fetchone()
    
    audit_report = {
        'audit_timestamp': datetime.now().isoformat(),
        'database_info': {
            'database_name': db_name,
            'current_user': db_user,
            'postgresql_version': db_version
        },
        'versioning_system': check_versioning_system(cursor),
        'tables': []
    }
    
    # Get all tables
    tables = get_all_tables(cursor)
    logger.info(f"Found {len(tables)} tables to audit")
    
    for i, table_info in enumerate(tables, 1):
        schema = table_info['schemaname']
        table = table_info['tablename']
        
        logger.info(f"Auditing {schema}.{table} ({i}/{len(tables)})")
        
        # Get detailed info for this table
        structure = get_table_structure(cursor, schema, table)
        record_count = get_record_counts(cursor, schema, table)
        created_by_info = check_created_by_field(cursor, schema, table)
        
        table_audit = {
            'schema': schema,
            'table': table,
            'owner': table_info['tableowner'],
            'has_indexes': table_info['hasindexes'],
            'has_rules': table_info['hasrules'],
            'has_triggers': table_info['hastriggers'],
            'record_count': record_count,
            'column_count': len(structure),
            'columns': structure,
            'audit_fields': created_by_info
        }
        
        audit_report['tables'].append(table_audit)
    
    cursor.close()
    return audit_report

def upload_to_s3(content: str, key: str, bucket: str, content_type: str = 'application/json'):
    """Upload content to S3"""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType=content_type
        )
        logger.info(f"Uploaded to s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise

def lambda_handler(event, context):
    """Main Lambda handler"""
    try:
        # Get S3 bucket from environment or event
        bucket = event.get('bucket', os.environ.get('S3_BUCKET'))
        if not bucket:
            raise ValueError("S3_BUCKET not specified in environment or event")
        
        # Generate timestamp for file naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Connect to database
        conn = get_database_connection()
        
        try:
            # Generate audit report
            audit_report = generate_audit_report(conn)
            
            # Convert datetime objects and decimals to strings for JSON serialization
            def json_serial(obj):
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                elif hasattr(obj, '__float__'):
                    return float(obj)
                elif str(type(obj)) == "<class 'decimal.Decimal'>":
                    return float(obj)
                raise TypeError(f"Type {type(obj)} not serializable")
            
            # Upload detailed JSON report
            json_content = json.dumps(audit_report, indent=2, default=json_serial)
            json_key = f"database_audits/audit_{timestamp}.json"
            json_s3_path = upload_to_s3(json_content, json_key, bucket, 'application/json')
            
            # Create CSV summary of tables
            tables_data = []
            for table in audit_report['tables']:
                tables_data.append({
                    'schema': table['schema'],
                    'table': table['table'],
                    'records': table['record_count'],
                    'columns': table['column_count'],
                    'has_created_by': table['audit_fields']['has_created_by'],
                    'has_created_at': table['audit_fields']['has_created_at'],
                    'has_updated_by': table['audit_fields']['has_updated_by'],
                    'has_updated_at': table['audit_fields']['has_updated_at'],
                    'created_by_values': ','.join(map(str, table['audit_fields']['created_by_values'])),
                    'owner': table['owner']
                })
            
            # Convert to CSV
            df = pd.DataFrame(tables_data)
            csv_content = df.to_csv(index=False)
            csv_key = f"database_audits/tables_summary_{timestamp}.csv"
            csv_s3_path = upload_to_s3(csv_content, csv_key, bucket, 'text/csv')
            
            # Generate summary statistics
            total_tables = len(audit_report['tables'])
            total_records = sum(t['record_count'] for t in audit_report['tables'] if t['record_count'] > 0)
            tables_with_created_by = sum(1 for t in audit_report['tables'] if t['audit_fields']['has_created_by'])
            
            versioning_status = audit_report['versioning_system']
            version_families_count = len(versioning_status.get('version_families', []))
            developers_count = len(versioning_status.get('developers', []))
            
            response = {
                'statusCode': 200,
                'body': {
                    'message': 'Database audit completed successfully',
                    'timestamp': audit_report['audit_timestamp'],
                    'database': audit_report['database_info']['database_name'],
                    'summary': {
                        'total_tables': total_tables,
                        'total_records': total_records,
                        'tables_with_created_by': tables_with_created_by,
                        'version_families': version_families_count,
                        'developers': developers_count
                    },
                    'reports': {
                        'detailed_json': json_s3_path,
                        'tables_csv': csv_s3_path
                    }
                }
            }
            
            logger.info(f"Audit completed: {total_tables} tables, {total_records:,} records")
            return response
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Audit failed: {e}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Database audit failed'
            }
        }
