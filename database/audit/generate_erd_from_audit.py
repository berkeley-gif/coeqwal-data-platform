#!/usr/bin/env python3
"""
Generate ERD documentation from database audit data
Creates comprehensive, accurate ERD based on actual database state

Usage:
    python generate_erd_from_audit.py <audit_json_path> <output_md_path>
    
Example:
    python generate_erd_from_audit.py ../../audits/Oct22.json ../schema/GENERATED_ERD.md
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime


# Table categories for organized documentation
TABLE_CATEGORIES = {
    'Versioning': ['version_family', 'version', 'developer', 'domain_family_map'],
    'Lookup/Reference': ['hydrologic_region', 'source', 'model_source', 'unit', 'spatial_scale', 
                         'temporal_scale', 'statistic_type', 'geometry_type', 'variable_type'],
    'Network': ['network', 'network_node', 'network_arc', 'network_gis', 'network_type', 
                'network_subtype', 'network_entity_type'],
    'Entities': ['reservoirs', 'reservoir_entity', 'channel_entity', 'inflow_entity',
                 'du_urban_entity', 'du_agriculture_entity', 'du_refuge_entity',
                 'wba', 'compliance_stations'],
    'Tier System': ['tier_definition', 'tier_result', 'tier_location_result', 'variable_tier'],
    'Statistics': ['reservoir_group', 'reservoir_group_member', 'reservoir_monthly_percentile',
                   'reservoir_storage_monthly', 'reservoir_spill_monthly', 'reservoir_period_summary'],
    'System': ['spatial_ref_sys']
}


def generate_erd_from_audit(audit_path: Path, output_path: Path) -> dict:
    """
    Generate ERD markdown from audit JSON
    
    Args:
        audit_path: Path to audit JSON file
        output_path: Path for output markdown file
        
    Returns:
        dict with generation stats
    """
    
    with open(audit_path) as f:
        audit = json.load(f)
    
    erd = []
    
    # Header
    erd.append("# COEQWAL DATABASE ERD")
    erd.append(f"**Generated from audit**: {audit['audit_timestamp']}")
    erd.append(f"**Database**: {audit['database_info']['database_name']}")
    erd.append(f"**PostgreSQL**: {audit['database_info']['postgresql_version']}")
    erd.append("")
    erd.append("---")
    erd.append("")
    
    # Summary
    tables = audit['tables']
    total_records = sum(t['record_count'] for t in tables)
    
    erd.append("## DATABASE SUMMARY")
    erd.append("")
    erd.append(f"- **Total Tables**: {len(tables)}")
    erd.append(f"- **Total Records**: {total_records:,}")
    erd.append(f"- **Audit Date**: {audit['audit_timestamp']}")
    erd.append("")
    
    # Track uncategorized tables
    categorized_tables = set()
    for table_list in TABLE_CATEGORIES.values():
        categorized_tables.update(table_list)
    
    # Table of contents
    erd.append("## TABLE OF CONTENTS")
    erd.append("")
    for category, table_list in TABLE_CATEGORIES.items():
        erd.append(f"### **{category}**")
        for table_name in table_list:
            table_data = next((t for t in tables if t['table'] == table_name), None)
            if table_data:
                erd.append(f"- `{table_name}` ({table_data['record_count']:,} records)")
        erd.append("")
    
    # Add uncategorized tables
    uncategorized = [t for t in tables if t['table'] not in categorized_tables]
    if uncategorized:
        erd.append("### **Uncategorized**")
        for table_data in sorted(uncategorized, key=lambda x: x['table']):
            erd.append(f"- `{table_data['table']}` ({table_data['record_count']:,} records)")
        erd.append("")
    
    erd.append("---")
    erd.append("")
    
    # Detailed table definitions
    for category, table_list in TABLE_CATEGORIES.items():
        matching_tables = [t for t in tables if t['table'] in table_list]
        if not matching_tables:
            continue
            
        erd.append(f"## {category.upper()} TABLES")
        erd.append("")
        
        for table_name in table_list:
            table_data = next((t for t in tables if t['table'] == table_name), None)
            if not table_data:
                continue
            
            erd.append(f"### **{table_name}**")
            erd.append("")
            erd.append("```")
            erd.append(f"Table: {table_name}")
            erd.append(f"Records: {table_data['record_count']:,}")
            erd.append(f"Columns: {table_data['column_count']}")
            
            # Check for audit fields
            audit_fields = table_data.get('audit_fields', {})
            if audit_fields.get('has_created_at') and audit_fields.get('has_updated_at'):
                erd.append("Audit: Full audit trail")
            
            erd.append("")
            erd.append("Columns:")
            
            # List columns
            for col in table_data['columns']:
                col_name = col['column_name']
                data_type = col['data_type']
                nullable = "" if col['is_nullable'] else " NOT NULL"
                
                # Check for primary key
                pk_marker = " [PK]" if col_name == 'id' else ""
                
                erd.append(f"  {col_name:<30} {data_type:<20}{nullable}{pk_marker}")
            
            erd.append("```")
            erd.append("")
            
            # Add indexes if available
            if table_data.get('has_indexes'):
                erd.append("**Indexes**: Present")
                erd.append("")
        
        erd.append("---")
        erd.append("")
    
    # Uncategorized tables section
    if uncategorized:
        erd.append("## UNCATEGORIZED TABLES")
        erd.append("")
        for table_data in sorted(uncategorized, key=lambda x: x['table']):
            erd.append(f"### **{table_data['table']}**")
            erd.append("")
            erd.append("```")
            erd.append(f"Table: {table_data['table']}")
            erd.append(f"Records: {table_data['record_count']:,}")
            erd.append(f"Columns: {table_data['column_count']}")
            erd.append("```")
            erd.append("")
        erd.append("---")
        erd.append("")
    
    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write('\n'.join(erd))
    
    stats = {
        'tables_documented': len(tables),
        'total_records': total_records,
        'lines_generated': len(erd),
        'uncategorized_count': len(uncategorized)
    }
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Generate ERD documentation from database audit JSON',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python generate_erd_from_audit.py ../../audits/Oct22.json ../schema/GENERATED_ERD.md
    python generate_erd_from_audit.py /path/to/audit.json /path/to/output.md
        """
    )
    parser.add_argument('audit_path', type=Path, help='Path to audit JSON file')
    parser.add_argument('output_path', type=Path, help='Path for output markdown file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if not args.audit_path.exists():
        print(f"Error: Audit file not found: {args.audit_path}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Generating ERD from: {args.audit_path}")
    print(f"Output: {args.output_path}")
    
    stats = generate_erd_from_audit(args.audit_path, args.output_path)
    
    print(f"\nERD generated successfully!")
    print(f"  Tables documented: {stats['tables_documented']}")
    print(f"  Total records: {stats['total_records']:,}")
    print(f"  Lines generated: {stats['lines_generated']}")
    if stats['uncategorized_count'] > 0:
        print(f"  Uncategorized tables: {stats['uncategorized_count']} (consider adding to TABLE_CATEGORIES)")


if __name__ == '__main__':
    main()
