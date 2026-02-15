#!/usr/bin/env python3
"""
Compare ERD documentation with actual database audit
Identifies missing tables, column mismatches, and documentation gaps

Usage:
    python verify_erd_against_audit.py <erd_md_path> <audit_json_path>
    
Example:
    python verify_erd_against_audit.py ../schema/COEQWAL_SCENARIOS_DB_ERD.md ../../audits/Oct22.json
"""

import argparse
import json
import re
import sys
from pathlib import Path


def parse_erd_tables(erd_path: Path) -> dict:
    """
    Extract table definitions from ERD markdown
    
    Args:
        erd_path: Path to ERD markdown file
        
    Returns:
        dict mapping table names to their documented columns
    """
    with open(erd_path) as f:
        content = f.read()
    
    tables = {}
    
    # Find all table definitions (handles various formats)
    # Pattern 1: ### **table_name** or ### table_name
    # Pattern 2: Table: table_name in code blocks
    
    # First, try to find explicit table blocks
    table_blocks = re.split(r'###\s+\*?\*?(\w+)\*?\*?\s*\n', content)
    
    # Alternative: find "Table: name" patterns
    table_pattern = r'Table[:\s]+(\w+)'
    for match in re.finditer(table_pattern, content, re.IGNORECASE):
        table_name = match.group(1).lower()
        
        # Skip if it's a reserved word
        if table_name in ('of', 'contents', 'summary'):
            continue
        
        # Find the section for this table
        section_start = match.end()
        # Look for next table or end of section
        next_table = re.search(r'###|Table[:\s]+\w+', content[section_start:], re.IGNORECASE)
        section_end = section_start + next_table.start() if next_table else min(section_start + 2000, len(content))
        section = content[section_start:section_end]
        
        # Extract column names from the section
        columns = []
        for line in section.split('\n'):
            # Look for column patterns:
            # - "├── column_name" or "└── column_name" (tree format)
            # - "  column_name    type" (indented format)
            # - "| column_name |" (table format)
            
            # Tree format
            col_match = re.match(r'[├└│|]?[──\s]*(\w+)\s+', line.strip())
            if col_match and col_match.group(1) not in ('Table', 'Records', 'Columns', 'Audit', 'Indexes'):
                col_name = col_match.group(1)
                # Skip numeric-only matches
                if not col_name.isdigit() and len(col_name) > 1:
                    columns.append(col_name)
        
        if columns:
            tables[table_name] = {
                'columns': list(set(columns)),  # Dedupe
                'section': section[:300]  # First 300 chars for reference
            }
    
    return tables


def load_audit_data(audit_path: Path) -> dict:
    """
    Load actual database schema from audit JSON
    
    Args:
        audit_path: Path to audit JSON file
        
    Returns:
        dict mapping table names to their actual columns and stats
    """
    with open(audit_path) as f:
        audit = json.load(f)
    
    tables = {}
    for table in audit['tables']:
        table_name = table['table'].lower()
        
        # Check audit fields
        audit_fields = table.get('audit_fields', {})
        has_audit = audit_fields.get('has_created_at', False) and audit_fields.get('has_updated_at', False)
        
        tables[table_name] = {
            'column_count': table['column_count'],
            'record_count': table['record_count'],
            'columns': [col['column_name'] for col in table['columns']],
            'has_audit': has_audit
        }
    
    return tables


def compare_schemas(erd_tables: dict, audit_tables: dict, verbose: bool = False) -> dict:
    """
    Compare ERD documentation with actual database
    
    Args:
        erd_tables: Parsed ERD table definitions
        audit_tables: Actual database table definitions
        verbose: Whether to print verbose output
        
    Returns:
        dict with verification results
    """
    
    print("\n" + "=" * 80)
    print("ERD VERIFICATION REPORT")
    print("=" * 80 + "\n")
    
    # Find tables in DB but not in ERD
    missing_from_erd = set(audit_tables.keys()) - set(erd_tables.keys())
    # Exclude system tables
    missing_from_erd = {t for t in missing_from_erd if not t.startswith('spatial_ref')}
    
    if missing_from_erd:
        print("TABLES IN DATABASE BUT MISSING FROM ERD:")
        print("-" * 80)
        for table in sorted(missing_from_erd):
            info = audit_tables[table]
            print(f"  - {table:<30} {info['column_count']:>3} cols  {info['record_count']:>8,} records")
        print()
    
    # Find tables in ERD but not in DB
    missing_from_db = set(erd_tables.keys()) - set(audit_tables.keys())
    if missing_from_db:
        print("TABLES IN ERD BUT NOT IN DATABASE (may be planned):")
        print("-" * 80)
        for table in sorted(missing_from_db):
            print(f"  - {table}")
        print()
    
    # Check tables that exist in both
    common_tables = set(erd_tables.keys()) & set(audit_tables.keys())
    
    mismatches = []
    for table in sorted(common_tables):
        erd_cols = set(erd_tables[table]['columns'])
        db_cols = set(audit_tables[table]['columns'])
        
        missing_cols = db_cols - erd_cols
        extra_cols = erd_cols - db_cols
        
        if missing_cols or extra_cols:
            mismatches.append({
                'table': table,
                'missing_in_erd': missing_cols,
                'extra_in_erd': extra_cols,
                'db_col_count': len(db_cols),
                'erd_col_count': len(erd_cols)
            })
    
    if mismatches:
        print("COLUMN MISMATCHES IN DOCUMENTED TABLES:")
        print("-" * 80)
        for mismatch in mismatches:
            print(f"\n  {mismatch['table'].upper()}:")
            print(f"    DB: {mismatch['db_col_count']} cols, ERD: {mismatch['erd_col_count']} cols")
            if mismatch['missing_in_erd']:
                print(f"    Missing from ERD: {', '.join(sorted(mismatch['missing_in_erd']))}")
            if mismatch['extra_in_erd']:
                print(f"    Extra in ERD: {', '.join(sorted(mismatch['extra_in_erd']))}")
        print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY:")
    print("-" * 80)
    
    correct_count = len(common_tables) - len(mismatches)
    print(f"  Tables documented correctly:    {correct_count:>4}")
    print(f"  Tables with column mismatches:  {len(mismatches):>4}")
    print(f"  Tables missing from ERD:        {len(missing_from_erd):>4}")
    print(f"  Tables in ERD but not DB:       {len(missing_from_db):>4}")
    print()
    
    if not missing_from_erd and not mismatches:
        print("STATUS: ERD IS SYNCHRONIZED WITH DATABASE")
    elif len(missing_from_erd) > 0 or len(mismatches) > 0:
        print("STATUS: ERD NEEDS UPDATES")
        print("\nRecommendation: Run generate_erd_from_audit.py to regenerate ERD")
    
    print("=" * 80 + "\n")
    
    return {
        'missing_from_erd': list(missing_from_erd),
        'missing_from_db': list(missing_from_db),
        'mismatches': mismatches,
        'correct_count': correct_count,
        'is_synchronized': not missing_from_erd and not mismatches
    }


def main():
    parser = argparse.ArgumentParser(
        description='Verify ERD documentation against database audit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python verify_erd_against_audit.py ../schema/COEQWAL_SCENARIOS_DB_ERD.md ../../audits/Oct22.json
    python verify_erd_against_audit.py /path/to/erd.md /path/to/audit.json
        """
    )
    parser.add_argument('erd_path', type=Path, help='Path to ERD markdown file')
    parser.add_argument('audit_path', type=Path, help='Path to audit JSON file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--json', action='store_true', help='Output results as JSON')
    
    args = parser.parse_args()
    
    if not args.erd_path.exists():
        print(f"Error: ERD file not found: {args.erd_path}", file=sys.stderr)
        sys.exit(1)
    
    if not args.audit_path.exists():
        print(f"Error: Audit file not found: {args.audit_path}", file=sys.stderr)
        sys.exit(1)
    
    print(f"ERD File: {args.erd_path.name}")
    print(f"Audit File: {args.audit_path.name}")
    
    erd_tables = parse_erd_tables(args.erd_path)
    audit_tables = load_audit_data(args.audit_path)
    
    print(f"\nERD Tables Documented: {len(erd_tables)}")
    print(f"Audit Tables Found: {len(audit_tables)}")
    
    results = compare_schemas(erd_tables, audit_tables, verbose=args.verbose)
    
    if args.json:
        print(json.dumps(results, indent=2))
    
    # Exit with error code if not synchronized
    sys.exit(0 if results['is_synchronized'] else 1)


if __name__ == '__main__':
    main()
