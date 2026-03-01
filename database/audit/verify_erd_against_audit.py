#!/usr/bin/env python3
"""
Compare ERD documentation with database

Checks (in order):
  1. Tables in DB but missing from ERD
  2. Tables in ERD but not in DB (planned)
  3. Column mismatches in tables present in both
  4. Audit field documentation mismatches (ERD vs DB)
  5. Tables with audit fields but no created_by values populated
  6. Index name mismatches (requires enhanced audit JSON)
  7. Foreign key constraint mismatches (requires enhanced audit JSON)
  8. CHECK constraint mismatches (requires enhanced audit JSON)
  9. Versioning system completeness (requires enhanced audit JSON)

Parses the tree-format code blocks used in COEQWAL_SCENARIOS_DB_ERD.md:

    ```
    Table: table_name
    ├── column_one   TEXT NOT NULL        ← column section
    └── column_two   INTEGER

    Constraints:                          ← switches to constraints section
    ├── FK: col → ref_table.ref_col
    ├── Unique: (col1, col2)
    └── Check: col BETWEEN 1 AND 12

    Indexes:                              ← switches to indexes section
    ├── idx_name (col1, col2)
    └── idx_other (col3)
    ```

Usage — run from database/schema/:

    python ../audit/verify_erd_against_audit.py COEQWAL_SCENARIOS_DB_ERD.md ../audits/latest.json [--verbose] [--json]
"""

import argparse
import json
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# System/extension tables never documented in the ERD
_SYSTEM_TABLES = {'spatial_ref_sys'}

# Indexes automatically created by PostgreSQL for PKs/unique constraints —
# these appear in pg_indexes but don't need to be documented in the ERD.
_AUTO_INDEX_PREFIXES = ('pg_',)


# ---------------------------------------------------------------------------
# ERD parser
# ---------------------------------------------------------------------------

def parse_erd_tables(erd_path: Path) -> dict:
    """
    Extract table definitions from ERD markdown.

    Scans fenced code blocks for `Table: name` headers, then collects:
      - columns      from tree lines (├──/└──) before any section marker
      - indexes      from lines in the `Indexes:` subsection
      - foreign_keys from `FK: col → ref_table.ref_col` lines in `Constraints:`
      - check_constraints from `Check: clause` lines in `Constraints:`
      - unique_constraints from `Unique: (cols)` lines in `Constraints:`

    Returns:
        dict mapping table_name → {
            columns: list[str],
            has_audit_fields: bool,
            indexes: list[str],                        # index names only
            foreign_keys: list[{column, ref_table, ref_column}],
            check_constraints: list[str],              # raw clause text
            unique_constraints: list[str],             # raw column-list text
        }
    """
    content = erd_path.read_text()
    tables = {}

    # Section header patterns
    _SEC_CONSTRAINTS = re.compile(r'^Constraints?[\s:(]', re.IGNORECASE)
    _SEC_INDEXES     = re.compile(r'^Indexes?[\s:(]',     re.IGNORECASE)
    _SEC_IGNORE      = re.compile(
        r'^(Values|Records|Columns|Audit|Notes?|Foreign\s+Keys?|Expected|DDL|ETL'
        r'|Baseline|Top|Comments?|Query|Status|Primary\s+key|Ref:|Example)[\s:(]',
        re.IGNORECASE,
    )

    for block in re.findall(r'```(.*?)```', content, re.DOTALL):
        lines = block.strip().split('\n')

        # Locate Table: declaration
        table_name = None
        table_line_idx = None
        for i, line in enumerate(lines):
            m = re.match(r'Table:\s+(\w+)', line.strip())
            if m:
                table_name = m.group(1).lower()
                table_line_idx = i
                break
        if table_name is None:
            continue

        columns: list[str]             = []
        indexes: list[str]             = []
        foreign_keys: list[dict]       = []
        check_constraints: list[str]   = []
        unique_constraints: list[str]  = []

        section = 'columns'

        for line in lines[table_line_idx + 1:]:
            stripped = line.strip()
            if not stripped:
                continue

            # Section transitions
            if _SEC_CONSTRAINTS.match(stripped):
                section = 'constraints'
                continue
            if _SEC_INDEXES.match(stripped):
                section = 'indexes'
                continue
            if _SEC_IGNORE.match(stripped):
                section = 'ignore'
                continue

            if section == 'columns':
                m = re.match(r'[├└]──\s+(\w+)', stripped)
                if m:
                    columns.append(m.group(1))

            elif section == 'indexes':
                # ├── idx_name (cols) -- optional comment
                m = re.match(r'[├└]──\s+(\w+)', stripped)
                if m:
                    indexes.append(m.group(1))

            elif section == 'constraints':
                # FK: column → ref_table.ref_column
                m = re.match(r'[├└]──\s+FK:\s+(\w+)\s*→\s*(\w+)\.(\w+)', stripped)
                if m:
                    foreign_keys.append({
                        'column': m.group(1),
                        'ref_table': m.group(2),
                        'ref_column': m.group(3),
                    })
                    continue

                # Unique: (col1, col2)
                m = re.match(r'[├└]──\s+Unique:\s*(.*)', stripped, re.IGNORECASE)
                if m:
                    unique_constraints.append(m.group(1).strip())
                    continue

                # Check: clause
                m = re.match(r'[├└]──\s+Check:\s*(.*)', stripped, re.IGNORECASE)
                if m:
                    check_constraints.append(m.group(1).strip())

        audit_cols = {'created_at', 'created_by', 'updated_at', 'updated_by'}
        tables[table_name] = {
            'columns':            list(dict.fromkeys(columns)),  # preserve order, dedupe
            'has_audit_fields':   bool(set(columns) & audit_cols),
            'indexes':            indexes,
            'foreign_keys':       foreign_keys,
            'check_constraints':  check_constraints,
            'unique_constraints': unique_constraints,
        }

    return tables


# ---------------------------------------------------------------------------
# Audit JSON loader
# ---------------------------------------------------------------------------

def load_audit_data(audit_path: Path) -> dict:
    """
    Load actual database schema from audit JSON.

    Returns a namespace dict:
      tables           — per-table detail (columns, counts, audit fields)
      indexes          — list of {table_name, index_name, definition}
      foreign_keys     — list of {table_name, column_name, ref_table, ref_column, …}
      check_constraints — list of {table_name, constraint_name, check_clause}
      unique_constraints — list of {table_name, constraint_name, columns}
      versioning_system — versioning check output incl. validation sub-dict
      has_structural_data — True if the enhanced Lambda was used
    """
    audit = json.loads(audit_path.read_text())

    tables = {}
    for t in audit['tables']:
        name = t['table'].lower()
        af = t.get('audit_fields', {})
        tables[name] = {
            'column_count':    t['column_count'],
            'record_count':    t['record_count'],
            'columns':         [col['column_name'] for col in t['columns']],
            'has_audit_fields': (
                af.get('has_created_at', False) and af.get('has_updated_at', False)
            ),
            'created_by_values': af.get('created_by_values', []),
        }

    has_structural = 'indexes' in audit

    return {
        'tables':              tables,
        'indexes':             audit.get('indexes', []),
        'foreign_keys':        audit.get('foreign_keys', []),
        'check_constraints':   audit.get('check_constraints', []),
        'unique_constraints':  audit.get('unique_constraints', []),
        'versioning_system':   audit.get('versioning_system', {}),
        'has_structural_data': has_structural,
    }


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------

def _normalize_check(clause: str) -> str:
    """
    Normalize a CHECK clause for loose comparison.

    PostgreSQL internally rewrites CHECK clauses — e.g. it:
      - Adds nested parentheses: ((col >= 1) AND (col <= 12))
      - Expands BETWEEN: col BETWEEN 1 AND 12 → col >= 1 AND col <= 12

    Strategy: strip ALL parentheses, collapse whitespace, lowercase, then
    convert any remaining BETWEEN syntax so ERD and DB forms can be compared.
    """
    import re as _re
    c = clause.strip().lower()
    c = c.replace('(', '').replace(')', '')        # remove all parens
    c = ' '.join(c.split())                        # normalize whitespace
    # Convert: col BETWEEN x AND y → col >= x and col <= y
    m = _re.match(r'^(\w+)\s+between\s+(\S+)\s+and\s+(\S+)$', c)
    if m:
        col, lo, hi = m.group(1), m.group(2), m.group(3)
        c = f'{col} >= {lo} and {col} <= {hi}'
    return c


# ---------------------------------------------------------------------------
# Main comparison
# ---------------------------------------------------------------------------

def compare_schemas(
    erd_tables: dict,
    audit_data: dict,
    verbose: bool = False,
    quiet: bool = False,
) -> dict:
    """
    Run all nine verification checks and return a result dict.

    quiet=True suppresses all console output (used with --json flag).
    verbose=True prints full column lists for mismatched tables.
    """
    audit_tables = audit_data['tables']
    has_structural = audit_data['has_structural_data']

    def out(*args, **kwargs):
        if not quiet:
            print(*args, **kwargs)

    out("\n" + "=" * 80)
    out("ERD VERIFICATION REPORT")
    out("=" * 80 + "\n")

    # ------------------------------------------------------------------ #
    # 1. Tables in DB but missing from ERD
    # ------------------------------------------------------------------ #
    missing_from_erd = {
        t for t in audit_tables
        if t not in erd_tables and t not in _SYSTEM_TABLES
    }
    if missing_from_erd:
        out("1. TABLES IN DATABASE BUT MISSING FROM ERD:")
        out("-" * 80)
        for t in sorted(missing_from_erd):
            info = audit_tables[t]
            out(f"  - {t:<35} {info['column_count']:>3} cols  {info['record_count']:>8,} records")
        out()

    # ------------------------------------------------------------------ #
    # 2. Tables in ERD but not in DB
    # ------------------------------------------------------------------ #
    missing_from_db = sorted(set(erd_tables) - set(audit_tables))
    if missing_from_db:
        out("2. TABLES IN ERD BUT NOT IN DATABASE (may be planned):")
        out("-" * 80)
        for t in missing_from_db:
            out(f"  - {t}")
        out()

    # ------------------------------------------------------------------ #
    # 3. Column mismatches
    # ------------------------------------------------------------------ #
    common_tables = set(erd_tables) & set(audit_tables)
    col_mismatches = []
    for t in sorted(common_tables):
        erd_cols = set(erd_tables[t]['columns'])
        db_cols  = set(audit_tables[t]['columns'])
        missing_in_erd = sorted(db_cols - erd_cols)
        extra_in_erd   = sorted(erd_cols - db_cols)
        if missing_in_erd or extra_in_erd:
            col_mismatches.append({
                'table':          t,
                'missing_in_erd': missing_in_erd,
                'extra_in_erd':   extra_in_erd,
                'db_col_count':   len(db_cols),
                'erd_col_count':  len(erd_cols),
            })

    if col_mismatches:
        out("3. COLUMN MISMATCHES IN DOCUMENTED TABLES:")
        out("-" * 80)
        for m in col_mismatches:
            out(f"\n  {m['table'].upper()}:")
            out(f"    DB: {m['db_col_count']} cols  |  ERD: {m['erd_col_count']} cols")
            if m['missing_in_erd']:
                out(f"    In DB but missing from ERD:  {', '.join(m['missing_in_erd'])}")
            if m['extra_in_erd']:
                out(f"    In ERD but not in DB:        {', '.join(m['extra_in_erd'])}")
            if verbose:
                out(f"    DB columns:  {', '.join(sorted(audit_tables[m['table']]['columns']))}")
                out(f"    ERD columns: {', '.join(sorted(erd_tables[m['table']]['columns']))}")
        out()

    # ------------------------------------------------------------------ #
    # 4. Audit field documentation mismatches
    # ------------------------------------------------------------------ #
    audit_field_mismatches = []
    for t in sorted(common_tables):
        if erd_tables[t]['has_audit_fields'] != audit_tables[t]['has_audit_fields']:
            audit_field_mismatches.append({
                'table':         t,
                'erd_has_audit': erd_tables[t]['has_audit_fields'],
                'db_has_audit':  audit_tables[t]['has_audit_fields'],
            })

    if audit_field_mismatches:
        out("4. AUDIT FIELD DOCUMENTATION MISMATCHES (ERD vs DB):")
        out("-" * 80)
        for m in audit_field_mismatches:
            erd_s = "has audit fields" if m['erd_has_audit'] else "no audit fields"
            db_s  = "has audit fields" if m['db_has_audit']  else "no audit fields"
            out(f"  - {m['table']:<35} ERD: {erd_s:<22}  DB: {db_s}")
        out()

    # ------------------------------------------------------------------ #
    # 5. Audit fields present but created_by unpopulated
    # ------------------------------------------------------------------ #
    unpopulated_audit = [
        t for t in sorted(common_tables)
        if audit_tables[t]['has_audit_fields']
        and audit_tables[t]['record_count'] > 0
        and not audit_tables[t]['created_by_values']
    ]
    if unpopulated_audit:
        out("5. TABLES WITH AUDIT FIELDS BUT NO created_by VALUES:")
        out("-" * 80)
        for t in unpopulated_audit:
            out(f"  - {t:<35} {audit_tables[t]['record_count']:>8,} records")
        out()

    # ------------------------------------------------------------------ #
    # Checks 6-9 require the enhanced Lambda audit JSON
    # ------------------------------------------------------------------ #
    index_mismatches:  list = []
    fk_mismatches:     list = []
    check_mismatches:  list = []
    versioning_issues: dict = {}

    if not has_structural:
        out("NOTE: Checks 6-9 (indexes, FKs, CHECKs, versioning) require an audit")
        out("      generated by the enhanced Lambda. Re-run the audit to enable them.")
        out()
    else:
        # Build per-table lookup sets from the audit
        db_indexes_by_table:  dict[str, set] = {}
        db_fks_by_table:      dict[str, set] = {}
        db_checks_by_table:   dict[str, list] = {}

        for row in audit_data['indexes']:
            t = row['table_name'].lower()
            # Skip auto-generated indexes (PK sequences, PostGIS, etc.)
            iname = row['index_name']
            if any(iname.startswith(p) for p in _AUTO_INDEX_PREFIXES):
                continue
            db_indexes_by_table.setdefault(t, set()).add(iname)

        for row in audit_data['foreign_keys']:
            t = row['table_name'].lower()
            key = (row['column_name'].lower(), row['ref_table'].lower(), row['ref_column'].lower())
            db_fks_by_table.setdefault(t, set()).add(key)

        for row in audit_data['check_constraints']:
            t = row['table_name'].lower()
            db_checks_by_table.setdefault(t, []).append(_normalize_check(row['check_clause']))

        # -------------------------------------------------------------- #
        # 6. Index name mismatches
        # -------------------------------------------------------------- #
        for t in sorted(common_tables):
            erd_idxs = set(erd_tables[t]['indexes'])
            if not erd_idxs:
                continue
            db_idxs = db_indexes_by_table.get(t, set())
            missing = sorted(erd_idxs - db_idxs)
            if missing:
                index_mismatches.append({'table': t, 'missing_in_db': missing})

        if index_mismatches:
            out("6. INDEX MISMATCHES (documented in ERD but absent from DB):")
            out("-" * 80)
            for m in index_mismatches:
                out(f"  {m['table']}: {', '.join(m['missing_in_db'])}")
            out()
        else:
            out("6. Indexes:          OK (all documented indexes present in DB)")
            out()

        # -------------------------------------------------------------- #
        # 7. Foreign key constraint mismatches
        # -------------------------------------------------------------- #
        for t in sorted(common_tables):
            erd_fks = erd_tables[t]['foreign_keys']
            if not erd_fks:
                continue
            db_fks = db_fks_by_table.get(t, set())
            missing = [
                fk for fk in erd_fks
                if (fk['column'].lower(), fk['ref_table'].lower(), fk['ref_column'].lower())
                not in db_fks
            ]
            if missing:
                fk_mismatches.append({'table': t, 'missing_in_db': missing})

        if fk_mismatches:
            out("7. FOREIGN KEY MISMATCHES (documented but not enforced in DB):")
            out("-" * 80)
            for m in fk_mismatches:
                out(f"\n  {m['table'].upper()}:")
                for fk in m['missing_in_db']:
                    out(f"    {fk['column']} → {fk['ref_table']}.{fk['ref_column']}")
            out()
        else:
            out("7. Foreign keys:     OK (all documented FKs enforced in DB)")
            out()

        # -------------------------------------------------------------- #
        # 8. CHECK constraint mismatches
        # -------------------------------------------------------------- #
        for t in sorted(common_tables):
            erd_checks = erd_tables[t]['check_constraints']
            if not erd_checks:
                continue
            db_checks = db_checks_by_table.get(t, [])
            missing = [
                clause for clause in erd_checks
                if not any(_normalize_check(clause) in dc or dc in _normalize_check(clause)
                           for dc in db_checks)
            ]
            if missing:
                check_mismatches.append({'table': t, 'missing_in_db': missing})

        if check_mismatches:
            out("8. CHECK CONSTRAINT MISMATCHES (documented but absent from DB):")
            out("-" * 80)
            for m in check_mismatches:
                out(f"  {m['table']}: {'; '.join(m['missing_in_db'])}")
            out()
        else:
            out("8. CHECK constraints: OK (all documented CHECKs present in DB)")
            out()

        # -------------------------------------------------------------- #
        # 9. Versioning system completeness
        # -------------------------------------------------------------- #
        vs  = audit_data['versioning_system']
        val = vs.get('validation', {})

        missing_map   = val.get('domain_map_missing_tables', [])
        unexpected_map = val.get('domain_map_unexpected_tables', [])
        no_active     = val.get('families_without_active_version', [])
        multi_active  = val.get('families_with_multiple_active_versions', [])

        versioning_issues = {
            'domain_map_missing':    missing_map,
            'domain_map_unexpected': unexpected_map,
            'families_no_active':    no_active,
            'families_multi_active': multi_active,
        }

        has_versioning_issues = any([missing_map, unexpected_map, no_active, multi_active])

        if has_versioning_issues:
            out("9. VERSIONING SYSTEM ISSUES:")
            out("-" * 80)
            if missing_map:
                out(f"  Tables in DB with NO domain_family_map entry ({len(missing_map)}):")
                for t in missing_map:
                    out(f"    - {t}")
            if unexpected_map:
                out(f"  Phantom entries in domain_family_map (table does not exist in DB) ({len(unexpected_map)}):")
                for t in unexpected_map:
                    out(f"    - {t}")
            if no_active:
                out(f"  Version families with NO active version: {no_active}")
            if multi_active:
                out("  Version families with MULTIPLE active versions:")
                for entry in multi_active:
                    out(f"    - family {entry['version_family_id']}: versions {entry['active_version_ids']}")
            out()
        else:
            out("9. Versioning system: OK")
            out()

    # ------------------------------------------------------------------ #
    # Summary
    # ------------------------------------------------------------------ #
    correct_count = len(common_tables) - len(col_mismatches)
    is_synchronized = not any([
        missing_from_erd, col_mismatches, audit_field_mismatches, unpopulated_audit,
        index_mismatches, fk_mismatches, check_mismatches,
        versioning_issues.get('domain_map_missing'),
        versioning_issues.get('families_no_active'),
        versioning_issues.get('families_multi_active'),
    ])

    out("=" * 80)
    out("SUMMARY:")
    out("-" * 80)
    out(f"  Tables documented correctly:              {correct_count:>4}")
    out(f"  Tables with column mismatches:            {len(col_mismatches):>4}")
    out(f"  Tables with audit field mismatches:       {len(audit_field_mismatches):>4}")
    out(f"  Tables with unpopulated audit fields:     {len(unpopulated_audit):>4}")
    out(f"  Tables missing from ERD:                  {len(missing_from_erd):>4}")
    out(f"  Tables in ERD but not DB:                 {len(missing_from_db):>4}")
    if has_structural:
        out(f"  Tables with index mismatches:             {len(index_mismatches):>4}")
        out(f"  Tables with FK mismatches:                {len(fk_mismatches):>4}")
        out(f"  Tables with CHECK mismatches:             {len(check_mismatches):>4}")
        out(f"  domain_family_map missing tables:         {len(versioning_issues.get('domain_map_missing', [])):>4}")
    out()

    if is_synchronized:
        out("STATUS: ERD IS SYNCHRONIZED WITH DATABASE")
    else:
        out("STATUS: ERD NEEDS UPDATES")
        if missing_from_erd or col_mismatches:
            out("  Tip: Run generate_erd_from_audit.py to regenerate the ERD from the live DB")

    out("=" * 80 + "\n")

    return {
        'missing_from_erd':        sorted(missing_from_erd),
        'missing_from_db':         missing_from_db,
        'column_mismatches':       col_mismatches,
        'audit_field_mismatches':  audit_field_mismatches,
        'unpopulated_audit_fields': unpopulated_audit,
        'index_mismatches':        index_mismatches,
        'fk_mismatches':           fk_mismatches,
        'check_mismatches':        check_mismatches,
        'versioning_issues':       versioning_issues,
        'correct_count':           correct_count,
        'is_synchronized':         is_synchronized,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Verify hand-written ERD documentation against a database audit snapshot',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Run from database/schema/:

    python ../audit/verify_erd_against_audit.py COEQWAL_SCENARIOS_DB_ERD.md ../../audits/latest.json

Or with absolute paths from anywhere:

    python verify_erd_against_audit.py /path/to/ERD.md /path/to/audit.json
        """,
    )
    parser.add_argument('erd_path',   type=Path, help='Path to ERD markdown file')
    parser.add_argument('audit_path', type=Path, help='Path to audit JSON file')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Show full column lists for tables with mismatches',
    )
    parser.add_argument(
        '--json', action='store_true',
        help='Output results as JSON only (suppresses human-readable report)',
    )

    args = parser.parse_args()

    if not args.erd_path.exists():
        print(f"Error: ERD file not found: {args.erd_path}", file=sys.stderr)
        sys.exit(1)
    if not args.audit_path.exists():
        print(f"Error: Audit file not found: {args.audit_path}", file=sys.stderr)
        sys.exit(1)

    if not args.json:
        print(f"ERD File:   {args.erd_path.name}")
        print(f"Audit File: {args.audit_path.name}")

    erd_tables  = parse_erd_tables(args.erd_path)
    audit_data  = load_audit_data(args.audit_path)

    if not args.json:
        print(f"\nERD Tables Documented: {len(erd_tables)}")
        print(f"Audit Tables Found:    {len(audit_data['tables'])}")
        if audit_data['has_structural_data']:
            print(f"Indexes in audit:      {len(audit_data['indexes'])}")
            print(f"Foreign keys in audit: {len(audit_data['foreign_keys'])}")

    results = compare_schemas(
        erd_tables, audit_data,
        verbose=args.verbose,
        quiet=args.json,
    )

    if args.json:
        print(json.dumps(results, indent=2))

    sys.exit(0 if results['is_synchronized'] else 1)


if __name__ == '__main__':
    main()
