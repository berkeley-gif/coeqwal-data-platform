#!/usr/bin/env python3
"""
Local database audit runner
============================
Wraps the Lambda audit logic for local / dev execution — no AWS infrastructure needed.

Connects to $DATABASE_URL, runs the full audit, and saves:
  - audits/audit_YYYYMMDD_HHMMSS.json   (complete audit report)
  - audits/tables_summary_YYYYMMDD_HHMMSS.csv  (per-table overview)
  - audits/latest.json                  (symlink → most recent audit JSON)

Usage:
    export DATABASE_URL="postgresql://user:pass@host:5432/coeqwal_scenario"
    python database/run_local_audit.py

Or from the database/ directory:
    python run_local_audit.py
"""

import json
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2

# Locate the Lambda module relative to this file's directory so it can be
# imported regardless of where the script is invoked from.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE / 'utils' / 'db_audit_lambda'))
from db_audit_lambda import generate_audit_report  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)


def json_serial(obj):
    """JSON serialiser for types not handled by default (datetime, Decimal)."""
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    if hasattr(obj, '__float__') or str(type(obj)) == "<class 'decimal.Decimal'>":
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def run_audit(output_dir: Path) -> Path:
    """
    Run the full audit and write output files.

    Returns the path of the JSON audit file just written.
    """
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.error(
            "DATABASE_URL is not set.\n"
            "Example:\n"
            "  export DATABASE_URL=\"postgresql://user:pass@host:5432/coeqwal_scenario\""
        )
        sys.exit(1)

    logger.info("Connecting to database…")
    try:
        conn = psycopg2.connect(database_url)
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        audit_report = generate_audit_report(conn)
    finally:
        conn.close()

    # --- JSON report ---
    json_path = output_dir / f"audit_{timestamp}.json"
    json_path.write_text(json.dumps(audit_report, indent=2, default=json_serial))
    logger.info(f"JSON report written → {json_path}")

    # --- CSV summary ---
    csv_path = output_dir / f"tables_summary_{timestamp}.csv"
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
    pd.DataFrame(tables_data).to_csv(csv_path, index=False)
    logger.info(f"CSV summary written  → {csv_path}")

    # --- latest.json symlink ---
    latest = output_dir / 'latest.json'
    if latest.is_symlink() or latest.exists():
        latest.unlink()
    latest.symlink_to(json_path.name)   # relative symlink within audits/
    logger.info(f"Symlink updated      → {latest} → {json_path.name}")

    # --- Print quick summary ---
    vs = audit_report['versioning_system']
    val = vs.get('validation', {})
    total_tables = len(audit_report['tables'])
    total_records = sum(t['record_count'] for t in audit_report['tables'] if t['record_count'] > 0)
    print()
    print("=" * 60)
    print("AUDIT COMPLETE")
    print("=" * 60)
    print(f"  Database:            {audit_report['database_info']['database_name']}")
    print(f"  Tables audited:      {total_tables}")
    print(f"  Total records:       {total_records:,}")
    print(f"  Indexes captured:    {len(audit_report['indexes'])}")
    print(f"  Foreign keys:        {len(audit_report['foreign_keys'])}")
    print(f"  CHECK constraints:   {len(audit_report['check_constraints'])}")
    print(f"  UNIQUE constraints:  {len(audit_report['unique_constraints'])}")
    print()
    print(f"  Version families:    {len(vs.get('version_families', []))}")
    missing_map = val.get('domain_map_missing_tables', [])
    if missing_map:
        print(f"  domain_family_map MISSING {len(missing_map)} expected tables:")
        for t in missing_map:
            print(f"    - {t}")
    else:
        print("  domain_family_map:   OK")
    families_no_active = val.get('families_without_active_version', [])
    if families_no_active:
        print(f"  Version families without an active version: {families_no_active}")
    print()
    print(f"  Output: {json_path}")
    print("=" * 60)
    print()

    return json_path


if __name__ == '__main__':
    # Resolve the audits/ directory relative to the repo root (one level up from database/)
    repo_root = _HERE.parent
    audits_dir = repo_root / 'audits'
    run_audit(audits_dir)
