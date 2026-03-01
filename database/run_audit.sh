#!/bin/bash
# =============================================================================
# DATABASE AUDIT RUNNER
# =============================================================================
# Runs the local database audit via run_local_audit.py.
# Output is written to ../audits/ (repo root) as:
#   audit_YYYYMMDD_HHMMSS.json
#   tables_summary_YYYYMMDD_HHMMSS.csv
#   latest.json  (symlink to the most recent JSON)
#
# Prerequisites:
#   pip install psycopg2-binary pandas
#
# Usage:
#   export DATABASE_URL="postgresql://user:pass@host:5432/coeqwal_scenario"
#   bash database/run_audit.sh
#
# Or from the database/ directory:
#   bash run_audit.sh
# =============================================================================

set -euo pipefail

echo "COEQWAL Database Audit Runner"
echo "=============================="

# Resolve to the database/ directory regardless of where the script is called from
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Check DATABASE_URL ---
if [ -z "${DATABASE_URL:-}" ]; then
    echo ""
    echo "ERROR: DATABASE_URL is not set."
    echo ""
    echo "Set it with your RDS connection string, then re-run:"
    echo ""
    echo "  export DATABASE_URL=\"postgresql://user:password@your-rds-endpoint:5432/coeqwal_scenario\""
    echo ""
    exit 1
fi

echo "DATABASE_URL: $(echo "$DATABASE_URL" | sed 's/:[^:@]*@/:***@/')"
echo ""

# --- Check Python dependencies ---
echo "Checking Python dependencies..."
if ! python3 -c "import psycopg2, pandas" 2>/dev/null; then
    echo ""
    echo "ERROR: Missing required Python packages."
    echo "Install them with:"
    echo "  pip install psycopg2-binary pandas"
    echo ""
    exit 1
fi
echo "Dependencies OK"
echo ""

# --- Run the audit ---
echo "Starting audit..."
python3 "$SCRIPT_DIR/run_local_audit.py"
