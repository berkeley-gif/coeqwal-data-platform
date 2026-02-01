#!/bin/bash
# =============================================================================
# LOCAL DEV RUN SCRIPT
# =============================================================================
# Run the ETL statistics calculations locally using a CSV file
#
# Usage:
#   ./dev_run.sh                    # Dry run (no DB writes)
#   ./dev_run.sh --write-db         # Write to database
#   ./dev_run.sh --output-sql       # Generate SQL file
#   ./dev_run.sh --output-json      # Output JSON to stdout
#
# Prerequisites:
#   - Python 3.8+ with pandas, numpy
#   - For --write-db: DATABASE_URL environment variable set
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV_FILE="${SCRIPT_DIR}/../pipelines/s0020_coeqwal_calsim_output.csv"
SCENARIO="s0020"

# Check if CSV exists
if [ ! -f "$CSV_FILE" ]; then
    echo "ERROR: CSV file not found at $CSV_FILE"
    echo "Please ensure the file exists or update the path"
    exit 1
fi

echo "=============================================="
echo "COEQWAL Statistics ETL - Local Dev Run"
echo "=============================================="
echo "CSV File: $CSV_FILE"
echo "Scenario: $SCENARIO"
echo ""

# Parse arguments
EXTRA_ARGS=""
if [[ "$*" == *"--write-db"* ]]; then
    if [ -z "$DATABASE_URL" ]; then
        echo "ERROR: DATABASE_URL environment variable not set"
        echo "Set it with: export DATABASE_URL='postgresql://user:pass@host:5432/db'"
        exit 1
    fi
    EXTRA_ARGS="--write-db"
    echo "Mode: WRITE TO DATABASE"
elif [[ "$*" == *"--output-sql"* ]]; then
    EXTRA_ARGS="--output-sql ${SCRIPT_DIR}/output_${SCENARIO}.sql"
    echo "Mode: OUTPUT SQL FILE"
elif [[ "$*" == *"--output-json"* ]]; then
    EXTRA_ARGS="--output-json"
    echo "Mode: OUTPUT JSON"
else
    EXTRA_ARGS="--dry-run"
    echo "Mode: DRY RUN (no output)"
fi
echo ""

# Run percentiles calculation
echo "----------------------------------------------"
echo "1. Running Percentile Calculations..."
echo "----------------------------------------------"
cd "$SCRIPT_DIR"
python reservoirs/calculate_reservoir_percentiles.py \
    --scenario "$SCENARIO" \
    --csv-path "$CSV_FILE" \
    --dry-run

# Run full statistics calculation
echo ""
echo "----------------------------------------------"
echo "2. Running Full Statistics Calculations..."
echo "----------------------------------------------"
python reservoirs/calculate_reservoir_statistics.py \
    --scenario "$SCENARIO" \
    --csv-path "$CSV_FILE" \
    $EXTRA_ARGS

echo ""
echo "=============================================="
echo "Dev run complete!"
echo "=============================================="
