# ETL Statistics Pipeline

Calculate and load monthly percentile statistics for reservoir storage.

## Overview

This pipeline processes CalSim model output CSVs to calculate monthly percentile bands for 8 major California reservoirs. The data powers frontend visualization charts showing the distribution of storage levels across water years.

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  S3 / Local     │───▶│  calculate_      │───▶│  PostgreSQL     │───▶│  API Endpoint   │
│  CSV Files      │    │  reservoir_      │    │  Database       │    │  /api/statistics│
│                 │    │  percentiles.py  │    │                 │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## Target Reservoirs

| Code | Reservoir | Capacity (TAF) | Dead Pool (TAF) |
|------|-----------|----------------|-----------------|
| S_SHSTA | Shasta | 4,552 | 115 |
| S_TRNTY | Trinity | 2,448 | 105 |
| S_OROVL | Oroville | 3,537 | 850 |
| S_FOLSM | Folsom | 975 | 115 |
| S_MELON | New Melones | 2,400 | 300 |
| S_MLRTN | Millerton | 520 | 115 |
| S_SLUIS_CVP | San Luis (CVP) | 1,062 | 15 |
| S_SLUIS_SWP | San Luis (SWP) | 979 | 10 |

Source: `database/seed_tables/04_calsim_data/reservoir_entity.csv`

## Output Statistics

For each reservoir and water month (Oct=1 ... Sep=12):
- **Percentiles**: q10, q20, q30, q40, q50 (median), q60, q70, q80, q90
- **Summary**: min, max, mean
- **Values**: Expressed as % of reservoir capacity (0-100+)

## Quick Start

### Prerequisites
```bash
pip install pandas numpy boto3  # boto3 only needed for S3 access
```

### Step 1: Create Database Table
```bash
psql $DATABASE_URL -f database/schema/reservoir_percentile_table.sql
```

### Step 2: Calculate Percentiles
```bash
# From local CSV file
python etl/statistics/calculate_reservoir_percentiles.py \
  --scenario s0020 \
  --csv-path etl/pipelines/s0020_coeqwal_calsim_output.csv \
  --output-csv /tmp/s0020_percentiles.csv

# From S3 (requires AWS credentials)
python etl/statistics/calculate_reservoir_percentiles.py \
  --scenario s0020 \
  --output-csv /tmp/s0020_percentiles.csv
```

### Step 3: Load to Database
```bash
# Using COPY (fast bulk load)
psql $DATABASE_URL -c "
  COPY reservoir_monthly_percentile (
    scenario_short_code, reservoir_code, water_month,
    q10, q20, q30, q40, q50, q60, q70, q80, q90,
    min_value, max_value, mean_value, max_capacity_taf
  ) FROM '/tmp/s0020_percentiles.csv' CSV HEADER;
"

# Or use upsert for updates (slower but handles duplicates)
# See database/schema/reservoir_percentile_table.sql for upsert function
```

### Step 4: Verify Data
```bash
psql $DATABASE_URL -c "
  SELECT scenario_short_code, reservoir_code, COUNT(*)
  FROM reservoir_monthly_percentile
  GROUP BY scenario_short_code, reservoir_code;
"
```

## CLI Reference

```bash
python calculate_reservoir_percentiles.py [OPTIONS]

Options:
  --scenario, -s TEXT     Scenario ID (e.g., s0020)
  --all-scenarios         Process all known scenarios
  --csv-path TEXT         Local CSV file path (uses S3 if not provided)
  --output-json           Output results as JSON to stdout
  --output-csv TEXT       Output results as CSV to specified path
```

### Examples

```bash
# Preview results as JSON
python calculate_reservoir_percentiles.py -s s0020 \
  --csv-path etl/pipelines/s0020_coeqwal_calsim_output.csv \
  --output-json

# Process all scenarios from S3
python calculate_reservoir_percentiles.py --all-scenarios \
  --output-csv /tmp/all_percentiles.csv

# Process single scenario, output CSV
python calculate_reservoir_percentiles.py -s s0020 \
  --output-csv /tmp/s0020_percentiles.csv
```

## CSV Input Format

The script expects CalSim DSS-export CSV format:

```
Row 0 (a):     CALSIM, CALSIM, CALSIM, ...     (source)
Row 1 (b):     A17, S_SHSTA, S_FOLSM, ...      (variable names)
Row 2 (c):     SURFACE-AREA, STORAGE, ...      (descriptions)
Row 3 (e):     1MON, 1MON, 1MON, ...           (time step)
Row 4 (f):     L2020A, L2020A, ...             (dataset)
Row 5 (type):  PER-AVER, PER-AVER, ...         (data type)
Row 6 (units): ACRES, TAF, TAF, ...            (units)
Row 7+:        1921-10-31 00:00:00, 0.0, ...   (data)
```

Key columns we extract:
- Column 0: DateTime
- S_SHSTA, S_TRNTY, S_OROVL, S_FOLSM, S_MELON, S_MLRTN, S_SLUIS_CVP, S_SLUIS_SWP

## S3 Bucket Structure

```
s3://coeqwal-model-run/
└── scenario/
    └── {scenario_id}/
        └── csv/
            └── {scenario_id}_coeqwal_calsim_output.csv
```

## API

After loading data, the statistics endpoints are available under `/api/statistics/`.

See interactive documentation: https://api.coeqwal.org/docs

## Database Schema

```sql
CREATE TABLE reservoir_monthly_percentile (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    reservoir_code VARCHAR(20) NOT NULL,
    water_month INTEGER NOT NULL,  -- 1-12 (Oct=1, Sep=12)

    -- Percentiles (% of capacity)
    q10, q20, q30, q40, q50, q60, q70, q80, q90 NUMERIC(6,2),

    -- Summary stats
    min_value, max_value, mean_value NUMERIC(6,2),
    max_capacity_taf NUMERIC(10,2),

    -- Audit fields
    is_active BOOLEAN DEFAULT TRUE,
    created_at, updated_at TIMESTAMPTZ,
    created_by, updated_by INTEGER,

    UNIQUE(scenario_short_code, reservoir_code, water_month)
);
```

See: `database/schema/reservoir_percentile_table.sql`

## Troubleshooting

### "No storage columns found"
- Check that the CSV has the expected 7 header rows
- Verify variable names in row 1 match exactly: S_SHSTA, S_TRNTY, etc.

### Memory issues with large CSVs
- The full CSV has ~20,000 columns and 1,200 rows
- Processing requires ~500MB RAM
- Consider filtering columns before loading if needed

### S3 access errors
- Ensure AWS credentials are configured: `aws configure`
- Check bucket name in `S3_BUCKET` environment variable
