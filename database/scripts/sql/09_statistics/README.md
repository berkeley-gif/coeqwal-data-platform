# 09_STATISTICS Layer SQL Scripts

SQL scripts for creating and loading the Statistics Layer tables.

## Tables

| Table | Description | Seed CSV |
|-------|-------------|----------|
| `reservoir_entity` | 92 reservoirs with capacity, location, attributes | `04_calsim_data/reservoir_entity.csv` |
| `reservoir_group` | Group definitions (major, cvp, swp, tier) | `04_calsim_data/reservoir_group.csv` |
| `reservoir_group_member` | Reservoir-to-group memberships | `04_calsim_data/reservoir_group_member.csv` |
| `reservoir_monthly_percentile` | Monthly percentile statistics for reservoir storage | ETL-generated |

## Workflow

### Step 1: Upload CSVs to S3

```bash
# From project root
aws s3 cp database/seed_tables/04_calsim_data/reservoir_entity.csv \
    s3://coeqwal-seeds-dev/04_calsim_data/reservoir_entity.csv

aws s3 cp database/seed_tables/04_calsim_data/reservoir_group.csv \
    s3://coeqwal-seeds-dev/04_calsim_data/reservoir_group.csv

aws s3 cp database/seed_tables/04_calsim_data/reservoir_group_member.csv \
    s3://coeqwal-seeds-dev/04_calsim_data/reservoir_group_member.csv
```

### Step 2: Create Tables

```bash
psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
    -f database/scripts/sql/09_statistics/01_create_reservoir_entity_tables.sql
```

### Step 3: Load Data from S3

```bash
psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
    -f database/scripts/sql/09_statistics/02_load_reservoir_entity_from_s3.sql
```

## Quick Start (All Steps)

```bash
# Set connection variables
export DB_HOST="your-rds-host.amazonaws.com"
export DB_USER="postgres"
export DB_NAME="coeqwal"

# Upload to S3
for file in reservoir_entity reservoir_group reservoir_group_member; do
    aws s3 cp database/seed_tables/04_calsim_data/${file}.csv \
        s3://coeqwal-seeds-dev/04_calsim_data/${file}.csv
done

# Create tables and load data
psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
    -f database/scripts/sql/09_statistics/01_create_reservoir_entity_tables.sql

psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
    -f database/scripts/sql/09_statistics/02_load_reservoir_entity_from_s3.sql
```

## Verification Queries

```sql
-- Check major reservoirs
SELECT short_code, name, capacity_taf
FROM reservoir_entity
WHERE has_tiers = TRUE
ORDER BY capacity_taf DESC;

-- Check group memberships
SELECT rg.short_code, COUNT(*) as members
FROM reservoir_group rg
JOIN reservoir_group_member rgm ON rg.id = rgm.reservoir_group_id
GROUP BY rg.short_code;

-- Get reservoirs in "major" group
SELECT re.short_code, re.name, re.capacity_taf
FROM reservoir_entity re
JOIN reservoir_group_member rgm ON re.id = rgm.reservoir_entity_id
JOIN reservoir_group rg ON rgm.reservoir_group_id = rg.id
WHERE rg.short_code = 'major'
ORDER BY rgm.display_order;

-- Regional aggregation (NOD vs SOD)
SELECT
    CASE
        WHEN hydrologic_region_id = 1 THEN 'NOD'
        WHEN hydrologic_region_id IN (2, 4) THEN 'SOD'
        ELSE 'Other'
    END as region,
    COUNT(*) as reservoir_count,
    SUM(capacity_taf) as total_capacity_taf
FROM reservoir_entity
WHERE has_tiers = TRUE
GROUP BY 1;
```

## Reservoir Percentile Table

### Step 4: Create Percentile Table

```bash
psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
    -f database/scripts/sql/09_statistics/03_create_reservoir_percentile_table.sql
```

### Step 5: Load Percentile Data (via ETL)

```bash
python etl/statistics/calculate_reservoir_percentiles.py --scenario s0020
```

### Percentile Table Structure

| Column | Description |
|--------|-------------|
| `scenario_short_code` | Scenario identifier (e.g., s0020) |
| `reservoir_code` | Reservoir variable code (e.g., S_SHSTA) |
| `water_month` | 1-12 (Oct=1, Sep=12) |
| `q0` | Minimum (0th percentile) |
| `q10, q30, q50, q70, q90` | Percentile bands |
| `q100` | Maximum (100th percentile) |
| `mean_value` | Mean storage (% of capacity) |

Note: `capacity_taf` and `dead_pool_taf` are reservoir attributes (from `reservoir_entity`), not stored in the statistics table. The API enriches responses with these values.

### Percentile Verification Queries

```sql
-- Check percentile data for a scenario
SELECT reservoir_code, water_month, q0, q50, q100, mean_value
FROM reservoir_monthly_percentile
WHERE scenario_short_code = 's0020'
ORDER BY reservoir_code, water_month;

-- Monthly summary for a reservoir
SELECT water_month, q10, q50, q90
FROM reservoir_monthly_percentile
WHERE scenario_short_code = 's0020' AND reservoir_code = 'S_SHSTA'
ORDER BY water_month;
```

## Related Files

- ERD: `database/schema/COEQWAL_SCENARIOS_DB_ERD.md` (09_STATISTICS LAYER + ENTITY LAYER)
- Schema: `database/schema/reservoir_percentile_table.sql`
- ETL: `etl/statistics/calculate_reservoir_percentiles.py`
- API: `api/coeqwal-api/routes/reservoir_statistics_endpoints.py`
- Seed CSVs: `database/seed_tables/04_calsim_data/`
- S3 Bucket: `coeqwal-seeds-dev`
