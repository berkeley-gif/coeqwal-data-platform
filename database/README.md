# COEQWAL Database

PostgreSQL database for COEQWAL scenario data, network, tiers, and statistics topology.

## Directory structure

```
database/
├── audit/                     # Audit and verification scripts
│   ├── generate_erd_from_audit.py
│   ├── verify_erd_against_audit.py
│   └── README.md
├── schema/                    # ERD and schema documentation
│   └── COEQWAL_SCENARIOS_DB_ERD.md
├── scripts/
│   ├── sql/                   # SQL scripts
│   │   ├── 09_statistics/     # Reservoir statistics tables
│   │   ├── 10_mi_statistics/  # Municipal & Industrial statistics
│   │   ├── 11_ag_statistics/  # Agricultural statistics
│   │   └── validate_data_integrity.sql
└── utils/
    └── db_audit_lambda/       # Database audit Lambda function
```

## Schema layers

The database follows a layered architecture:

| Layer | Prefix | Purpose |
|-------|--------|---------|
| 00_VERSIONING | `version_*`, `developer` | Version control and developer tracking |
| 01_LOOKUP | Various | Reference/lookup tables |
| 05_THEMES_SCENARIOS | `theme`, `scenario`, etc. | Scenario management |
| 09_STATISTICS | `reservoir_*` | Reservoir statistics |
| 10_MI_STATISTICS | `du_*`, `mi_*`, `cws_*` | Municipal & Industrial statistics |
| 11_AG_STATISTICS | `ag_*` | Agricultural statistics |

## Schema implementation status

| ERD Section | Key Tables | SQL Scripts | Status |
|-------------|------------|-------------|--------|
| 00_VERSIONING | version_family, version, developer | 00_create_helper_functions.sql | Implemented |
| 01_LOOKUP | hydrologic_region, source, model_source | 03_create_and_load_network_lookups.sql | Implemented |
| 05_THEMES_SCENARIOS | theme, scenario, hydroclimate | create_scenario_tables.sql | Implemented |
| 09_STATISTICS | reservoir_entity, reservoir_monthly_percentile | 09_statistics/*.sql | Implemented |
| 10_MI_STATISTICS | du_urban_entity, mi_contractor | 10_mi_statistics/*.sql | Implemented |
| 11_AG_STATISTICS | du_agriculture_entity, ag_aggregate_entity | 11_ag_statistics/*.sql | Implemented |

---

## Auditing and verification

### Regular audits

Run these audits periodically to ensure database integrity:

#### 1. Database structure audit

Run the Lambda audit function to capture current database state:

```bash
# Via AWS CLI
aws lambda invoke --function-name coeqwal-database-audit --region us-west-2 response.json
cat response.json

# Download reports from S3
aws s3 ls s3://coeqwal-model-run/database_audits/ --recursive | tail -5
aws s3 cp s3://coeqwal-model-run/database_audits/audit_YYYYMMDD_HHMMSS.json ./audits/
aws s3 cp s3://coeqwal-model-run/database_audits/tables_summary_YYYYMMDD_HHMMSS.csv ./audits/
```

See [utils/db_audit_lambda/README.md](utils/db_audit_lambda/README.md) for full documentation.

#### 2. ERD vs database verification

Compare the documented ERD against actual database structure:

```bash
cd database/audit

# Verify ERD matches database
python verify_erd_against_audit.py ../schema/COEQWAL_SCENARIOS_DB_ERD.md ../../audits/latest.json

# Generate fresh ERD from database audit (if updates needed)
python generate_erd_from_audit.py ../../audits/latest.json ../schema/GENERATED_ERD.md
```

See [audit/README.md](audit/README.md) for full documentation.

#### 3. Data integrity checks (after ETL runs)

Run these SQL queries to verify data integrity:

```sql
-- Check for orphaned statistics (no matching scenario)
SELECT 'reservoir_period_summary' as table_name, COUNT(*) as orphans
FROM reservoir_period_summary rps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = rps.scenario_id)
UNION ALL
SELECT 'mi_contractor_period_summary', COUNT(*)
FROM mi_contractor_period_summary mps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = mps.scenario_id)
UNION ALL
SELECT 'ag_aggregate_period_summary', COUNT(*)
FROM ag_aggregate_period_summary aps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = aps.scenario_id);

-- Check scenarios have statistics data
SELECT s.id, s.name,
       (SELECT COUNT(*) FROM reservoir_period_summary rps WHERE rps.scenario_id = s.id) as reservoir_stats,
       (SELECT COUNT(*) FROM mi_contractor_period_summary mps WHERE mps.scenario_id = s.id) as mi_stats,
       (SELECT COUNT(*) FROM ag_aggregate_period_summary aps WHERE aps.scenario_id = s.id) as ag_stats
FROM scenario s
ORDER BY s.id;

-- Check for NULL audit fields (should be 0)
SELECT 'Tables missing created_by' as check_type,
       (SELECT COUNT(*) FROM reservoir_entity WHERE created_by IS NULL) +
       (SELECT COUNT(*) FROM du_urban_entity WHERE created_by IS NULL) +
       (SELECT COUNT(*) FROM mi_contractor WHERE created_by IS NULL) as count;

-- Check water_month values are valid (1-12)
SELECT 'Invalid water_month values' as check_type,
       (SELECT COUNT(*) FROM reservoir_monthly_percentile WHERE water_month NOT BETWEEN 1 AND 12) +
       (SELECT COUNT(*) FROM du_delivery_monthly WHERE water_month NOT BETWEEN 1 AND 12) as count;
```

### Audit schedule

| Audit | Frequency | Owner | Notes |
|-------|-----------|-------|-------|
| Lambda structure audit | Weekly-monthly | Automated | Check S3 for reports |
| ERD verification | Monthly | Developer | Before major releases |
| Data integrity checks | After ETL | ETL pipeline | Run via validation scripts |
| Record count comparison | Weekly-montly | Developer | Compare against expected counts |

### Audit reports location

- **S3**: `s3://coeqwal-model-run/database_audits/`
- **Local**: `./audits/` (gitignored)

---

## Data validation tools

### ETL validation

CSV validation scripts for verifying ETL output:

```bash
# Compare CSVs
python etl/coeqwal-etl/python-code/validate_csvs.py \
  --reference data/reference/expected.csv \
  --output data/output/actual.csv

# Enhanced validation with reports
python etl/coeqwal-etl/python-code/validate_csvs_improved.py \
  --reference data/reference/ \
  --output data/output/ \
  --report validation_report.json
```

### Database constraints

Tables include CHECK constraints for data validation:
- `water_month` must be 1-12
- `tier_level` must be 1-4 (enforced: `CHECK (tier_level BETWEEN 1 AND 4 OR tier_level IS NULL)`)
- `location_type` must be valid enum value

---

## Development setup

### Connect to production (read-only)

```bash
# Set up SSO credentials
aws sso login --profile coeqwal-dev

# Connect via psql
psql "postgresql://user:pass@coeqwal-db.xxxxx.us-west-2.rds.amazonaws.com:5432/coeqwal"
```

---

## Troubleshooting

### Potential issues

**Missing statistics for scenario**
- Check ETL pipeline completed successfully
- Verify scenario_id exists in scenario table
- Run data integrity checks above

**ERD out of sync**
- Run ERD verification script
- Update COEQWAL_SCENARIOS_DB_ERD.md
- Document changes in schema/.archive/

**Audit Lambda fails**
- Check VPC configuration
- Verify security group allows Lambda → RDS
- See [utils/db_audit_lambda/README.md](utils/db_audit_lambda/README.md)
