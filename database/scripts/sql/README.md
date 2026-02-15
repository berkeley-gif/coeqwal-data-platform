# SQL Scripts

SQL scripts for database schema management, data operations, and validation.

## Script Organization

Scripts are numbered for execution order and organized by feature:

| Directory | Purpose |
|-----------|---------|
| `00_*` - `03_*` | Foundation: helper functions, lookup tables |
| `09_statistics/` | Reservoir statistics tables |
| `10_mi_statistics/` | Municipal & Industrial statistics |
| `11_ag_statistics/` | Agricultural statistics |
| `.archive/` | Deprecated/historical scripts |

## Key Scripts

| Script | Purpose |
|--------|---------|
| `create_scenario_tables.sql` | Core scenario management tables |
| `create_tier_location_result_table.sql` | Tier location results |
| `validate_data_integrity.sql` | Data quality validation queries |

## Running Validation

After ETL runs, validate data integrity:

```bash
psql $DATABASE_URL -f validate_data_integrity.sql
```

Review results for any non-zero counts in integrity checks.

## Naming Conventions

- `create_*.sql` - Creates new tables
- `*_from_s3.sql` - Loads data from S3
- `*_local.sql` - Local development variants (gitignored)
- `*_cloud9.sql` - Cloud9 variants (gitignored)
- `upsert_*.sql` - Insert or update operations
- `migrate_*.sql` - Schema migrations

## Adding New Scripts

1. Use appropriate numbering (e.g., `09_statistics/11_new_feature.sql`)
2. Include table comments and audit fields
3. Add CHECK constraints for data validation
4. Update the ERD documentation
5. Run validation after applying
