# SQL Scripts

SQL scripts for database schema management, data operations, and validation.

## Script organization

Scripts are numbered to match the database layer architecture:

| Directory | Layer | Purpose |
|-----------|-------|---------|
| `00_*` - `03_*` | 00-03 | Foundation: helper functions, lookup tables |
| `11_reservoir_statistics/` | 11 | Reservoir storage & spill statistics |
| `12_mi_statistics/` | 12 | Municipal & Industrial delivery statistics |
| `13_ag_statistics/` | 13 | Agricultural delivery statistics |
| `.archive/` | — | Deprecated/historical scripts |

See [../README.md](../README.md) for the full layer architecture.

## Key scripts

| Script | Purpose |
|--------|---------|
| `create_scenario_tables.sql` | Core scenario management tables |
| `create_tier_location_result_table.sql` | Tier location results |
| `validate_data_integrity.sql` | Data quality validation queries |

## Running validation

After ETL runs, validate data integrity:

```bash
psql $DATABASE_URL -f validate_data_integrity.sql
```

Review results for any non-zero counts in integrity checks.

## Naming conventions

- `create_*.sql` - Creates new tables
- `*_from_s3.sql` - Loads data from S3
- `*_local.sql` - Local development variants (gitignored)
- `*_cloud9.sql` - Cloud9 variants (gitignored)
- `upsert_*.sql` - Insert or update operations
- `migrate_*.sql` - Schema migrations

## Adding new scripts

1. Place in the appropriate layer directory (e.g., `11_reservoir_statistics/`)

2. Include table comments and audit fields
3. Add CHECK constraints for data validation
4. Update the ERD documentation
5. Run validation after applying
