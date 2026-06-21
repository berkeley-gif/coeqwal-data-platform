# SQL Scripts

SQL utility scripts. Only active utility DDL remains here at the top level. Almost everything that has already been applied to RDS lives in the archive at [`database/sql_archive/`](../../sql_archive/), organized by domain: `00_versioning/`, `02_network_layer/`, `03_entity_layers/` (`ag`, `channel`, `mi`, `reservoir`), `04_scenario/`, `05_tier_data/`, `06_lookup_data_updates/`, `07_audits_and_fixes/`, and `log/` (the numbered migration history). Re-apply from there only when rebuilding from scratch.

## What's here

| Path | What it is |
|------|------------|
| `audit_cleanup.sql` | Idempotent schema-audit cleanup script for the mechanical drift fixes. See [`../../SCHEMA_BACKLOG.md`](../../SCHEMA_BACKLOG.md) § 0. |
| `create_scenario_tables.sql` | Active superuser DDL. Creates the scenario / hydroclimate / theme tables before `upsert_scenario_data.sql` runs. |
| `create_tier_location_table.sql` | Active superuser DDL. Creates `tier_location`. |
| `create_tier_location_result_table.sql` | Active superuser DDL. Creates `tier_location_result`. |
| `upsert_scenario_data.sql` | UPSERT for scenario / theme / link tables. |
| `correct_projection_year.sql` | One-off fix for `hydroclimate.projection_year` rows that were stored as floats. |
| `find_tier_locations.sql` | Ad-hoc query utility for tier-location lookups. |
| `validate_data_integrity.sql` | Post-ETL integrity check (`psql $DATABASE_URL -f validate_data_integrity.sql`). |
| `01_lookup/` | Layer 01 helpers: `inspect_layer01.sql` and `09_verify_level01.sql`. |
| `02_network/` | Layer 02 helper: `09_verify_level02.sql`. |
| `actions/` | Numbered data-load actions for adding hydroclimates and scenario batches. |

## Running validation

After ETL runs, validate data integrity:

```bash
psql $DATABASE_URL -f database/scripts/sql/validate_data_integrity.sql
```

Review results for any non-zero counts in integrity checks.

## Naming conventions

- `create_*.sql`: creates new tables (superuser DDL)
- `upsert_*.sql`: insert or update operations
- `*_local.sql` / `*_cloud9.sql`: local / Cloud9 variants (gitignored)
- `*_from_s3.sql` (in `sql_archive/`): deprecated. Seed data is now loaded from the repo via `\copy`
- `migrate_*.sql` / numbered `NN_*.sql` (in `sql_archive/`): historical schema migrations

## Migrations

Schema migrations are owned by `elehmer`. The archive at [`database/sql_archive/`](../../sql_archive/) is the as-shipped record of RDS, organized by domain and numbered in apply order (highest is currently `57_*` under `04_scenario/`). It is the history of what has run, not a rerun queue. For the audit workflow that checks the live schema against the ERD, see [`../../audit/README.md`](../../audit/README.md).
