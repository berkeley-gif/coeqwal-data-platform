# SQL Scripts

SQL utility scripts. Almost everything that has been applied to RDS
already lives in [`.archive/`](.archive/). Only active utility DDL and
two layer verifier directories remain at the top level.

## What's here

| Path | What it is |
|------|------------|
| `create_scenario_tables.sql` | Active superuser DDL. Creates the scenario / hydroclimate / theme tables before `upsert_scenario_data.sql` runs. |
| `create_tier_location_table.sql` | Active superuser DDL. Creates `tier_location`. |
| `create_tier_location_result_table.sql` | Active superuser DDL. Creates `tier_location_result`. |
| `find_tier_locations.sql` | Ad-hoc query utility for tier-location lookups. |
| `upsert_scenario_data.sql` | UPSERT for scenario / theme / link tables. |
| `validate_data_integrity.sql` | Post-ETL integrity check (`psql $DATABASE_URL -f validate_data_integrity.sql`). |
| `01_lookup/` | `09_verify_level01.sql` + `inspect_layer01.sql` for the lookup layer. |
| `02_network/` | `09_verify_level02.sql` for the network layer. |
| `.archive/` | Historical one-shot DDL already applied to RDS. Includes `00_versioning/`, `11_reservoir_statistics/`, `12_mi_statistics/`, `13_ag_statistics/`, `14_channel_entity/`, `migrations/`, plus the top-level `00_create_helper_functions.sql` and `46_*.sql` - `57_*.sql` migrations. Re-apply from here only if rebuilding from scratch. |

## Running validation

After ETL runs, validate data integrity:

```bash
psql $DATABASE_URL -f database/scripts/sql/validate_data_integrity.sql
```

Review results for any non-zero counts in integrity checks.

## Naming conventions

- `create_*.sql` - Creates new tables (superuser DDL)
- `upsert_*.sql` - Insert or update operations
- `*_local.sql` / `*_cloud9.sql` - Local / Cloud9 variants (gitignored)
- `*_from_s3.sql` (in `.archive/`) - Deprecated. Seed data is now loaded from the repo via `\copy`
- `migrate_*.sql` / numbered `NN_*.sql` (in `.archive/`) - Historical schema migrations

## Adding a new migration

When the schema needs to change again:

1. Write a new numbered SQL file. Use the next free number after the
   highest one in `.archive/migrations/` or the top-level
   `.archive/46_*` - `57_*` series.
2. Apply it with `psql $SUPERUSER_URL -f <path>` on Cloud9.
3. Move the applied file into `.archive/migrations/` (or top-level
   `.archive/` if it is a one-off rather than a numbered migration)
   in the same commit. The folder is the as-shipped record of RDS,
   not a rerun queue.
4. Update the ERD documentation.
5. Run `validate_data_integrity.sql`.
