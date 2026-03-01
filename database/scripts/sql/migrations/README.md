# Applied Migrations

These migrations have been run against the production database. They are kept here as a historical record of schema evolution, **not** as scripts to rerun.

| File | Description | Status | Applied |
|------|-------------|--------|---------|
| `01_add_missing_indexes_fks_checks.sql` | Added missing indexes and FK constraints across early tables | Applied | 2025 |
| `02_fix_layer00_fk_rules_and_indexes.sql` | Corrected FK ON DELETE/UPDATE rules and cleaned up redundant indexes in Layer 00 | Applied | 2025 |
| `03_create_new_layer01_lookup_tables.sql` | Created `watershed`, `calsim_model_variable_type`, `derived_variable_type`; fixed permissions on `variable_type` | Applied | 2026-03-01 |
| `04_layer01_provenance_and_domain_map.sql` | Updated `created_by`/`updated_by` for Layer 01 tables; inserted missing entries into `domain_family_map` | Applied | 2026-03-01 |
| `05_layer01_cleanup.sql` | Renamed `EXTERNAL` → `EXPORT` in `hydrologic_region`; dropped `calsim_variable_type`; fixed provenance timestamps for new tables | Applied | 2026-03-01 |

## Important notes

- Migrations 01–02 were run via `psql $SUPERUSER_URL` (DDL changes; table owner is `postgres`)
- Migrations 03–05 were run via `psql $SUPERUSER_URL` (DDL) with `DISABLE TRIGGER USER` blocks for data provenance fixes
- Seed data `INSERT`s inside migration 03 should have been run as `$DATABASE_URL` (see `database/README.md` — Migration authoring rule)

## Naming convention for future migrations

Use sequential numbering: `06_<description>.sql`, `07_<description>.sql`, etc.
