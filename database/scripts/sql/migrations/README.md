# Applied Migrations

These migrations have been run against the production database. They are kept here as a historical record of schema evolution, **not** as scripts to rerun.

| File | Description | Status | Applied |
|------|-------------|--------|---------|
| `01_add_missing_indexes_fks_checks.sql` | Added missing indexes and FK constraints across early tables | Applied | 2025 |
| `02_fix_layer00_fk_rules_and_indexes.sql` | Corrected FK ON DELETE/UPDATE rules and cleaned up redundant indexes in Layer 00 | Applied | 2025 |
| `03_create_new_layer01_lookup_tables.sql` | Created `watershed`, `calsim_model_variable_type`, `derived_variable_type`; fixed permissions on `variable_type` | Applied | 2026-03-01 |
| `04_layer01_provenance_and_domain_map.sql` | Updated `created_by`/`updated_by` for Layer 01 tables; inserted missing entries into `domain_family_map` | Applied | 2026-03-01 |
| `05_layer01_cleanup.sql` | Renamed `EXTERNAL` → `EXPORT` in `hydrologic_region`; dropped `calsim_variable_type`; fixed provenance timestamps for new tables | Applied | 2026-03-01 |
| `06_layer_restructure_slr_scenario.sql` | Creates `slr` table; adds `source_scenario_id` and `slr_id` to `scenario`; drops `slr_value`/`slr_unit_id` from `hydroclimate`; seeds SLR values | **Pending** | — |
| `07_reclassify_assumptions_operations.sql` | Reclassifies TUCP, SGMA, infrastructure, flow, BiOps rows from `assumption_definition` → `operation_definition`; migrates `scenario_key_assumption_link` accordingly; removes SLR rows from assumptions; adds new land use assumption rows | **Pending** | — |

## Important notes

- Migrations 01–02 were run via `psql $SUPERUSER_URL` (DDL changes; table owner is `postgres`)
- Migrations 03–05 were run via `psql $SUPERUSER_URL` (DDL) with `DISABLE TRIGGER USER` blocks for data provenance fixes
- Seed data `INSERT`s inside migration 03 should have been run as `$DATABASE_URL` (see `database/README.md` — Migration authoring rule)
- Migration 06 requires `$SUPERUSER_URL` (CREATE TABLE, ALTER TABLE, CREATE TRIGGER). Seed INSERTs inside the migration use explicit `created_by = 2`.
- Migration 07 requires `$SUPERUSER_URL` for `DISABLE TRIGGER USER` DDL on RDS.

## Naming convention for future migrations

Use sequential numbering: `08_<description>.sql`, `09_<description>.sql`, etc.
