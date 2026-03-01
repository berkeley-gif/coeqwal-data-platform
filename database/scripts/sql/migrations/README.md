# Applied Migrations

These migrations have been run against the production database. They are kept here as a historical record of schema evolution, **not** as scripts to rerun.

| File | Description | Status | Applied |
|------|-------------|--------|---------|
| `01_add_missing_indexes_fks_checks.sql` | Added missing indexes and FK constraints across early tables | Applied | 2025 |
| `02_fix_layer00_fk_rules_and_indexes.sql` | Corrected FK ON DELETE/UPDATE rules and cleaned up redundant indexes in Layer 00 | Applied | 2025 |
| `03_create_new_layer01_lookup_tables.sql` | Created `watershed`, `calsim_model_variable_type`, `derived_variable_type`; fixed permissions on `variable_type` | Applied | 2026-03-01 |
| `04_layer01_provenance_and_domain_map.sql` | Updated `created_by`/`updated_by` for Layer 01 tables; inserted missing entries into `domain_family_map` | Applied | 2026-03-01 |
| `05_layer01_cleanup.sql` | Renamed `EXTERNAL` → `EXPORT` in `hydrologic_region`; dropped `calsim_variable_type`; fixed provenance timestamps for new tables | Applied | 2026-03-01 |
| `06_layer_restructure_slr_scenario.sql` | Creates `slr` table; adds `source_scenario_id` and `slr_id` to `scenario`; drops `slr_value`/`slr_unit_id` from `hydroclimate`; seeds SLR values | Applied | 2026-03-01 |
| `07_reclassify_assumptions_operations.sql` | Creates `assumption_category` and `operation_category` tables; seeds initial category data; reclassifies TUCP, SGMA, infrastructure, flow, BiOps rows from `assumption_definition` → `operation_definition`; migrates link tables; removes SLR rows from assumptions; adds new land use assumption rows | Applied | 2026-03-01 |
| `08_register_new_tables_domain_family_map.sql` | Registers `slr`, `assumption_category`, and `operation_category` in `domain_family_map`; fixes `created_by`/`updated_by` on category seed rows | Applied | 2026-03-01 |
| `09_add_source_to_operation_definition_and_slr.sql` | Adds `source TEXT` column to `operation_definition` and `slr`; sets `source = 'james_gilbert'` for all current rows | Applied | 2026-03-01 |
| `10_add_source_fk_constraints.sql` | Adds FK constraints on `source` → `source` lookup table for `assumption_definition`, `operation_definition`, and `slr` | Applied | 2026-03-01 |
| `11_add_source_to_theme.sql` | Adds `wietske_medema` to `source` lookup; adds `source`, `created_at`, `updated_at` columns to `theme` with FK; populates all rows | Applied | 2026-03-01 |
| `12_replace_themes_and_links.sql` | Replaces old 7-theme architecture with new 6 themes (cws, ag_gw, eco, delta, climate, governance); reseeds `theme_scenario_link` from THEME_SCENARIOS | **Pending** | — |

## Important notes

- Migrations 01–02 were run via `psql $SUPERUSER_URL` (DDL changes; table owner is `postgres`)
- Migrations 03–05 were run via `psql $SUPERUSER_URL` (DDL) with `DISABLE TRIGGER USER` blocks for data provenance fixes
- Seed data `INSERT`s inside migration 03 should have been run as `$DATABASE_URL` (see `database/README.md` — Migration authoring rule)
- Migrations 06–11 require `$SUPERUSER_URL` for DDL (`CREATE TABLE`, `ALTER TABLE`, `DISABLE TRIGGER USER`). Data `INSERT`s/`UPDATE`s use explicit `created_by = 2` / `updated_by = 2` with `DISABLE TRIGGER USER` to ensure correct provenance.

## Naming convention for future migrations

Use sequential numbering: `12_<description>.sql`, `13_<description>.sql`, etc.
