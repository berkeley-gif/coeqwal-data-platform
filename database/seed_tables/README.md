# Database Seed Data

CSV files for populating the COEQWAL PostgreSQL database with foundational data.
Organized by layer. Each layer depends on all layers with a lower number.

## Directory layout

```
seed_tables/
├── 00_versioning/           version families, versions, developer accounts, domain_family_map
├── 01_lookup/               shared reference data: regions, units, scales, geometry, sources,
│                            network types, watershed, wba
├── 02_network/              physical network infrastructure seed data
├── 03_entity/               operational entity definitions (demand units, contractors, reservoirs)
├── 04_variable/             CalSim variable definitions + type classifications
│                            calsim_model_variable_type, derived_variable_type, variable_type
│                            channel_variable, reservoir_variable, inflow_variable, derived_variable
├── 04_calsim_data/          legacy.entity and network seed CSVs (to be reorganized into 02/03)
├── 05_assumptions_operations/  assumption and operation definitions + category tables
├── 06_scenario/             scenario definitions, authors, source links, key assumption/op links
├── 07_hydroclimate/         hydroclimate definitions, sea level rise (slr) table
├── 08_theme/                research themes, theme-scenario links, theme focus/priority tables
└── 10_tier/                 tier definitions only (lookup). tier_result and
                             tier_location_result are project data, populated
                             by etl/tier_data/scripts/load_all_tier_results.py
```

## Loading seed data into the database

Seed data is loaded from these repo files directly.no S3 upload is required.
Migrations use the psql `\copy` meta-command, which reads from the local filesystem
of the machine running the command. **Always run migrations from the repo root** so
relative paths resolve correctly:

```bash
# Example: run a migration that loads seed data
psql $SUPERUSER_URL -f database/scripts/sql/.archive/migrations/20_create_refuge_entity_table.sql

# Example: re-sync scenario / theme / link tables after editing a CSV
psql $SUPERUSER_URL -f database/scripts/sql/upsert_scenario_data.sql
```

`\copy` vs `COPY`:
- `\copy` (lowercase, client-side).reads from the **client machine**. Works for Cloud9 and local dev.
- `COPY` (uppercase, server-side).reads from the **RDS server filesystem**. Not available on RDS.

Always use `\copy`.

## Audit and verify

```bash
# Run full audit
bash database/run_audit.sh

# Verify ERD matches live DB
python database/audit/verify_erd_against_audit.py \
    database/schema/COEQWAL_SCENARIOS_DB_ERD.md \
    audits/latest.json
```

## Inspect a layer

```bash
psql $DATABASE_URL -f database/scripts/sql/.archive/00_versioning/09_verify_level00.sql
psql $DATABASE_URL -f database/scripts/sql/01_lookup/inspect_layer01.sql
```
