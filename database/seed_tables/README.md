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
├── 04_calsim_data/          legacy — entity and network seed CSVs (to be reorganized into 02/03)
├── 05_assumptions_operations/  assumption and operation definitions + category tables
├── 06_scenario/             scenario definitions, authors, source links, key assumption/op links
├── 07_hydroclimate/         hydroclimate definitions, sea level rise (slr) table
├── 08_theme/                research themes, theme-scenario links, theme focus/priority tables
└── 10_tier/                 tier definitions and results
```

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
psql $DATABASE_URL -f database/scripts/sql/00_versioning/09_verify_level00.sql
psql $DATABASE_URL -f database/scripts/sql/01_lookup/inspect_layer01.sql
```
