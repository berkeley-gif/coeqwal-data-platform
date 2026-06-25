# Database Seed Data

CSV files for populating the COEQWAL PostgreSQL database with foundational data. Organized by layer. Each layer generally depends on layers with a lower number.

## Directory layout

```
seed_tables/
├── 00_versioning/              version families, versions, developer accounts, domain_family_map
├── 01_lookup/                  shared reference data: regions, units, scales, geometry, sources, network types, watershed, wba
├── 02_network/                 physical network infrastructure seed data
├── 03_GIS/                     spatial data: reservoir, WBA, and compliance-station geometries, plus du_4326.gpkg
├── 03_outcome_framework/       outcome-framework CSVs (unbuilt designs, see ../SCHEMA_BACKLOG.md § 5d)
├── 04_variable/                CalSim variable definitions and type classifications
├── 04_calsim_data/             entity and network seed CSVs (reservoir, du_*, network); some to be reorganized into 02/03
├── 05_assumptions_operations/  assumption and operation definitions + category tables
├── 06_scenario/                scenario definitions, authors, source links, key assumption/op links
├── 07_hydroclimate/            hydroclimate definitions, sea level rise (slr) table
├── 08_theme/                   research themes, theme-scenario links, theme focus/priority tables
└── 10_tier/                    tier definitions only (lookup). tier_result and tier_location_result are project data
```

## CSVs that do not map to a table

Some CSVs here are inputs, not table seeds. They load into existing tables through a script, or are intermediate CalSim data. They are not orphaned tables.

- `04_calsim_data/du_*_geometry.csv` load into the `du_*_entity` geometry columns via `database/scripts/data_processing/load_du_geometries.py`.
- `04_calsim_data/CalSim_*_geopackage.csv`, `connectivity_resolution_log.csv`, and `network_*_connectivity*.csv` are intermediate / staging data from the CalSim network resolution, not DB tables. Candidates for a `04_calsim_data/intermediate/` subfolder.

The `03_GIS/` spatial CSVs seed geometry that lives on the singular Layer 03 tables `reservoir`, `wba`, and `compliance_station` (each keeps `geom_wkt` plus a PostGIS `geom`), and `du_4326.gpkg` feeds the `du_*_entity` geometry columns via [`../scripts/data_processing/load_du_geometries.py`](../scripts/data_processing/load_du_geometries.py). The older [`../sql_archive/02_network_layer/load_spatial_tables.sql`](../sql_archive/02_network_layer/load_spatial_tables.sql) creates plural `reservoirs` / `compliance_stations` tables that do not match the live schema, so treat it as superseded.

## Loading seed data into the database

Seed data is loaded from these repo files directly. No S3 upload is required. Migrations use the psql `\copy` meta-command, which reads from the local filesystem of the machine running the command. **Always run migrations from the repo root** so relative paths resolve correctly:

```bash
# Example: run a migration that loads seed data
psql $SUPERUSER_URL -f database/sql_archive/log/20_create_refuge_entity_table.sql

# Example: re-sync scenario / theme / link tables after editing a CSV
psql $SUPERUSER_URL -f database/scripts/sql/upsert_scenario_data.sql
```

`\copy` vs `COPY`:
- `\copy` (lowercase, client-side) reads from the **client machine**. Works for Cloud9 and local dev.
- `COPY` (uppercase, server-side) reads from the **RDS server filesystem**. Not available on RDS.

Always use `\copy`.

## Audit and verify

After loading, run the monthly audit to check the live database. See [`../audit/README.md`](../audit/README.md) for the full workflow.

```bash
python database/audit/run_monthly_audit.py
```

Section 1b of the report compares the live schema against the ERD. The standalone comparator (`verify_erd_against_audit.py`) has drifted (it hardcodes an old ERD filename and expects the old tree-format ERD), so confirm schema-vs-ERD drift by reading § 1b of the report by hand.

## Inspect a layer

```bash
psql $DATABASE_URL -f database/scripts/sql/01_lookup/inspect_layer01.sql
```
