# Tier seed data (lookup only)

This directory holds the lookup CSV for the tier framework. The tier
result and per-location tables (`tier_result`, `tier_location_result`)
are project data, not seed data. They live in their respective database
tables and are populated by the ETL pipeline, not from CSVs in this
directory.

## What's here

### `tier_definition.csv` (lookup)

Defines the 9 tier indicators (one row per indicator). This is reference
data that rarely changes.

| Column | Meaning |
|---|---|
| `short_code` | Unique identifier (e.g. `ENV_FLOWS`, `DELTA_ECO`) |
| `name` | Display name for reporting |
| `description` | Detailed description |
| `tier_type` | `multi_value` (4 tier counts per row) or `single_value` (one tier level per row) |
| `tier_count` | 1 or 4 |
| `is_active` | Whether the indicator is currently used |

Tier indicators by type:

- **Multi-value** (`tier_1_value` ... `tier_4_value`, plus normalized variants):
  `ENV_FLOWS`, `RES_STOR`, `GW_STOR`, `CWS_DEL`, `AG_REV`
- **Single-value** (`single_tier_level` only):
  `DELTA_ECO`, `FW_DELTA_USES`, `FW_EXP`, `WRC_SALMON_AB`

## What is not here, and why

### `tier_location` (catalog table)

The catalog of which `location_id`s belong to each tier lives in the
`tier_location` database table. There is no seed CSV because the tier
teams' staging CSVs in
[`etl/tier_data/staging/`](../../../etl/tier_data/staging/) are the
source of truth for membership. Reconcile with:

```bash
python etl/tier_data/scripts/diff_tier_locations.py
python etl/tier_data/scripts/sync_tier_locations_from_staging.py --dry-run
python etl/tier_data/scripts/sync_tier_locations_from_staging.py
```

DDL: [`database/scripts/sql/create_tier_location_table.sql`](../../scripts/sql/create_tier_location_table.sql)
(superuser-only, runs once). Display names are not stored on
`tier_location`. The loader and API resolve them by joining `location_id`
to the entity tables documented in
[`etl/common/tier_location_entities.py`](../../../etl/common/tier_location_entities.py).
Rows that drop out of staging are soft-deleted (`is_active = FALSE`)
so historical `tier_location_result` rows still resolve to a catalog
row.

### `tier_result` and `tier_location_result` (project data)

The actual tier values per scenario are project data, not reference
data. They change every time the data team produces a new round of
scenarios or revises tier thresholds, and the source of truth is the
staging CSVs in [`etl/tier_data/staging/`](../../../etl/tier_data/staging/).

A from-scratch DB rebuild loads them by running the ETL loader after
the DDLs and seeds:

```bash
python etl/tier_data/scripts/load_all_tier_results.py --output-sql all_tiers.sql
psql $DATABASE_URL -f etl/tier_data/output/all_tiers.sql
```

The full workflow (dry-run, validate, etc.) lives in
[`etl/tier_data/README.md`](../../../etl/tier_data/README.md).
