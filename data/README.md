# Data

Source materials that feed the rest of the backend. This directory is the staging area for everything before it becomes a seed CSV (in [`database/seed_tables/`](../database/seed_tables/)) or an ETL input (in [`etl/`](../etl/)).

Most of `data/` is **local-only working tree**. Only one small slice is tracked in git (hand-extracted reference tables from the CalSim 3 published report). Everything else has a canonical home on S3, Google Drive, or the COEQWAL Shared Drive. This directory is where a developer keeps the working copies while pre-processing them.

## What lives here

| Type of data | Purpose | Location |
|---|---|---|
| Reference tables hand-extracted from the CalSim 3 final report PDF | Authoritative source for reservoir, watershed, and agricultural demand-unit reference data. Manually transcribed once and tracked in git so the seed CSVs in [`database/seed_tables/`](../database/seed_tables/) can be regenerated against the canonical text | `data/raw/csv_from_CalSim_report_pdf/` (tracked) |
| The CalSim 3 final report PDFs the CSVs above were extracted from | Audit trail. Lets you re-extract tables or verify the transcribed CSVs against the report text | `data/raw/pdf_tables_from_CalSim_report/`, `data/raw/manual pdf/` (local only, originals live on the COEQWAL Shared Drive) |
| CalSim 3 GeoSchematic geopackages (`.gpkg`) | GIS source for nodes, arcs (channels), demand units, water budget areas, and watersheds in EPSG:4326. Loaded into `database/seed_tables/` via the `from_geopackage` flow and surfaced by the API as the map layer | `data/GIS/` (local only) |
| CSV exports of the geopackages above | Working flat-file copies of nodes/arcs/du/wba that get cleaned, deduped, and shaped into seed CSVs | `data/raw/from_geopackage/` (local only) |
| CalSim 3 network schematic (XML + JSON) | Topology source for the `network`, `node`, and `channel_entity` tables. The XML is the DWR-distributed network definition. The JSON is a working derivative | `data/raw/network/` (local only) |
| Tier outcome drops from the data team (Ecology, Salmon, Export, Groundwater, In-Delta, Reservoir Storage, Environmental Flows) | Raw deliveries that get normalized by `etl/tier_data/stage_tier_results.py` before being loaded into `tier_result` / `tier_location_result`. Canonical post-normalization copies live in [`etl/tier_data/staging/tier_results/`](../etl/tier_data/staging/tier_results/) (tracked) | `data/raw/tier/`, `data/raw/tier_data/`, `data/intake/tier_data_upload/` (local only) |
| Sample CalSim model run output (one DV CSV + one ZIP) | Schema reference and local-only test fixture. Lets you smoke-test [`etl/batch-container/python-code/dss_to_csv.py`](../etl/batch-container/python-code/dss_to_csv.py) and the statistics ETL without pulling a fresh run from S3 | `data/example_data/`, `data/raw/model_run/` (local only, canonical: `s3://coeqwal-model-run/scenario/<id>/`) |
| Demand-unit processing workspace | Intermediate working files for shaping DU reference data before it becomes a seed CSV | `data/raw/database_tables_processing/` (local only) |

## What is tracked vs. local

[`.gitignore`](../.gitignore) ignores the entire `data/` tree except for the one allowlisted directory.

| Status | Path | Why |
|---|---|---|
| Tracked | `data/raw/csv_from_CalSim_report_pdf/**` | These are small, authoritative reference tables. Hand-transcribed from the CalSim 3 report PDF. They are the upstream source for several `database/seed_tables/` files, so they need to travel with the repo |
| Gitignored | everything else under `data/` | Large binary GIS files, full model-run outputs, raw tier drops, and PDFs. Either too big for git or already canonical elsewhere (S3, Google Drive, COEQWAL Shared Drive) |

If you need a file that lives only locally, ask another developer or pull it from the canonical source listed above.

## Downstream consumers

| If you change ... | ... you regenerate / re-load | Via |
|---|---|---|
| `data/raw/csv_from_CalSim_report_pdf/**` | The matching seed CSVs in `database/seed_tables/` (reservoirs, watersheds, AG demand units) | Manual: re-derive the seed CSV from the tracked source |
| `data/GIS/*.gpkg` or `data/raw/from_geopackage/**` | `database/seed_tables/` rows for `node`, `channel_entity` (arcs), `du_*_entity`, `wba`, `river_watershed` | Manual: re-export from the geopackage with QGIS, then re-shape into the seed CSV format |
| `data/raw/network/**` | `database/seed_tables/` rows for the network topology tables | Manual: re-derive via the network-ingestion utilities in `database/scripts/` |
| `data/raw/tier/**`, `data/raw/tier_data/**`, `data/intake/tier_data_upload/**` | `etl/tier_data/staging/tier_results/` (tracked), then `tier_result` / `tier_location_result` in the DB | `etl/tier_data/stage_tier_results.py` then `etl/tier_data/load_all_tier_results.py`. See [`etl/tier_data/README.md`](../etl/tier_data/README.md) |
| `data/example_data/**`, `data/raw/model_run/**` | Nothing downstream. Local-only test fixtures | n/a |

## Related

- [`etl/`](../etl/) - the production pipeline that consumes scenario CSVs from S3 (not from `data/`)
- [`database/seed_tables/`](../database/seed_tables/) - the canonical, tracked CSVs that get loaded into the DB by the seed scripts. Most of these are downstream of something in `data/`
- [`docs/INFRASTRUCTURE.md`](../docs/INFRASTRUCTURE.md) - S3 bucket, Drive, and Cloud9 details (local-only)
