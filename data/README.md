# Data

Where to find the canonical data the backend depends on. Most of it lives outside this repo (Google Drive, Google Sheets, a separate GIS repo). The local `data/` directory is mostly working space and is gitignored.

## Where to find the data

| Type of data | Purpose | Location |
|---|---|---|
| Scenario model run data | Source of SV (state-variable input) and DV (decision-variable output) data | [Google Drive: Scenarios folder](https://drive.google.com/drive/folders/1IBX1DjMnlxTEFqOO2Pwi0OCt61dG_Ezg), `Model_Files/` subfolder per scenario |
| Scenario trend reports | Verification of extracted data in the ETL process | [Google Drive: Scenarios folder](https://drive.google.com/drive/folders/1IBX1DjMnlxTEFqOO2Pwi0OCt61dG_Ezg), `Data_Extraction/Variables_From_trend_report_variables_v5/` subfolder per scenario |
| Tier results | Derived scenario data | [COEQWAL Platform Content Summary](https://docs.google.com/spreadsheets/d/1xcQIR_J96-cs7BuCrXjznwkinLgxl-Pf9tA3mJ2GiyA/edit?gid=728051596#gid=728051596), **Tiers** tab, column I |
| Statistics / outcomes | Derived statistical data. Listing in [COEQWAL Platform Content Summary](https://docs.google.com/spreadsheets/d/1xcQIR_J96-cs7BuCrXjznwkinLgxl-Pf9tA3mJ2GiyA/edit?gid=728051596#gid=728051596), **Outcomes** tab | Database (computed and loaded by the statistics ETL) |
| Geoschematic | GIS source for nodes, arcs (channels), demand units, water budget areas, and watersheds. Loaded into the database and surfaced by the API as the map layer | [`berkeley-gif/coeqwal-gis-kart`](https://github.com/berkeley-gif/coeqwal-gis-kart) (separate Kart repo) |
| CWS, M&I, crosswalk xlsx | Community water system delivery, demand unit refresh, and M&I crosswalk spreadsheets (tracked). See [`data/reference/cws/README.md`](reference/cws/README.md) for the file inventory. | [`data/reference/cws/`](reference/cws/) |
| CalSim 3 network schematic | CalSim 3 network schematic (Hongbing), reference PDF | [`data/reference/CS3_NetworkSchematic-Hongbing.pdf`](reference/CS3_NetworkSchematic-Hongbing.pdf) |
| CalSim 3 report | Official CalSim 3 model documentation | Too large for GitHub (>100 MB). Download from [CalSim 3 on DWR](https://water.ca.gov/Library/Modeling-and-Analysis/Central-Valley-models-and-tools/CalSim-3). A local copy may sit in `data/reference/` (gitignored). |
| Attribute data | Authoritative reference and entity definitions that give the scenario data its meaning: lookups, demand units, reservoirs, scenarios, hydroclimates, themes, tier definitions, and the version metadata that wires them all together | [`database/seed_tables/`](../database/seed_tables/), organized by schema layer (`00_versioning/`, `01_lookup/`, `02_network/`, `03_entity/`, `03_GIS/`, `03_outcome_framework/`, `04_variable/`, `04_calsim_data/`, `05_assumptions_operations/`, `06_scenario/`, `07_hydroclimate/`, `08_theme/`, `10_tier/`). Loaded via `psql \copy` from migrations |

## Related

- [`etl/`](../etl/) - the production pipeline that pulls model runs and trend reports from Google Drive, extracts them, and computes statistics for the database
- [`database/seed_tables/`](../database/seed_tables/) - the tracked seed CSVs that hold attribute and reference data
- [`docs/database_geometry_pattern.md`](../docs/database_geometry_pattern.md) - where geometry columns live in the schema
- [`docs/INFRASTRUCTURE.md`](../docs/INFRASTRUCTURE.md) - S3 bucket, Drive, and Cloud9 details (local-only)
