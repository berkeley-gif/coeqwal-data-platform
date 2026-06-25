# COEQWAL backend scripts

This directory contains utility scripts and tools for the COEQWAL web backend. Cloud9 is the supported development environment for everything that touches production data and the ETL pipeline. See [`etl/README.md`](../etl/README.md).

| Script | What it does |
|---|---|
| [`cloud9_snapshot.sh`](cloud9_snapshot.sh) | Read-only inventory of the supported Cloud9 environment. Prints to stdout; pass `--output=PATH` to save. |
| [`setup_etl_cloud9.sh`](setup_etl_cloud9.sh) | One-time setup of the ETL Python environment on a fresh Cloud9 instance. |
| [`mapbox_recipes/`](mapbox_recipes/) | Mapbox Tiling Service recipe JSON for COEQWAL tilesets. Currently the demand-unit tileset (`calsim_demand_units.json`, `demand_units` layer). |
