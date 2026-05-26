# COEQWAL backend scripts

This directory contains utility scripts and tools for the COEQWAL web backend. Cloud9 is the supported development environment for everything that touches production data and the ETL pipeline. See [`etl/README.md`](../etl/README.md).

| Script | What it does |
|---|---|
| [`cloud9_snapshot.sh`](cloud9_snapshot.sh) | Read-only inventory of Cloud9. Writes [`docs/CLOUD9_INVENTORY.md`](../docs/CLOUD9_INVENTORY.md) with `--write`. Supported. |

## Tools

### PDF Table Scraper
**Location**: `pdf_scraper/`

Tool for extracting tables from PDF documents (particularly the CalSim3 manual) and converting them to csv format for database integration.