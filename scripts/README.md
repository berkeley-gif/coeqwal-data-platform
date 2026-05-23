# COEQWAL backend scripts

This directory contains utility scripts and tools for the COEQWAL web backend.

## Developer environment

> **Unsupported (Linux-only).** The local-dev helpers in this table are best-effort scaffolding for the API + local Postgres loop on a Linux host. They are not maintained for macOS or Windows. Production ETL runs on Cloud9, see [`etl/README.md`](../etl/README.md). The Cloud9 inventory helper at the bottom is read-only and is the supported way to capture the production environment.

| Script | What it does |
|---|---|
| [`setup_dev_env.sh`](setup_dev_env.sh) | One-shot Linux dev setup: venv, rclone, AWS, Docker, local Postgres, schema + seeds. Idempotent. Unsupported. |
| [`check_env.sh`](check_env.sh) | Read-only smoke test. PASS/FAIL per prerequisite with a one-line fix hint. `--container` also builds the batch image. Unsupported. |
| [`load_local_seeds.sh`](load_local_seeds.sh) | Applies every schema + seed file under `database/scripts/sql/` to `$DATABASE_URL`. `--dry-run` shows the plan. Unsupported. |
| [`cloud9_snapshot.sh`](cloud9_snapshot.sh) | Read-only inventory of Cloud9. Writes [`docs/CLOUD9_INVENTORY.md`](../docs/CLOUD9_INVENTORY.md) with `--write`. Supported. |

The full flow is documented in the [top-level Developer setup](../README.md#developer-setup).

## Tools

### PDF Table Scraper
**Location**: `pdf_scraper/`

Tool for extracting tables from PDF documents (particularly the CalSim3 manual) and converting them to csv format for database integration.