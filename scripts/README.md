# COEQWAL backend scripts

This directory contains utility scripts and tools for the COEQWAL web backend.

## Developer environment

| Script | What it does |
|---|---|
| [`setup_dev_env.sh`](setup_dev_env.sh) | One-shot laptop setup: venv, rclone, AWS, Docker, local Postgres, schema + seeds. Idempotent. |
| [`check_env.sh`](check_env.sh) | Read-only smoke test. PASS/FAIL per prerequisite with a one-line fix hint. `--container` also builds the batch image. |
| [`load_local_seeds.sh`](load_local_seeds.sh) | Applies every schema + seed file under `database/scripts/sql/` to `$DATABASE_URL`. `--dry-run` shows the plan. |
| [`cloud9_snapshot.sh`](cloud9_snapshot.sh) | Read-only inventory of Cloud9, used as the parity spec for laptops. Writes [`docs/CLOUD9_INVENTORY.md`](../docs/CLOUD9_INVENTORY.md) with `--write`. |

The full flow and the laptop-vs-Cloud9 split is documented in the [top-level Developer setup](../README.md#developer-setup).

## Tools

### PDF Table Scraper
**Location**: `pdf_scraper/`

Tool for extracting tables from PDF documents (particularly the CalSim3 manual) and converting them to csv format for database integration.