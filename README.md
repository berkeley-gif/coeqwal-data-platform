# COEQWAL Data Platform

A comprehensive data platform for the Collaboratory for Equity in Water Allocation (COEQWAL) project, providing multi-level data schema, PostgreSQL database with PostGIS extension, data APIs, and upload and verification infrastructure for California water management scenario presentation, analysis, and review.

The data platform code repository and documentation is organized into four modules(`data`, `etl`, `database`, `api`) plus its AWS infrastructure (documented locally). Each module has its own README.

## Developer setup

Two environments cover daily work:

- **Cloud9 (`coeqwal-db-admin`)**: the operator environment. Owns direct access to production RDS, the only place ingestion against the live S3 buckets runs.
- **Laptop**: matches Cloud9 for everything except production RDS. Covers ETL scripts, tier loads, audit, API local dev, schema work against a local Postgres, and Docker builds for the batch container.

The Cloud9 environment is the spec. Capture it any time with `bash scripts/cloud9_snapshot.sh --write` and commit the resulting [`docs/CLOUD9_INVENTORY.md`](docs/CLOUD9_INVENTORY.md). Laptop setup tries to match that file.

### One-shot laptop setup

```bash
bash scripts/setup_dev_env.sh
```

Does, in order:

1. Verifies Python 3.10+ (advises an install path if missing).
2. Creates `.venv/` at repo root and runs `pip install -r requirements.txt` (the [aggregator](requirements.txt)).
3. Installs `rclone` if missing and prompts you to configure a `gdrive:` remote.
4. Checks the AWS CLI and walks you through `aws configure sso` if credentials are not set.
5. Verifies Docker is installed and the daemon is running.
6. Brings up a local Postgres via [`docker-compose.yml`](docker-compose.yml) (`postgis/postgis:16-3.4`).
7. Applies schema and seeds via [`scripts/load_local_seeds.sh`](scripts/load_local_seeds.sh).
8. Runs [`scripts/check_env.sh`](scripts/check_env.sh) to confirm everything came up.

Idempotent: rerun anytime. Flags `--skip-postgres` and `--skip-seeds` let you scope the run.

### Verify anytime

```bash
bash scripts/check_env.sh
```

Prints `PASS` or `FAIL` for each prerequisite, with a one-line remediation hint per `FAIL`. Returns non-zero if anything failed. A clean run is the definition of "laptop is ready". Add `--container` to also `docker build` the batch container (slow).

### Local Postgres only

```bash
docker compose up -d postgres
export DATABASE_URL="postgresql://coeqwal:coeqwal@localhost:5432/coeqwal_scenario"
bash scripts/load_local_seeds.sh
```

The container's data lives in the named volume `coeqwal_pgdata` and survives restarts. Reset with `docker compose down -v`. The local DB is intentionally separate from production RDS. Direct laptop-to-RDS access is out of scope and Cloud9 owns that path.

### What is intentionally out of scope locally

- Direct production RDS access (use Cloud9).
- Running ingestion against the live `coeqwal-model-run` S3 prefixes other than scoped, read-only checks. Operator runs live on Cloud9.
- Per-developer OAuth for the shared Drive. Each developer authorizes their own Drive access via `rclone config`.

### Validating a fresh setup

The intended end-to-end test runs `scripts/setup_dev_env.sh` on a brand-new account, then `scripts/check_env.sh`, then per-module smoke tests. Two ways to do this:

**Fresh user account on your laptop (most realistic):** create a new local user, log in as that user, install nothing manually, clone this repo, run `bash scripts/setup_dev_env.sh`. After it finishes, `bash scripts/check_env.sh` should show no `FAIL`. Delete the test user afterward.

**Disposable Linux VM (faster to throw away):**

```bash
# Inside a fresh Ubuntu 22.04 / Debian 12 container or VM, with sudo available
sudo apt-get update && sudo apt-get install -y git curl
git clone <repo-url> coeqwal-backend && cd coeqwal-backend
bash scripts/setup_dev_env.sh
bash scripts/check_env.sh
```

Per-module smoke checks after `check_env.sh` is clean:

| Module | Command | Expected |
|---|---|---|
| ETL ingestion | `python etl/ingestion/gdrive_bulk_download.py scan --scenarios s0042` | Prints a plan for one scenario (no S3 writes) |
| Tier data | `python etl/tier_data/load_all_tier_results.py --dry-run` | Prints what would be loaded |
| API | `cd api/coeqwal-api && uvicorn main:app --reload` then `curl http://localhost:8000/api/health` | `{"status":"ok"}` or similar |
| DB | `psql "$DATABASE_URL" -c "\\dt"` | Lists ~96 tables |
| Batch container | `bash scripts/check_env.sh --container` | `PASS  docker build etl/batch-container` |

If any of these fail, the failure mode and remediation should appear in `check_env.sh`'s output. File issues against the failing step.

### Module-specific notes

Editing dependencies for a single module is done in that module's `requirements.txt`. The top-level aggregator rarely changes. If you only work on one module you can install just that module's `requirements.txt`:

- ETL ingestion: `etl/ingestion/requirements.txt`
- ETL tier data: `etl/tier_data/requirements.txt`
- ETL verification: `etl/verification/requirements.txt`
- Database: `database/requirements.txt`
- API: `api/coeqwal-api/requirements.txt`

## Data

See [`data/README.md`](data/README.md) for the data inventory.

## ETL

The pipeline that moves CalSim 3 model output from the COEQWAL Shared Drive through extraction, validation, and statistical processing into the database, with audit artifacts at every step.

This pipeline:

- **Ingests** CalSim 3 model run ZIPs and trend report CSVs from the COEQWAL Shared Drive into S3, validated against [`etl/ingestion/scenario_listing/model_run_file_source_working.csv`](etl/ingestion/scenario_listing/model_run_file_source_working.csv) (the canonical scenario -> Drive folder mapping).
- **Reorganizes** uploaded ZIPs into a per-scenario S3 layout (`scenario/<id>/run/`) and submits a Batch job per ZIP through a Lambda trigger.
- **Extracts** CalSim 3 HEC-DSS binary data to CSV inside a Docker container on Fargate Spot. Classifies SV (state-variable input) and DV (decision-variable output) files, preserves the DSS unit metadata as a row-6 CSV header, and writes a `.units.json` sidecar.
- **Verifies units** that every CSV column's unit matches the DSS file's ground truth, and detects duplicate B-part pathnames within the same DSS.
- **Validates content** by comparing each extracted CSV against the modeling team's trend report CSV with configurable absolute and relative tolerances. Emits a per-scenario validation summary and a manifest JSON.
- **Audits extraction** across all scenarios into a single console table and audit CSV (manifest status, validation result, unit mismatches).
- **Computes statistics** from the extracted CSVs (reservoir storage, urban delivery, agricultural demand and shortage, M&I contractor reliability, environmental flow alteration, refuge delivery, delta salinity / NDO / X2, climate and operational sensitivity) and loads them into the layer 10+ tables in PostgreSQL.
- **Loads tier outcomes** delivered by the data team (CWS deliveries, AG revenue, env flows, reservoir storage, groundwater storage, delta ecology, salmon abundance, freshwater salinity) into `tier_result` and `tier_location_result` via idempotent UPSERT SQL.
- **Verifies accuracy** end-to-end at four layers: DSS extraction (Layer 1), DSS-vs-CSV units (Layer 1b), CSV-to-DB statistics (Layer 2), DB-to-API responses (Layer 3), and surfaces results on the public `/verification` page (Layer 4).
- **Produces audit artifacts** at every stage (`scan_audit.csv`, `audit_report.csv`, `extraction_audit.csv`, `stats_audit_<ts>.csv`, `duplicate_scan_results.csv`) so each step's correctness is independently reviewable.

Three of those stages run automatically, the rest are operator-driven. All are laid out as siblings under `etl/`:

| Stage | Directory | Trigger |
|---|---|---|
| 1. Ingestion | [`etl/ingestion/`](etl/ingestion/) | Operator (`gdrive_bulk_download.py`) |
| 2. Lambda trigger | [`etl/lambda/`](etl/lambda/) | Automatic (S3 PUT to `ready/`) |
| 3. Batch extraction | [`etl/batch-container/`](etl/batch-container/) | Automatic (AWS Batch on Fargate Spot) |
| 4a. Statistics ETL | [`etl/statistics/`](etl/statistics/) | Operator (`run_all.py`) |
| 4b. Tier data | [`etl/tier_data/`](etl/tier_data/) | Operator (team-delivered drops) |
| Verification | [`etl/verification/`](etl/verification/) | Operator (end-to-end accuracy checks) |

Tech: Python, `boto3`, `pydsstools`, AWS Batch on Fargate Spot, Docker (for the extraction container).

See [`etl/README.md`](etl/README.md) for the full pipeline diagram, per-stage runbooks, and output-file conventions.

## Database

PostgreSQL + PostGIS on RDS. A strictly layered schema of ~96 tables and ~402k rows. Every change goes through the audit-trigger framework and is verified against the canonical ERD.

**Layers:**

- **00-08** Foundational and reference data: versioning, lookups, network, **entities**, variables, assumptions and operations, scenarios, hydroclimate, themes.
- **10+** Derived results: tier results, monthly stats, period summaries.

<!-- TODO: refine this block, then un-comment.

**Standard data shape** (every domain follows this pattern: reservoirs, channels, ag DUs, refuges, MI contractors, and CWS for community water systems):

1. Entity table in `03_entity/` with `id` PK, `short_code` UNIQUE, FKs to lookup tables, and the audit columns populated by the `set_audit_fields()` trigger.
2. Variable mapping in `04_variable/` (e.g. `du_urban_variable`, `channel_variable`) holding the CalSim variable names per entity.
3. Optional multi-arc or sub-entity tables (`du_urban_delivery_arc`, `mi_contractor_delivery_arc`) for entities that sum multiple CalSim arcs.
4. Group / membership tables (`*_group` + `*_group_member`) for analytical filtering.
5. Statistics tables in layer 10+ (`*_monthly`, `*_period_summary`) keyed by `scenario_short_code` + `<entity>_id`.

Standards documented in [`database/CHECKLIST_TABLE_STANDARDS.md`](database/CHECKLIST_TABLE_STANDARDS.md): snake_case, FK IDs (never text), audit trigger applied, row in `domain_family_map` for versioning. Every new table also needs an SQL script under `database/scripts/sql/<layer>/` and a seed CSV under `database/seed_tables/<layer>/`.

-->


**Source-of-truth artifacts:**

- ERD: [`database/schema/COEQWAL_SCENARIOS_DB_ERD.md`](database/schema/COEQWAL_SCENARIOS_DB_ERD.md)
- Latest monthly audit: `audits/monthly_<timestamp>/report.md` (gitignored, regenerable)

**Audit chain** (each tool answers a different question):

| Question | Tool |
|---|---|
| Full monthly audit: content + verification + health + cost | `python database/audit/run_monthly_audit.py` |
| Is the DB shaped correctly? | `database/run_audit.sh`, `verify_erd_against_audit.py`, per-layer `09_verify_level*.sql` |
| Are layers 00-08 correct? | `database/scripts/export_layer_tables.py` + diff vs `database/seed_tables/` |
| Are computed results correct? | `etl/statistics/verify_all_sections.py` (CSV -> DB), `etl/statistics/verify_api.py` (DB -> API) |
| Is verification status visible to users? | `GET /api/verification/status` + frontend `/verification` page |

Tech: PostgreSQL, PostGIS, `psql`, `aws_s3` extension for S3-side loads.

See [`database/README.md`](database/README.md) for the full schema reference, audit guide, and developer onboarding.

## API

The public-facing surface at `https://api.coeqwal.org`. Two pieces:

| Piece | What it does | Source |
|---|---|---|
| FastAPI service on ECS Fargate | Serves the rest of the API. Statistics, tiers, verification status, etc. Async Python with automatic OpenAPI docs at `/docs` | [`api/coeqwal-api/`](api/coeqwal-api/) |
| `coeqwalPresignDownload` Lambda | Lists scenarios in S3 and presigns download URLs. Backs `GET /scenario` and `GET /download` through API Gateway v2 | [`api/lambda/coeqwalPresignDownload/`](api/lambda/coeqwalPresignDownload/) |

**Endpoints:**

- Production: <https://api.coeqwal.org/api>
- Interactive docs: <https://api.coeqwal.org/docs>

**Request flow (FastAPI service):**

```
Request -> Uvicorn -> FastAPI -> Pydantic (validates) -> asyncpg (queries DB) -> Response
```

Tech: FastAPI, Pydantic, `asyncpg`, Uvicorn. Containerized and deployed to ECS Fargate via GitHub Actions.

See [`api/coeqwal-api/README.md`](api/coeqwal-api/README.md) for endpoints, filtering options, and local development. See [`api/lambda/coeqwalPresignDownload/README.md`](api/lambda/coeqwalPresignDownload/README.md) for the download Lambda.

## AWS infrastructure

What runs where, at a glance:

| Service | Role |
|---|---|
| RDS PostgreSQL | Managed Postgres with PostGIS. Production database |
| ECS Fargate | Runs the FastAPI container (Docker -> ECR -> ECS) |
| S3 (`coeqwal-model-run`) | Model run ZIPs and extracted CSVs |
| Lambda (`coeqwalEtlTrigger`) | S3 PUT -> Batch SubmitJob for the extraction pipeline |
| Lambda (`coeqwalPresignDownload`) | Presigned-URL downloads for the website |
| Lambda (`coeqwal-database-audit`) | Scheduled DB audit |
| AWS Batch on Fargate Spot | Runs the DSS-to-CSV extraction container (`coeqwal-dss-queue`, job def `coeqwal-dss-jobdef:3`) |
| ECR | Docker image registry (`coeqwal-etl:latest`) |
| API Gateway (HTTP API v2) | Routes `GET /scenario` and `GET /download` to the presign Lambda |
| Route 53 | DNS routing to `api.coeqwal.org` |
| Cloud9 (`coeqwal-db-admin`) | Dev environment with the credentials and connection strings for running ETL and DB ops |
| GitHub Actions | CI for the FastAPI service (`api/coeqwal-api/`) and the ETL container (`etl/batch-container/`) |

Detailed runbook (AWS account IDs, ARNs, cost levers, IAM policies, deprecation notes) lives in `docs/INFRASTRUCTURE.md`. That file is **local-only** (gitignored) because it contains operational secrets-adjacent material. Ask another developer for a copy if you need it.

### Reclaiming disk space on the Cloud9 / EC2 instance

If the EC2 backing your Cloud9 instance is running low on disk space (common after OS updates, package installs, or prolonged operation):

**Check current disk usage:**

```bash
df -h
```

Shows disk usage for all mounted filesystems in human-readable units (GB/MB). The `/` (root) filesystem is the one most likely to fill up. Look for `Use%` approaching 100%.

**Clean the DNF package cache:**

```bash
sudo dnf clean all
```

DNF (the package manager on Amazon Linux 2023 / RHEL-based systems) caches downloaded packages and metadata on disk after installation. Over time this cache can grow significantly. `dnf clean all` removes all cached package data, repo metadata, and headers. Safe to run at any time. Packages are re-downloaded from the repo on the next `dnf` operation.

Run `df -h` again to confirm space was reclaimed.

**If `dnf clean all` doesn't reclaim much:**

Manually remove the DNF cache directory and check root disk usage:

```bash
sudo rm -rf /var/cache/dnf/*
df -h /
```

**Trim the system journal:**

`systemd` accumulates log journal files under `/var/log/journal/`. Check how much space the journal is using, then vacuum it down to 50 MB:

```bash
sudo journalctl --disk-usage
sudo journalctl --vacuum-size=50M
df -h /
```

`--disk-usage` reports total journal size. `--vacuum-size=50M` deletes the oldest journal files until the total size is at or below 50 MB. Safe to run. It only removes old log history, not active logs.

**If space is still low, check Docker (~1.2 GB):**

Docker can accumulate disk usage from stopped containers, dangling images, unused volumes, and build cache. See a breakdown of what Docker is holding:

```bash
docker system df
```

This shows how much space is used by images, containers, volumes, and build cache, and how much is "reclaimable" (unused).

Safely remove everything unused:

```bash
docker container prune -f   # removes all stopped containers
docker image prune -a -f    # removes all images not used by a running container
docker volume prune -f      # removes all volumes not attached to a container
df -h /
```

These commands only delete objects that are not currently in use. Running containers, their images, and attached volumes are untouched. The `-f` flag skips the confirmation prompt.

## License

See [LICENSE](./LICENSE) for details.
