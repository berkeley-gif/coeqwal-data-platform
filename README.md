# COEQWAL Data Platform

A comprehensive data platform for the Collaboratory for Equity in Water Allocation (COEQWAL) project for California water management scenario presentation, analysis, and review.

The data platform code repository and documentation is organized into four sections(`data`, `etl`, `database`, `api`). Each section has its own series of (often nested) READMEs. There is a local INFRASTRUCTURE.md script that details AWS infrastructure values. A schematic of that infrastructure can be found at https://dev.coeqwal.org/aws_architecture.html.

## Data

Directory of reference files related to project data. The most important reference file is the CalSim manual pdf, which is too large to house outside LFS, but you can download it from DWR at https://water.ca.gov/Library/Modeling-and-Analysis/Central-Valley-models-and-tools/CalSim-3.

## ETL

The ETL has two parallel pipelines. Pipeline 1, for CalSim scenario model run data, has two sub-pipelines that share the same ingest and DSS Batch extraction. 

The first subpipeline (1.1) deposits a scenario's full model run directory (zipped) as well as an extracted full sv input csv and full dv output csv into a retrievable location in the S3 bucket. The second subpipeline (1.2) calculates statistics from this data and writes them to the database. 

Pipeline 2, for tier data, is independent and loads team-delivered csvs straight into the database. 

Details follow.

### 1.1 Scenario model run data

Complete model run directories (zipped) plus their extracted CSVs land in S3 for the website's "Get Data" feature.

`gdrive_bulk_download.py` scans, downloads, and promotes scenarios from the COEQWAL shared Drive into `s3://coeqwal-model-run/ready/`. The `coeqwalEtlTrigger` Lambda fires on the promote, moves the ZIP under `scenario/<id>/run/`, and submits an AWS Batch job. The Batch container (`coeqwal-etl`) extracts DSS to CSV and, if a reference csv like the Water Allocation Modeling Team's Trend Report is also staged, runs `validate_csvs.py` against it with configurable absolute/relative tolerances. Mismatch counts are inlined into `extract_record.json`. Any mismatches are written to a csv in `scenario/<id>/validation/` only on failure.

**Products** (under `s3://coeqwal-model-run/scenario/<id>/`):

- Original CalSim ZIP at `run/`
- Extracted DV (output) CSV and SV (input) CSV at `csv/`
- `extract_record.json` at the scenario root (validation result inlined)
- `<id>_validation_mismatches.csv` at `validation/` (on validation failure only)

**Website**: Get Data (download links served by the `coeqwalPresignDownload` Lambda).

### 1.2 Statistics -> database

Same ingest and Batch extraction as 1.1. From there, `etl/statistics/run_all.py` reads the extracted CSVs from S3, computes derived metrics, and writes them into PostgreSQL tables.

**Products**: Statistics rows in the database tables: reservoir storage, urban delivery, ag demand and shortage, MI contractor reliability, environmental flow alteration, refuge delivery, delta salinity / NDO / X2, climate and operational sensitivity.

**Website**: Data in Depth (via the FastAPI service).

### 2. Tier data

Team-delivered tier outcome CSVs loaded straight into PostgreSQL, independent of the scenario data ingest described above.

The developer drops correctly-formatted CSVs into [`etl/tier_data/staging/`](etl/tier_data/staging/). `etl/tier_data/scripts/load_all_tier_results.py --output-sql all_tiers.sql` builds the idempotent UPSERT SQL, which is then applied with `psql -f` to the `tier_result` and `tier_location_result` tables.

**Products**: Tier result rows in database.

**Website**: Tier visualization tools.

### Receipts, verification, and audits

The pipeline produces a deliberate paper trail at every stage. Tags below: `[S3]` lives in S3, `[local]` is on disk but gitignored, `[tracked]` is committed to git.

```
Google Drive
   |
   | gdrive_bulk_download.py
   v
Ingest ----writes----> [S3]     scenario/<id>/ingest_record.json
   |                            (provenance + SHA-256 hashes)
   |
   |   ----writes----> [local] etl/ingestion/audit_reports/ingest_state.json
   v
AWS Batch  (DSS to CSV + Trend Report validation)
   |
   |---writes--> [S3]     scenario/<id>/csv/*.csv
   |
   |---writes--> [S3]     scenario/<id>/extract_record.json
   |                       (Batch status + inlined validation result)
   |
   +---writes--> [S3]     scenario/<id>/validation/<id>_validation_mismatches.csv
                           (only on validation failure)

etl/ingestion/tools/audit.py
   reads:   ingest_record.json + ingest_state.json + extract_record.json
   writes:  [tracked] etl/ingestion/audit.md
            ("what needs attention" digest across all scenarios)

----------------------------------------------------------------

[S3] csv/*.csv  ----> etl/statistics/run_all.py
                            |
                            +--> PostgreSQL stats tables
                            |
                            +--> [local] etl/statistics/audit_reports/
                                         stats_audit_<ts>.csv

etl/tier_data/staging/  ----> load_all_tier_results.py
                                    |
                                    +--> PostgreSQL tier tables


PostgreSQL  ----> verify_all_sections.py  --\
            ----> verify_api.py              >--> frontend /verification page
            ----> verify_tiers.py           -/

----------------------------------------------------------------

PostgreSQL  ----> database/audit/run_monthly_audit.py
                            |
                            +--> [tracked] audits/monthly_<ts>/
                                           (schema, row counts, entity exports)
```

For the full per-stage artifact table (writer, reader, location, contents), see [`etl/verification/README.md`](etl/verification/README.md#8-audit-artifacts-per-stage).

#### After a run: what to read

The diagram above is the full menu. You do not read all of it after every run. Each kind of run has a headline in the documents that tells you whether to drill deeper. Read in the listed order. Stop at the first level that gives a clean answer.

**After `gdrive_bulk_download.py download` or `run_full_pipeline.py`:**

1. The terminal output. The audit runs at the end by default and prints "what needs attention" inline.
2. `etl/ingestion/audit.md` if step 1 scrolled by or you missed it. Same content, tracked in git, surfaces in `git pull` for the rest of the team.
3. Drill down only on a flagged scenario: `s3://<bucket>/scenario/<id>/extract_record.json` for the validation summary, `s3://<bucket>/scenario/<id>/validation/<id>_validation_mismatches.csv` for per-row detail, CloudWatch and Batch logs at `/aws/batch/job/...` for runtime traces.

**After `etl/statistics/run_all.py`:**

1. The terminal output (errors and totals printed at the end).
2. `etl/statistics/audit_reports/stats_audit_<ts>.csv` for the per-(scenario, module) scorecard.

**After `etl/tier_data/scripts/load_all_tier_results.py`:**

1. The terminal output (idempotent UPSERT counts).
2. `etl/tier_data/staging/tier_upload_manifest.csv` if you passed `--verify`.

**After `verify_all_sections.py`, `verify_api.py`, or `verify_tiers.py`:**

1. The terminal output. One-line PASS / FAIL summary per scenario.
2. The `/verification` page on the website for the stakeholder-facing view of the same JSON reports.
3. Drill down only on FAIL: `audits/verification_reports/<scenario>_layer{2,3}.json` or `tiers_<ts>.json` for per-check detail.

**After `database/audit/run_monthly_audit.py`:**

1. `audits/monthly_<ts>/report.md`. Top-level summary, row counts, ERD diff, audit-field checks.
2. Drill down only if `report.md` flags a discrepancy: per-table CSVs under `audits/monthly_<ts>/layer_exports/` and `audits/monthly_<ts>/results_samples/`.

**Anytime ("when did X last run?"):** `python etl/status.py` reports freshness for ingest, batch, and stats.

**Rule of thumb.** Trust the headline at each level (terminal output, then `audit.md`, then `stats_audit_<ts>.csv` or `report.md`). The forensic artifacts shown in the diagram exist for triage. Read them only when the headline says to.

[`etl/README.md`](etl/README.md) has the developer runbook for running the whole ETL pipeline.

## Database

PostgreSQL + PostGIS on RDS. A strictly layered, highly-normalized schema of ~96 tables. Every change goes through the audit-trigger framework and is verified against the canonical ERD.

**Layers:**

- **00-09** Foundational and reference data: versioning, lookups, network, entities, variables, assumptions and operations, scenarios, hydroclimate, themes, tier locations.
- **10+** Derived results: tier results, statistics, period summaries.

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

## Open threads and roadmap

When picking up a partially-finished thread (geometry refactor, gw/sw
reconciliation, TAIESM1 ingest, master crosswalk reconciliation, etc.),
start at the team runbook. It indexes each open thread with current
state, files touched, and a "Next steps" block.

- [`docs/TEAM_RUNBOOK.md`](docs/TEAM_RUNBOOK.md) - active threads, rolled-back roadmap entries (R1, R2), and conventions for picking work back up
- [`docs/statistics_roadmap.md`](docs/statistics_roadmap.md) - statistics ETL roadmap (connection unification, atomic transactions, verification streamlining, reference-directory clarity)

## License

See [LICENSE](./LICENSE) for details.
