# COEQWAL Data Platform

A comprehensive data platform for the Collaboratory for Equity in Water Allocation (COEQWAL) project for California water management scenario presentation, analysis, and review.

#### Code

The data platform code repository and documentation is organized into four sections(`data`, `etl`, `database`, `api`). Each section has its own series of (often nested) READMEs for details. 

#### AWS infrastructure

There is a local INFRASTRUCTURE.md that details AWS infrastructure values. A schematic of that infrastructure can be found at https://dev.coeqwal.org/aws_architecture.html.

## Data

Directory of reference files related to project data. The most important reference file is the CalSim manual pdf, which is too large to house outside LFS, but you can download it from DWR at https://water.ca.gov/Library/Modeling-and-Analysis/Central-Valley-models-and-tools/CalSim-3.

## ETL

The ETL is still under iterative development, but provides a fully-functioning scenario and tier loading process. Development process has been, with each successive batch loaded over a period of months, to improve the process each time. Next steps are to unify the pipeline steps ([`etl/run_full_pipeline.py`](etl/run_full_pipeline.py), in progress), unify the verification, and check the (currently rough) calculations. It is important to know that I typically only had time to do a quick first pass on the calculations, copied from the Water Allocation Modeling Team's Jupyter notebook as best I could given time constraints. Future iterations can harden these. I also accepted inefficiencies when working alone that could be improved for a more developer-friendly process.

The ETL has two parallel pipelines: one for scenario model run data and one for tier data.

**Pipeline 1**, for CalSim scenario model run data, has two sub-pipelines.

**The first subpipeline (1.1)** deposits a scenario's full model run directory (zipped) as well as an extracted full sv input csv and full dv output csv into a retrievable location in the S3 bucket. **The second subpipeline (1.2)** calculates statistics from this data and writes them to the database. 

**Pipeline 2**, for tier data, is independent and loads team-delivered csvs straight into the database. 

Details follow.

### 1.1 Scenario model run data (zipped file) is transferred from Google Drive to AWS S3 bucket and extracted to csv's

The developer guides the process through:

scan -> download -> promote -> AWS Batch | verify

Complete model run directories (zipped) plus their extracted CSVs land in S3 for the website's "Get Data" feature.

`gdrive_bulk_download.py` scans, downloads, and promotes scenarios from the COEQWAL shared Drive into `s3://coeqwal-model-run/ready/`. The `coeqwalEtlTrigger` Lambda fires on the promote, moves the ZIP under `scenario/<id>/run/`, and submits an AWS Batch job. The Batch container (`coeqwal-etl`) extracts DSS to CSV and, if a reference csv like the Water Allocation Modeling Team's Trend Report is also staged, runs `validate_csvs.py` against it. Mismatch counts are inlined into `extract_record.json`. Any mismatches are written to a csv in `scenario/<id>/validation/` (only if mismatches occur).

**Products** (under `s3://coeqwal-model-run/scenario/<id>/`):

- Original CalSim ZIP at `run/`
- Extracted DV (output) CSV and SV (input) CSV at `csv/`
- `extract_record.json` at the scenario root (validation result inlined)
- `<id>_validation_mismatches.csv` at `validation/` (on validation failure only)

**Website**: Get Data (download links served by the `coeqwalPresignDownload` Lambda).

### 1.2 Statistics -> database

Following the ingest and Batch extraction in 1.1., running `etl/statistics/run_all.py` reads the extracted CSVs from S3, computes derived metrics, and writes them into PostgreSQL tables.

The developer guides the process through:

statistics/run_all | verify

**Products**: Statistics rows in the database tables: reservoir storage, urban delivery, ag demand and shortage, MI contractor reliability, environmental flow alteration, refuge delivery, delta salinity / NDO / X2. Climate and operational sensitivity tables (`sensitivity_climate`, `sensitivity_operational`) are also populated when `run_all.py --with-sensitivity` is invoked. That post-processing step is *experimental, under development*: cross-scenario sensitivity analysis has no `verify_*` coverage and the script itself is labeled experimental in its own header. See [`etl/verification/README.md` §18 Roadmap](etl/verification/README.md#18-roadmap) item 6.

**Website**: Data in Depth (via the FastAPI service).

**Experimental orchestrator.** [`etl/run_full_pipeline.py`](etl/run_full_pipeline.py) wires the scan, download, promote, batch, stats, and verify stages into a single subprocess driver with `--resume` support. It has not been exercised end-to-end against AWS at handoff time. See [`etl/README.md` Experimental orchestrator](etl/README.md#experimental-orchestrator) for the caveats and the recommended direct-script workflow.

### 2. Tier data

Team-delivered tier outcome CSVs loaded straight into PostgreSQL, independent of the scenario data pipeline described above.

The developer drops correctly-formatted CSVs into [`etl/tier_data/staging/`](etl/tier_data/staging/). `etl/tier_data/scripts/load_all_tier_results.py --output-sql all_tiers.sql` builds the idempotent UPSERT SQL, which is then applied with `psql -f` to the `tier_result` and `tier_location_result` tables.

**Products**: Tier result rows in database.

**Website**: Tier visualization tools.

### Receipts, verification, and audits

Four correctness checks live in the codebase, answering the following questions:

1. **Did the DSS file convert to CSV correctly?** Inside every Batch job, the container runs `validate_csvs.py` to compare each extracted CSV against the modeling team's "trend report" reference CSV. Automatic, runs on every ingest.
2. **Did the statistics get loaded into Postgres correctly?** `etl/statistics/verify_all_sections.py` *(experimental, under development)* is a developer diagnostic, not an automated pipeline step. It re-reads the reference DV / SV CSVs that `run_all.py` consumed, independently recomputes the headline statistics in plain pandas, and compares the result against what the ETL wrote to the database. It is a spot check. The workflow is that reference CSVs are deliberately copied to `etl/reference/` before running. See [`etl/verification/README.md` Layer 2](etl/verification/README.md#layer-2-etl-statistics-csv-to-db) for the spot-check scope and the maintenance tax that comes with an independent verifier.
3. **Does the public API return those same numbers?** `etl/statistics/verify_api.py` hits `api.coeqwal.org` over HTTP and compares the API responses against direct database queries.
4. **For tier data: does the database match what the team handed us?** `etl/tier_data/scripts/verify_tiers.py` compares rows in the `tier_result` table against the staging CSVs the team delivered.

Alongside those checks, the pipeline leaves a deliberate paper trail at every stage. Tags below: `[S3]` lives in S3, `[local]` is on disk but gitignored, `[tracked]` is committed to git.

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
   writes:  [tracked once committed] etl/ingestion/audit.md
            ("what needs attention" digest across all scenarios)

----------------------------------------------------------------

[S3] csv/*.csv  ----> etl/statistics/run_all.py
                            |
                            +--> PostgreSQL stats tables
                            |
                            +--> [local] etl/statistics/audit_reports/
                                         stats_audit_<ts>.csv

PostgreSQL  ----> verify_all_sections.py  --\
            ----> verify_api.py              >--> [local, gitignored]
                                            -/    audits/verification_reports/
                                                  *_layer2.json
                                                  *_layer3.json

----------------------------------------------------------------

etl/tier_data/staging/  ----> load_all_tier_results.py
                                    |
                                    +--> PostgreSQL tier tables

PostgreSQL  ----> verify_tiers.py  ----> [local, gitignored]
                                         audits/verification_reports/
                                         tiers_<ts>.json

----------------------------------------------------------------

PostgreSQL  ----> database/audit/run_monthly_audit.py
                            |
                            +--> [tracked] audits/monthly_<ts>/
                                           (schema, row counts, entity exports)
```

For the full per-stage artifact table (writer, reader, location, contents), see [`etl/verification/README.md`](etl/verification/README.md#8-audit-artifacts-per-stage).

#### After a run: what to read

These verifications and audits evolved as I was building the etl bit by bit to solve the need at hand. Development of a unified command was in  (`etl/run_full_pipeline.py`), but for now, the diagram above is the full menu. You do not need to read all of the verification files after every run. Each kind of run feedback follows the same shape: **console first, then the scoreboard or digest if the console scrolled, then forensic detail only if something was flagged.** Stop at the first level that gives a clean answer. Ingest and statistics use the same pattern (see [verification README](etl/verification/README.md#two-pipelines-same-audit-pattern) for the side-by-side).

| Run | After a run, what do I read? |
|---|---|
| **`gdrive_bulk_download.py scan`**<br>(preflight against Google Drive, no S3 writes) | **Console:** prints a `SCAN AUDIT SUMMARY` block at the end of the run. Three parts, in this order:<br>- A totals header counting `Total scenarios`, `OK` (clean count), `Missing files`, `Multiple` (indicates more than one scenario version in the drive. Need to pin correct version in model_run_file_source_working.csv), `Folder mismatches`, `No drive access`, and `Local-only entries` (if that option is selected at run time).<br>- A per-scenario table, one row per scenario, with columns `Scenario` (the `short_code`), `Via` (how the scan reached Drive: `id` if `ModelFilesLink` resolved to a folder ID, `path` if it fell back to walking the Shared Drive by folder name, `none` if neither worked), `Zips` (count), `CSVs` (count, refers to Trend Report CSVs), `Match` (folder-name convention check: `OK`, `MISMATCH`, or `NO_DV_PATH` when the working CSV row has no `DV_Path` to compare against), `Status` (pipe-delimited failure codes, or `OK`).<br>- A `SCENARIOS REQUIRING ATTENTION` block. Present only when at least one scenario is non-OK and non-LOCAL_ONLY. Each entry carries the scenario id, status code, ZIP name, trend csv name, and folder name details for mismatches.<br><br>Clean-run signal: `OK (clean): N` equals `Total scenarios` and no `SCENARIOS REQUIRING ATTENTION` block follows.<br><br>**Forensic:** the run also writes `etl/ingestion/audit_reports/ingest_state.json` (`scan` block). Replay the saved per-scenario table later (e.g. when the terminal has scrolled, or to share with a teammate) with `python etl/ingestion/tools/show_last_run.py --stage scan`. It reads from `ingest_state.json` on disk and does not call Drive or S3. Scan never touches S3 and never updates `audit.md`.<br><br>See [`etl/verification/README.md` "Reading the scan summary"](etl/verification/README.md#reading-the-scan-summary) for what each status counter means, where to set `pinned_model_run_zip` / `pinned_trend_csv` when "Multiple (need pin)" fires, and the full fix table. |
| **`gdrive_bulk_download.py download`**<br>(Drive -> S3 staging, with validation) | **Console:**<br>- Ends with a `DOWNLOAD & VALIDATION SUMMARY` block with totals (`Total scenarios`, `OK`, `Skipped (review)`). A clean run reads `OK: N`, `Skipped (review): 0`, with no follow-up block. If `Skipped (review)` is non-zero, a `SCENARIOS REQUIRING REVIEW` block lists each flagged scenario with its `error_code` and `error_message`. A row is skipped (not staged to S3, not promoted) for one of: no Drive access, missing or extra ZIPs / trend CSVs, a pin pointing at a filename Drive no longer has, a corrupt ZIP, or the ZIP not containing the SV / DV basenames the working CSV declared. The full code catalog and the fix for each is in [`etl/verification/README.md` "Reading the download summary"](etl/verification/README.md#reading-the-download-summary).<br>- Then `audit.py` auto-runs (unless `--skip-audit` is passed) and prints three lines: `Audit written to etl/ingestion/audit.md. Review and commit it manually when ready.`, then `Summary: N active scenarios in S3, M need developer action (extraction failures: ..., validation failures: ..., convention warnings: ...)`, then `Validation: K passed, F failed, S skipped, W awaiting extraction.` The audit at this point reflects ingest-side state only, because Batch has not yet run for any of the newly-staged scenarios. On a clean run, `M` is zero, all three parenthesized counts are zero, and `W` equals the number of scenarios just staged (they all sit at "awaiting extraction" until Batch finishes).<br><br>**Digest:** [`etl/ingestion/audit.md`](etl/ingestion/audit.md). `audit.py` rewrites the entire file on every call (walks all `scenario/` prefixes in S3). The download command auto-calls it once at the end of the run, so this snapshot reflects ingest-side state only. The "Did Batch finish extracting all the scenarios?" row below picks the file up again once extraction settles. |
| **`gdrive_bulk_download.py promote`**<br>(staging -> ready, fires the Batch Lambda to do the dss -> csv extraction) | **Console:** both a summary and per-object lines.<br>- Pre-flight: `About to promote N scenario(s) from staging/scenario_data/ to ready/.`, then `Upload order per scenario: ingest_record.json -> trend CSV -> ZIP last.`, then a per-scenario plan line listing the three files in upload order.<br>- Per-object copy lines `Copying s3://.../staging/... -> s3://.../ready/...` for every file (three per scenario), plus one `[sXXXX] Promoted to ready/` line per scenario.<br>- Closes with `Done. Promoted N scenario(s) to ready/.` followed by `The Lambda will trigger on each ZIP upload.`<br><br>When `Done.` prints, every S3 PUT succeeded. The per-object lines are there so you can grep when something looks wrong, the summary is there for when the console scrolled.<br><br>**What happens next.** The ZIP PUT fires the Lambda, which submits one Batch job per scenario. Extraction runs asynchronously in AWS, roughly 20 minutes per scenario, multiple in parallel. The next row's `audit.py` is how you confirm each Batch job actually finished cleanly.<br><br>**Status hint while Batch is in flight.** `python etl/status.py` reports active and recently-terminated Batch job counts straight from the queue, with no S3 walk and no `audit.md` rewrite. Use it for a quick "is anything still running?" check between promote and the next audit. It does not tell you whether finished jobs succeeded. Only `audit.py` does that. |
| **Did Batch finish extracting all the scenarios?**<br>`python etl/ingestion/tools/audit.py` | Batch runs asynchronously in AWS, roughly 20 minutes per scenario, multiple scenarios in parallel (Fargate Spot spins containers up on demand, capped by the compute environment's `maxvCpus` setting. Each container uses 2 vCPU, so the queue can run up to `maxvCpus / 2` scenarios concurrently. If `maxvCpus` is 64, that is 32 scenarios in parallel. If it is 16, that is 8 in parallel).<br><br>When you are ready to check completion, run `python etl/ingestion/tools/audit.py`. It walks S3, rewrites `etl/ingestion/audit.md`, and prints two console lines: `Summary: N active scenarios in S3, M need developer action (extraction failures: X, validation failures: Y, convention warnings: Z)` (the headline action count) followed by `Validation: K passed, F failed, S skipped, W awaiting extraction.` (the symmetric breakdown).<br><br>If `M` is zero, every promoted scenario landed cleanly. If `M` is non-zero, the named scenarios are in [`etl/ingestion/audit.md`](etl/ingestion/audit.md) under "What needs your attention" with the per-scenario action attached (Batch job id, mismatches CSV key, retrigger command).<br><br>**Per-scenario validation outcomes** for every active scenario in S3 (not just the flagged ones) appear in `audit.md`'s `## Active scenarios` table under the `status` column. Values: `OK` (extraction and validation both clean), `VALIDATION_FAILED` (extraction OK, trend-report check found mismatches), `FAILED` (Batch did not produce the expected CSV), `PARTIAL` (one of SV / DV missing), `AWAITING_EXTRACTION` (Batch has not written `extract_record.json` yet), or `NO_INGEST_RECORD` (ZIP in S3 but no record alongside).<br><br>**Forensic:** when validation flags a scenario, **open `s3://<bucket>/scenario/<id>/validation/<id>_validation_mismatches.csv`**. This is the per-row diff between the extracted CSV and the WAM team's trend report. Decide from this file whether the trend report or the extracted CSV is wrong, then re-extract. `audit.md`'s "What needs your attention" block prints the exact `aws s3 cp` command for each flagged scenario, so you do not construct the path by hand. <br><br> Other forensic artifacts: <br>- `s3://<bucket>/scenario/<id>/extract_record.json` (the full Batch outcome record) and the `/aws/batch/job/...` <br>- CloudWatch log stream named by the job id in `audit.md`.<br><br>**Validation indicators**, in triage order: (1) the `audit.py` console answers "did anything fail" and "what was the breakdown" via two lines: `Summary: ... validation failures: Y ...` (count of failures) and `Validation: K passed, F failed, S skipped, W awaiting extraction.` (full breakdown). The same numbers also appear in `audit.md`'s `## Run summary` table under `Validation failures` and `Validation breakdown`. (2) `audit.md` `## What needs your attention` answers "which scenarios failed and what do I run" with copy-paste fix commands. (3) `audit.md` `## Active scenarios` table `status` column answers "what about every other scenario" with one row per active scenario in S3. (4) `_validation_mismatches.csv` at the S3 path above answers "what rows mismatched and by how much" with per-cell detail.<br><br>See [`etl/verification/README.md` "Checking that Batch finished cleanly"](etl/verification/README.md#checking-that-batch-finished-cleanly) for the live-tail command, per-job tuning, parallelism details, and the audit.md section-by-section guide. |
| **Statistics ETL**<br>`etl/statistics/run_all.py` | **Console:** `ETL PROCESSING SCORECARD` with per-scenario PASS / FAIL markers, a `SUMMARY` block with task totals, and a `FAILURES (need attention)` block if any row failed.<br><br>**Digest:** `etl/statistics/audit_reports/stats_audit_<ts>.csv`. Columns: `module, scenario, success, wall_time_sec, rows_written, error`. The `error` column carries the failure reason. This file is the scoreboard and the forensic detail in one. Statistics has no separate markdown digest yet (see [`etl/verification/README.md` §18 Roadmap](etl/verification/README.md#18-roadmap) item 1). |
| **Tier data load**<br>`etl/tier_data/scripts/load_all_tier_results.py` | **Console:** Per-tier row counts (e.g. `CWS_DEL: N location records, M scenario aggregates`), then `Manifest written: etl/tier_data/staging/tier_upload_manifest.csv` with totals. The manifest is regenerated on every normal run. Actual UPSERT row counts come from the `psql -f` step that applies the generated SQL, not from this script.<br><br>**Digest:** `etl/tier_data/staging/tier_upload_manifest.csv` (per-tier totals). To confirm the DB matches, re-run `load_all_tier_results.py --verify`. It compares. It does not regenerate the manifest. |
| **Model-run verification**<br>`verify_all_sections.py`, `verify_api.py` | **Console:** Per-scenario `VERIFICATION SUMMARY` block with `PASS` / `FAIL` / `Skipped` / `No DB data` counts and a `FAILED CHECKS` list when any check failed.<br><br>**Digest:** `audits/verification_reports/<scenario>_layer2.json` (from `verify_all_sections.py`, Layer 2: DB vs reference CSVs) and `<scenario>_layer3.json` (from `verify_api.py`, Layer 3: API vs DB). |
| **Tier verification**<br>`etl/tier_data/scripts/verify_tiers.py` | Part of the tier pipeline, not the model-run pipeline. Compares loaded tier rows against `etl/tier_data/staging/` CSVs.<br><br>**Console:** One row per tier code (e.g. `PASS WRC_SALMON_AB    (N checks, 0 mismatches)` or `FAIL ... (N checks, M issues: ...)`), then an `Overall: X/Y tiers PASS` (or `Overall: X/Y tiers PASS, Z FAIL`) line and a `Detail: <json_path>` pointer.<br><br>**Digest:** `audits/verification_reports/tiers_<ts>.json`. See [`etl/tier_data/README.md`](etl/tier_data/README.md). |
| **Monthly database audit**<br>`database/audit/run_monthly_audit.py` | **Console:** Ends with a `MONTHLY AUDIT COMPLETE` block naming the output directory and the report filename.<br><br>**Digest:** `audits/monthly_<ts>/report.md` for the top-level summary (row counts, ERD diff, audit-field checks). Drill into `layer_exports/` or `results_samples/` only if a section is flagged. |
| **Status check** ("when did X last run?")<br>`python etl/status.py` | **Console:** Freshness across six sections: ingestion, batch, statistics, tiers, verification, and database connectivity. |

**Rule of thumb.** Trust the headline at each level (console, then `audit.md` or JSON report, then `stats_audit_<ts>.csv` or `report.md`). The forensic artifacts in the diagram exist for triage. Read them only when the headline says to.

[`etl/README.md`](etl/README.md) has the developer runbook for running the whole ETL pipeline.

## Database

PostgreSQL + PostGIS on RDS. A highly-normalized schema of ~96 tables organized conceptually into layers.

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
