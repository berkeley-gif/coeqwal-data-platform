# ETL (Extract, Transform, Load)

## Contents

- [Running the scenario model-run pipeline](#running-the-scenario-model-run-pipeline)
  - [1. Sync the scenario listing and refresh `ETL_SCENARIOS`](#1-sync-the-scenario-listing-and-refresh-etl_scenarios)
  - [2. Scan](#2-scan)
  - [3. Download](#3-download)
  - [4. Promote](#4-promote)
  - [5. Extract (AWS Batch), then audit](#5-extract-aws-batch-then-audit)
  - [6. Compute statistics](#6-compute-statistics)
  - [7a. Verify statistics against reference CSVs](#7a-verify-statistics-against-reference-csvs-experimental-under-development)
  - [7b. Verify the public API](#7b-verify-the-public-api)
  - [8. Activate scenarios (and hide or restore them later)](#8-activate-scenarios-and-hide-or-restore-them-later)
- [Operational topics](#operational-topics)
  - [The scenario table and its metadata](#the-scenario-table-and-its-metadata)
  - [Updating the working CSV](#updating-the-working-csv)
  - [Recovery and manual operations](#recovery-and-manual-operations)
    - [Manual upload](#manual-upload)
    - [Re-extraction](#re-extraction)
  - [Experimental orchestrator](#experimental-orchestrator)
  - [Troubleshooting](#troubleshooting)
- [ROADMAP](#roadmap)

## Running the scenario model-run pipeline

The run sequence:

> `scan` → `download` → `promote` → `AWS Batch` → `audit` → `run_all (statistics)` → `verify` → `activate`

Each step below leads with the commands and arguments, so an developer familiar witih the pipeline can move down the page running them in order. The details (what each step does, what to check) follow under each command block.

The vision is a fully automated pipeline through an orchestrator (see `etl/run_full_pipeline.py`), and it's close (it needs to be tested and troubleshot, then an EventBridge rule set up to fire the statistics stage after the AWS Batch job finishes). Until then, run the steps by hand.

**Signals to check** under each step:

- **Console** - as the command runs. The Batch step is the exception: those jobs run in AWS, not your terminal, so their console is **CloudWatch** - `/aws/lambda/coeqwalEtlTrigger`.
- **Sidecar** - per-scenario receipt, i.e. the JSON records and mismatch CSV in S3, plus the local `ingest_state.json`. Open one when a single scenario is flagged and you need more info.
- **Report** - the aggregated digest. The main one is the **audit report** ([`etl/ingestion/audit.md`](ingestion/audit.md)), rendered by `etl/ingestion/tools/audit.py`. It auto-renders at the end of `gdrive_bulk_download.py download` (pass `--skip-audit` to defer), and you can re-run it by hand (`python etl/ingestion/tools/audit.py`) after AWS Batch finishes. Promote and Batch do *not* auto-refresh it, so it shows the previous run until you re-render. It reads the `download` block of `ingest_state.json` plus every scenario's `ingest_record.json` and `extract_record.json` in S3, cross-references them, and writes one markdown digest. Its sections: `## Run summary` (headline counts), `## What needs your attention` (ingest skips, missing ingest records, extraction failures, validation failures, convention warnings, each with the exact fix command), `## Unverified scenarios` (if there are any), `## Active scenarios`, `## Per-scenario details` (expanded for non-OK rows, `--all` for every row), and an `## Appendix` pointing at the per-scenario JSON records. Read it first, and then open a scenario's JSON record or CloudWatch only when you need to dig into a single run. Statistics and verification add their own per-stage reports: `stats_audit_<ts>.csv` and the `*_layerN.json` files.

A nice roadmap task would be to consolidate and clean these up. They were created at different times to answer different questions as the ETL was developing.

Across all stages, `python etl/status.py` is a read-only **dashboard** (no flags beyond `--help`) that prints a one-screen snapshot in six sections: **Ingestion** (working-CSV row count, last download run, last orchestrator/pipeline run), **Batch (AWS)** (active jobs plus last-24h SUCCEEDED / FAILED), **Statistics** (latest `stats_audit_<ts>.csv` and its row count), **Tiers** (the loader's `tier_version_id`), **Verification** (latest report and how many are on disk), and **Connectivity** (live RDS / AWS / S3 / rclone pings). Run it anytime to see where you are or whether something landed.

For more details on verification, see [`etl/verification/README.md`](verification/README.md).

**Tier outcomes pipeline** has its own runbook: [`etl/tier_data/README.md`](tier_data/README.md).

Developer runbook for loading **scenario model run data** and **tier data** on Cloud9, summarized in the [top-level README](../README.md#etl).

> **Prep first: put the scenario's identity row in the DB:** Inserting a scenario into the `scenario` table is a *prep* step, not part of the ETL, technically. For each new scenario, insert the row - `short_code`, `run_name`, `hydroclimate_id`, `hydroclimate_sibling`, `scenario_version_id`, `scenario_author_id`, `model_source_id` with `is_active = FALSE`, so every later step keys to a scenario the catalog already knows. Author a one-shot insert modeled on [`add_s0107-s0156_scenarios.sql`](../database/scripts/sql/add_s0107-s0156_scenarios.sql) and run it: `psql "$DATABASE_URL" -f database/scripts/sql/add_<batch>_scenarios.sql`. The descriptive metadata (the sibling group's name and descriptions, theme/assumption/operation links, etc.) often isn't agreed upon or settled yet. That's fine, it can land into the tables later. Only the identity row needs to exist now. Activation (step 8) flips the `is_active` boolean. Details: [The scenario table and its metadata](#the-scenario-table-and-its-metadata).

### 1. Sync the scenario listing and refresh `ETL_SCENARIOS`

```bash
# 1. Download Dino's spreadsheet as CSV (link in the details below), save to
#    etl/ingestion/scenario_listing/model_run_file_source.csv
# 2. Copy it into model_run_file_source_working.csv, then create and edit its four
#    developer-managed columns for this run:
#      - pinned_model_run_zip : exact ZIP basename, when the Drive folder has >1
#      - pinned_trend_csv     : exact trend-CSV basename, when the folder has >1
#      - download_status      : "skip" or "retired" to exclude a row
#      - notes                : free-text, surfaced in the audit
# 3. Generate the list of scenarios to process (ETL_SCENARIOS in
#    etl/common/etl_scenarios.py) from the working CSV:
python etl/ingestion/tools/refresh_etl_scenarios.py
```

**Useful flags** `--dry-run` prints the regenerated `ETL_SCENARIOS` and a change summary without writing, so you can preview the set before committing. `--working-csv` / `--etl-py` override the input and output paths.

**What it does:**

- Download [`coeqwal_cs3_scenario_listing_v7`](https://docs.google.com/spreadsheets/d/1pzbVx191VYXgHcZNhAqJEKNn3lN8GCZo) (Dino's spreadsheet, current as of May 28, 2026) as CSV, save it as [`etl/ingestion/scenario_listing/model_run_file_source.csv`](ingestion/scenario_listing/model_run_file_source.csv), then copy that file into [`model_run_file_source_working.csv`](ingestion/scenario_listing/model_run_file_source_working.csv).
- `refresh_etl_scenarios.py` generates the list of scenarios the ETL will process, `ETL_SCENARIOS` in [`etl/common/etl_scenarios.py`](common/etl_scenarios.py), from the working CSV (rows whose `download_status` is `skip` or `retired` are excluded), bringing the list up to date for this run.
- `ETL_SCENARIOS` is the set the bulk flags expand to: `scan` and `download` here (`--all`), plus statistics (`run_all.py`) and statistics verification (`verify_all_sections.py`) later (`--all-scenarios`). (The one exception is `verify_api.py --all-scenarios`, which expands to the narrower `ACTIVE_SCENARIOS` - the scenarios live on the website. See Step 7b.) So whenever you change which rows belong (add a scenario row, or set a `download_status` to `skip`/`retired`), re-run this refresh or those bulk runs keep using the previous set. Edits to an existing row's other columns (pins, notes) are read fresh from the working CSV on every run and need no refresh.

**Why edit those columns:** The four developer-managed columns are how you steer this run:

- `pinned_model_run_zip` / `pinned_trend_csv` name the file to use when a scenario's Drive folder holds more than one candidate. These point at two Drive files: the **model run ZIP** (the CalSim run the pipeline ingests and extracts to CSV) and the **trend report CSV** (the WAM team's reference export). A candidate is any `.zip` in `Model_Files/` or any `.csv` in the trend folder. The ZIP is required and strict: more than one with no pin (`MULTIPLE_ZIPS_NO_PIN`), a pin that matches nothing (`PINNED_ZIP_NOT_FOUND`), or none at all (`MISSING_ZIP`) refuses the row, so it never stages. The trend CSV is optional and lenient: a missing CSV is noted in the audit (`unverified_multi_trend`, `unverified_pin_missing`, `unverified_no_trend`) but the scenario still stages.
- `download_status` set to `skip` or `retired` excludes the row from `ETL_SCENARIOS` (after a refresh), so every bulk run skips it: `scan`, `download` (`--all`), statistics, and verification (`--all-scenarios`). Anything else (blank, `done`, `needs_review`, ...) is included. An explicit `--scenarios s0042` always processes exactly the rows you name, regardless of this column.
- `notes` is free-text scratch, surfaced in the audit, but not read by a pipeline step.

The trend report CSV is the Water Allocation Modeling (WAM) team's reference export of expected values for a scenario. The Batch step (Step 5) validates each extracted CSV against it. It is optional: a scenario with no trend CSV still loads, just flagged unverified.

In the typical case, every scenario's Drive folder holds exactly one ZIP and one trend CSV. Then nothing is ambiguous to pin and nothing needs excluding, so you leave the four columns untouched. You only edit when a folder holds more than one ZIP or trend CSV (pin the right one) or you want to drop a row (`download_status`).

For the full procedure - refreshing the reference CSV from Dino's sheet, reconciling the working copy row-by-row, the complete column definitions, and regenerating the cached constant - see [Updating the working CSV](#updating-the-working-csv).

**Signals to check**

- **Console:** `refresh_etl_scenarios.py` logs how many rows it kept and excluded
- **Sidecar:** none (prep step, no per-scenario receipt)
- **Report:** the regenerated `etl/common/etl_scenarios.py` plus `model_run_file_source.csv` and `model_run_file_source_working.csv`. You can commit these.

### 2. Scan

```bash
# every scenario in ETL_SCENARIOS (skip/retired excluded):
python etl/ingestion/gdrive_bulk_download.py scan --all
# or a shorter list:
python etl/ingestion/gdrive_bulk_download.py scan --scenarios s0042 s0043
# check: per-scenario scan results
python etl/ingestion/tools/show_last_run.py --stage scan
```

**Useful flags** `--workers N` sets Drive-listing concurrency (default 4). `--local-only` parses the working CSV and writes the manifest without contacting Drive (a fast offline sanity check). Run `... scan --help` for the rest.

**What it does** Inventory each scenario against the working CSV and Drive. No downloads, no S3 writes. Writes the `scan` block of `etl/ingestion/audit_reports/ingest_state.json`.

**Signals to check**

- **Console:** `SCAN AUDIT SUMMARY` - per-scenario `OK (clean)` vs `Missing files` / `Multiple (need pin)` / `Folder mismatches` / `No drive access`.
- **Sidecar:** the `scan` block of `etl/ingestion/audit_reports/ingest_state.json` (local). Reprint it without re-walking Drive: `python etl/ingestion/tools/show_last_run.py --stage scan`.
- **Report:** none. Scan results live only in the console summary and the `scan` block. They never reach `audit.md`, which renders from the `download` block (Step 3 / 5).

### 3. Download

```bash
# every scenario in ETL_SCENARIOS (skip/retired excluded, re-pulls each from Drive):
python etl/ingestion/gdrive_bulk_download.py download --all
# or a shorter list:
python etl/ingestion/gdrive_bulk_download.py download --scenarios s0042 s0043
# check: audit.md auto-renders at the end; for per-scenario detail:
python etl/ingestion/tools/show_last_run.py --stage download
```

**Useful flags** `--workers N` sets download concurrency (default 4). `--dry-run` lists what it would pull without downloading or writing to S3. `--skip-audit` suppresses the end-of-run `audit.md` render (re-run `audit.py` yourself later).

**What it does** Pull each scenario's ZIP and trend CSV from Drive via rclone, validate filenames, compute SHA-256, stage to `s3://<bucket>/staging/`. Writes `ingest_record.json` to S3 and updates the `download` block of `ingest_state.json`.

**Signals to check**

- **Console:** `DOWNLOAD & VALIDATION SUMMARY` (`OK: N`, `Skipped (review): 0`).
- **Sidecar:** each scenario's `ingest_record.json` in `s3://<bucket>/staging/scenario_data/<id>/`, plus the `download` block of the local `ingest_state.json` (`python etl/ingestion/tools/show_last_run.py --stage download`).
- **Report:** [`etl/ingestion/audit.md`](ingestion/audit.md), auto-rendered at the end. Open its `## What needs your attention` section.
- **Dashboard:** `status.py`: Ingestion section (last download run, scenarios in state).

### 4. Promote

```bash
# promote everything currently in staging:
python etl/ingestion/gdrive_bulk_download.py promote
# or a shorter list:
python etl/ingestion/gdrive_bulk_download.py promote --scenarios s0042 s0043
# --dry-run to plan only:
python etl/ingestion/gdrive_bulk_download.py promote --dry-run
# check: confirm the promote fired the Lambda and a Batch job was submitted
python etl/status.py   # see the "Batch (AWS)" section: Active jobs > 0
```

**What it does** Copy each scenario's staged files from `staging/scenario_data/<id>/` to `s3://<bucket>/ready/<id>/` in the safe order (`ingest_record.json` -> trend CSV -> ZIP last). Staging is left in place (`copy_object`, not a move). The ZIP PUT under `ready/` is the Lambda trigger.

**Signals to check**

- **Console:** copy-by-copy lines confirm the PUTs landed. The downstream signal is the dispatch: `python etl/status.py` (`Batch (AWS)` -> `Active jobs`) or `Submitted Batch job ...` in `/aws/lambda/coeqwalEtlTrigger` (CloudWatch). Listing `ready/` isn't informative because the Lambda moves the ZIP out to `scenario/<id>/run/` as soon as it fires.
- **Sidecar:** none new. Promote copies the existing `ingest_record.json` and ZIP into `ready/<id>/`.
- **Report:** none. `audit.md` is *not* auto-refreshed by promote or Batch.

### 5. Extract (AWS Batch), then audit

```bash
# Batch runs in AWS automatically once promote fires the Lambda. Monitor it:
aws logs tail /aws/lambda/coeqwalEtlTrigger --follow
# Once the jobs settle, re-render the audit locally:
python etl/ingestion/tools/audit.py
# check: inspect a flagged scenario's validation mismatches (header-only on pass)
aws s3 cp s3://<bucket>/scenario/s0042/validation/s0042_validation_mismatches.csv -
```

**Useful flags (`audit.py`)** `--all` expands per-scenario detail for every scenario, not just flagged rows. `--dry-run` prints the rendered markdown to stdout instead of writing `audit.md`. To re-run a failed extraction, see the re-extraction tools (`reextract_all_scenarios.py`, `retrigger_extraction.sh`) below.

**What it does** Lambda dispatches AWS Batch. The container converts DSS to CSV, runs `validate_csvs.py` against the Trend Report, writes `extract_record.json` and `<id>_validation_mismatches.csv` to S3 (header-only on pass, populated on fail). Per-job wall time is 5-30 minutes. Jobs run in parallel up to the queue's compute cap. `audit.py` re-renders `etl/ingestion/audit.md` with the extraction outcomes.

**Signals to check**

- **Console:** CloudWatch - `/aws/lambda/coeqwalEtlTrigger` for the dispatch, and the Batch container's own log group for `validate_csvs.py` `PASSED` / `FAILED`.
- **Sidecar:** per scenario in `s3://<bucket>/scenario/<id>/`: `extract_record.json` (pass/fail inlined) and `validation/<id>_validation_mismatches.csv` (header-only on pass, rows on fail).
- **Report:** [`etl/ingestion/audit.md`](ingestion/audit.md) after re-running `audit.py`: `## Run summary` for the `Validation failures` count, `## What needs your attention` for flagged scenarios.
- **Dashboard:** `status.py` Batch (AWS) section (active jobs, last-24h SUCCEEDED / FAILED).

### 6. Compute statistics

```bash
# backfill every scenario in ETL_SCENARIOS (the curated ETL set, not the live/active set):
python etl/statistics/run_all.py --all-scenarios
# or a single scenario:
python etl/statistics/run_all.py --scenario s0042
# sensitivity post-step (experimental), run after the per-scenario runs complete:
python etl/statistics/run_all.py --all-scenarios --with-sensitivity
```

**Useful flags** For a full backfill, the recommended invocation is `run_all.py --all-scenarios --workers 4 --batch-size 20`. `--workers N` runs scenarios in parallel (~2-3 GB RAM each), `--batch-size N` + `--start-from sXXXX` make long runs chunked and resumable, `--continue-on-error` switches from fail-fast to fail-soft, and `--only <modules>` runs a subset of the 8 modules. Full reference including the `--workers` sizing table: [`etl/statistics/README.md` § Running the statistics ETL](statistics/README.md#running-the-statistics-etl).

**What it does** Read each scenario's CSVs from S3, compute derived metrics across the 8 per-scenario modules (reservoir, urban DU, ag, M&I, env flow, refuge, delta, CWS aggregates), write to PostgreSQL via UPSERT. The `--with-sensitivity` post-step is *experimental, under development*.

**Rerunning against CSVs already in S3** This step reads from `s3://<bucket>/scenario/<id>/csv/` and writes to the database, so it is independent of Steps 1-5. If a scenario's CSVs are already in S3 from a prior ingest, you can rerun statistics on their own without repeating scan/download/promote/Batch, using the same commands above. `--scenario s0042` reprocesses one scenario directly and does *not* read `ETL_SCENARIOS` (no refresh needed). `--all-scenarios` reprocesses the whole set and *does* read `ETL_SCENARIOS`, so make sure Step 1's refresh is current first. Rerunning is idempotent: it replaces that scenario's rows rather than appending, so it is always safe to run again.

**Signals to check**

- **Console:** `ETL PROCESSING SCORECARD` (✅ / ❌ / ⏭️ / ⚪ per module) and its `SUMMARY` block.
- **Sidecar:** none. The load target is the database itself. A direct query or Step 7 confirms the rows landed.
- **Report:** `etl/statistics/audit_reports/stats_audit_<ts>.csv` (row-by-row, with an `error` column for failures).
- **Dashboard:** `status.py` Statistics section (latest `stats_audit_<ts>.csv` + row count).

### 7a. Verify statistics against reference CSVs (*experimental, under development*)

```bash
# Reference CSVs (DV + SV) must be in audits/notebooks_reference/ first (or pass --ref-dir).
# every scenario in ETL_SCENARIOS:
python etl/statistics/verify_all_sections.py --all-scenarios
# or a single scenario:
python etl/statistics/verify_all_sections.py --scenario s0042
```

**Useful flags** `--ref-dir <path>` points at a different reference-CSV directory. `--with-tiers` adds tier checks to the run. `--json-stdout` / `--no-json` shape where the report goes. Full scope in [`etl/verification/README.md`](verification/README.md).

**What it does** Recompute statistics from reference CSVs and compare against the database. Spot check, not exhaustive. See [`etl/verification/README.md`](verification/README.md) for scope and maintenance tax.

**Signals to check**

- **Console:** `VERIFICATION SUMMARY`, ending in `Overall: N/N sections PASS`.
- **Sidecar:** none.
- **Report:** `audits/verification_reports/<id>_layer2.json`.
- **Dashboard:** `status.py` Verification section (latest report, count on disk).

### 7b. Verify the public API

```bash
# every scenario currently active on the website (is_active=TRUE):
python etl/statistics/verify_api.py --all-scenarios
# or pre-flight a list of not-yet-active scenarios (bypasses the active set for this run):
python etl/statistics/verify_api.py --scenarios-override s0042,s0043,s0044
# or a single scenario:
python etl/statistics/verify_api.py --scenario s0042
```

**Useful flags** `--api-url <url>` targets a non-production API (default is production). `--json-stdout` / `--no-json` shape where the report goes.

**What it does** Compare the public API responses against direct database queries.

**Signals to check**

- **Console:** `API VERIFICATION SUMMARY` (`FAIL: 0`, `Mismatch: 0`).
- **Sidecar:** none.
- **Report:** `audits/verification_reports/<id>_layer3.json`.
- **Dashboard:** `status.py` Verification section (latest report, count on disk).

### 8. Activate scenarios (and hide or restore them later)

```bash
# rows must already exist in the scenario table (see "How a scenario gets
# into the database"). Activate a fresh batch in one call:
python etl/ingestion/tools/set_scenario_active.py --activate \
  s0107 s0108 s0109 s0110 s0111
# later, take a live scenario off the site (or restore it):
python etl/ingestion/tools/set_scenario_active.py --deactivate s0042 s0043
python etl/ingestion/tools/set_scenario_active.py --activate s0042 s0043
```

**Useful flags** `--dry-run` prints the planned `UPDATE`s without touching the DB. `--skip-refresh` flips the DB but skips the cached-list regenation (see section below). Use it only when issuing several separate invocations in one session, so you run `refresh_active_scenarios.py` once at the end instead of after each call.

**What it does** Flip `scenario.is_active` for one or many scenarios in a single `UPDATE`, then regenerate the cached `ACTIVE_SCENARIOS` constant so the change reaches the public API. Use `--activate` for the bulk go-live of a freshly loaded batch, and `--deactivate` (or re-`--activate`) for the ongoing toggle of a scenario that is already live. You can pass the scenario short codes with space-, comma-, or newline-separated, so pasting a column straight from a spreadsheet works. Rows for those scenarios in the `scenario` RDS table must already exist. To add them, see [The scenario table and its metadata](#the-scenario-table-and-its-metadata).

**Signals to check**

- **Console:** the Before/After `is_active` tables, the `N activated, M deactivated` line, then `refresh_active_scenarios.py`'s `Fetched N scenarios; M are active` log.
- **Sidecar:** none.
- **Report:** the regenerated `etl/common/active_scenarios.py` and the `<!-- ACTIVE_SCENARIOS:BEGIN -->` block in `README.md` (diff both). Confirm live coverage with the curl snippet in [Confirm live scenario coverage](../README.md#confirm-live-scenario-coverage).

Tier data verification (`verify_tiers.py`) belongs to the tier pipeline, not this one. See [`etl/tier_data/README.md`](tier_data/README.md) for the tier-data workflow including loading and verification.

## Operational topics

## The scenario table and its metadata

`scenario` is a thin catalog table: identity and per-run attributes only. Its columns:

- `id`
- `short_code`
- `run_name`
- `is_active`
- `hydroclimate_id`
- `hydroclimate_sibling`
- `scenario_version_id`
- `scenario_author_id`
- `model_source_id`
- `created_by`
- `updated_by`
- `created_at`
- `updated_at`

The human-readable metadata lives in the tables around it:

- **`scenario_hydroclimate_sibling`** holds the display `name`, `short_description`, and `long_description`, keyed per sibling group and **shared across a strategy's hydroclimate variants**. Each scenario points at its group through `hydroclimate_sibling`.
- **`hydroclimate`** holds the list of hydroclimates, **`scenario_author`** the author, and **`model_source`** the producing model, for now always CalSim3. These are the FK targets the scenario row points at.
- Classification is many-to-many through link tables: **`theme_scenario_link`**, **`scenario_tag_link`**, **`scenario_key_assumption_link`**, and **`scenario_key_operation_link`**, each pairing `scenario.id` with a theme / tag / assumption / operation.

## Updating the working CSV

The scenario listing has two CSVs in the repo, both tracked in git. Together with one auto-generated Python module they form the three-way contract that every ingestion stage gates on.

| File | Role | Who writes it |
|---|---|---|
| [`etl/ingestion/scenario_listing/model_run_file_source.csv`](ingestion/scenario_listing/model_run_file_source.csv) | Reference snapshot, exported as-is from the WAM team's Google Sheet. Typically about 12 columns. | The developer, by hand, when the upstream sheet changes. |
| [`etl/ingestion/scenario_listing/model_run_file_source_working.csv`](ingestion/scenario_listing/model_run_file_source_working.csv) | Developer-editable copy. Add 4 developer-managed columns, if they don't exist already. The ingestion scripts read this file, not the reference. | The developer, row-by-row. |
| [`etl/common/etl_scenarios.py`](common/etl_scenarios.py) (`ETL_SCENARIOS`) | Cached set of scenarios the ETL is intended to process. The bulk flag expands to this set: `--all` for `scan` and `download`, `--all-scenarios` for `run_all.py` and `verify_all_sections.py`. (`verify_api.py --all-scenarios` uses `ACTIVE_SCENARIOS` instead.) | Auto-generated by `python etl/ingestion/tools/refresh_etl_scenarios.py`. |

The 5 essential columns (Index, GoogleDriveFolderName, ModelFilesLink, DV_Path, SV_Path) must be present in the header or `gdrive_bulk_download.py` refuses to start. The 4 developer-managed columns are optional. Authoritative definitions in [`etl/ingestion/lib/config.py`](ingestion/lib/config.py) (`COLUMN_MAP`, `ESSENTIAL_FIELDS`).

Four sub-steps to bring all three artifacts up to date for a new run.

**0.1. Refresh the reference CSV from Dino's spreadsheet**

The source is informally called "Dino's spreadsheet". Formally it is the WAM team source spreadsheet, hosted on Google Sheets. The URL is pinned in [`etl/ingestion/lib/config.py`](ingestion/lib/config.py) (`SPREADSHEET_URL`) and recorded into every scenario's `ingest_record.json` under `source.spreadsheet_url`:

```
https://docs.google.com/spreadsheets/d/1pzbVx191VYXgHcZNhAqJEKNn3lN8GCZo/edit?gid=371742646#gid=371742646
```

Snapshot the existing reference into the archive directory first so the previous state is preserved, then download a fresh copy from the sheet:

```bash
cp etl/ingestion/scenario_listing/model_run_file_source.csv \
   etl/ingestion/scenario_listing/model_run_file_source_archive/model_run_file_source_$(date +%Y%m%d).csv
```

In the browser, open the sheet, switch to the WAM tab (`gid=371742646`), then `File -> Download -> Comma-separated values (.csv, current sheet)`. Save the download as `etl/ingestion/scenario_listing/model_run_file_source.csv`, overwriting the prior copy. There is no scripted refresh yet (see the `Dino's spreadsheet` roadmap entry at the bottom of this file).

**0.2. Bring the working CSV up to date**

If the working CSV does not exist (fresh clone, accidentally deleted), bootstrap it from the reference. This is the exact command the script prints when the file is missing (see [`etl/ingestion/lib/csv_reader.py`](ingestion/lib/csv_reader.py) `_bootstrap_error_message`):

```bash
cp etl/ingestion/scenario_listing/model_run_file_source.csv \
   etl/ingestion/scenario_listing/model_run_file_source_working.csv
```

That gives a 12-column working CSV. The reader tolerates the 4 developer-managed columns being absent. Add them to the header (or to individual rows) only when you need them.

If the working CSV already exists (the normal case in this repo), reconcile it with the refreshed reference by hand: append rows from the reference for any scenarios that are new to the sheet, and preserve the four developer-managed column values on rows that already existed.

**0.3. Edit the working copy for this run**

The 4 developer-managed columns. Definitions match the comments in [`etl/ingestion/lib/config.py`](ingestion/lib/config.py) `COLUMN_MAP`:

| Column | What it does | Read by | Set when |
|---|---|---|---|
| `pinned_model_run_zip` | Exact basename of the ZIP to pick from the scenario's `Model_Files/` folder. Disambiguator. | `gdrive_bulk_download.py` | The Drive folder contains more than one ZIP. Without a pin, `scan` and `download` refuse the row with `MULTIPLE_ZIPS_NO_PIN`. |
| `pinned_trend_csv` | Exact basename of the trend report CSV to use as the validation reference. | `gdrive_bulk_download.py` | The Drive folder contains more than one trend CSV. Without a pin, the scenario stages anyway but is flagged `unverified_multi_trend` in the audit. |
| `download_status` | Informational with two reserved values. **`skip`** or **`retired`** exclude the row from `ETL_SCENARIOS`. Blank, `done`, `needs_review`, or any free-form value is included. | Read by `refresh_etl_scenarios.py`. `gdrive_bulk_download.py` does not read it directly, but its `--all` runs over `ETL_SCENARIOS`, so a `skip`/`retired` row is excluded there too. CLI scope is set with `--scenarios` / `--all`, not by editing this column. | You want to permanently or temporarily remove a row from the `ETL_SCENARIOS` set (legacy scenario, broken upstream data, paused project) without deleting the row from the working CSV. |
| `notes` | Free-text scratch. Surfaced in the audit. | Audit, for context. Not read by any pipeline step. | Always optional. Useful for explaining why a row was retired or why a pin was set. |

The 12 columns inherited from the reference CSV are read-only from the developer's perspective:

| Column | Internal name | What it is |
|---|---|---|
| `Index` | `short_code` | The scenario's identifier (e.g. `s0042`). Joins to every downstream artifact. Essential. |
| `StudyName` | informational | Human-readable identifier. |
| `GoogleDriveFolderName` | `drive_folder_name` | The Drive folder name. Essential. |
| `ModelFilesLink` | `drive_folder_url` | The Drive folder URL. The folder ID is regex-extracted from `/folders/<id>`. Essential. |
| `HydroClimate`, `ShortDescription` | informational | Scenario assumptions. Surfaces in `audit.md` and `ingest_record.json`. |
| `DV_Path` | `dv_path` | Full Drive path to the expected DV (CalSim output) DSS file. Only the basename is matched at ingest time. Essential. |
| `SV_Path` | `sv_path` | Full Drive path to the expected SV (CalSim input) DSS file. Only the basename is matched at ingest time. Essential. |
| `Start_Date`, `End_Date` | informational | Simulation horizon. |
| `Source` | informational | Originating agency or study (e.g. `DWR`). |
| `Metadata` | informational | Optional free-form text. |

**0.4. Regenerate the cached `ETL_SCENARIOS` constant**

```bash
python etl/ingestion/tools/refresh_etl_scenarios.py
```

Rewrites [`etl/common/etl_scenarios.py`](common/etl_scenarios.py) from the working CSV. Rows with `download_status` of `skip` or `retired` are excluded. Everything else is included. The script logs how many rows it kept and excluded.

`ETL_SCENARIOS` is the broader "what the ETL knows how to process" set. `ACTIVE_SCENARIOS` in [`etl/common/active_scenarios.py`](common/active_scenarios.py) is the narrower "what the website serves" set (regenerated from the live API by `refresh_active_scenarios.py`). Both files distinguish themselves in their auto-generated docstrings. The bulk flag expands to `ETL_SCENARIOS`: `--all` for `scan` and `download` (Steps 2-3), `--all-scenarios` for `run_all.py` and `verify_all_sections.py`. The exceptions gate on `ACTIVE_SCENARIOS` instead: `verify_api.py --all-scenarios`, the tier loader, and tier verification.

Commit the diff in all three files (`model_run_file_source.csv`, `model_run_file_source_working.csv`, `etl_scenarios.py`) plus the new archive snapshot as one change so the reference, the working copy, and the cached constant move forward together.

## Recovery and manual operations

- [Manual upload](#manual-upload) - when you want to load scenario model run zip files into the S3 bucket for Batch processing, or the ZIP is there but its sidecar `ingest_record.json` is missing.
- [Re-extraction](#re-extraction) - the ZIP is already in `scenario/<id>/run/` and you need to re-run extraction.

### Manual upload

Two ways:

- **`tools/manual_ingest.py upload`** - scripted: enforces upload order and builds the ingest record for you (DV/SV entries pinned, SHA-256 computed from the ZIP). Prefer this when the ZIP is ambiguous or you want hashes recorded at upload time.
- **AWS console drag-and-drop** - no script needed, fine for one or two scenarios. The developer is responsible for upload order (ZIP last, because the ZIP PUT is the Lambda trigger).

#### Upload a new scenario from a local ZIP

```bash
python etl/ingestion/tools/manual_ingest.py upload \
    --short-code s0042 \
    --zip-path /path/to/s0042.zip \
    --trend-csv-path /path/to/s0042_trend.csv \
    --dv-basename s0042_dv.dss \
    --sv-basename coeqwal_s9999_sv_v0.1.4.dss
```

Hashes the ZIP and the DV/SV entries inside it, builds the ingest record, uploads in safe order: `ingest_record.json` -> trend CSV -> ZIP last. Defaults to `staging/scenario_data/<id>/`. Pass `--dest-prefix ready` to bypass `promote` and trigger Lambda immediately (use with care). `--trend-csv-path` is optional.

#### Upload through the AWS console

The Batch container needs an `ingest_record.json`. If you drop a ZIP into `ready/<id>/` with no record beside it, the Lambda waits a short grace window for one to arrive, then writes a **minimal** record with `ingestion.path = "manual_inferred"` - empty DV/SV basenames and hashes, it does not open the ZIP - and submits Batch. The Batch container is what actually picks the DV/SV entries by basename and verifies hashes, so pure drag-and-drop still works. The audit report flags the inferred row for review.

Upload order, when uploading by hand through the S3 console:

1. `ingest_record.json` first (skip this file entirely if you want the Lambda to infer)
2. The trend CSV (if you have one)
3. The ZIP last, because the ZIP PUT is the Lambda trigger

Include an ingest record when the ZIP is ambiguous (multiple DV-looking or SV-looking entries) and you want to pin which copy to use. Omit it when the ZIP is correctly formatted.

If you already uploaded the ZIP first by mistake and Batch failed because there was no ingest record, do not re-upload the ZIP. Use the recovery flow below.

#### Recover from NO_INGEST_RECORD

```bash
python etl/ingestion/tools/manual_ingest.py ingest-record \
    --short-code s0030 \
    --dv-basename s0030_dcradjhist_2020lu_noflowreqt_dv_20260126v02.dss \
    --sv-basename coeqwal_s9999_sv_v0.1.4.dss \
    --compute-hashes \
    --retrigger-batch
```

Locates the existing ZIP in `scenario/<id>/run/`, streams it to compute SHA-256 for the chosen DV and SV entries (and for the ZIP itself), PUTs `ingest_record.json` at `scenario/<id>/`, then submits a Batch job directly with the right environment variables. No re-upload required.

### Re-extraction

When something went wrong at extraction time and you want to retry without re-downloading from Drive. Two tools:

- **Default**: `tools/retrigger_extraction.sh` re-fires the full production Lambda + Batch path. Reach for this first.
- **Surgical**: `tools/reextract_all_scenarios.py` submits to Batch directly, bypassing the Lambda. Use when you need an override knob (`--validate`, `--memory`/`--vcpus`, `--sv-only`/`--dv-only`).

#### Re-trigger one scenario through the production Lambda path

```bash
bash etl/ingestion/tools/retrigger_extraction.sh --go s0020
```

Copies the ZIP from `scenario/<id>/run/` back to `ready/`. The S3 PUT fires the Lambda, which dispatches Batch through the same path as a fresh upload.

#### Re-extract one or more scenarios with overrides

`tools/reextract_all_scenarios.py` submits Batch jobs directly against ZIPs already in `s3://coeqwal-model-run/scenario/<id>/run/`. Bypasses the Lambda. Use when the container code changed, when a Batch job ran out of memory and needs a larger allocation, or when you only need one of the two CSV sides.

```bash
# Plan only
python etl/ingestion/tools/reextract_all_scenarios.py --dry-run

# Re-extract everything
python etl/ingestion/tools/reextract_all_scenarios.py

# Re-extract specific scenarios
python etl/ingestion/tools/reextract_all_scenarios.py --scenarios s0020,s0028

# Re-extract only the SV input (skip the CalSim DV output)
python etl/ingestion/tools/reextract_all_scenarios.py --sv-only

# Re-extract only the CalSim (DV) output (skip the SV input)
python etl/ingestion/tools/reextract_all_scenarios.py --dv-only

# Validate against reference CSVs in scenario/<id>/verify/
python etl/ingestion/tools/reextract_all_scenarios.py --validate

# Override per-job memory (default 8 GB, raise for DCP scenarios)
python etl/ingestion/tools/reextract_all_scenarios.py --scenarios s0065 --memory 32768
```

#### Confirm extraction outcomes across all scenarios

```bash
python etl/ingestion/tools/audit.py
```

`audit.md`'s "Active scenarios" table gives a one-word `status` per scenario (`OK` / `AWAITING_EXTRACTION` / `FAILED` / `PARTIAL` / `VALIDATION_FAILED` / `NO_INGEST_RECORD`), the last-extracted timestamp, and a one-line `notes` pointer. "What needs your attention" surfaces extraction failures (the container ran but did not produce every requested CSV) and validation failures (the extracted CSV diverged from the trend report), each with an actionable command and the per-row mismatch counts.

To inspect one scenario manually:

```bash
aws s3 cp s3://coeqwal-model-run/scenario/s0021/extract_record.json - | python -m json.tool
aws s3 cp s3://coeqwal-model-run/scenario/s0021/ingest_record.json - | python -m json.tool
aws s3 ls s3://coeqwal-model-run/scenario/s0021/validation/
```

## Experimental orchestrator

[`etl/run_full_pipeline.py`](run_full_pipeline.py) wires the scan, download, promote, Batch poll, statistics, and verification stages into a single subprocess driver with `--resume` support. Writes a consolidated report under `etl/ingestion/audit_reports/pipeline_runs/<UTC>/` (per-stage logs, `pipeline_state.json`, `pipeline_summary.md`). Its verify stage runs [`etl/statistics/verify_all_sections.py`](statistics/verify_all_sections.py) per scenario (the `--verify` preset runs that stage alone). API verification ([`verify_api.py`](statistics/verify_api.py)) is a release-gating step the developer runs separately. Tier verification ([`verify_tiers.py`](tier_data/scripts/verify_tiers.py)) belongs to the tier-data pipeline.

Five caveats:

1. **Untested end-to-end against AWS at handoff time:**
2. **`--batch-timeout` (default 7200s) is the total budget for *all* scenarios to clear Batch, not a per-job limit:** A large backfill can run past two hours, especially when the Batch queue runs only a few jobs at once. Raise it for big runs (e.g. `--batch-timeout 14400`). Any scenario still pending when it expires is marked `batch:timeout` in the summary. Ideally this should be taken over by EventBridge.
3. **Stats stage runs scenarios serially:** The orchestrator does not yet surface `run_all.py`'s `--workers` for multithreading. The direct path is 3-4x faster: `python etl/statistics/run_all.py --all-scenarios --workers 4`.
4. **No `audit.md` regen:** Run `python etl/ingestion/tools/audit.py` separately after the orchestrator finishes. Otherwise [`etl/ingestion/audit.md`](ingestion/audit.md) keeps showing the previous run.
5. **Stops at `verify`, does not activate:** Intentional human-review gate. After verification looks good, activate with `set_scenario_active.py --activate` (step 8 of the [pipeline runbook](#running-the-scenario-model-run-pipeline)).

The [Running the scenario model-run pipeline](#running-the-scenario-model-run-pipeline) command steps remain the recommended path until the orchestrator has been validated on a real run.

**Roadmap** Finishing this orchestrator and adding the auto-trigger is on the [ROADMAP](#roadmap) below.

## Troubleshooting

Most developer-facing failures surface in the audit report [`etl/ingestion/audit.md`](ingestion/audit.md) with an `error_code` and an action message. Scenarios flagged with `verification_status: unverified_*` still stage successfully (the trend report is optional). They appear in their own informational section of the audit report, separate from actionable failures.

The table below is a **starting point**, and it covers mainly the **ingestion** side of the pipeline: rclone/Drive access and `scan` -> `download` -> `promote`, plus one common Batch out-of-memory case.

| Code or symptom | Where it shows up | Fix |
|---|---|---|
| `rclone: command not found` | `download` startup | Install rclone and run the Cloud9 preflight: `bash scripts/setup_etl_cloud9.sh`. |
| `Failed to create file system: google drive: didn't find section in config file` | `download` startup | rclone config is missing. Copy `~/.config/rclone/rclone.conf` from a machine where the `gdrive` remote is authenticated, then re-check with `bash scripts/setup_etl_cloud9.sh --check`. |
| `rclone lsjson` returns empty | `scan` audit | Check the Drive folder URL in the working CSV. Try manually: `rclone lsjson --drive-root-folder-id=<ID> gdrive:`. |
| `401 Unauthorized` from rclone | `scan` or `download` | Token expired. Re-authenticate on a local machine: `rclone config reconnect gdrive:` and re-copy the config to Cloud9. |
| `MISSING_ZIP` | scan audit, ingest audit | `Model_Files/` in the Drive folder has no ZIP. Check the Drive folder URL. |
| `MULTIPLE_ZIPS_NO_PIN` | scan audit, ingest audit | `Model_Files/` has more than one ZIP. Set `pinned_model_run_zip` on the row in the working CSV. |
| `PINNED_ZIP_NOT_FOUND` | scan audit, ingest audit | `pinned_model_run_zip` does not match any file in `Model_Files/`. Fix the pin, or upload the named file. |
| `EXPECTED_DV_NOT_IN_ZIP` / `EXPECTED_SV_NOT_IN_ZIP` | ingest audit | The basename in `DV_Path` or `SV_Path` is not in the downloaded ZIP. Fix the row, or check that the right ZIP was selected. |
| `MULTI_MATCH_DV` / `MULTI_MATCH_SV` | ingest audit | The expected basename matches multiple non-excluded paths inside the ZIP. Move the duplicates into a subfolder named `_archive/`, `archive/`, `discard/`, `old/`, or `backup/` (which the classifier ignores), or rename them. |
| `verification_status: unverified_no_trend` | audit "Unverified scenarios" | No CSV in `Data_Extraction/Variables_From_trend_report_variables_v5/`. The scenario still stages. Upload a trend CSV to Drive and re-run `download --scenarios <id>` if you want verification. |
| `verification_status: unverified_multi_trend` | audit "Unverified scenarios" | More than one CSV in the trend folder, no pin. Set `pinned_trend_csv` and re-run `download --scenarios <id>` if you want verification. |
| `verification_status: unverified_pin_missing` | audit "Unverified scenarios" | `pinned_trend_csv` does not match any file in the trend folder. Fix the pin or upload the named file. |
| `convention_check.short_code_in_dv_basename: false` | audit (convention warnings) | Informational only, non-blocking. The DV basename does not contain the scenario's `short_code`. No action required unless the warning indicates a cross-paste error in the working CSV. |
| `No space left on device` | `download` mid-run | Reduce `--workers` to 1, or resize the EBS volume (see [Reclaiming disk space on the Cloud9 / EC2 instance](../README.md#reclaiming-disk-space-on-the-cloud9--ec2-instance)). |
| `extract_record.json` shows `status_summary.dv_csv_written: false`, `OutOfMemoryError` in Batch logs | post-extraction audit | Re-extract with `--memory 16384` (or 32768). Common with the `*_DWRadapt25_*_DCP` group, which produces ~326 MB CSVs vs ~200 MB for typical scenarios. |

---

## ROADMAP

- Currently we are using "Dino's spreadsheet" as a listing of the paths to the model run data. This process needs to be hardened.
- Tier teams need to be regularly reminded of the row/column format of the csv's they place in the dropbox.
- Tier teams have been asked by the project to submit continuous data. We'll need to adjust the database, etl, and api.
- During the third tier data run, after the third batch of scenario data was released (hydroclimate cc 95) salmon data appeared on a scale of 1-5. This needs to be resolved with the current Tier scale.
- (Related) We need to set a LICENSE on [COEQWAL-pydsstools](https://github.com/berkeley-gif/COEQWAL-pydsstools). I'm noticing that `pydsstools` is undergoing updates, so we may (or may not) decide to update our library.
- The community water systems team decided against including the "functional delivery levels" scenarios. We need to deactivate s0036, s0076, s0096, and s0122.
- **Tier locations live in the database, sourced from tier-team staging CSVs:** The `tier_location` table is a narrow catalog (`tier_short_code`, `location_type`, `location_id`, `display_order`, `is_active`). The staging CSVs the tier teams drop into `etl/tier_data/staging/` are the source of truth for membership. When a tier team sends a new or renamed column, run [`etl/tier_data/scripts/diff_tier_locations.py`](tier_data/scripts/diff_tier_locations.py) to see the gaps and [`etl/tier_data/scripts/sync_tier_locations_from_staging.py`](tier_data/scripts/sync_tier_locations_from_staging.py) to reconcile. Display names and geometry are resolved at query time by joining `location_id` to the entity tables in the registry at [`etl/common/tier_location_entities.py`](common/tier_location_entities.py). See [`etl/tier_data/README.md`](tier_data/README.md#updating-tier-locations-when-a-tier-team-sends-new-data) for the full workflow and [`etl/tier_data/scripts/audit_tier_location_geometry.py`](tier_data/scripts/audit_tier_location_geometry.py) for the geometry coverage scorecard.
- **Statistics location lists and calculations need hardening and verification:** Every statistics module computes two things that are not yet verified and should not be assumed final: (1) the **list of CalSim locations** to include, and (2) the **calculation** applied at each location. The pipeline runs end-to-end across nine modules, but the inputs were assembled with a focus on a functioning pipeline, so that was the priority over the specifics. Ideally every location list is drawn from the database. For now only one is, as a test of that path: `du_urban` reads its `du_id` set and per-DU CalSim variable mappings from the `du_urban_variable` and `du_urban_delivery_arc` tables ([`etl/statistics/du_urban/calculate_du_statistics_v2.py`](statistics/du_urban/calculate_du_statistics_v2.py), with a `--mock-mappings` fallback). The rest draw their lists from the other methods currently available:
  - **Seed-table CSVs** under [`database/seed_tables/04_calsim_data/`](../database/seed_tables/04_calsim_data/): `reservoirs` (`reservoir_entity.csv`), `env_flows` (`channel_entity.csv`), `mi` (`mi_contractor.csv`), `ag` (`du_agriculture_entity.csv`), `refuge` (`du_refuge_entity.csv`).
  - **In-code constants** with no external source: `delta` (variable-name constants for Net Delta Outflow, X2, salinity stations, Banks / Tracy EC) and `cws_aggregate` (SWP / CVP / north / south rollup definitions).
  - Some variables are hard-coded (e.g. `CAPACITY_OVERRIDES`, the MWD Table A contract, `SACRAMENTO_WBAS`, `GW_ONLY_DU_IDS`, `REFUGE_DU_IDS`, `AW_/DN_/GP_` conventions). Next steps: confirm each module's location membership and metric definitions with the research team, migrate the remaining location lists into the database on the pattern `du_urban` already demonstrates. Proving that the membership and formulas are correct is tracked separately in the next item. This is a substantial job and part of the overall [outcomes pipeline](https://docs.google.com/spreadsheets/d/1xcQIR_J96-cs7BuCrXjznwkinLgxl-Pf9tA3mJ2GiyA)
- **Community water system locations need attention and refinement** The list of community water system locations has been developing and changing. Speak with Kristin Dobbins and the research team about the goals for community water system locations, including export areas and systems served by demand unit.
- **Pipeline scenario-selection flags are inconsistent and could be rectified:** The same "which scenarios" is flagged differently across the pipeline's functions, which makes the runbook/pipeline harder to run. Settle on one vocabulary and roll it through the per-module scripts and `run_all.py`:
  - `--etl-scenarios` - the subset of scenarios to process in this ETL run (any subset you choose, for example one hydroclimate group). This is the `ETL_SCENARIOS` set in [`etl/common/etl_scenarios.py`](common/etl_scenarios.py), which `main` currently exposes under the misleading name `--all-scenarios`. Rename the flag so the name matches the meaning: a chosen run set, not "everything."
  - `--all-scenarios` - reserve this name for literally every scenario.
  - The three tools that operate on the active scenario set - `verify_api.py`, `verify_tiers.py`, and `load_all_tier_results.py` - each use a different flag and default for it, so give them one consistent flag and default a developer can predict (keep `--scenarios-override` as the explicit escape hatch).
- **Once scenario and tier metadata and descriptions settle, incorporate them as database data:** Descriptive text is intentionally left unfinished in the database, because it often isn't agreed upon when a scenario's identity row is created. Today the `scenario` row carries only identity, while the descriptive metadata lives in `scenario_hydroclimate_sibling` (name, short / long description, shared across a strategy's hydroclimate variants) and the `theme_scenario_link` / `scenario_tag_link` / `scenario_key_assumption_link` / `scenario_key_operation_link` tables, and some display extras (icons, short labels, per-scenario operations / assumptions) are still authored in the website rather than the database. Similarly, Tier labels and descriptions should eventually live in `tier_definition`. Once the final wording and classifications are agreed with the research and tier teams, load them as data into these tables and migrate the website-authored extras to the backend, so the API serves them from a single source of truth.
- Finish the pipeline orchestrator and add an EventBridge `Batch` to `run_all` trigger (so statistics fire automatically on extraction completion) is tracked as thread A8 in [`TEAM_RUNBOOK.md`](../TEAM_RUNBOOK.md#a8-automate-the-etl-pipeline-end-to-end-orchestrator--batch--run_all-trigger).
