# ETL verifications and audits

## Did each step work?

### Scan (Google Drive inventory)

***Are the scenarios' Drive folders well-formed?*** Run [`gdrive_bulk_download.py scan`](../ingestion/gdrive_bulk_download.py) to walk each scenario's Drive folder. For each scenario it confirms the folder is reachable (by Drive folder ID, falling back to the folder name), expects exactly one model-run ZIP in `Model_Files/` and exactly one trend-report CSV (or the pinned file when one is set), and checks that the Drive folder name matches the `DV_Path` root.

```bash
python etl/ingestion/gdrive_bulk_download.py scan
```

**It worked** when the closing `SCAN AUDIT SUMMARY` shows `OK (clean)` for the scenarios you expect: those folders are reachable, have exactly one ZIP and one trend CSV, and the folder name matches.

The summary prints to the console, but the same per-scenario rows are also saved locally to `etl/ingestion/audit_reports/ingest_state.json` (the `scan` block -- you can override the directory with `--output-dir`) on every run. If the console has scrolled away, reprint the last scan with `python etl/ingestion/tools/show_last_run.py --stage scan` instead of running the scan again (a scan re-walks every Drive folder, which is slow and hits the Drive API).

**It didn't** for any scenario counted under `Missing files`, `Multiple (need pin)`, `Folder mismatches`, or `No drive access`. `Multiple (need pin)` means the Drive folder has more than one ZIP or more than one trend CSV, so the scan can't tell which to use: resolve it by setting `pinned_model_run_zip` and/or `pinned_trend_csv` with the exact filename on that scenario's row in the working CSV, [`etl/ingestion/scenario_listing/model_run_file_source_working.csv`](../ingestion/scenario_listing/model_run_file_source_working.csv). The per-scenario table and the `SCENARIOS REQUIRING ATTENTION` block name each affected scenario and its fix.

### Download (Drive to S3 staging)

***Did the bytes arrive in S3 intact and correctly labeled?*** Run [`gdrive_bulk_download.py download`](../ingestion/gdrive_bulk_download.py) to pull each ZIP, confirm it isn't corrupt, classify the SV and DV DSS files, SHA-256 hash every file, check the filename convention, and stage the result to `s3://coeqwal-model-run/staging/scenario_data/<id>/` alongside each scenario's `ingest_record.json`. That sidecar is the per-scenario provenance manifest `download` builds and ships to S3 with the ZIP: the SHA-256 hashes, the SV/DV classification, the filename-convention result, and the source spreadsheet row it came from.

```bash
python etl/ingestion/gdrive_bulk_download.py download --dry-run   # list what would download/stage
python etl/ingestion/gdrive_bulk_download.py download
```

**It worked** when the closing `DOWNLOAD & VALIDATION SUMMARY` in the console reports `OK: N` and `Skipped (review): 0`, and each scenario's `validation_status` is `OK`. The run auto-renders the ingestion audit at the end.

The summary prints to the console, but the same per-scenario rows are also saved to `etl/ingestion/audit_reports/ingest_state.json` (the `download` block). You can reprint the console display any time with `python etl/ingestion/tools/show_last_run.py --stage download` instead of re-downloading. For per-scenario detail, open that scenario's `ingest_record.json` sidecar at `s3://coeqwal-model-run/staging/scenario_data/<id>/`.

**It didn't** for any row under `Skipped (review)`. Each carries an `error_code` (`MISSING_ZIP`, `BAD_ZIP`, `NO_DSS_IN_ZIP`, `DOWNLOAD_FAILED`, ...) that points at the fix. Skipped rows do not stage and will not promote.

### Promote (staging to ready, triggers Batch)

***Did the validated bytes hand off to Batch?*** Run [`gdrive_bulk_download.py promote`](../ingestion/gdrive_bulk_download.py) to copy each staged scenario from `s3://coeqwal-model-run/staging/scenario_data/<id>/` to `s3://coeqwal-model-run/ready/<id>/`. The ZIP landing in `ready/` is what fires the Lambda that submits the Batch extraction job.

```bash
python etl/ingestion/gdrive_bulk_download.py promote --dry-run   # preview the list of what promote would copy
python etl/ingestion/gdrive_bulk_download.py promote
```

**It worked**

- **A job actually started:** `aws logs tail /aws/lambda/coeqwalEtlTrigger --follow` (look for `Submitted Batch job ...`), or `python etl/status.py` for active-job counts. This is the dispatcher Lambda's log, not the extraction itself.
- **The jobs finished:** `python etl/status.py` (the `batch` section) reports active and last-24h `SUCCEEDED` / `FAILED` counts from the `coeqwal-dss-queue` queue. The same data raw is `aws batch list-jobs --job-queue coeqwal-dss-queue --job-status RUNNING` (also `RUNNABLE`, `SUCCEEDED`, `FAILED`). For one job's exact status and stop time, `aws batch describe-jobs --jobs <job-id>` (the job id is in the Lambda log and `audit.md`). When nothing is left active, the jobs are done.
- **Extraction finished cleanly:** once the active count above is `0`, run `audit.py` at the [Extraction step](#extraction-dss-to-csv) (~5-30 min per scenario after promote). Wait for `0` first: an audit rendered while jobs are still in flight only counts the `extract_record.json` files written so far, so re-run it once the queue drains. This is the real answer to "did it work."

Batch runs in AWS, not your terminal, so its output goes to **CloudWatch Logs** in two separate groups. `/aws/lambda/coeqwalEtlTrigger` is the dispatcher Lambda (the `Submitted Batch job ...` line). The Batch container's own logs (extraction progress, the `validate_csvs.py` `PASSED` / `FAILED` lines, and any traceback) go to a separate group set by the Batch job definition in AWS, conventionally `/aws/batch/job/...`. Confirm the exact group in the AWS console. `promote` itself writes no summary file, and nothing auto-refreshes the audit when Batch finishes (unlike `scan` and `download`, Batch has no local Python wrapper to do that).

**It didn't** if nothing lands in `ready/` (the promote copy failed, so no Lambda fired) or no job shows up in the Lambda log / `status.py` (the PUT did not trigger a job). Once jobs do run, a scenario can still fail extraction or validation. `audit.py` names it under `## What needs your attention` in `audit.md` with the Batch job id, the `aws s3 cp` command for its `<id>_validation_mismatches.csv`, and the `bash etl/ingestion/tools/retrigger_extraction.sh --go <id>` command to re-run it. For a job that errored outright, read the traceback in the Batch container's CloudWatch group (conventionally `/aws/batch/job/...`; confirm in the console).

### Extraction (DSS to CSV)

***Did the Batch conversion from model run HEC-DSS to CSV preserve the data?*** In each scenario's Batch job, the container runs [`validate_csvs.py`](../batch-container/python-code/validate_csvs.py) to compare each extracted CSV against the modeling team's trend-report reference CSV, column by column, value by value with tolerances. Automatic, runs on every ingest, there is no command to run by hand.

The Batch job records a `passed` or `failed` result into the scenario's `extract_record.json` and uploads a per-row `<id>_validation_mismatches.csv` for detailed mismatch inspection. A mismatch does not fail the job or stop the downstream statistics load.

These files land in each
```
s3://coeqwal-model-run/scenario/<id>/
```

To check, run the ingestion audit after ingestion. `audit.py` aggregates every scenario's `ingest_record.json` and `extract_record.json` from S3 into one tracked report, `etl/ingestion/audit.md` (Run summary, What needs your attention, Active scenarios), so you read one file instead of opening per-scenario records:

```bash
python etl/ingestion/tools/audit.py
python etl/ingestion/tools/audit.py --dry-run   # print the report to stdout instead of writing audit.md
```

then open `etl/ingestion/audit.md`.

**It worked** when `audit.md`'s `## What needs your attention` section is empty and `## Run summary` shows `Validation failures: 0`: every scenario's `extract_record.json` recorded `result: passed`.

**It didn't** when `audit.md` lists scenarios under `Validation failed` (each with an `aws s3 cp ... <id>_validation_mismatches.csv` line to pull the per-row diffs) or under `Batch extraction failed or partial`. A validation mismatch is advisory: it does not fail the Batch job or block the statistics load, so it surfaces only here, never as a pipeline error.

**Status** Today `audit.md` names the scenarios that failed, and the ones skipped because they had no trend report or a bad one. Verification can be skipped for other reasons too, but the report shows only a count for those, so a developer can't tell which scenarios or why. audit.md also auto-renders only at the end of the download step, before Batch extraction runs, so it has to be re-run manually afterward to capture the verification results. See the [ETL verification developer experience](#etl-verification-developer-experience) roadmap items for naming all skipped scenarios and for a scheduled audit run.

### Statistics load (CSV to database)

***Did the statistics load into Postgres?*** [`run_all.py`](../statistics/run_all.py) reads each scenario's extracted CSVs and writes the per-module statistics (reservoirs, DU urban, M&I, CWS, AG, refuge, env flows, delta, and optional sensitivity) into the database. This is the load step itself.

```bash
python etl/statistics/run_all.py --scenario s0020
python etl/statistics/run_all.py --scenario s0020 --dry-run        # parse and compute, write nothing
python etl/statistics/run_all.py --scenario s0020 --only reservoirs,du_urban
python etl/statistics/run_all.py --all-scenarios --batch-size 10 --continue-on-error
```

**It worked** when the in-console closing `ETL PROCESSING SCORECARD` shows ✅ across every module column (legend: ✅ success, ❌ failed, ⏭️ skipped, ⚪ not run) and the `SUMMARY` block reports no failures.

If the console scrolled away, the same result survives off-screen in three places. The scorecard is also persisted, row by row, to `etl/statistics/audit_reports/stats_audit_<ts>.csv` with an `error` column that reports the failure reason for any row that failed. For a quick "did the last run finish and when," `python etl/status.py` (the `statistics` section) reports the newest `stats_audit_<ts>.csv`, its modification time, and its row count, without re-running anything. And the load target itself is the database, so a direct query (or the [API check](#api-database-to-public-api) below) confirms the rows actually landed.

**It didn't** when any cell is ❌ or the run prints a `FAILURES (need attention)` block. Re-run the named scenario (optionally `--only <module>`) once the cause is fixed. On a big run, `--continue-on-error` lets an `--all-scenarios` batch finish past a single bad scenario so you can triage them together.

***Did the numbers land correctly, not just without error?*** A clean scorecard means the load ran, not that the values match the source. An independent value-level check exists, [`verify_all_sections.py`](../statistics/verify_all_sections.py), which recomputes the headline statistics from the source DV / SV CSVs and compares them against what the ETL wrote to the database. **It is still in progress and not yet part of the pipeline.** It reads only local CSV reference files (a hand-staged DV / SV pair in `audits/notebooks_reference/`, or `--ref-dir`) for development and has no S3 access yet. Wiring it to the bucket is [Roadmap item 1: Point the statistics verifier at S3](#roadmap).

### API (database to public API)

***Does the public API return those same numbers?*** [`verify_api.py`](../statistics/verify_api.py) hits `api.coeqwal.org` over HTTP and compares the API responses against direct database queries.

```bash
python etl/statistics/verify_api.py --scenario s0020
python etl/statistics/verify_api.py --scenario s0020 --api-url http://localhost:8000
python etl/statistics/verify_api.py --all-scenarios
python etl/statistics/verify_api.py --scenario s0020 --with-tiers   # also cross-check tier results
```

**It worked** when the `API VERIFICATION SUMMARY` prints `FAIL: 0` and `Mismatch: 0`, the aggregated view ends `Overall: N/N sections PASS`, and the script exits `0`. It writes a per-scenario JSON report under `audits/verification_reports/`.

**It didn't** when `FAIL` or `Mismatch` is non-zero, or a section line reads `FAIL <section> (... mismatches: ...)`: the script exits `1` and the `Detail:` line points at the JSON report with the offending values. `--with-tiers` adds the tier endpoints to the run.

### Tier data load (CSVs to database)

***Does the tier database match what the team handed us?*** [`verify_tiers.py`](../tier_data/scripts/verify_tiers.py) compares `tier_result` rows and API tier responses against the staging CSVs the team delivered. This is the tier-data pipeline, a separate ingest from the model-run scenarios.

```bash
python etl/tier_data/scripts/verify_tiers.py
python etl/tier_data/scripts/verify_tiers.py --scenario s0020
python etl/tier_data/scripts/verify_tiers.py --tier CWS_DEL
```

**It worked** when every tier row prints `PASS <tier_code>`, the run ends `Overall: N/N tiers PASS`, and the script exits `0`. It writes a JSON report under `audits/verification_reports/`.

**It didn't** when any row reads `FAIL <tier_code> (... issues: ...)`: the run ends `... FAIL`, the script exits `1`, and the `Detail:` line points at the JSON report. Requires `DATABASE_URL` and API access; without `DATABASE_URL` the tier-location coverage scan is skipped with a warning.

---

## Monthly database audit

The schema + content snapshot, captured periodically as a checkpoint against the live database.

| | |
|---|---|
| **Script** | [`database/audit/run_monthly_audit.py`](../../database/audit/run_monthly_audit.py) |
| **When** | Manual. Run before major data changes. |
| **Output** | `audits/monthly_YYYYMMDD_HHMMSS/` |

Contents:

- `report.md` - row counts, ERD diff, index sizes, audit-field checks
- `tables_summary.csv` - per-table inventory
- `schema_snapshot.json` - full schema
- `layer_exports/` - full CSV export of reference / entity / lookup tables
- `results_samples/` - head / tail samples of statistics result tables

```bash
cd ~/environment/coeqwal-backend
python database/audit/run_monthly_audit.py
```

**Use for:** grounding documentation, cross-checking seed CSVs against live RDS, ERD verification, before-and-after for any seed reload or schema change.

---

## Roadmap

- **Point the statistics verifier at S3.** [`verify_all_sections.py`](../statistics/verify_all_sections.py) is still under development and reads only local DV / SV CSV reference files (default `audits/notebooks_reference/`, or `--ref-dir`) and has no S3 access yet to compare against the sv and dv files in the bucket. It needs the same S3 read path the production calculators already use, `s3://coeqwal-model-run/scenario/<id>/csv/<id>_coeqwal_calsim_output.csv` (DV) and `..._sv_input.csv` (SV).
- **Mirror the ingest digest to statistics.** `etl/statistics/tools/audit.py` walks recent `stats_audit_*.csv` and renders a tracked `etl/statistics/audit.md`. Closes the asymmetry where ingestion has a tracked `audit.md` digest and statistics has none. One afternoon, no new infrastructure.

### ETL verification developer experience

- **Give every scenario a verification status in the audit.** What a developer needs from `etl/ingestion/audit.md` is a per-scenario answer in one of three buckets: verification ran and passed, ran and failed (which ones), or did not run (which ones). Today the report answers only the middle bucket cleanly. Failed scenarios are named in "What needs your attention," but passed and not-run scenarios both render as `OK` in the active-scenarios table, because `_scenario_status` only special-cases `validation.result == "failed"`. The skipped *count* in the validation breakdown says how many did not run but never which, and the "Unverified scenarios" section names only the missing-trend-report subset, which is a different set than the count. Split `OK` into `VERIFIED` and `NOT VERIFIED`, driven by `validation.result` (already present in each `extract_record.json`), and list the not-verified scenarios by name so a developer knows which to inspect.
- **Scheduled audit run.** Run `audit.py` automatically after a batch run finishes, so `audit.md` is always current. Removes the "remember to run it" step with zero per-event noise. Timestamp the `audit.md` title so a reader can tell at a glance how fresh the digest is.
