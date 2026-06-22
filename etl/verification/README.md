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

## Receipts: the paper trail

Alongside the per-step checks above, the pipeline leaves a deliberate paper trail at every stage. Tags: `[S3]` lives in S3, `[local]` is on disk but gitignored, `[tracked]` is committed to git.

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
                           (every Batch run that validates,
                            header-only on pass, rows on fail)

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

## After a run: what to read

You do not need to read every verification file after every run. Each kind of run feedback follows the same shape: **console first, then the scoreboard or digest if the console scrolled, then forensic detail only if something was flagged.** Stop at the first level that gives a clean answer.

| Run | After a run, what do I read? |
|---|---|
| **`gdrive_bulk_download.py scan`**<br>(preflight against Google Drive, no S3 writes) | **Console:** prints a `SCAN AUDIT SUMMARY` block at the end of the run. Three parts, in this order:<br>- A totals header counting `Total scenarios`, `OK` (clean count), `Missing files`, `Multiple` (indicates more than one scenario version in the drive. Need to pin correct version in `model_run_file_source_working.csv`), `Folder mismatches`, `No drive access`, and `Local-only entries` (if that option is selected at run time).<br>- A per-scenario table, one row per scenario, with columns `Scenario` (the `short_code`), `Via` (how the scan reached Drive: `id` if `ModelFilesLink` resolved to a folder ID, `path` if it fell back to walking the Shared Drive by folder name, `none` if neither worked), `Zips` (count), `CSVs` (count, refers to Trend Report CSVs), `Match` (folder-name convention check: `OK`, `MISMATCH`, or `NO_DV_PATH` when the working CSV row has no `DV_Path` to compare against), `Status` (pipe-delimited failure codes, or `OK`).<br>- A `SCENARIOS REQUIRING ATTENTION` block. Present only when at least one scenario is non-OK and non-LOCAL_ONLY. Each entry carries the scenario id, status code, ZIP name, trend csv name, and folder name details for mismatches.<br><br>Clean-run signal: `OK (clean): N` equals `Total scenarios` and no `SCENARIOS REQUIRING ATTENTION` block follows.<br><br>**Forensic:** the run also writes `etl/ingestion/audit_reports/ingest_state.json` (`scan` block). Replay the saved per-scenario table later (e.g. when the terminal has scrolled, or to share with a teammate) with `python etl/ingestion/tools/show_last_run.py --stage scan`. It reads from `ingest_state.json` on disk and does not call Drive or S3. Scan never touches S3 and never updates `audit.md`.<br><br>See [Scan (Google Drive inventory)](#scan-google-drive-inventory) above for what each status counter means and where to set `pinned_model_run_zip` / `pinned_trend_csv` when "Multiple (need pin)" fires. |
| **`gdrive_bulk_download.py download`**<br>(Drive -> S3 staging, with validation) | **Console:**<br>- Ends with a `DOWNLOAD & VALIDATION SUMMARY` block with totals (`Total scenarios`, `OK`, `Skipped (review)`). A clean run reads `OK: N`, `Skipped (review): 0`, with no follow-up block. If `Skipped (review)` is non-zero, a `SCENARIOS REQUIRING REVIEW` block lists each flagged scenario with its `error_code` and `error_message`. A row is skipped (not staged to S3, not promoted) for one of: no Drive access, missing or extra ZIPs / trend CSVs, a pin pointing at a filename Drive no longer has, a corrupt ZIP, or the ZIP not containing the SV / DV basenames the working CSV declared. The full code catalog and the fix for each is in [Download (Drive to S3 staging)](#download-drive-to-s3-staging) above.<br>- Then `audit.py` auto-runs (unless `--skip-audit` is passed) and prints three lines: `Audit written to etl/ingestion/audit.md. Review and commit it manually when ready.`, then `Summary: N active scenarios in S3, M need developer action (extraction failures: ..., validation failures: ..., convention warnings: ...)`, then `Validation: K passed, F failed, S skipped, W awaiting extraction.` The audit at this point reflects ingest-side state only, because Batch has not yet run for any of the newly-staged scenarios. On a clean run, `M` is zero, all three parenthesized counts are zero, and `W` equals the number of scenarios just staged (they all sit at "awaiting extraction" until Batch finishes).<br><br>**Digest:** [`etl/ingestion/audit.md`](../ingestion/audit.md). `audit.py` rewrites the entire file on every call (walks all `scenario/` prefixes in S3). The download command auto-calls it once at the end of the run, so this snapshot reflects ingest-side state only. The "Did Batch finish extracting all the scenarios?" row below picks the file up again once extraction settles. |
| **`gdrive_bulk_download.py promote`**<br>(staging -> ready, fires the Batch Lambda to do the dss -> csv extraction) | **Console:** both a summary and per-object lines.<br>- Pre-flight: `About to promote N scenario(s) from staging/scenario_data/ to ready/.`, then `Upload order per scenario: ingest_record.json -> trend CSV -> ZIP last.`, then a per-scenario plan line listing the three files in upload order.<br>- Per-object copy lines `Copying s3://.../staging/... -> s3://.../ready/...` for every file (three per scenario), plus one `[sXXXX] Promoted to ready/` line per scenario.<br>- Closes with `Done. Promoted N scenario(s) to ready/.` followed by `The Lambda will trigger on each ZIP upload.`<br><br>When `Done.` prints, every S3 PUT succeeded. The per-object lines are there so you can grep when something looks wrong, the summary is there for when the console scrolled.<br><br>**What happens next.** The ZIP PUT fires the Lambda, which submits one Batch job per scenario. Extraction runs asynchronously in AWS, roughly 20 minutes per scenario, multiple in parallel. The next row's `audit.py` is how you confirm each Batch job actually finished cleanly.<br><br>**Status hint while Batch is in flight.** `python etl/status.py` reports active and recently-terminated Batch job counts straight from the queue, with no S3 walk and no `audit.md` rewrite. Use it for a quick "is anything still running?" check between promote and the next audit. It does not tell you whether finished jobs succeeded. Only `audit.py` does that. |
| **Did Batch finish extracting all the scenarios?**<br>`python etl/ingestion/tools/audit.py` | Batch runs asynchronously in AWS, roughly 20 minutes per scenario, multiple scenarios in parallel (Fargate Spot spins containers up on demand, capped by the compute environment's `maxvCpus` setting. Each container uses 2 vCPU, so the queue can run up to `maxvCpus / 2` scenarios concurrently. If `maxvCpus` is 64, that is 32 scenarios in parallel. If it is 16, that is 8 in parallel).<br><br>When you are ready to check completion, run `python etl/ingestion/tools/audit.py`. It walks S3, rewrites `etl/ingestion/audit.md`, and prints three console lines: `Audit written to etl/ingestion/audit.md. Review and commit it manually when ready.`, then `Summary: N active scenarios in S3, M need developer action (extraction failures: X, validation failures: Y, convention warnings: Z)` (convention warnings surfaces scenario file naming differences), then `Validation: K passed, F failed, S skipped, W awaiting extraction.`.<br><br>If `M` is zero, every promoted scenario landed cleanly. If `M` is non-zero, the named scenarios are in [`etl/ingestion/audit.md`](../ingestion/audit.md) under "What needs your attention".<br><br>**Per-scenario validation outcomes** for every active scenario in S3 (not just the flagged ones) appear in `audit.md`'s `## Active scenarios` table under the `status` column. Values: `OK` (extraction and validation both clean), `VALIDATION_FAILED` (extraction OK, trend-report check found mismatches), `FAILED` (Batch did not produce the expected CSV), `PARTIAL` (one of SV / DV missing), `AWAITING_EXTRACTION` (Batch has not written `extract_record.json` yet), or `NO_INGEST_RECORD` (ZIP in S3 but no record alongside).<br><br>**Forensic:** when validation flags a scenario, **open `s3://<bucket>/scenario/<id>/validation/<id>_validation_mismatches.csv`**. This is the per-row diff between the extracted CSV and the WAM team's trend report. <br><br> Other forensic artifacts:<br>- `s3://<bucket>/scenario/<id>/extract_record.json` (the full Batch outcome record).<br>- The `/aws/batch/job/...` CloudWatch log stream, named by the job id printed in `audit.md`.<br><br>See [Extraction (DSS to CSV)](#extraction-dss-to-csv) above for per-job tuning, parallelism details, and the audit.md section-by-section guide. |
| **Statistics ETL**<br>`etl/statistics/run_all.py` | **Console:** `ETL PROCESSING SCORECARD` with per-scenario PASS / FAIL markers, a `SUMMARY` block with task totals, and a `FAILURES (need attention)` block if any row failed.<br><br>**Digest:** `etl/statistics/audit_reports/stats_audit_<ts>.csv`. Columns: `module, scenario, success, wall_time_sec, rows_written, error`. The `error` column carries the failure reason. This file is the scoreboard and the forensic detail in one. Statistics has no separate markdown digest yet (see [Roadmap](#roadmap)). |
| **Tier data load**<br>`etl/tier_data/scripts/load_all_tier_results.py` | **Console:** Per-tier row counts (e.g. `CWS_DEL: N location records, M scenario aggregates`), then `Manifest written: etl/tier_data/staging/tier_upload_manifest.csv` with totals. The manifest is regenerated on every normal run.<br><br>**Digest:** `etl/tier_data/staging/tier_upload_manifest.csv` (per-tier totals). To confirm the DB matches, re-run `load_all_tier_results.py --verify`. It compares. It does not regenerate the manifest. |
| **Model-run verification**<br>`verify_all_sections.py`, `verify_api.py` | **Why / when:** Independent spot check that the statistics ETL landed numbers correctly and that the public API is serving them faithfully. `verify_all_sections.py` re-reads the reference DV / SV CSVs that `run_all.py` consumed, recomputes the headline statistics in plain pandas, and compares against the database. `verify_api.py` hits `api.coeqwal.org` over HTTP and compares each response against a direct database query. Run after `run_all.py` finishes (at least on a sample of scenarios), after deploying a change to the statistics calculation code, after a stats-table migration, or when a stakeholder reports numbers that look off. This is a developer diagnostic, not an automated pipeline step (experimental, see [Statistics load (CSV to database)](#statistics-load-csv-to-database) above).<br><br>**Console:** Per-scenario `VERIFICATION SUMMARY` block with `PASS` / `FAIL` / `Skipped` / `No DB data` counts and a `FAILED CHECKS` list when any check failed.<br><br>**Digest:** `audits/verification_reports/<scenario>_layer2.json` (from `verify_all_sections.py`, DB vs reference CSVs) and `<scenario>_layer3.json` (from `verify_api.py`, API vs DB). |
| **Tier verification**<br>`etl/tier_data/scripts/verify_tiers.py` | **Why / when:** Part of the tier pipeline, not the model-run pipeline. Confirms that `load_all_tier_results.py` transcribed the team-delivered staging CSVs into the `tier_result` and `tier_location_result` tables faithfully, for every active scenario and every tier code. Comparison is row-for-row against `etl/tier_data/staging/`.<br><br>**Console:** One row per tier code (e.g. `PASS WRC_SALMON_AB    (N checks, 0 mismatches)` or `FAIL ... (N checks, M issues: ...)`), then an `Overall: X/Y tiers PASS` (or `Overall: X/Y tiers PASS, Z FAIL`) line and a `Detail: <json_path>` pointer.<br><br>**Digest:** `audits/verification_reports/tiers_<ts>.json`. See [`etl/tier_data/README.md`](../tier_data/README.md). |
| **Status check** ("when did X last run?")<br>`python etl/status.py` | **Console:** Freshness across six sections: ingestion, batch, statistics, tiers, verification, and connectivity (RDS, AWS, S3, rclone). |

**Rule of thumb.** Trust the headline at each level (console, then `audit.md` or JSON report, then `stats_audit_<ts>.csv` or `report.md`). The forensic artifacts in the diagram exist for triage. Read them only when the headline says to.

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
