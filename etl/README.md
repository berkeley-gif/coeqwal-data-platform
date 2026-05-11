# ETL (Extract, Transform, Load) Framework

Automated DSS file processing pipeline that:

- copies scenario model run files from Water Allocation Modeling Team Google Drive
- extracts CSV data from CalSim model runs and validates against reference data
- computes statistics and loads statistics into database.

---

## Output files (audits, generated SQL)

Every script that produces an artifact writes it into a module-local `output/`
directory. The whole set is gitignored via the umbrella pattern
`etl/**/output/` in `.gitignore`, so these files **never** belong in git or in
the repo root — they're regeneratable artifacts that live next to the script
that creates them.

| Stage | File | Purpose | Default location | Generator | Override |
|---|---|---|---|---|---|
| Pre-download (Drive scan) | `scan_audit.csv` | "Are all the expected ZIPs and trend CSVs actually present on Google Drive?" Should be all `OK` before downloading. | `etl/scripts/output/` | `gdrive_bulk_download.py scan` | `--output-dir` |
| Post-download | `audit_report.csv` | "Did each scenario download cleanly from Drive and stage to S3?" Per-scenario validation flags. Also uploaded to `s3://coeqwal-model-run/staging/audit_report.csv`. | `etl/scripts/output/` | `gdrive_bulk_download.py download` | `--output-dir` |
| Post-extraction | `extraction_audit.csv` | "After the EC2 extraction Lambda ran on staged ZIPs, did each scenario produce valid CSVs?" | `etl/scripts/output/` | `check_extraction_results.py` | `-o` / `--output` |
| Statistics ETL | `stats_audit_<ts>.csv` | "For my last big stats run, which `(scenario × module)` pairs succeeded and how long did each take?" One file per run, timestamped — multiple runs accumulate. | `etl/statistics/output/` | `run_all.py` | `--audit-dir` |
| Data-quality scan | `duplicate_scan_results.csv` (+ sibling `_units.csv`) | "Which CalSim variables show up twice with the same column name in the same scenario CSV?" Cross-scenario diagnostic. | `etl/statistics/output/` | `scan_dupes.py` | `-o` / `--output` |
| Tier loader | `all_tiers.sql` | The big idempotent UPSERT script that loads tier results into `tier_result` and `tier_location_result`. Fed to `psql -f`. Working artifact — once `psql` succeeds, the data is in the DB and the file is no longer needed. | `etl/tier_data/output/` | `load_all_tier_results.py` | `--output-sql` (bare filenames are auto-routed into `output/`; paths with `/` are respected) |

### Why these files aren't in git

They're all **generated** from inputs that already live in git or S3:
- `all_tiers.sql` is regenerated from staging CSVs in `etl/tier_data/staging/tier_results/` (which are tracked).
- The audit CSVs are regenerated from S3 + Google Drive + database state every time their scripts run.
- The stats audit is a per-run scorecard — committing one is meaningless because the next run produces a new one.

Tracking any of them would bloat history without adding any information that isn't already recoverable.

### Why they live on Cloud9, not on your laptop

The ETL pipeline runs on Cloud9 because that's where the credentials and access live: AWS SSO for S3, `rclone gdrive` for Google Drive, and `DATABASE_URL` pointing at the RDS instance. You don't run the pipeline on your laptop, so you don't need its outputs there. If you want to inspect a file, copy it over with `aws s3 cp …` or `scp`.

### Cleaning up legacy mess in `cwd`

Older versions of these scripts wrote into the current working directory. If you have leftovers in your Cloud9 home (or repo root), one-time cleanup:

```bash
# safe: empty typo files and the regeneratable tier SQL
rm -f etl/statistics/ORDER export etl/tier_data/all_tiers.sql

# safe: audit CSVs from older runs (they're scratch logs)
git clean -fd \
  audit_report.csv extraction_audit.csv scan_audit.csv \
  etl/statistics/duplicate_scan_results.csv \
  etl/statistics/stats_audit_*.csv
```

After your next `git pull`, future runs land under `etl/<module>/output/` instead of polluting `cwd`, and `git status` stays quiet.

---

## How to load new scenarios

This is the process for bringing new CalSim model runs from the COEQWAL Shared Drive into the data platform. Each scenario is a CalSim3 model run packaged as a ZIP file on Google Drive, with a companion Trend Report CSV for validation.

### 1. Update the model run file source CSV

Google Drive paths and file selections for every scenario are tracked in our CSV:

```
database/reference/model_run_file_source.csv
```

This file is maintained from the model run file lists, often with corrected folder IDs, verified paths, and pinned filenames.

| Column | Purpose | Example |
|--------|---------|---------|
| `short_code` | Scenario identifier | `s0070` |
| `drive_folder_id` | Google Drive folder ID (from folder URL) | `1AxM4DmuTuoX...` |
| `drive_folder_name` | Folder name on the Shared Drive | `s0070_DCRadjHist_cc50_2020LU_eflowsV1` |
| `pinned_model_run_zip` | Exact ZIP filename to download (blank = auto-select single file) | `s0023_..._v2_20260217.zip` |
| `pinned_trend_csv` | Exact trend report CSV to download (blank = auto-select) | `s0023_..._DV_v2_20260217.csv` |
| `download_status` | `ready`, `needs_review`, or `skip` | `ready` |
| `notes` | Any issues or disambiguation notes | |

**How to get the `drive_folder_id`:**
1. Navigate to the scenario's top-level folder on the Shared Drive (e.g., `s0070_DCRadjHist_cc50_2020LU_eflowsV1`)
2. Copy the URL from the browser address bar: `https://drive.google.com/drive/folders/1AxM4DmuTuoX...`
3. The folder ID is the part after `/folders/` (before any `?` query string)

**When to pin filenames:**
- If a Drive folder contains **multiple ZIPs** (old + new versions), set `pinned_model_run_zip` to the correct one. Use the version/date suffix from the modeling team's DV_Path to identify it.
- If there are **multiple trend CSVs**, set `pinned_trend_csv` similarly.
- If there's only **one file**, leave the column blank - the script auto-selects it.

**`download_status` values:**
- `ready` - folder ID verified, files confirmed by scan, ready to download
- `needs_review` - known issue (missing files, wrong folder ID, etc.)
- `skip` - intentionally excluded from download

### 2. Add scenario metadata to the database

Before loading data, each scenario needs a row in the `scenario` table. Write a migration SQL script (see `database/scripts/sql/52_add_s0070_s0090.sql` for an example) that:
- Inserts the scenario with `short_code`, `run_name`, `is_active`, `hydroclimate_id`, `hydroclimate_sibling`, `scenario_version_id`, `scenario_author_id`, `model_source_id`
- Disables the audit trigger, sets `created_by=2` and `updated_by=2` (developer attribution), then re-enables the trigger
- Run with `psql $SUPERUSER_URL -f database/scripts/sql/<migration>.sql` on Cloud9

If the scenario belongs to an existing sibling group (same operational configuration, different hydroclimate), set `hydroclimate_sibling` to the group's reference short code. If it's a new operational configuration, also add a row to `scenario_hydroclimate_sibling`.

### 3. Scan Google Drive

Before downloading, validate that all files are accessible:

```bash
python etl/scripts/gdrive_bulk_download.py scan \
  --listing-csv database/reference/model_run_file_source.csv \
  --workers 4 2>&1 | tee scan_$(date +%Y%m%d).log
```

By default, only scenarios with `download_status=ready` are scanned. Use `--include-all` to scan everything, or `--scenarios s0070 s0090` to scan specific ones.

The scan lists `Model_Files/` for ZIPs and `Data_Extraction/Variables_From_trend_report_variables_v5/` for trend report CSVs. It reports:
- How many ZIPs and trend CSVs exist per scenario
- Which file it would select (pinned filename if specified, otherwise most recent by date)
- `OK` = exactly one file found (or pinned file found among multiples)
- `ALERT_MULTIPLE_ZIP` / `ALERT_MULTIPLE_TREND` = multiple files, no pinned filename set - add one to the CSV
- `MISSING_ZIP` / `MISSING_TREND` = file not found on Drive
- `PINNED_ZIP_NOT_FOUND` / `PINNED_TREND_NOT_FOUND` = pinned filename doesn't match any file on Drive

Review `etl/scripts/output/scan_audit.csv` before proceeding (override location with `--output-dir`). All scenarios should show `OK` (except known missing trend reports like s0011).

### 4. Download and stage to S3

```bash
# Dry run first (lists files, validates ZIPs, no S3 upload):
python etl/scripts/gdrive_bulk_download.py download \
  --listing-csv database/reference/model_run_file_source.csv \
  --s3-bucket coeqwal-model-run \
  --dry-run \
  --workers 4 2>&1 | tee download_dryrun_$(date +%Y%m%d).log

# Real download:
python etl/scripts/gdrive_bulk_download.py download \
  --listing-csv database/reference/model_run_file_source.csv \
  --s3-bucket coeqwal-model-run \
  --workers 4 2>&1 | tee download_$(date +%Y%m%d).log
```

For each scenario, the download command:
1. Lists `Model_Files/` on Drive and selects the ZIP (pinned or auto-selected)
2. Lists `Data_Extraction/.../` and selects the trend report CSV
3. Downloads the ZIP, validates it (classifies DSS files inside)
4. Uploads both the ZIP and trend CSV to `s3://coeqwal-model-run/staging/<shortcode>/`

Use `--scenarios s0070 s0090` to download specific scenarios only. Use `--include-all` to include non-ready scenarios.

### 5. Promote to trigger extraction

```bash
python etl/scripts/gdrive_bulk_download.py promote \
  --s3-bucket coeqwal-model-run
```

Copies files from `staging/` to `ready/`. The Lambda trigger detects the ZIP upload and submits an AWS Batch extraction job.

### 6. Monitor extraction and handle failures

After promoting, Lambda triggers Batch extraction jobs automatically. Monitor progress:

```bash
# Check how many jobs are running/pending/done
aws batch list-jobs --job-queue coeqwal-dss-queue --job-status RUNNING --query 'length(jobSummaryList)'
aws batch list-jobs --job-queue coeqwal-dss-queue --job-status SUCCEEDED --query 'length(jobSummaryList)'
aws batch list-jobs --job-queue coeqwal-dss-queue --job-status FAILED --query 'length(jobSummaryList)'

# Check extraction results across all scenarios
python etl/scripts/check_extraction_results.py --bucket coeqwal-model-run
```

If any jobs fail with `OutOfMemoryError`, re-extract them with more memory:

```bash
# Check what failed
python etl/scripts/check_extraction_results.py --bucket coeqwal-model-run --scenarios s0065

# Re-extract with 16 GB (default is 8 GB)
python etl/scripts/reextract_all_scenarios.py --scenarios s0065,s0085,s0105 --memory 16384
```

Known large scenarios that need 16 GB: the DWRadapt25 group (DCP operation) produces ~326 MB CalSim output CSVs vs ~200 MB for typical scenarios. These are the `*_DWRadapt25_*_DCP` ZIPs. If your new batch includes DCP scenarios, expect to re-extract those with `--memory 16384`.

### 7. Run statistics ETL and verify

```bash
cd etl/statistics
python run_all.py --scenario s0070
python verify_all_sections.py --scenario s0070
python verify_api.py --scenario s0070
```

### File layout on Google Drive

Each scenario folder on the COEQWAL Shared Drive follows this structure:

```
<scenario_folder_name>/
├── Model_Files/
│   ├── <scenario>.zip          # the model run ZIP (contains DSS files)
│   └── DSS/
│       ├── output/
│       │   └── <scenario>_DV_<version>.dss
│       └── input/
│           └── coeqwal_s9999_SV_<version>.dss
└── Data_Extraction/
    └── Variables_From_trend_report_variables_v5/
        └── <scenario>_trend_report_<version>.csv   # validation reference
```

The ZIP in `Model_Files/` is what gets downloaded. The trend report CSV is used for post-extraction validation. Both are authored by the modeling team (Dino Bellugi).

### What is automated vs. manual

The pipeline has automated and manual stages. Understanding the boundary is important:

```
                           AUTOMATED                                    MANUAL
                   +---------------------------+          +-------------------------------+
Google Drive -->   |  S3 ready/ --> Lambda     |          |  Statistics ETL               |
(gdrive_bulk_      |  --> Batch (DSS->CSV)     |          |  (run_all.py)                 |
 download.py)      |  --> S3 scenario/csv/     |          |  --> PostgreSQL tables        |
   [manual]        |  --> S3 validation/       |          |  Verification                 |
                   |  --> S3 manifest.json     |          |  (verify_all_sections.py,     |
                   +---------------------------+          |   verify_api.py)              |
                                                          +-------------------------------+
```

| Stage | Automated? | What happens |
|-------|-----------|-------------|
| **1. Download from Drive** | Manual | `gdrive_bulk_download.py download` downloads ZIPs + trend CSVs, stages to S3 `staging/` |
| **2. Promote to ready/** | Manual | `gdrive_bulk_download.py promote` copies from `staging/` to `ready/` |
| **3. Lambda trigger** | Automated | Detects ZIP in `ready/`, moves it to `scenario/<id>/run/`, finds companion trend CSV, submits Batch job |
| **4. Batch extraction** | Automated | Docker container classifies DSS files (SV/DV), runs `dss_to_csv.py`, uploads CSVs to `scenario/<id>/csv/`, runs optional validation against trend report, writes manifest JSON |
| **5. Statistics ETL** | **Manual** | `run_all.py --scenario <id>` computes derived metrics from the CSVs and loads them into PostgreSQL |
| **6. Verification** | **Manual** | `verify_all_sections.py` and `verify_api.py` confirm data integrity |

After Batch finishes (step 4), the CSVs and manifest exist in S3 but **no statistics are in the database yet**. You must run steps 5-6 manually on Cloud9 with `DATABASE_URL` set.

### Monitoring and logging

Every stage produces logs in a different place (TODO: streamline). Here is where to look and what to check:

#### Download/scan logs (Cloud9 terminal)

The `gdrive_bulk_download.py` script logs to stderr. To capture logs to a file while still seeing output:

```bash
# Scan with logs to file
python etl/scripts/gdrive_bulk_download.py scan \
  --listing-csv database/reference/model_run_file_source.csv \
  --workers 4 2>&1 | tee scan_$(date +%Y%m%d).log

# Download with logs to file
python etl/scripts/gdrive_bulk_download.py download \
  --listing-csv database/reference/model_run_file_source.csv \
  --s3-bucket coeqwal-model-run \
  --workers 4 2>&1 | tee download_$(date +%Y%m%d).log
```

**What to look for:** `MISSING_ZIP`, `MISSING_TREND`, `ALERT_MULTIPLE_ZIP`, `ALERT_MULTIPLE_TREND`, `FOLDER_MISMATCH` in the scan audit summary. All scenarios should show `OK`.

#### Lambda logs (CloudWatch)

```bash
# Tail recent Lambda logs
aws logs tail /aws/lambda/coeqwalEtlTrigger --since 30m

# Follow in real time (useful during promote)
aws logs tail /aws/lambda/coeqwalEtlTrigger --follow
```

**What to look for:**
- `Submitted Batch job <job-id> for scenario <id>` -- confirms the trigger fired
- `Moved ZIP to scenario/<id>/run/` -- confirms file reorganization
- `Found peer CSV` -- confirms trend report was paired with the ZIP
- Any `ERROR` lines indicating the Lambda failed to submit the Batch job

#### Batch extraction logs (CloudWatch)

```bash
# Find the log group (usually /aws/batch/job or similar)
aws logs describe-log-groups \
  --query "logGroups[?contains(logGroupName, 'batch') || contains(logGroupName, 'coeqwal-etl')].logGroupName" \
  --output table

# Check Batch job status
aws batch list-jobs --job-queue coeqwal-etl-queue --job-status SUCCEEDED
aws batch list-jobs --job-queue coeqwal-etl-queue --job-status FAILED
```

**What to look for:**
- `DSS classification` output -- confirms SV and DV files were identified
- `Extraction complete` -- CSVs were generated
- `Validation: PASS` or `Validation: FAIL` -- comparison against trend report
- The `_manifest.json` in S3 summarizes the job result

#### Extraction & validation audit (post-extraction)

After Batch jobs finish, run `check_extraction_results.py` to produce a single summary across all scenarios. It auto-discovers scenario folders in S3, reads each manifest and validation summary, and outputs a console table plus a CSV audit file.

```bash
# Check all scenarios (auto-discovers from S3)
python etl/scripts/check_extraction_results.py \
  --bucket coeqwal-model-run 2>&1 | tee extraction_audit_$(date +%Y%m%d).log

# Check specific scenarios
python etl/scripts/check_extraction_results.py \
  --bucket coeqwal-model-run --scenarios s0021,s0022

# Include cross-scenario mismatch pattern analysis for failed validations
python etl/scripts/check_extraction_results.py \
  --bucket coeqwal-model-run --mismatches --mismatch-output mismatches.csv
```

**What to look for in the output:**
- `extraction_status`: `SUCCEEDED` (both SV and DV), `SUCCEEDED_PARTIAL`, or `NO_MANIFEST` (pending)
- `validation_result`: `passed`, `failed`, or `skipped`
- `unit_verification.calsim_unit_mismatches`: `0` means all CSV units match DSS; non-zero requires investigation
- The `SCENARIOS REQUIRING ATTENTION` section lists anything that needs investigation
- With `--mismatches`: shows which variables (C parts) and locations (B parts) fail most often

**Inspecting a single scenario manually (if needed):**

```bash
# Read one manifest
aws s3 cp s3://coeqwal-model-run/scenario/s0021/s0021_manifest.json - | python -m json.tool

# Check validation reports
aws s3 ls s3://coeqwal-model-run/scenario/s0021/validation/
```

#### Statistics ETL logs (Cloud9 terminal)

```bash
# Run with logs to file
cd etl/statistics
python run_all.py --scenario s0070 2>&1 | tee statistics_s0070_$(date +%Y%m%d_%H%M%S).log

# Verification with logs to file
python verify_all_sections.py --scenario s0070 2>&1 | tee verify_s0070_$(date +%Y%m%d_%H%M%S).log
```

**What to look for:** Each module (reservoirs, du_urban, mi, cws_aggregate, ag) should report row counts loaded. Verification should show `PASS` for all sections.

#### Inspecting ETL logs after a full run

After `run_all.py` finishes, the log file contains per-scenario summaries, row counts, and any errors. Use these commands to quickly assess results without reading the entire file:

```bash
# Find the log file
ls -la ~/environment/coeqwal-backend/etl/statistics/stats_run_*.log

# Check every scenario's summary block (each should show 8 green checkmarks)
grep -A 12 "SUMMARY for" stats_run_*.log | head -200

# Count how many module runs completed successfully
grep -c "completed successfully" stats_run_*.log

# Find real errors (DB overflow, connection issues, data integrity)
grep -E "numeric field overflow|DataError|IntegrityError|could not connect|Traceback" stats_run_*.log

# Check the audit CSV for a compact summary (one row per scenario x module)
column -s, -t < etl/statistics/output/stats_audit_*.csv | head -30
```

**Verifying database row counts directly** (the most reliable check):

```bash
psql $DATABASE_URL -c "
SELECT 'reservoir_storage_monthly' AS tbl, COUNT(DISTINCT scenario_short_code) AS scenarios, COUNT(*) AS rows FROM reservoir_storage_monthly
UNION ALL SELECT 'du_delivery_monthly', COUNT(DISTINCT scenario_short_code), COUNT(*) FROM du_delivery_monthly
UNION ALL SELECT 'mi_delivery_monthly', COUNT(DISTINCT scenario_short_code), COUNT(*) FROM mi_delivery_monthly
UNION ALL SELECT 'cws_aggregate_monthly', COUNT(DISTINCT scenario_short_code), COUNT(*) FROM cws_aggregate_monthly
UNION ALL SELECT 'ag_du_demand_monthly', COUNT(DISTINCT scenario_short_code), COUNT(*) FROM ag_du_demand_monthly
UNION ALL SELECT 'refuge_du_delivery_monthly', COUNT(DISTINCT scenario_short_code), COUNT(*) FROM refuge_du_delivery_monthly
UNION ALL SELECT 'env_flow_channel_monthly', COUNT(DISTINCT scenario_short_code), COUNT(*) FROM env_flow_channel_monthly
UNION ALL SELECT 'delta_monthly', COUNT(DISTINCT scenario_short_code), COUNT(*) FROM delta_monthly
ORDER BY tbl;
"
```

All 8 tables should show 76 scenarios. If any table has fewer, re-run the missing scenarios with `python run_all.py --scenario s00XX --only <module>`.

**Copying logs off Cloud9** (if you need to share or archive them):

```bash
aws s3 cp stats_run_*.log s3://coeqwal-model-run/staging/etl-logs/
```

#### Recommended log retention

Keep log files on Cloud9 for each batch load in a dedicated directory:

```bash
mkdir -p ~/logs/load_$(date +%Y%m%d)
# Then use the tee commands above to write logs there
```

Key log files to keep per load:
- `etl/scripts/output/scan_audit.csv` -- pre-download validation (default location; override with `--output-dir`)
- `etl_download_*.log` -- download/staging output
- `etl/scripts/output/extraction_audit.csv` -- post-extraction status and validation results (default location; override with `-o`)
- `statistics_*.log` -- per-scenario statistics ETL output
- `verify_*.log` -- per-scenario verification output

---

## How to load tier results

Tier results are a separate data product: integer tier levels (1..4) assigned per scenario and per location for 9 tier outcome codes. They live in two tables, `tier_result` (scenario aggregates) and `tier_location_result` (per-location rows), both keyed by `tier_version_id` (currently `8`). Unlike the statistics ETL, tier assignments are produced by various teams from CalSim outputs and delivered to us as CSVs.

### Inputs: where the data team drops files

Tier teams drop their results csv's in drop boxes located at:

https://docs.google.com/spreadsheets/d/1xcQIR_J96-cs7BuCrXjznwkinLgxl-Pf9tA3mJ2GiyA

Tier tab, column I

### Pipeline

```
Team drop --> preprocess --> loader (dry run) --> manifest review -->
  loader (generate SQL) --> psql on Cloud9 --> verify manifest vs DB -->
  verify API vs staging
```

### EC2 sizing

The tier pipeline is lightweight compared to the scenario statistics ETL: it parses a handful of CSVs (largest is ~1 MB), emits an SQL file, and runs `psql`. Peak memory is well under 1 GB. Any of the following works:

- **t3.small / t3.medium** is sufficient if you spin up a dedicated instance.
- **t3a.2xlarge** is what the Cloud9 instance is currently sized to for the scenario statistics ETL (`run_all.py --workers 8`, see below). The tier pipeline happily reuses it with plenty of headroom, so in practice you'll run it on whatever Cloud9 is already provisioned.

**Stop the EC2 instance when you're done.** Tier runs are fast (minutes), so it's easy to forget and leave the Cloud9 instance running. See the callout in the scenario statistics ETL section below for how to stop it from the Cloud9 UI or EC2 console.

### 1. Preprocess: normalize raw drops into canonical flat files

```bash
python etl/tier_data/stage_tier_results.py
```

This reads `etl/tier_data/staging/tier_results/**` and writes the canonical flat files directly into `etl/tier_data/staging/`. The operation is idempotent -- it will overwrite existing flat files every run, so re-run after any new team drop. Use `--dry-run` to print what would be written without touching disk.

### 2. Dry run the loader and review the manifest

```bash
python etl/tier_data/load_all_tier_results.py --dry-run
```

Inspect `etl/tier_data/staging/tier_upload_manifest.csv`. This file lists every `tier_result` and `tier_location_result` row that will be upserted, along with the source CSV filename. Spot-check:
- Row counts per tier code match expectations (72 active scenarios).
- No unexpected scenarios (anything outside `ALLOWED_SCENARIOS` in `load_all_tier_results.py` is silently dropped -- the dry run summary will surface mismatches).

### 3. Generate the SQL file

```bash
python etl/tier_data/load_all_tier_results.py --output-sql all_tiers.sql
```

The bare filename lands in `etl/tier_data/output/all_tiers.sql` (gitignored). Pass an absolute or relative path containing `/` to write somewhere else. The script emits UPSERT statements (`ON CONFLICT ... DO UPDATE`) for both tables plus any required deactivations. It does **not** connect to the database in this mode.

### 4. Apply on Cloud9

```bash
psql "$DATABASE_URL" -f etl/tier_data/output/all_tiers.sql
```

`DATABASE_URL` must point at the target (staging or prod) RDS instance. The SQL is keyed by `(scenario_short_code, tier_short_code, tier_version_id)` on `tier_result` and `(scenario_short_code, tier_short_code, location_id, tier_version_id)` on `tier_location_result`, so re-running is safe and idempotent.

### 5. Verify: manifest vs database (THIS STEP IS MANDATORY)

**Never skip this.** After psql reports success, confirm every row in the manifest actually landed in the database:

```bash
DATABASE_URL="$DATABASE_URL" python etl/tier_data/load_all_tier_results.py --verify
```

This reads `staging/tier_upload_manifest.csv` back and checks each row against the live DB. It reports:
- `Rows checked` -- should equal manifest row count
- `Missing` -- rows in manifest but not in DB (must be 0)
- `Mismatches` -- rows present but with different tier levels (must be 0)
- `Status: PASS` or `Status: FAIL`

If `FAIL`, do **not** proceed. Re-run `psql`, investigate triggers/constraints, or revert.

### 6. Verify: staging vs API

Second verification layer, catches problems introduced between DB and API (views, caching, pagination):

```bash
python etl/tier_data/verify_tiers.py --api-url https://api.coeqwal.org/api
```

Narrow with `--scenario s0070` or `--tier ENV_FLOWS` when investigating a single failure.

### Troubleshooting

- **Unknown scenarios skipped** -- the loader enforces `ALLOWED_SCENARIOS` (currently 72 scenarios). If the team delivers data for a new scenario, it must first be added to the `scenario` table (see "How to load new scenarios" above) and then added to `ALLOWED_SCENARIOS` in `load_all_tier_results.py` and `verify_tiers.py`.
- **ENV_FLOWS scenarios appear in more than one split file** -- expected. The loader processes `historical` first, then `cc50`, then `cc95`, and later files overwrite earlier ones. If a scenario legitimately has data in only one file, nothing special is needed.
- **DELTA_ECO scenarios use numeric IDs** -- the source files list scenarios as `"11"`, `"65"`, etc. The loader's `normalize_scenario_id` converts these to `s0011`, `s0065`.
- **Salmon CSV missing** -- Run `stage_tier_results.py` to regenerate `staging/WRC_SALMON_AB.csv` from `staging/tier_results/salmon/`.

### Tier version bumps

`tier_version_id` is a constant at the top of `load_all_tier_results.py`. Bump it (and add a row to the `tier_version` table) only when the tier methodology or thresholds change in a way that should coexist with existing data. Day-to-day data refreshes should keep the same version id so UPSERT continues to work.

---

## AWS production deployment

### Architecture
**AWS-Native pipeline** for automated processing:
- **Trigger**: S3 upload of DSS ZIP files
- **Processing**: AWS Batch jobs using Docker containers
- **Validation**: Automatic comparison against reference CSVs (uploaded Trend Report)
- **Storage**: Results saved to S3 with reports

### Benefits of Docker + AWS Batch:
- **Consistent environment** (Linux + heclib.a)
- **Scalable processing** (multiple concurrent jobs)
- **Cost-effective** (pay only for processing time, no Windows licensing)
- **Automated workflow** (S3 upload triggers automatic processing)

### Components

#### `coeqwal-etl/` - Main ETL container
Docker-based DSS extraction using `pydsstools`:
- **Input**: DSS files from CalSim model runs
- **Output**: CSV time series data + `.units.json` sidecar files (DSS unit ground truth)
- **Validation**: Compares against reference data with configurable tolerances
- **Unit verification**: `--verify-units` flag on `dss_to_csv.py` checks every column's unit against the DSS source
- **Standalone verifier**: `verify_dss_csv_units.py` can re-verify any scenario's units on-demand from S3
- **Platform**: Linux containers (AWS Batch compatible)

#### `lambda-trigger/` - S3 Event Handler
AWS Lambda function that triggers ETL jobs:
- **Trigger**: S3 ObjectCreated events on DSS ZIP uploads
- **Action**: Submits AWS Batch job with validation parameters
- **Function name**: `coeqwalEtlTrigger`
- **Runtime**: Node.js 18+ (uses built-in AWS SDK v3, no `node_modules` needed)
- **Source**: Single file `lambda-trigger/index.mjs`

##### Deploying Lambda updates

The Lambda is a single `index.mjs` file with no external dependencies. Deploy via the AWS Console:

1. Go to **AWS Console to Lambda to Functions to coeqwalEtlTrigger**
2. Click the **Code** tab
3. Select all in the inline editor, paste the full contents of `etl/lambda-trigger/index.mjs`
4. Click **Deploy**

Alternatively, from the Cloud9 terminal:

```bash
cd ~/environment/coeqwal-backend/etl/lambda-trigger
zip lambda.zip index.mjs
aws lambda update-function-code --function-name coeqwalEtlTrigger --zip-file fileb://lambda.zip
rm lambda.zip
```

##### Monitoring Lambda logs

```bash
# Tail recent logs
aws logs tail /aws/lambda/coeqwalEtlTrigger --since 5m

# Follow logs in real time
aws logs tail /aws/lambda/coeqwalEtlTrigger --follow
```

##### Finding other log groups

```bash
aws logs describe-log-groups --query "logGroups[?contains(logGroupName, 'coeqwal')].logGroupName" --output table
```

Key log groups:
| Log group | Service |
|-----------|---------|
| `/aws/lambda/coeqwalEtlTrigger` | S3 to Lambda trigger |
| `/aws/lambda/coeqwal-database-audit` | DB audit Lambda |
| `/aws/lambda/coeqwalPresignDownload` | Download presigner |
| `/ecs/coeqwal-api` | API server |
| `/aws/rds/cluster/coeqwal-scenario-db-v1/postgresql` | RDS PostgreSQL |

### AWS workflow

#### 1. Upload DSS files
```
s3://coeqwal-model-run/ready/
```

#### 2. Automatic processing
- Lambda detects upload
- Submits Batch job
- Docker container extracts CSV data
- Validates against reference CSV (if provided)

#### 3. Results storage
```
s3://coeqwal-model-run/scenario/{scenario_short_code}/
├── csv/ # Extracted CSV files
├── validation/ # Validation reports (JSON + CSV)
└── {scenario_short_code}_manifest.json # Processing summary
```

---

## Bulk Loading Scenarios from Google Drive

When the modeling team delivers new or rerun scenarios, use the `gdrive_bulk_download.py` script to download model run ZIPs and trend report CSVs from the COEQWAL Shared Drive, validate DSS contents, stage to S3, and promote to trigger the extraction pipeline.

### Prerequisites

| Requirement | Where | Notes |
|-------------|-------|-------|
| **rclone** | Cloud9 (or local Mac) | Handles Google Drive auth; no GCP project needed |
| **rclone config** (`~/.config/rclone/rclone.conf`) | Cloud9 | Must be configured with a `gdrive` remote pointing to the COEQWAL Shared Drive |
| **Python 3.9+** | Cloud9 | Already available on Cloud9 |
| **boto3, openpyxl** | Cloud9 | `pip install -r etl/scripts/requirements-gdrive.txt` |
| **AWS credentials** | Cloud9 | Already configured on Cloud9 (IAM role) |
| **Scenario listing Excel** | `reference/COEQWAL_Completed_Scenario_Listing.xlsx` | Contains scenario short codes and Drive folder hyperlinks |

### Step 0: Check and increase Cloud9 storage

Cloud9 instances default to **10 GB** EBS, which is tight when downloading ~200 MB ZIPs for 24 scenarios. The script streams files through `/tmp/` and uploads to S3 immediately, so you only need space for one ZIP at a time per worker, but it's still good practice to check.

**Check current disk usage:**
```bash
df -h /
```

If usage is above ~70%, resize the EBS volume:

**Resize the EBS volume (no downtime required):**

1. Open the **AWS Console to EC2 to Volumes**
2. Find the volume attached to your Cloud9 instance (check the instance ID in Cloud9 terminal: `curl -s http://169.254.169.254/latest/meta-data/instance-id`)
3. Select the volume to **Actions to Modify Volume**
4. Change the size (e.g., 10 GB to 20 GB) to **Modify**
5. Wait ~30 seconds for the modification to complete
6. Back in Cloud9 terminal, grow the filesystem:

```bash
# Check the partition name (usually /dev/xvda1 or /dev/nvme0n1p1)
lsblk

# Grow the partition (adjust device name as needed)
sudo growpart /dev/xvda 1

# Resize the filesystem
sudo resize2fs /dev/xvda1

# Verify
df -h /
```

**Alternative - skip local storage entirely:** The script uses `/tmp/` as a transient staging area and uploads to S3 immediately. With `--workers 1` you only need ~200 MB free. With `--workers 4` you need ~800 MB. If storage is a concern, reduce workers.

### Step 1: Install rclone on Cloud9

```bash
curl https://rclone.org/install.sh | sudo bash
rclone version   # confirm installation
```

### Step 2: Set up rclone config

rclone must be authenticated on a machine with a web browser (e.g., your Mac) because Google OAuth requires a browser redirect. Once authenticated, copy the config to Cloud9.

**If you already authenticated on your Mac:**

```bash
# On your Mac, display the config:
cat ~/.config/rclone/rclone.conf

# On Cloud9, paste it:
mkdir -p ~/.config/rclone
nano ~/.config/rclone/rclone.conf
# Paste the contents, save with Ctrl+O, exit with Ctrl+X
```

**If you need to set up rclone from scratch:**

```bash
# On your Mac (which has a browser):
rclone config

# Choose:
#   n) New remote
#   name> gdrive
#   Storage> drive (Google Drive)
#   client_id> (leave blank - uses rclone's built-in OAuth client)
#   client_secret> (leave blank)
#   scope> 2 (drive.readonly)
#   service_account_file> (leave blank)
#   Edit advanced config> n
#   Use web browser to authenticate> y
#   to Browser opens, authenticate with your UC Berkeley Google account (2FA required)
#   Configure as Shared Drive> y
#   Select: COEQWAL
#   Keep this remote> y
```

**Verify it works on Cloud9:**

```bash
rclone lsd gdrive:   # Should list top-level Shared Drive folders
```

**Token refresh:** The rclone config contains a refresh token. It should auto-renew, but if you get 401 errors after weeks of inactivity, re-run `rclone config reconnect gdrive:` on your Mac and re-copy the config.

### Step 3: Install Python dependencies

```bash
cd ~/environment/coeqwal-backend
pip install -r etl/scripts/requirements-gdrive.txt
```

### Scan subcommand (v6 CSV-based workflow)

For loading CC50/CC95 sibling scenarios (or any set described in the v6 scenario listing CSV), use the `scan` subcommand. It reads `reference/coeqwal_cs3_scenario_listing_v6.xlsx - scenario_list.csv` directly -- no Excel parsing or hyperlink extraction needed.

**Phase 1: Local-only validation (no rclone, no Drive access)**

Run this on your Mac to validate CSV paths before touching Cloud9:

```bash
python etl/scripts/gdrive_bulk_download.py scan \
  --listing-csv "reference/coeqwal_cs3_scenario_listing_v6.xlsx - scenario_list.csv" \
  --local-only
```

This parses the CSV, extracts folder IDs from `ModelFilesLink` URLs, compares `GoogleDriveFolderName` against the `DV_Path` root, and writes `scan_manifest.csv`. Review the output for `FOLDER_MISMATCH` and `NO_FOLDER_ID` flags.

**Phase 2: Drive scan (rclone required -- run on Cloud9)**

```bash
python etl/scripts/gdrive_bulk_download.py scan \
  --listing-csv "reference/coeqwal_cs3_scenario_listing_v6.xlsx - scenario_list.csv" \
  --workers 4
```

This lists `Model_Files/` and `Data_Extraction/Variables_From_trend_report_variables_v5/` for each scenario via rclone, counts ZIP files and trend report CSVs, and writes `scan_audit.csv` to `etl/scripts/output/` (gitignored). Override with `--output-dir`.

**Scan a subset of scenarios:**
```bash
python etl/scripts/gdrive_bulk_download.py scan \
  --listing-csv "reference/coeqwal_cs3_scenario_listing_v6.xlsx - scenario_list.csv" \
  --scenarios s0070 s0090
```

**Scan audit statuses:**
| Status | Meaning |
|--------|---------|
| `OK` | Exactly 1 ZIP and 1 trend CSV found |
| `ALERT_MULTIPLE_ZIP` | More than 1 ZIP in `Model_Files/` -- most recent is auto-selected |
| `ALERT_MULTIPLE_TREND` | More than 1 trend CSV -- most recent is auto-selected |
| `MISSING_ZIP` | No ZIP file found in `Model_Files/` |
| `MISSING_TREND` | No trend report CSV found |
| `FOLDER_MISMATCH` | `GoogleDriveFolderName` differs from `DV_Path` root (cosmetic issue) |
| `NO_FOLDER_ID` | `ModelFilesLink` has no extractable Google Drive folder ID |

### Step 4: Dry run (no downloads, no S3 writes)

```bash
python etl/scripts/gdrive_bulk_download.py download \
  --listing reference/COEQWAL_Completed_Scenario_Listing.xlsx \
  --s3-bucket coeqwal-model-run \
  --dry-run
```

This will:
- Read the Excel listing and extract Drive folder IDs from hyperlinks
- List `Model_Files/` and `Data_Extraction/Variables_From_trend_report_variables_v5/` for each scenario via rclone
- Report what *would* be downloaded, including any alerts (multiple ZIPs, missing CSVs, etc.)
- Print a summary table

Review the output before proceeding.

### Step 5: Download a single scenario (smoke test)

```bash
python etl/scripts/gdrive_bulk_download.py download \
  --listing reference/COEQWAL_Completed_Scenario_Listing.xlsx \
  --s3-bucket coeqwal-model-run \
  --scenarios s0020
```

This will:
1. Download the most recent ZIP from `Model_Files/`
2. **Validate** the ZIP - open it, list all `.dss` files, classify them as SV (input) or DV (output), and alert if there is not exactly one of each type
3. Upload the ZIP to `s3://coeqwal-model-run/staging/s0020/`
4. Download the trend report CSV (starting with `s0020`) from `Data_Extraction/Variables_From_trend_report_variables_v5/`
5. Upload the CSV to `s3://coeqwal-model-run/staging/s0020/`
6. Write `audit_report.csv` to `etl/scripts/output/` (gitignored; override with `--output-dir`) and upload to `s3://coeqwal-model-run/staging/`

**Verify in S3:**
```bash
aws s3 ls s3://coeqwal-model-run/staging/s0020/
```

### Step 6: Review the audit report

```bash
# View locally (default location; --output-dir overrides)
column -s, -t < etl/scripts/output/audit_report.csv | less -S

# Or download from S3
aws s3 cp s3://coeqwal-model-run/staging/audit_report.csv .
```

Key columns to check:
| Column | What to look for |
|--------|-----------------|
| `validation_status` | Should be `OK`. Anything with `ALERT` needs manual review. |
| `sv_candidate_count` | Should be `1`. More means extra SV DSS files in the ZIP. |
| `dv_candidate_count` | Should be `1`. More means extra DV DSS files in the ZIP. |
| `zip_count` | Should be `1`. More means multiple ZIPs in `Model_Files/`. |
| `trend_csv_count` | Should be `1`. `0` means missing trend report. |

### Step 7: Download all scenarios

Once the smoke test looks good:

```bash
python etl/scripts/gdrive_bulk_download.py download \
  --listing reference/COEQWAL_Completed_Scenario_Listing.xlsx \
  --s3-bucket coeqwal-model-run \
  --workers 4
```

This processes 4 scenarios in parallel. Each downloads to `/tmp/`, validates, uploads to S3 staging, and cleans up. Total time depends on network speed; expect ~30-60 minutes for 24 scenarios.

**To download a subset:**
```bash
python etl/scripts/gdrive_bulk_download.py download \
  --listing reference/COEQWAL_Completed_Scenario_Listing.xlsx \
  --s3-bucket coeqwal-model-run \
  --scenarios s0020 s0023 s0024 s0025
```

### Step 8: Promote to trigger extraction

Files in `staging/` do not trigger the Lambda. To trigger extraction, promote to `ready/`:

```bash
# Promote one scenario (recommended first time)
python etl/scripts/gdrive_bulk_download.py promote \
  --s3-bucket coeqwal-model-run --scenarios s0020

# Promote all staged scenarios
python etl/scripts/gdrive_bulk_download.py promote \
  --s3-bucket coeqwal-model-run
```

The promote command:
1. Lists all files in `staging/<shortcode>/`
2. Shows what will be copied and asks for confirmation
3. Copies each file to `ready/<shortcode>/`
4. The Lambda triggers on the ZIP upload and starts the Batch extraction job

**Monitor extraction:**
```bash
# Check Lambda logs
aws logs tail /aws/lambda/coeqwal-etl-trigger --follow

# Check Batch jobs
aws batch list-jobs --job-queue coeqwal-etl-queue --job-status RUNNING
```

### Lambda deployment (required before first promote)

Before promoting any scenarios, ensure the Lambda has the subfolder-aware version deployed. See [Deploying Lambda updates](#deploying-lambda-updates) above. The key changes:
- Derives `sourcePrefix` from the S3 key to support `ready/<shortcode>/` subfolders
- Passes `sourcePrefix` to `findPeerCsv` so it finds companion CSVs in the same subfolder
- Cleans up the subfolder after processing

### How the Lambda handles subfolder uploads

The Lambda trigger (`lambda-trigger/index.mjs`) supports both flat and subfolder uploads:

| Upload pattern | How it works |
|---------------|-------------|
| `ready/scenario.zip` | Original flat pattern. Lambda finds peer CSV in `ready/`. |
| `ready/s0020/scenario.zip` | Subfolder pattern. Lambda finds peer CSV in `ready/s0020/`. After processing, cleans up the subfolder. |

The companion trend report CSV (uploaded alongside the ZIP in the same subfolder) is used as the validation reference for the extraction job.

### Troubleshooting

| Problem | Solution |
|---------|----------|
| `rclone: command not found` | Run `curl https://rclone.org/install.sh \| sudo bash` |
| `Failed to create file system: google drive: didn't find section in config file` | rclone config is missing. Copy from Mac (see Step 2). |
| `rclone lsjson` returns empty | Check the Drive folder ID in the Excel hyperlinks. Try manually: `rclone lsjson --drive-root-folder-id=<ID> gdrive:` |
| `401 Unauthorized` | Token expired. Re-authenticate on Mac: `rclone config reconnect gdrive:` and re-copy config. |
| `ALERT_MULTIPLE_SV` or `ALERT_MULTIPLE_DV` | ZIP contains extra DSS files. Check `sv_all_candidates` / `dv_all_candidates` in audit report. May need to add override in `SCENARIO_OVERRIDES`. |
| `ALERT_NO_SV` or `ALERT_NO_DV` | ZIP is missing expected DSS type. Check the ZIP contents manually. |
| `MISSING_ZIP` | `Model_Files/` in Drive folder has no ZIPs. Check the Drive folder link. |
| `MISSING_TREND_REPORT` | No CSV starting with scenario shortcode in `Variables_From_trend_report_variables_v5/`. |
| `No space left on device` | Reduce `--workers` to 1, or resize EBS volume (see Step 0). |

---

## Local development & processing

### Prerequisites
- Docker installed and running on your machine
- DSS files available locally

### Step-by-step instructions

#### 1. Build the Docker container
```bash
cd etl/coeqwal-etl/
docker build -t coeqwal-dss .
```

#### 2. Prepare your local directories
```bash
# Create directories for input and output
mkdir -p ./dss_processing/input
mkdir -p ./dss_processing/output

# Copy your DSS files to input directory
cp /path/to/your/file.dss ~/dss_processing/input/
```

#### 3. Run DSS to CSV conversion
```bash
# Basic conversion (CalSim output)
docker run -v ~/dss_processing/input:/input -v ~/dss_processing/output:/output --entrypoint python coeqwal-dss /app/python-code/dss_to_csv.py --dss /input/your_file.dss --csv /output/result.csv --type calsim_output

# SV input conversion
docker run -v ~/dss_processing/input:/input -v ~/dss_processing/output:/output --entrypoint python coeqwal-dss /app/python-code/dss_to_csv.py --dss /input/your_sv_file.dss --csv /output/sv_result.csv --type sv_input
```

#### 4. Validation (optional)
```bash
# Compare your extracted CSV against a reference
docker run --platform linux/amd64 -v ./dss_processing:/data --entrypoint python coeqwal-dss /app/python-code/validate_csvs.py --ref /data/output/coeqwal_s0011_adjBL_wTUCP_DV_v0.0.csv --file /data/output/result.csv --abs-tol 1e-6 --rel-tol 1e-6 --verbose --out-csv /data/output/detailed_mismatches.csv --out-json /data/output/validation_summary.json
```
---

## Data Accuracy Verification

End-to-end verification of data accuracy across the full pipeline, from DSS extraction through database statistics to API responses. Verification runs at five layers:

```
DSS Files --> S3 CSVs (DV + SV) --> PostgreSQL --> JSON API --> Frontend
  Layer 1        Layer 2              Layer 2b       Layer 3     Layer 4
  (extraction)   (ETL statistics)     (tier data)    (API)       (status page)
```

Variable lists sourced from `COEQWAL_V3/notebooks/variable_groupings.csv` and mapping CSVs (`DrinkingWater_Mapping.csv`, `Agricultural_Mapping.csv`, `Eflows_Mapping.csv`).

### Layer 1: Extraction (DSS to CSV)

Validates that `dss_to_csv.py` extracts data correctly from HEC-DSS files. Uses `validate_csvs.py` to compare extracted CSVs against reference CSVs.

Manifests stored in `audits/validation_mismatches/{scenario_id}_manifest.json`.

### Layer 1b: DSS-vs-CSV Unit Verification

Independently verifies that the unit metadata in every CSV column header matches what the original DSS file reports. This is a ground-truth check: it re-opens the DSS file with pydsstools and compares each variable's unit against the CSV header row 6.

**How it works:**

1. Downloads the model run ZIP from `s3://coeqwal-model-run/scenario/{id}/run/`
2. Extracts and opens the CalSim output DSS with pydsstools
3. For each DSS pathname, reads the unit from DSS metadata
4. Downloads the CSV header from `s3://coeqwal-model-run/scenario/{id}/csv/`
5. Compares: for every `(B-part, C-part)` present in both, does `DSS unit == CSV unit`?
6. Logs a unit-pair summary (e.g., `CFS<>CFS (18432), TAF<>TAF (3102)`) showing what units were actually compared

**Requires pydsstools** - runs inside the `coeqwal-etl` Docker image, not directly on Cloud9.

**Running on Cloud9 via Docker:**

```bash
# Build the extraction Docker image (one-time, or after code changes)
cd ~/environment/coeqwal-backend/etl/coeqwal-etl
docker build -t coeqwal-etl:test .

# Single scenario smoke test
docker run --rm --entrypoint "" coeqwal-etl:test \
  python /app/python-code/verify_dss_csv_units.py --scenario s0025 \
  2>&1 | tee verify_units_s0025.log

# All scenarios (auto-discovered from S3 bucket)
docker run --rm --entrypoint "" coeqwal-etl:test \
  python /app/python-code/verify_dss_csv_units.py --scenarios-from-s3 --workers 6 \
  2>&1 | tee verify_units_all_$(date +%Y%m%d_%H%M%S).log

# Save mismatch report to CSV
docker run --rm --entrypoint "" coeqwal-etl:test \
  python /app/python-code/verify_dss_csv_units.py --scenarios-from-s3 --output report.csv
```

Use `tmux` for long-running scans (all ~75 scenarios takes ~50 minutes with 6 workers). You can close your laptop or let SSO expire - the tmux session keeps running on the EC2 instance. When you come back, log into Cloud9 again and run `tmux attach` to reconnect. The log file (`~/environment/verify_units_all_*.log`) is also saved via `tee`, so you can read results with `cat` or `less` even if the tmux session ended.

**Always-on at extraction time:**

The `--verify-units` flag is wired into `batch_entrypoint.sh`. Every Batch extraction job automatically:
- Runs the DSS-vs-CSV unit check after conversion
- Writes a `.units.json` sidecar file alongside the CSV (DSS unit ground truth)
- Uploads the sidecar to S3 at `scenario/{id}/csv/{id}_coeqwal_calsim_output.csv.units.json`
- Records `unit_verification.calsim_unit_mismatches` in the manifest JSON

**Unit map sidecar format** (`*.csv.units.json`):

```json
{"AW_01_PA": {"c_part": "APPLIED-WATER", "unit": "CFS"}, "S_SHSTA": {"c_part": "STORAGE", "unit": "TAF"}, ...}
```

The sidecar is also emitted as a `UNIT_MAP` log line in CloudWatch for every extraction, providing a permanent audit trail without needing to re-open the DSS.

**Duplicate B-part detection:**

The extraction code (`dss_to_csv.py`) detects when multiple DSS pathnames share the same B-part but have different C-parts (e.g., `SHRTG_PCWA3/SHORTAGE` and `SHRTG_PCWA3/DELIVERY-SHORTAGE`). These are logged as warnings and counted in the manifest under `duplicate_b_parts`. The statistics ETL resolves these using C-part-aware deduplication (preferring the expected C-part, e.g., `SHORTAGE` over `DELIVERY-SHORTAGE` for `SHRTG_*` variables).

The `scan_dupes.py` script in `etl/statistics/` can scan all scenario CSVs for duplicates and audit cross-scenario unit consistency without Docker:

```bash
cd ~/environment/coeqwal-backend/etl/statistics
python scan_dupes.py --compare-values --audit-units --workers 4
```

### Layer 2: ETL Statistics (CSV to DB)

Computes expected values from reference CSVs and compares against database values.

```bash
# Single scenario
python etl/statistics/verify_all_sections.py --scenario s0020

# All scenarios with JSON reports
python etl/statistics/verify_all_sections.py --all-scenarios --report-dir audits/verification_reports

# CSV-only mode (no DB connection needed)
python etl/statistics/verify_all_sections.py --scenario s0020 --csv-only
```

**Sections verified:**
- **Reservoirs**: April/Sept storage (TAF + % capacity), annual average, spill frequency
- **CWS Aggregates**: Annual delivery (TAF), shortage, reliability
- **CWS Demand Units**: Per-DU annual delivery (TAF) for sample DUs
- **AG Demand Units**: SW delivery, GW pumping, demand, reliability for sample DUs
- **AG Aggregates**: Annual delivery (TAF)
- **M&I Contractors**: Delivery, shortage, reliability, % demand met
- **Env Flows**: Average CFS, Pearson r, % unimpaired, % functional flows
- **Refuge**: Delivery, shortage, reliability
- **Tiers**: All 9 tier codes verified against staging CSVs and DB

**Tolerances**: `abs_tol=0.5`, `rel_tol=0.01` (configurable per check)

**Output**: `audits/verification_reports/{scenario_id}_layer2.json`

### Layer 3: API Verification (DB to API)

Queries API endpoints and compares responses to direct database queries.

```bash
# Single scenario
python etl/statistics/verify_api.py --scenario s0020

# Custom API URL
python etl/statistics/verify_api.py --scenario s0020 --api-url http://localhost:8000

# All scenarios
python etl/statistics/verify_api.py --all-scenarios
```

**Endpoints verified:**
- `GET /api/statistics/batch` (storage, CWS, AG)
- `GET /api/tiers/scenarios/{id}/tiers` (all 9 tier codes)
- `GET /api/statistics/scenarios/{id}/channels/period-summary` (env flow)

**Output**: `audits/verification_reports/{scenario_id}_layer3.json`

### Layer 4: Public Status Page

Verification results are served by `GET /api/verification/status` and displayed at `/verification` on the frontend. Shows per-scenario pass/fail grid with drill-down to individual checks.

### Metric coverage

**Implemented and loaded (ETL + DB):**

| Module | Metrics | Entities | Tables |
|--------|---------|----------|--------|
| **Reservoirs** | Storage (TAF, % capacity), flood/dead pool probability, spill volume/frequency | 10 reservoirs (Shasta, Oroville, Folsom, Trinity, New Melones, Millerton, San Luis CVP/SWP/combined, Eastside Bypass) | `reservoir_storage_monthly`, `reservoir_period_summary` |
| **Urban DU** | Delivery, shortage, % demand met, reliability | 81 demand units | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |
| **M&I Contractors** | Delivery, shortage, % demand met (via PERDV), reliability | 16 SWP contractors + MWD aggregate | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |
| **CWS Aggregates** | Delivery, shortage, reliability by project/region | 6 aggregates (SWP total/N/S, CVP total/N/S) | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |
| **AG** | Demand (AW), SW delivery (DN), GW pumping (GP), shortage, reliability, GW restriction shortage | 131 demand units + 9 regional aggregates | `ag_du_demand_monthly`, `ag_du_sw_delivery_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary`, `ag_aggregate_monthly`, `ag_aggregate_period_summary` |
| **Refuge** | Delivery, derived shortage (demand - delivery), reliability | 18 wildlife refuge demand units | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` |
| **Env Flows** | Flow volume (CFS, TAF), % unimpaired, % functional flows, alteration index (Pearson r), CEFF seasonal metrics | 59 channels (20 with MIF, 17 with EFLOWS) | `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary` |
| **Delta** | Net Delta Outflow (NDO), X2 position (spring/fall), salinity at Emmaton, Jersey Point, Rock Slough, Collinsville, Banks and Tracy pumping plant EC | 8 variables | `delta_monthly`, `delta_period_summary` |
| **Sensitivity** | Climate sensitivity (hist/CC50/CC95 comparison), operational sensitivity (cross-scenario spread) | All entities from above modules | `sensitivity_climate`, `sensitivity_operational` |
| **Tiers** | CWS_DEL, AG_REV, ENV_FLOWS, RES_STOR, GW_STOR, DELTA_ECO, FW_DELTA_USES, FW_EXP, WRC_SALMON_AB | 9 tier codes | `tier_location_result` |

**Verified end-to-end (ETL + DB + API):**
- CWS: delivery volume, % of demand, absolute shortage
- AG: SW delivery, GW pumping, total shortage, shortage %, reliability
- Env Flows: volume, % unimpaired, % functional flows, alteration index
- Refuge: delivery, shortage, reliability
- Reservoirs: April/Sept storage (TAF + %), spill frequency
- Delta: NDO, X2, EC at 4 stations, pumping plant EC
- Tiers: all 9 tier codes

**Not yet implemented:**
- Groundwater level, storage volume, level/storage change (no CalSim variable mapping established)
- Salmon abundance as a continuous/raw metric (`WRC_SALMON_AB` is currently stored only as the categorical tier level parsed from the data team's CSV. `tier_score_cont` is passed through but not persisted)

### How to add a new scenario

1. Ensure DSS-to-CSV extraction has run and manifests show PASS in `audits/validation_mismatches/`
2. Run the ETL statistics: `python etl/statistics/run_all.py --scenario {id}`
3. Load tier data (see "How to load tier results" above for the full flow):
   - `python etl/tier_data/stage_tier_results.py` (normalize team drops into flat files)
   - `python etl/tier_data/load_all_tier_results.py --output-sql all_tiers.sql` then `psql "$DATABASE_URL" -f etl/tier_data/output/all_tiers.sql`
   - `DATABASE_URL="$DATABASE_URL" python etl/tier_data/load_all_tier_results.py --verify` (mandatory)
4. Run Layer 2 verification: `python etl/statistics/verify_all_sections.py --scenario {id}`
5. Run Layer 3 verification: `python etl/statistics/verify_api.py --scenario {id}`
6. Check results at `/verification` on the frontend

---

## Validation framework

### Tolerance parameters
- **Absolute tolerance (`abs_tol`)**: Maximum allowed absolute difference between values
- Example: `abs_tol=1e-6` means values must be within +/-0.000001 units
- Used for values close to zero where relative comparison isn't meaningful

- **Relative tolerance (`rel_tol`)**: Maximum allowed relative difference as a fraction
- Example: `rel_tol=1e-6` means values must be within 0.0001% of each other
- Used for larger values where proportional differences matter more

### Validation logic
Values are considered equal if:
```python
# Both are NaN OR within tolerances
np.isclose(value1, value2, atol=abs_tol, rtol=rel_tol, equal_nan=True)
```

### Testing details
- **Default tolerances**: 1e-6 absolute and relative
- **Scope**: Compares all common variables between reference and extracted data
- **Reporting**: Detailed mismatch analysis with exact differences
- **Status**: PASS/FAIL with comprehensive summaries

---

## Technical details

### DSS library
Uses `pydsstools` with `heclib.a` (Linux static library) for reading HEC-DSS files.

### Supported DSS types
- **CalSim Output**: Time series model results (typically monthly data)
- **SV Input**: Scenario variables and boundary conditions

---

## Community water systems (CWS)

This section documents the CalSim3 variables used to compute M&I (Municipal & Industrial) water supply metrics for community water systems.

### Overview

M&I deliveries in CalSim3 are tracked through PMI (Project Municipal & Industrial) variables. Three key metrics are computed:

1. **M&I surface water deliveries** (acre-feet)
2. **M&I surface water deliveries as percent of demand** (percent)
3. **Absolute M&I supply shortage** (acre-feet)

### Aggregate delivery variables

| Variable | Description |
|----------|-------------|
| `DEL_SWP_PMI` | Total SWP M&I deliveries |
| `DEL_SWP_PMI_N` | SWP M&I deliveries - North of Delta |
| `DEL_SWP_PMI_S` | SWP M&I deliveries - South of Delta |
| `DEL_CVP_PMI_N` | CVP M&I deliveries - North |
| `DEL_CVP_PMI_S` | CVP M&I deliveries - South |

### Aggregate shortage variables

| Variable | Description |
|----------|-------------|
| `SHORT_SWP_PMI` | Total SWP M&I shortage |
| `SHORT_SWP_PMI_N` / `SHORT_SWP_PMI_S` | SWP M&I shortage North/South |
| `SHORT_CVP_PMI_N` / `SHORT_CVP_PMI_S` | CVP M&I shortage North/South |

### Individual CWS variable patterns

Community water systems can have three associated variables:

| Pattern | Description | Example |
|---------|-------------|---------|
| `D_{node}_{district}_PMI` | Delivery | `D_SBA029_ACWD_PMI` |
| `SHORT_D_{node}_{district}_PMI` | Shortage | `SHORT_D_SBA029_ACWD_PMI` |
| `PERDV_SWP_{n}` | Percent allocation divisor | `PERDV_SWP_3` |

### Demand calculation formula

Demand can be back-calculated from delivery and shortage using the percent delivery allocation
(but there's probably a better way):

```
Demand = (Delivery + Shortage) / (PERDV / 100)
```

Percent of demand delivered:
```
Percent_of_Demand = (Delivery / Demand) x 100
                  = (Delivery x PERDV) / (Delivery + Shortage)
```

### Individual CWS mapping table (verified)

All PERDV mappings below are verified from `DataExtraction.py` with specific line references:

| Water District | Delivery Variable | Shortage Variable | PERDV | Source |
|---------------|-------------------|-------------------|-------|--------|
| Alameda County WD (ACWD) | `D_SBA029_ACWD_PMI` | `SHORT_D_SBA029_ACWD_PMI` | `PERDV_SWP_3` | Line 1248 |
| Santa Clara Valley WD (SCVWD) | `D_SBA036_SCVWD_PMI` | `SHORT_D_SBA036_SCVWD_PMI` | `PERDV_SWP_35` | Line 1268 |
| Santa Barbara | `D_CSB103_BRBRA_PMI` | `SHORT_D_CSB103_BRBRA_PMI` | `PERDV_SWP_34` | Line 1068 |
| San Luis Obispo | `D_CSB038_OBISPO_PMI` | `SHORT_D_CSB038_OBISPO_PMI` | `PERDV_SWP_35` | Line 1088 |
| Ventura (Castaic) | `D_CSTIC_VNTRA_PMI` | `SHORT_D_CSTIC_VNTRA_PMI` | `PERDV_SWP_39` | Line 1111 |
| Ventura (Pyramid) | `D_PYRMD_VNTRA_PMI` | `SHORT_D_PYRMD_VNTRA_PMI` | `PERDV_SWP_38` | Line 1117 |
| Antelope Valley-East Kern (AVEK) | `D_ESB324_AVEK_PMI` | `SHORT_D_ESB324_AVEK_PMI` | `PERDV_SWP_4` | Line 1128 |
| Palmdale | `D_ESB347_PLMDL_PMI` | `SHORT_D_ESB347_PLMDL_PMI` | `PERDV_SWP_29` | Line 1148 |
| San Bernardino | `D_ESB414_BRDNO_PMI` | `SHORT_D_ESB414_BRDNO_PMI` | `PERDV_SWP_30` | Line 1168 |
| San Gabriel | `D_ESB415_GABRL_PMI` | `SHORT_D_ESB415_GABRL_PMI` | `PERDV_SWP_31` | Line 1188 |
| Gorgonio | `D_ESB420_GRGNO_PMI` | `SHORT_D_ESB420_GRGNO_PMI` | `PERDV_SWP_32` | Line 1208 |
| Kern County (A) | `D_CAA194_KERNA_PMI` | `SHORT_D_CAA194_KERNA_PMI` | `PERDV_SWP_15` | Line 1322 |
| Castaic Lake (SVRWD) | `D_SVRWD_CSTLN_PMI` | `SHORT_D_SVRWD_CSTLN_PMI` | `PERDV_SWP_11` | Line 1302 |
| ACFC (term 1) | `D_SBA009_ACFC_PMI` | `SHORT_D_SBA009_ACFC_PMI` | `PERDV_SWP_1` | Line 1231 |
| ACFC (term 2) | `D_SBA020_ACFC_PMI` | `SHORT_D_SBA020_ACFC_PMI` | `PERDV_SWP_2` | Line 1237 |
| MWD Southern California (aggregate) | `D_MWD_PMI` (combined) | `SHORT_MWD_PMI` | `PERDV_SWP_MWD1` | Lines 1029-1044 |

**Note**: MWD aggregate combines 5 delivery nodes (PRRIS, ESB413, WSB031, ESB433, and Kern B). Additional CWS entries (Littlerock, Mojave, Castaic Lake LA, Desert, Clair Lake 2) have shortage variables but no verified PERDV mapping in DataExtraction.py.

### Canonical variable lists

The canonical M&I variable lists are maintained in `/etl/pipelines/CWS/`:

| File | Description | Count |
|------|-------------|-------|
| `CWS_delivery_variables.csv` | Core M&I delivery variables (DN_, GP_, D_) | 91 |
| `CWS_WT_delivery_variables.csv` | Water treatment plant delivery arcs | 9 |
| `CWS_shortage_variables.csv` | PMI shortage variables (SHORT_D_*_PMI) | 30 |
| `CWS_demand_calculation.csv` | **Verified** delivery/shortage/PERDV mappings for demand calculation | 16 |

### ETL output metrics for API

The ETL pipeline will calculate and load the following M&I metrics into the database:

| Metric | Units | Temporal Resolution | Coverage |
|--------|-------|---------------------|----------|
| **M&I surface water deliveries** | acre-feet | Monthly, annually | All 91 delivery variables |
| **Absolute M&I supply shortage** | acre-feet | Monthly, annually | All 30 shortage variables |
| **M&I deliveries as % of demand** | percent | Monthly, annually | **14 verified districts** (16 entries) |

#### Coverage details for percent-of-demand metric

The percent-of-demand calculation requires verified PERDV mappings. Currently verified for **14 water districts** (16 entries in `CWS_demand_calculation.csv`):

| Category | Districts |
|----------|-----------|
| **Single-term** (11) | ACWD, SCVWD, Santa Barbara, San Luis Obispo, AVEK, Palmdale, San Bernardino, San Gabriel, Gorgonio, Kern County A, Castaic Lake SVRWD |
| **Two-term** (2) | Ventura (2 delivery points), ACFC (2 delivery points) |
| **Multi-node** (1) | MWD Southern California (aggregate of 5 nodes including Kern B) |

**Not yet verified** (excluded from percent-of-demand): Littlerock, Mojave, Castaic Lake LA, Desert, Clair Lake 2

#### CWS_demand_calculation.csv file format

```csv
water_district,delivery_variable,shortage_variable,perdv_variable,calculation_type,notes,source_line
Alameda County Water District (ACWD),D_SBA029_ACWD_PMI,SHORT_D_SBA029_ACWD_PMI,PERDV_SWP_3,single,Single term calculation,DataExtraction.py:1248
```

| Column | Description |
|--------|-------------|
| `water_district` | Human-readable name of the water district |
| `delivery_variable` | CalSim3 delivery variable name (D_*_PMI) |
| `shortage_variable` | CalSim3 shortage variable name (SHORT_D_*_PMI) |
| `perdv_variable` | PERDV allocation variable (PERDV_SWP_*) |
| `calculation_type` | `single` or `two-term` |
| `notes` | Additional context (e.g., which terms to combine) |
| `source_line` | Source code reference for verification |

#### Variable types in delivery list

| Prefix | Type | Description |
|--------|------|-------------|
| `DN_` | Demand Node | Surface water delivery to demand node |
| `GP_` | Groundwater Pumping | Groundwater supply to demand unit |
| `D_` | Delivery/Diversion | Direct delivery or diversion arc |

#### Water treatment variables

These 9 variables represent specific water treatment plant delivery arcs that provide more granular tracking:

```
D_BCM003_WSPNT_NU    D_MFM007_WSPNT_NU    D_TBAUD_AMADR_NU
D_TGC003_AMADR_NU    D_WTPBNC_BNCIA       D_WTPFMH_VLLJO
D_WTPJAC_NAPA        D_WTPNBR_FRFLD       D_WTPWMN_FRFLD
```

### Back-calculating demand from delivery and shortage

CalSim3 does not directly output demand values. Instead, demand must be **back-calculated** from model outputs using the following process:

#### Why back-calculation is necessary

In CalSim3:
- **Contract demands** (e.g., Table A allocations) are model inputs, typically constant for a given land use scenario
- **Deliveries** (`D_*_PMI`) are model outputs that vary by scenario based on water availability
- **Shortages** (`SHORT_D_*_PMI`) are model outputs representing unmet demand
- **PERDV** (percent delivery) is a model output representing the allocation percentage

The relationship is:
```
Delivery + Shortage = Demand x (PERDV / 100)
```

#### Calculation steps

**Step 1: Gather variables for each CWS**

For each community water system, identify the corresponding variables from `CWS_demand_calculation.csv`:
- Delivery variable (e.g., `D_SBA029_ACWD_PMI`)
- Shortage variable (e.g., `SHORT_D_SBA029_ACWD_PMI`)
- PERDV variable (e.g., `PERDV_SWP_3`)

**Step 2: Calculate demand**

```python
# For each timestep:
demand = (delivery + shortage) / (perdv / 100)
```

Where:
- `delivery` = value from `D_*_PMI` variable (acre-feet)
- `shortage` = value from `SHORT_D_*_PMI` variable (acre-feet)
- `perdv` = value from `PERDV_SWP_*` variable (percent, 0-100)

**Step 3: Calculate percent of demand delivered**

```python
percent_of_demand = (delivery / demand) * 100
# Or equivalently:
percent_of_demand = (delivery * perdv) / (delivery + shortage)
```

#### Special cases

**Two-term calculations**: Some water districts receive water from multiple delivery points in the CalSim3 network. Each delivery point has its own PERDV allocation percentage. To calculate total district demand, each term is calculated separately and then summed:

| District | Calculation | Source |
|----------|-------------|--------|
| Ventura | `DEM_VNTRA = (D_CSTIC + SHORT_CSTIC)/PERDV_39 + (D_PYRMD + SHORT_PYRMD)/PERDV_38` | DataExtraction.py:1111-1117 |
| ACFC | `DEM_ACFC = (D_SBA009 + SHORT_SBA009)/PERDV_1 + (D_SBA020 + SHORT_SBA020)/PERDV_2` | DataExtraction.py:1231-1237 |

Why two-term? These districts have multiple physical connection points to the SWP conveyance system. Each connection operates under different allocation conditions, requiring separate PERDV values. Ventura receives water via both Castaic Lake and Pyramid Lake aqueducts. ACFC (Alameda County Flood Control) has two separate delivery arcs at nodes SBA009 and SBA020.

**Multi-node districts (MWD)**: MWD Southern California uses a combined calculation approach (DataExtraction.py lines 1029-1044):

```
D_MWD_PMI = D_PRRIS_MWDSC_PMI + D_ESB413_MWDSC_PMI + D_WSB031_MWDSC_PMI + D_ESB433_MWDSC_PMI + D_CAA194_KERNB_PMI
```

The aggregate uses `PERDV_SWP_MWD1` for demand calculation, not individual PERDV_SWP_* numbers. Note: Kern B (D_CAA194_KERNB_PMI) is included in the MWD aggregate in DataExtraction.py.

**Unverified districts**: The following delivery variables exist in CalSim3 output with shortage variables but have **NO verified PERDV mapping** in the codebase:

| District | Delivery Variable | Status |
|----------|-------------------|--------|
| Littlerock | D_ESB355_LROCK_PMI | Not implemented in DataExtraction.py |
| Mojave | D_ESB403_MOJVE_PMI | Not implemented in DataExtraction.py |
| Castaic Lake LA | D_ESB407_CCHLA_PMI | Not implemented in DataExtraction.py |
| Desert | D_ESB408_DESRT_PMI | Not implemented in DataExtraction.py |
| Clair Lake 2 | D_WSB032_CLRTA2_PMI | Not implemented in DataExtraction.py |

PERDV_SWP numbers 5-10, 12-14, 16-28, 33, 36-37 exist in CalSim3 output but have no documented mapping. The mappings would need to be verified from CalSim3 model WRESL files or DWR documentation.

#### Does demand vary by scenario?

| Component | Varies by scenario? | Notes |
|-----------|---------------------|-------|
| Contract demand (Table A) | No* | Fixed by land use scenario (L2020A) |
| Delivery (`D_*_PMI`) | **Yes** | Depends on water availability and operations |
| Shortage (`SHORT_D_*_PMI`) | **Yes** | Unmet portion of demand |
| PERDV (% allocation) | **Yes** | Allocation percentage varies with conditions |
| Back-calculated demand | **Yes** | Derived from outputs, will show scenario variation |

*Contract demands only change if the land use scenario changes (e.g., L2020A vs L2040).

#### Table A contract demand evidence

From `DataExtraction.py` (lines 914-920), contract demands are treated as fixed constants for a given land use scenario:

```python
# MWD Table A contract (1911.5 TAF/year, fixed for L2020A)
MWD_yearly_taf_value = 1911.5
demands_df[('MANUAL-ADD','TABLEA_CONTRACT_MWD','URBAN-DEMAND','1MON','L2020A','PER-CUM','TAF')] = \
    len(demands_df) * [MWD_yearly_taf_value/12]
```

1. **Contract amounts are model inputs**, not outputs
2. **Values are constant across all timesteps** within a scenario (`len(demands_df)*[value]`)
3. **Land use scenario determines contract level** (tagged as `L2020A`)

Table A contracts represent the maximum annual water entitlement each SWP contractor is entitled to request. The actual delivery may be less based on water availability (reflected in PERDV and shortage variables).

### Source reference

Variable mappings derived from:
- `COEQWAL_repo/coeqwal/notebooks/coeqwalpackage/DataExtraction.py` (lines 1061-1330)
- `/etl/pipelines/MI_variable_list_comparison.md` (list comparison analysis)

---

## Agricultural demand units (AG)

### Variable naming conventions

| Metric | Variable pattern | Example | Source | Units |
|--------|-----------------|---------|--------|-------|
| Applied water demand | `AW_{DU_ID}` | `AW_02_PA1` | DV | CFS |
| Surface water delivery | `DN_{DU_ID}` | `DN_02_PA1` | DV | CFS |
| Groundwater pumping | `GP_{DU_ID}` | `GP_02_PA1` | DV | CFS |
| GW restriction shortage | `GW_SHORT_{DU_ID}` | `GW_SHORT_50_PU` | DV | CFS |

All CFS values converted to TAF via `CFS x DaysInMonth x 0.001983471`.

### Shortage calculation

AG shortage = `max(demand - delivery, 0)` computed in the ETL.

**Important**: `GW_SHORT_*` variables track groundwater pumping restriction shortages
(SGMA-related), not total delivery shortage. They are only defined for San Joaquin River
basin DUs and only in scenarios with SGMA groundwater limits enabled.

### AG aggregates

AG demand units roll up to predefined aggregate regions (CVP North, CVP South, SWP North, SWP South, etc.) using the `ag_aggregate_entity` table. Aggregate statistics are sums of their constituent DU statistics.

### Source reference

Variable lists derived from:
- `COEQWAL_V3/notebooks/variable_groupings.csv` (AG mapping)
- `COEQWAL_V3/notebooks/coeqwalpackage/DataExtraction.py`
- `/database/seed_tables/04_calsim_data/ag_du_entity.csv`

---

## Reservoir variables

### Storage

| Variable | Description | Units |
|----------|-------------|-------|
| `S_{CODE}` | End-of-month storage | TAF |
| `S_{CODE}LEVELxDV` | Storage zone x (x = 1-6) | TAF |

The highest zone (5 or 6 depending on reservoir) represents capacity.

### Spill

| Variable | Description | Units |
|----------|-------------|-------|
| `C_{CODE}_FLOOD` | Flood release (spill) | CFS |

Spill is converted to TAF via `CFS x DaysInMonth x 0.001983471`.

### Key capacity values

| Reservoir | Code | Capacity (TAF) | Capacity variable |
|-----------|------|----------------|-------------------|
| Folsom | `FOLSM` | 967.0 | `S_FOLSMLEVEL6DV` (hardcoded) |
| Millerton | `MLRTN` | 524.0 | `S_MLRTNLEVEL5DV` (hardcoded) |
| Oroville | `OROVL` | 3424.8 | `S_OROVLLEVEL6DV` (hardcoded) |
| New Melones | `MELON` | 2420.0 | `S_MELONLEVEL5DV` (hardcoded) |
| Shasta | `SHSTA` | 4552.0 | `S_SHSTALEVEL5DV` (from DV) |
| Trinity | `TRNTY` | 2447.7 | `S_TRNTYLEVEL5DV` (from DV) |

See `CAPACITY_OVERRIDES` in `etl/statistics/reservoirs/calculate_reservoir_statistics.py`.

---

## Statistics ETL

The statistics ETL calculates derived metrics from CalSim scenario output CSVs and loads them into the database for API consumption.

### Consolidated runner

Use `run_all.py` to run all statistics modules for a scenario:

```bash
cd etl/statistics

# Run all statistics for a single scenario
python run_all.py --scenario s0020

# Dry run (calculate but don't write to DB)
python run_all.py --scenario s0020 --dry-run --continue-on-error

# Run only specific modules
python run_all.py --scenario s0020 --only reservoirs,du_urban

# Run all 76 scenarios with 4 parallel workers + sensitivity analysis
python run_all.py --all-scenarios --workers 4 --continue-on-error --with-sensitivity

# Full production run with logging (recommended)
tmux new -s etl
python run_all.py \
  --all-scenarios --workers 4 --continue-on-error --with-sensitivity \
  2>&1 | tee stats_run_$(date +%Y%m%d).log

# List available modules
python run_all.py --list-modules
```

**After a run completes**, `run_all.py` automatically:
- Writes a structured **audit CSV** (`etl/statistics/output/stats_audit_YYYYMMDD_HHMMSS.csv`, override with `--audit-dir`) with one row per (scenario, module) including status and timing
- Prints a **scorecard** showing success/failure per scenario x module
- Runs **DB row-count verification** across all 18 statistics tables (non-dry-run only)

**EC2 sizing:** Each worker loads a ~300 MB CSV into memory. Recommended:
- `--workers 1`: t3.medium (4 GB) - ~8 hours for 76 scenarios
- `--workers 4`: t3.xlarge (16 GB) - ~2-3 hours for 76 scenarios
- `--workers 8`: **t3a.2xlarge (8 vCPU, 32 GB) - current production choice for the multi-threaded scenario ETL + DB upload.** Headroom for pandas spikes on the DCP/DWRadapt25 scenarios and fast enough to finish the full 76-scenario run in a single Cloud9 session.

**Cloud9 timeout:** Set "Stop my environment" to 4+ hours in Cloud9 Preferences before a full run. Use `tmux` so browser disconnects don't kill the process. Detach with `Ctrl+B d`, reattach with `tmux attach -t etl`.

> [!IMPORTANT]
> **Stop the EC2 instance when you're done.** `t3a.2xlarge` is not free when idle. After your run finishes (and after you've copied off any logs you care about), stop the Cloud9 environment so the underlying EC2 instance is shut down:
> - Cloud9 UI: **File > Quit Cloud9** or set "Stop my environment" to the shortest timeout (30 min) so it auto-stops.
> - Or from the EC2 console: **Instances > select the Cloud9 instance > Instance state > Stop instance**.
> Stopped instances preserve their EBS volume (so state and configs are kept) but don't accrue compute charges. Verify with `aws ec2 describe-instances --query 'Reservations[].Instances[].[InstanceId,State.Name]'` or the EC2 console before logging off.

### Modules (run in order)

| Order | Module | Script | Database Tables |
|-------|--------|--------|-----------------|
| 1 | **reservoirs** | `main.py` | `reservoir_monthly_percentile`, `reservoir_storage_monthly`, `reservoir_spill_monthly`, `reservoir_period_summary` |
| 2 | **du_urban** | `du_urban/main.py` | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |
| 3 | **mi** | `mi/main.py` | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |
| 4 | **cws_aggregate** | `cws_aggregate/main.py` | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |
| 5 | **ag** | `ag/main.py` | `ag_du_demand_monthly`, `ag_du_sw_delivery_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary`, `ag_aggregate_monthly`, `ag_aggregate_period_summary` |
| 6 | **refuge** | `refuge/main.py` | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` |
| 7 | **env_flows** | `env_flows/main.py` | `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary` |
| 8 | **delta** | `delta/main.py` | `delta_monthly`, `delta_period_summary` |
| *post* | **sensitivity** | `sensitivity/calculate_sensitivity.py` | `sensitivity_climate`, `sensitivity_operational` |

### Unit conversion rules

CalSim DV output variables are typically in **CFS** (cubic feet per second) as monthly averages. Most ETL modules convert flow and volume variables to **TAF** (thousand acre-feet) before storing statistics, but not all metrics use TAF. Each module reads the unit declared in row 6 of the CSV header and applies the appropriate conversion.

**Stored units by module:**

| Module | Metric type | Stored unit | Notes |
|--------|------------|-------------|-------|
| **Reservoirs** | Storage | TAF | Natively TAF in CalSim (no conversion needed) |
| **Urban DU** | Delivery, shortage | TAF | Converted from CFS |
| **M&I Contractors** | Delivery, shortage | TAF | Converted from CFS; PERDV fractions are dimensionless |
| **CWS Aggregates** | Delivery, shortage | TAF | Converted from CFS |
| **AG** | Demand, delivery, pumping, shortage | TAF | Converted from CFS |
| **Refuge** | Delivery, shortage | TAF | Converted from CFS |
| **Env Flows** | Flow volume (`flow_avg_taf`) | TAF | Converted from CFS |
| **Env Flows** | Flow percentiles | CFS | Stored in native CFS |
| **Env Flows** | `pct_unimpaired`, `pct_ff`, `alteration_index` | Dimensionless | Ratios and correlation coefficients |
| **Delta** | Net Delta Outflow (NDO) | TAF | Converted from CFS |
| **Delta** | X2 position | KM | Stored in native kilometers |
| **Delta** | EC at compliance/pumping stations | UMHOS/CM | Stored in native electrical conductivity units |
| **Sensitivity** | All | Inherited | Reads pre-aggregated values from other tables; no conversion |

**Conversion factor** (defined once in `units.py`):

```
CFS_TO_TAF_PER_DAY = 86400 / 43560000    (approximately 0.001983471)
TAF = CFS x DaysInMonth x CFS_TO_TAF_PER_DAY
```

This is the exact form of the COEQWAL notebook formula `CFS x 0.001984 x days_in_month` (from `coeqwalpackage/metrics.py` and `cqwlutils.py`). The notebook rounds to 0.001984; the ETL uses the full-precision value. The difference is ~0.027%, which is negligible.

**Per-module conversion behavior:**

| Module | CSV source | Variables converted | Input unit | Notes |
|--------|-----------|-------------------|------------|-------|
| **reservoirs** | DV | None (validates S_* are TAF) | TAF | CalSim storage output is natively TAF |
| **du_urban** | DV + SV | DL_*, D_*_PMI, DN_*, GP_*, DEL_*, SHORT_*, SHRTG_* | CFS | SV demand (UD_*) is already TAF |
| **mi** | DV + SV | D_*_PMI, DEL_*, SHORT_* | CFS | PERDV_* fractions left dimensionless; MWD uses Table A constant |
| **cws_aggregate** | DV | DEL_*, SHORT_*, nod/sod splits | CFS | Falls back to "assume CFS" if unit header is missing |
| **ag** | DV | AW_*, DN_*, GP_*, SHRTG_*, GW_SHORT_*, DEL_*, SHORT_* | CFS | TAF columns passed through without conversion |
| **refuge** | DV | AW_*, DN_*, SHRTG_*, GW_SHORT_* | CFS | Same pattern as ag |
| **env_flows** | DV + SV | C_*, C_*_MIF (DV flows) | CFS | SV columns in TAF are reverse-converted to CFS for ratio consistency |
| **delta** | DV | NDO only | CFS | EC (UMHOS/CM) and X2 (KM) left in native units |
| **sensitivity** | DB | None | N/A | Reads pre-aggregated statistics from database |

**Safeguards** (all in `units.py`):

- `check_post_conversion_magnitude`: flags any column exceeding 2000 TAF/month after conversion (likely double-conversion)
- `validate_water_balance`: checks GP vs AW ratio for ag DUs (GP > 1.15x AW is suspicious)
- `compute_cv`: caps coefficient of variation at 99.0 and returns 0.0 when mean is near zero (prevents NUMERIC overflow)
- `safe_pct`: warns when computed percentages exceed 200% (possible unit mismatch)

### Notebook alignment audit (March 2026)

The ETL was audited module-by-module against the COEQWAL Jupyter notebooks (`coeqwalpackage/metrics.py`, `DataExtraction.py`, `Metrics.ipynb`, tier assignment notebooks). Results:

**Verified correct (no changes needed):**
- CFS-to-TAF factor matches notebooks (ETL uses exact `86400/43560000`, notebooks round to `0.001984`)
- AG demand variable (`AW_*`), NOD/SOD aggregate components, all PERDV mappings, MWD Table A constant
- Reservoir flood/dead pool logic (epsilon trick, threshold constants)
- Delta X2 and EC seasonal groupings (Fall=9,10,11; Spring=3,4,5)
- Shortage clipping, CV safeguards, reliability formulas

**Fixes applied during audit:**
- **San Luis flood levels**: SLUIS_CVP, SLUIS_SWP, SLUIS changed from LEVEL5DV (capacity) to LEVEL4DV (flood control). Rule: flood = one level below capacity. Matches `Tier_Assignment_Storage.ipynb`.
- **CVP North M&I variable**: CWS aggregate changed from `DEL_CVP_PMI_N` to `DEL_CVP_PMI_N_WAMER` to match `DataExtraction.py`. Includes Western Area deliveries. Falls back to `DEL_CVP_PMI_N` with a warning if `_WAMER` is not in the CSV.
- **KERN contractor scope**: Removed `D_CAA194_KERNB_PMI` from KERN in MI module. KERNB is a MWD allocation (notebook only has KERNA under Kern County WA).
- **GW-only AG DU delivery**: 18 DUs now synthesize delivery as GP + RU (matching `DataExtraction.py`). Previously skipped because `DN_*` is absent from WRESL for these DUs.
- **Env flows CV consistency**: `_safe_cv` now returns 99.0 (capped) instead of None, matching `compute_cv()` in `units.py`.
- **Dead code removed**: Deleted orphaned `du/calculate_du_statistics.py` (not wired into `run_all.py`, no unit conversion).

**Intentional design differences from notebooks:**
- ETL uses water year (Oct-Sep) for all annualization. Notebooks use contract year (Mar-Feb) for some delivery totals. Long-run averages are nearly identical.
- ETL computes DU-level AG shortage from `SHRTG_*`/`GW_SHORT_*`. Notebooks do not compute DU-level shortage.
- ETL computes many metrics the notebooks don't: shortage frequency, exceedance percentiles, env flow alteration indices, CEFF seasonal aggregation, 60-channel and 92-reservoir coverage.

### Cross-scenario sensitivity analysis

After per-scenario statistics are computed, an optional post-processing step computes
how sensitive each metric is to **climate change** and to **changes in operations**.

```bash
# Standalone
cd etl/statistics
python sensitivity/calculate_sensitivity.py

# Or as part of the consolidated runner
python run_all.py --all-scenarios --with-sensitivity

# Dry run
python sensitivity/calculate_sensitivity.py --dry-run

# Only specific modules
python sensitivity/calculate_sensitivity.py --only reservoir,ag
```

**Climate sensitivity** (`sensitivity_climate` table): For each hydroclimate sibling
group (scenarios with identical operations but different climate: historical, cc50, cc95),
measures how each metric changes. Stored per (sibling_group, entity, metric, water_month):
- `hist_value`, `cc50_value`, `cc95_value`
- `cc50_abs_change`, `cc95_abs_change` (absolute difference from historical)
- `cc50_pct_change`, `cc95_pct_change` (percent change from historical)

**Operational sensitivity** (`sensitivity_operational` table): For each hydroclimate
level (e.g. all historical-hydrology scenarios), measures how each metric varies across
different operational configurations. Stored per (hydroclimate_id, entity, metric, water_month):
- `scenario_count`, `min_value`, `max_value`, `mean_value`, `std_value`
- `range_value` (max - min), `pct_range` ((max - min) / |mean| x 100)

Both tables include `water_month` 1-12 (monthly resolution) and 0 (annual/period-of-record),
covering reservoirs, AG, urban DU, MI, CWS, refuge, env flows, and delta metrics.

**How it works:**

The 74 active scenarios form a matrix of 24 operational configurations x 3 hydroclimate levels (historical, CC50, CC95). Each operational configuration has a "sibling group" of 3 scenarios that share the same rules but differ in climate. Climate sensitivity holds operations constant and varies climate across the sibling group. Operational sensitivity holds climate constant (e.g., historical only) and compares across all 24 operational configurations.

Only active scenarios (`is_active = TRUE`) are included in the analysis.

**Metrics in the sensitivity tables (March 2026):**

The sensitivity calculation extracts these period-level metrics (stored as `water_month = 0`):

| Module | metric_name values | Source table |
|--------|-------------------|--------------|
| **ag** | `annual_demand_avg`, `annual_sw_delivery_avg`, `annual_shortage_avg`, `reliability` | `ag_du_period_summary` |
| **du_urban** | `annual_delivery_avg`, `annual_shortage_avg`, `annual_demand_avg`, `reliability` | `du_period_summary` |
| **mi** | `annual_delivery_avg`, `annual_shortage_avg`, `annual_demand_avg`, `reliability` | `mi_contractor_period_summary` |
| **cws_aggregate** | `annual_delivery_avg`, `annual_shortage_avg`, `annual_demand_avg`, `reliability` | `cws_aggregate_period_summary` |
| **refuge** | `annual_delivery_avg`, `annual_shortage_avg`, `reliability` | `refuge_du_period_summary` |
| **reservoir** | `storage_avg` (monthly only) | `reservoir_storage_monthly` |
| **env_flows** | `annual_pct_unimpaired`, `annual_pct_ff` (period); `flow_avg_taf`, `flow_avg_cfs`, `pct_unimpaired` (monthly) | `env_flow_channel_*` |
| **delta** | `avg_{variable_code}` (data-driven; monthly + annual) | `delta_monthly` |

If the sensitivity tables are missing `annual_demand_avg` rows for du_urban/mi/cws_aggregate, or missing `water_month = 0` rows for delta, re-run the sensitivity calculation after pulling the latest code:

```bash
python sensitivity/calculate_sensitivity.py 2>&1 | tee ~/sensitivity_rerun.log
```

**Resilience analysis queries**

Three questions:

1. **Which entities are most vulnerable to climate change?** Which deliveries, reservoirs, ecosystems, and Delta conditions swing the most across the range of possible climates (historical, CC50, CC95)? (Queries 1a-1d per entity; 4a per sector; 6a/6b per metric; 7a/7b named entity lists for reporting)
2. **Which entities are most sensitive to operational choices?** Under a given climate, which entities change the most depending on how the system is operated? (Queries 2a-2d per entity; 4b per sector; 6c/6d per metric)
3. **Which operations best protect vulnerable entities?** For climate-vulnerable entities, which operational configuration keeps their outcomes most stable? Answered per sector (5a deliveries, 5b reservoirs, 5c env flows, 5e Delta), system-wide (5d), and by metric (6c Part 2).

**Measuring resilience without hiding small systems**

Resilience means stability: an entity is resilient if its outcomes hold up across the range of possible climates, and vulnerable if they swing wildly. The measure is the **spread across all three climates**.

The question is how to normalize that spread so you can compare a large system (1000 TAF/year) to a small one (5 TAF/year). A simple percentage, spread / mean * 100, breaks down for entities near zero: a system delivering 0.01 TAF that fluctuates to 0.02 TAF shows a 100% swing despite negligible absolute volume. Instead, the queries below normalize by demand or capacity rather than by the metric itself:

| Module | Spread numerator | Normalizer (denominator) | Interpretation |
|--------|-----------------|--------------------------|----------------|
| DU_urban, MI, CWS, Refuge | Delivery spread across 3 climates | Historical demand (delivery + shortage) | "What % of this entity's water need is at stake?" |
| AG | SW delivery spread across 3 climates | Historical demand (AW, a direct metric) | "What % of ag water need is at stake?" |
| Reservoirs | Storage spread across 3 climates | Reservoir capacity (from `reservoir_entity`) | "What % of usable storage swings with climate?" |
| Env Flows | pct_unimpaired / pct_ff already percentages | None needed | Spread is in percentage points |
| Delta -- NDO | Outflow spread across 3 climates | Historical outflow (always large/positive) | "What % of Delta outflow swings with climate?" |
| Delta -- X2 | X2 position spread across 3 climates | Historical X2 (always 60-90 KM) | "By what % does salt intrusion shift with climate?" |
| Delta -- EC (salinity) | EC spread across 3 climates | Historical EC (always measurable) | "By what % does salinity change at this station?" |

Demand and capacity are always positive, so there is no near-zero denominator problem. Small systems are not filtered out. They rank alongside large ones.

Notes on demand computation:
- For ag, du_urban, mi, and cws_aggregate: `annual_demand_avg` is a direct metric in the sensitivity table, pulled from each module's period summary. This is the actual model demand (from SV input for DU_urban, PERDV/Table A for MI, applied water for AG), not a proxy.
- For refuge: `refuge_du_period_summary` does not store a demand column. Demand is computed as delivery + shortage from the sensitivity table, which is exact for CalSim refuge DUs.
- All queries use `water_month = 0` (annual / period-of-record values) and average across all 24 sibling groups to dilute operational outliers.

---

**Query 1a. Climate vulnerability for water deliveries (demand-normalized)**

Which delivery entities swing the most across climates, as a fraction of their demand?

```sql
WITH delivery AS (
  SELECT sibling_group, module, entity_id,
         hist_value, cc50_value, cc95_value,
         GREATEST(hist_value, cc50_value, cc95_value)
           - LEAST(hist_value, cc50_value, cc95_value) AS spread
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name IN ('annual_delivery_avg', 'annual_sw_delivery_avg')
    AND hist_value IS NOT NULL
    AND cc50_value IS NOT NULL
    AND cc95_value IS NOT NULL
),
demand AS (
  -- ag, du_urban, mi, cws_aggregate: direct demand metric from sensitivity table
  SELECT sibling_group, module, entity_id, hist_value AS hist_demand
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name = 'annual_demand_avg'
    AND module IN ('ag', 'du_urban', 'mi', 'cws_aggregate')
  UNION ALL
  -- refuge: no stored demand column; demand = delivery + shortage
  SELECT del.sibling_group, del.module, del.entity_id,
         del.hist_value + COALESCE(sh.hist_value, 0) AS hist_demand
  FROM sensitivity_climate del
  LEFT JOIN sensitivity_climate sh
    ON del.sibling_group = sh.sibling_group
   AND del.module = sh.module
   AND del.entity_id = sh.entity_id
   AND sh.water_month = 0
   AND sh.metric_name = 'annual_shortage_avg'
  WHERE del.water_month = 0
    AND del.metric_name = 'annual_delivery_avg'
    AND del.module = 'refuge'
),
entity_name AS (
  SELECT short_code AS eid, contractor_name AS name FROM mi_contractor
  UNION ALL
  SELECT network_arc_id, name FROM channel_entity
)
SELECT d.module, d.entity_id,
       en.name AS entity_name,
       ROUND(AVG(dm.hist_demand)::numeric, 1) AS avg_demand_taf,
       ROUND(AVG(d.spread)::numeric, 1)        AS avg_spread_taf,
       ROUND(AVG(
         d.spread / NULLIF(dm.hist_demand, 0) * 100
       )::numeric, 1) AS pct_demand_at_risk
FROM delivery d
JOIN demand dm
  ON d.sibling_group = dm.sibling_group
 AND d.module = dm.module
 AND d.entity_id = dm.entity_id
LEFT JOIN entity_name en ON d.entity_id = en.eid
GROUP BY d.module, d.entity_id, en.name
HAVING AVG(dm.hist_demand) > 0
ORDER BY pct_demand_at_risk DESC
LIMIT 30;
```

The `entity_name` column is populated for MI contractors (e.g., "Kern County Water Agency") and env_flow channels (e.g., "Sacramento R below Shasta"). For ag, du_urban, refuge, and cws_aggregate, the `entity_id` itself is the identifier (e.g., "02_NA", "FRFLD", "SWP_total"). For **most resilient** (least vulnerable), change `ORDER BY pct_demand_at_risk ASC`.

**Query 1b. Climate vulnerability for reservoirs (capacity-normalized)**

Which reservoirs have the largest storage swing as a fraction of capacity?

```sql
SELECT sc.entity_id,
       re.short_code,
       re.name,
       ROUND(re.capacity_taf::numeric, 0) AS capacity_taf,
       ROUND(AVG(sc.hist_value)::numeric, 1) AS avg_hist_storage,
       ROUND(AVG(
         GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
         - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value)
       )::numeric, 1) AS avg_spread_taf,
       ROUND(AVG(
         (GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
          - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value))
         / NULLIF(re.capacity_taf, 0) * 100
       )::numeric, 1) AS pct_capacity_at_risk
FROM sensitivity_climate sc
JOIN reservoir_entity re ON sc.entity_id = re.id::text
WHERE sc.water_month = 0
  AND sc.module = 'reservoir'
  AND sc.metric_name = 'storage_avg'
  AND sc.hist_value IS NOT NULL
  AND sc.cc50_value IS NOT NULL
  AND sc.cc95_value IS NOT NULL
GROUP BY sc.entity_id, re.short_code, re.name, re.capacity_taf
ORDER BY pct_capacity_at_risk DESC;
```

**Query 1c. Climate vulnerability -- environmental flows**

`pct_unimpaired` and `pct_ff` are already expressed as percentages of unimpaired/functional flow. The spread across climates is in percentage points and directly meaningful -- no normalizer needed.

```sql
SELECT sc.entity_id, ce.name AS channel_name, sc.metric_name,
       ROUND(AVG(sc.hist_value)::numeric, 1) AS avg_hist_pct,
       ROUND(AVG(sc.cc50_value)::numeric, 1) AS avg_cc50_pct,
       ROUND(AVG(sc.cc95_value)::numeric, 1) AS avg_cc95_pct,
       ROUND(AVG(
         GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
         - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value)
       )::numeric, 1) AS avg_spread_points
FROM sensitivity_climate sc
LEFT JOIN channel_entity ce ON sc.entity_id = ce.network_arc_id
WHERE sc.water_month = 0
  AND sc.module = 'env_flows'
  AND sc.metric_name IN ('annual_pct_unimpaired', 'annual_pct_ff')
  AND sc.hist_value IS NOT NULL
  AND sc.cc50_value IS NOT NULL
  AND sc.cc95_value IS NOT NULL
GROUP BY sc.entity_id, ce.name, sc.metric_name
ORDER BY avg_spread_points DESC
LIMIT 30;
```

**Query 1d. Climate vulnerability -- Delta metrics (outflow, X2, salinity)**

Delta metrics each have different units and different "worse" directions, so they are listed separately. NDO (outflow) is in TAF -- lower is worse. X2 (salinity intrusion) is in KM -- higher means salt moves further inland. EC (salinity) is in UMHOS/CM -- higher is worse. Normalization uses the historical value as denominator, which is safe because these metrics are always large/positive.

```sql
SELECT entity_id,
       CASE entity_id
         WHEN 'ndo'       THEN 'Net Delta Outflow'
         WHEN 'x2'        THEN 'X2 Position (2 ppt isohaline)'
         WHEN 'em_ec'     THEN 'Emmaton EC'
         WHEN 'jp_ec'     THEN 'Jersey Point EC'
         WHEN 'rs_ec'     THEN 'Rock Slough EC'
         WHEN 'co_ec'     THEN 'Collinsville EC'
         WHEN 'banks_ec'  THEN 'Banks Pumping Plant EC'
         WHEN 'tracy_ec'  THEN 'Tracy Pumping Plant EC'
         ELSE entity_id
       END AS entity_name,
       unit,
       ROUND(AVG(hist_value)::numeric, 2) AS avg_hist,
       ROUND(AVG(cc50_value)::numeric, 2) AS avg_cc50,
       ROUND(AVG(cc95_value)::numeric, 2) AS avg_cc95,
       ROUND(AVG(
         GREATEST(hist_value, cc50_value, cc95_value)
         - LEAST(hist_value, cc50_value, cc95_value)
       )::numeric, 2) AS avg_spread,
       ROUND(AVG(
         (GREATEST(hist_value, cc50_value, cc95_value)
          - LEAST(hist_value, cc50_value, cc95_value))
         / NULLIF(ABS(hist_value), 0) * 100
       )::numeric, 1) AS pct_spread
FROM sensitivity_climate
WHERE water_month = 0
  AND module = 'delta'
  AND hist_value IS NOT NULL
  AND cc50_value IS NOT NULL
  AND cc95_value IS NOT NULL
GROUP BY entity_id, unit
ORDER BY pct_spread DESC;
```

With only 8 delta variables, no LIMIT is needed. For NDO, a negative trend (cc95 < hist) means less Delta outflow under hot/dry climate. For EC stations, a positive trend (cc95 > hist) means saltier water at that compliance point.

---

**Query 2a. Operational vulnerability for water deliveries (demand-normalized)**

Under historical climate, which entities vary the most across operational configurations, as a fraction of demand?

```sql
WITH del AS (
  SELECT module, entity_id,
         mean_value AS del_mean,
         range_value AS del_range
  FROM sensitivity_operational
  WHERE hydroclimate_id = 2
    AND water_month = 0
    AND metric_name IN ('annual_delivery_avg', 'annual_sw_delivery_avg')
),
demand AS (
  -- ag, du_urban, mi, cws_aggregate: direct demand metric
  SELECT module, entity_id, mean_value AS demand_mean
  FROM sensitivity_operational
  WHERE hydroclimate_id = 2
    AND water_month = 0
    AND metric_name = 'annual_demand_avg'
    AND module IN ('ag', 'du_urban', 'mi', 'cws_aggregate')
  UNION ALL
  -- refuge: demand = mean delivery + mean shortage
  SELECT del_op.module, del_op.entity_id,
         del_op.mean_value + COALESCE(sh_op.mean_value, 0) AS demand_mean
  FROM sensitivity_operational del_op
  LEFT JOIN sensitivity_operational sh_op
    ON del_op.module = sh_op.module
   AND del_op.entity_id = sh_op.entity_id
   AND sh_op.hydroclimate_id = del_op.hydroclimate_id
   AND sh_op.water_month = 0
   AND sh_op.metric_name = 'annual_shortage_avg'
  WHERE del_op.hydroclimate_id = 2
    AND del_op.water_month = 0
    AND del_op.metric_name = 'annual_delivery_avg'
    AND del_op.module = 'refuge'
),
entity_name AS (
  SELECT short_code AS eid, contractor_name AS name FROM mi_contractor
  UNION ALL
  SELECT network_arc_id, name FROM channel_entity
)
SELECT d.module, d.entity_id,
       en.name AS entity_name,
       ROUND(dm.demand_mean::numeric, 1)  AS demand_taf,
       ROUND(d.del_range::numeric, 1)     AS op_range_taf,
       ROUND(
         d.del_range / NULLIF(dm.demand_mean, 0) * 100
       ::numeric, 1) AS pct_demand_at_risk
FROM del d
JOIN demand dm
  ON d.module = dm.module
 AND d.entity_id = dm.entity_id
LEFT JOIN entity_name en ON d.entity_id = en.eid
WHERE dm.demand_mean > 0
ORDER BY pct_demand_at_risk DESC
LIMIT 30;
```

For **most resilient to operations**, change `ORDER BY pct_demand_at_risk ASC`.

**Query 2b. Operational vulnerability for reservoirs (capacity-normalized)**

```sql
SELECT so.entity_id,
       re.short_code,
       re.name,
       ROUND(re.capacity_taf::numeric, 0) AS capacity_taf,
       ROUND(so.range_value::numeric, 1)  AS op_range_taf,
       ROUND(
         so.range_value / NULLIF(re.capacity_taf, 0) * 100
       ::numeric, 1) AS pct_capacity_at_risk
FROM sensitivity_operational so
JOIN reservoir_entity re ON so.entity_id = re.id::text
WHERE so.hydroclimate_id = 2
  AND so.water_month = 0
  AND so.module = 'reservoir'
  AND so.metric_name = 'storage_avg'
ORDER BY pct_capacity_at_risk DESC;
```

**Query 2c. Operational vulnerability -- environmental flows**

Under historical climate, which channels vary the most across operational configurations? Since pct_unimpaired and pct_ff are already percentages, the range is in percentage points.

```sql
SELECT so.entity_id, ce.name AS channel_name, so.metric_name,
       ROUND(so.mean_value::numeric, 1) AS mean_pct,
       ROUND(so.min_value::numeric, 1)  AS min_pct,
       ROUND(so.max_value::numeric, 1)  AS max_pct,
       ROUND(so.range_value::numeric, 1) AS op_range_points
FROM sensitivity_operational so
LEFT JOIN channel_entity ce ON so.entity_id = ce.network_arc_id
WHERE so.hydroclimate_id = 2
  AND so.water_month = 0
  AND so.module = 'env_flows'
  AND so.metric_name IN ('annual_pct_unimpaired', 'annual_pct_ff')
ORDER BY op_range_points DESC
LIMIT 30;
```

**Query 2d. Operational vulnerability -- Delta metrics**

Under historical climate, how much do Delta outflow, X2, and salinity change across operational configurations?

```sql
SELECT entity_id,
       CASE entity_id
         WHEN 'ndo'       THEN 'Net Delta Outflow'
         WHEN 'x2'        THEN 'X2 Position (2 ppt isohaline)'
         WHEN 'em_ec'     THEN 'Emmaton EC'
         WHEN 'jp_ec'     THEN 'Jersey Point EC'
         WHEN 'rs_ec'     THEN 'Rock Slough EC'
         WHEN 'co_ec'     THEN 'Collinsville EC'
         WHEN 'banks_ec'  THEN 'Banks Pumping Plant EC'
         WHEN 'tracy_ec'  THEN 'Tracy Pumping Plant EC'
         ELSE entity_id
       END AS entity_name,
       unit,
       ROUND(mean_value::numeric, 2) AS mean_val,
       ROUND(min_value::numeric, 2)  AS min_val,
       ROUND(max_value::numeric, 2)  AS max_val,
       ROUND(range_value::numeric, 2) AS op_range,
       ROUND(range_value / NULLIF(ABS(mean_value), 0) * 100::numeric, 1)
         AS pct_range
FROM sensitivity_operational
WHERE hydroclimate_id = 2
  AND water_month = 0
  AND module = 'delta'
ORDER BY pct_range DESC;
```

Compare 1d and 2d: if NDO has a large `pct_spread` in 1d but small `pct_range` in 2d, Delta outflow is driven by climate, not operations. If a salinity station has a large `pct_range` in 2d, operational choices significantly affect water quality there -- that is a policy lever.

---

**Query 4a. Cross-sector comparison -- which sectors are most at risk from climate?**

Aggregates per-entity climate vulnerability into one row per module. Answers: are refuges more at risk than CWS? Are environmental flows more climate-sensitive than ag?

```sql
WITH delivery AS (
  SELECT sibling_group, module, entity_id,
         GREATEST(hist_value, cc50_value, cc95_value)
           - LEAST(hist_value, cc50_value, cc95_value) AS spread
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name IN ('annual_delivery_avg', 'annual_sw_delivery_avg')
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
),
demand AS (
  SELECT sibling_group, module, entity_id, hist_value AS hist_demand
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name = 'annual_demand_avg'
    AND module IN ('ag', 'du_urban', 'mi', 'cws_aggregate')
  UNION ALL
  SELECT del.sibling_group, del.module, del.entity_id,
         del.hist_value + COALESCE(sh.hist_value, 0) AS hist_demand
  FROM sensitivity_climate del
  LEFT JOIN sensitivity_climate sh
    ON del.sibling_group = sh.sibling_group
   AND del.module = sh.module AND del.entity_id = sh.entity_id
   AND sh.water_month = 0 AND sh.metric_name = 'annual_shortage_avg'
  WHERE del.water_month = 0 AND del.metric_name = 'annual_delivery_avg'
    AND del.module = 'refuge'
),
entity_risk AS (
  -- Delivery modules: spread / demand
  SELECT d.module, d.entity_id,
         AVG(d.spread / NULLIF(dm.hist_demand, 0) * 100) AS pct_at_risk
  FROM delivery d
  JOIN demand dm ON d.sibling_group = dm.sibling_group
   AND d.module = dm.module AND d.entity_id = dm.entity_id
  WHERE dm.hist_demand > 0
  GROUP BY d.module, d.entity_id
  UNION ALL
  -- Reservoirs: spread / capacity
  SELECT 'reservoir', sc.entity_id,
         AVG((GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
              - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value))
             / NULLIF(re.capacity_taf, 0) * 100)
  FROM sensitivity_climate sc
  JOIN reservoir_entity re ON sc.entity_id = re.id::text
  WHERE sc.water_month = 0 AND sc.module = 'reservoir' AND sc.metric_name = 'storage_avg'
    AND sc.hist_value IS NOT NULL AND sc.cc50_value IS NOT NULL AND sc.cc95_value IS NOT NULL
  GROUP BY sc.entity_id
  UNION ALL
  -- Env flows: percentage-point spread (already normalized)
  SELECT 'env_flows', entity_id,
         AVG(GREATEST(hist_value, cc50_value, cc95_value)
             - LEAST(hist_value, cc50_value, cc95_value))
  FROM sensitivity_climate
  WHERE water_month = 0 AND module = 'env_flows'
    AND metric_name IN ('annual_pct_unimpaired', 'annual_pct_ff')
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
  GROUP BY entity_id
  UNION ALL
  -- Delta: spread / historical (simple % -- safe because values are always large)
  SELECT 'delta', entity_id,
         AVG(
           (GREATEST(hist_value, cc50_value, cc95_value)
            - LEAST(hist_value, cc50_value, cc95_value))
           / NULLIF(ABS(hist_value), 0) * 100
         )
  FROM sensitivity_climate
  WHERE water_month = 0 AND module = 'delta'
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
  GROUP BY entity_id
)
SELECT module,
       COUNT(*) AS entity_count,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_at_risk)::numeric, 1)
         AS median_pct_at_risk,
       ROUND(AVG(pct_at_risk)::numeric, 1)   AS mean_pct_at_risk,
       ROUND(MAX(pct_at_risk)::numeric, 1)    AS worst_entity_pct,
       ROUND(MIN(pct_at_risk)::numeric, 1)    AS best_entity_pct
FROM entity_risk
GROUP BY module
ORDER BY median_pct_at_risk DESC;
```

The sector with the highest `median_pct_at_risk` is the most climate-vulnerable overall. Delta appears as its own row so you can compare it directly against deliveries, storage, and environmental flows.

**Query 4b. Cross-sector comparison -- which sectors are most sensitive to operations?**

Same structure but using the operational sensitivity table under historical climate. Answers: which sector's outcomes change the most depending on how the system is operated?

```sql
WITH del AS (
  SELECT module, entity_id, range_value AS del_range
  FROM sensitivity_operational
  WHERE hydroclimate_id = 2 AND water_month = 0
    AND metric_name IN ('annual_delivery_avg', 'annual_sw_delivery_avg')
),
demand AS (
  SELECT module, entity_id, mean_value AS demand_mean
  FROM sensitivity_operational
  WHERE hydroclimate_id = 2 AND water_month = 0
    AND metric_name = 'annual_demand_avg'
    AND module IN ('ag', 'du_urban', 'mi', 'cws_aggregate')
  UNION ALL
  SELECT del_op.module, del_op.entity_id,
         del_op.mean_value + COALESCE(sh_op.mean_value, 0)
  FROM sensitivity_operational del_op
  LEFT JOIN sensitivity_operational sh_op
    ON del_op.module = sh_op.module AND del_op.entity_id = sh_op.entity_id
   AND sh_op.hydroclimate_id = 2 AND sh_op.water_month = 0
   AND sh_op.metric_name = 'annual_shortage_avg'
  WHERE del_op.hydroclimate_id = 2 AND del_op.water_month = 0
    AND del_op.metric_name = 'annual_delivery_avg' AND del_op.module = 'refuge'
),
entity_risk AS (
  -- Delivery modules: operational range / demand
  SELECT d.module, d.entity_id,
         d.del_range / NULLIF(dm.demand_mean, 0) * 100 AS pct_at_risk
  FROM del d
  JOIN demand dm ON d.module = dm.module AND d.entity_id = dm.entity_id
  WHERE dm.demand_mean > 0
  UNION ALL
  -- Reservoirs: operational range / capacity
  SELECT 'reservoir', so.entity_id,
         so.range_value / NULLIF(re.capacity_taf, 0) * 100
  FROM sensitivity_operational so
  JOIN reservoir_entity re ON so.entity_id = re.id::text
  WHERE so.hydroclimate_id = 2 AND so.water_month = 0
    AND so.module = 'reservoir' AND so.metric_name = 'storage_avg'
  UNION ALL
  -- Env flows: operational range in percentage points
  SELECT 'env_flows', entity_id, range_value
  FROM sensitivity_operational
  WHERE hydroclimate_id = 2 AND water_month = 0
    AND module = 'env_flows'
    AND metric_name IN ('annual_pct_unimpaired', 'annual_pct_ff')
  UNION ALL
  -- Delta: operational range / mean (simple % -- safe for large values)
  SELECT 'delta', entity_id,
         range_value / NULLIF(ABS(mean_value), 0) * 100
  FROM sensitivity_operational
  WHERE hydroclimate_id = 2 AND water_month = 0
    AND module = 'delta'
)
SELECT module,
       COUNT(*) AS entity_count,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_at_risk)::numeric, 1)
         AS median_pct_at_risk,
       ROUND(AVG(pct_at_risk)::numeric, 1)   AS mean_pct_at_risk,
       ROUND(MAX(pct_at_risk)::numeric, 1)    AS worst_entity_pct,
       ROUND(MIN(pct_at_risk)::numeric, 1)    AS best_entity_pct
FROM entity_risk
GROUP BY module
ORDER BY median_pct_at_risk DESC;
```

Comparing 4a and 4b side by side reveals which sectors are hydrology-driven (high in 4a, low in 4b) vs. policy-driven (low in 4a, high in 4b) vs. doubly exposed (high in both).

---

**Query 5a. Best-protecting operations -- water deliveries**

For each delivery entity, which operational configuration produces the smallest climate spread as a fraction of demand?

```sql
WITH delivery AS (
  SELECT sibling_group, module, entity_id,
         GREATEST(hist_value, cc50_value, cc95_value)
           - LEAST(hist_value, cc50_value, cc95_value) AS spread
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name IN ('annual_delivery_avg', 'annual_sw_delivery_avg')
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
),
demand AS (
  SELECT sibling_group, module, entity_id, hist_value AS hist_demand
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name = 'annual_demand_avg'
    AND module IN ('ag', 'du_urban', 'mi', 'cws_aggregate')
  UNION ALL
  SELECT del.sibling_group, del.module, del.entity_id,
         del.hist_value + COALESCE(sh.hist_value, 0)
  FROM sensitivity_climate del
  LEFT JOIN sensitivity_climate sh
    ON del.sibling_group = sh.sibling_group
   AND del.module = sh.module AND del.entity_id = sh.entity_id
   AND sh.water_month = 0 AND sh.metric_name = 'annual_shortage_avg'
  WHERE del.water_month = 0 AND del.metric_name = 'annual_delivery_avg'
    AND del.module = 'refuge'
),
ranked AS (
  SELECT d.module, d.entity_id, d.sibling_group,
         ROUND(dm.hist_demand::numeric, 1) AS demand_taf,
         ROUND(d.spread::numeric, 1) AS spread_taf,
         ROUND(d.spread / NULLIF(dm.hist_demand, 0) * 100::numeric, 1) AS pct_risk,
         ROW_NUMBER() OVER (
           PARTITION BY d.module, d.entity_id
           ORDER BY d.spread / NULLIF(dm.hist_demand, 0) ASC
         ) AS best_rank
  FROM delivery d
  JOIN demand dm ON d.sibling_group = dm.sibling_group
   AND d.module = dm.module AND d.entity_id = dm.entity_id
  WHERE dm.hist_demand > 0
),
entity_name AS (
  SELECT short_code AS eid, contractor_name AS name FROM mi_contractor
  UNION ALL
  SELECT network_arc_id, name FROM channel_entity
)
SELECT r.module, r.entity_id, en.name AS entity_name,
       r.sibling_group AS best_operation,
       r.demand_taf, r.spread_taf, r.pct_risk AS lowest_pct_risk
FROM ranked r
LEFT JOIN entity_name en ON r.entity_id = en.eid
WHERE r.best_rank = 1
ORDER BY lowest_pct_risk DESC
LIMIT 30;
```

**Query 5b. Best-protecting operations -- reservoirs**

For each reservoir, which operational configuration keeps storage most stable across climates?

```sql
WITH ranked AS (
  SELECT sc.entity_id, re.short_code, re.name,
         sc.sibling_group,
         ROUND(re.capacity_taf::numeric, 0) AS capacity_taf,
         ROUND((GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
                - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value))::numeric, 1)
           AS spread_taf,
         ROUND((GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
                - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value))
               / NULLIF(re.capacity_taf, 0) * 100::numeric, 1)
           AS pct_risk,
         ROW_NUMBER() OVER (
           PARTITION BY sc.entity_id
           ORDER BY GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
                    - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value) ASC
         ) AS best_rank
  FROM sensitivity_climate sc
  JOIN reservoir_entity re ON sc.entity_id = re.id::text
  WHERE sc.water_month = 0 AND sc.module = 'reservoir' AND sc.metric_name = 'storage_avg'
    AND sc.hist_value IS NOT NULL AND sc.cc50_value IS NOT NULL AND sc.cc95_value IS NOT NULL
)
SELECT entity_id, short_code, name, sibling_group AS best_operation,
       capacity_taf, spread_taf, pct_risk AS lowest_pct_risk
FROM ranked
WHERE best_rank = 1
ORDER BY lowest_pct_risk DESC;
```

**Query 5c. Best-protecting operations -- environmental flows**

For each channel, which operational configuration keeps % unimpaired most stable across climates?

```sql
WITH ranked AS (
  SELECT entity_id, metric_name, sibling_group,
         ROUND(hist_value::numeric, 1) AS hist_pct,
         ROUND((GREATEST(hist_value, cc50_value, cc95_value)
                - LEAST(hist_value, cc50_value, cc95_value))::numeric, 1)
           AS spread_points,
         ROW_NUMBER() OVER (
           PARTITION BY entity_id, metric_name
           ORDER BY GREATEST(hist_value, cc50_value, cc95_value)
                    - LEAST(hist_value, cc50_value, cc95_value) ASC
         ) AS best_rank
  FROM sensitivity_climate
  WHERE water_month = 0 AND module = 'env_flows'
    AND metric_name IN ('annual_pct_unimpaired', 'annual_pct_ff')
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
)
SELECT r.entity_id, ce.name AS channel_name,
       r.metric_name, r.sibling_group AS best_operation,
       r.hist_pct, r.spread_points AS smallest_spread
FROM ranked r
LEFT JOIN channel_entity ce ON r.entity_id = ce.network_arc_id
WHERE r.best_rank = 1
ORDER BY smallest_spread DESC
LIMIT 30;
```

**Query 5e. Best-protecting operations -- Delta metrics**

For each Delta variable (outflow, X2, salinity stations), which operational configuration keeps the metric most stable across climates?

```sql
WITH ranked AS (
  SELECT entity_id, metric_name, unit, sibling_group,
         ROUND(hist_value::numeric, 2) AS hist_val,
         ROUND((GREATEST(hist_value, cc50_value, cc95_value)
                - LEAST(hist_value, cc50_value, cc95_value))::numeric, 2)
           AS spread,
         ROUND((GREATEST(hist_value, cc50_value, cc95_value)
                - LEAST(hist_value, cc50_value, cc95_value))
               / NULLIF(ABS(hist_value), 0) * 100::numeric, 1)
           AS pct_spread,
         ROW_NUMBER() OVER (
           PARTITION BY entity_id, metric_name
           ORDER BY GREATEST(hist_value, cc50_value, cc95_value)
                    - LEAST(hist_value, cc50_value, cc95_value) ASC
         ) AS best_rank
  FROM sensitivity_climate
  WHERE water_month = 0 AND module = 'delta'
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
)
SELECT entity_id,
       CASE entity_id
         WHEN 'ndo'       THEN 'Net Delta Outflow'
         WHEN 'x2'        THEN 'X2 Position (2 ppt isohaline)'
         WHEN 'em_ec'     THEN 'Emmaton EC'
         WHEN 'jp_ec'     THEN 'Jersey Point EC'
         WHEN 'rs_ec'     THEN 'Rock Slough EC'
         WHEN 'co_ec'     THEN 'Collinsville EC'
         WHEN 'banks_ec'  THEN 'Banks Pumping Plant EC'
         WHEN 'tracy_ec'  THEN 'Tracy Pumping Plant EC'
         ELSE entity_id
       END AS entity_name,
       unit, sibling_group AS best_operation,
       hist_val, spread, pct_spread AS lowest_pct_spread
FROM ranked
WHERE best_rank = 1
ORDER BY lowest_pct_spread DESC;
```

With only 8 delta variables, this shows the full picture. If the same sibling group appears as best for both NDO and salinity stations, that operation provides the most Delta-wide climate protection.

**Query 5d. Which single operational configuration provides the best overall climate protection?**

This is the key question for decision-makers. For each of the 24 operational configurations, compute the average climate risk across all entities and sectors. The operation with the lowest score is the one that keeps the overall water system most stable under climate uncertainty.

Each sector is weighted equally (average within sector first, then average across sectors) so that a sector with many entities (131 ag DUs) does not dominate a sector with few (10 reservoirs).

```sql
WITH delivery AS (
  SELECT sibling_group, module, entity_id,
         GREATEST(hist_value, cc50_value, cc95_value)
           - LEAST(hist_value, cc50_value, cc95_value) AS spread
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name IN ('annual_delivery_avg', 'annual_sw_delivery_avg')
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
),
demand AS (
  SELECT sibling_group, module, entity_id, hist_value AS hist_demand
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name = 'annual_demand_avg'
    AND module IN ('ag', 'du_urban', 'mi', 'cws_aggregate')
  UNION ALL
  SELECT del.sibling_group, del.module, del.entity_id,
         del.hist_value + COALESCE(sh.hist_value, 0)
  FROM sensitivity_climate del
  LEFT JOIN sensitivity_climate sh
    ON del.sibling_group = sh.sibling_group
   AND del.module = sh.module AND del.entity_id = sh.entity_id
   AND sh.water_month = 0 AND sh.metric_name = 'annual_shortage_avg'
  WHERE del.water_month = 0 AND del.metric_name = 'annual_delivery_avg'
    AND del.module = 'refuge'
),
entity_op_risk AS (
  -- Delivery modules: spread / demand per (sibling_group, entity)
  SELECT d.sibling_group, d.module, d.entity_id,
         d.spread / NULLIF(dm.hist_demand, 0) * 100 AS pct_risk
  FROM delivery d
  JOIN demand dm ON d.sibling_group = dm.sibling_group
   AND d.module = dm.module AND d.entity_id = dm.entity_id
  WHERE dm.hist_demand > 0
  UNION ALL
  -- Reservoirs: spread / capacity per (sibling_group, entity)
  SELECT sc.sibling_group, 'reservoir', sc.entity_id,
         (GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
          - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value))
         / NULLIF(re.capacity_taf, 0) * 100
  FROM sensitivity_climate sc
  JOIN reservoir_entity re ON sc.entity_id = re.id::text
  WHERE sc.water_month = 0 AND sc.module = 'reservoir' AND sc.metric_name = 'storage_avg'
    AND sc.hist_value IS NOT NULL AND sc.cc50_value IS NOT NULL AND sc.cc95_value IS NOT NULL
  UNION ALL
  -- Env flows: percentage-point spread per (sibling_group, entity)
  SELECT sibling_group, 'env_flows', entity_id,
         GREATEST(hist_value, cc50_value, cc95_value)
           - LEAST(hist_value, cc50_value, cc95_value)
  FROM sensitivity_climate
  WHERE water_month = 0 AND module = 'env_flows'
    AND metric_name IN ('annual_pct_unimpaired', 'annual_pct_ff')
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
  UNION ALL
  -- Delta: spread / |historical| per (sibling_group, entity)
  SELECT sibling_group, 'delta', entity_id,
         (GREATEST(hist_value, cc50_value, cc95_value)
          - LEAST(hist_value, cc50_value, cc95_value))
         / NULLIF(ABS(hist_value), 0) * 100
  FROM sensitivity_climate
  WHERE water_month = 0 AND module = 'delta'
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
),
sector_avg AS (
  -- Average risk per (sibling_group, module) -- equal weight per entity within sector
  SELECT sibling_group, module, AVG(pct_risk) AS avg_sector_risk
  FROM entity_op_risk
  GROUP BY sibling_group, module
),
overall AS (
  -- Average across sectors -- equal weight per sector
  SELECT sibling_group,
         AVG(avg_sector_risk) AS avg_cross_sector_risk
  FROM sector_avg
  GROUP BY sibling_group
)
SELECT o.sibling_group,
       s.run_name,
       ROUND(o.avg_cross_sector_risk::numeric, 1) AS avg_system_risk
FROM overall o
JOIN scenario s ON o.sibling_group = s.short_code AND s.is_active = TRUE
ORDER BY avg_system_risk ASC;
```

The top rows are the operations that keep the overall system most climate-resilient. The bottom rows are the operations where climate change causes the most disruption. Compare the top and bottom `run_name` descriptions to understand what operational choices drive system-wide resilience.

To decode which operations a sibling group represents:

```sql
SELECT short_code, run_name FROM scenario
WHERE short_code IN ('s0023', 's0024')
  AND is_active = TRUE;
```

**Query 5d-vol. Same question, volume-weighted: how much actual water is at risk?**

Query 5d treats every entity equally -- a small refuge and Shasta both get one vote. This is useful for counting how many locations are at risk. But a 20% swing at Shasta (4,552 TAF) has a far greater real-world impact than a 20% swing at a small farm needing 5 TAF/year. Query 5d-vol sums the actual TAF of delivery swing and storage swing across the whole system, then divides by total system demand + capacity. The result tells you how much physical water is climate-dependent. For delivery modules, each entity's weight is its historical demand in TAF. For reservoirs, the weight is capacity in TAF. Env flows and delta are excluded because they are not measured in comparable volumetric units.

The output sentence is: "Under [operation], approximately X,000 TAF out of Y,000 TAF system-wide is climate-dependent (Z%)."

```sql
WITH delivery AS (
  SELECT sibling_group, module, entity_id,
         GREATEST(hist_value, cc50_value, cc95_value)
           - LEAST(hist_value, cc50_value, cc95_value) AS spread_taf
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name IN ('annual_delivery_avg', 'annual_sw_delivery_avg')
    AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
),
demand AS (
  SELECT sibling_group, module, entity_id, hist_value AS hist_demand
  FROM sensitivity_climate
  WHERE water_month = 0
    AND metric_name = 'annual_demand_avg'
    AND module IN ('ag', 'du_urban', 'mi', 'cws_aggregate')
  UNION ALL
  SELECT del.sibling_group, del.module, del.entity_id,
         del.hist_value + COALESCE(sh.hist_value, 0)
  FROM sensitivity_climate del
  LEFT JOIN sensitivity_climate sh
    ON del.sibling_group = sh.sibling_group
   AND del.module = sh.module AND del.entity_id = sh.entity_id
   AND sh.water_month = 0 AND sh.metric_name = 'annual_shortage_avg'
  WHERE del.water_month = 0 AND del.metric_name = 'annual_delivery_avg'
    AND del.module = 'refuge'
),
vol_delivery AS (
  SELECT d.sibling_group,
         SUM(d.spread_taf) AS total_spread_taf,
         SUM(dm.hist_demand) AS total_demand_taf
  FROM delivery d
  JOIN demand dm ON d.sibling_group = dm.sibling_group
   AND d.module = dm.module AND d.entity_id = dm.entity_id
  WHERE dm.hist_demand > 0
  GROUP BY d.sibling_group
),
vol_reservoir AS (
  SELECT sc.sibling_group,
         SUM(GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
             - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value)) AS total_spread_taf,
         SUM(re.capacity_taf) AS total_capacity_taf
  FROM sensitivity_climate sc
  JOIN reservoir_entity re ON sc.entity_id = re.id::text
  WHERE sc.water_month = 0 AND sc.module = 'reservoir' AND sc.metric_name = 'storage_avg'
    AND sc.hist_value IS NOT NULL AND sc.cc50_value IS NOT NULL AND sc.cc95_value IS NOT NULL
  GROUP BY sc.sibling_group
),
combined AS (
  SELECT sibling_group,
         total_spread_taf AS spread, total_demand_taf AS base
  FROM vol_delivery
  UNION ALL
  SELECT sibling_group,
         total_spread_taf, total_capacity_taf
  FROM vol_reservoir
),
overall AS (
  SELECT sibling_group,
         SUM(spread) AS system_spread_taf,
         SUM(base) AS system_base_taf
  FROM combined
  GROUP BY sibling_group
)
SELECT o.sibling_group,
       s.run_name,
       ROUND(o.system_spread_taf::numeric, 0) AS taf_at_risk,
       ROUND(o.system_base_taf::numeric, 0) AS total_system_taf,
       ROUND((o.system_spread_taf / NULLIF(o.system_base_taf, 0) * 100)::numeric, 1)
         AS pct_system_water_at_risk
FROM overall o
JOIN scenario s ON o.sibling_group = s.short_code AND s.is_active = TRUE
ORDER BY pct_system_water_at_risk ASC;
```

Reading the output: `taf_at_risk` is the total TAF of delivery + storage that swings with climate across the entire system. `total_system_taf` is the total demand + capacity. `pct_system_water_at_risk` is the ratio -- "X% of the system's water is climate-dependent." This directly answers: "how much actual water is in play?"

Comparing 5d and 5d-vol: if 5d ranks an operation highly but 5d-vol does not, it means that operation protects many small entities but lets large ones swing. If 5d-vol ranks it highly but 5d does not, it protects the big systems but leaves small ones exposed.

---

**Interpreting the results:**

- `pct_demand_at_risk` answers: "what fraction of this entity's water need could change depending on which climate shows up?" A value of 30% means the delivery swing across all three climates equals 30% of the entity's demand. For example, if a farming district needs 100 TAF/year and gets 95, 85, or 75 TAF depending on climate, the swing is 20 TAF -- 20% of its need.
- `pct_capacity_at_risk` answers: "what fraction of this reservoir's usable storage swings with climate?" If a 500 TAF reservoir holds 400, 350, or 300 TAF across climates, the swing is 100 TAF -- 20% of capacity.
- `avg_system_risk` in Query 5d answers: "what percentage of the system's locations are climate-exposed?" A value of 8% means the typical entity sees about 8% of its water need at stake from climate; the other 92% is stable regardless. Every entity counts equally -- a small refuge and Shasta reservoir both get one vote.
- `pct_system_water_at_risk` in Query 5d-vol answers the volume-weighted version: "how much actual TAF of water is climate-dependent?" This weights large entities proportionally -- a 20% swing at Shasta (4,552 TAF capacity) contributes far more than a 20% swing at a 50 TAF foothill reservoir. Use 5d for identifying how many locations are at risk; use 5d-vol for quantifying how much water is at risk.
- Small community water systems and refuges appear in the rankings alongside large ones, because demand (not delivery) is the normalizer.
- Compare 4a (climate) and 4b (operations) to classify sectors: hydrology-driven (high in 4a, low in 4b), policy-driven (low in 4a, high in 4b), or doubly exposed (high in both).
- Queries 5a-5c identify the best-protecting operation per entity within each sector. If the same sibling group keeps appearing as the best across multiple entities and sectors, that operation is a strong candidate for system-wide climate resilience.
- Query 5d aggregates across all sectors with equal sector weighting, directly answering: "if we had to pick one operational configuration to minimize climate risk system-wide, which would it be?"
- Queries 6a-6d shift the lens from entities to metrics. Shortage, demand, and reliability metrics are excluded because their near-zero baselines produce misleading percentages (e.g. refuge shortage at 200% just means shortage went from near zero to slightly more). The meaningful signal is in delivery, storage, and flow metrics.
- Queries 7a/7b show the top 20 most and least climate-vulnerable individual entities with human-readable names and actual values (TAF, CFS, KM). These are designed for direct use in reports and emails.

---

**Query 6a. Top 10 metrics most sensitive to climate change**

Which aspects of the water system swing the most with climate? Each (module, metric_name) pair is scored by its average percentage spread across all entities and all 24 sibling groups.

Shortage, demand, and reliability are excluded because they have near-zero baselines that produce misleading percentages (e.g. refuge shortage might report 200%+ spread, but that just means shortage went from nearly zero to slightly-less-nearly-zero). The meaningful signal is in delivery, storage, and flow metrics.

```sql
SELECT module, metric_name,
       COUNT(DISTINCT entity_id) AS entity_count,
       ROUND(AVG(
         (GREATEST(hist_value, cc50_value, cc95_value)
          - LEAST(hist_value, cc50_value, cc95_value))
         / NULLIF(ABS(hist_value), 0) * 100
       )::numeric, 1) AS avg_pct_spread
FROM sensitivity_climate
WHERE water_month = 0
  AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
  AND ABS(hist_value) > 1.0
  AND metric_name NOT IN ('annual_shortage_avg', 'annual_demand_avg', 'reliability')
GROUP BY module, metric_name
ORDER BY avg_pct_spread DESC
LIMIT 10;
```

**Query 6b. Top 10 metrics most resilient to climate change**

Same calculation, reversed ranking. These metrics barely change regardless of climate.

```sql
SELECT module, metric_name,
       COUNT(DISTINCT entity_id) AS entity_count,
       ROUND(AVG(
         (GREATEST(hist_value, cc50_value, cc95_value)
          - LEAST(hist_value, cc50_value, cc95_value))
         / NULLIF(ABS(hist_value), 0) * 100
       )::numeric, 1) AS avg_pct_spread
FROM sensitivity_climate
WHERE water_month = 0
  AND hist_value IS NOT NULL AND cc50_value IS NOT NULL AND cc95_value IS NOT NULL
  AND ABS(hist_value) > 1.0
  AND metric_name NOT IN ('annual_shortage_avg', 'annual_demand_avg', 'reliability')
GROUP BY module, metric_name
ORDER BY avg_pct_spread ASC
LIMIT 10;
```

**Query 6c. Top 10 metrics most sensitive to operational choices (and which operations drive them)**

Part 1 -- rank metrics by operational sensitivity. Uses `sensitivity_operational` under historical climate (hydroclimate_id = 2). The `range_value` in that table is the spread across all operational configurations for a fixed climate. Same exclusions as 6a/6b.

```sql
SELECT module, metric_name,
       COUNT(DISTINCT entity_id) AS entity_count,
       ROUND(AVG(range_value / NULLIF(ABS(mean_value), 0) * 100)::numeric, 1) AS avg_pct_range
FROM sensitivity_operational
WHERE hydroclimate_id = 2
  AND water_month = 0
  AND ABS(mean_value) > 1.0
  AND metric_name NOT IN ('annual_shortage_avg', 'annual_demand_avg', 'reliability')
GROUP BY module, metric_name
ORDER BY avg_pct_range DESC
LIMIT 10;
```

Part 2 -- for a specific metric from Part 1, drill down to see which operations produce the highest and lowest values. Substitute the `module` and `metric_name` from your Part 1 results:

```sql
-- Which operations drive the most variation for a specific metric?
-- Substitute module and metric_name from Part 1 results.
WITH entity_vals AS (
  SELECT sibling_group, entity_id, hist_value AS val
  FROM sensitivity_climate
  WHERE water_month = 0
    AND module = 'ag'                     -- substitute from Part 1
    AND metric_name = 'annual_shortage_avg'  -- substitute from Part 1
    AND hist_value IS NOT NULL
),
op_avg AS (
  SELECT sibling_group,
         ROUND(AVG(val)::numeric, 1) AS avg_value,
         COUNT(*) AS entities
  FROM entity_vals
  GROUP BY sibling_group
)
SELECT oa.sibling_group,
       s.run_name,
       oa.avg_value,
       oa.entities
FROM op_avg oa
JOIN scenario s ON oa.sibling_group = s.short_code AND s.is_active = TRUE
ORDER BY avg_value DESC;
```

The top rows are the operations that produce the highest average value for that metric (worst shortage, highest delivery, etc.). The bottom rows produce the lowest. The gap between top and bottom is what Part 1 measured as `avg_pct_range`.

**Query 6d. Top 10 metrics most resilient to operational choices**

Same as 6c Part 1, reversed. These metrics barely change no matter how the system is operated -- they are structurally determined by hydrology rather than policy.

```sql
SELECT module, metric_name,
       COUNT(DISTINCT entity_id) AS entity_count,
       ROUND(AVG(range_value / NULLIF(ABS(mean_value), 0) * 100)::numeric, 1) AS avg_pct_range
FROM sensitivity_operational
WHERE hydroclimate_id = 2
  AND water_month = 0
  AND ABS(mean_value) > 1.0
  AND metric_name NOT IN ('annual_shortage_avg', 'annual_demand_avg', 'reliability')
GROUP BY module, metric_name
ORDER BY avg_pct_range ASC
LIMIT 10;
```

**How to read 6a-6d together:** metrics appearing in 6b (climate-resilient) AND 6d (operationally-resilient) are the most stable aspects of the water system. Metrics appearing in 6a (climate-sensitive) AND 6c (operationally-sensitive) are the most volatile. Metrics high in 6a but low in 6c are climate-driven and cannot be mitigated by operations. Metrics low in 6a but high in 6c are the best targets for policy intervention: operations matter, climate does not.

---

**Query 7a. Top 20 most climate-vulnerable entities (named, with actual values)**

Entity-level detail for reporting. Shows named locations/agencies, their historical values, and the actual TAF/CFS/KM swing across climates. Excludes shortage/demand/reliability for the same reasons as 6a-6d.

```sql
WITH ranked AS (
  SELECT sc.module, sc.entity_id, sc.metric_name,
         ROUND(AVG(sc.hist_value)::numeric, 1) AS hist_val,
         ROUND(AVG(sc.cc50_value)::numeric, 1) AS cc50_val,
         ROUND(AVG(sc.cc95_value)::numeric, 1) AS cc95_val,
         ROUND(AVG(
           GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
           - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value)
         )::numeric, 1) AS spread,
         COALESCE(mc.contractor_name, ce.name, re.name,
                  sc.entity_id) AS entity_name
  FROM sensitivity_climate sc
  LEFT JOIN mi_contractor mc ON sc.module = 'mi' AND sc.entity_id = mc.short_code
  LEFT JOIN channel_entity ce ON sc.module = 'env_flows' AND sc.entity_id = ce.network_arc_id
  LEFT JOIN reservoir_entity re ON sc.module = 'reservoir' AND sc.entity_id = re.id::text
  WHERE sc.water_month = 0
    AND sc.metric_name NOT IN ('annual_shortage_avg', 'annual_demand_avg', 'reliability')
    AND sc.hist_value IS NOT NULL AND sc.cc50_value IS NOT NULL AND sc.cc95_value IS NOT NULL
    AND ABS(sc.hist_value) > 1.0
  GROUP BY sc.module, sc.entity_id, sc.metric_name,
           mc.contractor_name, ce.name, re.name
)
SELECT module, entity_name, metric_name,
       hist_val, cc50_val, cc95_val, spread,
       ROUND((spread / NULLIF(ABS(hist_val), 0) * 100)::numeric, 1) AS pct_spread
FROM ranked
ORDER BY pct_spread DESC
LIMIT 20;
```

Reading the output: the entity at the top has the largest climate swing relative to its baseline. The `hist_val`, `cc50_val`, `cc95_val` columns show actual values (TAF, CFS, KM, etc.) so you can see exactly how much water or flow is at stake.

**Query 7b. Top 20 most climate-stable entities (named, with actual values)**

Same structure, reversed. These are the entities that barely change across climates.

```sql
WITH ranked AS (
  SELECT sc.module, sc.entity_id, sc.metric_name,
         ROUND(AVG(sc.hist_value)::numeric, 1) AS hist_val,
         ROUND(AVG(sc.cc50_value)::numeric, 1) AS cc50_val,
         ROUND(AVG(sc.cc95_value)::numeric, 1) AS cc95_val,
         ROUND(AVG(
           GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
           - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value)
         )::numeric, 1) AS spread,
         COALESCE(mc.contractor_name, ce.name, re.name,
                  sc.entity_id) AS entity_name
  FROM sensitivity_climate sc
  LEFT JOIN mi_contractor mc ON sc.module = 'mi' AND sc.entity_id = mc.short_code
  LEFT JOIN channel_entity ce ON sc.module = 'env_flows' AND sc.entity_id = ce.network_arc_id
  LEFT JOIN reservoir_entity re ON sc.module = 'reservoir' AND sc.entity_id = re.id::text
  WHERE sc.water_month = 0
    AND sc.metric_name NOT IN ('annual_shortage_avg', 'annual_demand_avg', 'reliability')
    AND sc.hist_value IS NOT NULL AND sc.cc50_value IS NOT NULL AND sc.cc95_value IS NOT NULL
    AND ABS(sc.hist_value) > 1.0
  GROUP BY sc.module, sc.entity_id, sc.metric_name,
           mc.contractor_name, ce.name, re.name
)
SELECT module, entity_name, metric_name,
       hist_val, cc50_val, cc95_val, spread,
       ROUND((spread / NULLIF(ABS(hist_val), 0) * 100)::numeric, 1) AS pct_spread
FROM ranked
WHERE spread > 0
ORDER BY pct_spread ASC
LIMIT 20;
```

---

**Drilling into a single entity group**

The cross-module queries above rank all delivery entities together. To compare entities within a single group (e.g., "which of the 10 reservoirs is most robust?"), add a `WHERE` filter on `module` or use the module-specific queries (1b, 2b for reservoirs). Examples:

Within **reservoirs** -- most and least robust to climate (Query 1b already does this; remove `LIMIT` to see all):

```sql
-- All reservoirs ranked by climate vulnerability (capacity-normalized)
-- Same as Query 1b, but without LIMIT
SELECT sc.entity_id,
       re.short_code,
       re.name,
       ROUND(re.capacity_taf::numeric, 0) AS capacity_taf,
       ROUND(AVG(sc.hist_value)::numeric, 1) AS avg_hist_storage,
       ROUND(AVG(
         GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
         - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value)
       )::numeric, 1) AS avg_spread_taf,
       ROUND(AVG(
         (GREATEST(sc.hist_value, sc.cc50_value, sc.cc95_value)
          - LEAST(sc.hist_value, sc.cc50_value, sc.cc95_value))
         / NULLIF(re.capacity_taf, 0) * 100
       )::numeric, 1) AS pct_capacity_at_risk
FROM sensitivity_climate sc
JOIN reservoir_entity re ON sc.entity_id = re.id::text
WHERE sc.water_month = 0
  AND sc.module = 'reservoir'
  AND sc.metric_name = 'storage_avg'
  AND sc.hist_value IS NOT NULL
  AND sc.cc50_value IS NOT NULL
  AND sc.cc95_value IS NOT NULL
GROUP BY sc.entity_id, re.short_code, re.name, re.capacity_taf
ORDER BY pct_capacity_at_risk DESC;
```

The top rows are the most vulnerable reservoirs; the bottom rows are the most robust. With only 10 reservoirs, no `LIMIT` is needed.

Within **urban demand units** -- add `AND d.module = 'du_urban'` to Query 1a:

```sql
-- ... same CTEs as Query 1a ...
SELECT d.module, d.entity_id,
       ROUND(AVG(dm.hist_demand)::numeric, 1) AS avg_demand_taf,
       ROUND(AVG(d.spread)::numeric, 1)        AS avg_spread_taf,
       ROUND(AVG(
         d.spread / NULLIF(dm.hist_demand, 0) * 100
       )::numeric, 1) AS pct_demand_at_risk
FROM delivery d
JOIN demand dm
  ON d.sibling_group = dm.sibling_group
 AND d.module = dm.module
 AND d.entity_id = dm.entity_id
WHERE d.module = 'du_urban'                      -- filter to urban DUs only
GROUP BY d.module, d.entity_id
HAVING AVG(dm.hist_demand) > 0
ORDER BY pct_demand_at_risk DESC;
```

The same pattern works for any module. Replace `'du_urban'` with:
- `'mi'` for SWP contractors
- `'cws_aggregate'` for CWS project aggregates
- `'ag'` for agricultural demand units
- `'refuge'` for wildlife refuges
- `'env_flows'` for environmental flow channels (use Query 1c with no module filter change needed)

### Individual module usage

Each module can also be run standalone:

```bash
# Reservoir statistics
cd etl/statistics && python main.py --scenario s0020

# Urban demand unit statistics
cd etl/statistics/du_urban && python main.py --scenario s0020

# M&I contractor statistics  
cd etl/statistics/mi && python main.py --scenario s0020

# CWS aggregate statistics
cd etl/statistics/cws_aggregate && python main.py --scenario s0020

# Agricultural statistics (loads SV + DV)
cd etl/statistics/ag && python main.py --scenario s0020

# Wildlife refuge statistics (loads SV + DV)
cd etl/statistics/refuge && python main.py --scenario s0020

# Environmental river flow statistics (loads SV + DV)
cd etl/statistics/env_flows && python main.py --scenario s0020

# Delta statistics (outflow, X2, salinity)
cd etl/statistics/delta && python main.py --scenario s0020

# Cross-scenario sensitivity (runs after all per-scenario stats)
cd etl/statistics && python sensitivity/calculate_sensitivity.py
```

### Available scenarios

76 scenarios are defined in `etl/statistics/scenarios.py`. These match the scenarios
extracted to S3 (see `check_extraction_results.py` for the full verified list).

### Reliability calculation

For CWS aggregates and M&I contractors, **reliability** is calculated as:

```
Reliability % = (1 - Average Annual Shortage / Average Annual Delivery) x 100
```

**Example:**
- Average annual delivery = 1,000 TAF
- Average annual shortage = 50 TAF
- Reliability = (1 - 50/1000) x 100 = **95%**

This represents the percentage of requested water that was actually delivered across the simulation period (1922-2021).

### Shortage frequency calculation

**Shortage frequency** is the percentage of years (or months) with a meaningful shortage:

```
Shortage Frequency % = (Years with annual shortage > 0.1 TAF / Total years) x 100
```

**Why the 0.1 TAF threshold?**

CalSim uses a linear programming solver that can produce floating-point precision artifacts (e.g., shortage values of 0.0000001 TAF). Without a threshold, these artifacts would be counted as "shortage years," producing misleading high frequencies.

The 0.1 TAF (100 acre-feet) threshold:
- Filters out numerical noise from the solver
- Is small enough to catch real shortages (< 0.05% of typical delivery)
- Approximately equals 1 day of supply for a small M&I contractor

### Shortage data provenance

Shortage in CalSim represents unmet water delivery:

```
Shortage = Delivery Target - Actual Delivery
```

Where **Delivery Target** = Demand x Contract Allocation % (not raw demand).

#### CWS (Community Water Systems / M&I) shortage

**Variables:** `SHORT_CVP_PMI_N`, `SHORT_CVP_PMI_S`, `SHORT_SWP_PMI_N`, `SHORT_SWP_PMI_S`, `SHORT_SWP_PMI`

**Source:** `Run/DeliveryLogic/output/deliv_short_cvp_n.wresl`

Individual contractor shortage is calculated as:
```wresl
define X_WTPCSD_02_PU {alias target - D_WTPCSD_02_PU kind 'delivery-shortage' units 'cfs'}
```

Aggregate variables sum individual shortages:
```wresl
define short_cvp_pmi_n {alias X_WTPCSD_02_PU + X_WKYTN_02_PU + X_SHSTA_03_PU1 + ...
                        kind 'delivery-shortage-cvp' units 'cfs'}
```

#### Agricultural (AG) shortage

**Variables:** `SHORT_CVP_PAG_N`, `SHORT_CVP_PAG_S`, `SHORT_SWP_PAG_N`, `SHORT_SWP_PAG_S`, `SHORT_SWP_PAG`

**Source:** `Run/DeliveryLogic/output/deliv_short_cvp_s.wresl`

Individual contractor shortage:
```wresl
define X_50_PA1 {alias CLM_50_PA1 * taf_cfs * perdel_cvpag_s - D_DMC021_50_PA1
                 kind 'delivery-shortage' units 'cfs'}
```

Where:
- `CLM_*` = Climate-based demand
- `perdel_cvpag_s` = Percent delivery allocation (e.g., 0.75 for 75%)
- `D_*` = Actual delivery

Aggregate variables sum individual shortages:
```wresl
define short_cvp_pag_s {alias X_50_PA1 + X_71_PA1 + X_71_PA2 + ...
                        kind 'delivery-shortage-cvp' units 'cfs'}
```

#### Important distinctions

| Concept | Definition |
|---------|------------|
| **Shortage** | Target - Delivery (accounts for allocation %) |
| **Target** | Demand x Allocation % |
| **Reliability** | 1 - (Avg Shortage / Avg Delivery) |

**Note:** Individual DU `GW_SHORT_*` variables represent **groundwater restriction shortage** (a COEQWAL-specific variable for testing groundwater pumping limits), NOT total delivery shortage. For aggregate delivery shortage, use `SHORT_CVP_PAG_*` and `SHORT_SWP_PAG_*`.

**Note:** The COEQWAL Jupyter notebooks back-calculate demand using `Demand = (Shortage + Delivery) / percent_delivery`. Our ETL uses the shortage variables directly without this transformation.

### Prerequisites

- `DATABASE_URL` environment variable set
- CalSim output CSV available in S3: `s3://coeqwal-model-run/scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv`
- Python packages: `pandas`, `numpy`, `psycopg2`, `boto3`

---

## Cloud9 IAM permissions

The Cloud9 EC2 instance uses `AWSCloud9SSMAccessRole`. This role has AWS-managed policies for SSM and S3 access, plus an inline policy (`ETLOperations`) for ETL-specific operations. If you're setting up a new Cloud9 environment, add this inline policy to the role:

**IAM console:** https://us-west-2.console.aws.amazon.com/iam/home#/roles/details/AWSCloud9SSMAccessRole

**Inline policy name:** `ETLOperations`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudWatchLogsRead",
      "Effect": "Allow",
      "Action": [
        "logs:GetLogEvents",
        "logs:FilterLogEvents",
        "logs:DescribeLogStreams",
        "logs:DescribeLogGroups"
      ],
      "Resource": "arn:aws:logs:us-west-2:533266975152:log-group:*"
    },
    {
      "Sid": "ECRRead",
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:DescribeImages",
        "ecr:DescribeRepositories",
        "ecr:ListImages"
      ],
      "Resource": "*"
    },
    {
      "Sid": "BatchOperations",
      "Effect": "Allow",
      "Action": [
        "batch:SubmitJob",
        "batch:DescribeJobs",
        "batch:DescribeJobDefinitions",
        "batch:RegisterJobDefinition",
        "batch:ListJobs",
        "batch:DescribeComputeEnvironments",
        "batch:DescribeJobQueues",
        "batch:TerminateJob",
        "batch:CancelJob"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassBatchRoles",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::533266975152:role/BatchEcsTaskExecutionRole",
        "arn:aws:iam::533266975152:role/coeqwal-dss-batch-task-role"
      ]
    }
  ]
}
```

| Statement | What it allows | Why you need it |
|-----------|---------------|-----------------|
| CloudWatchLogsRead | Read Batch, Lambda, and RDS logs | Debugging failed extractions |
| ECRRead | Check Docker image push timestamps | Confirming GitHub Actions built the image |
| BatchOperations | Submit, monitor, cancel, and update Batch jobs | Running `reextract_all_scenarios.py`, managing jobs, updating job definitions |
| PassBatchRoles | Pass the two Batch IAM roles when registering job definitions | Required by `batch:RegisterJobDefinition` |

Note: the Cloud9 IAM role credentials never expire. Long-running jobs in tmux keep running even when your SSO session drops. SSO expiring only locks you out of the Cloud9 browser UI until you re-authenticate.

---

## AWS Cheatsheet

Quick reference commands for inspecting and managing the ETL infrastructure.

### Batch compute environment

The job definition allocates 16 GB memory and 2 vCPUs per extraction job (revision 3, updated April 2026). Previously 8 GB, which caused OOM kills on larger scenarios like the DWRadapt25 group (s0065, s0085, s0105) whose CalSim output CSVs are ~326 MB vs ~200 MB for typical scenarios. Signs of OOM: manifest shows `calsim_csv_written: false` with status `FAILED`, and `aws batch describe-jobs` shows `OutOfMemoryError: container killed due to memory usage`. If needed, `reextract_all_scenarios.py` supports `--memory` to override per-job (e.g., `--memory 32768` for 32 GB).

```bash
# Check compute environment sizing and type
aws batch describe-compute-environments \
  --query 'computeEnvironments[].{name: computeEnvironmentName, type: computeResources.type, minvCpus: computeResources.minvCpus, maxvCpus: computeResources.maxvCpus, instanceTypes: computeResources.instanceTypes}' \
  --output table

# Update maxvCpus for a bulk load (scale up)
aws batch update-compute-environment \
  --compute-environment coeqwal-dss-ce \
  --compute-resources maxvCpus=256

# Check job definition (per-job resource allocation)
aws batch describe-job-definitions \
  --job-definition-name coeqwal-dss-jobdef --status ACTIVE \
  --query 'jobDefinitions[-1].{revision: revision, containerProps: ecsProperties.taskProperties[0].containers[0].resourceRequirements}' \
  --output json
```

### Batch job monitoring

```bash
# Job counts by status
aws batch list-jobs --job-queue coeqwal-dss-queue --job-status RUNNABLE --query 'length(jobSummaryList)'
aws batch list-jobs --job-queue coeqwal-dss-queue --job-status RUNNING --query 'length(jobSummaryList)'
aws batch list-jobs --job-queue coeqwal-dss-queue --job-status SUCCEEDED --query 'length(jobSummaryList)'
aws batch list-jobs --job-queue coeqwal-dss-queue --job-status FAILED --query 'length(jobSummaryList)'

# Inspect a specific job
aws batch describe-jobs --jobs <job-id> \
  --query 'jobs[0].{status: status, started: startedAt, stopped: stoppedAt, reason: statusReason}' \
  --output table

# Diagnose a failed job (shows failure reason, exit code, and log stream)
aws batch describe-jobs --jobs <job-id> --output json \
  | python -c "
import sys, json
job = json.load(sys.stdin)['jobs'][0]
print('Status:', job.get('status'))
print('Reason:', job.get('statusReason', 'none'))
for att in job.get('attempts', []):
    print('---')
    print('Attempt reason:', att.get('statusReason', 'none'))
    for tc in att.get('taskProperties', []):
        for c in tc.get('containers', []):
            print('Container reason:', c.get('reason', 'none'))
            print('Exit code:', c.get('exitCode', 'none'))
            ls = c.get('logStreamName', 'none')
            print('Log stream:', ls)
            if ls != 'none':
                print('  View logs: aws logs get-log-events --log-group-name /aws/batch/job --log-stream-name', ls, '--limit 200 --query events[].message --output text')
"
```

### Lambda

```bash
# Tail recent logs
aws logs tail /aws/lambda/coeqwalEtlTrigger --since 30m

# Follow in real time
aws logs tail /aws/lambda/coeqwalEtlTrigger --follow

# Deploy updated Lambda code (from Cloud9)
cd ~/environment/coeqwal-backend/etl/lambda-trigger
zip lambda.zip index.mjs
aws lambda update-function-code --function-name coeqwalEtlTrigger --zip-file fileb://lambda.zip
rm lambda.zip
```

### ECR (Docker image)

```bash
# Check latest image push
aws ecr describe-images --repository-name coeqwal-etl \
  --query 'imageDetails | sort_by(@, &imagePushedAt) | [-1].{pushed: imagePushedAt, tags: imageTags}' \
  --output table
```

### S3

```bash
# List scenario folders
aws s3 ls s3://coeqwal-model-run/scenario/

# Check what's in staging
aws s3 ls s3://coeqwal-model-run/staging/

# Check CSVs for a scenario
aws s3 ls s3://coeqwal-model-run/scenario/s0021/csv/

# Read a manifest
aws s3 cp s3://coeqwal-model-run/scenario/s0021/s0021_manifest.json - | python -m json.tool
```

### ETL scripts (run from Cloud9)

```bash
# Scan scenarios
python etl/scripts/gdrive_bulk_download.py scan \
  --listing-csv database/reference/model_run_file_source.csv --workers 4

# Download from Drive to S3 staging
python etl/scripts/gdrive_bulk_download.py download \
  --listing-csv database/reference/model_run_file_source.csv \
  --s3-bucket coeqwal-model-run --workers 4

# Promote to trigger extraction (all, or specific)
python etl/scripts/gdrive_bulk_download.py promote --s3-bucket coeqwal-model-run
python etl/scripts/gdrive_bulk_download.py promote --s3-bucket coeqwal-model-run --scenarios s0021

# Check extraction results
python etl/scripts/check_extraction_results.py --bucket coeqwal-model-run
python etl/scripts/check_extraction_results.py --bucket coeqwal-model-run --scenarios s0021
python etl/scripts/check_extraction_results.py --bucket coeqwal-model-run --mismatches

# Re-extract scenarios (re-trigger Batch)
python etl/scripts/reextract_all_scenarios.py --dry-run
python etl/scripts/reextract_all_scenarios.py --scenarios s0021,s0022

# Re-extract with more memory (default is now 16 GB; use 32 GB if still OOM)
python etl/scripts/reextract_all_scenarios.py --scenarios s0065 --memory 32768

# Unit verification (requires Docker - build image first)
cd ~/environment/coeqwal-backend/etl/coeqwal-etl && docker build -t coeqwal-etl:test .
docker run --rm --entrypoint "" coeqwal-etl:test \
  python /app/python-code/verify_dss_csv_units.py --scenarios-from-s3 --workers 6

# Duplicate B-part scan + cross-scenario unit consistency (no Docker needed)
cd ~/environment/coeqwal-backend/etl/statistics
python scan_dupes.py --compare-values --audit-units --workers 4
```