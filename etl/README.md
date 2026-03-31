# ETL (Extract, Transform, Load) Framework

Automated DSS file processing pipeline that:

- copies scenario model run files from Water Allocation Modeling Team Google Drive
- extracts CSV data from CalSim model runs and validates against reference data
- computes statistics and loads statistics into database.

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
- If there's only **one file**, leave the column blank — the script auto-selects it.

**`download_status` values:**
- `ready` — folder ID verified, files confirmed by scan, ready to download
- `needs_review` — known issue (missing files, wrong folder ID, etc.)
- `skip` — intentionally excluded from download

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
- `ALERT_MULTIPLE_ZIP` / `ALERT_MULTIPLE_TREND` = multiple files, no pinned filename set — add one to the CSV
- `MISSING_ZIP` / `MISSING_TREND` = file not found on Drive
- `PINNED_ZIP_NOT_FOUND` / `PINNED_TREND_NOT_FOUND` = pinned filename doesn't match any file on Drive

Review `scan_audit.csv` before proceeding. All scenarios should show `OK` (except known missing trend reports like s0011).

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

### 6. Run statistics ETL and verify

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
│   ├── <scenario>.zip          ← the model run ZIP (contains DSS files)
│   └── DSS/
│       ├── output/
│       │   └── <scenario>_DV_<version>.dss
│       └── input/
│           └── coeqwal_s9999_SV_<version>.dss
└── Data_Extraction/
    └── Variables_From_trend_report_variables_v5/
        └── <scenario>_trend_report_<version>.csv   ← validation reference
```

The ZIP in `Model_Files/` is what gets downloaded. The trend report CSV is used for post-extraction validation. Both are authored by the modeling team (Dino Bellugi).

### What is automated vs. manual

The pipeline has automated and manual stages. Understanding the boundary is important:

```
                           AUTOMATED                                    MANUAL
                   ┌──────────────────────────┐          ┌──────────────────────────────┐
Google Drive ──►   │  S3 ready/ ──► Lambda    │          │  Statistics ETL              │
(gdrive_bulk_      │  ──► Batch (DSS→CSV)     │          │  (run_all.py)                │
 download.py)      │  ──► S3 scenario/csv/    │          │  ──► PostgreSQL tables       │
   [manual]        │  ──► S3 validation/       │          │  Verification                │
                   │  ──► S3 manifest.json     │          │  (verify_all_sections.py,    │
                   └──────────────────────────┘          │   verify_api.py)             │
                                                         └──────────────────────────────┘
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

#### Recommended log retention

Keep log files on Cloud9 for each batch load in a dedicated directory:

```bash
mkdir -p ~/logs/load_$(date +%Y%m%d)
# Then use the tee commands above to write logs there
```

Key log files to keep per load:
- `scan_audit.csv` -- pre-download validation
- `etl_download_*.log` -- download/staging output
- `extraction_audit.csv` -- post-extraction status and validation results
- `statistics_*.log` -- per-scenario statistics ETL output
- `verify_*.log` -- per-scenario verification output

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
- **Output**: CSV time series data
- **Validation**: Compares against reference data with configurable tolerances
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

Cloud9 instances default to **10 GB** EBS, which is tight when downloading ~200 MB ZIPs for 24 scenarios. The script streams files through `/tmp/` and uploads to S3 immediately, so you only need space for one ZIP at a time per worker — but it's still good practice to check.

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

**Alternative — skip local storage entirely:** The script uses `/tmp/` as a transient staging area and uploads to S3 immediately. With `--workers 1` you only need ~200 MB free. With `--workers 4` you need ~800 MB. If storage is a concern, reduce workers.

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
#   client_id> (leave blank — uses rclone's built-in OAuth client)
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

This lists `Model_Files/` and `Data_Extraction/Variables_From_trend_report_variables_v5/` for each scenario via rclone, counts ZIP files and trend report CSVs, and writes `scan_audit.csv`.

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
2. **Validate** the ZIP — open it, list all `.dss` files, classify them as SV (input) or DV (output), and alert if there is not exactly one of each type
3. Upload the ZIP to `s3://coeqwal-model-run/staging/s0020/`
4. Download the trend report CSV (starting with `s0020`) from `Data_Extraction/Variables_From_trend_report_variables_v5/`
5. Upload the CSV to `s3://coeqwal-model-run/staging/s0020/`
6. Write `audit_report.csv` locally and to `s3://coeqwal-model-run/staging/`

**Verify in S3:**
```bash
aws s3 ls s3://coeqwal-model-run/staging/s0020/
```

### Step 6: Review the audit report

```bash
# View locally
column -s, -t < audit_report.csv | less -S

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

This processes 4 scenarios in parallel. Each downloads to `/tmp/`, validates, uploads to S3 staging, and cleans up. Total time depends on network speed; expect ~30–60 minutes for 24 scenarios.

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
DSS Files ──► S3 CSVs (DV + SV) ──► PostgreSQL ──► JSON API ──► Frontend
  Layer 1        Layer 2              Layer 2b       Layer 3     Layer 4
  (extraction)   (ETL statistics)     (tier data)    (API)       (status page)
```

Variable lists sourced from `COEQWAL_V3/notebooks/variable_groupings.csv` and mapping CSVs (`DrinkingWater_Mapping.csv`, `Agricultural_Mapping.csv`, `Eflows_Mapping.csv`).

### Layer 1: Extraction (DSS to CSV)

Validates that `dss_to_csv.py` extracts data correctly from HEC-DSS files. Uses `validate_csvs.py` to compare extracted CSVs against reference CSVs.

Manifests stored in `audits/validation_mismatches/{scenario_id}_manifest.json`.

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

**Fully verified (ETL + DB + API):**
- CWS: delivery volume, % of demand, absolute shortage
- AG: SW delivery, GW pumping, total shortage, shortage %, reliability
- Env Flows: volume, % unimpaired, % functional flows, alteration index (Pearson r)
- Refuge: delivery, shortage, reliability
- Reservoirs: April/Sept storage (TAF + %), spill frequency
- Tiers: CWS_DEL, AG_REV, ENV_FLOWS, RES_STOR, GW_STOR, DELTA_ECO, FW_DELTA_USES, FW_EXP, WRC_SALMON_AB

**Not yet implemented:**
- Delta outflow volumes (NDO)
- April/September X2 position (X2_PRV_KM)
- Salinity at Rock Slough, Collinsville (RS_EC_MONTH, CO_EC_MONTH)
- Groundwater level, storage volume, level/storage change
- Salmon abundance (real metric, not hardcoded tier)

### How to add a new scenario

1. Ensure DSS-to-CSV extraction has run and manifests show PASS in `audits/validation_mismatches/`
2. Run the ETL statistics: `python etl/statistics/run_all.py --scenario {id}`
3. Load tier data: `python etl/tier_data/load_all_tier_results.py`
4. Run Layer 2 verification: `python etl/statistics/verify_all_sections.py --scenario {id}`
5. Run Layer 3 verification: `python etl/statistics/verify_api.py --scenario {id}`
6. Check results at `/verification` on the frontend

---

## Validation framework

### Tolerance parameters
- **Absolute tolerance (`abs_tol`)**: Maximum allowed absolute difference between values
- Example: `abs_tol=1e-6` means values must be within ±0.000001 units
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
Percent_of_Demand = (Delivery / Demand) × 100
                  = (Delivery × PERDV) / (Delivery + Shortage)
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
Delivery + Shortage = Demand × (PERDV / 100)
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

All CFS values converted to TAF via `CFS × DaysInMonth × 0.001983471`.

### Shortage calculation

AG shortage = `max(demand − delivery, 0)` computed in the ETL.

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
| `S_{CODE}LEVELxDV` | Storage zone x (x = 1–6) | TAF |

The highest zone (5 or 6 depending on reservoir) represents capacity.

### Spill

| Variable | Description | Units |
|----------|-------------|-------|
| `C_{CODE}_FLOOD` | Flood release (spill) | CFS |

Spill is converted to TAF via `CFS × DaysInMonth × 0.001983471`.

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
screen -S etl
python run_all.py \
  --all-scenarios --workers 4 --continue-on-error --with-sensitivity \
  2>&1 | tee stats_run_$(date +%Y%m%d).log

# List available modules
python run_all.py --list-modules
```

**After a run completes**, `run_all.py` automatically:
- Writes a structured **audit CSV** (`stats_audit_YYYYMMDD_HHMMSS.csv`) with one row per (scenario, module) including status and timing
- Prints a **scorecard** showing success/failure per scenario × module
- Runs **DB row-count verification** across all 18 statistics tables (non-dry-run only)

**EC2 sizing:** Each worker loads a ~300 MB CSV into memory. Recommended:
- `--workers 1`: t3.medium (4 GB) — ~8 hours for 76 scenarios
- `--workers 4`: t3.xlarge (16 GB) — ~2–3 hours for 76 scenarios

**Cloud9 timeout:** Set "Stop my environment" to 4+ hours in Cloud9 Preferences before a full run. Use `screen` so browser disconnects don't kill the process.

### Modules (run in order)

| Order | Module | Script | Database Tables |
|-------|--------|--------|-----------------|
| 1 | **reservoirs** | `main.py` | `reservoir_monthly_percentile`, `reservoir_storage_monthly`, `reservoir_spill_monthly`, `reservoir_period_summary` |
| 2 | **du_urban** | `du_urban/main.py` | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |
| 3 | **mi** | `mi/main.py` | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |
| 4 | **cws_aggregate** | `cws_aggregate/main.py` | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |
| 5 | **ag** | `ag/main.py` | `ag_du_delivery_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary`, `ag_aggregate_monthly`, `ag_aggregate_period_summary` |
| 6 | **refuge** | `refuge/main.py` | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` |
| 7 | **env_flows** | `env_flows/main.py` | `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary` |
| 8 | **delta** | `delta/main.py` | `delta_monthly`, `delta_period_summary` |
| *post* | **sensitivity** | `sensitivity/calculate_sensitivity.py` | `sensitivity_climate`, `sensitivity_operational` |

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
group (scenarios with identical operations but different climate — historical, cc50, cc95),
measures how each metric changes. Stored per (sibling_group, entity, metric, water_month):
- `hist_value`, `cc50_value`, `cc95_value`
- `cc50_abs_change`, `cc95_abs_change` (absolute difference from historical)
- `cc50_pct_change`, `cc95_pct_change` (percent change from historical)

**Operational sensitivity** (`sensitivity_operational` table): For each hydroclimate
level (e.g. all historical-hydrology scenarios), measures how each metric varies across
different operational configurations. Stored per (hydroclimate_id, entity, metric, water_month):
- `scenario_count`, `min_value`, `max_value`, `mean_value`, `std_value`
- `range_value` (max − min), `pct_range` ((max − min) / |mean| × 100)

Both tables include `water_month` 1–12 (monthly resolution) and 0 (annual/period-of-record),
covering reservoirs, AG, urban DU, MI, CWS, refuge, env flows, and delta metrics.

**Querying examples:**
```sql
-- Top 20 metrics most sensitive to climate (cc95 vs historical)
SELECT module, entity_id, metric_name, water_month,
       cc95_pct_change, unit
FROM sensitivity_climate
WHERE cc95_pct_change IS NOT NULL AND water_month > 0
ORDER BY ABS(cc95_pct_change) DESC
LIMIT 20;

-- Top 20 metrics most sensitive to operations (historical climate)
SELECT module, entity_id, metric_name, water_month,
       pct_range, scenario_count, unit
FROM sensitivity_operational
WHERE hydroclimate_id = 2 AND pct_range IS NOT NULL AND water_month > 0
ORDER BY pct_range DESC
LIMIT 20;
```

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
Reliability % = (1 - Average Annual Shortage / Average Annual Delivery) × 100
```

**Example:**
- Average annual delivery = 1,000 TAF
- Average annual shortage = 50 TAF
- Reliability = (1 - 50/1000) × 100 = **95%**

This represents the percentage of requested water that was actually delivered across the simulation period (1922-2021).

### Shortage frequency calculation

**Shortage frequency** is the percentage of years (or months) with a meaningful shortage:

```
Shortage Frequency % = (Years with annual shortage > 0.1 TAF / Total years) × 100
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
Shortage = Delivery Target − Actual Delivery
```

Where **Delivery Target** = Demand × Contract Allocation % (not raw demand).

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
| **Shortage** | Target − Delivery (accounts for allocation %) |
| **Target** | Demand × Allocation % |
| **Reliability** | 1 − (Avg Shortage / Avg Delivery) |

**Note:** Individual DU `GW_SHORT_*` variables represent **groundwater restriction shortage** (a COEQWAL-specific variable for testing groundwater pumping limits), NOT total delivery shortage. For aggregate delivery shortage, use `SHORT_CVP_PAG_*` and `SHORT_SWP_PAG_*`.

**Note:** The COEQWAL Jupyter notebooks back-calculate demand using `Demand = (Shortage + Delivery) / percent_delivery`. Our ETL uses the shortage variables directly without this transformation.

### Prerequisites

- `DATABASE_URL` environment variable set
- CalSim output CSV available in S3: `s3://coeqwal-model-run/scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv`
- Python packages: `pandas`, `numpy`, `psycopg2`, `boto3`

---

## AWS Cheatsheet

Quick reference commands for inspecting and managing the ETL infrastructure.

### Batch compute environment

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
```