# Cloud9 procedures

Copy-paste-ready command blocks for the active threads in
[`TEAM_RUNBOOK.md`](TEAM_RUNBOOK.md) that need execution on the team
Cloud9 instance. Each thread has its own section with pre-checks,
the change, and verification. All commands assume you are logged in
to the shared Cloud9 EC2 instance with the existing IAM role.

These procedures are pure developer harnesses around code and migrations
that already live in the repo. Nothing here changes behavior on its own
- if a step fails its pre-check, stop and triage rather than forcing
through.

## Common setup

Run once per shell session. These assume the standard Cloud9 layout
(`~/environment/coeqwal-backend`) and that `database/setup_db_connection.sh`
exports `DATABASE_URL`.

```bash
cd ~/environment/coeqwal-backend

source database/setup_db_connection.sh

source venv/bin/activate

echo "REPO:      $(pwd)"
echo "GIT BRANCH: $(git rev-parse --abbrev-ref HEAD)"
echo "DB host:    $(psql "$DATABASE_URL" -At -c 'SHOW host' 2>/dev/null || echo 'CHECK DATABASE_URL')"
psql "$DATABASE_URL" -At -c "SELECT current_database(), current_user, version()" \
  | sed 's/^/DB:        /'
```

If the `psql` line errors, fix `DATABASE_URL` before continuing. None of
the threads should proceed without a confirmed connection.

---

## Thread A1: Move CWS reference xlsx into the repo

**Runbook entry.** [`TEAM_RUNBOOK.md`](TEAM_RUNBOOK.md) thread A1.

**Goal.** Move the spring-2026 CWS reference spreadsheets from the
ephemeral Cloud9 `audits/cws/` folder into the tracked
`data/reference/cws/` folder.

**Inputs already in the repo.**

- Target folder: [`data/reference/cws/`](../data/reference/cws/)
  (tracked, with README listing the expected files)
- `.gitignore` whitelists `/data/reference/` and `/data/reference/**`

### Step 1: inventory what is on Cloud9

```bash
cd ~/environment/coeqwal-backend

echo '--- expected target folder ---'
ls -la data/reference/cws/ 2>&1

echo ''
echo '--- source folder on Cloud9 ---'
ls -la audits/cws/ 2>&1
```

**Expect.** Target folder exists with a `README.md`. Source folder
contains the team's xlsx files (filenames may not match the README's
the existing names exactly. That is fine).

### Step 2: move files and stage

```bash
mkdir -p data/reference/cws

mv audits/cws/*.xlsx data/reference/cws/

git add data/reference/cws/

echo '--- post-move target folder ---'
ls -la data/reference/cws/

echo ''
echo '--- git status (should show new xlsx under data/reference/cws/) ---'
git status --short data/reference/cws/ audits/cws/
```

**Expect.** All xlsx now in `data/reference/cws/`. `audits/cws/` is
empty (or gone). `git status --short` shows `A  data/reference/cws/...`
for each xlsx (plus the existing `README.md` and `.gitignore` whitelist
if not yet committed).

### Step 3: verify nothing in `audits/` got accidentally tracked

```bash
git check-ignore -v audits/cws/ 2>&1 || echo '(audits/cws/ is empty or removed)'

git check-ignore -v "data/reference/cws/Final_M&Idemandunits_withlatlongs.xlsx" 2>&1 \
  || echo '(file is tracked - expected)'
```

**Expect.** First command: `audits/cws/` is ignored or missing (good).
Second command: returns nothing because the file is **tracked** (the
`!/data/reference/**` whitelist in `.gitignore` overrides the broader
`/data/` exclusion).

### Step 4: commit

Per project policy, the developer commits. Suggested message:

```
chore(data): track CWS reference xlsx under data/reference/cws/

Moves the spring-2026 community water system reference spreadsheets
from the ephemeral Cloud9 audits/cws/ folder into the tracked
data/reference/cws/ folder. Downstream CSV / DB work is deferred -
this is just the file relocation.
```

---

## Thread A2: Ingest TAIESM1 hydroclimate scenarios

**Runbook entry.** [`TEAM_RUNBOOK.md`](TEAM_RUNBOOK.md) thread A2.

**Goal.** Pull 23 TAIESM1 climate scenarios (`s0107` through `s0131`,
gaps at `s0116` and `s0122`) through the existing Drive -> S3 -> Batch
-> RDS pipeline.

**Inputs already in the repo.**

- Source CSV: [`etl/ingestion/scenario_listing/model_run_file_source_working.csv`](../etl/ingestion/scenario_listing/model_run_file_source_working.csv)
- Pipeline entry: [`etl/ingestion/gdrive_bulk_download.py`](../etl/ingestion/gdrive_bulk_download.py)
  (subcommands: `scan`, `download`, `promote`)
- Audit renderer: [`etl/ingestion/tools/audit.py`](../etl/ingestion/tools/audit.py)
- ETL scenario list refresh: [`etl/ingestion/tools/refresh_etl_scenarios.py`](../etl/ingestion/tools/refresh_etl_scenarios.py)
- Statistics runner: [`etl/statistics/run_all.py`](../etl/statistics/run_all.py)

The 23 scenarios are listed below. Confirm the count and IDs match
the source CSV before any state change.

```bash
TAIESM1_IDS=$(awk -F, '/^s01(0[7-9]|1[0-5]|1[7-9]|2[01]|2[3-9]|3[01]),/ {print $1}' \
  etl/ingestion/scenario_listing/model_run_file_source_working.csv \
  | sort -u | paste -sd, -)
echo "TAIESM1 scenarios ($(echo "$TAIESM1_IDS" | tr ',' '\n' | wc -l)): $TAIESM1_IDS"
```

**Expect.**

```
TAIESM1 scenarios (23): s0107,s0108,s0109,s0110,s0111,s0112,s0113,s0114,s0115,s0117,s0118,s0119,s0120,s0121,s0123,s0124,s0125,s0126,s0127,s0128,s0129,s0130,s0131
```

If the count is not 23 or the IDs are not contiguous (with the two
known gaps `s0116`, `s0122`), stop and triage the working CSV before
proceeding.

### Step 1: pre-flight scan

This reads the working CSV and walks each Drive folder. Read-only, no
downloads, no S3 writes. Fails loudly on CSV problems before consuming
bandwidth.

```bash
python etl/ingestion/gdrive_bulk_download.py scan \
  --scenarios $(echo "$TAIESM1_IDS" | tr ',' ' ')
```

**Expect.** Each row reports the Drive folder contents and the DV / SV
basename match. No `FOLDER_MISMATCH`, no `MISSING_DV`, no
`MISSING_SV`. A known issue: `s0128` has a `FOLDER_MISMATCH` flag in
`etl/ingestion/audit_reports/scan_audit.csv` (the recorded
`drive_folder_name` differs from the actual folder name). Reconcile
that one before downloading.

### Step 2: clear `download_status` and refresh `ETL_SCENARIOS`

The working CSV currently has `download_status=skip` for all 23 TAIESM1
rows. Clear them in-place so the ETL scenario list picks them up.

```bash
python - <<'PY'
import csv
from pathlib import Path

WORKING = Path("etl/ingestion/scenario_listing/model_run_file_source_working.csv")
TAIESM1 = {
    "s0107","s0108","s0109","s0110","s0111","s0112","s0113","s0114",
    "s0115","s0117","s0118","s0119","s0120","s0121","s0123","s0124",
    "s0125","s0126","s0127","s0128","s0129","s0130","s0131",
}

with WORKING.open(newline="") as f:
    rows = list(csv.reader(f))

header, body = rows[0], rows[1:]
dl_idx = header.index("download_status")

cleared = []
for row in body:
    short_code = row[0]
    if short_code in TAIESM1 and row[dl_idx] == "skip":
        row[dl_idx] = ""
        cleared.append(short_code)

with WORKING.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(header)
    w.writerows(body)

print(f"Cleared download_status=skip for {len(cleared)} rows:")
print(", ".join(cleared))
PY
```

**Verify the change.**

```bash
git diff --stat etl/ingestion/scenario_listing/model_run_file_source_working.csv
git diff etl/ingestion/scenario_listing/model_run_file_source_working.csv \
  | grep -E '^[-+]s01' | head -30
```

**Expect.** Diff shows 23 rows with `,skip,` becoming `,,`. The `notes`
column is unchanged.

**Refresh the ETL scenario list:**

```bash
python etl/ingestion/tools/refresh_etl_scenarios.py
```

This regenerates `etl/common/etl_scenarios.py`. The 23 TAIESM1 IDs
should now be in the frozenset. Verify:

```bash
grep -E 's01(0[7-9]|1[0-5]|1[7-9]|2[01]|2[3-9]|3[01])' \
  etl/common/etl_scenarios.py | wc -l
```

**Expect.** `23`

### Step 3: download from Drive into S3 staging

```bash
python etl/ingestion/gdrive_bulk_download.py download \
  --scenarios $(echo "$TAIESM1_IDS" | tr ',' ' ')
```

Per `gdrive_bulk_download.py` docstring, this is skip-not-abort: a
per-row error lands in the audit and the run continues. The end of the
run auto-renders `etl/ingestion/audit.md`.

**Verify the run.**

```bash
cat etl/ingestion/audit.md | sed -n '/^## Run summary/,/^## /p'
cat etl/ingestion/audit.md | sed -n '/needs your attention/,/^## /p'
```

**Expect.** Run summary shows 23 scenarios processed. The "needs your
attention" section should be empty for the 23. If it lists any, those
need triage before promote.

### Step 4: promote staged files to `ready/` (fires Batch)

```bash
python etl/ingestion/gdrive_bulk_download.py promote \
  --scenarios $(echo "$TAIESM1_IDS" | tr ',' ' ')
```

The ZIP `PUT` under `ready/<id>/` is what the Lambda watches. Each
promote fires Batch automatically. Expect the Batch queue to fill
within ~1 minute of the last promote.

### Step 5: wait for Batch and audit extractions

Batch typically takes 5-15 minutes per scenario in parallel. Monitor:

```bash
aws batch list-jobs --job-queue coeqwal-extract-queue --job-status RUNNING
aws batch list-jobs --job-queue coeqwal-extract-queue --job-status FAILED
```

Once Batch is idle, re-render the audit (it cross-references
`ingest_record.json` against `extract_record.json`):

```bash
python etl/ingestion/tools/audit.py
```

**Verify extraction.**

```bash
grep -E 'VALIDATION|extract.*FAIL|MISSING' etl/ingestion/audit.md | head -20
```

**Expect.** No matches. If any scenarios appear, rerun their
extraction with `etl/ingestion/retrigger_extraction.sh <short_code>`.

### Step 6: run statistics ETL per scenario

For each scenario, populate the per-DU / per-reservoir / per-arc tables.
This is the step that writes to RDS. Statistics are upsert-per-scenario
(see thread A7 in the runbook), so reruns overwrite the scenario's rows
rather than duplicating them.

```bash
for id in $(echo "$TAIESM1_IDS" | tr ',' ' '); do
  echo "=== statistics: $id ==="
  python etl/statistics/run_all.py --scenario "$id"
done
```

Or in one shot using `--all-scenarios` (relies on the refreshed
`etl_scenarios.py`, so it will hit every non-skip scenario. Use the
per-scenario loop above if you want to limit to TAIESM1 only):

```bash
python etl/statistics/run_all.py --all-scenarios
```

**Verify per-scenario rows landed.**

```bash
psql "$DATABASE_URL" <<SQL
SELECT s.short_code,
       (SELECT COUNT(*) FROM du_delivery_monthly d
        JOIN scenario sx ON sx.id = d.scenario_id
        WHERE sx.short_code = s.short_code)        AS du_delivery_rows,
       (SELECT COUNT(*) FROM reservoir_storage_monthly r
        JOIN scenario sx ON sx.id = r.scenario_id
        WHERE sx.short_code = s.short_code)        AS reservoir_rows
FROM scenario s
WHERE s.short_code IN (
  $(echo "$TAIESM1_IDS" | sed "s/,/','/g; s/^/'/; s/$/'/")
)
ORDER BY s.short_code;
SQL
```

**Expect.** `du_delivery_rows` and `reservoir_rows` are both non-zero
for every scenario.

### Step 7: Layer-2 and Layer-3 verification

Run the standard verification suite per scenario. The expected counts
and tolerances are documented in
[`etl/verification/README.md`](../etl/verification/README.md).

```bash
for id in $(echo "$TAIESM1_IDS" | tr ',' ' '); do
  echo "=== verify: $id ==="
  python etl/verify_all_sections.py --scenario "$id" || true
done
```

Any FAIL gets a follow-up ticket. Do not block ingest of subsequent
scenarios on one bad row.

### Step 8: frontend availability spot-check

Once statistics land, the scenarios should appear in scenario-listing
API responses. Spot-check (replace `<API_BASE>` with the deployed API):

```bash
curl -s "<API_BASE>/api/scenarios?hydroclimate=COEQWAL%20TAIESM1" \
  | jq -r '.[] | .short_code' \
  | sort -u
```

**Expect.** The 23 TAIESM1 IDs appear. They should be visible in the
*Warmer and Drier III* hydroclimate filter on the public site.

### Step 9: commit

Per project policy, the developer commits the working CSV change and
the regenerated `etl_scenarios.py`. Suggested message:

```
feat(ingestion): activate 23 TAIESM1 hydroclimate scenarios

Clears download_status=skip on s0107-s0131 (except gaps s0116, s0122)
in the working CSV and regenerates etl_scenarios.py to match. Drive,
Batch, statistics, and Layer 2/3 verification were run before this
commit and recorded in etl/ingestion/audit.md.
```

### Known issues

- `s0128` has a `FOLDER_MISMATCH` flag in
  `etl/ingestion/audit_reports/scan_audit.csv`. The Drive folder name
  differs from the recorded `drive_folder_name`. Reconcile before
  promoting.
- `s0107`'s SV file is `SV_COEQWAL_TAIESM1_20260309.dss` while the
  other 22 use `coeqwal_s9999_SV_v*.dss`. Confirm the
  `gdrive_bulk_download.py scan` step in step 1 still matched it
  against the expected basename. If not, the `SV_Path` column in
  the working CSV is the source of truth.

---

## Threads not on this doc

The runbook lists threads A1 through A7 and roadmap items R1, R2.
Threads not in this procedures doc are either team-decision blockers
(no Cloud9 commands to copy), reference material, or rolled back:

- Thread A3 (gw / sw value reconciliation): per-ID team decisions needed
  before any DB write. See [`gw_sw_reconciliation.md`](gw_sw_reconciliation.md).
- Threads A4-A6 (`cvp_total`, master crosswalk, tier coverage gaps):
  team decisions, no Cloud9 commands.
- Thread A7 (statistics correction pattern): reference only - explains
  what the per-scenario ETL writes. Not a task.
- Roadmap R1 (gw/sw BOOLEAN migration) and R2 (DU geometry refactor):
  rolled back in May 2026. Future developers picking these up should
  start from the design notes in
  [`docs/database_geometry_pattern.md`](database_geometry_pattern.md)
  (for R2) and the BOOLEAN section in
  [`docs/gw_sw_reconciliation.md`](gw_sw_reconciliation.md) (for R1),
  not from a stashed procedures block.
