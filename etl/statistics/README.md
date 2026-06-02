# ETL Statistics Pipeline

Calculate and load CalSim model output statistics into the COEQWAL PostgreSQL database, one module per topic.

## Modules at a glance

Nine production modules live under `etl/statistics/`, one subdirectory per topic. Each one reads scenario CSVs from S3, computes derived metrics, and writes them straight to PostgreSQL. They are usually invoked together via `run_all.py`, but each module's `main.py` is independently runnable.

| Module | What it computes | Tables written |
|---|---|---|
| [`reservoirs/`](reservoirs/) | Monthly storage, end-of-April / end-of-September percentiles, spill frequency and volume | `reservoir_storage_monthly`, `reservoir_monthly_percentile`, `reservoir_spill_monthly`, `reservoir_period_summary` |
| [`du_urban/`](du_urban/) | Per-demand-unit delivery, shortage, % demand met, reliability for urban DUs | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |
| [`mi/`](mi/) | M&I contractor delivery, shortage, % demand met, reliability (SWP contractors + MWD aggregate) | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |
| [`cws_aggregate/`](cws_aggregate/) | CWS project / region rollups (SWP, CVP, north / south splits) | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |
| [`ag/`](ag/) | Ag demand (AW), surface-water delivery (DN), groundwater pumping (GP), shortage, reliability, regional aggregates | `ag_du_*_monthly`, `ag_du_period_summary`, `ag_aggregate_*` |
| [`refuge/`](refuge/) | Refuge demand-unit delivery, derived shortage, reliability | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` |
| [`env_flows/`](env_flows/) | River flow metrics: CFS / TAF volumes, % unimpaired, % functional flows, alteration index (Pearson r), CEFF seasonal metrics | `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary` |
| [`delta/`](delta/) | Net Delta Outflow, X2 position (spring / fall), salinity at key stations, Banks / Tracy pumping plant EC | `delta_monthly`, `delta_period_summary` |
| [`sensitivity/`](sensitivity/) | Climate sensitivity (hist vs CC50 vs CC95) and operational sensitivity (cross-scenario spread) across all entities above. *Experimental, under development*: labeled experimental in the script header, no `verify_*` coverage. Run via `run_all.py --with-sensitivity` after the per-scenario modules complete, not as part of the per-scenario loop. | `sensitivity_climate`, `sensitivity_operational` |

Utility code (not a module): `charts/` for visualization helpers, top-level `verify_*.py` / `visualize_*.py` / `scan_dupes.py` scripts for ad-hoc tasks.

> **Output files** - `run_all.py` writes a per-run scorecard to
> `etl/statistics/audit_reports/stats_audit_<ts>.csv`, and `scan_dupes.py` writes
> `etl/statistics/audit_reports/duplicate_scan_results.csv` (+ sibling `_units.csv`).
> The whole `audit_reports/` directory is gitignored. Override locations with
> `--audit-dir` or `-o`. See [`etl/README.md`](../README.md#output-files-audits-generated-sql)
> for the full output catalog.

## Overview

This pipeline processes CalSim model output CSVs stored in S3 to calculate
reservoir statistics. The calculated metrics are loaded directly into
PostgreSQL for the COEQWAL website API.

**Data flow:**
```
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│  S3 bucket       │──▶│  run_all.py      │──▶│  PostgreSQL      │──▶│  API endpoints   │
│  CalSim CSVs     │   │  on Cloud9 EC2   │   │  database        │   │  /api/statistics │
└──────────────────┘   └──────────────────┘   └──────────────────┘   └──────────────────┘
                              │
                              │  manually triggered, see "Running the ETL"
                              │  writes via psycopg2 (direct, no API layer)
```

See "Scenario backfill strategy and pipeline status" below for the full
extraction-to-statistics flow, including where the manual handoff happens.

## Scenario backfill strategy and pipeline status

### Current workflow (manual)

The statistics ETL is a **separate, manually-triggered step** from the extraction pipeline.
The extraction Batch job (`batch_entrypoint.sh`) handles DSS → CSV conversion and uploads to S3,
then writes `SUCCEEDED` to DynamoDB. The statistics ETL (`run_all.py`) must be run separately
in Cloud9 after extraction completes:

```
ZIP dropped in S3
    → Lambda trigger → AWS Batch (batch_entrypoint.sh)
        → DSS → CSV extraction
        → CSV uploaded to S3
        → DynamoDB: status = SUCCEEDED
    ← STOPS HERE - statistics ETL is not triggered automatically

Separately in Cloud9:
    → python etl/statistics/run_all.py --scenario {id}
        → reads CSVs from S3
        → writes statistics to PostgreSQL
```

### ETL module coverage

All six statistics modules must be run for a scenario to have complete data in the website:

| Module | Key tables | Status |
| ------ | ---------- | ------ |
| `reservoirs` | `reservoir_monthly_percentile`, `reservoir_storage_monthly`, `reservoir_spill_monthly`, `reservoir_period_summary` | Production |
| `du_urban` | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` | Production |
| `mi` | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` | Production |
| `cws_aggregate` | `cws_aggregate_monthly`, `cws_aggregate_period_summary` | Production |
| `ag` | `ag_du_delivery_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary`, aggregates | Production |
| `refuge` | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` | Production (added Feb 2026) |
| `env_flows` | River flow metrics (% unimpaired, % functional flows, alteration index) | Production |

### Backfill status

**Current state (April 2026):**
- 76 scenarios have CSVs extracted in S3 (see `ETL_SCENARIOS` in `etl/common/etl_scenarios.py`, regenerated by `etl/ingestion/tools/refresh_etl_scenarios.py`)
- All 8 ETL modules are production-ready
- Grand backfill is in progress (see "Running the ETL" below)

### Verification notes

Two things to know about `verify_all_sections.py`:

- **Two checks are skipped** (logged as warnings, not failures) because a data
  value the ETL loads is unconfirmed. It disagrees with the verifier's
  reference, and confirming it is a question for the Modeling Team: San Luis
  CVP/SWP `pct_capacity`, and the `GDPUD_NU` delivery variable. Background and
  how to resolve: see `docs/statistics_roadmap.md`.
- **Tier checks are opt-in.** Tier results come from a separate ETL, so the tier
  sections only run with `--with-tiers`. The default stats run never touches
  them and exits clean for any scenario.

### Running the ETL

#### Prerequisites

1. **Cloud9 environment** on EC2 (t3a.2xlarge recommended for 4 parallel workers)
2. **IAM instance role** with S3 read access (see "AWS credentials" below)
3. **DATABASE_URL** environment variable set to the PostgreSQL connection string
4. **Python venv** activated: `source ~/environment/coeqwal-backend/venv/bin/activate`

#### Single scenario

```bash
cd ~/environment/coeqwal-backend/etl/statistics
python run_all.py --scenario s0029
```

#### Full batch run (all scenarios)

```bash
python run_all.py --all-scenarios --workers 4 --batch-size 20
```

#### Resuming after interruption

The ETL uses DELETE+INSERT per module per scenario, so re-running a scenario is
idempotent (safe to repeat). Use `--start-from` to skip already-completed scenarios:

```bash
python run_all.py --all-scenarios --workers 4 --batch-size 20 --start-from s0027
```

#### Key flags

| Flag | Purpose |
|------|---------|
| `--workers N` | Parallel scenario processing. See "Choosing `--workers`" below. |
| `--batch-size N` | Split a long run into chunks of N scenarios with a logged checkpoint between batches. Recommended 10-20 for a full backfill. Lets you cleanly resume from the last completed batch with `--start-from` if anything goes wrong mid-run. |
| `--start-from sXXXX` | Skip scenarios before this one (inclusive). For resuming partial runs. |
| `--continue-on-error` | Don't stop on failure. Default is fail-fast. |
| `--only module1,module2` | Run only specific modules (e.g. `--only reservoirs,ag`). |
| `--dry-run` | Calculate but don't write to the database. |

#### Choosing `--workers`

Each worker loads one scenario's CSV (~300MB on disk) and expands it into a
pandas DataFrame plus intermediate arrays, peaking at roughly 2-3 GB of RAM
per worker during computation. Pick `--workers` to fit your environment:

| Environment | Recommended `--workers` |
|---|---|
| Cloud9 `t3a.2xlarge` (32 GB RAM, 8 vCPU) - the standard ETL host | 4 |
| Cloud9 `t3a.xlarge` (16 GB RAM, 4 vCPU) | 2 |
| Cloud9 `t3a.large` or smaller | 1 |
| Developer laptop (any size) | 1 |

To check the instance size you're on:

```bash
nproc                                      # vCPU count
free -h                                    # available RAM
curl -s http://169.254.169.254/latest/meta-data/instance-type   # EC2 instance type
```

Higher worker counts risk OOM (the kernel will SIGKILL Python) or heavy
swapping that slows the run more than it speeds it up. When in doubt,
start with 1, watch `free -h` for a few minutes, then increase.

#### When to use `--batch-size`

For a full backfill (40+ scenarios), always pair `--workers` with
`--batch-size`:

```bash
python run_all.py --all-scenarios --workers 4 --batch-size 15
```

Why batching helps:

1. **Resumability.** Each batch is a clean log checkpoint. If something fails
   in batch 3 of 5, you know to resume with `--start-from sXXXX` at the start
   of batch 3.
2. **Long-run resilience.** AWS SSO and IAM session tokens have expiration
   limits. Breaking a multi-hour run into ~10-15 scenario batches gives you
   natural pause points if a credential needs refreshing.
3. **Easier scorecard reading.** The end-of-batch summary is easier to scan
   than one giant log of 60 scenarios.

#### Using tmux for long runs

Always run inside tmux so the process survives if your browser disconnects:

```bash
tmux new -s etl
```

If you get `server version is too old for client`, kill the stale server first:

```bash
tmux kill-server && tmux new -s etl
```

If tmux is not installed:

```bash
sudo yum install -y tmux
```

**Inside tmux**, activate the venv and run the ETL. Pipe to `tee` so output goes to
both screen and a timestamped log file:

```bash
source ~/environment/coeqwal-backend/venv/bin/activate && cd ~/environment/coeqwal-backend/etl/statistics && python run_all.py --all-scenarios --workers 4 --batch-size 20 --start-from s0027 2>&1 | tee ~/environment/coeqwal-backend/etl_run_$(date +%Y%m%d_%H%M%S).log
```

**Detach** (walk away): press `Ctrl+B` then `D`. The process keeps running.

**Reattach** (check back later): `tmux attach -t etl`

#### Monitoring progress

Reattach to tmux to see live output, or tail the log from a separate terminal:

```bash
tail -f ~/environment/coeqwal-backend/etl_run_*.log
```

Quick count of completed scenarios:

```bash
grep -c 'finished' ~/environment/coeqwal-backend/etl_run_*.log
```

**What to look for in the log:**

| Log pattern | Meaning |
|-------------|---------|
| `[N/68] sXXXX finished` | Scenario completed all 8 modules |
| `BATCH N/M: sXXXX .. sYYYY` | Starting a new batch |
| `✅ Module Name completed successfully` | Individual module success |
| `ABORTING` | ETL stopped due to an error (fail-fast mode) |
| `FAILURE` | A specific module failed for a scenario |
| `Traceback` | Python exception |
| `Total wall-clock time: X.X minutes` | Run finished (success or partial) |

#### Checking for errors

```bash
grep -E 'ABORTING|FAILURE|ERROR|Traceback' ~/environment/coeqwal-backend/etl_run_*.log
```

If this returns nothing, the run is either still going or completed cleanly.

#### Confirming completion from the database

Query the last module (delta) to see which scenarios have been processed:

```bash
psql $DATABASE_URL -c "SELECT DISTINCT scenario_short_code FROM delta_period_summary WHERE created_at >= '2026-04-07' ORDER BY 1;"
```

#### After completion

The log ends with a **scorecard** showing pass/fail for every scenario × module, plus
a CSV audit file (`stats_audit_*.csv`) written to `etl/statistics/audit_reports/` by
default (gitignored). Override the destination with `--audit-dir`.

#### Resuming after a failure

1. Check the log for the last `sXXXX finished` line to find the last completed scenario
2. Look at `scenarios.py` to find the next scenario in order
3. Re-run with `--start-from` set to the next scenario:

```bash
python run_all.py --all-scenarios --workers 4 --batch-size 20 --start-from sXXXX 2>&1 | tee ~/environment/coeqwal-backend/etl_resume_$(date +%Y%m%d_%H%M%S).log
```

### AWS credentials for S3 access

The ETL reads CalSim CSVs from the `coeqwal-model-run` S3 bucket. Cloud9's default
"AWS managed temporary credentials" expire with the user's SSO session, which can
interrupt multi-hour ETL runs.

**Solution: IAM instance role** (set up April 2026)

The Cloud9 EC2 instance uses `AWSCloud9SSMAccessRole` with the `coeqwal-etl-s3-readonly`
policy attached, granting `s3:GetObject` and `s3:ListBucket` on the `coeqwal-model-run`
bucket. To use the instance role instead of SSO credentials:

```bash
# Disable Cloud9 managed credentials (one-time)
aws cloud9 update-environment \
  --environment-id 48dc921ad0fd48ea93c2a2e218bd8ace \
  --managed-credentials-action DISABLE

# Remove stale SSO credential file
rm -f ~/.aws/credentials

# Verify instance role is active
aws sts get-caller-identity
# Should show: assumed-role/AWSCloud9SSMAccessRole/...
```

If you re-authenticate your SSO session and Cloud9 re-enables managed credentials,
repeat the commands above before starting the ETL.

**To restore SSO credentials** for day-to-day work after the ETL:

```bash
aws cloud9 update-environment \
  --environment-id 48dc921ad0fd48ea93c2a2e218bd8ace \
  --managed-credentials-action ENABLE
```

### Website intermediate state

While all ETL modules are now production-ready, scenarios without data for a given section return
empty result sets from the API. The frontend renders an empty/loading state for missing
sections rather than hiding the scenario entirely. Scenarios are only hidden by setting
`is_active = 0` in the `scenario` table, which is reserved for scenarios that are
intentionally excluded (e.g., `s0029`).

### Future: automated post-extraction trigger

The long-term target is to connect extraction completion to automatic statistics ETL
execution. The recommended architecture:

```
DynamoDB status → SUCCEEDED
    → EventBridge rule (on DynamoDB Stream)
        → Lambda submits AWS Batch job
            → python etl/statistics/run_all.py --scenario {id}
```

This has not been implemented yet. Until it is, the manual Cloud9 step is the working path.
When a new scenario ZIP is dropped in S3 and extraction completes, run:

```bash
python run_all.py --scenario {new_scenario_id}
```

---

## Directory Structure

```
etl/statistics/
├── README.md                            # This file
├── dev_run.sh                           # Local development script
├── test_local.py                        # Local test runner
└── reservoirs/                          # Reservoir statistics
    ├── main.py                            # CLI entry point for direct database writes
    ├── calculate_reservoir_statistics.py  # Full statistics for all 92 reservoirs
    ├── calculate_reservoir_percentiles.py # Percentile bands for website charts
    └── reservoir_metrics.py               # Core calculation functions
```

## Scripts

| Script | Purpose |
|--------|---------|
| `reservoirs/main.py` | **CLI entry point** for reservoir statistics writes (invoked by `run_all.py`) |
| `dev_run.sh` | Local development runner for testing with CSV files |
| `test_local.py` | Quick sanity check for individual reservoir calculations |
| `reservoirs/calculate_reservoir_statistics.py` | Calculates all reservoir statistics including probability metrics |
| `reservoirs/calculate_reservoir_percentiles.py` | Percentile band calculation for website charts |
| `reservoirs/reservoir_metrics.py` | Core calculation functions aligned with COEQWAL modeler Jupyter notebooks |

## Unit conversion: CFS to TAF

All modules use the same precise conversion factor:

```
TAF = CFS × DaysInMonth × 0.001983471
```

where `0.001983471 = 86400 / 43560 / 1000` (seconds-per-day / sq-ft-per-acre / kilo-acre-feet).

The V3 Jupyter notebooks use `0.001984` (rounded), which differs by 0.027% - negligible.

Each module derives `DaysInMonth` from `pd.DatetimeIndex.daysinmonth` so leap years
and short months are handled exactly.

---

## Data sources by module

| Module | Delivery source | Demand source | Shortage source | Units (raw) |
|--------|----------------|---------------|-----------------|-------------|
| **Reservoirs** | DV: `S_{code}` (storage) | - | DV: `C_{code}_FLOOD` (spill) | TAF (storage), CFS (spill) |
| **DU Urban** | DV: `DN_*`, `GP_*`, `D_*_PMI` | SV: `UD_*` (TAF) | DV: `SHRTG_*`, `SHORT_D_*_PMI` | CFS |
| **MI Contractors** | DV: `D_*_PMI`, `DEL_SWP_MWD` | Computed: delivery + shortage (via PERDV) | DV: `SHORT_D_*_PMI` | CFS |
| **CWS Aggregate** | DV: `DEL_SWP_PMI`, `DEL_CVP_PMI_*` | DEMANDS CSV | DV: `SHORT_SWP_PMI`, `SHORT_CVP_PMI_*` | CFS |
| **AG** | DV: `DN_*`, `GP_*` | DV: `AW_*` (CFS → TAF) | DV: `SHRTG_*` (Sac) / `GW_SHORT_*` (SJR) | CFS |
| **Env Flows** | DV: `C_{reach}` | - | - | CFS |
| **Refuge** | DV: `DN_*` | DV: `AW_*` (CFS → TAF) | DV: `SHRTG_*` / `GW_SHORT_*` (fallback: `max(AW−DN,0)`) | CFS |

> **Note (March 2026):** AG and Refuge demand was switched from `AWO_*` (SV input, pre-model
> demand order) to `AW_*` (DV output, model-optimised applied water) to match the COEQWAL
> V3 notebook (`DataExtraction.py`). See [Water Balance](#water-balance) below.

### V3 notebook alignment (March 2026 audit)

A thorough review of `COEQWAL_V3/coeqwalpackage/DataExtraction.py` and
`coeqwal/notebooks/coeqwalpackage/DataExtraction.py` confirmed the following
conventions that this ETL follows:

| Convention | V3 Notebook Behaviour | ETL Alignment |
|---|---|---|
| Demand source for ag DUs | `AW_*` from DV (most), a few from SV (`AW_NIDDC_NA3`, `AW_ELDID_NA1`) | ✅ Uses `AW_*` from DV |
| Demand source for refuge DUs | `AW_*` from DV | ✅ Uses `AW_*` from DV |
| GP for refuge DUs | **Not used** - notebooks never reference `GP_*_PR*` | ✅ Not used |
| GP for 11 GW-only _NA DUs | `GP + RU → DN` synthetic delivery | ✅ Does not synthesize delivery. Reports GP separately |
| Water balance check | **Not present** in notebooks | ✅ Checks GP vs AW for ag only; GP/AW up to ~1.15× is expected per WRESL (`AW + RP = DN + GP + RU + SHORTAGE`) |
| Shortage for ag DUs | **Not computed** in notebooks | ETL uses `SHRTG_*` (Sac) and `GW_SHORT_*` (SJR/Tulare) - full coverage |
| Shortage for refuge DUs | **Not computed** in notebooks | ETL uses model `SHRTG_*`/`GW_SHORT_*` when available, falls back to `max(AW−DN,0)` |
| Shortage for M&I | `SHORT_*` used as intermediates for demand back-calculation, then dropped | ETL uses `SHORT_*` directly |
| CFS→TAF constant | `0.001984` (coeqwal) / `0.0019834714` (V3) | ✅ Uses `86400/43560000 ≈ 0.001983471` |
| DU type classification | Name-based (`UD` prefix = urban. Everything else = DV list). No programmatic type filtering | ✅ Filters ag DUs via entity table |

---

## Reservoir capacity overrides

For percent-of-capacity calculations, capacity should come from the highest `S_{code}LEVELxDV`
variable in the DV file. Four major reservoirs have their top-level variable absent from the DV
output. Their capacities are hardcoded from V3's `DataExtraction.py`:

| Reservoir | Entity CSV | V3 Hardcoded | Variable (absent) |
|-----------|-----------|-------------|-------------------|
| Folsom | 975 TAF | **967 TAF** | `S_FOLSMLEVEL6DV` |
| Millerton | 520 TAF | **524 TAF** | `S_MLRTNLEVEL5DV` |
| Oroville | 3537 TAF | **3424.8 TAF** | `S_OROVLLEVEL6DV` |
| New Melones | 2400 TAF | **2420 TAF** | `S_MELONLEVEL5DV` |

These overrides are applied in `CAPACITY_OVERRIDES` in both
`calculate_reservoir_statistics.py` and `calculate_reservoir_percentiles.py`.

For spill, the flood control level (one zone below capacity) is used as the
threshold for flood pool probability. See `RESERVOIR_THRESHOLDS` in
`reservoir_metrics.py` for the full 92-reservoir mapping.

---

## Calculation methodology

All calculations are aligned with the COEQWAL modeler Jupyter notebooks located at https://github.com/maramahmedd/coeqwal. See:
- `coeqwal/notebooks/coeqwalpackage/metrics.py`
- `coeqwal/notebooks/Metrics.ipynb`

### 1. Percentile bands

**Purpose**: Supply data for the reservoir storage percentile band charts on the website.

**Implementation here** (`calculate_reservoir_percentiles.py:293-294`):
```python
for p in PERCENTILES:  # [0, 10, 30, 50, 70, 90, 100]
    stats[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)
```

**Notebook reference** (`metrics.py:399-400`):
```python
iqr_values = subset_df.apply(lambda x: x.quantile(q), axis=0)
```

**Comparison**:

| Aspect | Our ETL | Notebook |
|--------|---------|----------|
| Method | `np.percentile()` | `pandas.quantile()` |
| Interpolation | Linear (NumPy default) | Linear (Pandas default) |
| Grouping | By **water month** (Oct=1 ... Sep=12) | By entire time series |

**Note**: Here we group by water month before calculating percentiles for the purposes of showing range and variability by month in charts on the website. Both methods produce mathematically equivalent percentile values. The grouping by month is a deliberate design choice for the frontend website visualization.

### 2. Flood pool probability

**Definition**: Probability that storage reaches or exceeds the flood control level.

**Implementation** (`reservoir_metrics.py:calculate_flood_pool_probability()`):
```python
# Flood pool hit when: storage >= threshold
difference = storage.values - flood_threshold
difference = difference + 0.000001  # Small epsilon for >= comparison
hit_count = int((difference >= 0).sum())
probability = hit_count / total_count
```

**Notebook reference** (`metrics.py:617-655 frequency_hitting_level()`):
```python
subset_df_res_comp_values = subset_df_res.values - subset_df_floodzone.values
if floodzone:
    subset_df_res_comp_values += 0.000001
exceedance_days = count_exceedance_days(subset_df_res_comp, 0)
```

**Threshold sources** (from `Metrics.ipynb`):

| Reservoir | Flood Threshold Variable |
|-----------|-------------------------|
| SHSTA | `S_SHSTALEVEL5DV` (model output) |
| OROVL | `S_OROVLLEVEL5DV` (model output) |
| TRNTY | `S_TRNTYLEVEL5DV` (model output) |
| FOLSM | `S_FOLSMLEVEL5DV` (model output) |
| MELON | `S_MELONLEVEL4DV` (model output) |
| MLRTN | 524 TAF (constant) | <- question
| SLUIS_CVP | `S_SLUIS_CVPLEVEL5DV` (model output) |
| SLUIS_SWP | `S_SLUIS_SWPLEVEL5DV` (model output) |

### 3. Dead pool probability

**Definition**: Probability that storage drops to or below the dead pool (minimum operating level).

**Implementation** (`reservoir_metrics.py:calculate_dead_pool_probability()`):
```python
# Dead pool hit when: storage <= threshold
difference = storage.values - dead_pool_threshold
hit_count = int((difference <= 0).sum())
probability = hit_count / total_count
```

**Notebook reference**: Same `frequency_hitting_level()` with `floodzone=False`.

**Threshold Sources**:

| Reservoir | Dead Pool Threshold |
|-----------|---------------------|
| SHSTA | `S_SHSTALEVEL1DV` (model output) |
| OROVL | `S_OROVLLEVEL1DV` (model output) |
| TRNTY | `S_TRNTYLEVEL1DV` (model output) |
| FOLSM | `S_FOLSMLEVEL1DV` (model output) |
| MELON | 80 TAF (constant) |
| MLRTN | 135 TAF (constant) |
| Others | `dead_pool_taf` from `reservoir_entity.csv` |

### 4. Coefficient of variation (CV)

Measure of relative variability.

**Formula**: `CV = standard_deviation / mean`

**Implementation** (`reservoir_metrics.py:calculate_cv()`):
```python
mean = float(data.mean())
std = float(data.std())
return std / mean if mean != 0 else 0
```

**Notebook reference** (`metrics.py:383-393 compute_cv()`):
```python
cv = (subset_df.std(axis=0) / subset_df.mean(axis=0))
```

**Metrics calculated**:
- `storage_cv_all`: CV across entire period
- `storage_cv_april`: CV for April values only (spring storage)
- `storage_cv_september`: CV for September values only (end of water year)

### 5. Annual average

**Definition**: Mean of annual means. Calculates yearly average, then averages across years.

**Implementation** (`reservoir_metrics.py:calculate_annual_average()`):
```python
# Calculate mean for each water year
annual_means = data.groupby('WaterYear')['value'].mean()
# Return mean of annual means
return float(annual_means.mean())
```

**Notebook reference** (`metrics.py:526-534 ann_avg()`):
```python
metric_value = compute_mean(df, var_name, [study_index], units, months=months)
# where compute_mean groups by WaterYear and calculates mean of means
```

### 6. Monthly average

**Definition**: Mean value for a specific calendar month across all years.

**Implementation** (`reservoir_metrics.py:calculate_monthly_average()`):
```python
month_mask = date_index.month == month
month_data = values.loc[month_mask].dropna()
return float(month_data.mean())
```

**Notebook reference** (`metrics.py:545-554 mnth_avg()`):
```python
metric_value = compute_mean(df, var_name, [study_index], units, months=[mnth_num])
```

**Metrics calculated**:
- `april_avg_taf`: Average April storage (spring peak)
- `september_avg_taf`: Average September storage (end of water year)

---

## Database tables

### reservoir_monthly_percentile
Monthly percentile bands for UI charts.

| Column | Description |
|--------|-------------|
| `scenario_short_code` | e.g., "s0020" |
| `reservoir_entity_id` | FK to reservoir_entity |
| `water_month` | 1-12 (Oct=1, Sep=12) |
| `q0, q10, q30, q50, q70, q90, q100` | Percentiles (% of capacity) |
| `mean_value` | Mean storage (% of capacity) |

### reservoir_storage_monthly
Monthly storage statistics including CV.

### reservoir_spill_monthly
Monthly spill/flood release statistics.

### reservoir_period_summary
Period-of-record summary with probability metrics.

| Column | Description |
|--------|-------------|
| `flood_pool_prob_all` | P(storage >= flood level), all months |
| `flood_pool_prob_september` | P(storage >= flood level), September only |
| `flood_pool_prob_april` | P(storage >= flood level), April only |
| `dead_pool_prob_all` | P(storage <= dead pool), all months |
| `dead_pool_prob_september` | P(storage <= dead pool), September only |
| `storage_cv_all` | Coefficient of variation, all months |
| `storage_cv_april` | CV for April |
| `storage_cv_september` | CV for September |
| `annual_avg_taf` | Mean of annual mean storage |
| `april_avg_taf` | Mean April storage |
| `september_avg_taf` | Mean September storage |

---

## Target reservoirs

The ETL processes **all 92 reservoirs** defined in the reservoir_entity.csv file. This includes major reservoirs such as:

| Code | Reservoir | Capacity (TAF) | Dead Pool (TAF) |
|------|-----------|----------------|-----------------|
| SHSTA | Shasta | 4,552 | 115 |
| TRNTY | Trinity | 2,448 | 105 |
| OROVL | Oroville | 3,537 | 850 |
| FOLSM | Folsom | 975 | 115 |
| MELON | New Melones | 2,400 | 300 |
| MLRTN | Millerton | 520 | 115 |
| SLUIS_CVP | San Luis (CVP) | 1,062 | 15 |
| SLUIS_SWP | San Luis (SWP) | 979 | 10 |

For the complete list of all 92 reservoirs, see: `database/seed_tables/04_calsim_data/reservoir_entity.csv`

---

## Usage

### Automated

Use `reservoirs/main.py` for direct database writes:

```bash
# Process single scenario and write to database
DATABASE_URL=postgres://... python reservoirs/main.py --scenario s0020

# Process all scenarios
DATABASE_URL=postgres://... python reservoirs/main.py --all-scenarios

# Dry run (calculate without writing)
python reservoirs/main.py --scenario s0020 --dry-run

# Output as JSON (for debugging)
python reservoirs/main.py --scenario s0020 --output-json
```

### Manual (via direct SQL)

For manual review or custom loading:

```bash
# Generate SQL file
python calculate_reservoir_statistics.py --scenario s0020 --output-sql output.sql

# Load to database
psql $DATABASE_URL -f output.sql
```

### Percentiles only

```bash
python calculate_reservoir_percentiles.py --scenario s0020 --output-sql percentiles.sql
```

---

## Relevant S3 bucket structure

```
s3://coeqwal-model-run/
├── reference/
│   └── all_metrics_output.csv       # Verification reference from Metrics.ipynb
└── scenario/
    └── {scenario_id}/
        └── csv/
            └── {scenario_id}_coeqwal_calsim_output.csv
```

---

## CSV input format

CalSim DSS-export CSV with 7 header rows:

```
Row 0 (a):     CALSIM, CALSIM, ...          (source)
Row 1 (b):     S_SHSTA, S_SHSTALEVEL5DV, ... (variable names)
Row 2 (c):     STORAGE, STORAGE-LEVEL5, ... (descriptions)
Row 3 (e):     1MON, 1MON, ...              (time step)
Row 4 (f):     L2020A, L2020A, ...          (dataset)
Row 5 (type):  PER-AVER, PER-AVER, ...      (data type)
Row 6 (units): TAF, TAF, ...                (units)
Row 7+:        1921-10-31, 1234.5, ...      (data)
```

**Variables loaded**:
- `S_{code}`: Storage (TAF)
- `C_{code}_FLOOD`: Flood release (CFS)
- `S_{code}LEVEL5DV`: Flood control level (TAF)
- `S_{code}LEVEL1DV`: Dead pool level (TAF)

---

## Verification

Reservoir statistics are verified end-to-end alongside every other section
by [`etl/statistics/verify_all_sections.py`](verify_all_sections.py)
(Layer 2). See [`etl/verification/README.md`](../verification/README.md)
for the full layered walkthrough, tolerances, and known notes (including
the SLUIS monthly-average discrepancy carried forward from the original
notebook comparison).

---

## Local Development

### Prerequisites

```bash
# Python 3.9+
python --version

# Install dependencies
pip install pandas numpy psycopg2-binary boto3
```

### Running Locally

**1. Get a sample CSV file**

Place a CalSim output CSV in the `etl/reference/` directory:
```bash
# If you have AWS access:
aws s3 cp s3://coeqwal-model-run/scenario/s0020/csv/s0020_coeqwal_calsim_output.csv ../reference/

# Or use any existing CSV with the 7-header format
ls ../reference/*.csv
```

**2. Run with dry-run (no database required)**

```bash
cd /path/to/coeqwal-backend/etl/statistics

# Dry run - calculates metrics, prints summary, no database writes
python reservoirs/main.py --scenario s0020 --csv-path ../reference/s0020_coeqwal_calsim_output.csv --dry-run

# With JSON output for debugging
python reservoirs/main.py --scenario s0020 --csv-path ../reference/s0020_coeqwal_calsim_output.csv --dry-run --output-json
```

**3. Run quick tests**

```bash
# Test imports and basic calculations
python test_local.py
```

**4. Use the dev script**

```bash
# Runs common development scenarios
./dev_run.sh
```
---

## Troubleshooting

### "No storage columns found"
- Verify CSV has 7 header rows
- Check variable names in row 1 match: `S_SHSTA`, `S_TRNTY`, etc.

### "Threshold column not found"
- Not all scenarios include LEVEL5DV/LEVEL1DV columns
- The ETL falls back to constant thresholds for these reservoirs

### S3 Access Errors
- Configure AWS credentials: `aws configure`
- Set bucket: `export COEQWAL_S3_BUCKET=coeqwal-model-run`

### Skipped Reservoirs
Two reservoirs are skipped (no storage data in CalSim CSV):
- **EBMUD** - EBMUD Terminal Reservoirs
- **RELIE** - Relief Reservoir

---

## Urban Demand Unit Statistics

The `du_urban/` module calculates delivery statistics for the 71 urban demand units in the tier matrix.

### Unit Conversion: CFS to TAF

CalSim outputs demands and deliveries in **CFS** (cubic feet per second). We convert to **TAF** (thousand acre-feet) using the formula from the COEQWAL notebook (`metrics.py`):

```python
TAF = CFS × 0.001984 × days_in_month
```

| Parameter | Value | Notes |
|-----------|-------|-------|
| Conversion factor | `0.001984` | Acre-feet per CFS per day (÷1000 for TAF) |
| Days in month | 28-31 | Calculated per row, handles leap years |

**Derivation:** 1 CFS for 1 day = 86,400 ft³ ÷ 43,560 ft³/acre = 1.9835 acre-feet ≈ 0.001984 TAF

### Calculated Metrics

| Metric | Formula | Units |
|--------|---------|-------|
| `demand_taf` | `SWDEM × 0.001984 × days` | TAF |
| `delivery_taf` | `DN × 0.001984 × days` | TAF |
| `percent_delivered` | `(delivery_taf / demand_taf) × 100` | % |

### Demand Variable Availability by Category

| Category | Units | Demand Variable | Source |
|----------|-------|-----------------|--------|
| **WBA-style** | 40 | `DN_{zone}` (e.g., `DN_02_PU`) | ✅ Main CalSim output |
| **GW-only** | 3 | N/A | No surface demand (GW only) |
| **SWP contractors** | 11 | `DEM_D_*_PMI` (e.g., `DEM_D_SBA029_ACWD_PMI`) | ⚠️ DEMANDS CSV only |
| **Named localities** | 15 | `UD_*` (e.g., `UD_NAPA`, `UD_BNCIA`) | ⚠️ DEMANDS CSV only |
| **MWD** | 1 | `D_MWD_PMI` or `TABLEA_CONTRACT_MWD` | ⚠️ DEMANDS CSV only |
| **Missing** | 2 | N/A | No CalSim data |

### Data Source Summary

| Source | Content | Coverage |
|--------|---------|----------|
| **Main CalSim output** | `DN_*` (WBA demand), `SUMUD_*` (some localities) | ~44 units |
| **DEMANDS CSV** | `DEM_D_*_PMI`, `UD_*` (all urban demands) | All 71 units |
| **WRESL lookup table** | Static demand patterns | Template only |

### Key Points

- **40 WBA units**: Demand is in the main CalSim output (`DN_*`) - directly usable
- **26 other units**: Demand requires loading the separate `*_DEMANDS.csv` file
- **5 units**: No demand data (3 GW-only + 2 missing)

### Files

| File | Purpose |
|------|---------|
| `du_urban/main.py` | CLI entry point (thin wrapper that calls `calculate_du_statistics_v2.main()`) |
| `du_urban/calculate_du_statistics_v2.py` | Main calculation module: reads CalSim DV CSV, applies unit-aware CFS->TAF conversion, writes per-DU monthly and period-summary tables |

---

## M&I Contractor Statistics

The `mi/` module calculates delivery and shortage statistics for SWP (State Water Project) and CVP (Central Valley Project) M&I contractors.

### CalSim Variable Naming Convention

CalSim uses a structured naming convention for delivery variables:

```
D_{location}_{contractor}_{type}
```

| Suffix | Meaning | Description |
|--------|---------|-------------|
| `_PMI` | **Project M&I** | Table A allocation for Municipal & Industrial use |
| `_PAG` | Project Ag | Table A allocation for Agricultural use |
| `_PIN` | Project Interruptible | Article 21 / surplus water (when available) |
| `_PCO` | Project Carryover | Water banked from previous year's unused allocation |
| `_PRJ` | Project Total | Sum of all delivery types |

**Example for Desert Water Agency:**
```
D_ESB408_DESRT       ← Total deliveries (all types)
D_ESB408_DESRT_PMI   ← Table A M&I allocation only (what we track)
D_ESB408_DESRT_PIN   ← Interruptible/Article 21
D_ESB408_DESRT_PCO   ← Carryover from previous year
D_ESB408_DESRT_PRJ   ← Project total
```

### Why We Use `_PMI` Variables

We track `_PMI` (Project M&I) variables specifically, NOT total deliveries (`_PRJ`). This is intentional:

1. **Scenario Comparison**: COEQWAL scenarios compare SWP reliability. Table A allocations (`_PMI`) show how allocation policies affect contractors, while total deliveries include carryover and interruptible water that obscure policy impacts.

2. **Shortage Pairing**: Shortage variables (`SHORT_D_xxx_PMI`) are calculated against M&I demand. Using `_PRJ` delivery but `_PMI` shortage would produce inconsistent metrics.

3. **Model Intent**: The CalSim model tracks Table A allocations to measure SWP reliability. Zeros in `_PMI` during dry years are the model's way of showing "100% allocation cut" scenarios.

### Reference Sources

Variable mappings come from:

| Source | Location | Content |
|--------|----------|---------|
| `swp_contractor_perdel_A.wresl` | CalSim model files | Contractor delivery logic definitions |
| `CWS_shortage_variables.csv` | `etl/pipelines/CWS/` | Shortage variable list from DWR/COEQWAL |
| `mi_contractor.csv` | `database/seed_tables/` | Contractor metadata (names, contracts) |

### Understanding Zero Values in Percentiles

When `q0` (minimum/0th percentile) = 0 for a contractor-month, it means:

> **In at least one year of the 100-year simulation, that contractor received zero Table A M&I allocation for that month.**

This is **legitimate model behavior**, not a data error:

| Pattern | Meaning |
|---------|---------|
| `q0=0, q10>0` | Worst ~10% of years had zero delivery |
| `q0=0, q10=0, q50>0` | Worst ~50% of years had zero delivery |
| `q0=0, q10=0, q50=0, avg>0` | Most years had zero, but wet years brought up average |

**Example - Coachella Valley WD (CCHLA):**
```
Month 1 (Oct): q0=0, q10=30, q50=133, avg=126 TAF
Month 7 (Apr): q0=0.4, q10=4, q50=18, avg=24 TAF
```

Interpretation: In October during dry years, Table A allocations can be cut to 0%. In April (spring), even dry years get some water (minimum 0.4 TAF).

### Alternative: Total Deliveries

If you need "total water received regardless of allocation type," use `D_{loc}_{contractor}` or `D_{loc}_{contractor}_PRJ` variables instead. However:

- This would require modifying `MI_CONTRACTOR_VARIABLES` in `calculate_mi_statistics.py`
- Shortage metrics would need recalculation or removal
- The interpretation changes from "allocation reliability" to "total supply"

### Unit Conversion: Demands Are Already in TAF

**IMPORTANT**: Unlike delivery variables (which are in CFS and need conversion), the **demand variables in the DEMANDS CSV are already in TAF**.

| Data Type | Source | Units | Conversion |
|-----------|--------|-------|------------|
| Deliveries | Main CalSim output | CFS | `TAF = CFS × 0.001984 × days_in_month` |
| Demands | DEMANDS CSV | **TAF** | None needed |
| Shortages | Main CalSim output | TAF | None needed |

This was identified in February 2026 when MWD showed `annual_demand_avg_taf = 14` and `avg_pct_demand_met = 1700%`. Investigation revealed:

1. The demand variable `TABLEA_CONTRACT_MWD` had values of ~230-240 (already TAF)
2. The code was incorrectly multiplying by `days × 0.001984` (treating as CFS)
3. Result: 235 × 0.001984 × 30 ≈ 14 (far too small)

**Fix applied**: Removed CFS→TAF conversion for demand variables in:
- `calculate_contractor_statistics()` - annual demand calculation
- Monthly demand calculations

**Expected values after fix**:
- MWD demand: ~230-240 TAF/year (not 14)
- Percent of demand met: ~100% (not 1700%)

### Files

| File | Purpose |
|------|---------|
| `mi/calculate_mi_statistics.py` | Main calculation module |
| `mi/MI_CONTRACTOR_VARIABLES` | Built-in variable mappings (dict in code) |

---

## Agricultural Demand Unit Statistics

The `ag/` module calculates demand, delivery, pumping, and shortage statistics for 144 agricultural demand units.

### Data source: DV output only

All AG variables come from a single file - the CalSim DV output CSV
(`{scenario}_coeqwal_calsim_output.csv`). The SV input CSV is **not** loaded.

| CalSim Variable | Description | Raw Unit | Conversion |
|-----------------|-------------|----------|------------|
| `AW_{DU_ID}` | Applied Water = **Demand** | CFS | `TAF = CFS × days × 0.001984` |
| `DN_{DU_ID}` | Net Delivery = **Surface Water Delivery** | CFS | same |
| `GP_{DU_ID}` | Groundwater Pumping | CFS | same |
| `SHRTG_{DU_ID}` | Shortage (Sacramento, kind=`SHORTAGE`) | CFS | same |
| `GW_SHORT_{DU_ID}` | GW Restriction Shortage (SJR/Tulare, kind=`GW-RESTRICT-SHORT`) | CFS | same |
| `DEL_SWP_PAG`, `SHORT_CVP_PAG`, … | Project-level aggregate delivery / shortage | CFS | same |

### Water balance

The actual CalSim 3 demand-unit water balance (from WRESL `constraints-Deliveries.wresl`)
is:

```
AW + RP = DN + GP + RU + SHORTAGE
```

| Variable | Meaning | WRESL definition |
|---|---|---|
| **AW** | Applied Water (crop demand) = AWR + AWO | timeseries input (CFS, converted from TAF) |
| **RP** | Riparian / misc ET = AW × RPF | typically 5-15% of AW |
| **DN** | Net Delivery = DG − DL (gross diversion minus conveyance losses) | DL = EV + DP + LF + OS |
| **GP** | Groundwater Pumping | decision variable, bounded by GPmin/GPmax |
| **RU** | Reuse = min(TW, RUFR×AWR + RUFO×AWO) | bounded by available tailwater |
| **SHORTAGE** | SHRTG (Sac) or GW_SHORT (SJR/Tulare) | slack variable for unmet demand |

Source: `Run/System/SystemTables_Sac/constraints-Deliveries.wresl` line 39:
```
goal meetAW_02_NA  {AW_02_NA + RP_02_NA = DN_02_NA + GP_02_NA + RU_02_NA + SHRTG_02_NA}
```

For GW-only DUs (no surface delivery), DN drops out:
```
goal meetAW_07N_NA  {AW_07N_NA + RP_07N_NA = GP_07N_NA + RU_07N_NA + SHRTG_07N_NA}
```

**GP is explicitly allowed to exceed AW.** The GP upper bound (from
`constraints-gwpumping.wresl`) is:

```
GP <= GPmax × AW × (1 + RPF − RUF)
```

Since RPF > RUF typically, the factor `(1 + RPF − RUF)` is > 1.0. GP/AW ratios
of 1.0-1.15× are expected and reflect that GP must also supply RP (riparian losses)
beyond crop demand. The s0020 dry run's GP/AW ratios of 1.0-1.1× are consistent
with this.

**Note:** The WRESL water balance IS the same for refuge DUs (`AW + RP = DN + GP + RU + SHORTAGE`),
but the V3 notebooks never use GP for refuge DUs. The AG module filters refuge DUs out
before running water balance checks.

### WRESL verification (March 2026)

All ETL equations were verified against the CalSim 3 WRESL model files
(`reference/s0020_DCRadjBL_2020LU_wTUCP/Run/`):

| Variable | WRESL Declaration | Kind | Units | ETL Handling |
|----------|------------------|------|-------|-------------|
| `AW_{DU}` | `std` dvar, AW = AWR + AWO (+AWW) | `APPLIED-WATER` | CFS | ✅ Correct |
| `DN_{DU}` | `std` dvar, DN = DG − DL | `SW-DELIVERY-NET` | CFS | ✅ Correct |
| `GP_{DU}` | `std` dvar | `GW-PUMPING` | CFS | ✅ Correct |
| `SHRTG_{DU}` | `std` dvar (Sacramento) | `SHORTAGE` | CFS | ✅ Added |
| `GW_SHORT_{DU}` | bounded dvar (SJR/Tulare) | `GW-RESTRICT-SHORT` | CFS | ✅ Correct |
| `RU_{DU}` | `std` dvar | `REUSE` | CFS | (not directly used) |
| `S_{code}` | `std` dvar | `STORAGE` | **TAF** | ✅ No conversion needed |
| `C_{code}_FLOOD` | `std` dvar | `SPILL` | CFS | ✅ Correct |
| `D_*_PMI` | `std` dvar | `FLOW-DELIVERY` | CFS | ✅ Correct |
| `SHORT_D_*_PMI` | `alias` (post-solve) | `delivery-shortage` | CFS | ✅ Correct |
| `PERDV_SWP_*` | `alias` of perdel_N | `swp-output` | percent (fraction 0-1) | ✅ Not converted |
| `DEL_*` aggregates | `alias` | `delivery-cvp/swp` | CFS | ✅ Correct |
| `UD_{DU}` | `timeseries` (SV input) | `URBAN-DEMAND` | TAF → CFS | (used by DU Urban) |

Key findings:
- Urban water balance is simpler than AG: `UD = DN + GP + SHRTG` (no RP, no RU)
- `nod_ag` / `sod_ag` are valid ETL-only aggregations (no WRESL equivalent)
- MWD Table A = 1911.5 TAF/yr confirmed from V3 DataExtraction.py
- MI demand formula `(delivery + shortage) / PERDV` is algebraically correct per WRESL

### AWO vs AW: demand variable choice

The SV input CSV contains `AWO_*` (Applied Water Order) - the pre-model demand
*target*. The DV output contains `AW_*` (Applied Water) - the model's optimised
water application. `AWO > AW` in most months because the model may not fully meet
the order.

The COEQWAL V3 notebook (`DataExtraction.py`) uses `AW_*` from the DV output as
the demand variable for agricultural demand units. This ETL follows that
convention. The switch from `AWO_*` (SV) to `AW_*` (DV) was made in March 2026.

### Groundwater-only demand units

18 DUs have no `DN` term in their WRESL `meetAW` constraint - their entire
supply is GP + RU. CalSim does not produce a `DN_*` output for them, so the
ETL does **not** synthesise a surface water delivery value.

| Region | GW-only DUs |
|--------|-------------|
| Sacramento (9) | `06_NA`, `07N_NA`, `07S_NA`, `15N_NA1`, `15S_NA1`, `16_NA1`, `17N_NA`, `20_NA2`, `26N_NA` |
| SJR/Tulare (9) | `60S_NA1`, `60S_NA2`, `61_NA1`, `62_NA1`, `63_NA1`, `64_NA1`, `72_NA2`, `73_NA` |

Note: `26S_NA` is commented out in WRESL (moved to Lower Mokelumne system).

The V3 notebook lists 11 of these (without 26N_NA and the 7 SJR DUs) and gives
them a synthetic `DN = GP + RU` column labelled `SW_DELIVERY-NET`. The V3 GP
and RU columns are then dropped as intermediate variables.

### Shortage - two variable families by region

The WRESL model defines shortage as the slack variable in the `meetAW`
water balance constraint. Two naming conventions exist by region:

| Region | Variable | WRESL kind tag | Columns in DV |
|--------|----------|---------------|---------------|
| Sacramento (WBAs 02-26) | `SHRTG_{DU_ID}` | `SHORTAGE` | ~185 |
| SJR/Tulare (WBAs 50-91) | `GW_SHORT_{DU_ID}` | `GW-RESTRICT-SHORT` | ~89 |

Both represent the same concept - unmet demand after DN, GP, and RU.
The ETL detects the correct variable for each DU based on its WBA.

### Files

| File | Purpose |
|------|---------|
| `ag/calculate_ag_statistics.py` | Main calculation module |
| `ag/main.py` | CLI entry point |

---

## Wildlife Refuge Statistics

The `refuge/` module calculates delivery, shortage, and reliability statistics for 18 wildlife
refuge demand units.

### Data source: DV output only

All variables are loaded from the DV output CSV:
- `AW_*` - demand
- `DN_*` - delivery
- `SHRTG_*` (Sacramento _PR DUs) / `GW_SHORT_*` (SJR/Tulare _PR DUs) - shortage

### Shortage: model variables preferred

The WRESL model defines the same `meetAW` constraint for refuge DUs as for AG DUs:
`AW + RP = DN + GP + RU + SHORTAGE`. Actual shortage variables exist for all 18
refuge DUs in the DV output:

| Region | Variable | DUs |
|--------|----------|-----|
| Sacramento | `SHRTG_{DU_ID}` | 08N_PR1, 08N_PR2, 08S_PR, 09_PR, 11_PR, 17N_NR, 17N_PR, 17S_PR |
| SJR/Tulare | `GW_SHORT_{DU_ID}` | 63_PR1-3, 72_PR1-6, 91_PR |

The ETL uses these model-computed shortage values when available. If a DU's
shortage column is missing, it falls back to `max(AW − DN, 0)`.

### Why GP is not used for refuge DUs

CalSim 3 *does* output `GP_*` columns for refuge DUs, and the WRESL water
balance is identical to AG. However, the V3 notebook (`DataExtraction.py`)
**never uses GP or RU for any `_PR` DU**, so the refuge module follows that
convention. The `validate_water_balance` check is skipped for refuge DUs.

### DU overlap with AG module

The DV output CSV contains `AW_*` columns for all demand unit types (ag, refuge, urban).
The AG module filters its DU list against `du_agriculture_entity.csv` to avoid
accidentally processing refuge DUs, which would produce incorrect results due to
the different water accounting frameworks.

### Files

| File | Purpose |
|------|---------|
| `refuge/calculate_refuge_statistics.py` | Main calculation module |

---

## Data integrity safeguards

Three automated checks run during every ETL execution. They **warn** (log) but do
**not** clamp or discard data, so suspicious values remain visible for investigation.

| Safeguard | Where | What it checks |
|-----------|-------|----------------|
| `validate_water_balance` | AG only (after CFS→TAF conversion) | `GP_{DU} ≤ AW_{DU} × 1.01` for ag DU-months. GP/AW ratios of 1.0-1.15× are expected due to riparian losses (RP) per WRESL `constraints-gwpumping.wresl`. **Not applied to refuge DUs.** |
| `check_post_conversion_magnitude` | AG, MI, Refuge (after CFS→TAF conversion) | Max monthly TAF value < 2 000 per column. Values above this strongly suggest a double conversion or a missed CFS→TAF step. |
| `safe_pct` | AG, MI period summaries | Percentage result > 200 % triggers a warning. Catches cases where numerator/denominator have different units. |
| AG DU filtering | AG (before computing statistics) | DU IDs discovered from `AW_*` columns are filtered against `du_agriculture_entity.csv`. Non-ag DUs (refuges, urban) that happen to have `AW_*` columns in the DV output are excluded to prevent cross-contamination. |

These functions live in `etl/statistics/units.py` and are imported by each module.
Thresholds are defined as constants (`PCT_WARNING_THRESHOLD`, `MONTHLY_TAF_SANITY_LIMIT`)
and can be adjusted without changing module code.

### Unit-aware CSV loading

The shared helper `parse_dss_csv_header()` in `units.py` reads the 7-row DSS
header to extract variable names (row 1) and units (row 6).  Each loader builds
a `units_map = dict(zip(var_names, units_row))` so the caller knows which
columns are CFS vs TAF *before* applying any conversion.

> **Note:** An earlier version included a `deduplicate_columns()` helper that
> resolved duplicate CFS/TAF columns.  Diagnostics on the S3 CSVs confirmed
> **zero duplicate columns**, so the deduplication logic was removed.

---

## Appendix: Complete Variable Reference (WRESL-verified, March 2026)

All variable declarations verified against CalSim 3 WRESL files in
`reference/s0020_DCRadjBL_2020LU_wTUCP/Run/`. Cross-checked against
COEQWAL V3 `DataExtraction.py` and old `coeqwal` repo notebooks.

### A. Master Variable Table - WRESL Declarations

| Variable | Type | Kind | Native Unit | DSS Unit | ETL Module(s) | Notes |
|----------|------|------|-------------|----------|---------------|-------|
| `AW_{DU}` | std | APPLIED-WATER | CFS | CFS | AG, Refuge | AW = AWR + AWO (+AWW) |
| `AWR_{DU}` | timeseries | APPLIED-WATER | TAF → CFS | CFS | (not used directly) | Rice applied water. Auto-converted by CalSim |
| `AWO_{DU}` | timeseries | APPLIED-WATER | TAF → CFS | CFS | (not used directly) | Other-crop applied water |
| `AWW_{DU}` | timeseries | APPLIED-WATER | TAF → CFS | CFS | (not used directly) | Wetlands. Only some DUs |
| `DN_{DU}` | std | SW-DELIVERY-NET / SW_DELIVERY-NET | CFS | CFS | AG, Refuge, DU Urban | Sac uses hyphen; SJR uses underscore in kind |
| `DG_{DU}` | std | SW-DELIVERY-GROSS | CFS | CFS | (not used) | DN = DG − DL |
| `DL_{DU}` | std | DELIVERY-LOSS | CFS | CFS | (not used) | Conveyance loss |
| `GP_{DU}` | std | GW-PUMPING | CFS | CFS | AG | Groundwater pumping |
| `RU_{DU}` | std | REUSE | CFS | CFS | (not used directly) | Reuse (part of balance) |
| `RP_{DU}` | std | RIPARIAN-MISC-ET | CFS | CFS | (not used directly) | RP = AW × RPF |
| `SHRTG_{DU}` | std | SHORTAGE | CFS | CFS | AG, Refuge | Sacramento region only |
| `GW_SHORT_{DU}` | std (bounded 0-99999) | GW-RESTRICT-SHORT | CFS | CFS | AG, Refuge | SJR/Tulare only; @COEQWAL tag |
| `UD_{DU}` | timeseries | URBAN-DEMAND | TAF → CFS | CFS | DU Urban | Auto-converted by CalSim |
| `S_{code}` | std | STORAGE | **TAF** | **TAF** | Reservoir | Only native-TAF variable in the solver |
| `S_{code}level{N}` | value/timeseries | STORAGE-LEVEL | TAF | TAF | Reservoir | Flood control / dead pool levels |
| `C_{reach}` | std | CHANNEL | CFS | CFS | Env Flows | Channel flow / reservoir release |
| `C_{code}_Flood` | std | SPILL | CFS | CFS | Reservoir | Flood spill = C − C_NCF |
| `C_{code}_NCF` | std (bounded) | CHANNEL | CFS | CFS | (not used) | Normal channel flow ≤ release capacity |
| `C_{reach}_MIF` | std | FLOW-MIN-INSTREAM | CFS | CFS | Env Flows | Minimum instream flow requirement |
| `I_{code}` | timeseries | INFLOW | TAF → CFS | CFS | (not used) | Reservoir inflow |
| `E_{code}` | std | EVAPORATION | CFS | CFS | (not used) | Reservoir evaporation |
| `D_{node}_{contractor}_PMI` | std | FLOW-DELIVERY | CFS | CFS | MI | M&I delivery arc |
| `SHORT_D_{node}_{contractor}_PMI` | alias | delivery-shortage | CFS | CFS | MI | MI shortage (post-solve) |
| `DEL_SWP_MWD` | alias | delivery-swp | CFS | CFS | MI | MWD total delivery (5 arcs) |
| `DEL_SWP_PMI` / `_N` / `_S` | alias | delivery-swp | CFS | CFS | MI, CWS | SWP M&I aggregate |
| `DEL_CVP_PMI_N` / `_S` | alias | delivery-cvp | CFS | CFS | MI, CWS | CVP M&I aggregate |
| `SHORT_SWP_PMI` / `_N` / `_S` | alias | delivery-shortage-swp | CFS | CFS | MI, CWS | SWP M&I aggregate shortage |
| `SHORT_CVP_PMI_N` / `_S` | alias | delivery-shortage-cvp | CFS | CFS | MI, CWS | CVP M&I aggregate shortage |
| `DEL_CVP_PAG_N` / `_S` | alias | delivery-cvp | CFS | CFS | AG | CVP Project AG delivery |
| `DEL_SWP_PAG_N` / `_S` | alias | delivery-swp | CFS | CFS | AG | SWP Project AG delivery |
| `DEL_CVP_PSC_N` | alias | delivery-cvp | CFS | CFS | AG | CVP Settlement Contractors N |
| `DEL_CVP_PEX_S` | alias | delivery-cvp | CFS | CFS | AG | CVP Exchange Contractors S |
| `SHORT_CVP_PAG_N` / `_S` | alias | delivery-shortage-cvp | CFS | CFS | AG | CVP AG shortage |
| `SHORT_SWP_PAG_N` / `_S` | alias | delivery-shortage-swp | CFS | CFS | AG | SWP AG shortage |
| `SHORT_CVP_PSC_N` | alias | delivery-shortage-cvp | CFS | CFS | AG | Settlement shortage |
| `SHORT_CVP_PEX_S` | alias | delivery-shortage-cvp | CFS | CFS | AG | Exchange shortage |
| `DEL_CVP_PRF_N` / `_S` | alias | delivery-cvp | CFS | CFS | (not used) | CVP Refuge delivery aggregate |
| `PERDV_SWP_{1-39}` | alias | swp-output | **PERCENT** | **NONE** | MI | Fraction 0-1. Despite `units 'percent'` tag |
| `NDO` | std | FLOW-NDO | CFS | CFS | Delta | Net Delta Outflow |
| `X2_PRV_KM` | std | X2-POSITION-PREV | **KM** | **KM** | Delta | X2 salinity intrusion position |
| `EM_EC_MONTH` | alias | SALINITY | **UMHOS/CM** | **UMHOS/CM** | Delta | Emmaton electrical conductivity |
| `JP_EC_MONTH` | alias | SALINITY | **UMHOS/CM** | **UMHOS/CM** | Delta | Jersey Point EC |
| `RS_EC_MONTH` | alias | SALINITY | **UMHOS/CM** | **UMHOS/CM** | Delta | Rock Slough EC |
| `UNIMP_{watershed}` | timeseries | FLOW-UNIMPAIRED | **TAF** | TAF or CFS | Env Flows | SV input. Names use abbreviations (SHAS, OROV) |
| `EFLOWS_{reach}` | timeseries | FLOW-MIN-EFLOW | TAF → CFS | CFS | Env Flows | SV input. Functional flow target |
| `taf_cfs` / `cfs_taf` | **built-in** | - | - | - | - | WRESL system functions. Not user-defined |

### B. Water Balance Equations (WRESL-verified)

| DU Type | WRESL Constraint | Equation | Shortage Variable |
|---------|-----------------|----------|-------------------|
| AG (with surface water) | `goal meetAW_{DU}` | `AW + RP = DN + GP + RU + SHORTAGE` | `SHRTG_` (Sac) / `GW_SHORT_` (SJR) |
| AG (GW-only, 18 DUs) | `goal meetAW_{DU}` | `AW + RP = GP + RU + SHORTAGE` (no DN) | Same |
| AG (GW-only, SJR simplified) | `goal meetAW_{DU}` | `AW = GP + SHORTAGE` (no RP, no RU) | `GW_SHORT_` |
| Refuge (_PR) | `goal meetAW_{DU}` | `AW + RP = DN + GP + RU + SHORTAGE` | Same as AG by region |
| Urban (_PU/_SU) | `goal setUD_{DU}` | `UD = DN + GP + SHORTAGE` (no RP, no RU) | Same as AG by region |
| Urban (GW-only, _NU) | `goal setUD_{DU}` | `UD = GP + SHORTAGE` (no DN) | Same as AG by region |
| MI (SWP contractors) | (implicit) | `demand × taf_cfs × perdel = delivery + shortage` | `SHORT_D_*_PMI` (alias) |

### C. CFS → TAF Conversion Factors

| Source | Factor | Code | Difference from exact |
|--------|--------|------|----------------------|
| **Exact** | `86400 / 43560 / 1000 = 0.001983471074...` | - | - |
| **ETL (`units.py`)** | `86400 / 43560000 = 0.00198347107438` | `CFS_TO_TAF_PER_DAY` | **Exact** (integer division) |
| **V3 `cqwlutils.py`** | `0.0019834714` | hardcoded literal | 0.000003% (negligible) |
| **V3 `metrics.py`** | `0.001984` | hardcoded literal | 0.027% (negligible) |
| **Old repo `AuxFunctions.py`** | `(86400/43560) * day / 1000` | computed per-row | **Exact** |

All implementations produce equivalent results for practical purposes.

### D. ETL Module Unit Handling Matrix

| Module | Uses `parse_dss_csv_header`? | Builds `units_map`? | Checks CSV header units? | CFS→TAF conversion? | `check_post_conversion_magnitude`? | Double-conversion risk? |
|--------|-------|-------|-------|-------|-------|-------|
| **AG** | ✅ | ✅ | ✅ | ✅ CFS only | ✅ | None |
| **Refuge** | ✅ | ✅ | ✅ | ✅ CFS only | ✅ | None |
| **MI** | ✅ | ✅ | ✅ | ✅ CFS only (PERDV skipped) | ✅ | None |
| **DU Urban** | ✅ | ✅ | ✅ | ✅ CFS only | ✅ | None (fixed March 2026) |
| **Reservoir** | ❌ (own parser) | ❌ | ✅ (own check) | N/A (storage is TAF) | N/A | None |
| **Env Flows** | ❌ (own parser) | ❌ | ✅ (SV only) | ✅ (volume output only) | ❌ | None |
| **Delta** | ❌ (own parser) | ❌ (own dedup) | ❌ | ✅ (NDO only) | ❌ | None |
| **CWS Aggregate** | ✅ | ✅ | ✅ | ✅ (unit-aware) | ✅ | None (fixed March 2026) |

### E. GW-Only Demand Units (no DN in WRESL, confirmed 18 total)

| Region | DU IDs | WRESL Balance Form |
|--------|--------|-------------------|
| Sacramento (9) | `06_NA`, `07N_NA`, `07S_NA`, `15N_NA1`, `15S_NA1`, `16_NA1`, `17N_NA`, `20_NA2`, `26N_NA` | `AW + RP = GP + RU + SHRTG` |
| SJR East (6) | `60S_NA1`, `60S_NA2`, `61_NA1`, `62_NA1`, `63_NA1`, `64_NA1` | `AW = GP + GW_SHORT` (no RP, no RU) |
| SJR West (3) | `72_NA2`, `73_NA` | `AW = GP + GW_SHORT` |
| Note | `26S_NA` is commented out in WRESL (moved to Lower Mokelumne) | - |

V3 `DataExtraction.py` lists 11 of these (06_NA through 60S_NA2) and computes `DN = GP + RU`.

### F. MI Contractor PERDV Mapping (WRESL-verified)

| PERDV Variable | WRESL units tag | Actual value range | Contractor(s) |
|----------------|----------------|--------------------|---------------|
| `PERDV_SWP_1` | `percent` | 0-1 (fraction) | ACFC (SBA009) |
| `PERDV_SWP_2` | `percent` | 0-1 | ACFC (SBA020) |
| `PERDV_SWP_3` | `percent` | 0-1 | ACWD |
| `PERDV_SWP_4` | `percent` | 0-1 | AVEK |
| `PERDV_SWP_11` | `percent` | 0-1 | CSTLN (Castaic/SVRWD) |
| `PERDV_SWP_15` | `percent` | 0-1 | KERN (Kern County) |
| `PERDV_SWP_29` | `percent` | 0-1 | PLMDL (Palmdale) |
| `PERDV_SWP_30` | `percent` | 0-1 | BRDNO (San Bernardino) |
| `PERDV_SWP_31` | `percent` | 0-1 | GABRL (San Gabriel) |
| `PERDV_SWP_32` | `percent` | 0-1 | GRGNO (San Gorgonio) |
| `PERDV_SWP_34` | `percent` | 0-1 | BRBRA (Santa Barbara) |
| `PERDV_SWP_35` | `percent` | 0-1 | OBISPO + SCVWD (shared) |
| `PERDV_SWP_38` | `percent` | 0-1 | VNTRA (Ventura, PYRMD arc) |
| `PERDV_SWP_39` | `percent` | 0-1 | VNTRA (Ventura, CSTIC arc) |

MI demand formula: `demand_TAF_per_month = Σ (D_i + SHORT_i) / PERDV_i` (per arc).
MWD demand: hardcoded `1911.5 TAF/yr` (Table A contract).

### G. Reservoir Capacity Constants (WRESL-verified)

| Reservoir | WRESL `level6` / gross | V3 Hardcoded | ETL `CAPACITY_OVERRIDES` | Entity CSV |
|-----------|----------------------|-------------|-------------------------|------------|
| **SHSTA** | 4552 TAF | - (from DSS) | - (from entity CSV) | 4552 |
| **TRNTY** | 2447.65 TAF | - | - | 2448 |
| **OROVL** | 3424.8 TAF | **3424.8** | **3424.8** ✅ | 3537 |
| **FOLSM** | 967 TAF | **967** | **967** ✅ | 975 |
| **MLRTN** | - (≈524) | **524** | **524** ✅ | 520 |
| **MELON** | - (≈2420) | **2420** | **2420** ✅ | 2400 |

### H. Computed Aggregates (ETL vs V3)

| Aggregate | ETL Formula | V3 Formula | Match? |
|-----------|-------------|------------|--------|
| `nod_ag` | `DEL_CVP_PAG_N + DEL_SWP_PAG_N + DEL_CVP_PSC_N` | `DEL_CVP_PAG_N + DEL_SWP_PAG_N + DEL_CVP_PSC_N` | ✅ |
| `sod_ag` | `DEL_CVP_PAG_S + DEL_SWP_PAG_S + DEL_CVP_PEX_S` | `DEL_CVP_PAG_S + DEL_SWP_PAG_S + DEL_CVP_PEX_S` | ✅ |
| `NOD_STORAGE` | (not computed) | `S_TRNTY + S_SHSTA + S_OROVL + S_FOLSM + S_NBLDB` | - |
| `SOD_STORAGE` | (not computed) | `S_SLUIS_CVP + S_SLUIS_SWP + S_MELON + S_NHGAN + S_MLRTN + S_PEDRO + S_MCLRE + S_HNSLY` | - |

---

## Appendix: Known Errors in COEQWAL Repositories

Flagged during WRESL verification (March 2026). These are in the *notebook* codebases,
not in the ETL. Documented here for reference.

### COEQWAL_V3 / `DataExtraction.py`

| # | Severity | Issue | Location |
|---|----------|-------|----------|
| 1 | **CRITICAL** | `UD_ANTOC` double conversion: value is pre-converted to TAF (`25 × 0.001984 × days`) but labeled `CFS`, so `convert_all_cfs_to_taf` converts it again | Line 926 |
| 2 | **CRITICAL** | `DEL_CVPSWP_TOTAL` double-counts SOD CVP deliveries: adds `DEL_CVP_TOTAL` (which includes PAG_S + PEX_S) then separately adds `DEL_CVP_PAG_S + DEL_CVP_PEX_S` | Lines 389-399 |
| 3 | **HIGH** | `SBA036_SCVWD` shortage variable has wrong case (`short_D_` lowercase) and wrong C-part (`FLOW-DELIVERY` instead of `DELIVERY-SHORTAGE`). Commented-out code (lines 1322-1324) has the correct version | Lines 1342-1345 |
| 4 | **MEDIUM** | Trailing space in `S_OROVLLEVEL6DV ` and `S_MELONLEVEL5DV ` column names in `preprocess_compound_data_dss` (not in `preprocess_study_dss`) - causes column name mismatch in multi-study mode | Lines 494, 496 |
| 5 | **LOW** | `D_AMADR_NU`, `D_AMCYN`, `D_ACFC_PMI` defined 2-3 times each (later overwrites earlier. No functional harm) | Various |
| 6 | **LOW** | Hardcoded study name `L2020A` in all manually constructed column tuples - cannot handle other studies | Throughout |

### COEQWAL (old repo) / `metrics.py`

| # | Severity | Issue | Location |
|---|----------|-------|----------|
| 1 | **HIGH** | `var_filter` undefined in `create_subset_var` - runtime crash when WYT filter is provided | Line 195 |
| 2 | **HIGH** | `df_copy` undefined in `create_subset_var` - runtime crash | Line 199 |
| 3 | **LOW** | Deprecated `applymap()` call (should be `map()` in pandas ≥ 2.0) | Line 300 |
| 4 | **LOW** | Variable named `prob_less` actually represents P(≥) | Line 884 |

### COEQWAL (old repo) / `csPlots.py`

| # | Severity | Issue | Location |
|---|----------|-------|----------|
| 1 | **MEDIUM** | `cfs_to_taf()` uses `.index.day` (day-of-month) instead of `.dt.days_in_month` - works correctly ONLY if timestamps are end-of-month | Line 1334 |

### ETL Issues Found and Fixed During This Audit

| # | Severity | Module | Issue | Status |
|---|----------|--------|-------|--------|
| 1 | **CRITICAL** | `du_urban/calculate_du_statistics.py` (now deleted, see `calculate_du_statistics_v2.py`) | No CFS->TAF conversion at all. All `*_taf` database columns contained CFS values. Did not import `units.py`, did not compute `DaysInMonth`, did not check CSV header units. | **FIXED** in `calculate_du_statistics_v2.py`, which uses `parse_dss_csv_header`, unit-aware CFS->TAF conversion, and `check_post_conversion_magnitude`. Original file removed in the dead-code audit. |
| 2 | **HIGH** | `cws_aggregate/calculate_cws_aggregate_statistics.py` | Unconditionally applied CFS→TAF conversion without checking declared units. No `check_post_conversion_magnitude` safeguard. | **FIXED** - now uses `parse_dss_csv_header`, unit-aware `_to_taf()` helper, and `check_post_conversion_magnitude` |
