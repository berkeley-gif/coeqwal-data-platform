# ETL Statistics Pipeline

Calculate and load reservoir statistics from CalSim model outputs to the database.

## Overview

This pipeline processes CalSim model output CSVs stored in S3 to calculate reservoir statistics. The calculated metrics are loaded directly into PostgreSQL for the COEQWAL website API.

**Automated pipeline flow:**
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  S3 bucket      │───▶│  AWS Lambda      │───▶│  PostgreSQL     │───▶│  API endpoints  │
│  CSV upload     │    │  (main.py)       │    │  database       │    │  /api/statistics│
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
        │                      │
        │  S3 Event Trigger    │  Direct DB writes
        └──────────────────────┘  via psycopg2
```

## Directory Structure

```
etl/statistics/
├── main.py                              # Entry point - Lambda handler + CLI
├── README.md                            # This file
├── dev_run.sh                           # Local development script
├── test_local.py                        # Local test runner
├── verify_metrics.py                    # Verification against notebook output
└── reservoirs/                          # Reservoir statistics
    ├── calculate_reservoir_statistics.py  # Full statistics for all 92 reservoirs
    ├── calculate_reservoir_percentiles.py # Percentile bands for website charts
    └── reservoir_metrics.py               # Core calculation functions
```

## Scripts

| Script | Purpose |
|--------|---------|
| `main.py` | **Entry point** - Lambda handler + CLI for automated ETL |
| `dev_run.sh` | Local development runner for testing with CSV files |
| `test_local.py` | Quick sanity check for individual reservoir calculations |
| `verify_metrics.py` | **Verification** - Compare ETL output against metrics in all_metrics_output.csv |
| `reservoirs/calculate_reservoir_statistics.py` | Calculates all reservoir statistics including probability metrics |
| `reservoirs/calculate_reservoir_percentiles.py` | Percentile band calculation for website charts |
| `reservoirs/reservoir_metrics.py` | Core calculation functions aligned with COEQWAL modeler Jupyter notebooks |

## Automated pipeline setup

### 1. AWS Lambda configuration

Lambda function with:
- **Runtime**: Python 3.9+
- **Handler**: `main.lambda_handler`
- **Memory**: 512 MB (for large CSV processing)
- **Timeout**: 5 minutes
- **Environment variables**:
  - `DATABASE_URL`: PostgreSQL connection string
  - `S3_BUCKET`: `coeqwal-model-run`

### 2. S3 event trigger

Trigger Lambda on new DSS uploads:

```json
{
  "LambdaFunctionConfigurations": [{
    "LambdaFunctionArn": "arn:aws:lambda:...:function:coeqwal-statistics-etl",
    "Events": ["s3:ObjectCreated:*"],
    "Filter": {
      "Key": {
        "FilterRules": [
          {"Name": "prefix", "Value": "scenario/"},
          {"Name": "suffix", "Value": "_coeqwal_calsim_output.csv"}
        ]
      }
    }
  }]
}
```

### 3. Dependencies (Lambda layer)

```
pandas
numpy
psycopg2-binary
boto3
```

### 4. New scenario pipeline

When a new scenario DSS is uploaded to S3:

1. **Extraction** → pydsstools implementation creates output csv
2. **S3 event** → Triggers lambda
3. **Lambda** → Extracts scenario ID from path (`s0030`)
4. **ETL** → Calculates percentiles, statistics, probabilities
5. **Database** → Writes directly via psycopg2
6. **API** → Data available at `/api/statistics/`

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

Use `main.py` for direct database writes:

```bash
# Process single scenario and write to database
DATABASE_URL=postgres://... python main.py --scenario s0020

# Process from S3 key (extracts scenario ID automatically)
DATABASE_URL=postgres://... python main.py --s3-key scenario/s0020/csv/s0020_coeqwal_calsim_output.csv

# Process all scenarios
DATABASE_URL=postgres://... python main.py --all-scenarios

# Dry run (calculate without writing)
python main.py --scenario s0020 --dry-run

# Output as JSON (for debugging)
python main.py --scenario s0020 --output-json
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

### Verification Script

Use `verify_metrics.py` to validate ETL calculations against the COEQWAL research notebook output:

```bash
# Calculate metrics and display
python verify_metrics.py

# Compare against notebook output (stored in S3/audits)
python verify_metrics.py --compare /path/to/all_metrics_output.csv

# Save calculated metrics to CSV
python verify_metrics.py --output my_metrics.csv

# Test specific reservoirs
python verify_metrics.py --reservoirs SHSTA OROVL FOLSM
```

### Verification Results (s0020 Baseline)

**Tolerance: 0.01% (0.0001)**

| Category | Passed | Failed | Notes |
|----------|--------|--------|-------|
| Flood Pool Probabilities | 14/14 | 0 | All match within tolerance |
| Dead Pool Probabilities | 14/14 | 0 | All match within tolerance |
| Monthly Averages (TAF) | 12/16 | 4 | See SLUIS note below |
| Coefficient of Variation | 16/16 | 0 | All match exactly |
| **TOTAL** | **56/60** | **4** | 93% exact match |

### Naming Convention

Column names match the notebook's `all_metrics_output.csv` exactly:

| Metric Type | Column Pattern | Example |
|-------------|----------------|---------|
| Flood Probability (all) | `All_Prob_S_{RES}_flood` | `All_Prob_S_SHSTA_flood` |
| Flood Probability (Sep) | `Sep_Prob_S_{RES}_flood` | `Sep_Prob_S_SHSTA_flood` |
| Dead Pool Probability | `All_Prob_S_{RES}_dead` | `All_Prob_S_OROVL_dead` |
| April Average | `Apr_Avg_S_{RES}_TAF` | `Apr_Avg_S_SHSTA_TAF` |
| September Average | `Sep_Avg_S_{RES}_TAF` | `Sep_Avg_S_TRNTY_TAF` |
| April CV | `Apr_S_{RES}_CV` | `Apr_S_FOLSM_CV` |
| September CV | `Sep_S_{RES}_CV` | `Sep_S_MELON_CV` |

**Note**: SLUIS reservoirs use condensed naming (no underscore before TAF/CV):
- `Apr_Avg_S_SLUIS_SWPTAF` (not `Apr_Avg_S_SLUIS_SWP_TAF`)
- `Apr_S_SLUIS_CVPCV` (not `Apr_S_SLUIS_CVP_CV`)

### Sample Verified Values (s0020)

| Metric | ETL Value | Notebook Value | Status |
|--------|-----------|----------------|--------|
| All_Prob_S_SHSTA_flood | 0.3117 | 0.3117 | ✅ |
| All_Prob_S_SHSTA_dead | 0.0150 | 0.0150 | ✅ |
| Sep_Prob_S_OROVL_flood | 0.0800 | 0.0800 | ✅ |
| Apr_Avg_S_SHSTA_TAF | 3906.98 | 3906.98 | ✅ |
| Sep_S_SHSTA_CV | 0.3045 | 0.3045 | ✅ |
| Apr_S_SLUIS_SWPCV | 0.3692 | 0.3692 | ✅ |
| Sep_S_SLUIS_CVPCV | 0.9886 | 0.9886 | ✅ |

### ⚠️ SLUIS Monthly Averages - Known Discrepancy

**Status**: Under investigation with modeling team

The notebook outputs constant values for SLUIS monthly averages that don't match the ETL calculations:

| Metric | ETL Value | Notebook Value | Analysis |
|--------|-----------|----------------|----------|
| Apr_Avg_S_SLUIS_SWPTAF | 710.54 | 1067.0 | ⚠️ |
| Sep_Avg_S_SLUIS_SWPTAF | 442.29 | 1067.0 | ⚠️ |
| Apr_Avg_S_SLUIS_CVPTAF | 746.59 | 972.0 | ⚠️ |
| Sep_Avg_S_SLUIS_CVPTAF | 262.42 | 972.0 | ⚠️ |

**Evidence that ETL values are correct**:

1. **CV values match exactly** - The same storage data produces matching CV calculations:
   - ETL `Apr_S_SLUIS_SWPCV`: 0.3692 = Notebook: 0.3692 ✅
   - ETL `Sep_S_SLUIS_CVPCV`: 0.9886 = Notebook: 0.9886 ✅

2. **Hydrological validity** - ETL values show expected seasonal pattern:
   - September storage < April storage (reservoirs draw down in summer)
   - SLUIS_CVP September (262 TAF) much lower than April (747 TAF) - reflects CVP pumping patterns

3. **Notebook values are constant** - Same value for April and September (1067, 972) is hydrologically impossible for storage reservoirs

4. **Probability metrics match** - Same data source produces matching probabilities:
   - ETL `All_Prob_S_SLUIS_CVP_flood`: 0.125 = Notebook: 0.125 ✅

**Hypothesis**: The notebook may be outputting threshold constants instead of calculated averages for these variables.

### Metrics Not in Notebook

The following metrics are calculated by the ETL but not included in the notebook output:

| Metric | ETL Value | Notes |
|--------|-----------|-------|
| All_Prob_S_FOLSM_flood | 0.4267 | FOLSM excluded from notebook probability calculations |
| Sep_Prob_S_FOLSM_flood | 0.2900 | FOLSM excluded from notebook probability calculations |

### Verification Workflow

1. **Notebook generates reference data**:
   - Run `coeqwal/notebooks/Metrics.ipynb`
   - Outputs `all_metrics_output.csv` to `{GroupDataDirPath}/metrics_output/`

2. **Reference data stored in two locations**:
   - **Local**: `coeqwal-backend/audits/all_metrics_output.csv`
   - **S3**: `s3://coeqwal-model-run/reference/all_metrics_output.csv`

   Upload command:
   ```bash
   aws s3 cp audits/all_metrics_output.csv s3://coeqwal-model-run/reference/all_metrics_output.csv
   ```

3. **ETL verification** (from `etl/statistics/` directory):
   ```bash
   # Using local file
   python verify_metrics.py --compare ../../audits/all_metrics_output.csv

   # Or download from S3 first
   aws s3 cp s3://coeqwal-model-run/reference/all_metrics_output.csv /tmp/
   python verify_metrics.py --compare /tmp/all_metrics_output.csv
   ```

4. **Expected results**:
   - 56/60 metrics pass at 0.01% tolerance
   - 4 SLUIS monthly averages flagged (known discrepancy)
   - 2 FOLSM flood probabilities not in notebook (ETL-only)

---

## Verification Summary

### ✅ Verified Against Notebook (56 metrics)

| Statistic | Reservoirs | Verification Status |
|-----------|------------|---------------------|
| **Flood Pool Probability (All)** | SHSTA, OROVL, TRNTY, SLUIS_CVP, SLUIS_SWP, MLRTN, MELON | ✅ 0.01% tolerance |
| **Flood Pool Probability (Sep)** | SHSTA, OROVL, TRNTY, SLUIS_CVP, SLUIS_SWP, MLRTN, MELON | ✅ 0.01% tolerance |
| **Dead Pool Probability (All)** | SHSTA, OROVL, TRNTY, SLUIS_CVP, SLUIS_SWP, MLRTN, MELON | ✅ 0.01% tolerance |
| **Dead Pool Probability (Sep)** | SHSTA, OROVL, TRNTY, SLUIS_CVP, SLUIS_SWP, MLRTN, MELON | ✅ 0.01% tolerance |
| **April Average (TAF)** | SHSTA, OROVL, TRNTY, FOLSM, MELON, MLRTN | ✅ 0.01% tolerance |
| **September Average (TAF)** | SHSTA, OROVL, TRNTY, FOLSM, MELON, MLRTN | ✅ 0.01% tolerance |
| **April CV** | SHSTA, OROVL, TRNTY, FOLSM, MELON, MLRTN, SLUIS_CVP, SLUIS_SWP | ✅ 0.01% tolerance |
| **September CV** | SHSTA, OROVL, TRNTY, FOLSM, MELON, MLRTN, SLUIS_CVP, SLUIS_SWP | ✅ 0.01% tolerance |

### ⚠️ Known Discrepancy (4 metrics)

| Statistic | Reservoirs | Issue |
|-----------|------------|-------|
| **April Average (TAF)** | SLUIS_CVP, SLUIS_SWP | Notebook outputs constant values |
| **September Average (TAF)** | SLUIS_CVP, SLUIS_SWP | Notebook outputs constant values |

### 📊 ETL-Only Metrics (not in notebook)

| Statistic | Reservoirs | Notes |
|-----------|------------|-------|
| **Flood Pool Probability** | FOLSM | FOLSM not in notebook threshold calculations |
| **All 92 reservoirs** | Full entity list | Notebook only calculates 8 major reservoirs |
| **Monthly percentile bands** | All | Website-specific visualization data |
| **Spill statistics** | All | Not calculated in Metrics.ipynb |
| **Storage exceedance percentiles** | All | Period summary metrics |

### 🔍 Cannot Verify (no notebook equivalent)

| Statistic | Reason |
|-----------|--------|
| Monthly percentile bands by water month | Website-specific grouping approach |
| Spill frequency and volume metrics | Not in Metrics.ipynb scope |
| Storage exceedance curves | Different calculation in notebook |
| April flood pool probability | Notebook only calculates All and September; `verify_metrics.py` does not calculate April probabilities (including FOLSM) |
| 84 additional reservoirs | Notebook limited to 8 major reservoirs |

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

Place a CalSim output CSV in the `pipelines/` directory:
```bash
# If you have AWS access:
aws s3 cp s3://coeqwal-model-run/scenario/s0020/csv/s0020_coeqwal_calsim_output.csv ../pipelines/

# Or use any existing CSV with the 7-header format
ls ../pipelines/*.csv
```

**2. Run with dry-run (no database required)**

```bash
cd /path/to/coeqwal-backend/etl/statistics

# Dry run - calculates metrics, prints summary, no database writes
python main.py --scenario s0020 --csv-path ../pipelines/s0020_coeqwal_calsim_output.csv --dry-run

# With JSON output for debugging
python main.py --scenario s0020 --csv-path ../pipelines/s0020_coeqwal_calsim_output.csv --dry-run --output-json
```

**3. Run quick tests**

```bash
# Test imports and basic calculations
python test_local.py

# Verify metrics against notebook output
python verify_metrics.py --reservoirs SHSTA OROVL
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
- Set bucket: `export S3_BUCKET=coeqwal-model-run`

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
| `du_urban/main.py` | CLI entry point |
| `du_urban/calculate_du_statistics.py` | Main calculation module for tier matrix DUs |
| `du_urban/calculate_du_statistics_v2.py` | Version 2 with database variable mappings |

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

### Canonical Sources

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
