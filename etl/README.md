# ETL (Extract, Transform, Load) Framework

Automated DSS file processing pipeline that extracts CSV data from CalSim model runs and validates against reference data.

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
- **Monitoring**: Updates DynamoDB with job status

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

## Statistics ETL

The statistics ETL calculates derived metrics from CalSim scenario output CSVs and loads them into the database for API consumption.

### Consolidated runner

Use `run_all.py` to run all statistics modules for a scenario:

```bash
cd etl/statistics

# Run all statistics for a scenario
python run_all.py --scenario s0029

# Dry run (calculate but don't write to DB)
python run_all.py --scenario s0029 --dry-run

# Run only specific modules
python run_all.py --scenario s0029 --only reservoirs,du_urban

# Run all scenarios
python run_all.py --all-scenarios

# List available modules
python run_all.py --list-modules
```

### Modules (run in order)

| Order | Module | Script | Database Tables |
|-------|--------|--------|-----------------|
| 1 | **reservoirs** | `main.py` | `reservoir_monthly_percentile`, `reservoir_storage_monthly`, `reservoir_spill_monthly`, `reservoir_period_summary` |
| 2 | **du_urban** | `du_urban/main.py` | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |
| 3 | **mi** | `mi/main.py` | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |
| 4 | **cws_aggregate** | `cws_aggregate/main.py` | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |

### Individual module usage

Each module can also be run standalone:

```bash
# Reservoir statistics
python main.py --scenario s0029

# Urban demand unit statistics
cd du_urban && python main.py --scenario s0029

# M&I contractor statistics  
cd mi && python main.py --scenario s0029

# CWS aggregate statistics
cd cws_aggregate && python main.py --scenario s0029
```

### Available scenarios

```
s0011, s0020, s0021, s0023, s0024, s0025, s0027, s0029
```

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

**Note:** This differs from the `Percent_of_Demand` calculation used for individual CWS contractors (documented below), which uses PERDV allocation variables to back-calculate demand. The aggregate reliability formula directly compares shortage to delivery without the PERDV adjustment.

### Prerequisites

- `DATABASE_URL` environment variable set
- CalSim output CSV available in S3: `s3://coeqwal-model-run/scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv`
- Python packages: `pandas`, `numpy`, `psycopg2`, `boto3`