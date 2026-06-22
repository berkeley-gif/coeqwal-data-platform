# ETL statistics.Wildlife refuge delivery

Calculate delivery, shortage, and reliability statistics for the 18 wildlife refuge demand units
defined in CalSim 3.

## Overview

This module processes per-scenario CalSim output to produce monthly and period-of-record
statistics for environmental water deliveries to wildlife refuges and wetland areas in the
Sacramento and San Joaquin hydrologic regions.

**Four metrics are computed for each of the 18 refuge demand units:**


| Metric                 | Unit         | Temporal                    | Statistics                                               |
| ---------------------- | ------------ | --------------------------- | -------------------------------------------------------- |
| Surface water delivery | TAF          | Monthly (water months 1-12) | Monthly percentile bands, monthly mean/CV. Annual avg/CV |
| Delivery shortage      | TAF          | Monthly, annually           | Monthly percentile bands, annual avg/CV                  |
| Delivery shortage      | % of demand  | Monthly, annually           | Monthly percentile bands, monthly avg/CV, annual avg/CV  |
| Delivery reliability   | % (95th pct) | Period of record            | Single value per DU per scenario                         |


**Reliability definition:** The 95th percentile of annual shortage %. Interpretation: in 95 out of 100
simulated years, the demand unit's shortage is at or below this value. A value of 0% means no shortage in
95% of years. A value of 50% means even in "normal" years the DU is chronically under-supplied.

**No shortage variable exists natively in CalSim for refuge DUs.** Unlike M&I contractors (which have
`SHORT_D_*_PMI` variables), shortage is derived entirely as `demand − delivery`.

---

## Data sources

### 1. SV input CSV.demand (`AWO_{DU_ID}`)


| Attribute         | Value                                                                            |
| ----------------- | -------------------------------------------------------------------------------- |
| S3 path           | `s3://coeqwal-model-run/scenario/{scenario}/csv/{scenario}_coeqwal_sv_input.csv` |
| DSS variable name | `AWO_{DU_ID}` (Applied Water Output).Part C = `APPLIED-WATER`                  |
| Units             | TAF.**no conversion needed**                                                   |
| Staging           | Extracted per scenario alongside the main CalSim output                          |


> **Important:** The raw DSS path uses `AWO_`* naming (e.g., `AWO_09_PR`), but the staged CSV
> column names may appear as `AW_`* depending on the extraction tooling. Verify column names
> in the staged SV CSV before running the ETL. See open questions below.

The SV input is the **primary demand source** because it represents the original applied water
requirement fed into the model.prior to any optimization or delivery logic.

### 2. DV output.delivery (`DN_{DU_ID}`)


| Attribute         | Value                                                                                   |
| ----------------- | --------------------------------------------------------------------------------------- |
| S3 path           | `s3://coeqwal-model-run/scenario/{scenario}/csv/{scenario}_coeqwal_calsim_output.csv`   |
| DSS variable name | `DN_{DU_ID}`.Part C = `SW-DELIVERY-NET` (SAC) or `SW_DELIVERY-NET` (SJR/Tulare)       |
| Units             | **CFS.conversion to TAF required** (see unit conversion section below)                |
| Staging           | Extracted per scenario as part of the standard ETL trigger.no additional steps needed |


#### What is `coeqwal_calsim_output.csv`?

`{scenario}_coeqwal_calsim_output.csv` **is** the DV (Decision Variable) file. The batch ETL
entrypoint (`batch_entrypoint.sh`) calls `classify_dss.py` on the model run ZIP, which identifies
the DV DSS by looking for `_dv` in the filename inside `DSS/output/`:

```
DSS/output/s0020_DCRadjBL_2020LU_wTUCP_DV_v0.1.dss   ← this is the DV file
            └─ classify_dss.py picks this as DV_PATH (Tier 3: "_dv" in name)
            └─ dss_to_csv.py converts it
            └─ output: s0020_coeqwal_calsim_output.csv (CSV basename unchanged for back-compat)
```

The name `coeqwal_calsim_output` is a generic convention applied by the ETL script, not a
description of the contents. The source data is the CalSim 3 DV output.

All 18 refuge `DN_{DU_ID}` delivery variables are **confirmed present** in the DV DSS
(`s0020_DCRadjBL_2020LU_wTUCP_DV_v0.1.dss`) and will therefore be in the extracted CSV:


| Region     | Variable names confirmed in DV DSS                                                                                              |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------- |
| SAC        | `DN_08N_PR1`, `DN_08N_PR2`, `DN_08S_PR`, `DN_09_PR`, `DN_11_PR`, `DN_17N_NR`, `DN_17N_PR`, `DN_17S_PR`                          |
| SJR/Tulare | `DN_63_PR1`, `DN_63_PR2`, `DN_63_PR3`, `DN_72_PR1`, `DN_72_PR2`, `DN_72_PR3`, `DN_72_PR4`, `DN_72_PR5`, `DN_72_PR6`, `DN_91_PR` |


#### How the ETL loader reads column names

The CSV produced by `dss_to_csv.py` has **7 header rows** followed by data rows. No pandas
column header is written.instead, DSS path parts are stored as data in the first 7 rows:

```
Row 0  Part A:    CALSIM, CALSIM, ...
Row 1  Part B:    DN_08N_PR1, DN_08N_PR2, ...    ← variable names (used as column identifiers)
Row 2  Part C:    SW-DELIVERY-NET, SW-DELIVERY-NET, ...
Row 3  Part E:    1MON, ...
Row 4  Part F:    L2020A, ...
Row 5  Type:      PER-AVER, ...
Row 6  Units:     CFS, CFS, ...
Row 7+            data values
```

The loader reads the header separately, assigns Part B (row index 1) as the DataFrame column
names, then reads the data rows:

```python
header_df = pd.read_csv(body, header=None, nrows=8)
col_names = header_df.iloc[1].tolist()           # Part B = variable names e.g. "DN_08N_PR1"

data_df = pd.read_csv(body, header=None, skiprows=7, low_memory=False)
data_df.columns = col_names                       # columns are now just the variable names
```

Column selection therefore uses **Part B only**.`DN_08N_PR1`.not a compound key.

**Note on Part-C naming difference:** The DV DSC shows SAC units use `SW-DELIVERY-NET` (hyphen)
and SJR/Tulare units use `SW_DELIVERY-NET` (underscore). This is a CalSim 3 naming inconsistency
but it does **not** affect column selection since the loader uses Part B names, not Part C.

> **Deliveries CSV note:** An alternative `*_deliveries.csv` file exists and contains
> `DN_{DU_ID}` in a two-block structure (CFS and pre-converted TAF). This file is **not staged
> for all scenarios** and is not used as the primary delivery source. It is useful only as a
> reference for verifying unit conversion accuracy against pre-computed TAF values.

### DSS date convention.period-beginning vs period-ending

CalSim DSS files use two different month-labelling conventions depending on the file type.
Both must map to the same calendar month before any merge or calculation can occur.

| File | Convention | Example date | Actual data period |
| ---- | ---------- | ------------ | ------------------ |
| SV input (`coeqwal_sv_input.csv`) | **Period-beginning** | `1920-11-01` | October 1920 (WM=1) |
| DV output (`coeqwal_calsim_output.csv`) | **Period-ending** | `1921-10-31` | October 1921 (WM=1) |

The SV file stamps each row with the **first day of the following month**: `1920-11-01` is labelled
November 1 but represents water delivered *during* October 1920.
The DV file stamps each row with the **last day of the current month**: `1921-10-31` is the last day
of October and correctly represents October 1921.

**Consequence:** a naive date-string join between SV and DV produces 0 matching rows because no
SV date (`YYYY-MM-01`) ever equals a DV date (`YYYY-MM-{28,29,30,31}`).

**ETL normalisation** (`add_water_year_month`): before deriving `WaterYear`, `WaterMonth`, and
`DaysInMonth`, the function detects period-beginning rows by checking if `day == 1` and shifts
those dates back by one day (`1920-11-01 to 1920-10-31`). End-of-month DV dates are used as-is.
After normalisation, both files yield October to WM=1 for the same model month, and the
`WaterYear + WaterMonth` merge produces the expected row count (one row per month of record).

> The raw date values in the CSV files are left unchanged.this normalisation is applied only
> within the ETL at the point of water-year calendar derivation, never during DSStoCSV extraction.

---

## Unit conversion

Delivery values from the DV output are in **CFS (cubic feet per second)**, a flow rate, not a
volume. To convert to **TAF (thousand acre-feet)**, which is the standard water accounting unit
in California, the formula is:

```
TAF = CFS × 0.001984 × days_in_month
```

Unit conversion is required for three reasons:

1. **Volume vs. rate**: CFS measures instantaneous flow rate. TAF is volume over a period.
  Reporting and comparing deliveries requires volume.
2. **Monthly aggregation**: Months have different lengths (28-31 days), so a given CFS rate
  produces different volumes in different months. Converting normalizes across months.
3. **Comparability with demand**: Demand (`AWO_{DU_ID}` from SV input) is already in TAF.
  Shortage cannot be computed as `demand − delivery` unless both are in the same unit.

The constant `0.001984` = `(1 ft³/s × 1 ac·ft / 43,560 ft³ × 86,400 s/day) / 1,000`.

---

## Calculations

### Water year convention

Water month 1 = October, water month 12 = September. Annual totals span October-September.

### Metric 1.Monthly delivery statistics

For each DU and each water month (1-12), across all simulated water years:

```python
monthly_values = delivery_taf[water_month == m]         # all years for month m
delivery_avg_taf = mean(monthly_values)
delivery_cv      = std(monthly_values) / mean(monthly_values)  # 0 if mean == 0
q0, q10, q30, q50, q70, q90, q100 = percentile(monthly_values, [0,10,30,50,70,90,100])
exc_p5 ... exc_p95                 = percentile(monthly_values, [95,90,75,50,25,10,5])
sample_count                       = len(monthly_values)
```

### Metric 2.Monthly shortage statistics (TAF and %)

Shortage is computed first, then bands are applied:

```python
shortage_taf = max(demand_taf - delivery_taf, 0)         # per timestep, floor at 0
shortage_pct = shortage_taf / demand_taf * 100           # 0 if demand == 0

# Per water month:
shortage_avg_taf       = mean(monthly_shortage_taf)
shortage_cv            = std(monthly_shortage_taf) / mean(monthly_shortage_taf)
shortage_pct_avg       = mean(monthly_shortage_pct)
shortage_pct_cv        = std(monthly_shortage_pct) / mean(monthly_shortage_pct)
shortage_frequency_pct = count(shortage_taf > THRESHOLD) / total_months
q0..q100               = percentile(monthly_shortage_taf, [0,10,30,50,70,90,100])
exc_p5..exc_p95        = percentile(monthly_shortage_taf, [95,90,75,50,25,10,5])
```

`SHORTAGE_THRESHOLD_TAF = 0.1` (100 acre-feet) filters out floating-point precision artifacts.

### Metric 3.Period-of-record summary

```python
# Annual totals
annual_delivery_taf  = sum(monthly_delivery_taf per water year)
annual_shortage_taf  = sum(monthly_shortage_taf per water year)
annual_demand_taf    = sum(monthly_demand_taf per water year)
annual_shortage_pct  = annual_shortage_taf / annual_demand_taf * 100

# Summary statistics across all years
annual_delivery_avg_taf = mean(annual_delivery_taf)
annual_delivery_cv      = std(annual_delivery_taf) / mean(annual_delivery_taf)
annual_shortage_avg_taf = mean(annual_shortage_taf)
annual_shortage_cv      = std(annual_shortage_taf) / mean(annual_shortage_taf)
annual_shortage_pct_avg = mean(annual_shortage_pct)
annual_shortage_pct_cv  = std(annual_shortage_pct) / mean(annual_shortage_pct)

# Reliability.see open question #5 below
reliability_pct_95 = np.percentile(annual_shortage_pct, 95)

# Exceedance percentiles for annual delivery
delivery_exc_p5..delivery_exc_p95 = np.percentile(annual_delivery_taf, [95,90,75,50,25,10,5])
```

---

## Percentile band conventions

Two sets of summary statistics are stored for each distribution:

### Standard percentile bands (q columns)

`q0, q10, q30, q50, q70, q90, q100`.cumulative distribution percentiles, ascending.
`q50` is the median; `q0` is the minimum; `q100` is the maximum.

### Exceedance percentiles (exc_p columns)

`exc_p5, exc_p10, exc_p25, exc_p50, exc_p75, exc_p90, exc_p95`.**exceedance probabilities**.

> **Exceedance convention:** `exc_pX` is the value exceeded X% of the time, which equals the
> `(100 − X)`th cumulative percentile. For example:
>
> - `exc_p5` = value exceeded 5% of time = 95th percentile (a high/wet-year value)
> - `exc_p95` = value exceeded 95% of time = 5th percentile (a low/dry-year value)
>
> This is standard hydrological convention: a Q5 flow is a high flow exceeded only 5% of the
> time. A Q95 flow is a low flow exceeded 95% of the time (nearly always met). Both band sets
> are provided because they serve different visualization purposes (percentile bands for ribbon
> charts. Exceedance percentiles for frequency curves). These match the conventions used in all
> other COEQWAL ETL modules (`ag`, `mi`, `du_urban`, `reservoirs`).

---

## Database tables

### `refuge_du_delivery_monthly`

Monthly percentile bands for delivery. One row per `(scenario, du_id, water_month)`.


| Column                | Type          | Description                                           |
| --------------------- | ------------- | ----------------------------------------------------- |
| `scenario_short_code` | VARCHAR(20)   | e.g., `s0020`                                         |
| `du_id`               | VARCHAR(20)   | e.g., `08N_PR1`.references `du_refuge_entity.du_id` |
| `water_month`         | INTEGER       | 1-12 (Oct=1, Sep=12)                                  |
| `delivery_avg_taf`    | NUMERIC(10,2) | Mean delivery for this month across all years         |
| `delivery_cv`         | NUMERIC(10,4) | CV of monthly delivery                                |
| `q0..q100`            | NUMERIC(10,2) | Percentile bands (0,10,30,50,70,90,100th)             |
| `exc_p5..exc_p95`     | NUMERIC(10,2) | Exceedance percentiles (5,10,25,50,75,90,95%)         |
| `sample_count`        | INTEGER       | Number of years included                              |


### `refuge_du_shortage_monthly`

Monthly shortage bands (TAF and %). Same structure plus shortage-specific columns.


| Column                   | Type          | Description                                    |
| ------------------------ | ------------- | ---------------------------------------------- |
| `shortage_avg_taf`       | NUMERIC(10,2) | Mean TAF shortage for this month               |
| `shortage_cv`            | NUMERIC(10,4) | CV of TAF shortage                             |
| `shortage_pct_avg`       | NUMERIC(10,4) | Mean shortage as % of demand                   |
| `shortage_pct_cv`        | NUMERIC(10,4) | CV of shortage %                               |
| `shortage_frequency_pct` | NUMERIC(10,4) | Fraction of months with shortage > threshold   |
| `q0..q100`               | NUMERIC(10,2) | Percentile bands of monthly shortage TAF       |
| `exc_p5..exc_p95`        | NUMERIC(10,2) | Exceedance percentiles of monthly shortage TAF |


### `refuge_du_period_summary`

One row per `(scenario, du_id)`. Period-of-record summary statistics.


| Column                              | Type          | Description                          |
| ----------------------------------- | ------------- | ------------------------------------ |
| `annual_delivery_avg_taf`           | NUMERIC(10,2) | Mean of annual delivery totals       |
| `annual_delivery_cv`                | NUMERIC(10,4) | CV of annual delivery                |
| `annual_shortage_avg_taf`           | NUMERIC(10,2) | Mean of annual shortage totals       |
| `annual_shortage_cv`                | NUMERIC(10,4) | CV of annual shortage                |
| `annual_shortage_pct_avg`           | NUMERIC(10,4) | Mean annual shortage as % of demand  |
| `annual_shortage_pct_cv`            | NUMERIC(10,4) | CV of annual shortage %              |
| `reliability_pct_95`                | NUMERIC(10,4) | 95th percentile of annual shortage % |
| `delivery_exc_p5..delivery_exc_p95` | NUMERIC(10,2) | Annual delivery exceedance curve     |
| `simulation_start_year`             | INTEGER       | First water year included            |
| `simulation_end_year`               | INTEGER       | Last water year included             |
| `total_years`                       | INTEGER       | Total simulated years                |


---

## Metric coverage verification


| Specified metric       | Unit         | Temporal          | Statistics                | Covered by                                                                                                                                     |
| ---------------------- | ------------ | ----------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Surface water delivery | acre-feet    | Monthly, annually | Annual avg, annual CV     | `refuge_du_delivery_monthly` (monthly bands), `refuge_du_period_summary` (annual_delivery_avg_taf, annual_delivery_cv)                         |
| Delivery shortage      | acre-feet    | Monthly, annually | Annual avg, annual CV     | `refuge_du_shortage_monthly` (monthly TAF bands), `refuge_du_period_summary` (annual_shortage_avg_taf, annual_shortage_cv)                     |
| Delivery shortage      | % of demand  | Monthly, annually | Monthly avg/CV, annual CV | `refuge_du_shortage_monthly` (shortage_pct_avg, shortage_pct_cv), `refuge_du_period_summary` (annual_shortage_pct_avg, annual_shortage_pct_cv) |
| Delivery reliability   | % (95th pct) | Period of record  | Single value per DU       | `refuge_du_period_summary` (reliability_pct_95)                                                                                                |


---

## Demand unit reference

All 18 refuge demand units from `database/seed_tables/04_calsim_data/du_refuge_entity.csv`.
Tables reproduced below for verification against the CalSim 3 Main Report (Tables 3-9 and 3-10).

**Type codes:** PR = Project Refuge (CVP/Central Valley Project contract deliveries); NR = Non-project Refuge (water rights only, no CVP deliveries).
The `cs3_type` column in `du_refuge_entity` stores these values. See open question #6 for whether
a separate lookup table should be created to support frontend display.

**GW / SW flags:** The `gw` and `sw` boolean columns indicate whether each demand unit has access
to groundwater and surface water respectively, as documented in CalSim 3 Main Report Tables 3-9
and 3-10. These attributes should be surfaced in the frontend (tooltip or attribute panel) since
they affect how delivery shortages should be interpreted.a GW-capable unit has a fallback supply
that a SW-only unit does not.

### Sacramento River hydrologic region (Table 3-9)


| DU_ID     | Refuge / Wildlife area                                          | Managed by     | Water provider                   | GW  | SW  | Point of diversion / conveyance                              |
| --------- | --------------------------------------------------------------- | -------------- | -------------------------------- | --- | --- | ------------------------------------------------------------ |
| `08N_PR1` | Sacramento NWR                                                  | USFWS          | Reclamation                      | -   | •   | Glenn-Colusa Canal                                           |
| `08N_PR2` | Delevan NWR                                                     | USFWS          | Reclamation                      | -   | •   | Glenn-Colusa Canal                                           |
| `08S_PR`  | Colusa NWR                                                      | USFWS          | Reclamation                      | -   | •   | Glenn-Colusa Canal, Colusa Basin Drain                       |
| `09_PR`   | Llano Seco Unit, Upper Butte Basin WA, Sacramento River NWR     | CDFW, USFWS    | Water rights                     | -   | •   | Sacramento River, Butte Creek                                |
| `11_PR`   | Upper Butte Basin WA - Little Dry Creek and Howard Slough Units | CDFW           | Western Canal WD, Richvale ID    | •   | •   | Thermalito Afterbay via Western Canal and Sutter-Butte Canal |
| `17N_NR`  | Butte Sink Duck Clubs                                           | Private, USFWS | Water rights, Western Canal WD   | -   | •   | Thermalito Afterbay via Western Canal, Butte Creek           |
| `17N_PR`  | Gray Lodge WA                                                   | CDFW           | Reclamation, DWR (by exchange)   | •   | •   | Thermalito Afterbay via Biggs-West Gridley WD canals         |
| `17S_PR`  | Sutter NWR                                                      | USFWS          | Reclamation, Sutter Extension WD | -   | •   | Sutter Bypass, Sutter Extension Canal                        |


### San Joaquin River and Tulare Lake hydrologic regions (Table 3-10)


| DU_ID    | Refuge / Wildlife area                                                    | Managed by | Water provider           | GW  | SW  | Point of diversion / conveyance                                                                            |
| -------- | ------------------------------------------------------------------------- | ---------- | ------------------------ | --- | --- | ---------------------------------------------------------------------------------------------------------- |
| `63_PR1` | Arena Plains and Snow Bird units, Merced NWR                              | USFWS      |                          | -   | •   | Drainage water from Turlock ID and Stevenson WC                                                            |
| `63_PR2` | Merced and Lone Tree Units, Merced NWR                                    | USFWS      | Reclamation              | •   | •   | Merced ID, via Deadman Creek and Duck Slough                                                               |
| `63_PR3` | East Bear Creek Unit, San Luis NWR                                        | USFWS      | Reclamation              | •   | •   | Eastside Bypass, Bear Creek, Livingston Drain                                                              |
| `72_PR1` | Volta WA                                                                  | CDFW       | Reclamation              | -   | •   | Delta-Mendota Canal via the Volta Wasteway, Central California ID Main Canal                               |
| `72_PR2` | Kesterson NWR, Freitas Unit and Blue Goose Unit (San Luis NWR)            | USFWS      | Reclamation              | •   | •   | Grassland WD via San Luis Canal, Santa Fe Canal, and Fremont Canal                                         |
| `72_PR3` | San Luis Unit and West Bear Creek Unit, San Luis NWR                      | USFWS      | Reclamation              | -   | •   | San Luis Canal Company via island C Canal, Salt Slough                                                     |
| `72_PR4` | Los Banos WA, Gadwall/Salt Slough/China Island Units (North Grassland WA) | CDFW       | Reclamation              | •   | •   | San Luis Canal Company via San Pedro Canal, West Delta Canal, Grassland WD Boundary Drain, and Salt Slough |
| `72_PR5` | Grassland WD - north                                                      | Private    | Reclamation              | -   | •   | Delta-Mendota Canal via the Volta Wasteway, Central California ID Main Canal                               |
| `72_PR6` | Grassland WD - south                                                      | Private    | Reclamation              | -   | •   | Central California ID via Main Canal, Arroyo Canal, and San Pedro Canal                                    |
| `91_PR`  | Mendota WA                                                                | CDFW       | Reclamation water rights | -   | •   | Mendota Pool via Fresno Slough                                                                             |


### Full CSV.`du_refuge_entity.csv`

```csv
"DU_ID","WBA_ID","hydrologic_region","Dups","Class","CS3_Type","total_acres","polygon_count","refuge_or_wildlife_area","managed_by","provider","gw","sw","point_of_diversion_conveyance","source","model_source","has_gis_data"
"08N_PR1","08N","SAC","0","Refuge","PR","10875.4872444","1","Sacramento NWR","USFWS","Reclamation","0","1","Glenn-Colusa Canal","geopackage,calsim_report","calsim3","True"
"08N_PR2","08N","SAC","0","Refuge","PR","5832.93450985","1","Delevan NWR","USFWS","Reclamation","0","1","Glenn-Colusa Canal","geopackage,calsim_report","calsim3","True"
"08S_PR","08S","SAC","0","Refuge","PR","4099.41153749","1","Colusa NWR","USFWS","Reclamation","0","1","Glenn-Colusa Canal, Colusa Basin Drain","geopackage,calsim_report","calsim3","True"
"09_PR","09","SAC","0","Refuge","PR","3304.863684099","3","Llano Seco Unit, Upper Butte Basin WA, Sacramento River NWR","CDFW, USFWS","Water rights","0","1","Sacramento River, Butte Creek","geopackage,calsim_report","calsim3","True"
"11_PR","11","SAC","-1","Refuge","PR","7664.99988766","2","Upper Butte Basin WA - Little Dry Creek and Howard Slough Units","CDFW","Western Canal WD, Richvale ID","1","1","Thermalito Afterbay via Western Canal and Sutter-Butte Canal","geopackage,calsim_report","calsim3","True"
"17N_NR","17N","SAC","-1","Refuge","NR","7670.82240852","1","Butte Sink Duck Clubs","Private, USFWS","Water rights, Western Canal WD","0","1","Thermalito Afterbay via Western Canal, Butte Creek","geopackage,calsim_report","calsim3","True"
"17N_PR","17N","SAC","-1","Refuge","PR","8447.60874191","1","Gray Lodge WA","CDFW","Reclamation, DWR (by exchange)","1","1","Thermalito Afterbay via Biggs-West Gridley WD canals","geopackage,calsim_report","calsim3","True"
"17S_PR","17S","SAC","-1","Refuge","PR","3746.60996333","1","Sutter NWR","USFWS","Reclamation, Sutter Extension WD","0","1","Sutter Bypass, Sutter Extension Canal","geopackage,calsim_report","calsim3","True"
"63_PR1","63","SJR","-1","Refuge","PR","2484.560611","1","Arena Plains and Snow Bird units, Merced NWR","USFWS","","0","1","Drainage water from Turlock ID and Stevenson WC","geopackage,calsim_report","calsim3","True"
"63_PR2","63","SJR","-1","Refuge","PR","5806.17630416","1","Merced and Lone Tree Units, Merced NWR","USFWS","Reclamation","1","1","Merced ID, via Deadman Creek and Duck Slough","geopackage,calsim_report","calsim3","True"
"63_PR3","63","SJR","-1","Refuge","PR","4117.25870339","1","East Bear Creek Unit, San Luis NWR","USFWS","Reclamation","1","1","Eastside Bypass, Bear Creek, Livingston Drain","geopackage,calsim_report","calsim3","True"
"72_PR1","72","SJR","-1","Refuge","PR","2788.9306764867","2","Volta WA","CDFW","Reclamation","0","1","Delta-Mendota Canal via the Volta Wasteway, Central California ID Main Canal","geopackage,calsim_report","calsim3","True"
"72_PR2","72","SJR","-1","Refuge","PR","11489.308702213","4","Kesterson NWR, Freitas Unit and Blue Goose Unit (San Luis NWR)","USFWS","Reclamation","1","1","Grassland WD via San Luis Canal, Santa Fe Canal, and Fremont Canal","geopackage,calsim_report","calsim3","True"
"72_PR3","72","SJR","-1","Refuge","PR","12901.912344269998","3","San Luis Unit and West Bear Creek Unit, San Luis NWR","USFWS","Reclamation","0","1","San Luis Canal Company via island C Canal, Salt Slough","geopackage,calsim_report","calsim3","True"
"72_PR4","72","SJR","-1","Refuge","PR","11261.214398820699","8","Los Banos WA, Gadwall/Salt Slough/China Island Units (North Grassland WA)","CDFW","Reclamation","1","1","San Luis Canal Company via San Pedro Canal, West Delta Canal, Grassland WD Boundary Drain, and Salt Slough","geopackage,calsim_report","calsim3","True"
"72_PR5","72","SJR","-1","Refuge","PR","27106.162152633","2","Grassland WD - north","Private","Reclamation","0","1","Delta-Mendota Canal via the Volta Wasteway, Central California ID Main Canal","geopackage,calsim_report","calsim3","True"
"72_PR6","72","SJR","-1","Refuge","PR","20701.0365681353","5","Grassland WD - south","Private","Reclamation","0","1","Central California ID via Main Canal, Arroyo Canal, and San Pedro Canal","geopackage,calsim_report","calsim3","True"
"91_PR","91","TULARE","0","Refuge","PR","","0","Mendota WA","CDFW","Reclamation water rights","0","1","Mendota Pool via Fresno Slough","calsim_report","calsim3","False"
```

**Notes from CalSim 3 Main Report:**

- `09_PR` encompasses Llano Seco Unit (CDFW) and Sacramento River NWR (USFWS).two managing agencies
- `17N_NR` is the only NR (non-priority) unit. Represents private Butte Sink Duck Clubs (see open question #2)
- `72_PR2` aggregates Kesterson NWR + Freitas Unit + Blue Goose Unit of San Luis NWR into a single DU
- `72_PR3` aggregates San Luis Unit + West Bear Creek Unit of San Luis NWR
- `72_PR4` aggregates Los Banos WA + three North Grassland WA units (8 polygons total)
- `72_PR5` and `72_PR6` are privately managed Grassland Water District lands
- `91_PR` is in the Tulare Lake hydrologic region but diverts from Fresno Slough (San Joaquin watershed)
- `91_PR` has no GIS geometry in the database. Acreage not recorded

---

## Uploading entity data to the database

The `du_refuge_entity` table must be created and seeded before the statistics ETL can run.
Seed data is loaded directly from the repo via `\copy`.no S3 upload is required.
Run from the repo root (Cloud9 or local with VPN):

```bash
# Create table and load seed data from repo CSV
psql $SUPERUSER_URL -f database/sql_archive/log/20_create_refuge_entity_table.sql
```

To reload after editing the CSV (e.g., correcting provider or GW/SW values):

```bash
psql $SUPERUSER_URL -c "TRUNCATE du_refuge_entity CASCADE;"
psql $SUPERUSER_URL -f database/sql_archive/log/20_create_refuge_entity_table.sql
```

---

## Usage

```bash
# Process single scenario and write to database
DATABASE_URL=postgres://... python main.py --scenario s0020

# Process all known scenarios
DATABASE_URL=postgres://... python main.py --all-scenarios

# Dry run (calculate without writing)
python main.py --scenario s0020 --dry-run

# Output as JSON (for debugging)
python main.py --scenario s0020 --dry-run --output-json
```

---

## Data lineage

All statistics in this module are derived from CalSim 3 model outputs for COEQWAL scenarios.


| Data type       | CalSim source file                             | DSS Part C        | Notes                                              |
| --------------- | ---------------------------------------------- | ----------------- | -------------------------------------------------- |
| Demand (AWO_*)  | SV input (`*_coeqwal_sv_input.csv`)            | `APPLIED-WATER`   | Applied water requirement, TAF                     |
| Delivery (DN_*) | Main DV output (`*_coeqwal_calsim_output.csv`) | `SW-DELIVERY-NET` | Net surface water delivery, CFS to converted to TAF |
| Shortage        | Derived: `demand − delivery`                   |.                | No native CalSim shortage variable for refuge DUs  |


The `created_by` field on all inserted rows is set to `2` (jfantauzza) to correctly attribute ETL-generated
data. See `database/sql_archive/00_versioning/01_create_audit_trigger_function.sql` for the trigger logic.

---

## Open questions

1. `**AWO`** vs `AW*`* in staged SV CSV**: The raw DSS uses `AWO_{DU_ID}` naming (confirmed in
  `coeqwal_s9999_SV_v0.1.1.dsc`). Verify whether the staged `*_coeqwal_sv_input.csv` in S3
   preserves `AWO`_* or renames columns to `AW_`*. The ETL loader handles both names.
2. `**17N_NR` inclusion**: This non-priority (NR) unit represents private Butte Sink duck clubs, not
  a federally managed refuge. Confirm whether it should be included in refuge delivery reporting
   or treated separately.
3. **Zero-demand months**: Some demand units have seasonal-only demands (e.g., waterfowl flooding is
  Oct-Feb only). When `demand_taf == 0`, shortage % is undefined. The ETL sets `shortage_pct = 0`
   for zero-demand months.
4. **Scenarios with refuge-specific operations**: Scenarios s0029, s0031, s0032 modify environmental
  flow requirements that may indirectly affect refuge deliveries. No scenario-specific handling is  
   needed in the ETL. The statistics are computed identically across all scenarios.
5. **Reliability calculation method**: The current implementation uses the 95th percentile of the
  distribution of annual shortage percentages: `reliability_pct_95 = np.percentile(annual_shortage_pct, 95)`.
   Interpretation: in 95 out of 100 simulated years, annual shortage is at or below this value.
   **This method has not been formally validated against a published standard or prior COEQWAL
   analysis.** Alternative approaches include: (a) fraction of years with zero shortage (binary
   reliability), (b) fraction of years with shortage below a threshold, (c) exceedance-curve value
   at 95% exceedance probability on annual delivery. Confirm the intended definition with the
   modeling team before using `reliability_pct_95` in the interface.
6. **PR/NR type lookup table**: The `cs3_type` field in `du_refuge_entity` stores `PR` (Project refuge? CVP contract deliveries) and `NR` (Non-nonproject...water rights?) as plain strings.
  here is no separate database lookup table for these codes. If the frontend should display a
  escriptive label or tooltip for these types (e.g., in a paragraph description or attribute
  anel), consider adding a `du_refuge_type` lookup table in the Layer 01 (lookup) schema. Would
  lso consolidate with any other `cs3_type` categorizations used in the model.
7. **DV output variable availability.resolved**: All 18 `DN_{DU_ID}` variables are confirmed
  present in the DV DSS for scenario s0020 and will be present in `*_coeqwal_calsim_output.csv`
   for all scenarios using the same CalSim 3 variable structure. No additional staging is needed.

