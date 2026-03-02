# ETL statistics — Environmental river flows

> **Status: DEFERRED — documentation only.**
>
> This README documents the planned CalSim variables, data sources, and calculation methodology for
> environmental river flow metrics. ETL implementation is pending resolution of the open questions
> listed at the end of this file. No calculation code exists in this module yet.

## Overview

Three metrics are planned for 17 river reach locations across California:

| Metric | Unit | Temporal | Statistics | Description |
|--------|------|----------|------------|-------------|
| River flows (% unimpaired) | % | Monthly, seasonally | Monthly avg, monthly CV | Simulated flow as percent of natural unimpaired flow |
| River flows (% functional flows) | % | Seasonally | Seasonal avg deviation from FF targets, annual CV | Simulated flow as percent of prescribed functional flow targets |
| River flow alteration index | Correlation coefficient | Period of record | Single value per reach (by season or month) | Pearson correlation between simulated and unimpaired monthly flows |

These metrics characterize how much CalSim-modeled water management alters natural river hydrology,
and how well environmental flow requirements are being met.

---

## Data sources

### 1. TR/DV CSV — simulated channel flows and MIF

| Attribute | Value |
|-----------|-------|
| S3 path | `s3://coeqwal-model-run/scenario/{scenario}/csv/{scenario}_DV_v*.csv` |
| Local example | `data/example_data/s0020_DCRadjBL_2020LU_wTUCP_DV_v0.1.csv` |
| Description | "Trend Report" — a curated subset of the CalSim 3 DV (Decision Variable) output |
| Units | CFS — conversion to TAF required: `TAF = CFS × 0.001984 × days_in_month` |
| Time range | 1,200 months (October 1921 – September 2021) |
| Staging | **Not yet staged in S3 per scenario.** Staging this file is a prerequisite for ETL implementation. |

**Variables confirmed present in TR CSV (all Part F = `L2020A`):**

| Variable pattern | Part C | Description |
|---|---|---|
| `C_{reach_code}` | `CHANNEL` | Simulated monthly channel flow at reach |
| `C_{reach_code}_MIF` | `FLOW-MIN-INSTREAM` | Model-computed minimum instream flow requirement |

**Data quality notes:**
- `C_SAC122` appears **twice** in the TR CSV (both with Part C = `CHANNEL`). Use only the first occurrence.
- `C_SAC000_MIF` is **absent** from both the TR CSV and the DV DSC catalog. The `C_SAC000` channel
  flow variable is present, but has no MIF companion in the s0020 baseline.

### 2. SV input CSV — functional flow targets and unimpaired flows

| Attribute | Value |
|-----------|-------|
| S3 path | `s3://coeqwal-model-run/scenario/{scenario}/csv/{scenario}_coeqwal_sv_input.csv` |
| Units | CFS — conversion to TAF required for volumetric stats |
| Staging | Staged per scenario (same as used for refuge demand) |

**Variables confirmed present in SV DSC (`coeqwal_s9999_SV_v0.1.1.dsc`):**

| Variable pattern | Part C | Description |
|---|---|---|
| `EFLOWS_{reach_code}` | `FLOW-MIN-EFLOW` | Functional flow target input to CalSim — all 17 reaches confirmed |
| `UNIMP_{watershed}` | `FLOW-UNIMPAIRED` | Natural/unimpaired flow at watershed level — 11 variants |

> **Do not use `UNIMP_*_UHH` variants.** The `_UHH` suffix indicates "upper-half hydrology" —
> a different hydrological representation. Use base `UNIMP_*` names only.

---

## `_MIF` vs `EFLOWS_*` — important distinction

These two variable types serve different purposes and should NOT be substituted for each other:

| | `C_{reach}_MIF` | `EFLOWS_{reach}` |
|---|---|---|
| Source | DV output (CalSim computes it) | SV input (prescribed as model constraint) |
| Part C | `FLOW-MIN-INSTREAM` | `FLOW-MIN-EFLOW` |
| Meaning | **Total binding MIF** — combines D-1641, VAMP, biological opinions, EFLOWS, and all other regulatory minimums into a single enforced floor | **Functional flow target component only** — the prescribed FF target for scenarios s0029, s0031, s0032, s0033 |
| Scenario dependence | Changes across scenarios as regulatory frameworks differ | Fixed per SV version (same across all scenarios using the same SV file) |
| `SAC000` available? | **No** — absent from DV DSC and TR CSV | **Yes** — `EFLOWS_SAC000` confirmed in SV DSC |

**For the "% functional flows" metric, use `EFLOWS_{reach}`** (SV input, fixed FF target) as the
denominator. This measures specifically how much flow the model delivers relative to the FF target,
rather than relative to the combined regulatory floor.

**For the alteration index and % unimpaired metrics, use `UNIMP_{watershed}`** (SV input) as the
natural-flow reference.

The existing tier assignment (pre-computed externally) uses `C_{reach}_MIF` for a binary shortage
frequency metric — that is a different calculation from the continuous percentage metrics planned here.

---

## Reach inventory

All 17 reaches used in the COEQWAL environmental flows tier analysis:

| Reach Code | River / Location | `UNIMP_*` Variable | `_MIF` in TR? | Notes |
|---|---|---|---|---|
| `AMR004` | American River | `UNIMP_FOLS` | Yes | Folsom watershed |
| `FTR003` | Feather River (upper) | `UNIMP_OROV` | Yes | Oroville watershed |
| `FTR029` | Feather River at Yuba City | `UNIMP_OROV` | Yes | |
| `YUB002` | Yuba River at Marysville | `UNIMP_YUBA` | Yes | |
| `MCD005` | Merced River at Stevinson | `UNIMP_ME` | Yes | MCD = Merced, confirmed in COEQWAL archive notebook |
| `MOK028` | Mokelumne River | **TBD** | Yes | No `UNIMP_MOK` found in SV DSC — needs modeling team confirmation |
| `SAC000` | Sacramento River (Delta confluence) | `UNIMP_SHAS` | **No** | `C_SAC000` present but `C_SAC000_MIF` absent from DV |
| `SAC049` | Sacramento River | `UNIMP_SHAS` | Yes | |
| `SAC122` | Sacramento River at Tisdale | `UNIMP_SHAS` | Yes | Duplicate column in TR CSV — use first occurrence |
| `SAC148` | Sacramento River at Colusa Weir | `UNIMP_SHAS` | Yes | |
| `SAC257` | Sacramento River above Bend Bridge | `UNIMP_SRBB` | Yes | Bend Bridge gauge — may differ from SHAS |
| `SAC289` | Sacramento River (South) | `UNIMP_SHAS` | Yes | Confirm SHAS vs SRBB mapping |
| `SJR070` | San Joaquin near Vernalis | `UNIMP_SJ` | Yes | |
| `SJR127` | San Joaquin at Salt Slough | `UNIMP_SJ` | Yes | |
| `STS011` | Stanislaus River | `UNIMP_ST` | Yes | |
| `TUO003` | Tuolumne River | `UNIMP_TU` | Yes | |
| `TRN111` | Trinity River at Lewiston | `UNIMP_TRIN` | Yes | |

**`UNIMP_*` variables available in SV (non-UHH):**
`UNIMP_FOLS`, `UNIMP_ME`, `UNIMP_OROV`, `UNIMP_SHAS`, `UNIMP_SJ`, `UNIMP_SRBB`,
`UNIMP_ST`, `UNIMP_TRIN`, `UNIMP_TU`, `UNIMP_WH`, `UNIMP_YUBA`

---

## Planned calculations

### Metric 1 — River flows (% unimpaired)

```python
# Per timestep:
pct_unimpaired = C_{reach} / UNIMP_{watershed} * 100   # both in CFS

# Per water month, across all years:
pct_unimpaired_avg = mean(pct_unimpaired[water_month == m])
pct_unimpaired_cv  = std / mean
```

### Metric 2 — River flows (% functional flows)

```python
# Per timestep:
pct_ff = C_{reach} / EFLOWS_{reach} * 100   # both in CFS

# Per season (season definitions TBD — see Open Questions):
seasonal_pct_ff_avg   = mean(pct_ff for months in season)
seasonal_deviation    = seasonal_pct_ff_avg - 100.0     # negative = below target

# Annual CV:
annual_mean_pct_ff = mean(monthly pct_ff over water year)
annual_cv          = std(annual_mean_pct_ff) / mean(annual_mean_pct_ff)
```

Note: when `EFLOWS_{reach} == 0` (reaches without FF requirements in baseline scenarios), the
ratio is undefined. Handle by setting `pct_ff = null` when denominator is zero or below a threshold.

### Metric 3 — Flow alteration index (correlation coefficient)

```python
# Full period of record, monthly time series:
r, p_value = pearsonr(C_{reach}_series, UNIMP_{watershed}_series)
# Result: r close to +1 = well-preserved natural flow timing
#         r close to 0 = flow pattern substantially altered by operations
```

---

## Prerequisite: stage TR CSV in S3


The TR/DV CSV is not currently staged per scenario in S3. Before implementing this ETL module,
the DSS extraction pipeline must be updated to export the TR CSV for each scenario using the
same format as `data/example_data/s0020_DCRadjBL_2020LU_wTUCP_DV_v0.1.csv`.

Suggested S3 path: `s3://coeqwal-model-run/scenario/{scenario}/csv/{scenario}_DV_v*.csv`

The TR CSV can also serve as a **verification reference** — the pre-computed tier results in
`data/intake/tier_data_upload/WIDE_FORMAT_ALTERNATIVE/env_flows_wide.csv` can be cross-checked
against ETL-computed shortage frequencies.

---

## Open questions

1. **MOK028 → unimpaired mapping**: No `UNIMP_MOK` (Mokelumne) variable exists in the SV DSC.
   Confirm with modeling team whether MOK028 should map to `UNIMP_SJ` or a separate reference.

2. **SAC mainstem UNIMP sub-reach mapping**: `UNIMP_SHAS` (Shasta/Sacramento headwaters) and
   `UNIMP_SRBB` (Sacramento at Bend Bridge) both apply to Sacramento mainstem reaches. Confirm
   which UNIMP variable is appropriate for each of SAC000, SAC049, SAC122, SAC148, SAC257, SAC289.

3. **Season definitions for % functional flows and alteration index**: COEQWAL scenarios reference
   functional flow seasonal targets. Confirm whether to use the 5-season CEFF calendar (wet peak,
   wet base, spring recession, dry season, fall pulse) or a simpler seasonal grouping.

4. **`C_SAC000_MIF` absence**: SAC000 has no MIF companion in the baseline DV output. Determine
   whether to (a) exclude SAC000 from MIF-based analysis, (b) use `EFLOWS_SAC000` from SV as a
   proxy, or (c) treat SAC000 as a special case.

5. **TR CSV staging logistics**: Coordinate with data pipeline team to stage the TR/DV CSV in S3
   for all scenarios before building the ETL Python module.

6. **`_UHH` variant clarification**: Confirm that `UNIMP_*_UHH` variants represent an alternative
   hydrological baseline and should always be excluded from COEQWAL analysis.
