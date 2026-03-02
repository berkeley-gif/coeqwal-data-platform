# ETL statistics — Environmental river flows

> **Status: READY FOR IMPLEMENTATION**
>
> All blocking design decisions resolved. Watershed and channel attribute schema complete
> (migration 23). Next: migration 24 (statistics tables + season seed data), then ETL script.

---

## Overview

Three metrics are computed for **60 river channel reaches** in the CalSim DV output,
covering streams, reservoir releases, and conveyance canals across California:

| # | Metric | Unit | Temporal | Statistics |
|---|--------|------|----------|------------|
| 1 | River flows — % unimpaired | % | Monthly | Monthly avg, monthly CV per reach per scenario |
| 2 | River flows — % functional flows | % | Seasonal (5 CEFF seasons) | Seasonal avg deviation from FF target, annual CV |
| 3 | River flow alteration index | Pearson r | Period of record | Correlation between simulated and unimpaired monthly flow |

These metrics characterize how much CalSim-modeled water management alters natural river
hydrology, and how well environmental flow requirements are being met.

---

## Data sources

All required variables are in the **standard per-scenario DV and SV CSV files** already staged
in S3. No additional data pipeline work is required before building this ETL module.

### 1. CalSim DV output CSV — simulated channel flows and MIF

| Attribute | Value |
|-----------|-------|
| S3 path | `s3://coeqwal-model-run/scenario/{scenario}/csv/{scenario}_coeqwal_calsim_output.csv` |
| Fallback | `s3://coeqwal-model-run/scenario/{scenario}/csv/{scenario}_DV_v*.csv` |
| Local example | `data/example_data/s0020_DCRadjBL_2020LU_wTUCP_DV_v0.1.csv` |
| Units | CFS (Part F = PER-AVER). Volume conversion: `TAF = CFS × 0.001984 × days_in_month` |
| Time range | 1,200 months (October 1921 – September 2021, water years 1922–2021) |

**Variables in DV (Part B pattern → Part C label):**

| Variable pattern | Part C | Description |
|---|---|---|
| `C_{reach_code}` | `CHANNEL` | Simulated monthly channel flow |
| `C_{reach_code}_MIF` | `FLOW-MIN-INSTREAM` | Model-computed binding minimum instream flow |

**Data quality notes:**
- `C_SAC122` appears **twice** in the DV (both `CHANNEL`). Use the **first occurrence** only.
- `C_SAC000_MIF` is **absent** from the DV — SAC000 has no MIF companion variable.
- The "Trend Report" CSV (`_DV_v*.csv`) is a reference artifact only. Read variables directly
  from the standard `_coeqwal_calsim_output.csv` using Part B and Part C filters.

### 2. CalSim SV input CSV — functional flow targets and unimpaired flows

| Attribute | Value |
|-----------|-------|
| S3 path | `s3://coeqwal-model-run/scenario/{scenario}/csv/{scenario}_coeqwal_sv_input.csv` |
| Units | CFS. Same volume conversion applies where TAF is needed. |
| Staging | **Already staged per scenario** (same file used for refuge demand ETL). |

**Variables in SV (Part B pattern → Part C label):**

| Variable pattern | Part C | Description |
|---|---|---|
| `EFLOWS_{reach_code}` | `FLOW-MIN-EFLOW` | Functional flow target input — confirmed for 17 env-flow reaches |
| `UNIMP_{watershed}` | `FLOW-UNIMPAIRED` | Natural unimpaired flow at watershed gauge — 11 variants |

> **Do not use `UNIMP_*_UHH` variants.** The `_UHH` suffix indicates "upper-half hydrology,"
> a different hydrological baseline. Always use base `UNIMP_*` names.

---

## `C_*_MIF` vs `EFLOWS_*` — important distinction

| | `C_{reach}_MIF` | `EFLOWS_{reach}` |
|---|---|---|
| Source | DV output (CalSim computes it) | SV input (prescribed as model constraint) |
| Part C | `FLOW-MIN-INSTREAM` | `FLOW-MIN-EFLOW` |
| Meaning | **Total binding MIF** — combines D-1641, VAMP, biological opinions, EFLOWS, and all other regulatory minimums into a single enforced floor | **Functional flow target only** — the prescribed FF target for e-flow scenarios (s0029, s0031, s0032, s0033) |
| Scenario dependence | Changes across scenarios as regulatory frameworks vary | Fixed per SV version — same across all scenarios sharing the same SV |
| `SAC000` available? | **No** — absent from DV | **Yes** — `EFLOWS_SAC000` confirmed in SV |

- **Metric 1 and 3** use `UNIMP_{watershed}` from SV as the natural-flow reference.
- **Metric 2** uses `EFLOWS_{reach}` from SV as the functional flow target denominator.

---

## Reach inventory

**60 channels** appear in the CalSim DV with `Part C = CHANNEL`. All are included in the ETL.
Channels are attributed in `channel_entity` (migration 23) with `watershed_short_code`,
`unimp_sv_variable`, `has_mif`, `has_eflows`, and `channel_class`.

### Channels with MIF companion (20)

These channels have a `C_{reach}_MIF` variable in the DV and are the primary env-flow monitoring
locations. They are candidates for all three metrics.

| Reach | Location | Watershed | `UNIMP_*` | EFLOWS? |
|-------|----------|-----------|-----------|---------|
| `AMR004` | American River at I-80 Bridge | UPPER_AMERICAN | `UNIMP_FOLS` | ✓ |
| `FTR003` | Feather River | UPPER_FEATHER | `UNIMP_OROV` | ✓ |
| `FTR029` | Feather River at Yuba City | UPPER_FEATHER | `UNIMP_OROV` | ✓ |
| `FTR059` | Feather River at Thermalito Afterbay | UPPER_FEATHER | `UNIMP_OROV` | — |
| `KSWCK` | Keswick Dam (Sacramento below Shasta) | SAC_UPPER | `UNIMP_SHAS` | — |
| `MCD005` | Merced River at Stevinson | UPPER_MERCED | `UNIMP_ME` | ✓ |
| `MOK028` | Mokelumne River | UPPER_MOKELUMNE | — (see note) | ✓ |
| `NTOMA` | American River at Lake Natoma | UPPER_AMERICAN | `UNIMP_FOLS` | — |
| `SAC049` | Sacramento River at Freeport | SAC_LOWER | `UNIMP_SRBB` | ✓ |
| `SAC122` | Sacramento River at Tisdale Weir | SAC_LOWER | `UNIMP_SRBB` | ✓ |
| `SAC148` | Sacramento River at Colusa Weir | SAC_LOWER | `UNIMP_SRBB` | ✓ |
| `SAC257` | Sacramento River at Bend Bridge | SAC_LOWER | `UNIMP_SRBB` | ✓ |
| `SAC289` | Sacramento River at South Bonnieville | SAC_UPPER | `UNIMP_SHAS` | ✓ |
| `SJR070` | San Joaquin near Vernalis | SAN_JOAQUIN | `UNIMP_SJ` | ✓ |
| `SJR127` | San Joaquin at Salt Slough | SAN_JOAQUIN | `UNIMP_SJ` | ✓ |
| `STS011` | Stanislaus River | UPPER_STANISLAUS | `UNIMP_ST` | ✓ |
| `STS059` | Stanislaus River (upper) | UPPER_STANISLAUS | `UNIMP_ST` | — |
| `TRN111` | Trinity River at Lewiston | TRINITY_RIVER | `UNIMP_TRIN` | ✓ |
| `TUO003` | Tuolumne River | UPPER_TUOLUMNE | `UNIMP_TU` | ✓ |
| `YUB002` | Yuba River at Marysville | YUBA_RIVER | `UNIMP_YUBA` | ✓ |

### Additional channels without MIF (40)

All remaining 40 channels (Sacramento mainstem nodes, other tributaries, reservoir releases,
canals) are computed for Metric 1 (% unimpaired) where a `UNIMP_*` variable is available.
Metric 2 and 3 are skipped where `EFLOWS_*` or `UNIMP_*` are absent.

Full list with watershed and UNIMP mapping: see `channel_entity` table,
`WHERE channel_class IS NOT NULL`.

### `UNIMP_*` variable assignments (Sacramento mainstem split)

The Sacramento River mainstem uses two different unimpaired flow references, split at Bend Bridge
(rm 257). This is reflected in the `watershed` table and `channel_entity.unimp_sv_variable`:

| Sub-reach | River Miles | `UNIMP_*` | Channels |
|-----------|-------------|-----------|---------|
| `SAC_UPPER` — above Bend Bridge | rm 257–310 | `UNIMP_SHAS` | SHSTA, KSWCK, SAC289 |
| `SAC_LOWER` — at/below Bend Bridge | rm 0–257 | `UNIMP_SRBB` | SAC257, SAC240, SAC201, SAC148, SAC122, SAC120, SAC085, SAC083, SAC049, SAC048, SAC041, SAC029B, SAC007, SAC000, SSL001, YBP020 |

`UNIMP_SRBB` = "Sacramento River Below Bend Bridge" — captures additional inflow
(Cottonwood Creek, Stony Creek) between Shasta and Bend Bridge.

### Mokelumne River — no UNIMP variable

No `UNIMP_MOK` variable exists in the CalSim SV. The Mokelumne is entirely regulated
by East Bay MUD below Camanche Reservoir and does not have a standalone unimpaired flow node
in the CalSim SV input. `unimp_sv_variable` is NULL for MOK019 and MOK028. Metric 1 and
Metric 3 cannot be computed for these reaches. Metric 2 (% functional flows vs EFLOWS_MOK028)
can still be computed for MOK028.

---

## Season definitions

Metric 2 (% functional flows) is computed by **water year season** using the
**5-season CEFF (California Environmental Flows Framework) calendar**.

Water year definition: October = WY month 1, September = WY month 12.

| Season ID | `short_code` | Calendar Months | WY Months | Ecological Role |
|-----------|--------------|-----------------|-----------|-----------------|
| 1 | `wet_peak` | December, January, February | 3, 4, 5 | High-flow wet season pulse; spawning habitat creation |
| 2 | `wet_base` | March, April | 6, 7 | Sustained baseflow after wet-season peak |
| 3 | `spring_recession` | May, June | 8, 9 | Snowmelt-driven gradual recession; juvenile fish outmigration |
| 4 | `dry` | July, August, September, October | 10, 11, 12, 1 | Summer low flows; thermal stress period |
| 5 | `fall_pulse` | November | 2 | First flush; adult salmon migration trigger |

Note: the dry season spans the water year boundary (WY months 10–12 + month 1). In the ETL,
group by `(water_year, season_id)` treating October as belonging to the *preceding* water year's
dry season (i.e., October 1922 is part of water year 1922's dry season, not 1923's).

These seasons are seeded in the `env_flow_season` lookup table (migration 24).

---

## Planned calculations

### Metric 1 — River flows (% unimpaired) — monthly

Computed for all reaches with a `unimp_sv_variable` assigned (54 of 60 channels;
MOK019 and MOK028 excluded).

```python
# Per timestep (monthly):
pct_unimpaired = (C_{reach}[t] / UNIMP_{watershed}[t]) * 100   # both in CFS

# Per water month m (1–12), across all years in period of record:
pct_unimpaired_avg[m] = mean(pct_unimpaired[t] for t where water_month[t] == m)
pct_unimpaired_cv[m]  = std(pct_unimpaired[t] for t where water_month[t] == m) / pct_unimpaired_avg[m]
# Store NULL where UNIMP_{watershed}[t] == 0 (divide-by-zero guard)
```

Output: one row per (channel_entity_id, scenario_id, water_month) →
`env_flow_channel_monthly`.

### Metric 2 — River flows (% functional flows) — seasonal

Computed for reaches with `has_eflows = true` (17 confirmed reaches; others may be added
if EFLOWS variable is discovered in SV).

```python
# Per timestep (monthly):
pct_ff[t] = (C_{reach}[t] / EFLOWS_{reach}[t]) * 100   # both in CFS
# Set NULL when EFLOWS_{reach}[t] == 0 (no target prescribed in this scenario)

# Per CEFF season s, per water year y:
season_months = [months belonging to season s]
pct_ff_seasonal[s, y] = mean(pct_ff[t] for t where water_month[t] in season_months
                                                 and water_year[t] == y)
deviation[s, y] = pct_ff_seasonal[s, y] - 100.0   # negative = below FF target

# Annual CV across water years:
annual_mean[y] = mean(pct_ff[t] for t in water year y)
annual_cv = std(annual_mean[y] across all y) / mean(annual_mean[y] across all y)
```

Output: one row per (channel_entity_id, scenario_id, season_id, water_year) →
`env_flow_channel_seasonal`.

### Metric 3 — Flow alteration index — period of record

Computed for all reaches with `unimp_sv_variable` assigned (same 54 channels as Metric 1).

```python
# Full period of record monthly time series (1,200 months):
from scipy.stats import pearsonr
r, p_value = pearsonr(C_{reach}_series, UNIMP_{watershed}_series)
n_months = len(C_{reach}_series)

# r ≈ +1: simulated flow closely tracks natural seasonal timing
# r ≈  0: flow pattern substantially altered by reservoir operations
```

Output: one row per (channel_entity_id, scenario_id) →
`env_flow_channel_period_summary` (also includes avg_pct_unimpaired, avg_pct_ff).

---

## Database schema (migration 24)

Three statistics tables are created in migration 24:

### `env_flow_channel_monthly`
```
channel_entity_id  INTEGER FK → channel_entity
scenario_id        INTEGER FK → scenario
water_year         SMALLINT
water_month        SMALLINT (1=Oct … 12=Sep)
flow_cfs           NUMERIC(12,3)   -- raw C_{reach} value
unimp_cfs          NUMERIC(12,3)   -- raw UNIMP_{watershed} value
pct_unimpaired     NUMERIC(8,3)    -- NULL when unimp_cfs = 0
PRIMARY KEY (channel_entity_id, scenario_id, water_year, water_month)
```

### `env_flow_channel_seasonal`
```
channel_entity_id  INTEGER FK → channel_entity
scenario_id        INTEGER FK → scenario
season_id          INTEGER FK → env_flow_season
water_year         SMALLINT
pct_ff_avg         NUMERIC(8,3)    -- mean C/EFLOWS × 100 within season-year
deviation          NUMERIC(8,3)    -- pct_ff_avg − 100.0 (negative = below target)
PRIMARY KEY (channel_entity_id, scenario_id, season_id, water_year)
```

### `env_flow_channel_period_summary`
```
channel_entity_id      INTEGER FK → channel_entity
scenario_id            INTEGER FK → scenario
pearson_r              NUMERIC(6,4)
p_value                NUMERIC(8,6)
n_months               SMALLINT
avg_pct_unimpaired     NUMERIC(8,3)   -- mean of monthly pct_unimpaired over full record
avg_pct_ff             NUMERIC(8,3)   -- mean of monthly pct_ff over full record (NULL if no EFLOWS)
annual_cv_pct_unimpaired NUMERIC(8,4)
PRIMARY KEY (channel_entity_id, scenario_id)
```

### `env_flow_season` (lookup)
```
id          SERIAL PRIMARY KEY
short_code  VARCHAR UNIQUE   -- 'wet_peak', 'wet_base', 'spring_recession', 'dry', 'fall_pulse'
name        VARCHAR          -- 'Wet Season Peak', etc.
description TEXT
wy_months   INTEGER[]        -- water year month numbers belonging to this season
sort_order  SMALLINT
```

---

## Implementation sequence

1. **Migration 24** — create `env_flow_season`, `env_flow_channel_monthly`,
   `env_flow_channel_seasonal`, `env_flow_channel_period_summary`
2. **Seed data** — populate `env_flow_season` (5 rows)
3. **ETL script** — `calculate_env_flow_statistics.py` (following the refuge ETL pattern)
4. **Register** in `etl/statistics/run_all.py`
5. **API endpoints** — expose monthly, seasonal, and period-summary data

---

## Resolved questions

| # | Question | Resolution |
|---|----------|------------|
| 1 | MOK028 → UNIMP mapping | No `UNIMP_MOK` in CalSim SV. `unimp_sv_variable = NULL` for MOK019 and MOK028. Metrics 1 and 3 skipped for Mokelumne reaches. Metric 2 (EFLOWS) still computed for MOK028. |
| 2 | SAC mainstem UNIMP sub-reach mapping | Split at Bend Bridge (rm 257). Above rm 257 → `UNIMP_SHAS` (SAC_UPPER watershed). At/below rm 257 → `UNIMP_SRBB` (SAC_LOWER watershed). Implemented in migration 23. |
| 3 | Season definitions | **CEFF 5-season calendar** confirmed. Seed data in `env_flow_season`. |
| 4 | TR CSV staging | Not required. All variables are in the standard `_coeqwal_calsim_output.csv` and `_coeqwal_sv_input.csv` files already staged in S3. |
| 5 | `UNIMP_*_UHH` variants | Excluded. `_UHH` suffix = "upper-half hydrology" alternative baseline. Always use base `UNIMP_*` names. |

## Remaining open question

| # | Question |
|---|----------|
| 1 | **`C_SAC000_MIF` absence** — SAC000 has no MIF in the DV. For Metric 1 (% unimpaired), SAC000 is computed normally using `UNIMP_SRBB`. For Metric 2, `EFLOWS_SAC000` is in the SV and can serve as the denominator. For MIF-based analysis, SAC000 is excluded (`has_mif = false`). No further action required unless the modeling team adds `C_SAC000_MIF` in a future SV version. |
