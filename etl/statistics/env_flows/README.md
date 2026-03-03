# ETL statistics — Environmental river flows

> **Status: IMPLEMENTED**
>
> All migrations (23–27) applied. ETL complete for all 19 scenarios. Data loaded into
> `env_flow_channel_monthly`, `env_flow_channel_seasonal`, and `env_flow_channel_period_summary`.
> API endpoints are the next step.

---

## Overview

Three metrics are computed for **59 river channel reaches** in the CalSim DV output,
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

**59 channels** are currently attributed in `channel_entity` with `channel_class IS NOT NULL`
and are included in the ETL. The original planning estimate was "60 channels," and it is
possible one channel with `Part C = CHANNEL` was missed during the attribution step.

> **Open question:** Run the command below from Cloud9 to get the true count directly from
> the DV header, independent of `channel_entity.csv`:
>
> ```bash
> python - <<'EOF'
> import boto3, csv, io
> s3 = boto3.client('s3')
> obj = s3.get_object(Bucket='coeqwal-model-run',
>     Key='scenario/s0020/csv/s0020_coeqwal_calsim_output.csv')
> first_mb = obj['Body'].read(200_000).decode('latin-1')
> lines = first_mb.splitlines()
> # Find Part C row (typically row index 2 or 3)
> for i, line in enumerate(lines[:10]):
>     print(i, line[:120])
> EOF
> ```
> Then count the `CHANNEL` occurrences in the Part C row. If the count is 60, one channel
> is missing from `channel_entity.csv` and must be identified and added.

Channels are attributed in `channel_entity` (migration 23) with `watershed_short_code`,
`unimp_sv_variable`, `has_mif`, `has_eflows`, and `channel_class`.

Current class breakdown: 47 stream reaches, 7 reservoir releases, 5 conveyance canals (= 59 total).

---

## Channel subsets used in the data explorer

The frontend data explorer exposes four channel filter options, each corresponding to a
meaningful analytical subset. The `channel_entity` table flags drives these filters.

| Filter | Column flag | Count | Description |
|--------|------------|-------|-------------|
| **Stream reaches** | `channel_class = 'stream'` | 47 | All natural river channels; excludes reservoir releases (below-dam flows) and conveyance canals |
| **EFLOWS streams** | `has_eflows = true` | 17 | Streams with a prescribed functional flow (`EFLOWS_{reach}`) target in the SV input — the reach set used for tier results and CEFF analysis (Metric 2) |
| **MIF streams** | `has_mif = true` | 20 | Streams with a binding minimum instream flow companion variable (`C_{reach}_MIF`) in the DV output — the primary regulatory monitoring locations |
| **All channels** | (no filter) | 59 | Complete CalSim reach set including reservoir releases (e.g. below Shasta, Oroville, Folsom) and conveyance canals (e.g. Delta Cross Channel, Clifton Court Forebay) |

### EFLOWS streams (17) — tier analysis reaches

These 17 reaches have `has_eflows = true` in `channel_entity` and represent the CEFF
functional-flow monitoring network. They are used as the denominator set for tier scoring
and the basis for Metric 2 (% functional flows) and Metric 3 (flow alteration index).

| Reach | Location | Watershed | `UNIMP_*` | MIF? |
|-------|----------|-----------|-----------|------|
| `AMR004` | American River at I-80 Bridge | UPPER_AMERICAN | `UNIMP_FOLS` | ✓ |
| `FTR003` | Feather River | UPPER_FEATHER | `UNIMP_OROV` | ✓ |
| `FTR029` | Feather River at Yuba City | UPPER_FEATHER | `UNIMP_OROV` | ✓ |
| `MCD005` | Merced River at Stevinson | UPPER_MERCED | `UNIMP_ME` | ✓ |
| `MOK028` | Mokelumne River at Woodbridge | UPPER_MOKELUMNE | — | ✓ |
| `SAC000` | Sacramento River at Chipps Island | SAC_LOWER | `UNIMP_SRBB` | — |
| `SAC049` | Sacramento River at Freeport | SAC_LOWER | `UNIMP_SRBB` | ✓ |
| `SAC122` | Sacramento River at Tisdale Weir | SAC_LOWER | `UNIMP_SRBB` | ✓ |
| `SAC148` | Sacramento River at Colusa Weir | SAC_LOWER | `UNIMP_SRBB` | ✓ |
| `SAC257` | Sacramento River at Bend Bridge | SAC_LOWER | `UNIMP_SRBB` | ✓ |
| `SAC289` | Sacramento River at South Bonnieville | SAC_UPPER | `UNIMP_SHAS` | ✓ |
| `SJR070` | San Joaquin near Vernalis | SAN_JOAQUIN | `UNIMP_SJ` | ✓ |
| `SJR127` | San Joaquin at Salt Slough | SAN_JOAQUIN | `UNIMP_SJ` | ✓ |
| `STS011` | Stanislaus River | UPPER_STANISLAUS | `UNIMP_ST` | ✓ |
| `TRN111` | Trinity River at Lewiston | TRINITY_RIVER | `UNIMP_TRIN` | ✓ |
| `TUO003` | Tuolumne River | UPPER_TUOLUMNE | `UNIMP_TU` | ✓ |
| `YUB002` | Yuba River at Marysville | YUBA_RIVER | `UNIMP_YUBA` | ✓ |

> **Note:** `SAC000` (Chipps Island) has `has_eflows = true` but **no MIF** (`has_mif = false`)
> because `C_SAC000_MIF` is absent from the DV. It does have `EFLOWS_SAC000` in the SV.
> The MIF subset (20 reaches) includes all EFLOWS reaches except SAC000 plus 3 additional
> non-EFLOWS streams: `FTR059`, `KSWCK`, `NTOMA`, `STS059`.

---

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
network_arc_id     VARCHAR(30)      -- References channel_entity.network_arc_id
scenario_short_code VARCHAR(20)
season_id          INTEGER FK → env_flow_season

-- Raw flow volume (CFS) — all 60 channels
flow_avg_cfs       NUMERIC(12,3)   -- mean of per-year seasonal mean flows
flow_cv            NUMERIC(8,4)
flow_q0 … flow_q100               -- percentile distribution across years
flow_exc_p5 … flow_exc_p95

-- Natural flow reference + % unimpaired (Metric 1, seasonal) — 58 channels
unimp_avg_cfs      NUMERIC(12,3)   -- mean of UNIMP seasonal averages (natural reference)
pct_unimpaired_avg NUMERIC(8,3)    -- mean (C/UNIMP × 100) across years
pct_unimpaired_cv  NUMERIC(8,4)
unimp_q0 … unimp_q100             -- percentile distribution of pct_unimpaired
unimp_exc_p5 … unimp_exc_p95

-- % Functional flows (Metric 2) — ~17 EFLOWS channels
pct_ff_avg         NUMERIC(8,3)    -- mean C/EFLOWS × 100 within season-year, across years
pct_ff_cv          NUMERIC(8,4)
deviation_avg      NUMERIC(8,3)    -- pct_ff_avg − 100.0 (negative = below target)
q0 … q100                         -- percentile distribution of pct_ff
exc_p5 … exc_p95
target_met_pct     NUMERIC(6,2)    -- % of years where seasonal pct_ff >= 100%

UNIQUE (network_arc_id, scenario_short_code, season_id)
```
Coverage: all 60 channels (flow_* columns); 58 with UNIMP (pct_unimpaired_*); ~17 with EFLOWS (pct_ff_*). Extended by migration 26.

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

## ETL run results (all 19 scenarios)

ETL executed via `python etl/statistics/run_all.py --module env_flows` on all 19 scenarios.
All rows inserted successfully into the three statistics tables.

| Table | Rows inserted |
|-------|--------------|
| `env_flow_channel_monthly` | 19 scenarios × 59 channels × 12 months = ~13,452 |
| `env_flow_channel_seasonal` | 19 scenarios × 59 channels × 5 seasons = ~5,605 |
| `env_flow_channel_period_summary` | 19 scenarios × 59 channels = ~1,121 |

---

## Data quality notes by scenario

### MIF variable availability (20 expected)

Not all scenarios model the same minimum instream flow (MIF) constraints. The following
MIF variables are absent from some scenarios' DV output — this is expected and reflects
different regulatory frameworks being tested, not a data pipeline error.

| Scenario(s) | MIF present / 20 | Missing variables |
|------------|-----------------|-------------------|
| s0020, s0021, s0025–s0028, s0029, s0030, s0031–s0033, s0044 | **20 / 20** | — |
| s0039–s0042 | **7 / 20** | `C_FTR029_MIF`, `C_MCD005_MIF`, `C_MOK028_MIF`, `C_SAC049_MIF`, `C_SAC122_MIF`, `C_SAC148_MIF`, `C_SAC289_MIF`, `C_SJR070_MIF`, `C_SJR127_MIF`, `C_STS011_MIF`, `C_TRN111_MIF`, `C_TUO003_MIF`, `C_YUB002_MIF` |
| s0011 | **8 / 20** | Same 13 as s0039–s0042 except `C_STS011_MIF` is present; missing: `C_FTR029_MIF`, `C_MCD005_MIF`, `C_MOK028_MIF`, `C_SAC049_MIF`, `C_SAC122_MIF`, `C_SAC148_MIF`, `C_SAC289_MIF`, `C_SJR070_MIF`, `C_SJR127_MIF`, `C_TRN111_MIF`, `C_TUO003_MIF`, `C_YUB002_MIF` |
| s0023, s0024 | **6 / 20** | Same 12 as s0011, plus `C_SAC257_MIF` and `C_STS011_MIF` |

All 59 channel flow variables (`C_{reach}`) are present in every scenario — **no channel
flow data is missing from any scenario.**

The ETL handles absent MIF variables gracefully: `pct_mif_*` columns are NULL for those
reaches in those scenarios.

### EFLOWS (functional flow targets) variable availability (17 expected)

EFLOWS targets are SV inputs — they are prescribed constraints, not DV outputs.

| Scenario(s) | SV columns | EFLOWS present | Notes |
|------------|------------|----------------|-------|
| s0020, s0021, s0023–s0028, s0031–s0033, s0039–s0042, s0044 | 28 | All 17 | Full EFLOWS suite |
| s0011 | 12 | **None** | Pre-EFLOWS baseline — no functional flow targets prescribed |
| **s0029, s0030** | **12** | **1 of 17 — only `EFLOWS_STS011`** | Unexpected — see open question below |

**s0029/s0030 detail (confirmed via `diagnose_dv_columns.py`):** Both scenarios have all 79 DV
channel-flow variables (including all 20 MIF), so their channel flow data is complete. However,
their SV contains only 12 columns: 11 `UNIMP_*` unimpaired flow variables + `EFLOWS_STS011`.
The other 16 EFLOWS variables (`EFLOWS_AMR004`, `EFLOWS_FTR003`, `EFLOWS_FTR029`, `EFLOWS_MCD005`,
`EFLOWS_MOK028`, `EFLOWS_SAC000`, `EFLOWS_SAC049`, `EFLOWS_SAC122`, `EFLOWS_SAC148`,
`EFLOWS_SAC257`, `EFLOWS_SAC289`, `EFLOWS_SJR070`, `EFLOWS_SJR127`, `EFLOWS_TRN111`,
`EFLOWS_TUO003`, `EFLOWS_YUB002`) are absent. Functional flow statistics (`pct_ff_*`) are
NULL for all those 16 reaches in s0029 and s0030 in the database.

### Percentage overflow

Some scenarios produce `pct_unimpaired` values far exceeding 100% for heavily regulated
reaches (e.g., canal diversions near zero natural flow). Values up to ~100,000% are
possible and physically valid. Migration 27 widened affected columns from `NUMERIC(8,3)`
to `NUMERIC(12,3)` to accommodate these.

---

## Resolved questions

| # | Question | Resolution |
|---|----------|------------|
| 1 | MOK028 → UNIMP mapping | No `UNIMP_MOK` in CalSim SV. `unimp_sv_variable = NULL` for MOK019 and MOK028. Metrics 1 and 3 skipped for Mokelumne reaches. Metric 2 (EFLOWS) still computed for MOK028. |
| 2 | SAC mainstem UNIMP sub-reach mapping | Split at Bend Bridge (rm 257). Above rm 257 → `UNIMP_SHAS` (SAC_UPPER watershed). At/below rm 257 → `UNIMP_SRBB` (SAC_LOWER watershed). Implemented in migration 23. |
| 3 | Season definitions | **CEFF 5-season calendar** confirmed. Seed data in `env_flow_season`. |
| 4 | TR CSV staging | Not required. All variables are in the standard `_coeqwal_calsim_output.csv` and `_coeqwal_sv_input.csv` files already staged in S3. |
| 5 | `UNIMP_*_UHH` variants | Excluded. `_UHH` suffix = "upper-half hydrology" alternative baseline. Always use base `UNIMP_*` names. |
| 6 | Channel count discrepancy | Planning estimate was "60 channels." `channel_entity.csv` has 59 rows with `channel_class` set. Whether the DV truly contains 60 or 59 distinct `CHANNEL` variables has **not been independently verified** — see open question 2. |
| 7 | `C_SAC000_MIF` absence | SAC000 has no MIF in the DV (`has_mif = false`). Metric 1 computed normally using `UNIMP_SRBB`. Metric 2 uses `EFLOWS_SAC000` from SV. No action required unless modeling team adds this variable in a future SV version. |
| 8 | MIF variable absence in some scenarios | Confirmed expected: different scenarios model different regulatory frameworks. The absent variables reflect a policy choice in those scenario configs, not a data pipeline error (verified via `diagnose_dv_columns.py`). |

## Open questions

| # | Question | Priority |
|---|----------|----------|
| 1 | **s0029/s0030 missing 16 of 17 EFLOWS targets.** Both scenarios have complete DV (all 59 channel flows + 20 MIF) but their SV contains only `EFLOWS_STS011` and no other functional flow targets. If these scenarios were intended to include an EFLOWS regulatory framework, the `pct_ff_*` columns in the database are NULL for 16 of 17 EFLOWS reaches in those scenarios. **Verify with the modeling team** whether this is intentional (e.g., s0029/s0030 test Stanislaus-only EFLOWS) or a missing SV file. | High |
| 2 | **59 vs 60 channels.** The diagnose script confirms all 79 expected DV variables are present (59 channels + 20 MIF), but its target list is built from `channel_entity.csv` — it cannot detect a channel that was never attributed. **Verify the true count** by reading the raw DV header and counting `CHANNEL` occurrences in the Part C row (see script in "Reach inventory" above). If the count is 60, identify the missing channel and add it to `channel_entity.csv`. | Medium |
