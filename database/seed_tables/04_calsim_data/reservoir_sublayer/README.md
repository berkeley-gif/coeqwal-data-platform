# Reservoir Sublayer

Three-tier structure within the ENTITY layer for reservoir data.

## Architecture

```
reservoir_entity (92 reservoirs)
    │  Physical attributes: capacity_taf, dead_pool_taf, location
    │
    ├── reservoir_variable (~466 variables)
    │   │  Links CalSim variables to entities
    │   │
    │   ├── storage (S_*)              -- ~100 vars
    │   ├── storage_level (S_*LEVEL*)  -- ~90 vars
    │   ├── release_total (C_*)        -- 92 vars
    │   ├── release_normal (C_*_NCF)   -- 92 vars
    │   └── release_flood (C_*_FLOOD)  -- 92 vars
    │
    ├── reservoir_group / reservoir_group_member
    │   │  Logical groupings (major, CVP, SWP, tier)
    │
    └── Statistics Tables (3 normalized)
        ├── reservoir_storage_monthly   -- Monthly storage statistics
        ├── reservoir_spill_monthly     -- Monthly spill statistics
        └── reservoir_period_summary    -- Period-of-record spill summary
```

## Variable Types

| Type | CalSim Pattern | Units | Description |
|------|----------------|-------|-------------|
| storage | S_{code} | TAF | Reservoir storage volume |
| storage_level | S_{code}LEVEL* | TAF | Storage zone decision variables |
| release_total | C_{code} | CFS | Total dam release |
| release_normal | C_{code}_NCF | CFS | Normal controlled release (≤ capacity) |
| release_flood | C_{code}_FLOOD | CFS | Flood spill (> release capacity) |

## CalSim Release Logic

From `constraints-FloodSpill.wresl`:

```
C_{res}_NCF + C_{res}_FLOOD = C_{res}
│           │               │
│           │               └── Total release from reservoir
│           └── Flood spill ABOVE release capacity (kind='SPILL')
└── Normal Channel Flow (≤ release capacity)
```

Key points:
- Normal release is constrained by release capacity (RelCap), which is a function of storage level
- Flood spill is penalized heavily in optimization (-900000 weight)
- Spill frequency = count of water years where C_*_FLOOD > 0 at any point

## Statistics Tables

### reservoir_storage_monthly

Monthly storage statistics for all 12 water months (8,832 rows per 8 scenarios = 70,656 total).

| Column | Description |
|--------|-------------|
| storage_avg_taf | Mean storage (TAF) |
| storage_cv | Coefficient of variation |
| storage_pct_capacity | Mean as % of capacity |
| q0 - q100 | Percentile bands (% capacity) |
| capacity_taf | Denormalized for convenience |
| sample_count | Number of months in sample |

Use cases:
- Monthly storage exceedance curves
- Seasonal storage patterns
- Scenario comparison of storage behavior

### reservoir_spill_monthly

Monthly spill (flood release) statistics for all 12 water months (8,832 rows per 8 scenarios).

| Column | Description |
|--------|-------------|
| spill_months_count | Count of months with spill > 0 |
| total_months | Total months in sample |
| spill_frequency_pct | % of months with spill |
| spill_avg_cfs | Mean spill when spilling |
| spill_max_cfs | Max spill this month |
| spill_q50, q90, q100 | Spill percentiles (CFS) |
| storage_at_spill_avg_pct | Avg storage % when spill occurs |

Use cases:
- Identify high-spill months (typically winter/spring)
- Storage at spill threshold indicates when reservoirs typically spill
- Percentile bands for spill magnitude when spilling

### reservoir_period_summary

Period-of-record summary (736 rows = 92 × 8 scenarios).

**Storage Exceedance (for full-period exceedance curves):**
| Column | Description |
|--------|-------------|
| storage_exc_p5 | Storage (% capacity) exceeded 95% of the time |
| storage_exc_p10 | Storage exceeded 90% of the time |
| storage_exc_p25 | Storage exceeded 75% of the time |
| storage_exc_p50 | Storage exceeded 50% of the time (median) |
| storage_exc_p75 | Storage exceeded 25% of the time |
| storage_exc_p90 | Storage exceeded 10% of the time |
| storage_exc_p95 | Storage exceeded 5% of the time |

**Threshold Markers (for horizontal lines on charts):**
| Column | Description |
|--------|-------------|
| dead_pool_taf | Dead pool volume (from reservoir_entity) |
| dead_pool_pct | Dead pool as % of capacity |
| spill_threshold_pct | Avg storage % when spill begins |

**Spill Metrics:**
| Column | Description |
|--------|-------------|
| simulation_start_year | First water year |
| simulation_end_year | Last water year |
| total_years | Number of simulation years |
| spill_years_count | Years with any spill |
| spill_frequency_pct | % of years with spill |
| spill_mean_cfs | Mean magnitude when spilling |
| spill_peak_cfs | Maximum spill observed |
| annual_spill_avg_taf | Mean annual spill volume |
| annual_spill_cv | CV of annual spill |
| annual_spill_max_taf | Max annual spill volume |
| annual_max_spill_q50/q90/q100 | Distribution of annual worst spills |

**Chart Example with Thresholds:**
```
100% ─┬────────────────── Capacity
      │    ╱╲
  90% │   ╱  ╲   ← spill_threshold_pct
      │  ╱    ╲
  50% │ ╱      ╲  ← Percentile bands
      │╱        ╲
  10% ├──────────── dead_pool_pct
      │
   0% ─┴──────────────────
       Oct  Jan  Apr  Jul
```

Use cases:
- Exceedance curves: storage_exc_* enables full period storage duration curves
- Chart thresholds: dead_pool_pct and spill_threshold_pct for visual markers
- Spill risk assessment: spill_frequency_pct is probability of annual spill
- Infrastructure planning: annual_max_spill_q90 is 90th percentile worst case
- Climate comparison: compare spill patterns across scenarios
- Volume impacts: annual_spill_avg_taf quantifies water "lost" to spill

## Related Files

| File | Description |
|------|-------------|
| `reservoir_entity.csv` | 92 reservoirs with physical attributes |
| `reservoir_variable.csv` | ~466 CalSim variables linked to reservoirs |
| `reservoir_group.csv` | Group definitions (major, CVP, SWP, tier) |
| `reservoir_group_member.csv` | Reservoir-to-group memberships |

## SQL Scripts

| Script | Description |
|--------|-------------|
| `01_create_reservoir_entity_tables.sql` | Creates reservoir_entity, reservoir_group, reservoir_group_member |
| `02_load_reservoir_entity_from_s3.sql` | Loads seed data from S3 |
| `03_create_reservoir_percentile_table.sql` | Creates reservoir_monthly_percentile (legacy) |
| `04_create_reservoir_storage_monthly.sql` | Creates reservoir_storage_monthly |
| `05_create_reservoir_spill_monthly.sql` | Creates reservoir_spill_monthly |
| `06_create_reservoir_period_summary.sql` | Creates reservoir_period_summary |

## ETL Scripts

| Script | Description |
|--------|-------------|
| `calculate_reservoir_percentiles.py` | Legacy ETL for percentile table (8 major reservoirs) |
| `calculate_reservoir_statistics.py` | Comprehensive ETL for all 92 reservoirs |
| `generate_release_variables.py` | Generates release variable rows for reservoir_variable.csv |

## Verification Queries

```sql
-- Check variable type distribution
SELECT variable_type, COUNT(*)
FROM reservoir_variable
GROUP BY variable_type
ORDER BY variable_type;

-- Expected:
-- release_flood          92
-- release_normal         92
-- release_total          92
-- storage               ~100
-- storage_level         ~90

-- Check monthly statistics row counts
SELECT scenario_short_code, COUNT(*)
FROM reservoir_storage_monthly
GROUP BY scenario_short_code;
-- Expected: 1,104 rows per scenario (92 × 12)

-- Monthly spill patterns (higher in wet months)
SELECT
    water_month,
    AVG(spill_frequency_pct) as avg_spill_freq,
    AVG(spill_avg_cfs) as avg_spill_mag
FROM reservoir_spill_monthly
WHERE scenario_short_code = 's0020'
  AND reservoir_code IN ('S_SHSTA', 'S_OROVL', 'S_FOLSM')
GROUP BY water_month
ORDER BY water_month;

-- Top spill-prone reservoirs
SELECT
    reservoir_code,
    spill_frequency_pct,
    spill_peak_cfs,
    annual_spill_avg_taf
FROM reservoir_period_summary
WHERE scenario_short_code = 's0020'
ORDER BY spill_frequency_pct DESC
LIMIT 10;

-- Compare spill risk across scenarios
SELECT
    scenario_short_code,
    AVG(spill_frequency_pct) as avg_spill_freq,
    AVG(annual_spill_avg_taf) as avg_annual_spill
FROM reservoir_period_summary
GROUP BY scenario_short_code
ORDER BY avg_spill_freq DESC;
```
