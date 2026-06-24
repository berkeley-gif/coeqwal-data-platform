# ETL Statistics Pipeline

Calculate and load CalSim model output statistics into the COEQWAL PostgreSQL database.

> **Status:** The pipeline has been stood up. Eight production modules run end to end through [`run_all.py`](run_all.py) and load statistics into PostgreSQL, and the experimental `sensitivity/` module runs separately. Next steps are to work with the Water Allocation Modeling Team to harden the variable lists and calculations, and to fill in the remaining requested statistics. See [Statistics roadmap](#statistics-roadmap).

> **Provenance:** The location lists and the calculations in this pipeline were ported, as a first pass, from the Water Allocation Modeling Team's Jupyter notebooks: the [`coeqwal`](https://github.com/maramahmedd/coeqwal) repo (`notebooks/coeqwalpackage/metrics.py`, `notebooks/Metrics.ipynb`) and the COEQWAL_V3 toolkit (`coeqwalpackage/metrics.py`, `tier.py`, `DataExtraction.py`, and the `data/` mapping CSVs). The priority was to stand up the pipeline, with a next step of carefully reviewing and hardening the location lists and calculations. Treat them as a working draft that needs review, not a verified source of truth.

## Contents

- Orientation
  - [Overview](#overview)
  - [Directory structure](#directory-structure)
  - [Scripts](#scripts)
  - [Modules at a glance](#modules-at-a-glance)
  - [Statistics we compute](#statistics-we-compute)
  - [Reading the outputs (percentile bands and exceedance)](#reading-the-outputs-percentile-bands-and-exceedance)
- Reference
  - [Data sources by module](#data-sources-by-module)
  - [Unit conversion: CFS to TAF](#unit-conversion-cfs-to-taf)
  - [CSV input format](#csv-input-format)
  - [Data integrity safeguards](#data-integrity-safeguards)
- Operations
  - [Statistics ETL operations](#statistics-etl-operations)
  - [Manual and development runs](#manual-and-development-runs)
- Methodology and modules
  - [Reservoir calculation methodology](#reservoir-calculation-methodology)
  - [Urban demand unit statistics](#urban-demand-unit-statistics)
  - [M&I contractor statistics](#mi-contractor-statistics)
  - [Agricultural demand unit statistics](#agricultural-demand-unit-statistics)
  - [Wildlife refuge statistics](#wildlife-refuge-statistics)
  - [Environmental river flow statistics](#environmental-river-flow-statistics)
  - [Modules not yet documented](#modules-not-yet-documented)
- Provenance and audit
  - [Provenance and verification](#provenance-and-verification)
  - [Appendix: Variable reference](#appendix-variable-reference-wresl-verified-march-2026)
  - [Appendix: Questions about the notebook code](#appendix-questions-about-the-notebook-code)
- Roadmap
  - [Statistics roadmap](#statistics-roadmap)

## Overview

This pipeline processes CalSim model output CSVs stored in S3 to calculate statistics across all modules. The calculated metrics are loaded directly into PostgreSQL for the COEQWAL website API.

**Data flow:**
```
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│  S3 bucket       │──▶│  run_all.py      │──▶│  PostgreSQL      │──▶│  API endpoints   │
│  CalSim CSVs     │   │  on Cloud9 EC2   │   │  database        │   │  /api/statistics │
└──────────────────┘   └──────────────────┘   └──────────────────┘   └──────────────────┘
                              │
                              │  manually triggered, see "Running the statistics ETL"
                              │  writes via psycopg2
```

## Directory structure

```
etl/statistics/
├── README.md                          # This file
├── run_all.py                         # Orchestrator: run all modules for one or more scenarios
├── units.py                           # Shared constants (CFS-to-TAF factor, CV guards)
├── verify_all_sections.py             # Recomputes every section and checks it against the database
├── verify_api.py                      # Check the public API against the database
├── scan_dupes.py                      # Scan CSVs for duplicate variable columns
├── generate_release_variables.py      # Build the release variable manifest
├── visualize_percentile_bands.py      # Render reservoir percentile-band charts
├── dev_run.sh                         # Local development runner
├── test_local.py                      # Quick local sanity check
├── requirements.txt
├── lib/                               # Shared module-runner framework
│   ├── runner.py                        # Dispatches each module's run() contract
│   ├── commands.py
│   ├── config.py
│   └── protocol.py
├── charts/                            # Generated chart output (PNG, PDF)
├── audit_reports/                     # Per-run scorecards (gitignored)
├── reservoirs/                        # Reservoir storage, percentiles, spill
│   ├── main.py
│   ├── module.py                        # Adapter exposing run() to lib/runner
│   ├── calculate_reservoir_statistics.py
│   ├── calculate_reservoir_percentiles.py
│   └── reservoir_metrics.py             # Core calculation functions
├── du_urban/                          # Urban demand-unit delivery, shortage
│   ├── main.py
│   └── calculate_du_statistics_v2.py
├── mi/                                # M&I contractor delivery, shortage
│   ├── main.py
│   └── calculate_mi_statistics.py
├── cws_aggregate/                     # CWS project / region rollups
│   ├── main.py
│   └── calculate_cws_aggregate_statistics.py
├── ag/                                # Agricultural demand, delivery, pumping, shortage
│   ├── main.py
│   └── calculate_ag_statistics.py
├── refuge/                            # Wildlife refuge delivery, shortage
│   ├── main.py
│   └── calculate_refuge_statistics.py
├── env_flows/                         # River flow metrics, functional flows
│   ├── main.py
│   └── calculate_env_flow_statistics.py
├── delta/                             # Net Delta Outflow, X2, salinity
│   ├── main.py
│   └── calculate_delta_statistics.py
└── sensitivity/                       # Climate / operational sensitivity (experimental)
    └── calculate_sensitivity.py
```

Most modules are just a `main.py` plus a `calculate_*.py`. `reservoirs/` was the first statistics module built, and it carries extra files for two reasons. It is the pilot for the shared in-process `lib/` runner: the runner and its `MODULE_REGISTRY` (`lib/config.py`) were set up to dispatch every module through a common `run()` contract, but reservoirs is the only one moved onto it so far, so the others still run from their own `main.py`. Its `module.py` is the thin `run()` adapter that wraps the older `main.py`. Reservoirs also emits several outputs (storage, percentile bands, spill, period summary), so its work is split across `calculate_reservoir_statistics.py` and `calculate_reservoir_percentiles.py`, with shared helpers in `reservoir_metrics.py`.

## Scripts

Top-level entry points and utilities:

| Script | Purpose |
|--------|---------|
| `run_all.py` | **Orchestrator:** Runs the eight production modules for one or more scenarios and writes a per-run scorecard. The main entry point for the daily pipeline. |
| `verify_all_sections.py` | Recomputes each section's stats and compares against the database. See [`etl/verification/README.md`](../verification/README.md). |
| `verify_api.py` | Checks public API responses against the database. |
| `scan_dupes.py` | Scans scenario CSVs for duplicate variable columns. |
| `generate_release_variables.py` | Builds the release variable manifest. |
| `visualize_percentile_bands.py` | Renders reservoir percentile-band charts into `charts/`. |
| `units.py` | Shared constants (CFS-to-TAF factor, CV guards). Imported by every module. |
| `dev_run.sh` | Local development runner for testing with CSV files. |
| `test_local.py` | Quick sanity check for individual reservoir calculations. |

Each module directory has its own `main.py` (independently runnable) and a `calculate_*.py` that does the work. For what each module computes and the tables it writes, see [Modules at a glance](#modules-at-a-glance).

---

## Modules at a glance

Nine modules live under `etl/statistics/`, one subdirectory per topic. Eight are production and run together via `run_all.py`. The ninth (`sensitivity/`) is experimental and runs separately via `--with-sensitivity`. Each production module reads scenario CSVs from S3, computes derived metrics, writes them to PostgreSQL, and has an independently runnable `main.py`. `sensitivity/` instead reads the already-written statistics from the database and runs via `calculate_sensitivity.py` (it has no `main.py`).

The **Needs hardening** column flags known correctness or integrity gaps, verified against the code in May 2026.

| Module | What it computes | Tables written | Needs hardening |
|---|---|---|---|
| [`reservoirs/`](reservoirs/) | Monthly storage, monthly storage percentiles, April and September storage averages, spill frequency | `reservoir_storage_monthly`, `reservoir_monthly_percentile`, `reservoir_spill_monthly`, `reservoir_period_summary` | Spill threshold coverage: only ~12 of 92 reservoirs have a usable flood level in the DV data, the rest report `spill_frequency_pct = 0` rather than NULL (while `flood_pool_prob_*` is NULL in the same case), conflating "no threshold" with "never spills". Spill volume columns (`spill_*_cfs`, `annual_spill_*_taf`) are always NULL. `spill_threshold_pct` is interpreted as the average storage during spill. See [reservoir spill hardening](#reservoir-spill-threshold-coverage-and-spill-volume) |
| [`du_urban/`](du_urban/) | Per-demand-unit delivery, shortage, % demand met, reliability for urban DUs | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |  |
| [`mi/`](mi/) | M&I contractor delivery, shortage, % demand met, reliability (SWP and CVP contractors, MWD, and project aggregates) | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |  |
| [`cws_aggregate/`](cws_aggregate/) | CWS project / region rollups (SWP and CVP north / south splits, plus MWD) | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |  |
| [`ag/`](ag/) | Ag demand (AW), surface-water delivery (DN), groundwater pumping (GP), shortage, reliability, regional aggregates | `ag_du_*_monthly`, `ag_du_period_summary`, `ag_aggregate_*` |  |
| [`refuge/`](refuge/) | Refuge demand-unit delivery, derived shortage, reliability | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` | Reliability is the 95th-percentile annual shortage `reliability_pct_95` only, not the mean-based `reliability_pct` other modules report. Shortage falls back to `max(AW - DN, 0)` when no model shortage variable exists |
| [`env_flows/`](env_flows/) | River flow metrics: CFS / TAF volumes, % unimpaired, % functional flows, alteration index (Pearson r), CEFF seasonal metrics | `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary` |  |
| [`delta/`](delta/) | Net Delta Outflow, X2 position (April, September, spring, fall), salinity at compliance stations, Banks / Tracy pumping plant EC | `delta_monthly`, `delta_period_summary` |  |
| [`sensitivity/`](sensitivity/) | Climate sensitivity (hist vs CC50 vs CC95) and operational sensitivity (cross-scenario spread) across a selected metric per module above. *Experimental, under development*: labeled experimental in the script header, run via `run_all.py --with-sensitivity` after the per-scenario modules complete, not as part of the per-scenario loop. | `sensitivity_climate`, `sensitivity_operational` | Experimental: no `verify_*` coverage. Covers one selected metric per module (for example reservoirs only via `storage_avg_taf`), not every metric. cc50 / cc95 hydroclimate identification falls back to scenario-order heuristics |

Utility code (not a module): `charts/` for visualization helpers, top-level `verify_*.py` / `visualize_*.py` / `scan_dupes.py` scripts for ad-hoc tasks.

> **Output files:** `run_all.py` writes a per-run scorecard to `etl/statistics/audit_reports/stats_audit_<ts>.csv`, and `scan_dupes.py` writes `etl/statistics/audit_reports/duplicate_scan_results.csv` (+ sibling `_units.csv`). The whole `audit_reports/` directory is gitignored. Override locations with `--audit-dir` or `-o`. See [`etl/README.md`](../README.md#output-files-audits-generated-sql) for the full output catalog.

## Statistics we compute

Each per-scenario module reads its variables from the scenario's CalSim CSVs in the s3 bucket. If we move to parquet to store the time series, they could read from then instead. The modules compute statistics at monthly and period-of-record granularity for each location of interest (LOI). The delivery and demand modules convert to TAF, while reservoirs report storage in TAF and percent of capacity, environmental flows report volumes in CFS and TAF alongside percentage and correlation indices, and Delta converts outflow (NDO) to TAF (keeping the raw CFS mean alongside as `avg_cfs`), reports X2 position in kilometers from the Golden Gate, and salinity (EC) in µmhos/cm. The experimental sensitivity module is the exception. It reads the already-written statistics back from the database rather than from CalSim CSVs. The derived metrics below do not appear in the raw CSV columns. They are computed in the module `calculate_*.py` scripts.

Each statistic depends on an accurate LOI list and the correct calculation. These need to be reviewd and hardened in collaboration with the Water Allocation Modeling Team.

**Conventions**

- **Water month:** Oct = 1 through Sep = 12, computed as `((calendar_month - 10) % 12) + 1`.
- **Percentile bands:** `q0`, `q10`, `q30`, `q50`, `q70`, `q90`, `q100` = `np.percentile(series, p)` for `p` in `[0, 10, 30, 50, 70, 90, 100]`.
- **Exceedance percentiles:** `exc_p5`, `exc_p10`, `exc_p25`, `exc_p50`, `exc_p75`, `exc_p90`, `exc_p95` = value exceeded p% of the time = `np.percentile(series, 100 - p)`.
- **CV:** `compute_cv(series)` = `std / |mean|`, returns 0 when `|mean| <= 0.01` TAF, capped at 99. Refuge uses a local `_safe_cv` that is close but not identical. It guards on strict `< 0.01` and returns 0 (rather than capping at 99) when the CV would exceed 99.
- **Shortage noise threshold:** 0.1 TAF (`SHORTAGE_THRESHOLD_TAF`) for monthly shortage frequency and annual shortage-year counts in all five delivery and demand modules.

Input variable types per module are summarized in [Data sources by module](#data-sources-by-module). The reservoir formulas and their notebook alignment are in [Reservoir calculation methodology](#reservoir-calculation-methodology), and per-module specifics are in each module's section below. LOI provenance and known list discrepancies are in [Location lists](#location-lists).

### Locations of interest by module

| Module | Entity key | Count | Defined in |
|--------|------------|-------|------------|
| **Reservoirs** | `reservoir_entity_id` (`short_code`) | 92 | All rows of [`reservoir_entity.csv`](../../database/seed_tables/04_calsim_data/reservoir_entity.csv), all 92 of which have `capacity_taf > 0`. The percentile table skips any row with `capacity_taf <= 0`. The other reservoir tables load every row and zero out percent-of-capacity when capacity is 0. |
| **DU Urban** | `du_id` | 90 | Active rows (`is_active = TRUE`) in the live `du_urban_variable` table. The mapping is seeded across [`01d_load_du_urban_variable.sql`](../../database/sql_archive/03_entity_layers/mi/01d_load_du_urban_variable.sql) (delivery and shortage), `01h_add_missing_du_urban_variables.sql` (adds `demand_variable`), and `01i_add_demand_mode_column.sql` (adds the `demand_mode` column). The May 2026 audit export holds 90 active rows. This is a runtime figure and is not derivable from the seed files alone. |
| **MI Contractors** | `mi_contractor_code` | 23 | `MI_CONTRACTOR_VARIABLES` in [`mi/calculate_mi_statistics.py`](mi/calculate_mi_statistics.py): 18 individual SWP contractors (MWD is one of them) plus 5 project aggregates (`SWP_PMI_TOTAL`, `SWP_PMI_N`, `SWP_PMI_S`, `CVP_PMI_N`, `CVP_PMI_S`). |
| **CWS Aggregate** | `cws_aggregate_id` | 6 | `CWS_AGGREGATES` in [`cws_aggregate/calculate_cws_aggregate_statistics.py`](cws_aggregate/calculate_cws_aggregate_statistics.py): `swp_total`, `swp_nod`, `swp_sod`, `cvp_nod`, `cvp_sod`, `mwd`. |
| **AG** | `du_id` | 144 | The per-scenario intersection of `AW_*` columns in the DV CSV with the 144 DUs in [`du_agriculture_entity.csv`](../../database/seed_tables/04_calsim_data/du_agriculture_entity.csv). 144 is the full seed size, so the runtime count can be lower. Includes 17 GW-only DUs in `GW_ONLY_DU_IDS`, all of them within the 144. |
| **AG aggregates** | `aggregate_code` | 9 | 7 direct (`AG_AGGREGATES`) + 2 computed (`AG_COMPUTED_AGGREGATES`: `nod_ag`, `sod_ag`) in [`ag/calculate_ag_statistics.py`](ag/calculate_ag_statistics.py). |
| **Refuge** | `du_id` | 18 | `REFUGE_DU_IDS` in [`refuge/calculate_refuge_statistics.py`](refuge/calculate_refuge_statistics.py), validated against [`du_refuge_entity.csv`](../../database/seed_tables/04_calsim_data/du_refuge_entity.csv). |
| **Env Flows** | `network_arc_id` | 59 | Rows in [`channel_entity.csv`](../../database/seed_tables/04_calsim_data/channel_entity.csv) with non-empty `channel_class` and `network_arc_id`. 51 with UNIMP mapping, 17 with EFLOWS targets, 20 with MIF. |
| **Delta** | `variable_code` | 8 | `DELTA_VARIABLES` in [`delta/calculate_delta_statistics.py`](delta/calculate_delta_statistics.py): `ndo`, `x2`, four compliance EC stations, two pumping-plant EC stations. |
| **Sensitivity** *(experimental)* | per source module | all entities above | Reads pre-computed monthly statistics from the database across scenarios. See [`sensitivity/calculate_sensitivity.py`](sensitivity/calculate_sensitivity.py). |

### Where variable lists live

A module's variable list is the set of CalSim B-part pathnames it pulls from the CSVs, together with a per-entity mapping that says which pathname plays which role for each location of interest. The roles differ by module: delivery, demand, and shortage for the five delivery/demand modules (DU Urban, MI, CWS aggregate, AG, Refuge). Storage plus flood and dead-pool thresholds for reservoirs. Simulated flow, unimpaired reference, functional-flow target, and MIF for env flows. NDO, X2, and salinity indicators for Delta. See the table below for where each mapping lives.

For delivery/demand modules, demand is not always a single pathname (MI uses `PERDV_*` fractions or a fixed Table A constant, CWS uses delivery plus shortage). Shortage may be a model variable (`SHRTG_*`, `GW_SHORT_*`), several pathnames summed, or derived as `max(demand - delivery, 0)` when no model variable exists. Delivery can also be several pathnames summed per entity (MI contractors with multiple arcs).

Today these mappings live in three different places for historical development reasons. The long-term direction is to harden the lists in collaboration with the WAM team, and migrate each hardened mapping to a SQL table, the way `du_urban_variable` already works, so the lists are queryable and versioned with the rest of the schema instead of being buried in Python or seed CSVs. See [Migrate module variable lists into SQL tables](#migrate-module-variable-lists-into-sql-tables) on the roadmap.

- **SQL table:** DU Urban is the only module migrated so far. It reads its delivery, demand, shortage, and demand-mode mapping per `du_id` from the `du_urban_variable` table, and its delivery arcs from `du_urban_delivery_arc`, both filtered to `is_active = TRUE`.
- **Seed CSV:** Env Flows reads its per-channel mapping (`unimp_sv_variable` plus the `has_eflows` and `has_mif` flags) from [`channel_entity.csv`](../../database/seed_tables/04_calsim_data/channel_entity.csv). Reservoirs, AG, and Refuge read their entity lists from seed CSVs but still hardcode or derive the variable names elsewhere (see the next two items). For reservoirs the per-reservoir `capacity_taf` and `dead_pool_taf` come from `reservoir_entity.csv`, with capacity then overridden by the `CAPACITY_OVERRIDES` constant and dead pool used only as a fallback when `RESERVOIR_THRESHOLDS` has no `dead_var` for that reservoir.
- **Python constant:** MI (`MI_CONTRACTOR_VARIABLES`), CWS Aggregate (`CWS_AGGREGATES`), Delta (`DELTA_VARIABLES`), the reservoir flood and dead-pool mapping (`RESERVOIR_THRESHOLDS`), the AG aggregates (`AG_AGGREGATES`, `AG_COMPUTED_AGGREGATES`), the AG groundwater-only set (`GW_ONLY_DU_IDS`), and the refuge entity list (`REFUGE_DU_IDS`) are dicts or sets hardcoded in Python. Each lives in its module's `calculate_*.py`, except `RESERVOIR_THRESHOLDS`, which is defined in `reservoir_metrics.py`.
- **Naming convention:** AG builds most per-DU pathnames at runtime by prefix: `AW_{du_id}` (demand/applied water), `DN_{du_id}` (surface-water delivery), and `GP_{du_id}` (groundwater pumping). When `GP_{du_id}` is absent, pumping is derived as `AW - DN`, or as `AW` alone when `DN_{du_id}` is also absent. Shortage is `SHRTG_{du_id}` when the DU's `wba_id` is in `SACRAMENTO_WBAS` (an explicit list of Sacramento WBA codes, 02 through 26 with N and S subdivisions), else `GW_SHORT_{du_id}`. GW-only DUs in `GW_ONLY_DU_IDS` synthesize delivery as `GP_{du_id} + RU_{du_id}` (with `RU` treated as 0 when absent) when `DN_*` is absent. AG has no derived shortage fallback in the code: if the shortage column is missing, AG omits only the shortage metric for that DU (no monthly shortage rows, and the period summary leaves out the shortage fields) while still computing its demand, delivery, and groundwater-pumping statistics. Project aggregates use explicit `DEL_*` / `SHORT_*` names, not prefixes. Refuge uses `AW_{du_id}` and `DN_{du_id}` but not GP. Shortage is `SHRTG_{du_id}` when the WBA prefix from `du_id` is in `SACRAMENTO_REFUGE_WBAS` (`08N`, `08S`, `09`, `11`, `17N`, `17S`), else `GW_SHORT_{du_id}`, with a derived fallback of `max(AW - DN, 0)` when the model variable is missing. Reservoirs use `S_{code}` for storage. Flood and dead-pool levels come from per-reservoir entries in `RESERVOIR_THRESHOLDS`, where each entry is either an `S_{code}LEVEL*DV` variable name, a fixed TAF constant, or `None`, not a single prefix rule. These conventions are what the code implements today. Confirm the intended variable choices with the WAM team before treating them as hardened lists.

| Module | Variable list lives in | Form | In SQL |
|--------|------------------------|------|--------|
| DU Urban | `du_urban_variable`, `du_urban_delivery_arc` | SQL table | Yes |
| Env Flows | `channel_entity.csv` | Seed CSV | No |
| Reservoirs | `RESERVOIR_THRESHOLDS` (flood and dead-pool variables), `reservoir_entity.csv` (entities, capacity, dead pool) | Python constant + seed CSV | No |
| MI Contractors | `MI_CONTRACTOR_VARIABLES` | Python constant | No |
| CWS Aggregate | `CWS_AGGREGATES` | Python constant | No |
| Delta | `DELTA_VARIABLES` | Python constant | No |
| AG | `du_agriculture_entity.csv` (entities), `AG_AGGREGATES` and `GW_ONLY_DU_IDS` (Python), `AW_` / `DN_` / `GP_` prefixes | Seed CSV + Python + convention | No |
| Refuge | `REFUGE_DU_IDS` (Python, validated against `du_refuge_entity.csv`), `AW_` / `DN_` prefixes | Python + seed CSV + convention | No |

The DU Urban crosswalk that feeds `du_urban_variable` is tracked in [Master crosswalk vs `du_urban_variable`](#master-crosswalk-vs-du_urban_variable).

### Delivery and demand modules

DU Urban, MI Contractors, CWS Aggregate, AG, and Refuge convert CalSim delivery, demand, and shortage variables to monthly TAF series, then derive the statistics below. The CFS-to-TAF conversion uses each row's actual calendar `DaysInMonth` (`TAF = CFS × DaysInMonth × CFS_TO_TAF_PER_DAY`). Inputs already in TAF, such as SV demand (`UD_*`) and the MWD Table A constant, are used directly without re-conversion. AG, CWS, and Refuge also read each column's declared CSV unit and skip any column already labeled TAF. See [Data sources by module](#data-sources-by-module) for the per-module unit handling. Granularity is per LOI × water month (monthly tables) and per LOI (period summary).

#### Shared monthly statistics

| Statistic | Computed from | Calculation | Reported as |
|-----------|---------------|-------------|-------------|
| Mean | Monthly TAF series (delivery, demand, or shortage) | `mean(series)` | `*_avg_taf` (e.g. `delivery_avg_taf`, `shortage_avg_taf`, `demand_avg_taf`, `sw_delivery_avg_taf`, `gw_pumping_avg_taf`) |
| CV | Same series | `compute_cv(series)` | `*_cv` |
| Percentile bands | Same series | `np.percentile(series, p)` for p in 0, 10, 30, 50, 70, 90, 100 | `q0` … `q100` (CWS prefix: `delivery_q*`, `shortage_q*`) |
| Exceedance percentiles | Same series | `np.percentile(series, 100 - p)` for p in 5, 10, 25, 50, 75, 90, 95 | `exc_p5` … `exc_p95` |
| Demand met (monthly) | Delivery and demand TAF | `(delivery_avg / demand_avg) × 100`, clipped 0-100 where applied | `percent_of_demand_avg` on delivery monthly rows (DU Urban, MI, CWS) |
| Shortage frequency (monthly) | Shortage TAF | `(months with shortage > 0.1 TAF) / total months × 100` | `shortage_frequency_pct` on shortage monthly rows |
| Sample count | Same series | `len(series)` | `sample_count` |

#### Shared period summary statistics

| Statistic | Computed from | Calculation | Reported as |
|-----------|---------------|-------------|-------------|
| Simulation bounds | Water year index | min, max, count of `WaterYear` | `simulation_start_year`, `simulation_end_year`, `total_years` |
| Annual mean delivery | Sum of monthly delivery TAF by water year | `mean(annual_delivery)` | `annual_delivery_avg_taf` (or `annual_sw_delivery_avg_taf` for AG) |
| Annual delivery CV | Annual delivery totals | `compute_cv(annual_delivery)` | `annual_delivery_cv` (or `annual_sw_delivery_cv`) |
| Delivery exceedance | Annual delivery totals | `np.percentile(annual_delivery, 100 - p)` | `delivery_exc_p5` … `delivery_exc_p95` (or `sw_delivery_exc_p*`) |
| Annual mean shortage | Sum of monthly shortage TAF by water year | `mean(annual_shortage)` | `annual_shortage_avg_taf` |
| Shortage years | Annual shortage totals | count of years with `annual_shortage > 0.1 TAF` | `shortage_years_count` |
| Shortage frequency (period) | Annual shortage totals | `shortage_years_count / total_years × 100` | `shortage_frequency_pct` |
| Shortage exceedance | Annual shortage totals | `np.percentile(annual_shortage, 100 - p)` | `shortage_exc_p5` … `shortage_exc_p95` |
| Annual mean demand | Sum of monthly demand TAF by water year (where computed) | `mean(annual_demand)` | `annual_demand_avg_taf` |

Reliability and demand-met columns differ by module (see next table). They are not interchangeable across modules.

#### Reliability and demand-met by module

| Module | Statistic | Calculation | Reported as |
|--------|-----------|-------------|-------------|
| **DU Urban** | Mean annual demand met | Mean over years of `clip(annual_delivery / annual_demand × 100, 0, 100)` | `reliability_pct`, `avg_pct_demand_met` (same value) |
| **MI** | Period-average demand met | `safe_pct(annual_delivery_avg_taf, annual_demand_avg_taf)` when PERDV or Table A demand exists | `reliability_pct`, `avg_pct_demand_met` (same value). Omitted for contractors without demand variables. |
| **CWS Aggregate** | Delivery vs demand | `clip(annual_delivery_avg / annual_demand_avg × 100, 0, 100)` | `avg_pct_demand_met` |
| **CWS Aggregate** | Shortage-based reliability | `(1 - annual_shortage_avg / annual_demand_avg) × 100` | `reliability_pct`, `avg_pct_allocation_met` (same value) |
| **AG (DU)** | Demand minus shortage | `safe_pct(annual_demand_avg - annual_shortage_avg, annual_demand_avg)`. 100 when no shortage variable. | `reliability_pct`, `avg_pct_demand_met` |
| **AG (aggregate)** | Delivery share of supply | `annual_delivery_avg / (annual_delivery_avg + annual_shortage_avg) × 100` | `reliability_pct` |
| **Refuge** | 95th-percentile annual shortage % | `np.percentile(annual_shortage_pct, 95)` where `annual_shortage_pct = annual_shortage / annual_demand × 100` | `reliability_pct_95` only (no mean-based `reliability_pct`) |

#### Module-specific additions

**DU Urban** (`du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary`):

- Delivery arcs summed from `du_urban_delivery_arc` when `requires_sum = TRUE`.
- Demand from `compute_demand_for_du()` by `demand_mode` (`sv`, `perdv`, `table_a`, `constant_cfs`, `dv_sum`).
- Delivery and shortage written to separate monthly tables.

**MI Contractors** (`mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary`):

- PERDV demand per arc: `(D_i + SHORT_i) / PERDV_i` summed to monthly TAF.
- MWD demand: `MWD_TABLE_A_ANNUAL_TAF / 12` monthly, 1911.5 TAF/yr period total.

**CWS Aggregate** (`cws_aggregate_monthly`, `cws_aggregate_period_summary`):

- Single monthly row per aggregate combines delivery and shortage statistics.
- Demand for five project rollups: in-month `delivery_taf + shortage_taf`. MWD: Table A.
- `annual_delivery_min_taf`, `annual_delivery_max_taf` on period summary.

**AG** (`ag_du_*_monthly`, `ag_du_period_summary`, `ag_aggregate_*`):

| Statistic | Computed from | Calculation | Reported as |
|-----------|---------------|-------------|-------------|
| Applied-water demand | `AW_{du_id}` | CFS to TAF, monthly stats | `ag_du_demand_monthly` |
| Annual demand CV / exceedance | Annual demand totals | `compute_cv`, `np.percentile` | `annual_demand_cv`, `demand_exc_p5` … `demand_exc_p95` on period summary |
| Surface-water delivery | `DN_{du_id}` (GW-only: `GP + RU`) | CFS to TAF, monthly stats | `ag_du_sw_delivery_monthly` |
| Groundwater pumping | `GP_{du_id}` (fallback `AW - DN`) | CFS to TAF, clipped ≥ 0 | `ag_du_gw_pumping_monthly`, `is_calculated` flag |
| Shortage % (monthly) | Shortage and `AW` | `mean(safe_pct(shortage, AW))` | `shortage_pct_of_demand_avg` |
| Shortage % (period) | Annual shortage and demand | `mean(safe_pct(annual_shortage, annual_demand))` | `annual_shortage_pct_of_demand` |
| GW pumping % of demand | Annual GW pumping and demand | `safe_pct(annual_gw_pumping_avg, annual_demand_avg)` | `gw_pumping_pct_of_demand` |
| Aggregate delivery/shortage | `DEL_*` / `SHORT_*` project variables | Same percentile/CV pattern as DU delivery | `ag_aggregate_monthly`, `ag_aggregate_period_summary` |

**Refuge** (`refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary`):

| Statistic | Computed from | Calculation | Reported as |
|-----------|---------------|-------------|-------------|
| Shortage TAF | `SHRTG_*` (Sacramento) or `GW_SHORT_*` (SJR/Tulare), fallback `max(AW - DN, 0)` | CFS to TAF, clipped ≥ 0 | `shortage_avg_taf` |
| Shortage % (monthly) | Shortage TAF and `AW_*` | `mean((shortage / demand) × 100)` | `shortage_pct_avg`, `shortage_pct_cv` |
| Shortage % (period) | Annual shortage and demand | mean and CV of annual `(shortage / demand) × 100` | `annual_shortage_pct_avg`, `annual_shortage_pct_cv` |
| Annual shortage CV | Annual shortage TAF totals | `_safe_cv(annual_shortage)` | `annual_shortage_cv` |

No monthly demand table. Delivery monthly rows have no demand columns.

### Reservoir statistics

LOI: all 92 reservoirs. Input: monthly storage `S_{code}` (TAF). Flood and dead pool thresholds from `RESERVOIR_THRESHOLDS` in [`reservoirs/reservoir_metrics.py`](reservoirs/reservoir_metrics.py) (DV level variables or constants). Capacity from `reservoir_entity.csv` with four hardcoded overrides (FOLSM, MLRTN, OROVL, MELON). See [Reservoir calculation methodology](#reservoir-calculation-methodology).

| Statistic | Computed from | Calculation | Reported as |
|-----------|---------------|-------------|-------------|
| Monthly storage mean/CV | `S_{code}` pooled by water month | `mean`, `calculate_cv` | `reservoir_storage_monthly`: `storage_avg_taf`, `storage_cv` |
| Monthly storage % capacity | Storage and capacity | `(storage_avg_taf / capacity_taf) × 100` | `storage_pct_capacity` |
| Monthly storage percentiles | `S_{code}` by water month | `np.percentile` in TAF and as `(value / capacity) × 100` | `q*`, `q*_taf`, `exc_p*`, `exc_p*_taf` |
| Monthly percentile bands (charts) | Same storage series | Percentiles of TAF, expressed as % capacity | `reservoir_monthly_percentile`: `q0`-`q100`, `mean_value`, `mean_taf` |
| Flood pool probability | Storage vs flood threshold | `P(storage >= flood_threshold)` over all months, April only, September only | `flood_pool_prob_all`, `flood_pool_prob_april`, `flood_pool_prob_september` |
| Dead pool probability | Storage vs dead threshold | `P(storage <= dead_threshold)` over all months, September only | `dead_pool_prob_all`, `dead_pool_prob_september` |
| Storage CV (period) | Full record, April only, September only | `calculate_cv` on filtered series | `storage_cv_all`, `storage_cv_april`, `storage_cv_september` |
| Annual and seasonal averages | Storage TAF | Mean of per-water-year means. April/Sep: mean of calendar-month values. | `annual_avg_taf`, `april_avg_taf`, `september_avg_taf` |
| Storage exceedance (period) | `(S / capacity) × 100` over full record | `np.percentile(..., 100 - p)` | `storage_exc_p5` … `storage_exc_p95` |
| Spill frequency (monthly) | Storage vs flood threshold | Months with `S >= flood_threshold` / total months × 100 | `reservoir_spill_monthly`: `spill_frequency_pct`, `spill_months_count`, `total_months`, `storage_at_spill_avg_pct`, `spill_threshold_pct` |
| Spill frequency (period) | Storage vs flood threshold by water year | Years with any spill month / `total_years × 100` | `reservoir_period_summary`: `spill_years_count`, `spill_frequency_pct` |

Spill is storage-based (not `C_*_FLOOD` flow), reported as a frequency, not a volume. The spill volume columns in the schema (`spill_avg_cfs`, `spill_max_cfs`, `spill_mean_cfs`, `spill_peak_cfs`, `annual_spill_avg_taf`, `annual_spill_cv`, `annual_spill_max_taf`) are never assigned and stay NULL. `spill_threshold_pct` and `storage_at_spill_avg_pct` are the average storage as a percent of capacity during spill months, not the flood trigger level.

Spill depends on a flood-control level (`flood_var` in `RESERVOIR_THRESHOLDS`) being present in the DV output. When the configured `flood_var` is `None` or is a level variable absent from the scenario CSV, the period summary writes `spill_frequency_pct = 0` (not NULL), which reads the same as a reservoir that truly never spills. In the `s0020` reference run only about 12 of 92 reservoirs have a usable flood level, so most report 0. See [reservoir spill threshold coverage and spill volume](#reservoir-spill-threshold-coverage-and-spill-volume) for the hardening plan.

### Environmental flow statistics

LOI: 59 channel reaches (`network_arc_id`). Input: `C_{reach}` (DV), `UNIMP_{watershed}` and `EFLOWS_{reach}` (SV), optional `C_{reach}_MIF`. Five CEFF seasons (wet peak, wet base, spring recession, dry, fall pulse) for seasonal tables.

| Statistic | Computed from | Calculation | Reported as |
|-----------|---------------|-------------|-------------|
| Flow mean/CV/percentiles | `C_{reach}` | Monthly and seasonal stats on CFS and TAF (`TAF = CFS × DaysInMonth × CFS_TO_TAF_PER_DAY`, actual calendar days) | `env_flow_channel_monthly`, `env_flow_channel_seasonal`: `flow_avg_cfs`, `flow_avg_taf`, `flow_cv`, `flow_q*_cfs` / `flow_q*_taf`, `flow_exc_p*_cfs` / `flow_exc_p*_taf` |
| % unimpaired | `C_{reach}`, `UNIMP_{watershed}` | `(C / UNIMP) × 100` where `UNIMP >= 1.0` CFS | `pct_unimpaired_avg`, `pct_unimpaired_cv`, `q*`, `exc_p*` (monthly and seasonal) |
| % functional flows | `C_{reach}`, `EFLOWS_{reach}` | `(C / EFLOWS) × 100` where `EFLOWS >= 1.0` CFS | `pct_ff_avg`, `pct_ff_cv`, `deviation_avg` (= mean - 100), `target_met_pct` (% years >= 100%), `q*`, `exc_p*` (seasonal, ~17 channels) |
| Flow alteration index | Monthly `C` and `UNIMP` | `scipy.stats.pearsonr(C, UNIMP)` on months with both valid, ≥ 10 pairs | `env_flow_channel_period_summary`: `pearson_r`, `p_value` |
| Period % unimpaired / % FF | Full record | Mean and annual CV of per-year means | `avg_pct_unimpaired`, `annual_cv_pct_unimpaired`, `avg_pct_ff`, `annual_cv_pct_ff` |
| MIF met | `C_{reach}`, `C_{reach}_MIF` | `% months where C >= MIF` | `mif_met_pct` |
| Annual flow exceedance | Per-water-year mean CFS or total TAF | `np.percentile(annual_values, 100 - p)` | `flow_exc_p*_cfs`, `flow_exc_p*_taf` |

### Delta statistics

LOI: 8 `variable_code` rows (one NDO outflow node, one X2 marker, four compliance EC stations, two pumping-plant EC stations). Period metrics are stored in `delta_period_summary.summary_data` JSONB.

| Statistic | Computed from | Calculation | Reported as |
|-----------|---------------|-------------|-------------|
| Monthly mean/CV/percentiles | Each DV variable × water month | NDO converted to TAF (`CFS × DaysInMonth × CFS_TO_TAF_PER_DAY`, actual calendar days). X2 and EC in native units. | `delta_monthly`: `avg` (TAF for NDO, native for X2 and EC), `cv`, `q*`, `exc_p*`, `avg_cfs` (NDO only) |
| NDO annual mean/CV/exceedance | Water-year sum of monthly NDO TAF | `mean`, `_safe_cv`, `np.percentile` on annual totals | `summary_data`: `annual_avg_taf`, `annual_cv`, `exc_p*`, `avg_cfs` |
| NDO September stats | Calendar September NDO | Mean and CV of September TAF | `sept_avg_taf`, `sept_cv` |
| X2 April/September/spring/fall | `X2_PRV_KM` filtered by calendar month or season | April and September: mean, CV, exceedance. Spring (Mar-May) and fall (Sep-Nov): mean and CV only. | `april_avg_km`, `sept_avg_km`, `spring_avg_km`, `fall_avg_km`, each with a `*_cv`, plus `april_exc_p*` and `sept_exc_p*` |
| Salinity mean/CV/exceedance | EC monthly series | Mean, CV, exceedance on full record | `avg_ec`, `cv`, `exc_p*` |
| Salinity spring/fall | EC filtered Mar-May / Sep-Nov | Mean and CV | `spring_avg_ec`, `fall_avg_ec`, plus `_cv` |
| Threshold exceedance | EC monthly values | `% months where EC > threshold` (450, 900, 1600, 2500 µmhos/cm) | `exceed_d1641_pct`, `exceed_low_pct`, `exceed_mid_pct`, `exceed_high_pct` |

### Sensitivity statistics *(experimental)*

Reads pre-computed monthly statistics from the database after all per-scenario modules finish. Groups scenarios by hydroclimate sibling (climate) or hydroclimate level (operational). Writes `sensitivity_climate` and `sensitivity_operational`. Reservoir input uses `storage_avg_taf` only.

| Statistic | Computed from | Calculation | Reported as |
|-----------|---------------|-------------|-------------|
| Climate sensitivity | Metric values at historical, cc50, cc95 hydroclimate within a sibling group | `cc50_abs_change = cc50 - hist`, `cc95_abs_change = cc95 - hist`, pct change = `(value - hist) / |hist| × 100` | `sensitivity_climate`: `hist_value`, `cc50_value`, `cc95_value`, `cc50_abs_change`, `cc95_abs_change`, `cc50_pct_change`, `cc95_pct_change` |
| Operational sensitivity | Metric values across operational scenarios at one hydroclimate level | min, max, mean, std, range, `pct_range = range / |mean| × 100` (requires ≥ 2 scenarios) | `sensitivity_operational`: `scenario_count`, `min_value`, `max_value`, `mean_value`, `std_value`, `range_value`, `pct_range` |


## Reading the outputs (percentile bands and exceedance)

Most modules emit the same two chart-oriented statistics, so they are described once here rather than repeated per module.

**Percentile band charts:** For each water month (Oct = 1 through Sep = 12) the pipeline computes percentile bands (`q0`, `q10`, `q30`, `q50`, `q70`, `q90`, `q100`) across all simulated years. Plotted as nested bands by month, they are especially helpful for a lay audience to understand how a quantity differs between wet years and dry years. For supply quantities (reservoir storage, delivery, and flow), the upper bands are the wettest years and the lower bands are the driest: `q90` and `q100` are the wettest roughly 10% of years, `q0` and `q10` are the driest roughly 10% of years, and `q50` is the median (typical) year. The spread between the outer bands is the year-to-year variability a location sees. For shortage the orientation flips, since a high shortage corresponds to a dry year.

**Exceedance charts:** Exceedance percentiles (`exc_p5` through `exc_p95`) report the value exceeded p% of the time, computed as `np.percentile(series, 100 - p)`. They answer reliability questions in plain language, for example "this delivery was met or exceeded in 90% of years for that month." A flatter exceedance curve means a more reliable supply, and a steep drop at the dry end means frequent shortfalls.

## Data sources by module

| Module | Delivery source | Demand source | Shortage / spill source | Units |
|--------|----------------|---------------|-------------------------|-------------|
| **Reservoirs** | Storage `S_{code}` (DV) | - (no demand; storage is normalized against capacity and dead pool from `reservoir_entity.csv`, with 4 capacity overrides) | Spill = fraction of months storage is at or above the flood-control level, one zone below the top/capacity level (`S_{code}LEVEL(x-1)DV`, from DV, or `RESERVOIR_THRESHOLDS` constants where absent). Not a `C_*_FLOOD` flow | TAF (storage) |
| **DU Urban** | Per-DU `delivery_variable` from `du_urban_variable` (`DL_*`, `D_*` incl. `D_*_PMI` and `D_WTP*` arcs, `DEL_SWP_MWD`). Multi-arc DUs sum arcs from `du_urban_delivery_arc` | Per-DU, by `demand_mode`: `sv` (`UD_*` from SV), `perdv` ((delivery + shortage) / `PERDV_SWP_*`), `table_a` (MWD constant), `constant_cfs`, `dv_sum` | Per-DU `shortage_variable` (`SHRTG_*`, `SHORT_D_*_PMI`, `SHORT_SWP_MWD`) | CFS (delivery, shortage from DV), TAF (`UD_*` from SV, Table A) |
| **MI Contractors** | Per-contractor `D_*_PMI` (summed across arcs), `DEL_SWP_MWD` (MWD). Aggregates `DEL_SWP_PMI` / `_N` / `_S`, `DEL_CVP_PMI_N` / `_S` | SWP contractors: (delivery + shortage) / `PERDV_SWP_*`. MWD: Table A constant. Aggregates: none | `SHORT_D_*_PMI`, `SHORT_D_*_MWDSC_PMI` (MWD, four arcs summed). Aggregates `SHORT_SWP_PMI` / `_N` / `_S`, `SHORT_CVP_PMI_N` / `_S` | Mixed per DSS header (`D_*_PMI` / `SHORT_*` CFS, some aggregates TAF), converted unit-aware. Table A TAF |
| **CWS Aggregate** | Six aggregates: `DEL_SWP_PMI` / `_N` / `_S`, `DEL_CVP_PMI_N_WAMER` (fallback `DEL_CVP_PMI_N`) / `_S`, `DEL_SWP_MWD` | `del_plus_short` (delivery + shortage) for the five project rollups. MWD: Table A. No DEMANDS CSV | Matching `SHORT_SWP_PMI` / `_N` / `_S`, `SHORT_CVP_PMI_N_WAMER` (fallback `SHORT_CVP_PMI_N`) / `_S`, `SHORT_SWP_MWD` | Mixed per DSS header (CFS converted to TAF, TAF kept). Table A TAF |
| **AG** | Net delivery `DN_*` (GW-only DUs: synthesized `GP_* + RU_*`) | Applied water `AW_*` (DV) | `SHRTG_*` (Sacramento) / `GW_SHORT_*` (SJR/Tulare) | CFS (converted unit-aware, TAF columns kept) |
| **Refuge** | Net delivery `DN_*` (DV). No GW-only synthesis, unlike AG | Applied water `AW_*` (DV) | `SHRTG_*` (Sacramento `_PR` DUs) / `GW_SHORT_*` (SJR/Tulare `_PR` DUs). Fallback `max(AW - DN, 0)` where no model variable exists | CFS (converted unit-aware, TAF columns kept) |
| **Env Flows** | River flow `C_{reach}`, `C_{reach}_MIF` (DV); `UNIMP_*`, `EFLOWS_*` (SV) | - | Flow metrics (percent unimpaired, functional flows, alteration index), not delivery/shortage | CFS |
| **Delta** | Outflow `NDO`, X2 `X2_PRV_KM`, compliance-point EC `EM_EC_MONTH` / `JP_EC_MONTH` / `RS_EC_MONTH` / `CO_EC_MONTH`, pumping-plant EC `BANKSEC_MAX14DAY` / `TRACYEC_MAX14DAY` (all DV) | - | Flow and salinity metrics (outflow, X2 position, compliance-point EC), not delivery/shortage | CFS (`NDO`), KM (X2), UMHOS/CM (EC) |
| **Sensitivity** *(experimental)* | Pre-computed per-scenario statistics read from the database, not S3 CSVs | - | Cross-scenario climate and operational spread, not delivery/shortage | n/a (derived) |

This table is a summary. The authoritative per-module variable lists live in the module code and are best read there:

- **Reservoirs:** [`reservoirs/calculate_reservoir_statistics.py`](reservoirs/calculate_reservoir_statistics.py) (storage `S_{code}`, flood thresholds in `_get_flood_threshold`), with the full treatment in [Reservoir calculation methodology](#reservoir-calculation-methodology).
- **DU Urban:** the `du_urban_variable` database table (per-DU delivery, demand, shortage), read by [`du_urban/calculate_du_statistics_v2.py`](du_urban/calculate_du_statistics_v2.py). See also [`water_user_categories.md`](../../database/topic_docs/cws/water_user_categories.md).
- **MI Contractors:** `MI_CONTRACTOR_VARIABLES` in [`mi/calculate_mi_statistics.py`](mi/calculate_mi_statistics.py).
- **CWS Aggregate:** `CWS_AGGREGATES` in [`cws_aggregate/calculate_cws_aggregate_statistics.py`](cws_aggregate/calculate_cws_aggregate_statistics.py).
- **AG:** the module docstring plus `GW_ONLY_DU_IDS` and `AG_AGGREGATES` in [`ag/calculate_ag_statistics.py`](ag/calculate_ag_statistics.py).
- **Refuge:** the module docstring in [`refuge/calculate_refuge_statistics.py`](refuge/calculate_refuge_statistics.py), with the full treatment in [Wildlife refuge statistics](#wildlife-refuge-statistics).
- **Env Flows:** the module docstring in [`env_flows/calculate_env_flow_statistics.py`](env_flows/calculate_env_flow_statistics.py), with the full treatment in [Environmental river flow statistics](#environmental-river-flow-statistics).
- **Delta:** `DELTA_VARIABLES` plus the module docstring in [`delta/calculate_delta_statistics.py`](delta/calculate_delta_statistics.py).
- **Sensitivity:** the module docstring in [`sensitivity/calculate_sensitivity.py`](sensitivity/calculate_sensitivity.py) (reads per-scenario statistics from the database; experimental).

> **Note (March 2026):** AG and refuge demand uses `AW_*` (DV output, model-optimised applied water), not `AWO_*` (SV input, pre-model demand order), matching the COEQWAL notebooks. See [Water balance](#water-balance) below.

### Notebook alignment (coeqwal and COEQWAL_V3, March 2026 audit)

A review of both reference notebooks, `COEQWAL_V3/coeqwalpackage/DataExtraction.py` and `coeqwal/notebooks/coeqwalpackage/DataExtraction.py`, confirmed the conventions below. The two notebooks agree on every point checked. Known intentional differences and items to confirm are listed under the table.

| Convention | Notebook behaviour (both repos) | ETL behaviour |
|---|---|---|
| Demand source for ag DUs | `AW_*` from DV for most, with `AW_NIDDC_NA3` and `AW_ELDID_NA1` routed to SV | Reads `AW_*` from DV for all ag DUs (does not special-case those two). See note 1 |
| Demand source for refuge DUs | `AW_*` from DV | Uses `AW_*` from DV |
| GP for refuge DUs | Not used. Notebooks never reference `GP_*_PR*` | Not used |
| GP for GW-only `_NA` DUs | Synthesizes `DN = GP + RU` (`SW_DELIVERY-NET`) for 11 `_NA` DUs | Synthesizes delivery as `GP + RU` for its 17 `GW_ONLY_DU_IDS`. Set membership differs, see note 2 |
| Water balance check | Could not find in notebooks | Checks GP vs AW for ag only. GP/AW up to ~1.15x is expected per WRESL (`AW + RP = DN + GP + RU + SHORTAGE`) |
| Shortage for ag DUs | Could not find in notebooks | Uses `SHRTG_*` (Sacramento) and `GW_SHORT_*` (SJR/Tulare), full coverage |
| Shortage for refuge DUs | Could not find in notebooks | Uses model `SHRTG_*`/`GW_SHORT_*` when available, falls back to `max(AW - DN, 0)` |
| Shortage for M&I | `SHORT_*` used as intermediates for demand back-calculation, then dropped | Uses `SHORT_*` directly |
| CFS to TAF constant | `0.001984` (rounded), both repos | Intentionally uses the exact `86400/43560000 = 0.001983471`. See note 3 |
| DU type classification | Name-based (`UD` prefix = urban, everything else goes to the DV list). No programmatic type filtering | Filters ag DUs via the entity table |

**Note 1 (ag demand for two DUs):** the notebooks read `AW_NIDDC_NA3` and `AW_ELDID_NA1` from the SV input, while the ETL reads all `AW_*` from the DV output, including these two. The difference is confined to two DUs. Confirm with the modeling team whether the DV or SV value is intended for them.

**Note 2 (GW-only set membership):** the notebooks synthesize `GP + RU` for 11 `_NA` DUs (the eight Sacramento DUs plus `26S_NA`, `60S_NA1`, `60S_NA2`), whereas the ETL's `GW_ONLY_DU_IDS` holds 17 (it includes `26N_NA` rather than `26S_NA`, and covers more SJR/Tulare DUs). The counts and membership differ. Confirm the intended set with the modeling team.

**Note 3 (CFS to TAF):** the ETL deliberately uses the exact factor `0.001983471` rather than the notebooks' rounded `0.001984`. The two differ by about 0.027%. See [Unit conversion: CFS to TAF](#unit-conversion-cfs-to-taf).

## Unit conversion: CFS to TAF

All modules use the same precise conversion factor:

```
TAF = CFS × DaysInMonth × 0.001983471
```

where `0.001983471 = 86400 / 43560 / 1000` (seconds-per-day / sq-ft-per-acre / kilo-acre-feet).

Both reference notebooks (coeqwal and COEQWAL_V3) use the rounded `0.001984`. The ETL deliberately uses the exact factor instead. The two differ by about 0.027%, which is negligible but avoids carrying a rounding error through every converted value.

Each module derives `DaysInMonth` from `pd.DatetimeIndex.daysinmonth` so leap years and short months are handled exactly.

## CSV input format

The statistics modules read CalSim DSS-export CSVs from the `coeqwal-model-run` S3 bucket:

```
s3://coeqwal-model-run/
├── reference/
│   └── all_metrics_output.csv                                  # verification reference from Metrics.ipynb
└── scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv
```

Each scenario DV CSV has 7 header rows before the data. Column 0 holds a row label (the DSS pathname part), and the per-variable metadata starts in column 1. The rows map to DSS pathname parts (A, B, C, E, F), then record type, then units. The D-part is not written for DV output.

```
Col 0    Col 1            Col 2                  ...
a        CALSIM           CALSIM                       (A-part: source)
b        S_SHSTA          S_SHSTALEVEL5DV              (B-part: variable name)
c        STORAGE          STORAGE-LEVEL                (C-part: kind)
e        1MON             1MON                         (E-part: time step)
f        L2020A           L2020A                       (F-part: level)
type     PER-AVER         PER-AVER                     (record type)
units    TAF              TAF                          (units: TAF, CFS, ACRES, NONE, ...)
1921-10-31 00:00:00  313.0  335.14  ...                (data rows start here)
```

The ETL identifies columns by the B-part (row 1, `var_names`) and reads units from row 6. When two pathnames share a B-part but differ in C-part (for example `SHRTG_*` with `SHORTAGE` vs `DELIVERY-SHORTAGE`), the loader resolves the duplicate using the C-part (row 2). See [`units.py`](units.py) for the parser. SV input CSVs use a different 7-row layout (A, B, C, D, E, F, units, with no record-type row).

Each module selects its own variables from these columns. For the variables a module loads, its output tables, and any module-specific data, see that module's section below (for example, [Reservoir calculation methodology](#reservoir-calculation-methodology)).

## Data integrity safeguards

The delivery and demand modules run three warn-only safeguards. They **warn** (log) but do **not** clamp or discard data, so suspicious values remain visible for investigation. They run only in the modules listed below, not in every module (`du_urban`, `reservoirs`, `delta`, `env_flows`, and `sensitivity` call none of them).

| Safeguard | Where | What it checks |
|-----------|-------|----------------|
| `validate_water_balance` | AG only (after CFS to TAF conversion) | `GP_{DU} ≤ AW_{DU} × 1.01` for ag DU-months. GP/AW ratios of 1.0-1.15× are expected due to riparian losses (RP). The function docstring cites the `AW + RP = DN + GP + RU + SHORTAGE` balance from WRESL `constraints-Deliveries.wresl`, and the GP bound (`GPmax`) it references lives in `constraints-gwpumping.wresl`. Not applied to refuge DUs |
| `check_post_conversion_magnitude` | AG, MI, CWS, Refuge (after CFS to TAF conversion) | Max monthly TAF value < 2 000 per column. Values above this strongly suggest a double conversion or a missed CFS to TAF step. |
| `safe_pct` | AG (monthly shortage and period summary), MI (period summary) | Percentage result > 200 % triggers a warning. Catches cases where numerator/denominator have different units. |

These three functions live in `etl/statistics/units.py` and are imported by the modules above. Thresholds are defined as constants (`PCT_WARNING_THRESHOLD`, `MONTHLY_TAF_SANITY_LIMIT`) and can be adjusted without changing module code.

Separately, AG applies a DU filter before computing statistics: DU IDs discovered from `AW_*` columns are filtered against `du_agriculture_entity.csv`, so non-ag DUs (refuges, urban) that happen to have `AW_*` columns in the DV output are excluded to prevent cross-contamination. Unlike the safeguards above, this filter discards rows rather than only logging, and it is inline logic in `calculate_ag_statistics.py`, not a `units.py` function.

### Unit-aware CSV loading

The shared helper `parse_dss_csv_header()` in `units.py` reads the 7-row DSS header to extract variable names (row 1) and units (row 6).  Each loader builds a `units_map = dict(zip(var_names, units_row))` so the caller knows which columns are CFS vs TAF *before* applying any conversion.

---

## Statistics ETL operations

Statistics is step 6 of the scenario pipeline. For the end-to-end sequence (`scan` through `activate`) and where statistics fits, see the pipeline runbook in [`etl/README.md`](../README.md#running-the-scenario-model-run-pipeline). Running the statistics is a manual Cloud9 step today, run after extraction lands a scenario's CSVs in S3. This section covers the statistics-specific operational details: worker sizing, long-run mechanics, and S3 credentials.


### Running the statistics ETL

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

The ETL uses DELETE+INSERT per module per scenario, so re-running a scenario is idempotent (safe to repeat). Use `--start-from` to skip already-completed scenarios:

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

Each worker loads one scenario's CSV (~300MB on disk) and expands it into a pandas DataFrame plus intermediate arrays, peaking at roughly 2-3 GB of RAM per worker during computation. Pick `--workers` to fit your environment:

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

Higher worker counts risk OOM (the kernel will SIGKILL Python) or heavy swapping that slows the run more than it speeds it up. When in doubt, start with 1, watch `free -h` for a few minutes, then increase.

#### When to use `--batch-size`

For a full backfill (40+ scenarios), always pair `--workers` with `--batch-size`:

```bash
python run_all.py --all-scenarios --workers 4 --batch-size 15
```

Why batching helps:

1. **Resumability:** Each batch is a clean log checkpoint. If something fails in batch 3 of 5, you know to resume with `--start-from sXXXX` at the start of batch 3.
2. **Long-run resilience:** AWS SSO and IAM session tokens have expiration limits. Breaking a multi-hour run into ~10-15 scenario batches gives you natural pause points if a credential needs refreshing.
3. **Easier scorecard reading:** The end-of-batch summary is easier to scan than one giant log of 60 scenarios.

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

**Inside tmux**, activate the venv and run the ETL. Pipe to `tee` so output goes to both screen and a timestamped log file:

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

The log ends with a **scorecard** showing pass/fail for every scenario × module, plus a CSV audit file (`stats_audit_*.csv`) written to `etl/statistics/audit_reports/` by default (gitignored). Override the destination with `--audit-dir`.

#### Resuming after a failure

1. Check the log for the last `sXXXX finished` line to find the last completed scenario
2. Look at `scenarios.py` to find the next scenario in order
3. Re-run with `--start-from` set to the next scenario:

```bash
python run_all.py --all-scenarios --workers 4 --batch-size 20 --start-from sXXXX 2>&1 | tee ~/environment/coeqwal-backend/etl_resume_$(date +%Y%m%d_%H%M%S).log
```

### AWS credentials for S3 access

The statistics ETL reads CalSim CSVs from the `coeqwal-model-run` S3 bucket. Cloud9's default "AWS managed temporary credentials" expire with the user's SSO session, which can interrupt multi-hour ETL runs.

**Solution: IAM instance role** (set up April 2026)

The Cloud9 EC2 instance uses `AWSCloud9SSMAccessRole` with the `coeqwal-etl-s3-readonly` policy attached, granting `s3:GetObject` and `s3:ListBucket` on the `coeqwal-model-run` bucket. To use the instance role instead of SSO credentials:

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

If you re-authenticate your SSO session and Cloud9 re-enables managed credentials, repeat the commands above before starting the ETL.

**To restore SSO credentials** for day-to-day work after the ETL:

```bash
aws cloud9 update-environment \
  --environment-id 48dc921ad0fd48ea93c2a2e218bd8ace \
  --managed-credentials-action ENABLE
```

## Manual and development runs

### Prerequisites

```bash
# Python 3.9+
python --version

# Install dependencies
pip install -r requirements.txt
```

`scipy` is optional. It is commented out in `requirements.txt` and enables the Pearson r alteration index in `env_flows`. Without it that one metric is skipped and everything else runs.

### Single-module invocation

The production path runs every module together via `run_all.py` (see [Running the statistics ETL](#running-the-statistics-etl)). Each module also exposes its own `main.py` for re-running or debugging one section in isolation. `reservoirs` is shown here as the example; the same flags apply to every module's `main.py`.

Write directly to the database:

```bash
# One scenario
DATABASE_URL=postgres://... python reservoirs/main.py --scenario s0020

# All scenarios
DATABASE_URL=postgres://... python reservoirs/main.py --all-scenarios
```

Read input from a local CSV instead of S3, or skip the database entirely:

```bash
# Local CSV instead of S3
python reservoirs/main.py --scenario s0020 --csv-path ../reference/s0020_coeqwal_calsim_output.csv

# Calculate without writing (add --output-json to print results)
python reservoirs/main.py --scenario s0020 --dry-run
```

Emit SQL to review or load by hand instead of writing directly:

```bash
python calculate_reservoir_statistics.py --scenario s0020 --output-sql output.sql
python calculate_reservoir_percentiles.py --scenario s0020 --output-sql percentiles.sql
psql $DATABASE_URL -f output.sql
```

### Sample data and quick checks

Module runs can read input from a local CSV (see `--csv-path` above). Place a CalSim output CSV in `etl/reference/`. Any file with the 7-header format works:

```bash
aws s3 cp s3://coeqwal-model-run/scenario/s0020/csv/s0020_coeqwal_calsim_output.csv ../reference/
```

`test_local.py` and `dev_run.sh` are reservoir-specific helpers for a fast sanity check:

```bash
python test_local.py    # import and calculation smoke test
./dev_run.sh            # runs common reservoir dev scenarios
```

---

## Reservoir calculation methodology

This section covers the reservoir-specific calculations (storage percentile bands, flood and dead pool probability, CV, and annual and monthly averages). The implementations all live in `calculate_reservoir_percentiles.py` and `reservoir_metrics.py`. The conventions shared across the delivery and demand modules (percentile bands, exceedance, CV, shortage thresholds) are in [Statistics we compute](#statistics-we-compute), and each delivery and demand module has its own section below.

All calculations should be aligned with the WAM team Jupyter notebooks located at https://github.com/maramahmedd/coeqwal:

- `coeqwal/notebooks/coeqwalpackage/metrics.py`
- `coeqwal/notebooks/Metrics.ipynb`

### 1. Percentile bands

**Purpose**: Supply data for the reservoir storage percentile band charts on the website. For how to read these charts (wet vs dry years, exceedance, zero years), see [Reading the outputs](#reading-the-outputs-percentile-bands-and-exceedance).

**Implementation here** (`calculate_reservoir_percentiles.py` `calculate_percentiles_for_reservoir()`):
```python
for p in PERCENTILES:  # [0, 10, 30, 50, 70, 90, 100]
    taf_value = float(np.percentile(month_data_taf, p))
    pct_value = (taf_value / capacity_taf) * 100 if capacity_taf > 0 else 0
    stats[f'q{p}'] = round(pct_value, 2)       # percent of capacity
    stats[f'q{p}_taf'] = round(taf_value, 2)   # raw TAF
```

**Notebook reference** (`metrics.py` `compute_iqr_value()`):
```python
iqr_values = subset_df.apply(lambda x: x.quantile(iqr_value), axis=0)
```

**Comparison**:

| Aspect | ETL | Notebook |
|--------|---------|----------|
| Method | `np.percentile()` | `pandas.quantile()` |
| Grouping | By **water month** (Oct=1 ... Sep=12) | By annual means by default (`compute_iqr_value` runs with `annual=True`), or the full month-filtered series when `annual=False` |

**Note**: Here we group by water month before calculating percentiles to show range and variability by month in the website charts. This is a deliberate design choice for the frontend visualization. Because the backend pools monthly values while the notebook's default percentile path operates on annual means, the two populations differ and the percentile values are not identical. See the [Provenance and verification](#provenance-and-verification) entry for reservoir percentile bands, which is flagged for modeling-team review.

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

**Notebook reference** (`metrics.py` `frequency_hitting_level()`):
```python
subset_df_res_comp_values = subset_df_res.values - subset_df_floodzone.values
if floodzone:
    subset_df_res_comp_values += 0.000001
exceedance_days = count_exceedance_days(subset_df_res_comp, 0)
```

**Threshold sources** (the ETL's `RESERVOIR_THRESHOLDS` in `reservoir_metrics.py`):

| Reservoir | Flood threshold variable |
|-----------|-------------------------|
| SHSTA | `S_SHSTALEVEL5DV` (model output) |
| OROVL | `S_OROVLLEVEL5DV` (model output) |
| TRNTY | `S_TRNTYLEVEL4DV` (model output) |
| FOLSM | `S_FOLSMLEVEL5DV` (model output) |
| MELON | `S_MELONLEVEL4DV` (model output) |
| MLRTN | 524 TAF (constant) |
| SLUIS_CVP | `S_SLUIS_CVPLEVEL4DV` (model output) |
| SLUIS_SWP | `S_SLUIS_SWPLEVEL4DV` (model output) |

For TRNTY, MELON, and the two San Luis shares, `LEVEL5DV` is the capacity curve, so the ETL reads flood control from `LEVEL4DV`. SHSTA, OROVL, and FOLSM use `LEVEL5DV` directly.

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

**Threshold sources**:

| Reservoir | Dead pool threshold |
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

**Formula**: `CV = standard_deviation / |mean|`, with guards for near-zero means and implausible ratios.

**Implementation** (`reservoir_metrics.py:calculate_cv()`):
```python
mean = float(data.mean())
if abs(mean) < CV_MIN_MEAN_TAF:   # near-zero mean: CV undefined, return 0
    return 0.0
std = float(data.std())
cv = std / abs(mean)
if cv > 99.0:                     # implausible ratio: return 0 (not capped)
    return 0.0
return cv
```

**Notebook reference** (`metrics.py` `compute_cv()`):
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

**Notebook reference** (`metrics.py` `ann_avg()`):
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

**Notebook reference** (`metrics.py` `mnth_avg()`):
```python
metric_value = compute_mean(df, var_name, [study_index], units, months=[mnth_num])
```

**Metrics calculated**:

- `april_avg_taf`: Average April storage (spring peak)
- `september_avg_taf`: Average September storage (end of water year)

### Input variables

Storage and the level thresholds come from the CalSim DV output CSV (`scenario/{id}/csv/{id}_coeqwal_calsim_output.csv`):

| Variable | Meaning | Units |
|----------|---------|-------|
| `S_{code}` | Storage | TAF |
| `S_{code}LEVEL(x-1)DV` (`flood_var`) | Flood-control level, intended to be one zone below the top (capacity) level. The exact level number varies by reservoir (`LEVEL5DV` for Shasta, `LEVEL4DV` for Trinity). The configured level is absent from the DV output for most reservoirs (see [Reservoir spill threshold coverage and spill volume](#reservoir-spill-threshold-coverage-and-spill-volume)) | TAF |
| `S_{code}LEVEL1DV` (`dead_var`) | Dead-pool level. Some reservoirs use a hardcoded constant instead | TAF |

The per-reservoir mapping of which level variable to use lives in `RESERVOIR_THRESHOLDS` in `reservoir_metrics.py`. Reservoir spill is derived from storage relative to the flood-control level, not from a `C_*_FLOOD` flow variable. Capacity (`capacity_taf`) and dead pool (`dead_pool_taf`) for normalization are read from `reservoir_entity.csv`, not computed from the DV file (see [Capacity overrides](#capacity-overrides)).

### Output tables

| Table | Contents |
|-------|----------|
| `reservoir_monthly_percentile` | Monthly percentile bands for UI charts (`q0`-`q100` as percent of capacity, `mean_value`, `mean_taf`) |
| `reservoir_storage_monthly` | Monthly storage statistics including CV |
| `reservoir_spill_monthly` | Monthly spill and flood-release frequency statistics |
| `reservoir_period_summary` | Period-of-record summary with flood-pool, dead-pool, CV, and annual and monthly average metrics |

### Target reservoirs

The ETL processes all 92 reservoirs defined in `reservoir_entity.csv`. Major reservoirs:

| Code | Reservoir | Capacity (TAF) | Dead pool (TAF) |
|------|-----------|----------------|-----------------|
| SHSTA | Shasta | 4,552 | 115 |
| TRNTY | Trinity | 2,448 | 105 |
| OROVL | Oroville | 3,537 | 850 |
| FOLSM | Folsom | 975 | 115 |
| MELON | New Melones | 2,400 | 300 |
| MLRTN | Millerton | 520 | 115 |
| SLUIS_CVP | San Luis (CVP) | 1,062 | 15 |
| SLUIS_SWP | San Luis (SWP) | 979 | 10 |

### Capacity overrides

For percent-of-capacity calculations, capacity and dead pool are read from `reservoir_entity.csv` by `load_reservoir_entities`. The entity-CSV capacity is meant to match each reservoir's highest `S_{code}LEVELxDV` level. Four major reservoirs have that top-level variable absent from the DV output, so their capacities are hardcoded from V3's `DataExtraction.py` and override the entity-CSV value at load time (`CAPACITY_OVERRIDES` in `calculate_reservoir_statistics.py` and `calculate_reservoir_percentiles.py`). The exact override values are in [Appendix G](#g-reservoir-capacity-constants-wresl-verified).

### Spill reporting

Spill is reported as a frequency, not a volume. `spill_frequency_pct`, `spill_years_count`, `spill_months_count`, and `spill_monthly_frequency_pct` carry real values. The schema's spill volume columns are never computed and stay NULL, and the "one level below the highest" flood-threshold rule is only partly realized. See [Reservoir spill threshold coverage and spill volume](#reservoir-spill-threshold-coverage-and-spill-volume) for the full accounting and hardening plan.

### Files

| File | Purpose |
|------|---------|
| `reservoirs/main.py` | CLI entry point |
| `reservoirs/calculate_reservoir_statistics.py` | Storage, spill, and period-summary statistics |
| `reservoirs/calculate_reservoir_percentiles.py` | Percentile bands for website charts |
| `reservoirs/reservoir_metrics.py` | Core calculation functions and `RESERVOIR_THRESHOLDS` |

## Urban demand unit statistics

The `du_urban/` module calculates delivery statistics for the urban demand units defined in the `du_urban_variable` table (90 active rows in the May 2026 audit export).

### Unit conversion

CalSim outputs demands and deliveries in CFS. The module converts to TAF with the shared precise factor documented in [Unit conversion: CFS to TAF](#unit-conversion-cfs-to-taf).

### Calculated metrics

See [Statistics we compute](#statistics-we-compute) for derived statistics, LOI, and formulas. Input variables and demand modes are in [Data sources by module](#data-sources-by-module) (DU Urban row) and the `du_urban_variable` table.

### Files

| File | Purpose |
|------|---------|
| `du_urban/main.py` | CLI entry point (thin wrapper that calls `calculate_du_statistics_v2.main()`) |
| `du_urban/calculate_du_statistics_v2.py` | Main calculation module: reads CalSim DV CSV, applies unit-aware CFS->TAF conversion, writes per-DU monthly and period-summary tables |

## M&I contractor statistics

The `mi/` module calculates delivery and shortage statistics for SWP (State Water Project) and CVP (Central Valley Project) M&I contractors.

### CalSim variable naming convention

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
D_ESB408_DESRT       <- Total deliveries (all types)
D_ESB408_DESRT_PMI   <- Table A M&I allocation only (what we track)
D_ESB408_DESRT_PIN   <- Interruptible/Article 21
D_ESB408_DESRT_PCO   <- Carryover from previous year
D_ESB408_DESRT_PRJ   <- Project total
```

### Why we use `_PMI` variables

We track `_PMI` (Project M&I) variables specifically, NOT total deliveries (`_PRJ`). This is intentional:

1. **Scenario Comparison**: COEQWAL scenarios compare SWP reliability. Table A allocations (`_PMI`) show how allocation policies affect contractors, while total deliveries include carryover and interruptible water that obscure policy impacts.

2. **Shortage Pairing**: Shortage variables (`SHORT_D_xxx_PMI`) are calculated against M&I demand. Using `_PRJ` delivery but `_PMI` shortage would produce inconsistent metrics.

3. **Model Intent**: The CalSim model tracks Table A allocations to measure SWP reliability. Zeros in `_PMI` during dry years are the model's way of showing "100% allocation cut" scenarios.

### Reference sources

The runtime variable mappings live in the `MI_CONTRACTOR_VARIABLES` dict in `calculate_mi_statistics.py`. That dict was assembled from:

| Source | Location | Content |
|--------|----------|---------|
| `swp_contractor_perdel_A.wresl` | CalSim model files (external) | Contractor delivery and PERDV allocation logic |
| `mi_contractor.csv` | `database/seed_tables/04_calsim_data/` | Contractor metadata (names, contracts) |

### Understanding zero values in percentiles

A `q0 = 0` for a contractor-month means that in at least one simulated year the contractor received zero Table A M&I allocation for that month. This is legitimate model behavior, not a data error. For the general reading of zeros and percentile bands, see [Reading the outputs](#reading-the-outputs-percentile-bands-and-exceedance).

**Example, Coachella Valley WD (CCHLA):**
```
Month 1 (Oct): q0=0, q10=30, q50=133, avg=126 TAF
Month 7 (Apr): q0=0.4, q10=4, q50=18, avg=24 TAF
```

Interpretation: In October during dry years, Table A allocations can be cut to 0%. In April (spring), even dry years get some water (minimum 0.4 TAF).

### Alternative: total deliveries

If you need "total water received regardless of allocation type," use `D_{loc}_{contractor}` or `D_{loc}_{contractor}_PRJ` variables instead. However:

- This would require modifying `MI_CONTRACTOR_VARIABLES` in `calculate_mi_statistics.py`
- Shortage metrics would need recalculation or removal
- The interpretation changes from "allocation reliability" to "total supply"

### Unit conversion and demand

The module reads deliveries, shortages, and PERDV allocation fractions from a single CalSim DV output CSV. Conversion to TAF is unit-aware: each referenced column is converted using its header-declared unit (see [Unit conversion: CFS to TAF](#unit-conversion-cfs-to-taf) for the precise factor).

| Input | Declared unit | Handling |
|-------|---------------|----------|
| Delivery variables (`D_*_PMI`) | CFS | Converted: `TAF = CFS × DaysInMonth × CFS_TO_TAF_PER_DAY` using actual calendar days |
| Shortage variables (`SHORT_*_PMI`) | CFS | Converted the same way when the header declares CFS, kept as-is when already TAF |
| PERDV allocation fractions (`PERDV_SWP_*`) | dimensionless | Not converted |

Demand is computed, not read from a demand variable:

- SWP contractors with PERDV variables: `demand = sum_i (delivery_i + shortage_i) / PERDV_i`, following V3 `DataExtraction.py`.
- MWD (`demand_mode = "table_a"`): a fixed Table A contract of `MWD_TABLE_A_ANNUAL_TAF` (1911.5 TAF/yr, defined in `units.py`).
- Aggregate rollups: no demand computed.

After conversion the module runs `check_post_conversion_magnitude` to flag implausible values, so a future units regression is caught before it reaches the database.

### Files

| File | Purpose |
|------|---------|
| `mi/calculate_mi_statistics.py` | Main calculation module |
| `mi/MI_CONTRACTOR_VARIABLES` | Built-in variable mappings (dict in code) |

## Agricultural demand unit statistics

The `ag/` module calculates demand, delivery, pumping, and shortage statistics for 144 agricultural demand units.

### Data source: DV output only

All AG variables come from a single file, the CalSim DV output CSV (`{scenario}_coeqwal_calsim_output.csv`). The SV input CSV is **not** loaded.

| CalSim Variable | Description | Raw Unit | Conversion |
|-----------------|-------------|----------|------------|
| `AW_{DU_ID}` | Applied Water = **Demand** | CFS | `TAF = CFS × days × 0.001983471` |
| `DN_{DU_ID}` | Net Delivery = **Surface Water Delivery** | CFS | same |
| `GP_{DU_ID}` | Groundwater Pumping | CFS | same |
| `SHRTG_{DU_ID}` | Shortage (Sacramento, kind=`SHORTAGE`) | CFS | same |
| `GW_SHORT_{DU_ID}` | GW Restriction Shortage (SJR/Tulare, kind=`GW-RESTRICT-SHORT`) | CFS | same |
| `DEL_SWP_PAG`, `SHORT_CVP_PAG`, … | Project-level aggregate delivery / shortage | CFS | same |

### Water balance

The CalSim 3 demand-unit water balance is expressed by the `meetAW` goal in `constraints-Deliveries.wresl`. The shortage slack and the `GPmax` bound are toggled per demand unit and per scenario (see the per-unit notes below). For most Sacramento ag (`_NA`) units in both retained runs (`s0098`, `s0107`) the goal is solved without the slack:

```
AW + RP = DN + GP + RU
```

| Variable | Meaning | WRESL definition |
|---|---|---|
| **AW** | Applied Water (crop demand) = AWR + AWO | timeseries input (CFS, converted from TAF) |
| **RP** | Riparian and misc ET = AW × RPF | typically 5 to 15% of AW |
| **DN** | Net Delivery = DG minus DL (gross diversion minus conveyance losses) | DL = EV + DP + LF + OS |
| **GP** | Groundwater Pumping | decision variable, lower-bounded by GPmin |
| **RU** | Reuse = min(TW, RUFR×AWR + RUFO×AWO) | bounded by available tailwater |
| **SHRTG** | SHRTG (Sac) or GW_SHORT (SJR/Tulare) | standalone SHORTAGE decision variable (declared in `arcs-Deliveries.wresl`). Commented out of the `meetAW` goal for Sacramento ag (`_NA`) units, but active for refuge (`_PR`/`_NR`) units in both runs and for SJR/Tulare ag in `s0107` |

Source: the `meetAW_02_NA` goal in `s0107_adjBL_cqlTAI_wTUCP/Run/System/SystemTables_Sac/constraints-Deliveries.wresl` (the `!` starts a WRESL comment, so the `SHRTG` term is commented out):
```
goal meetAW_02_NA  {AW_02_NA + RP_02_NA = DN_02_NA + GP_02_NA + RU_02_NA } ! + SHRTG_02_NA }
```

For GW-only DUs (no surface delivery), DN drops out:
```
goal meetAW_07N_NA  {AW_07N_NA + RP_07N_NA = GP_07N_NA + RU_07N_NA } ! + SHRTG_07N_NA }
```

The shortage variable is still declared and written to the DV output (`define SHRTG_02_NA {std kind 'SHORTAGE' units 'CFS'}` in `arcs-Deliveries.wresl`), which is what the ETL reads. The `02_NA` example above is a Sacramento ag unit, where the slack is commented out. Refuge (`_PR`/`_NR`) units instead carry an active `+ SHRTG` term in both runs, and SJR/Tulare ag carries an active `+ GW_SHORT` term in `s0107` (absent in `s0098`). **Open question for the WAM team:** because slack activation is per-unit and per-scenario, confirm which demand units the pipeline runs should treat as carrying a real shortage slack, so the shortage definition is unambiguous.

In the retained runs the GP maximum bound (`setGPmax`) is commented out for most demand units, leaving the minimum bound (`setGPmin`) active throughout, expressed per applied-water component:

```
GP > GPmin * AWo * (1 + RPF - RUFo) + GPmin * AWr * (1 + RPF - RUFr)
```

The matching `setGPmax` goal carries the same `(1 + RPF - RUF)` factor, which exceeds 1.0 when RPF > RUF, so GP can exceed AW to also supply riparian losses (RP). GP/AW ratios of 1.0 to 1.15 in the s0020 dry run are consistent with this. `setGPmax` is active for a subset of units (16 goals in `s0098`, 76 in `s0107`) and commented out for the rest. **Open question for the WAM team:** confirm the intended GP upper-bounding per unit for the pipeline runs.

**Note:** The WRESL water balance IS the same for refuge DUs (`AW + RP = DN + GP + RU + SHORTAGE`), but the V3 notebooks never use GP for refuge DUs. The AG module filters refuge DUs out before running water balance checks.

### WRESL verification (March 2026)

All ETL equations were verified against the CalSim 3 WRESL model files.

| Variable | WRESL Declaration | Kind | Units | ETL Handling |
|----------|------------------|------|-------|-------------|
| `AW_{DU}` | `std` dvar, AW = AWR + AWO (+AWW) | `APPLIED-WATER` | CFS | Correct |
| `DN_{DU}` | `std` dvar, DN = DG - DL | `SW-DELIVERY-NET` | CFS | Correct |
| `GP_{DU}` | `std` dvar | `GW-PUMPING` | CFS | Correct |
| `SHRTG_{DU}` | `std` dvar (Sacramento) | `SHORTAGE` | CFS | Added |
| `GW_SHORT_{DU}` | bounded dvar (SJR/Tulare) | `GW-RESTRICT-SHORT` | CFS | Correct |
| `RU_{DU}` | `std` dvar | `REUSE` | CFS | Not directly used |
| `S_{code}` | `std` dvar | `STORAGE` | **TAF** | No conversion needed |
| `C_{code}_FLOOD` | `std` dvar | `SPILL` | CFS | Correct |
| `D_*_PMI` | `std` dvar | `FLOW-DELIVERY` | CFS | Correct |
| `SHORT_D_*_PMI` | `alias` (post-solve) | `delivery-shortage` | CFS | Correct |
| `PERDV_SWP_*` | `alias` of perdel_N | `swp-output` | percent (fraction 0-1) | Not converted |
| `DEL_*` aggregates | `alias` | `delivery-cvp/swp` | CFS | Correct |
| `UD_{DU}` | `timeseries` (SV input) | `URBAN-DEMAND` | TAF to CFS | (used by DU Urban) |

Key findings:

- Urban water balance is simpler than AG: `UD = DN + GP + SHRTG` (no RP, no RU)
- `nod_ag` / `sod_ag` are valid ETL-only aggregations (no WRESL equivalent)
- MWD Table A = 1911.5 TAF/yr confirmed from V3 DataExtraction.py
- MI demand formula `(delivery + shortage) / PERDV` is algebraically correct per WRESL

### AWO vs AW: demand variable choice

The SV input CSV contains `AWO_*` (Applied Water Order), the pre-model demand *target*. The DV output contains `AW_*` (Applied Water), the model's optimised water application. `AWO > AW` in most months because the model may not fully meet the order.

The COEQWAL V3 notebook (`DataExtraction.py`) uses `AW_*` from the DV output as the demand variable for agricultural demand units. This ETL follows that convention. The switch from `AWO_*` (SV) to `AW_*` (DV) was made in March 2026.

### Groundwater-only demand units

17 DUs have no `DN` term in their WRESL `meetAW` constraint, so their entire supply is GP + RU. CalSim does not produce a `DN_*` output for them, so the ETL synthesises surface water delivery as `GP + RU` for the `GW_ONLY_DU_IDS` set in `ag/calculate_ag_statistics.py`, matching V3's `SW_DELIVERY-NET`.

| Region | GW-only DUs |
|--------|-------------|
| Sacramento (9) | `06_NA`, `07N_NA`, `07S_NA`, `15N_NA1`, `15S_NA1`, `16_NA1`, `17N_NA`, `20_NA2`, `26N_NA` |
| SJR/Tulare (8) | `60S_NA1`, `60S_NA2`, `61_NA1`, `62_NA1`, `63_NA1`, `64_NA1`, `72_NA2`, `73_NA` |

Note: `26S_NA` is commented out in WRESL (moved to Lower Mokelumne system).

V3 `DataExtraction.py` lists 11 of these (the 9 Sacramento DUs plus `60S_NA1` and `60S_NA2`) and computes a synthetic `DN = GP + RU` labelled `SW_DELIVERY-NET`, dropping the GP and RU columns as intermediates.

### Shortage: two variable families by region

The WRESL model defines shortage as the slack variable in the `meetAW` water balance constraint. Two naming conventions exist by region:

| Region | Variable | WRESL kind tag | Columns in DV |
|--------|----------|---------------|---------------|
| Sacramento (WBAs 02-26) | `SHRTG_{DU_ID}` | `SHORTAGE` | ~185 |
| SJR/Tulare (WBAs 50-91) | `GW_SHORT_{DU_ID}` | `GW-RESTRICT-SHORT` | ~89 |

Both represent the same concept, unmet demand after DN, GP, and RU. The ETL detects the correct variable for each DU based on its WBA.

### Files

| File | Purpose |
|------|---------|
| `ag/calculate_ag_statistics.py` | Main calculation module |
| `ag/main.py` | CLI entry point |

## Wildlife refuge statistics

The `refuge/` module calculates delivery, shortage, and reliability statistics for the 18 wildlife refuge demand units defined in CalSim 3, covering environmental water deliveries to refuges and wetland areas in the Sacramento and San Joaquin hydrologic regions.

Four metrics are computed for each refuge demand unit:

| Metric | Unit | Temporal | Statistics |
|--------|------|----------|------------|
| Surface water delivery | TAF | Monthly (water months 1-12) and annual | Monthly percentile bands, monthly mean and CV, annual avg and CV |
| Delivery shortage | TAF | Monthly and annual | Monthly percentile bands, annual avg and CV |
| Delivery shortage | % of demand | Monthly and annual | Monthly percentile bands, monthly avg and CV, annual avg and CV |
| Delivery reliability | % (95th pct) | Period of record | Single value per DU per scenario |

**Reliability definition:** the 95th percentile of annual shortage %. In 95 of 100 simulated years the demand unit's shortage is at or below this value. A value of 0% means no shortage in 95% of years. A value of 50% means even in a normal year the DU is chronically under-supplied. This method has not been formally validated, see open questions below.

### Data source: DV output only

All variables are loaded from the DV output CSV:

- `AW_*`: demand (Applied Water, the model's optimised applied water). `AWO_*` in the SV input is the pre-model demand order, a different and higher quantity, and is not used.
- `DN_*`: delivery (Part C `SW-DELIVERY-NET` for Sacramento, `SW_DELIVERY-NET` for SJR/Tulare). The loader selects columns by Part B variable name, so the Part C hyphen/underscore difference does not affect selection.
- `SHRTG_*` (Sacramento `_PR` DUs) / `GW_SHORT_*` (SJR/Tulare `_PR` DUs): shortage.

### Unit conversion

Demand and delivery are in CFS and are converted to TAF using the shared precise factor (see [Unit conversion: CFS to TAF](#unit-conversion-cfs-to-taf)).

### DSS date convention: period-beginning vs period-ending

CalSim DSS files use two month-labelling conventions, and both must map to the same calendar month before any merge or calculation:

| File | Convention | Example date | Actual data period |
|------|------------|--------------|--------------------|
| SV input (`coeqwal_sv_input.csv`) | Period-beginning | `1920-11-01` | October 1920 (WM=1) |
| DV output (`coeqwal_calsim_output.csv`) | Period-ending | `1921-10-31` | October 1921 (WM=1) |

The SV file stamps each row with the first day of the following month, and the DV file stamps the last day of the current month. A naive date-string join between SV and DV produces zero matching rows. The ETL normalisation (`add_water_year_month`) detects period-beginning rows (`day == 1`) and shifts them back one day before deriving `WaterYear`, `WaterMonth`, and `DaysInMonth`. The raw CSV values are left unchanged.

### Shortage: model variables preferred

The WRESL model defines the same `meetAW` constraint for refuge DUs as for AG DUs. Shortage variables exist for all 18 refuge DUs in the DV output:

| Region | Variable | DUs |
|--------|----------|-----|
| Sacramento | `SHRTG_{DU_ID}` | 08N_PR1, 08N_PR2, 08S_PR, 09_PR, 11_PR, 17N_NR, 17N_PR, 17S_PR |
| SJR/Tulare | `GW_SHORT_{DU_ID}` | 63_PR1-3, 72_PR1-6, 91_PR |

The ETL uses these model-computed shortage values when available. If a DU's shortage column is missing, it falls back to `max(AW - DN, 0)`. `SHORTAGE_THRESHOLD_TAF = 0.1` filters floating-point precision artifacts from shortage frequency counts.

### Why GP is not used for refuge DUs

CalSim 3 does output `GP_*` columns for refuge DUs, and the WRESL water balance is identical to AG. However, the V3 notebook (`DataExtraction.py`) never uses GP or RU for any `_PR` DU, so the refuge module follows that convention. The `validate_water_balance` check is skipped for refuge DUs.

### DU overlap with the ag module

The DV output CSV contains `AW_*` columns for all demand unit types (ag, refuge, urban). The AG module filters its DU list against `du_agriculture_entity.csv` to avoid accidentally processing refuge DUs, which would produce incorrect results due to the different water accounting frameworks.

### Output tables

| Table | Contents |
|-------|----------|
| `refuge_du_delivery_monthly` | Monthly delivery percentile bands, one row per `(scenario, du_id, water_month)` |
| `refuge_du_shortage_monthly` | Monthly shortage bands in TAF and % of demand, plus `shortage_frequency_pct` |
| `refuge_du_period_summary` | Period-of-record annual delivery and shortage averages and CV, `reliability_pct_95`, and annual delivery exceedance curve |

### Demand unit reference

All 18 refuge demand units come from `du_refuge_entity.csv`. Type codes: `PR` = Project Refuge (CVP contract deliveries), `NR` = Non-project Refuge (water rights only). The `gw` and `sw` columns indicate groundwater and surface water access, which affects how a shortage should be read (a GW-capable unit has a fallback supply that an SW-only unit does not).

Sacramento River hydrologic region (CalSim 3 Main Report Table 3-9):

| DU_ID | Refuge / Wildlife area | Managed by | Water provider | GW | SW |
|-------|------------------------|------------|----------------|----|----|
| `08N_PR1` | Sacramento NWR | USFWS | Reclamation | no | yes |
| `08N_PR2` | Delevan NWR | USFWS | Reclamation | no | yes |
| `08S_PR` | Colusa NWR | USFWS | Reclamation | no | yes |
| `09_PR` | Llano Seco, Upper Butte Basin WA, Sacramento River NWR | CDFW, USFWS | Water rights | no | yes |
| `11_PR` | Upper Butte Basin WA (Little Dry Creek and Howard Slough) | CDFW | Western Canal WD, Richvale ID | yes | yes |
| `17N_NR` | Butte Sink Duck Clubs | Private, USFWS | Water rights, Western Canal WD | no | yes |
| `17N_PR` | Gray Lodge WA | CDFW | Reclamation, DWR (by exchange) | yes | yes |
| `17S_PR` | Sutter NWR | USFWS | Reclamation, Sutter Extension WD | no | yes |

San Joaquin River and Tulare Lake hydrologic regions (Table 3-10):

| DU_ID | Refuge / Wildlife area | Managed by | Water provider | GW | SW |
|-------|------------------------|------------|----------------|----|----|
| `63_PR1` | Arena Plains and Snow Bird units, Merced NWR | USFWS | (drainage water) | no | yes |
| `63_PR2` | Merced and Lone Tree Units, Merced NWR | USFWS | Reclamation | yes | yes |
| `63_PR3` | East Bear Creek Unit, San Luis NWR | USFWS | Reclamation | yes | yes |
| `72_PR1` | Volta WA | CDFW | Reclamation | no | yes |
| `72_PR2` | Kesterson NWR, Freitas and Blue Goose Units (San Luis NWR) | USFWS | Reclamation | yes | yes |
| `72_PR3` | San Luis Unit and West Bear Creek Unit, San Luis NWR | USFWS | Reclamation | no | yes |
| `72_PR4` | Los Banos WA and three North Grassland WA units | CDFW | Reclamation | yes | yes |
| `72_PR5` | Grassland WD north | Private | Reclamation | no | yes |
| `72_PR6` | Grassland WD south | Private | Reclamation | no | yes |
| `91_PR` | Mendota WA | CDFW | Reclamation water rights | no | yes |

`91_PR` is in the Tulare Lake region but diverts from Fresno Slough, and has no GIS geometry in the database.

### Open questions

- **Reliability method:** `reliability_pct_95 = np.percentile(annual_shortage_pct, 95)` has not been validated against a published standard or prior COEQWAL analysis. Alternatives include the fraction of years with zero shortage, the fraction below a threshold, or a 95% exceedance value on annual delivery. Confirm the intended definition with the modeling team before using it in the interface.
- **`17N_NR` inclusion:** This is the only non-project (NR) unit and represents private Butte Sink duck clubs. Confirm whether it belongs in refuge delivery reporting or should be treated separately.
- **Zero-demand months:** Some DUs have seasonal-only demand. When `demand_taf == 0` the ETL sets `shortage_pct = 0`.
- **PR/NR lookup table:** `cs3_type` is stored as a plain string with no lookup table. Consider a `du_refuge_type` lookup if the frontend needs descriptive labels.

### Files

| File | Purpose |
|------|---------|
| [`refuge/README.md`](refuge/README.md) | CalSim reference: delivery variable inventory, point of diversion / conveyance, composition notes, entity-data location |
| `refuge/calculate_refuge_statistics.py` | Main calculation module |
| `refuge/main.py` | CLI entry point |

---

## Environmental river flow statistics

The `env_flows/` module computes three flow-alteration metrics for 59 river channel reaches in the CalSim DV output, covering streams, reservoir releases, and conveyance canals. The metrics characterize how much CalSim-modeled water management alters natural river hydrology and how well environmental flow requirements are met.

| Metric | Unit | Temporal | What it measures |
|--------|------|----------|------------------|
| River flows, % unimpaired | % | Monthly | Simulated flow as a percent of natural unimpaired flow |
| River flows, % functional flows | % | Seasonal (5 CEFF seasons) | Seasonal flow as a percent of the prescribed functional-flow target |
| Flow alteration index | Pearson r | Period of record | Correlation between simulated and unimpaired monthly flow |

### Data sources

Both the DV output and SV input CSVs are used (both already staged per scenario):

- DV output (`*_coeqwal_calsim_output.csv`): `C_{reach}` simulated channel flow (Part C `CHANNEL`) and `C_{reach}_MIF` model-computed binding minimum instream flow (Part C `FLOW-MIN-INSTREAM`).
- SV input (`*_coeqwal_sv_input.csv`): `EFLOWS_{reach}` functional-flow target (Part C `FLOW-MIN-EFLOW`) and `UNIMP_{watershed}` natural unimpaired flow (Part C `FLOW-UNIMPAIRED`). Do not use the `UNIMP_*_UHH` variants, which are a different upper-half-hydrology baseline.

All variables are in CFS. Ratio metrics keep CFS so units cancel. Where a volume is reported it uses the shared CFS-to-TAF factor (see [Unit conversion: CFS to TAF](#unit-conversion-cfs-to-taf)).

`C_{reach}_MIF` (from the DV) is the total binding minimum instream flow that combines D-1641, VAMP, biological opinions, EFLOWS, and other regulatory minimums into one enforced floor, and it varies across scenarios. `EFLOWS_{reach}` (from the SV) is the prescribed functional-flow target only and is fixed per SV version. `C_SAC122` appears twice in the DV (use the first occurrence), and `C_SAC000_MIF` is absent from the DV.

### Reach inventory

59 channels are attributed in `channel_entity` with `channel_class IS NOT NULL` (47 stream reaches, 7 reservoir releases, 5 conveyance canals). The data explorer exposes four subsets driven by `channel_entity` flags:

| Filter | Flag | Count | Description |
|--------|------|-------|-------------|
| Stream reaches | `channel_class = 'stream'` | 47 | Natural river channels, excluding reservoir releases and canals |
| EFLOWS streams | `has_eflows = true` | 17 | Streams with a prescribed functional-flow target, the CEFF monitoring set |
| MIF streams | `has_mif = true` | 20 | Streams with a binding MIF companion variable in the DV |
| All channels | (no filter) | 59 | Complete reach set including reservoir releases and canals |

The Sacramento mainstem uses two unimpaired references split at Bend Bridge (rm 257): `UNIMP_SHAS` above (SAC_UPPER), `UNIMP_SRBB` at and below (SAC_LOWER). The Mokelumne has no `UNIMP_MOK` variable, so `unimp_sv_variable` is NULL for MOK019 and MOK028 and metrics 1 and 3 cannot be computed there (metric 2 against `EFLOWS_MOK028` still can).

The structured attribution (watershed, `unimp_sv_variable`, `has_mif`, `has_eflows`, `channel_class`) lives in `channel_entity`. The two tables below reproduce the curated reach inventory for the EFLOWS and MIF subsets with human-readable gauge locations.

#### EFLOWS reaches (17), the tier and CEFF set

These have `has_eflows = true` and are the denominator set for metric 2 (% functional flows) and the basis for metric 3 (flow alteration index).

| Reach | Location | Watershed | `UNIMP_*` | MIF? |
|-------|----------|-----------|-----------|------|
| `AMR004` | American River at I-80 Bridge | UPPER_AMERICAN | `UNIMP_FOLS` | yes |
| `FTR003` | Feather River | UPPER_FEATHER | `UNIMP_OROV` | yes |
| `FTR029` | Feather River at Yuba City | UPPER_FEATHER | `UNIMP_OROV` | yes |
| `MCD005` | Merced River at Stevinson | UPPER_MERCED | `UNIMP_ME` | yes |
| `MOK028` | Mokelumne River at Woodbridge | UPPER_MOKELUMNE | (none) | yes |
| `SAC000` | Sacramento River at Chipps Island | SAC_LOWER | `UNIMP_SRBB` | no |
| `SAC049` | Sacramento River at Freeport | SAC_LOWER | `UNIMP_SRBB` | yes |
| `SAC122` | Sacramento River at Tisdale Weir | SAC_LOWER | `UNIMP_SRBB` | yes |
| `SAC148` | Sacramento River at Colusa Weir | SAC_LOWER | `UNIMP_SRBB` | yes |
| `SAC257` | Sacramento River at Bend Bridge | SAC_LOWER | `UNIMP_SRBB` | yes |
| `SAC289` | Sacramento River at South Bonnieville | SAC_UPPER | `UNIMP_SHAS` | yes |
| `SJR070` | San Joaquin near Vernalis | SAN_JOAQUIN | `UNIMP_SJ` | yes |
| `SJR127` | San Joaquin at Salt Slough | SAN_JOAQUIN | `UNIMP_SJ` | yes |
| `STS011` | Stanislaus River | UPPER_STANISLAUS | `UNIMP_ST` | yes |
| `TRN111` | Trinity River at Lewiston | TRINITY_RIVER | `UNIMP_TRIN` | yes |
| `TUO003` | Tuolumne River | UPPER_TUOLUMNE | `UNIMP_TU` | yes |
| `YUB002` | Yuba River at Marysville | YUBA_RIVER | `UNIMP_YUBA` | yes |

`SAC000` (Chipps Island) has an EFLOWS target but no MIF, because `C_SAC000_MIF` is absent from the DV (it does have `EFLOWS_SAC000` in the SV).

#### MIF reaches (20), the binding minimum-instream-flow set

These have `has_mif = true` (a `C_{reach}_MIF` companion in the DV). The set is the 17 EFLOWS reaches minus `SAC000` (no MIF) plus four additional non-EFLOWS streams: `FTR059`, `KSWCK`, `NTOMA`, `STS059`.

| Reach | Location | Watershed | `UNIMP_*` | EFLOWS? |
|-------|----------|-----------|-----------|---------|
| `AMR004` | American River at I-80 Bridge | UPPER_AMERICAN | `UNIMP_FOLS` | yes |
| `FTR003` | Feather River | UPPER_FEATHER | `UNIMP_OROV` | yes |
| `FTR029` | Feather River at Yuba City | UPPER_FEATHER | `UNIMP_OROV` | yes |
| `FTR059` | Feather River at Thermalito Afterbay | UPPER_FEATHER | `UNIMP_OROV` | no |
| `KSWCK` | Keswick Dam (Sacramento below Shasta) | SAC_UPPER | `UNIMP_SHAS` | no |
| `MCD005` | Merced River at Stevinson | UPPER_MERCED | `UNIMP_ME` | yes |
| `MOK028` | Mokelumne River | UPPER_MOKELUMNE | (none) | yes |
| `NTOMA` | American River at Lake Natoma | UPPER_AMERICAN | `UNIMP_FOLS` | no |
| `SAC049` | Sacramento River at Freeport | SAC_LOWER | `UNIMP_SRBB` | yes |
| `SAC122` | Sacramento River at Tisdale Weir | SAC_LOWER | `UNIMP_SRBB` | yes |
| `SAC148` | Sacramento River at Colusa Weir | SAC_LOWER | `UNIMP_SRBB` | yes |
| `SAC257` | Sacramento River at Bend Bridge | SAC_LOWER | `UNIMP_SRBB` | yes |
| `SAC289` | Sacramento River at South Bonnieville | SAC_UPPER | `UNIMP_SHAS` | yes |
| `SJR070` | San Joaquin near Vernalis | SAN_JOAQUIN | `UNIMP_SJ` | yes |
| `SJR127` | San Joaquin at Salt Slough | SAN_JOAQUIN | `UNIMP_SJ` | yes |
| `STS011` | Stanislaus River | UPPER_STANISLAUS | `UNIMP_ST` | yes |
| `STS059` | Stanislaus River (upper) | UPPER_STANISLAUS | `UNIMP_ST` | no |
| `TRN111` | Trinity River at Lewiston | TRINITY_RIVER | `UNIMP_TRIN` | yes |
| `TUO003` | Tuolumne River | UPPER_TUOLUMNE | `UNIMP_TU` | yes |
| `YUB002` | Yuba River at Marysville | YUBA_RIVER | `UNIMP_YUBA` | yes |

The remaining 40 channels (Sacramento mainstem nodes, other tributaries, reservoir releases, canals) are computed for metric 1 where a `UNIMP_*` variable is available. For the full machine-readable list, query `channel_entity WHERE channel_class IS NOT NULL`.

### Calculations

Metric 1 (% unimpaired), per timestep and aggregated per water month across years, for reaches with a `unimp_sv_variable`:

```python
pct_unimpaired = C_{reach}[CFS] / UNIMP_{watershed}[CFS] * 100   # NULL when UNIMP == 0
```

Metric 2 (% functional flows), per CEFF season and water year, for reaches with `has_eflows = true`, using the 5-season CEFF calendar (water year months, Oct = 1):

```python
pct_ff = C_{reach}[CFS] / EFLOWS_{reach}[CFS] * 100   # NULL when EFLOWS == 0
deviation_avg = pct_ff_avg - 100.0                    # negative means below target
```

Metric 3 (flow alteration index), over the full monthly record, for the same reaches as metric 1:

```python
from scipy.stats import pearsonr
r, p_value = pearsonr(C_{reach}_series, UNIMP_{watershed}_series)
```

The period summary also reports `mif_met_pct` (percent of months where `C >= MIF`), `avg_pct_unimpaired`, and `avg_pct_ff`. Percentile bands (`q*`) and exceedance percentiles (`exc_p*`) follow the [shared conventions](#statistics-we-compute) and the [Reading the outputs](#reading-the-outputs-percentile-bands-and-exceedance) section.

The flow alteration index uses `scipy.stats.pearsonr`. SciPy is an optional dependency (commented out in `requirements.txt`), and without it this one metric is skipped while everything else runs. Replacing it with a NumPy correlation so the metric never depends on an optional package is a known cleanup item.

### Output tables

| Table | Contents |
|-------|----------|
| `env_flow_channel_monthly` | Per `(reach, scenario, water_month)`: raw flow and unimpaired CFS, `pct_unimpaired` |
| `env_flow_channel_seasonal` | Per `(reach, scenario, season)`: seasonal flow bands, `pct_unimpaired_*`, `pct_ff_*`, `deviation_avg`, `target_met_pct` |
| `env_flow_channel_period_summary` | Per `(reach, scenario)`: `pearson_r`, `p_value`, `mif_met_pct`, `avg_pct_unimpaired`, `avg_pct_ff` |

Season definitions are seeded in `env_flow_season`. The dry season spans the water-year boundary, so October is grouped with the preceding water year's dry season.

### Data quality notes

- MIF variables are absent from some scenarios (for example s0039-s0042 carry 7 of 20). This reflects different regulatory frameworks per scenario, not a pipeline error. The ETL writes NULL `pct_mif_*` for those reaches. The full per-scenario breakdown is in [Per-scenario variable availability](#per-scenario-variable-availability-snapshot) below.
- EFLOWS targets are SV inputs. s0011 has none (pre-EFLOWS baseline), and s0029/s0030 carry only `EFLOWS_STS011` (see open question below).
- `pct_unimpaired` can far exceed 100% for heavily regulated reaches near zero natural flow, which is physically valid. Values up to ~100,000% occur, so migration 27 widened the affected columns from `NUMERIC(8,3)` to `NUMERIC(12,3)`.

### Per-scenario variable availability (snapshot)

Snapshot from the 19-scenario `env_flows` run (2026), recording which scenarios are missing which `C_*_MIF` (DV) and `EFLOWS_*` (SV) variables. Absent variables are expected: they reflect different regulatory frameworks per scenario, not a pipeline error. The ETL writes NULL `pct_mif_*` / `pct_ff_*` for the affected reaches. This is a point-in-time snapshot and drifts as scenarios are added or re-run, so regenerate it by reading the DV/SV CSV headers (Part C `FLOW-MIN-INSTREAM` for MIF, the `EFLOWS_*` columns for targets) per scenario rather than trusting the table below. All 59 `C_{reach}` channel-flow variables are present in every scenario, so no channel-flow data is missing.

**MIF variable availability (20 expected):**

| Scenario(s) | MIF present / 20 | Missing variables |
|---|---|---|
| s0020, s0021, s0025-s0028, s0029, s0030, s0031-s0033, s0044 | 20 / 20 | (none) |
| s0039-s0042 | 7 / 20 | `C_FTR029_MIF`, `C_MCD005_MIF`, `C_MOK028_MIF`, `C_SAC049_MIF`, `C_SAC122_MIF`, `C_SAC148_MIF`, `C_SAC289_MIF`, `C_SJR070_MIF`, `C_SJR127_MIF`, `C_STS011_MIF`, `C_TRN111_MIF`, `C_TUO003_MIF`, `C_YUB002_MIF` |
| s0011 | 8 / 20 | Same 13 as s0039-s0042 except `C_STS011_MIF` is present |
| s0023, s0024 | 6 / 20 | Same as s0011 plus `C_SAC257_MIF` and `C_STS011_MIF` |

**EFLOWS (functional-flow target) availability (17 expected):**

| Scenario(s) | EFLOWS present | Notes |
|---|---|---|
| s0020, s0021, s0023-s0028, s0031-s0033, s0039-s0042, s0044 | All 17 | Full EFLOWS suite (28 SV columns) |
| s0011 | None | Pre-EFLOWS baseline (12 SV columns, no functional-flow targets) |
| s0029, s0030 | 1 of 17 (`EFLOWS_STS011` only) | 12 SV columns. The other 16 EFLOWS targets are absent, so `pct_ff_*` is NULL for those reaches. See the s0029/s0030 EFLOWS coverage open question below. Absent: `EFLOWS_AMR004`, `EFLOWS_FTR003`, `EFLOWS_FTR029`, `EFLOWS_MCD005`, `EFLOWS_MOK028`, `EFLOWS_SAC000`, `EFLOWS_SAC049`, `EFLOWS_SAC122`, `EFLOWS_SAC148`, `EFLOWS_SAC257`, `EFLOWS_SAC289`, `EFLOWS_SJR070`, `EFLOWS_SJR127`, `EFLOWS_TRN111`, `EFLOWS_TUO003`, `EFLOWS_YUB002` |

### Open questions

- **s0029/s0030 EFLOWS coverage:** Both have complete DV output but their SV contains only `EFLOWS_STS011`, so `pct_ff_*` is NULL for 16 of 17 EFLOWS reaches. Confirm with the modeling team whether this is intentional.
- **59 vs 60 channels:** The planning estimate was 60 channels but `channel_entity` has 59 with `channel_class` set. Confirm the true count by reading the raw DV header and counting `CHANNEL` occurrences in the Part C row.

### Files

| File | Purpose |
|------|---------|
| [`env_flows/README.md`](env_flows/README.md) | CalSim reference: CEFF season catalog, Sacramento mainstem unimpaired-flow split, resolved questions |
| `env_flows/calculate_env_flow_statistics.py` | Main calculation module |
| `env_flows/main.py` | CLI entry point |

---

## Modules not yet documented

CWS aggregate, Delta, and Sensitivity have working ETL code and appear in [Modules at a glance](#modules-at-a-glance), [Statistics we compute](#statistics-we-compute), and [Data sources by module](#data-sources-by-module), but they do not yet have a module deep-dive section here. Full module documentation (data sources, per-entity variable choices, water balance, quirks, and open questions) still needs to be written for each, ideally with the WAM team.

---

## Provenance and verification

Every location list and calculation in this pipeline traces back to the modeling team notebooks. This section records where each one came from and the result of a first review pass that compared the backend implementation against the readable source code.

**Sources of truth (what was compared against):**

- `coeqwal` repo: `notebooks/coeqwalpackage/metrics.py` and `notebooks/Metrics.ipynb` (plain JSON, readable).
- `COEQWAL_V3` repo: `coeqwalpackage/metrics.py`, `tier.py`, `DataExtraction.py`, and the location/grouping CSVs under `data/mappings/`, `data/variables/`, `data/tiers/`.

**Method and limits:** This was a read-and-compare review of formulas, constants, unit conversions, and location membership, not automated and not an exhaustive line-by-line audit.

- **Match:** backend formula/list agrees with the source within the tolerances the existing verifiers already use.
- **Discrepancy:** a real difference exists that a reviewer should resolve.
- **Needs review:** intentional-looking backend choice (often with a code comment) that diverges from the notebook and should be confirmed by the modeling team.
- **Unverifiable:** no readable notebook/Python source exists to compare against (backend-only extension or notebook logic only present in LFS-stubbed `.ipynb`).

Items marked Discrepancy, Needs review, or Unverifiable are summarized as a checklist in the [Needs-review backlog](#needs-review-backlog-modeling-team-decisions) at the end of this README.

### Calculations

| Calculation | Source of truth | Verdict | Note |
|---|---|---|---|
| CFS to TAF conversion | `metrics.py` `convert_cfs_to_taf` | Match | Backend uses `86400/43560000 ≈ 0.00198347`. Notebooks use rounded `0.001984`. ~0.027% drift, negligible. |
| Water-year definition (Oct start) | `metrics.py` `add_water_year_column` | Match | Both set `WaterYear = Year + 1` for months >= 10. |
| Reservoir flood-pool probability | `metrics.py` `frequency_hitting_level` | Match | Same `storage - threshold` with `+1e-6` epsilon. Only an exact-tie boundary differs. |
| Reservoir dead-pool probability | `metrics.py` `frequency_hitting_level` | Match | Backend computes direct `P(storage <= dead)` (`reservoir_metrics.py` `calculate_dead_pool_probability`). The notebook reaches the same quantity by inverting its flood-zone branch with `floodzone=False`: `100 - count(storage - dead > 0)` equals `count(storage <= dead)`, and the dead-pool branch adds no epsilon (`metrics.py` `frequency_hitting_level`). The two are equivalent at the boundary. |
| Reservoir CV | `metrics.py` `compute_cv` | Discrepancy | Backend adds guards (returns 0 when `|mean| <= 0.01`, uses `std/abs(mean)`, returns 0 when `cv > 99`). Notebook is raw `std/mean`. |
| Reservoir annual / monthly average | `metrics.py` `ann_avg`, `mnth_avg` | Match | Mean of per-water-year monthly means. Matches. |
| Reservoir percentile bands (% capacity) | `V3 metrics.py` `compute_percent_of_capacity` | Discrepancy | Formula agrees when capacity is a fixed scalar. Backend uses fixed `CAPACITY_OVERRIDES`, the notebook path can use time-varying capacity columns, and the full `{0,10,30,50,70,90,100}` band set is not emitted by the notebook metrics loop. |
| Reservoir capacity overrides / level constants | `Metrics.ipynb` constants, `V3 DataExtraction.py` | Match (1 exception) | FOLSM 967, OROVL 3424.8, MELON 2420, MLRTN flood 524 / dead 135, MELON dead 80 all match. **TRNTY flood** uses `LEVEL4DV` in backend vs `LEVEL5DV` in the notebook (the TRNTY entry in `RESERVOIR_THRESHOLDS`, `reservoir_metrics.py`, has a rationale comment) - see Needs review. |
| Delta NDO (net Delta outflow) | `metrics.py` `ann_avg`, `Metrics.ipynb` | Discrepancy | Backend `annual_avg_taf` is an annual **sum** of monthly TAF (`calculate_delta_statistics.py` `calculate_delta_period_summary`). The notebook headline NDO is a **mean** of monthly CFS. Backend's separate `avg_cfs` does match the notebook quantity. |
| Delta X2 / EM / JP spring and fall avg + CV | `compute_metrics_suite` (V3 `metrics.py`) | Match | Same months ([3,4,5] spring, [9,10,11] fall) and variable names. |
| Delta RS/CO EC, Banks/Tracy EC, D-1641-style exceedance thresholds (450/900/1600/2500) | none found | Unverifiable | Backend-only relative to the readable notebook suite (`compute_metrics_suite` covers only EM/JP). No source for the exceedance thresholds. |
| M&I SWP demand via PERDV, MWD Table A = 1911.5 TAF/yr | `V3 DataExtraction.py` | Match | PERDV demand recovery `(D + SHORT)/PERDV` and the `1911.5` constant trace directly to the notebook code. |
| M&I contractors LROCK / MOJVE / DESRT / CCHLA | `V3 DataExtraction.py` | Unverifiable | Backend defines delivery/shortage (and a backend-only demand var for CCHLA) with no matching PERDV demand block in V3. |
| CWS aggregate CalSim variable names | `V3 DataExtraction.py` (notebook delivery logic) | Match | Aligns with the notebook delivery logic (including `DEL_CVP_PMI_N_WAMER`). Note: `data/variables/variable_groupings.csv` is a chart-grouping list, **not** the CWS aggregate definition source. |
| Ag AW (demand) / DN (SW delivery) / GP (GW pumping) / shortage variables | `V3 DataExtraction.py` | Match | Variable choices follow the documented WRESL balance. |
| Ag GW-only DN = GP + RU synthesis set (`GW_ONLY_DU_IDS`) | `V3 DataExtraction.py` | Needs review | Backend lists **17** DUs (`GW_ONLY_DU_IDS` in `calculate_ag_statistics.py`). V3 synthesizes **11**. Backend also has `26N_NA` where V3 uses `26S_NA` (possible ID typo). |
| % demand met, reliability, shortage-percentile methodology (urban/MI/ag/refuge) | `metrics.py` `ann_percentile`, `exceedance_metric` | Discrepancy | No delivery-family `% demand met` formula exists in the notebooks. Backend definitions are extensions and use a different percentile population (pooled months vs annual means) than `compute_iqr_value`. |
| Refuge demand source (per-DU `AW_*` from DV) | `V3 DataExtraction.py` | Discrepancy | V3 only aggregates annual `AWOANN_*` applied-water vars for refuge reporting. Per-refuge-DU demand is a backend construction. |
| Refuge shortage + `reliability_pct_95` | none found | Unverifiable | Backend-only. No refuge shortage/reliability logic in the readable notebook modules. |
| Env flows: % unimpaired, % functional flows, CEFF 5-season calendar, Pearson alteration index | none found | Unverifiable | No implementation of these in the readable `coeqwal`/`V3` `.py` sources. Logic, if it exists, is only in LFS-stubbed notebooks. |
| Env flows: MIF met % | `metrics.py` `probability_var1_gte_var2_for_scenario` | Discrepancy | Shares the `var1 >= var2` comparator, but the source returns a 0-1 probability and the backend stores a percent (x100). |

### Location lists

| Location list | Source of truth | Verdict | Note |
|---|---|---|---|
| Env-flow tier reaches (17) | `data/mappings/Eflows_Mapping.csv` | Match | `channel_entity.csv` `has_eflows` rows and `ENV_FLOWS.csv` columns equal the 17 mapping stations exactly. |
| Ag tier regions (`AG_REV`, 132) | `data/mappings/Agricultural_Mapping.csv` | Match | 132/132 identical IDs. |
| Reservoir tier set (`RES_STOR`, 8) | `data/variables/parallel_variable_groupings.csv`, `tier.py` | Match | NOD 4 + SOD 5 minus the aggregate `S_SLUIS`. |
| Single-value tiers (`DELTA_ECO`, `FW_DELTA_USES`, `FW_EXP`, `WRC_SALMON_AB`) | `data/tiers/tiers_descriptions.csv`, `tier.py` | Match | Loader location expansion (EM+JP, Banks+Jones, DETAW, SAC299) matches the V3 tier descriptions. |
| Urban tier DUs (`CWS_DEL` staging) | `data/mappings/DrinkingWater_Mapping.csv` | Discrepancy | 7 ID-level mismatches between the staging columns and the V3 drinking-water mapping. |
| Ag statistics entity list (`du_agriculture_entity.csv`, ~144) | `data/mappings/Agricultural_Mapping.csv` (132) | Discrepancy | Backend includes ~10 placeholder rows plus a few extra DUs. One V3 ID (`07S_PA`) is absent from the backend list. |
| Urban statistics entity list (`du_urban_entity.csv`, ~125) | `data/mappings/DrinkingWater_Mapping.csv` (78) | Discrepancy | Backend is the full urban seed (includes ~47 `_NU` groundwater-community DUs). The mapping is a curated drinking-water subset. Not a 1:1 comparison. |

### Automated verification

Reservoir statistics are verified end-to-end alongside every other section by [`etl/statistics/verify_all_sections.py`](verify_all_sections.py). See [`etl/verification/README.md`](../verification/README.md) for the full walkthrough, tolerances, and known notes (including the SLUIS monthly-average discrepancy carried forward from the original notebook comparison).

Two things to know about the verifier:

- **Two checks are skipped** (logged as warnings, not failures) because a data value the ETL loads is unconfirmed. It disagrees with the verifier's reference, and confirming it is a question for the Modeling Team: San Luis CVP/SWP `pct_capacity`, and the `GDPUD_NU` delivery variable. Background and how to resolve: see [Unconfirmed data values](#unconfirmed-data-values-two-verification-checks-skipped-meanwhile).
- **Tier checks are opt-in:** Tier results come from a separate ETL, so the tier sections only run with `--with-tiers`. The default stats run never touches them and exits clean for any scenario.

## Appendix: Variable reference (WRESL-verified, March 2026)

This table covers the variables the ETL modules read. It aims to be complete but is not guaranteed exhaustive, so confirm against the module code (and the WAM team) before relying on it as the sole source. All listed variable declarations were verified against CalSim 3 WRESL files.

### A. Master variable table (WRESL declarations)

| Variable | Type | Kind | Native Unit | DSS Unit | ETL Module(s) | Notes |
|----------|------|------|-------------|----------|---------------|-------|
| `AW_{DU}` | std | APPLIED-WATER | CFS | CFS | AG, Refuge | AW = AWR + AWO (+AWW) |
| `AWR_{DU}` | timeseries | APPLIED-WATER | TAF to CFS | CFS | (not used directly) | Rice applied water. Auto-converted by CalSim |
| `AWO_{DU}` | timeseries | APPLIED-WATER | TAF to CFS | CFS | (not used directly) | Other-crop applied water |
| `AWW_{DU}` | timeseries | APPLIED-WATER | TAF to CFS | CFS | (not used directly) | Wetlands. Only some DUs |
| `DN_{DU}` | std | SW-DELIVERY-NET / SW_DELIVERY-NET | CFS | CFS | AG, Refuge, DU Urban | Sac uses hyphen; SJR uses underscore in kind |
| `DG_{DU}` | std | SW-DELIVERY-GROSS | CFS | CFS | (not used) | DN = DG - DL |
| `DL_{DU}` | std | DELIVERY-LOSS | CFS | CFS | (not used) | Conveyance loss |
| `GP_{DU}` | std | GW-PUMPING | CFS | CFS | AG | Groundwater pumping |
| `RU_{DU}` | std | REUSE | CFS | CFS | (not used directly) | Reuse (part of balance) |
| `RP_{DU}` | std | RIPARIAN-MISC-ET | CFS | CFS | (not used directly) | RP = AW × RPF |
| `SHRTG_{DU}` | std | SHORTAGE | CFS | CFS | AG, Refuge | Sacramento region only |
| `GW_SHORT_{DU}` | std (bounded 0-99999) | GW-RESTRICT-SHORT | CFS | CFS | AG, Refuge | SJR/Tulare GW-only DUs. `@COEQWAL` addition, declared in the s0107 baseline run. Absent from non-COEQWAL runs such as USBR Alt3 (s0098), where the ETL derives a fallback |
| `UD_{DU}` | timeseries | URBAN-DEMAND | TAF to CFS | CFS | DU Urban | SV input, read in TAF from the SV CSV. Auto-converted by CalSim |
| `S_{code}` | std | STORAGE | **TAF** | **TAF** | Reservoir | Only native-TAF variable in the solver |
| `S_{code}level{N}` | value/timeseries | STORAGE-LEVEL | TAF | TAF | Reservoir | Flood control / dead pool levels |
| `C_{reach}` | std | CHANNEL | CFS | CFS | Env Flows | Channel flow / reservoir release |
| `C_{code}_Flood` | std | SPILL | CFS | CFS | Reservoir | Flood spill = C - C_NCF |
| `C_{code}_NCF` | std (bounded) | CHANNEL | CFS | CFS | (not used) | Normal channel flow ≤ release capacity |
| `C_{reach}_MIF` | std | FLOW-MIN-INSTREAM | CFS | CFS | Env Flows | Minimum instream flow requirement |
| `I_{code}` | timeseries | INFLOW | TAF to CFS | CFS | (not used) | Reservoir inflow |
| `E_{code}` | std | EVAPORATION | CFS | CFS | (not used) | Reservoir evaporation |
| `D_{node}_{contractor}_PMI` | std | FLOW-DELIVERY | CFS | CFS | MI | M&I delivery arc |
| `SHORT_D_{node}_{contractor}_PMI` | alias | delivery-shortage | CFS | CFS | MI | MI shortage (post-solve) |
| `DEL_SWP_MWD` | alias | delivery-swp | CFS | CFS | MI | MWD total delivery (5 arcs) |
| `DEL_SWP_PMI` / `_N` / `_S` | alias | delivery-swp | CFS | CFS | MI, CWS | SWP M&I aggregate |
| `DEL_CVP_PMI_N` / `_S` | alias | delivery-cvp | CFS | CFS | MI, CWS | CVP M&I aggregate |
| `DEL_CVP_PMI_N_WAMER` | alias | delivery-cvp | CFS | CFS | CWS | CWS `cvp_nod` primary delivery (incl. Western Area). Falls back to `DEL_CVP_PMI_N` when absent |
| `SHORT_SWP_PMI` / `_N` / `_S` | alias | delivery-shortage-swp | CFS | CFS | MI, CWS | SWP M&I aggregate shortage |
| `SHORT_CVP_PMI_N` / `_S` | alias | delivery-shortage-cvp | CFS | CFS | MI, CWS | CVP M&I aggregate shortage |
| `SHORT_CVP_PMI_N_WAMER` | alias | delivery-shortage-cvp | CFS | CFS | CWS | CWS `cvp_nod` primary shortage. Absent in the retained `s0098` run, so the ETL falls back to `SHORT_CVP_PMI_N` |
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
| `CO_EC_MONTH` | alias | SALINITY | **UMHOS/CM** | **UMHOS/CM** | Delta | Collinsville EC (compliance point) |
| `BANKSEC_MAX14DAY` | alias | SALINITY-APPROX | **UMHOS/CM** | **UMHOS/CM** | Delta | Banks Pumping Plant EC, 14-day max |
| `TRACYEC_MAX14DAY` | alias | SALINITY-APPROX | **UMHOS/CM** | **UMHOS/CM** | Delta | Tracy/Jones Pumping Plant EC, 14-day max |
| `UNIMP_{watershed}` | timeseries | FLOW-UNIMPAIRED | **TAF** | TAF or CFS | Env Flows | SV input. Names use abbreviations (SHAS, OROV) |
| `EFLOWS_{reach}` | timeseries | FLOW-MIN-EFLOW | TAF to CFS | CFS | Env Flows | SV input. Functional flow target |
| `taf_cfs` / `cfs_taf` | **built-in** | - | - | - | - | WRESL system functions. Not user-defined |

### B. Water balance equations (WRESL-verified)

| DU Type | WRESL Constraint | Equation | Shortage Variable |
|---------|-----------------|----------|-------------------|
| AG (with surface water) | `goal meetAW_{DU}` | `AW + RP = DN + GP + RU + SHORTAGE` | `SHRTG_` (Sac) / `GW_SHORT_` (SJR) |
| AG (GW-only, Sacramento, 9 DUs) | `goal meetAW_{DU}` | `AW + RP = GP + RU + SHORTAGE` (no DN) | `SHRTG_` |
| AG (GW-only, SJR/Tulare, 8 DUs) | `goal meetAW_{DU}` | `AW = GP + SHORTAGE` (no RP, no RU) | `GW_SHORT_` |
| Refuge (_PR) | `goal meetAW_{DU}` | `AW + RP = DN + GP + RU + SHORTAGE` | Same as AG by region |
| Urban (_PU/_SU) | `goal setUD_{DU}` | `UD = DN + GP + SHORTAGE` (no RP, no RU) | Same as AG by region |
| Urban (GW-only, _NU) | `goal setUD_{DU}` | `UD = GP + SHORTAGE` (no DN) | Same as AG by region |
| MI (SWP contractors) | (implicit) | `demand × taf_cfs × perdel = delivery + shortage` | `SHORT_D_*_PMI` (alias) |

These are accounting issues. The `goal meetAW_{DU}` constraint omits an active `SHRTG_` term for Sacramento ag (`_NA`) units in both runs (it is commented out), so shortage there is the residual. Refuge (`_PR`/`_NR`) units keep an active `SHRTG_` term, and the SJR/Tulare `GW_SHORT_` term is active in `s0107` (absent in `s0098`).

### C. CFS to TAF conversion factors

| Source | Factor | Code | Difference from exact |
|--------|--------|------|----------------------|
| **Exact** | `86400 / 43560 / 1000 = 0.001983471074...` | - | - |
| **ETL (`units.py`)** | `86400 / 43560000 = 0.00198347107438` | `CFS_TO_TAF_PER_DAY` | **Exact** (Python float division) |
| **V3 `cqwlutils.py`** | `0.0019834714` | hardcoded literal | 0.0000164% (negligible) |
| **V3 `metrics.py`** | `0.001984` | hardcoded literal | 0.027% (negligible) |
| **Old repo `AuxFunctions.py`** | `(86400/43560) * day / 1000` | computed per-row | **Exact** |

All implementations produce equivalent results for practical purposes.

### D. ETL module unit handling matrix

| Module | Uses `parse_dss_csv_header`? | Builds `units_map`? | Checks CSV header units? | CFS to TAF conversion? | `check_post_conversion_magnitude`? | Double-conversion risk? |
|--------|-------|-------|-------|-------|-------|-------|
| **AG** | Yes | Yes | Yes | Yes (CFS only) | Yes | None |
| **Refuge** | Yes | Yes | Yes | Yes (CFS only) | Yes | None |
| **MI** | Yes | Yes | Yes | Yes (CFS only, PERDV skipped) | Yes | None |
| **DU Urban** | No (own parser) | No | Reads names and Part C only | Yes (CFS only, UD demand already TAF) | No | None (fixed March 2026) |
| **Reservoir** | No (own parser) | No | Yes (own check) | N/A (storage is TAF) | N/A | None |
| **Env Flows** | No (own parser) | No | Yes (SV only) | Yes (volume output only) | No | None |
| **Delta** | No (own parser) | No (own dedup) | No | Yes (NDO only) | No | None |
| **CWS Aggregate** | Yes | Yes | Yes | Yes (unit-aware) | Yes | None (fixed March 2026) |

### E. GW-only demand units (no DN in WRESL, 17 total)

| Region | DU IDs | WRESL Balance Form |
|--------|--------|-------------------|
| Sacramento (9) | `06_NA`, `07N_NA`, `07S_NA`, `15N_NA1`, `15S_NA1`, `16_NA1`, `17N_NA`, `20_NA2`, `26N_NA` | `AW + RP = GP + RU + SHRTG` |
| SJR East (6) | `60S_NA1`, `60S_NA2`, `61_NA1`, `62_NA1`, `63_NA1`, `64_NA1` | `AW = GP + GW_SHORT` (no RP, no RU) |
| SJR West (2) | `72_NA2`, `73_NA` | `AW = GP + GW_SHORT` |
| Note | `26S_NA` is commented out in WRESL (moved to Lower Mokelumne) | - |

V3 `DataExtraction.py` lists 11 of these (06_NA through 60S_NA2) and computes `DN = GP + RU`.

### F. MI contractor PERDV mapping (WRESL-verified)

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

MI demand formula: `demand_TAF_per_month = Σ (D_i + SHORT_i) / PERDV_i` (per arc). MWD demand: hardcoded `1911.5 TAF/yr` (Table A contract).

### G. Reservoir capacity constants (WRESL-verified)

| Reservoir | WRESL `level6` / gross | V3 Hardcoded | ETL `CAPACITY_OVERRIDES` | Entity CSV |
|-----------|----------------------|-------------|-------------------------|------------|
| **SHSTA** | 4552 TAF | - (from DSS) | - (from entity CSV) | 4552 |
| **TRNTY** | 2447.65 TAF | - | - | 2448 |
| **OROVL** | 3424.8 TAF | **3424.8** | **3424.8** | 3537 |
| **FOLSM** | 967 TAF | **967** | **967** | 975 |
| **MLRTN** | - (~524) | **524** | **524** | 520 |
| **MELON** | - (~2420) | **2420** | **2420** | 2400 |

### H. Computed aggregates (ETL vs V3)

| Aggregate | ETL Formula | V3 Formula | Match? |
|-----------|-------------|------------|--------|
| `nod_ag` | `DEL_CVP_PAG_N + DEL_SWP_PAG_N + DEL_CVP_PSC_N` | `DEL_CVP_PAG_N + DEL_SWP_PAG_N + DEL_CVP_PSC_N` | Yes |
| `sod_ag` | `DEL_CVP_PAG_S + DEL_SWP_PAG_S + DEL_CVP_PEX_S` | `DEL_CVP_PAG_S + DEL_SWP_PAG_S + DEL_CVP_PEX_S` | Yes |
| `NOD_STORAGE` | (not computed) | `S_TRNTY + S_SHSTA + S_OROVL + S_FOLSM + S_NBLDB` | - |
| `SOD_STORAGE` | (not computed) | `S_SLUIS_CVP + S_SLUIS_SWP + S_MELON + S_NHGAN + S_MLRTN + S_PEDRO + S_MCLRE + S_HNSLY` | - |

## Appendix: Questions about the notebook code

Raised during WRESL verification (March 2026). These are open questions about the notebook code (the `coeqwalpackage` Python modules in the notebook repositories), not the ETL. This is a point-in-time snapshot, so the upstream repositories may have changed since.

### COEQWAL_V3 / `DataExtraction.py`

| # | Question | Location |
|---|----------|----------|
| 1 | Is `UD_ANTOC` meant to be pre-converted to TAF? It is set to `25 × 0.001984 × days_in_month` (already TAF) but labeled `CFS`, so `convert_all_cfs_to_taf` converts it a second time | `UD_ANTOC` (L928, L932); conversion at L1416 |
| 2 | Does `DEL_CVPSWP_TOTAL` double-count SOD CVP deliveries? It sums `DEL_CVP_TOTAL` (= `DEL_CVP_TOTAL_N` + `DEL_CVP_TOTAL_S`) then adds `DEL_CVP_PAG_S` and `DEL_CVP_PEX_S` again. WRESL defines `del_cvp_total_s = del_cvp_pag_s + del_cvp_pmi_s + del_cvp_pex_s + del_cvp_prf_s + del_cvp_pls_s`, so both are already inside the total | `DEL_CVPSWP_TOTAL` (L389-L399); WRESL `DeliveryLogic/output/deliv_short_cvp_s.wresl` |
| 3 | Are the trailing spaces in `S_OROVLLEVEL6DV ` and `S_MELONLEVEL5DV ` column names in `preprocess_compound_data_dss` intentional? `preprocess_study_dss` has no trailing space, so the names mismatch across the two paths | L494, L496 vs L237, L239 |
| 4 | Are `D_AMADR_NU` and `D_AMCYN` meant to be defined twice each? The second definition overwrites the first | `D_AMADR_NU` (L1821, L1872), `D_AMCYN` (L1809, L1884) |

### COEQWAL (WAM repo) / `metrics.py`

| # | Question | Location |
|---|----------|----------|
| 1 | In `create_subset_var`, the `water_year_type` branch references `var_filter`, which is never defined in this function (it exists only in `create_subset_unit`). Runtime crash. Bug? | `create_subset_var` (L195) |
| 2 | The same branch reads `df_copy` before it is assigned later in the function, so it is unbound at runtime. Bug? | `create_subset_var` (L199, assigned L214) |
| 3 | `prob_less` holds P(≥) (`count_gte / n`) despite its name. The function output and docstring are correct, so this is naming only. Rename? | `prob_less` (L884, L921) |

## Statistics roadmap

Deferred and in-progress work for the statistics and model-run pipeline.

### Migrate module variable lists into SQL tables

**Current state:** only DU Urban reads its variable mapping from SQL (`du_urban_variable`, `du_urban_delivery_arc`). MI, CWS, Delta, the reservoir thresholds, and the AG aggregate and GW-only sets are Python constants. Env flows reads its mapping from `channel_entity.csv`. Reservoirs, AG, and Refuge read entity lists from seed CSVs and derive variable names by prefix.

**Goal:** harden each list in collaboration with the WAM team, then move it into a seed SQL table like `du_urban_variable`, and have the module read from the database instead of a hardcoded dict, so the lists are queryable, versioned, and verifiable.

**Why deferred:** each list needs WAM confirmation before it is worth freezing into schema, and each migration needs a new table, seed, and reader change.

### CVP contractor load (unfinished)

**Current state (verified May 2026 audit):** all 30 `mi_contractor` rows came from `swp_contractor_perdel_A.wresl` with `project = SWP`. Zero CVP rows.

**Schema expectation:** the contractor schema ([`database/sql_archive/03_entity_layers/mi/03_create_mi_contractor_entity_tables.sql`](../../database/sql_archive/03_entity_layers/mi/03_create_mi_contractor_entity_tables.sql)) already anticipates CVP: `mi_contractor_group` enumerates `CVP_NOD` and `CVP_SOD`, and `mi_contractor.project` documents `SWP` or `CVP`. The CVP source tables / WRESL files still need to be located (see Work below).

**Work:**

1. Locate CVP contractor source tables / WRESL files.
2. Load CVP rows into `mi_contractor` and `mi_contractor_delivery_arc`.
3. Re-run M&I statistics ETL and Layer 2 verification for a sample scenario.


### `cvp_total` aggregate row (decision pending)

**Current state:** `cws_aggregate_entity` has 6 rows (`swp_total`, `swp_nod`, `swp_sod`, `cvp_nod`, `cvp_sod`, `mwd`). SWP has `swp_total` plus NOD/SOD splits. CVP has `cvp_nod` and `cvp_sod` only. No `cvp_total`. SWP works because CalSim ships a single unsuffixed `DEL_SWP_PMI` variable. CVP only exposes `DEL_CVP_PMI_N` and `DEL_CVP_PMI_S`.

**Question for WAM team:** should a CVP-wide total row exist (mirroring `swp_total`), or is NOD+SOD sufficient? Three options:

- **A.** Use a single CalSim-native CVP variable if one exists (would mirror SWP). Confirm with the WAM team.
- **B.** ETL-computed sum of `cvp_nod` + `cvp_sod` (a new `CWS_AGGREGATES` entry that sums the two existing aggregate values).
- **C.** Leave as-is. Treat NOD + SOD as the default CVP split, no total row.



### Master crosswalk vs `du_urban_variable`

**File:** [`data/reference/cws/Updated Master crosswalk SW DUs M&I May7 2026.xlsx`](../../data/reference/cws/Updated%20Master%20crosswalk%20SW%20DUs%20M&I%20May7%202026.xlsx)

86 rows mapping `du_id` to CalSim `UD_*` demand and `DN_*` delivery variables.

**DB table:** `du_urban_variable` (90 rows in May 2026 audit).

**Comparison (May 2026):**

- match: 17
- conflict: 54 (all on `delivery_variable`: xlsx `DN_<id>` vs DB `DL_<id>` / `D_<plant>_<id>`)
- xlsx-only: 15
- db-only: 19 (mostly `_PA` ag-suffix ids, worth confirming they belong in the urban table)

**Work (statistics batch):**

1. Implement `etl/statistics/scripts/compare_master_crosswalk.py` (not yet in the repo. The comparison logic existed in a prior local checkout but was never committed). Run with `--csv-out` to refresh the audit CSV against the latest audit snapshot (`audits/monthly_*/layer_exports/04_variable/du_urban_variable.csv`). The audit CSV (`etl/statistics/audit_reports/master_crosswalk_audit.csv`) is gitignored.
2. Decide delivery-variable policy (xlsx wins, DB wins, or case-by-case). `D_<plant>_<id>` codes may encode the correct CalSim variable for that specific DU, so check with the M&I team before normalizing to `DN_<id>`.
3. Confirm whether the `_PA` rows in `du_urban_variable` are intentional (they look like agricultural rows misfiled in the urban table).
4. Update `du_urban_variable` and re-run urban DU statistics verification.


### `gw` / `sw` BOOLEAN migration

**Current:** `du_urban_entity.gw` and `.sw` are `VARCHAR(5)` with `'0'`/`'1'`.

**Target:** `BOOLEAN NULL` with reader audit across ETL and API.

Tracked in [`SCHEMA_BACKLOG.md` § 6](../../database/SCHEMA_BACKLOG.md#6-schema-pattern-inconsistencies) and [`gw_sw_reconciliation.md` Step 7](../../database/topic_docs/cws/gw_sw_reconciliation.md#step-7-boolean-type-migration-independent-of-value-reconciliation).


### Reference data sources for gw/sw

| Source | Location | Role |
|---|---|---|
| Seed CSV | `database/seed_tables/04_calsim_data/du_urban_entity.csv` | Current committed reference |
| CalSim report PDF | `data/raw/pdf_tables_from_CalSim_report/urban_du.pdf` | Upstream source for urban gw/sw |
| Ag PDF extracts | `data/raw/csv_from_CalSim_report_pdf/du+diversion/*.csv` | Upstream for ag gw/sw |
| M&I team xlsx | `data/reference/cws/Final_M&Idemandunits_withlatlongs.xlsx` | Team refresh, may override seed |

Reconciliation script: [`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../../etl/tier_data/scripts/reconcile_gw_sw_sources.py)


### Urban gw/sw reconciliation (in progress)

**Walkthrough:** [`gw_sw_reconciliation.md`](../../database/topic_docs/cws/gw_sw_reconciliation.md)

**Status (May 2026):**

- Urban seed vs M&I xlsx: 88/120 agree, **32 disagree** (semantic, not format)
- Ag SAC Table 3-3 vs seed: 82/82 agree
- Ag SJR Table 3-6 vs seed: 62/62 agree
- CalSim authority: the CalSim manual Table 3-7 rollup `urban_demand_unit_water_sources.csv` (**111 du_ids**) already exists and covers all 32 seed-vs-xlsx disagreements.

Each DU carries a `(gw, sw)` flag pair: whether it has groundwater-supplied systems and whether it has surface-water-supplied systems. Three sources give a value for it: the committed **seed** (`du_urban_entity.csv`), **CalSim** (the Table 3-7 rollup), and the team **xlsx** (which sometimes applies analysis-specific "tier" rules, such as forcing `sw=0` even when CalSim shows surface delivery, or turning `gw` on if any community in the DU uses groundwater). Compared against CalSim, the 32 seed-vs-xlsx disagreements split into four groups:

- **Safe to update seed (5):** seed differs from CalSim and the xlsx agrees with CalSim, so the model justifies the change (`03_PU1`, `24_NU4`, `26N_NU5`, `26N_PU1`, `26S_PU2`).
- **Override, no CalSim-driven fix (24):** the seed already equals CalSim and only the xlsx differs, because the team applied a tier rule (examples `02_PU`, `24_NU1`, `62_NU`). There is no CalSim basis to edit the seed unless team formally adopts and documents the xlsx tier rule (else it will be confusing 3 years from now).
- **Three-way conflict, defer (2):** seed, CalSim, and xlsx all differ (`60N_NU2`, `90_PU`).
- **Team Notes override (1):** `03_PU3`, where CalSim matches the seed but the xlsx clears `sw` by a deliberate note. Needs team sign-off and documentation.

**Remaining:**

1. Apply the 5 safe seed updates, then resolve the small residual the rollup does not cover (seed du_ids absent from it, and `NAPA2` present but blank) from `urban_du.pdf`
2. Get a team decision on the tier-rule group (24), the two three-way conflicts, and the `03_PU3` Notes override
3. Update `du_urban_entity.csv` seed
4. Then `gw`/`sw` BOOLEAN migration (see [`SCHEMA_BACKLOG.md` § 6](../../database/SCHEMA_BACKLOG.md#6-schema-pattern-inconsistencies) and [`gw_sw_reconciliation.md` Step 7](../../database/topic_docs/cws/gw_sw_reconciliation.md#step-7-boolean-type-migration-independent-of-value-reconciliation))

Ag PDF tables 3-4 and 3-5 have no gw/sw columns (diversion arcs only). Do not compare them to seed gw/sw.

### Per-scenario atomic transactions in stats writers

**Current state:** every writer opens a connection, runs DELETE plus INSERT, commits, and closes within seconds. Most writers commit per scenario but rely on garbage collection for cleanup on error. Refuge and env_flows are the only modules with deterministic try/finally cleanup today.

**Goal:** wrap each writer's DELETE plus INSERT in `with conn:` and `with conn.cursor() as cur:` so a mid-scenario failure rolls back deterministically and the connection is released on every path.

**Why deferred:** requires live RDS verification of rollback behavior per module. The SQL is battle-tested but the rollback semantics have to be exercised against a real database.

**Connection lifecycle (do not change):** Each module opens its own short-lived connection at the moment it writes, lasting seconds. Do not refactor toward one shared connection held across all 8 modules during a scenario run. A module's calculation phase takes minutes, sometimes 30 or more, and RDS behind an NLB with a ~350-second idle timeout will drop an idle TCP connection mid-calculation, so the next write call crashes.


### Unconfirmed data values (two verification checks skipped meanwhile)

Two values the statistics pipeline depends on are unconfirmed: deciding which is correct is a modeling question for the Water Allocation Modeling Team, not a code fix. Until the team confirms each value, `etl/statistics/verify_all_sections.py` skips the affected check (logged as a warning) so an unresolved data question does not read as a verification failure.

Two data questions for the Water Allocation Modeling Team:

- **San Luis CVP/SWP capacity split:** What is the correct capacity split between the federal (CVP) and state (SWP) shares of San Luis? `*_pct_capacity` for `SLUIS_CVP` / `SLUIS_SWP` is affected. If `reservoir_entity` is wrong, the ETL's San Luis `pct_capacity` (and the API/frontend values that read it) are wrong.

- **GDPUD_NU delivery variable:** Which CalSim pathname is GDPUD_NU's surface delivery: `DN_GDPUD_NU` (`SW_DELIVERY-NET`) or `DL_GDPUD_NU` (what `du_urban_variable` currently maps)? If the mapping is wrong, this DU's delivery is wrong for every scenario.

Once the team answers, for each item the developer:

1. Fixes the source of truth in the ETL (the seed table, the `du_urban_variable` mapping, or a `CAPACITY_OVERRIDES` entry) and re-runs the affected module so the loaded values are correct. Developer doesn't need to run all of the statistics modules. Just the one pertaining to the change (but for all scenarios).
2. Updates the expected value in the verification script (`RESERVOIR_VARS` or `verify_cws_du`) to match.
3. Removes the `KNOWN ISSUE` skip guard so the check runs again.


### Reservoir spill threshold coverage and spill volume are only partially implemented

**Current state:** The reservoir module reports spill as the frequency of storage being at or above a flood-control level, not as a released volume. Two problems limit how much of that output is trustworthy.

**Problem 1: flood-level coverage:** Spill needs a flood-control level (`flood_var` in `RESERVOIR_THRESHOLDS`, `etl/statistics/reservoirs/reservoir_metrics.py`) that exists as a column in the scenario DV CSV. The intended rule is "one DV level below the highest for that reservoir," and the highest level varies by reservoir:

- Correct (one below the highest present level): SHSTA, TRNTY, SLUIS_CVP, SLUIS_SWP, FRMDW, HHOLE.
- Defensible (the true highest level is absent and capacity is overridden, so the configured level is effectively one below the true top): FOLSM, OROVL, MELON.
- Questionable (configured `flood_var` equals the highest present level, i.e. the capacity curve, with no override): SLYCK, UNVLY.
- No usable threshold (configured `flood_var` is `None` or a level variable absent from the DV output): about 80 of the 92 configured entries. Of those 92 entries, 50 are `None` and most of the rest point to a level variable (often `LEVEL5DV`) that is not in the data. One entry (MLRTN) is a numeric constant, which does resolve (`_get_flood_threshold` returns a constant Series), so it is counted among the ~12 that compute, not here.

When the configured `flood_var` is absent from the data or is `None`, `_get_flood_threshold` (`calculate_reservoir_statistics.py`) returns `None`, and the no-threshold branch of `calculate_period_summary` writes `spill_years_count = 0`, `spill_frequency_pct = 0`, and `spill_monthly_frequency_pct = 0` (with `spill_threshold_pct = None`). A reservoir with no modeled flood level is therefore indistinguishable from one that genuinely never spills. The companion `flood_pool_prob_*` fields are set to NULL in the same situation, so the two are treated inconsistently. Every reservoir in `reservoir_entity.csv` has a `RESERVOIR_THRESHOLDS` entry (92 entries for 92 reservoirs), so coverage is limited by which level variables are present in the data, not by missing entries. In `s0020` only about 12 reservoirs get a real spill computation. The other ~80 of the 92 configured threshold entries fall through to the zero branch.

**Problem 2: spill volume is never computed:** The spill block assigns only frequency and probability keys: in the period summary `spill_years_count`, `spill_frequency_pct`, `spill_monthly_frequency_pct`, and `spill_threshold_pct`, and in the monthly table `spill_months_count`, `total_months`, `spill_frequency_pct`, and `storage_at_spill_avg_pct`. The spill volume columns (`spill_avg_cfs`, `spill_max_cfs`, `spill_mean_cfs`, `spill_peak_cfs`, `annual_spill_avg_taf`, `annual_spill_cv`, `annual_spill_max_taf`) appear only in the INSERT column lists and `EXCLUDED` upsert clauses. They are never assigned a value, so they are always written as NULL. Any consumer expecting spill volume gets nothing. This matches the note in the [statistics we compute](#statistics-we-compute) section.

**Naming:** `spill_threshold_pct` (period summary) and `storage_at_spill_avg_pct` (monthly) are the mean storage as a percent of capacity during spill months, not the flood trigger level. The names suggest the trigger.

**Goal:**

1. Derive `flood_var` per reservoir per scenario as "highest present `S_{code}LEVEL{n}DV` minus one," instead of a hardcoded level number that may be absent. Confirm with the Water Allocation Modeling Team which level is the true flood-control curve for the reservoirs where this is ambiguous (notably SLYCK, UNVLY, and any reservoir whose top level equals capacity).
2. Write NULL, not 0, for `spill_frequency_pct` and the related counts when no flood level is available, so "not modeled" is distinguishable from "never spills." Align this with the `flood_pool_prob_*` NULL behavior.
3. Either compute the spill volume columns (from `C_*_FLOOD` or a storage-above-threshold integration) or drop them from the schema and the writer so they stop implying data that does not exist.
4. Rename `spill_threshold_pct` / `storage_at_spill_avg_pct`, or document them in the schema as "average storage (% capacity) during spill," and update any API or frontend reader. A rename is a breaking DB and API change, so it needs a migration plan.

Items 1 and 2 change values the API and frontend already read, so they need live-RDS verification and a re-run of the reservoir module across all scenarios. Item 1 also has a modeling-team dependency. Item 4 is a breaking schema change.

### Needs-review backlog (WAM team decisions)

Open questions checklist:

- **Demand-unit water balance and shortage slack form:** Slack (`SHRTG`/`GW_SHORT`) and the `GPmax` bound activate per demand unit and per scenario, not uniformly. Confirm which units the pipeline should treat as carrying a real shortage or `GP > AW` slack. See [Water balance](#water-balance).
- **Ag GW-only synthesis set:** Backend lists 17 DUs (`GW_ONLY_DU_IDS`) where V3 synthesizes 11, and uses `26N_NA` where V3 uses `26S_NA`. Reconcile the set and the ID. See the Calculations table under [Provenance and verification](#provenance-and-verification).
- **Delta NDO annual metric:** `annual_avg_taf` is an annual sum of monthly TAF; the notebook headline NDO is a mean of monthly CFS (backend also stores a matching `avg_cfs`). Decide which the website displays. See the Calculations table.
- **Reservoir CV guards, percentile bands, and TRNTY flood level:** Confirm the CV guards, the fixed-capacity percentile bands, and the `LEVEL4DV` (vs notebook `LEVEL5DV`) Trinity flood threshold. See the Calculations table.
- **Percent demand met and reliability methodology:** No delivery-family "percent demand met" formula exists in the notebooks; the backend definitions are extensions using a different percentile population. These need a definition sign-off. See the Calculations table.
- **Location-list reconciliations:** `CWS_DEL` (7 ID mismatches vs `DrinkingWater_Mapping.csv`), the ag entity list (144 rows vs the 132 mapping, `07S_PA` absent), and the urban entity list (125-row superset of the 78-row mapping, 64 `_NU` DUs). See the Location lists table.
- **Unconfirmed data values:** San Luis CVP/SWP capacity split and the `GDPUD_NU` delivery variable, both skipped by `KNOWN ISSUE` guards in the verifier. See [Unconfirmed data values](#unconfirmed-data-values-two-verification-checks-skipped-meanwhile).
- **`s0065` salmon-tier exclusion:** Excluded by the data team (documented in [`etl/tier_data/README.md`](../tier_data/README.md)).

### Minor cleanup:

None changes a calculated value.

- **Dead constant in the M&I module:** `CWS_SHORTAGE_CSV` in [`mi/calculate_mi_statistics.py`](mi/calculate_mi_statistics.py) points to `etl/pipelines/CWS/CWS_shortage_variables.csv`, a path that does not exist, and is never used. The live shortage mappings are the `MI_CONTRACTOR_VARIABLES` dict in the same file. Remove the dead constant.
- **Idealized balance in code docstrings:** The ag module docstring and the `calculate_du_shortage_monthly` docstring ([`ag/calculate_ag_statistics.py`](ag/calculate_ag_statistics.py)) state the balance as `AW + RP = DN + GP + RU + SHORTAGE`. Per [Water balance](#water-balance), the `SHRTG`/`GW_SHORT` slack is active for some units and scenarios and absent for others, so the docstring form is correct for part of the set, not all of it. Align the docstrings once the modeling team confirms the intended form.

