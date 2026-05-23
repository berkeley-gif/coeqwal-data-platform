# Verification

ETL-developer-facing walkthrough of each verification layer, with tolerance numbers, per-section commands, and metric coverage. For the orientation across the whole pipeline (verification vs auditing, hashes, orchestrator, known gaps), see [`docs/VERIFICATION.md`](../../docs/VERIFICATION.md).

End-to-end accuracy checks for the pipeline, from DSS extraction through database statistics to API responses. Verification runs at four layers:

```
DSS Files --> S3 CSVs (DV + SV) --> PostgreSQL --> JSON API --> Frontend
  Layer 1        Layer 2              Layer 2b       Layer 3     Layer 4
  (extraction)   (ETL statistics)     (tier data)    (API)       (status page)
```

Variable lists sourced from `COEQWAL_V3/notebooks/variable_groupings.csv` and the mapping CSVs (`DrinkingWater_Mapping.csv`, `Agricultural_Mapping.csv`, `Eflows_Mapping.csv`).

This directory holds visualization scripts and reference PDFs ([`visualize_custom_reservoirs.py`](visualize_custom_reservoirs.py), `reservoir_percentile_bands_s0020.pdf`, `verification_reservoirs_s0020.pdf`). The verification scripts themselves live with the code they verify: Layer 1 / 1b in [../batch-container/](../batch-container/), Layers 2 and 3 in [../statistics/](../statistics/), Layer 4 in the frontend.

## Layer 1: Extraction (DSS to CSV)

Validates that `dss_to_csv.py` extracts data correctly from HEC-DSS files. Uses `validate_csvs.py` to compare extracted CSVs against the modeling team's trend report CSVs with configurable tolerances.

Extract records stored in `audits/validation_mismatches/{scenario_id}_extract_record.json`.

Runs automatically inside every Batch job. See [../batch-container/README.md](../batch-container/README.md).

## Layer 1b: DSS-vs-CSV unit verification

Independently verifies that the unit metadata in every CSV column header matches what the original DSS file reports. It is a ground-truth check: re-opens the DSS file with `pydsstools` and compares each variable's unit against the CSV header row 6.

Runs automatically inside every Batch job, with results in the manifest at `unit_verification.dv_unit_mismatches`. Can also be re-run on-demand from Cloud9 via the Docker image. See [../batch-container/README.md](../batch-container/README.md#layer-1b-dss-vs-csv-unit-verification) for details.

## Layer 2: ETL Statistics (CSV to DB)

Computes expected values from reference CSVs and compares against database values.

```bash
# Single scenario
python etl/statistics/verify_all_sections.py --scenario s0020

# All scenarios with JSON reports
python etl/statistics/verify_all_sections.py --all-scenarios --report-dir audits/verification_reports

# CSV-only mode (no DB connection needed)
python etl/statistics/verify_all_sections.py --scenario s0020 --csv-only
```

**Sections verified:**

- **Reservoirs**: April/Sept storage (TAF + % capacity), annual average, spill frequency
- **CWS Aggregates**: Annual delivery (TAF), shortage, reliability
- **CWS Demand Units**: Per-DU annual delivery (TAF) for sample DUs
- **AG Demand Units**: SW delivery, GW pumping, demand, reliability for sample DUs
- **AG Aggregates**: Annual delivery (TAF)
- **M&I Contractors**: Delivery, shortage, reliability, % demand met
- **Env Flows**: Average CFS, Pearson r, % unimpaired, % functional flows
- **Refuge**: Delivery, shortage, reliability
- **Tiers**: All 9 tier codes verified against staging CSVs and DB

**Tolerances**: `abs_tol=0.5`, `rel_tol=0.01` (configurable per check)

**Output**: `audits/verification_reports/{scenario_id}_layer2.json`

## Layer 3: API verification (DB to API)

Queries API endpoints and compares responses to direct database queries.

```bash
# Single scenario
python etl/statistics/verify_api.py --scenario s0020

# Custom API URL
python etl/statistics/verify_api.py --scenario s0020 --api-url http://localhost:8000

# All scenarios
python etl/statistics/verify_api.py --all-scenarios
```

**Endpoints verified:**

- `GET /api/statistics/batch` (storage, CWS, AG)
- `GET /api/tiers/scenarios/{id}/tiers` (all 9 tier codes)
- `GET /api/statistics/scenarios/{id}/channels/period-summary` (env flow)

**Output**: `audits/verification_reports/{scenario_id}_layer3.json`

## Layer 4: Public status page

Verification results are served by `GET /api/verification/status` and displayed at `/verification` on the frontend. Shows a per-scenario pass/fail grid with drill-down to individual checks.

## Validation framework

### Tolerance parameters

- **Absolute tolerance (`abs_tol`)**: Maximum allowed absolute difference between values. Used for values close to zero where relative comparison is not meaningful. Example: `abs_tol=1e-6` means values must be within +/-0.000001 units.
- **Relative tolerance (`rel_tol`)**: Maximum allowed relative difference as a fraction. Used for larger values where proportional differences matter more. Example: `rel_tol=1e-6` means values must be within 0.0001% of each other.

### Validation logic

Values are considered equal if both are NaN OR within tolerances:

```python
np.isclose(value1, value2, atol=abs_tol, rtol=rel_tol, equal_nan=True)
```

Default tolerances: `1e-6` absolute and relative. Compares all common variables between reference and extracted data. Reports mismatches with exact differences. Status: PASS / FAIL with per-section summaries.

## Metric coverage

**Implemented and loaded (ETL + DB):**

| Module | Metrics | Entities | Tables |
|---|---|---|---|
| **Reservoirs** | Storage (TAF, % capacity), flood/dead pool probability, spill volume/frequency | 10 reservoirs (Shasta, Oroville, Folsom, Trinity, New Melones, Millerton, San Luis CVP/SWP/combined, Eastside Bypass) | `reservoir_storage_monthly`, `reservoir_period_summary` |
| **Urban DU** | Delivery, shortage, % demand met, reliability | 81 demand units | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |
| **M&I Contractors** | Delivery, shortage, % demand met (via PERDV), reliability | 16 SWP contractors + MWD aggregate | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |
| **CWS Aggregates** | Delivery, shortage, reliability by project/region | 6 aggregates (SWP total/N/S, CVP total/N/S) | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |
| **AG** | Demand (AW), SW delivery (DN), GW pumping (GP), shortage, reliability, GW restriction shortage | 131 demand units + 9 regional aggregates | `ag_du_demand_monthly`, `ag_du_sw_delivery_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary`, `ag_aggregate_monthly`, `ag_aggregate_period_summary` |
| **Refuge** | Delivery, derived shortage (demand - delivery), reliability | 18 wildlife refuge demand units | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` |
| **Env Flows** | Flow volume (CFS, TAF), % unimpaired, % functional flows, alteration index (Pearson r), CEFF seasonal metrics | 59 channels (20 with MIF, 17 with EFLOWS) | `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary` |
| **Delta** | Net Delta Outflow (NDO), X2 position (spring/fall), salinity at Emmaton, Jersey Point, Rock Slough, Collinsville, Banks and Tracy pumping plant EC | 8 variables | `delta_monthly`, `delta_period_summary` |
| **Sensitivity** | Climate sensitivity (hist/CC50/CC95 comparison), operational sensitivity (cross-scenario spread) | All entities from above modules | `sensitivity_climate`, `sensitivity_operational` |
| **Tiers** | CWS_DEL, AG_REV, ENV_FLOWS, RES_STOR, GW_STOR, DELTA_ECO, FW_DELTA_USES, FW_EXP, WRC_SALMON_AB | 9 tier codes | `tier_location_result` |

**Verified end-to-end (ETL + DB + API):**

- CWS: delivery volume, % of demand, absolute shortage
- AG: SW delivery, GW pumping, total shortage, shortage %, reliability
- Env Flows: volume, % unimpaired, % functional flows, alteration index
- Refuge: delivery, shortage, reliability
- Reservoirs: April/Sept storage (TAF + %), spill frequency
- Delta: NDO, X2, EC at 4 stations, pumping plant EC
- Tiers: all 9 tier codes

**Not yet implemented:**

- Groundwater level, storage volume, level/storage change (no CalSim variable mapping established)
- Salmon abundance as a continuous/raw metric (`WRC_SALMON_AB` is currently stored only as the categorical tier level parsed from the data team's CSV. `tier_score_cont` is passed through but not persisted)

## How to add a new scenario (verification side)

After [../README.md](../README.md) (ingestion) and [../statistics/README.md](../statistics/README.md) get a new scenario into the DB:

1. Ensure DSS-to-CSV extraction has run and manifests show PASS in `audits/validation_mismatches/`
2. Run the ETL statistics: `python etl/statistics/run_all.py --scenario {id}`
3. Load tier data (see [../tier_data/README.md](../tier_data/README.md) for the full flow):
   - `python etl/tier_data/stage_tier_results.py` (normalize team drops into flat files)
   - `python etl/tier_data/load_all_tier_results.py --output-sql all_tiers.sql` then `psql "$DATABASE_URL" -f etl/tier_data/output/all_tiers.sql`
   - `DATABASE_URL="$DATABASE_URL" python etl/tier_data/load_all_tier_results.py --verify` (mandatory)
4. Run Layer 2 verification: `python etl/statistics/verify_all_sections.py --scenario {id}`
5. Run Layer 3 verification: `python etl/statistics/verify_api.py --scenario {id}`
6. Check results at `/verification` on the frontend
