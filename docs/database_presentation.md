# COEQWAL database

As of May 18, 2026

All row counts and table inventories are from the May 11, 2026 monthly audit (`audits/monthly_20260511_125419/`).

---

### COEQWAL database: architecture, audit framework, and roadmap

1. Purpose of the database
2. Layered schema and patterns
3. Audits, versioning, ETL (how we know the data is right)
4. Operations, security, and workflow
5. Roadmap

---

## Purpose of the database

The COEQWAL database has three concrete jobs:

**1. Serve model-run derived data to the public website** Every chart on the website is rendered by the FastAPI service issuing SQL queries against this database. Postgres responds with indexed reads against pre-aggregated period-summary tables in milliseconds.

*Indexed reads* Postgres jumps straight to the rows matching the query instead of reading every row in the table, by following pre-built lookup structures called indexes. For the API's typical query shapes, the relevant indexes are on composite keys like `(scenario_short_code, du_id)`.

*Pre-aggregated period-summary tables* Tables named `*_period_summary` (e.g. `ag_period_summary`, `reservoir_period_summary`) hold statistics the ETL has already computed across the CalSim run time period. One row per scenario × entity.

**2. Answer analyst queries across scenarios** Analysts can query the database for, for example, "compare X to Y" questions.

**3. Serve as a repository for COEQWAL project data** Reference data (every demand unit, reservoir, channel, etc) and project content data (tier definitions, hydroclimate defintions) live here. Seed CSVs are in the repo, ERD in the repo. There's a monthly audit available (`python database/audit/run_monthly_audit.py`, run from the repo root on Cloud9).

### Where the database lives

**The database lives in AWS**, in the `us-west-2` (Oregon) region:

- AWS RDS PostgreSQL 17.4 + PostGIS: Multi-AZ across two availability zones for automatic failover.
- Placed in a private VPC subnet. Not reachable from the public Internet. The only paths in are (a) workloads inside the same VPC (the API on Fargate, the audit Lambda), or (b) developer entry through our current bastion, AWS Cloud9, an AWS-managed EC2 inside the VPC, identified per-user via AWS IAM. *(Roadmap alternative: AWS Systems Manager Session Manager, the planned post-Cloud9 path, see below.)*

*Cloud9 as our bastion.* A bastion in a castle is a structure projecting outward from a castle wall, intentionally exposed, narrowly used, guarded. Cloud9 plays that role here: it is the only developer-facing door into the VPC, identified per-user via AWS IAM, narrowly scoped and audited (every `psql` command runs as a registered developer). It is the one exposed entry point, on purpose.

**Why run commands in Cloud9**

| Need | What Cloud9 gives us |
| --- | --- |
| In-VPC `psql` to RDS | An AWS-managed EC2 instance running inside the same VPC as RDS. `psql` connects via the private subnet's local route, no VPN required. |
| Reproducible environment | Configured with: <ul><li>`psql`</li><li>the AWS CLI</li><li> project virtual environment at `~/environment/coeqwal-backend/venv/` (`source venv/bin/activate` before running any Python script)</li><li>the repo</li><li>`$DATABASE_URL` / `$SUPERUSER_URL` exported in `~/.bashrc`</li></ul> |
| Heavy model files stay on EC2 | No need to round-trip gigabytes through a local dev host. |
| Cost control | Auto-stops when idle.

**AWS architecture diagram:** [dev.coeqwal.org/aws_architecture.html](https://dev.coeqwal.org/aws_architecture.html)

---

## Example queries to try

A community member asks: *"How much might Alameda County Water District's South Bay Aqueduct delivery reliability change under a high climate risk water future, if we hold current operations constant?"*

**The two connection URLs.** Cloud9 has two PostgreSQL connection strings pre-exported in `~/.bashrc`. Pick one based on what you're doing:

- **`$DATABASE_URL`**: your registered developer role. Use this for everything day-to-day: queries, seed loads, ETL, audits. Every write to the db is attributed to someone.

- **`$SUPERUSER_URL`**: the RDS master account (`postgres`). Use this **only** for DDL (`CREATE` / `ALTER` / `DROP` table). Writes through this URL are attributed to the system account.

For the query on this slide and everything else in this deck, you only ever need `$DATABASE_URL`. To verify both are set on Cloud9:

```bash
# List the database-related environment variables:
env | sort | grep -E "DATABASE|SUPERUSER|PG"

# Or list every environment variable on the Cloud9 session:
env | sort
```

**About this example.** `SBA029` is Alameda County Water District's South Bay Aqueduct delivery, captured in CalSim 3 as variable `D_SBA029_ACWD_PMI`.

**Example SQL query** (indexed on `(scenario_short_code, du_id)`):

```bash
psql "$DATABASE_URL" -c "
SELECT
    s.short_code              AS scenario,
    h.short_code              AS hydroclimate,
    due.du_id                 AS demand_unit,
    due.hydrologic_region     AS region,
    due.point_of_diversion    AS diversion_points,
    dps.annual_delivery_avg_taf  AS avg_annual_delivery_taf,
    dps.delivery_exc_p90         AS dry_year_delivery_p90_taf,
    dps.reliability_pct,
    dps.annual_shortage_avg_taf
FROM du_period_summary dps
JOIN scenario        s   ON s.short_code      = dps.scenario_short_code
JOIN hydroclimate    h   ON h.id              = s.hydroclimate_id
JOIN du_urban_entity due ON due.du_id         = dps.du_id
WHERE due.du_id      = 'SBA029'
  AND s.short_code  IN ('s0020', 's0056');
"
```

Or, if you want an interactive session:

```bash
psql "$DATABASE_URL"
```

To see what columns each table has, and one example row from each:

```bash
# Column layout (names + types) of the two tables the query joins:
psql "$DATABASE_URL" -c "\d du_period_summary"
psql "$DATABASE_URL" -c "\d du_urban_entity"

# One example row from each:
psql "$DATABASE_URL" -x -c "SELECT * FROM du_period_summary WHERE scenario_short_code='s0020' AND du_id='SBA029';"
psql "$DATABASE_URL" -x -c "SELECT * FROM du_urban_entity WHERE du_id='SBA029';"
```

---

## Behind the numbers: how reliability and shortage are computed

The two metric columns in the query results above (`reliability_pct` and `annual_shortage_avg_taf`) are derived from raw CalSim outputs by our ETL. This section lays out the formulas, the code locations, and the known nuances so anyone reading the deck can trace a number back to its origin.

CalSim3 outputs monthly time series for SBA029, and the ETL derives a fourth:

| Variable | What CalSim gives us | Units |
|---|---|---|
| `D_SBA029_ACWD_PMI` | actual monthly delivery on the arc | CFS |
| `SHORT_D_SBA029_ACWD_PMI` | DELIVERY-SHORTAGE in CFS | CFS |
| `PERDV_SWP_3` | monthly SWP allocation fraction | DSS-labeled `PERCENT`, used as a 0-1 fraction in the inversion formula |
| (derived) `DEM_D_SBA029_ACWD_PMI` | demand, recovered by the ETL via the PERDV inversion ([`calculate_du_statistics_v2.py` L313-L337](https://github.com/berkeley-gif/coeqwal-data-platform/blob/main/etl/statistics/du_urban/calculate_du_statistics_v2.py#L313-L337). Formula at L336) | TAF (after conversion) |

### Step 1: recover demand (the PERDV inversion)

CalSim emits delivery and shortage **already scaled** by the monthly allocation fraction. To recover the unscaled monthly demand (what would obtain when `perdv_swp_N == 1`), invert:

```
demand_cfs = (delivery_cfs + shortage_cfs) / perdv_swp_N
```

Our ETL implements this in the `perdv` demand_mode branch of [`etl/statistics/du_urban/calculate_du_statistics_v2.py` L313-L337](https://github.com/berkeley-gif/coeqwal-data-platform/blob/main/etl/statistics/du_urban/calculate_du_statistics_v2.py#L313-L337):

```python
del_cfs = get_column_value(output_df, delivery_var)
short_cfs = get_column_value(output_df, shortage_var) if shortage_var else 0.0
pv_var = perdv_vars[0]
pv = get_column_value(output_df, pv_var)
pv_safe = pv.replace(0, np.nan)
demand_cfs = (del_cfs + short_cfs) / pv_safe
return demand_cfs * output_df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
```

### Step 2: CFS to TAF conversion

Every CFS time series is multiplied by `DaysInMonth × CFS_TO_TAF_PER_DAY` to get TAF per month:

```python
# etl/statistics/units.py
CFS_TO_TAF_PER_DAY = 86_400 / 43_560_000  # ≈ 0.001983471
```

Derivation: `(seconds in a day) / (cubic feet per acre-foot × 1000)`. A constant CFS rate for one day delivers 0.001983 TAF.

### Step 3: reliability_pct

Once monthly delivery and recovered demand are in TAF, the ETL computes annual sums for each water year (Oct - Sep) and takes the mean ratio across the 100-year window:

```python
# etl/statistics/du_urban/calculate_du_statistics_v2.py, around line 590
common_years = ad.index.intersection(adm.index)
safe_adm     = adm[common_years].replace(0, np.nan)
pct_met      = (ad[common_years] / safe_adm) * 100
pct_met      = np.clip(pct_met, 0, 100)
reliability_pct = round(float(pct_met.dropna().mean()), 2)
```

In plain English: **for each water year, what fraction of the recovered annual demand did SBA029's turnout receive? Average that fraction across all 100 years.** For SBA029 / s0020, that average is **54.99%**.

`reliability_pct` is a database-team derived metric. Verifying our chosen formula and denominator with the COEQWAL modeling team is a Phase 2 task.

### Step 4: annual_shortage_avg_taf (CalSim's own SHORT_*, summed)

`SHORT_D_SBA029_ACWD_PMI` is computed by CalSim. We read it directly:

```python
shortage_cfs = get_column_value(output_df, shortage_var)
shortage_taf = shortage_cfs * output_df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
shortage_taf = shortage_taf.clip(lower=0)  # clamp tiny LP-solver negatives
```

The `clip(lower=0)` step removes occasional negative micro-values that CalSim's LP can emit as numerical noise.  Shortage is non-negative by definition. The ETL also logs the count of clamped values for audit. Then sum to annual and take the 100-year mean. For SBA029 / s0020 that mean is **0.85 TAF/year**.

### Subtle point

| Metric | Numerator | Denominator | Question it answers |
|---|---|---|---|
| `reliability_pct` | actual annual delivery | **recovered annual demand** (PERDV-inverted) | "what fraction of the recovered demand did we get, averaged over years?" |
| `annual_shortage_avg_taf` | shortfall, summed monthly | **perdv-scaled in-month demand** (CalSim's `SHORT_*` baseline) | "on average each year, how much of the in-month CalSim target went unmet?" |

The two metrics are **not complementary**. `(1 − reliability_pct/100) × demand` will **not** equal `annual_shortage_avg_taf`. For SBA029 / s0020, reliability is 54.99% and shortage is 0.85 TAF - both numbers are correct. They simply measure against different baselines.

In `du_period_summary`, the column comments now document this explicitly (see [`02_create_du_statistics_tables.sql` L216-L230](https://github.com/berkeley-gif/coeqwal-data-platform/blob/main/database/scripts/sql/.archive/12_mi_statistics/02_create_du_statistics_tables.sql#L216-L230)). The ETL has matching inline comments at the PERDV and reliability_pct blocks.

---

### Database summary

**The database in one sentence.** PostgreSQL 17 + PostGIS on AWS RDS, **95 tables**, **473 MB**, **~1.2M rows**, layered schema, automatic audit trigger on every write, ERD-driven verification.

**What it holds (May 11, 2026 snapshot):**

| Thing | Count |
| --- | ---: |
| Scenarios | 77 |
| Hydroclimates | 6 |
| Themes | 6 |
| Reservoirs | 92 |
| Channels | 669 |
| Urban demand units | 145 |
| Ag demand units | 144 |
| Refuge demand units | 18 |
| M&I contractors | 30 |
| Water budget areas | 42 |

**Re-run the audit on Cloud9:**

```bash
cd ~/environment/coeqwal-backend
source venv/bin/activate
python database/audit/run_monthly_audit.py
```

Output: `audits/monthly_YYYYMMDD_HHMMSS/`.

---

## The layered schema

---

- **00 VERSIONING** - `developer`, `version_family`, `version`, `domain_family_map`
- **01 LOOKUP** - `hydrologic_region`, `source`, `unit`, `statistic_type`, `network_type`, …
- **02 NETWORK** - `network`, `network_arc`, `network_node`, `network_gis`
- **03 ENTITY** - `reservoir`, `channel`, `du_*`, `mi_contractor`, `*_aggregate_entity`
- **04 VARIABLE** - `channel_variable`, `du_urban_variable`, `calsim_model_variable_type`
- **05 ASSUMPTIONS + OPS** - `assumption_definition`, `operation_definition`, `scenario_*_link`
- **06 SCENARIO** - `scenario`, `scenario_author`, `scenario_tag`, `scenario_hydroclimate_sibling`
- **07 HYDROCLIMATE** - `hydroclimate`, `slr`
- **08 THEME** - `theme`, `theme_scenario_link`
- **10+ RESULTS** - `tier_result`, `*_monthly`, `*_period_summary`, `sensitivity_*`

> see also: `database/README.md` § Schema layers.

---

### Layer 02 NETWORK: CalSim infrastructure as a graph

- **6,908** `network` records (master registry), split into **2,610** `network_arc` rows + **1,544** `network_node` rows + **4,154** `network_gis` geometries.
- Classification via `network_type` (21 types) and `network_subtype` (28 subtypes) from Layer 01.
- Every arc/node has `source_id` and `model_source_id` - full provenance back to the CalSim GeoSchematic.

| Type code | Description | Networks |
| --- | --- | ---: |
| STR | Stream | 1,310 |
| CH | Channel | 1,139 |
| D | Delivery | 539 |
| RT | Return flow | 259 |
| X | Demand unit | 240 |
| IN | Inflow | 225 |
| S | Storage | 94 |
| WTP | Water treatment plant | 42 |
| WWTP | Wastewater treatment plant | 22 |

> Source: `database/README.md` § 02_NETWORK: network topology; `audits/monthly_20260511_125419/tables_summary.csv` rows for `network*`.

---

### Layer 03 ENTITY

Every statistics row in Layers 10+ FKs back to a Layer 03 entity. These are the rows modelers query against.

| Entity table | Records | What it represents |
| --- | ---: | --- |
| `reservoir_entity` | 92 | Every reservoir CalSim simulates |
| `channel_entity` | 669 | Every channel arc |
| `du_urban_entity` | 145 | Urban (M&I / CWS) demand units |
| `du_agriculture_entity` | 144 | Agricultural demand units |
| `du_refuge_entity` | 18 | Refuge demand units |
| `mi_contractor` | 30 | M&I contractors (SWP today; CVP coming) |
| `wba` | 42 | Water budget areas |
| `compliance_station` | 2 | Delta compliance stations |
| `ag_aggregate_entity` | 9 | CalSim project-level ag rollups (SWP/CVP NOD/SOD, settlement, exchange) |
| `cws_aggregate_entity` | 6 | CalSim project-level CWS rollups (SWP total/NOD/SOD, CVP NOD/SOD, MWD) |

**Example: pull MWD's row from the `mi_contractor` entity table.**

```bash
psql "$DATABASE_URL" -c "
SELECT id, short_code, contractor_name, project, region, contractor_type,
       contract_amount_taf, source_file
FROM mi_contractor
WHERE short_code = 'MWD';
"
```

Returns one row carrying MWD's identity (`short_code = 'MWD'`, `contractor_name = 'Metropolitan Water District…'`), its classification (`project = 'SWP'`, `region = 'SOD'`, `contractor_type = 'MWD'`), its contract scale (`contract_amount_taf = 1911.50`), and provenance back to the upstream WRESL file. Every `mi_contractor_period_summary` row joins to MWD via the stable `id` returned here.

> Source: `database/README.md` § Implemented entity tables; `audits/monthly_20260511_125419/tables_summary.csv`.

---

### Layer 04 VARIABLE: the bridge from CalSim names to entities

CalSim emits time series keyed by variable names like `DEL_SWP_PMI`, `S_OROVL`, `C_SAC097_FLOOD`. Layer 04 maps those names to Layer 03 entities so the ETL knows what to extract per entity.

| Variable table | Records | What it maps |
| --- | ---: | --- |
| `channel_variable` | 1,352 | CalSim flow / loss variables per channel |
| `du_urban_variable` | 90 | Delivery / shortage / demand variables per urban DU |
| `calsim_model_variable_type` | 8 | Top-level variable typology (state-variable, decision-variable, derived, ...) |
| `derived_variable_type` | 4 | Sub-typology for computed variables |
| `variable_type` | 6 | Cross-cutting typology |

> Source: `database/README.md` § Implemented entity tables; `database/seed_tables/04_variable/`.

---

### Layers 06-08: scenarios, hydroclimates, themes

- **`scenario`** (77 rows) - one row per CalSim run. `short_code` (e.g. `s0020`), `run_name` (full DSS name), FK to `hydroclimate`, sibling link via `scenario_hydroclimate_sibling`.
- **`hydroclimate`** (6 rows) - `dwr_hist`, `dwr_hist_adj`, `cc50` (median +1.5°C, -3% precip), `cc95` (extreme +1.8°C, -9% precip), `CMIP6_TaiESM1_SSP370`, `CMIP6_CESM2-LENS_SSP370`.
- **`scenario_hydroclimate_sibling`** (27 rows) - links a baseline scenario to its climate twin. This is what lets the opening query say "baseline + cc95 sibling" without hardcoding pairs.
- **`theme`** (6 rows) + **`theme_scenario_link`** (79 rows) - which scenarios belong to which research theme.
- **`scenario_tag`** (10) + **`scenario_tag_link`** (109) - free-form tagging (`baseline`, `dcr`, `dcp`).

> Source: `audits/monthly_20260511_125419/layer_exports/06_scenario/`, `07_hydroclimate/`, `08_theme/`.

---

### Layers 10+: the wide statistics tables

These are the tables the API reads. Wide (lots of columns: monthly averages, percentiles, exceedances), keyed by `scenario_short_code` + entity FK. 29 result tables in total, grouped by domain:

**Reservoir (4 tables)**

| Table | Rows | Cols |
| --- | ---: | ---: |
| `reservoir_storage_monthly` | 82,080 | 42 |
| `reservoir_monthly_percentile` | 82,080 | 26 |
| `reservoir_spill_monthly` | 16,416 | 18 |
| `reservoir_period_summary` | 6,840 | 43 |

**Agriculture - per-DU and project rollup (7 tables)**

| Table | Rows | Cols |
| --- | ---: | ---: |
| `ag_du_demand_monthly` | 119,472 | 26 |
| `ag_du_gw_pumping_monthly` | 119,472 | 27 |
| `ag_du_sw_delivery_monthly` | 118,560 | 26 |
| `ag_du_shortage_monthly` | 67,668 | 28 |
| `ag_du_period_summary` | 9,956 | 52 |
| `ag_aggregate_monthly` | 8,208 | 29 |
| `ag_aggregate_period_summary` | 684 | 31 |

**Urban (M&I) - per-DU + CWS aggregate + contractor (8 tables)**

| Table | Rows | Cols |
| --- | ---: | ---: |
| `du_delivery_monthly` | 73,872 | 28 |
| `du_shortage_monthly` | 38,688 | 27 |
| `du_period_summary` | 6,156 | 33 |
| `cws_aggregate_monthly` | 5,472 | 45 |
| `cws_aggregate_period_summary` | 456 | 36 |
| `mi_delivery_monthly` | 20,940 | 28 |
| `mi_shortage_monthly` | 20,940 | 27 |
| `mi_contractor_period_summary` | 1,745 | 34 |

**Refuge (3 tables)**

| Table | Rows | Cols |
| --- | ---: | ---: |
| `refuge_du_delivery_monthly` | 16,416 | 26 |
| `refuge_du_shortage_monthly` | 16,416 | 29 |
| `refuge_du_period_summary` | 1,368 | 32 |

**Environmental flow (2 tables)**

| Table | Rows | Cols |
| --- | ---: | ---: |
| `env_flow_channel_monthly` | 53,808 | 58 |
| `env_flow_channel_period_summary` | 4,484 | 32 |

**Delta (2 tables)**

| Table | Rows | Cols |
| --- | ---: | ---: |
| `delta_monthly` | 7,296 | 27 |
| `delta_period_summary` | 608 | 14 |

**Tier outcomes (1 table)**

| Table | Rows | Cols |
| --- | ---: | ---: |
| `tier_result` | 653 | 19 |

**Sensitivity sweeps (2 tables)**

| Table | Rows | Cols |
| --- | ---: | ---: |
| `sensitivity_climate` | 306,272 | 15 |
| `sensitivity_operational` | 39,702 | 15 |

**Pattern across every domain.** Each group is two layers deep:

- `*_monthly` - one row per (scenario x entity x water month), used for time-series and percentile-band charts.
- `*_period_summary` - one row per (scenario x entity), pre-aggregated 100-year statistics (avg, std, exc_p5..p95, reliability). The fast slide-1 path.

> Source: `audits/monthly_20260511_125419/tables_summary.csv`; `database/README.md` § Schema implementation status.

---

## Versioning and audit framework

---

### Verification: three concerns, three tools

| # | Concern | Question | Tool |
| --- | --- | --- | --- |
| A | Schema structure | Is the DB shaped the way we documented it? | `database/run_audit.sh` + `verify_erd_against_audit.py` + `09_verify_level*.sql` |
| B | Reference data content | Do layers 00-08 contain the correct records? | `export_layer_tables.py` + diff vs `seed_tables/` |
| C | ETL statistics accuracy | Do the computed results (layers 10+) match the source? | `etl/statistics/verify_all_sections.py` (CSV→DB), `verify_api.py` (DB→API) |

All three are rolled up monthly by `python database/audit/run_monthly_audit.py` → `audits/monthly_<ts>/report.md` (the May 11 audit is the one we will demo in Section VIII).

A fourth concern, "is verified/unverified visible externally?", is on the roadmap. See [`docs/statistics_roadmap.md` V7](statistics_roadmap.md#v7-layer-4-smoke-test-verification-page-renders).

> Source: `database/README.md` § Audit and verification strategy.

---

### Provinance: per-row attribution

Every domain table has `created_at`, `created_by`, `updated_at`, `updated_by`. They are populated automatically by a `BEFORE INSERT/UPDATE` trigger calling `set_audit_fields()`.

```
INSERT/UPDATE on any table
  BEFORE trigger fires: set_audit_fields()
    calls: coeqwal_current_operator()
      reads: session_user        (PG session var - set at connection time)
                                 (NOT current_user: SECURITY DEFINER would return
                                  the function owner instead of the actual user)
      looks up: developer.id     (4 strategies: aws_sso_username → email → name → display_name)
      returns: INTEGER           (the developer.id)
    writes:
      NEW.created_by/updated_by = developer.id
      NEW.created_at/updated_at = NOW()
```

| Event | created_at | created_by | updated_at | updated_by |
| --- | --- | --- | --- | --- |
| INSERT | `NOW()` | `coeqwal_current_operator()` | `NOW()` | `coeqwal_current_operator()` |
| UPDATE | preserved | preserved | `NOW()` | `coeqwal_current_operator()` |

**Unregistered users cannot write.** The function raises an exception if no developer row matches.

> Source: `database/README.md` § Automatic audit triggers.

---

### Connection model: two URLs, one per concern

| Variable | Whose role | Use for |
| --- | --- | --- |
| `$DATABASE_URL` | Your registered developer role (e.g. `jfantauzza`) | Everything day-to-day: queries, seed loads, ETL, API, audits |
| `$SUPERUSER_URL` | RDS master (`postgres`) | DDL only: `ALTER TABLE`, `CREATE INDEX`, `GRANT` |

**Verification one-liner:**

```bash
psql $DATABASE_URL -c "SELECT session_user, coeqwal_current_operator() AS developer_id;"
```

If `developer_id = 1` you are connected as `postgres` and writes will be attributed to the system account. Fix `DATABASE_URL` and reconnect.

**Why two URLs:** keeps schema migrations (`SUPERUSER_URL`) separate from per-developer attribution (`DATABASE_URL`). A developer cannot accidentally do a DDL change as themselves. An admin cannot accidentally do bulk-data work as the system user.

> Source: `database/README.md` § Connection strings, § Connecting as yourself (getting correct attribution).

---

### Slide 17 - Versioning: version_family + version + domain_family_map

```
┌─────────────────────────┐       ┌─────────────────────────┐
│      developer (2)      │       │   version_family (14)   │
│  id, email, role        │       │  one per domain         │
│  aws_sso_username       │◄──────│  (entity, scenario,     │
└─────────────────────────┘       │   network, ..., stats)  │
         ▲                        └─────────────────────────┘
         │                                    │ 1:N
         │                                    ▼
         │                        ┌─────────────────────────┐
         │                        │       version (14)      │
         └────────────────────────│  one active per family  │
                                  │  + changelog            │
                                  └─────────────────────────┘
                                              │ 1:N
                                              ▼
                                  ┌─────────────────────────┐
                                  │  domain_family_map (93) │
                                  │  table → version_family │
                                  └─────────────────────────┘
```

- **14 version families** today - one per domain. Independent so a schema bump in `entity` does not force a bump in `network`.
- **93 table-to-family mappings** - every table is covered.
- **`audit_log`** (separate table) - opt-in row-level history with full JSONB diffs. Not active by default because of write volume on bulk tables. Enabled on sensitive tables (`scenario`, `developer`, `version`) via `apply_audit_log_trigger_to_table(...)`.

> Source: `database/README.md` § Layer 00_VERSIONING schema, § audit_log table (Scripts and Verification queries).

---

### Security summary

The database has five overlapping defenses. Any one of them failing alone is not enough to compromise the data.

| Layer | Mechanism |
| --- | --- |
| 1. Network isolation | RDS in a private VPC subnet. Not on the public internet |
| 2. Firewall (security groups) | RDS port 5432 reachable only from inside the VPC (Cloud9, Fargate, audit Lambda) |
| 3. Authentication | Per-developer PostgreSQL roles; DDL-only superuser separated into `$SUPERUSER_URL` |
| 4. Authorization (write attribution) | `set_audit_fields()` BEFORE trigger refuses INSERT/UPDATE from unregistered users |
| 5. Transport + at-rest encryption | TLS-required connections (RDS enforces); AWS-managed encryption on RDS storage + S3 buckets |

**Public vs. private surface (deliberate):**

- **Public:** `coeqwal.org` (the website) and `api.coeqwal.org` (the JSON API, with CORS restricting which sites can call it).
- **Private:** the RDS database itself, the Cloud9 IDE, the Batch ETL jobs, and the audit Lambda. Nobody outside the AWS account can reach these directly.

The website is public, the database is not, and even a connection bypassing the network firewall would still be blocked from writing by an in-database trigger that requires an identified human.

> Source: Slides 0.75, 15, 16, 19; `docs/INFRASTRUCTURE.md` § 3.3 (networking), § 3.4 (encryption), § 7.11 (CORS).

---

### Data provenance summary

Every row in the database can answer four basic questions: where it came from, who put it there, when, and what schema version it conforms to. The mechanisms below are how.

| Provenance question | How the database answers it |
| --- | --- |
| Where did the data come from? | `source` + `source_id` (FK to lookup), `model_source_id` on Layer 02 network and Layer 03 entity rows. Descriptive columns like `community_agency`, `point_of_diversion`, `provider` carry informal origin notes |
| Who put it there or changed it? | `created_by`, `updated_by` columns → `developer` table, populated by the `set_audit_fields()` BEFORE trigger. Unregistered writes are refused |
| When was it put in or changed? | `created_at`, `updated_at` set by the same trigger |
| What ETL run produced it? | Today: implicit via `created_by`. Planned: `data_load_log` table linking each row to a specific Batch ETL run |
| What schema version is it under? | `version_family` (14 domains) + `version` + `domain_family_map` (93 tables → families). Independent per domain so a change in one domain does not cascade |
| What CalSim run is it from? | `scenario_short_code` on every statistics row → `scenario` → `hydroclimate` → `model_source` |

**In one sentence:** every row in the database can answer where it came from, who put it there, when, and what schema version it conforms to - by design, without consulting a notebook or asking a person.

---

## The API: serving the database to the website and the public

The database publishes through a FastAPI service at `api.coeqwal.org`. This is what the COEQWAL website hits for every scenario comparison, every reservoir chart, every Delta map tile. It is also a public JSON endpoint: anyone with the URL can query.

### What it serves

16 route modules, grouped by domain:

- **Reservoir statistics** - monthly percentiles, storage, spill, period summaries for the 8 major reservoirs (or filter by group: `major`, `cvp`, `swp`)
- **Network** - nodes, arcs, and spatial queries against the PostGIS geometry layer
- **Scenarios** - scenario list, tier results, tier maps per indicator
- **Demand units** - urban + ag, with monthly and period-summary endpoints
- **M&I contractors** - SWP / CVP contractor-level rollups
- **Aggregates** - CalSim project-level rollups (SWP total, SWP NOD/SOD, CVP NOD/SOD, MWD)
- **Refuge, Delta, Environmental flow, Verification, Bulk download** - one route module each

Every endpoint is FastAPI + Pydantic. The response shape is validated against a typed schema before it leaves the server, and the OpenAPI / Swagger documentation at `/docs` is auto-generated from those schemas.

### One example call

```bash
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/reservoir-percentiles?group=major"
```

Returns JSON: 8 reservoirs x 12 water months of percentile bands (p5, p25, p50, p75, p95), straight from `reservoir_percentile_monthly`, indexed on `(scenario_short_code, reservoir_entity_id)`. The website renders the box-and-whisker plot directly from this payload.

### Tech and infrastructure

- **Language / framework:** Python + FastAPI
- **Schema validation:** Pydantic models for every response
- **Database driver:** `psycopg2` / `asyncpg`, connection pool 5-50 (auto-scaling)
- **Deployment:** GitHub Actions builds a Docker image, pushes to ECR, ECS Fargate pulls and runs it on `git push origin main`
- **Routing:** Internet -> Route 53 -> ALB -> ECS Fargate -> PostgreSQL RDS
- **TLS:** AWS Certificate Manager, terminated at the ALB (`api.coeqwal.org` matches one cert, the website hostnames match another)
- **Response time:** 50-300 ms typical for spatial and statistics queries. The database's pre-aggregated `*_period_summary` and `*_monthly` tables are what make this fast

### How the API stays in sync with the database

The API and the database share the schema by design. When a developer adds a new table or column in the database (DDL), the corresponding Pydantic model and route get added in the API repo. CI lint (`ruff check .`) runs on every push. If the API and the database drift, the failing endpoint is the failure signal - there is no "stale materialized view" problem.

### Live docs and source

- **Interactive docs:** [api.coeqwal.org/docs](https://api.coeqwal.org/docs) - every endpoint, every parameter, try-it-now from the browser
- **Health check:** [api.coeqwal.org/api/health](https://api.coeqwal.org/api/health)
- **Source:** `api/coeqwal-api/` in this repo. `main.py` wires the routes; `routes/*.py` is one file per domain.
- **API README:** [api/coeqwal-api/README.md](https://github.com/berkeley-gif/coeqwal-data-platform/blob/main/api/coeqwal-api/README.md) - deployment, local dev, full endpoint reference

