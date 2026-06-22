# COEQWAL backend team runbook

Operational guide for in-flight work on the COEQWAL backend. Each
section is a self-contained thread: it states where the work is, what
the next concrete step is, and which files carry the detail.

Use this as your dashboard. Detailed reasoning, audit numbers, and
historical context live in the linked docs.

---

## Quick orientation

| If you are looking for... | Read |
|---|---|
| What this codebase is | [`README.md`](../README.md) |
| The schema | [`database/schema/ERD.md`](../database/schema/ERD.md) |
| What the database looks like right now | `audits/monthly_20260524_143951/report.md` |
| Verification and audit systems | [`etl/verification/README.md`](../etl/verification/README.md) |
| How DU, M&I, contractor, and CWS concepts relate | [`water_user_categories.md`](../database/topic_docs/cws/water_user_categories.md) |
| Statistics and model-run roadmap | [`docs/statistics_roadmap.md`](statistics_roadmap.md) |
| **Copy-paste Cloud9 procedures (threads A1, A2)** | [`docs/CLOUD9_PROCEDURES.md`](CLOUD9_PROCEDURES.md) |

---

## Active threads (next developer can pick these up)

| # | Thread | Blocker |
|---|---|---|
| A1 | Move CWS reference xlsx into the repo | Cloud9 access |
| A2 | Ingest TAIESM1 hydroclimate scenarios | Drive / S3 / Cloud9 access |
| A3 | Reconcile urban `gw` / `sw` values (Kristin vs CalSim) | Data team |
| A4 | `cvp_total` aggregate row | Data team |
| A5 | Reconcile master crosswalk with `du_urban_variable` | Data team |
| A6 | Tier location coverage gaps | Case-by-case decision |
| A7 | How statistics corrections behave (reference, not a task) | n/a |

## Roadmap (rolled back, not in flight)

| # | Thread | Status |
|---|---|---|
| R1 | Convert `du_urban_entity.gw` / `.sw` from VARCHAR(5) to BOOLEAN | Reverted. Live RDS is VARCHAR(5). |
| R2 | Move DU polygon geometry into dedicated tables | Reverted. Live RDS keeps geometry on `du_*_entity` rows. |

R1 and R2 had partial code prep that was rolled back in May 2026
because the team lost the access window needed to apply them safely.
The design intent is preserved in the linked docs. Future developers
can pick them up. See [Roadmap detail](#roadmap-detail-r1-and-r2)
below.

---

### A1. Move CWS reference spreadsheets into the repo

**Where we are.** `data/reference/cws/` exists with a README explaining
the expected files. CWS xlsx files still live under `audits/cws/` on
Cloud9. Reference data does not belong in audit snapshots.

**Files.**

- Target folder: [`data/reference/cws/`](../data/reference/cws/) (tracked, with README)
- Move from: `audits/cws/` (Cloud9 only today)
- **Cloud9 procedure (copy-paste):** [`docs/CLOUD9_PROCEDURES.md`](CLOUD9_PROCEDURES.md) thread A1

All M&I/CWS reference spreadsheets (Kristin's tier xlsx, the M&I
crosswalk, and the spring-2026 CWS delivery files) live together under
[`data/reference/cws/`](../data/reference/cws/). What the team
eventually does with those spreadsheets (CSV conversion, schema design,
DB staging) is out of scope for now and not on this runbook.

---

### A2. Ingest TAIESM1 hydroclimate scenarios

**Where we are.** The `CMIP6_TaiESM1_SSP370` hydroclimate row already
exists in `database/seed_tables/07_hydroclimate/hydroclimate.csv` (id 5,
short_code `CMIP6_TaiESM1_SSP370`, marketed as *Warmer and Drier III*).
The DB carries the hydroclimate label, but the 23 TAIESM1 model-run
scenarios are not yet through the ingestion pipeline.

**Counts (verified May 2026 from `model_run_file_source_working.csv`).**

| HydroClimate label | Total rows | `download_status` |
|---|---:|---|
| 2023 DWR Historical Adjusted | 25 | (blank, ingested) |
| 2023DCR CC50-2043 | 24 | (blank, ingested) |
| 2023DCR CC95-2043 | 24 | (blank, ingested) |
| **COEQWAL TAIESM1** | **23** | **`skip` on all 23** |
| COEQWAL EC-Earth3-Veg | 4 | partial |

All 23 TAIESM1 rows carry the note: *"Pre-S3 backfill: not yet ingested.
Clear download_status when ingesting."*

The 23 scenarios are `s0107` through `s0131` (gaps `s0116`, `s0122`),
each using a different operations baseline (DCR, USBR Alt2/3, SJV/CV
groundwater limits, etc.) crossed with the COEQWAL TAIESM1 climate.

**Files.**

- Working CSV: [`etl/ingestion/scenario_listing/model_run_file_source_working.csv`](../etl/ingestion/scenario_listing/model_run_file_source_working.csv)
- Published CSV: [`etl/ingestion/scenario_listing/model_run_file_source.csv`](../etl/ingestion/scenario_listing/model_run_file_source.csv)
- Pipeline entry: [`etl/ingestion/gdrive_bulk_download.py`](../etl/ingestion/gdrive_bulk_download.py)
- Ingestion tools: [`etl/ingestion/tools/README.md`](../etl/ingestion/tools/README.md)
- Hydroclimate seed: [`database/seed_tables/07_hydroclimate/hydroclimate.csv`](../database/seed_tables/07_hydroclimate/hydroclimate.csv)
- **Cloud9 procedure (copy-paste with verification at each step):**
  [`docs/CLOUD9_PROCEDURES.md`](CLOUD9_PROCEDURES.md) thread A2

The procedure walks through: clearing `download_status` for the 23 rows,
refreshing `ETL_SCENARIOS`, running `gdrive_bulk_download.py`, auditing
Batch extractions, running statistics ETL, and Layer-2/3 verification.

**Watch out for.**

- One scenario (`s0128`) has a known **FOLDER_MISMATCH** flag in
  `etl/ingestion/audit_reports/scan_audit.csv`. The Drive folder name
  differs from the recorded `drive_folder_name`. Resolve before
  ingesting that one.
- TAIESM1 SV path for `s0107` is `SV_COEQWAL_TAIESM1_20260309.dss`
  while the other 22 use `coeqwal_s9999_SV_v*.dss`. Confirm the
  container picks up the right SV when extracting.
- The hydroclimate row uses `short_code = CMIP6_TaiESM1_SSP370` but the
  scenario CSV uses label `COEQWAL TAIESM1`. The ETL maps them through
  the scenario listing. Do not rename either in isolation.

---

### A3. Reconcile urban `gw` / `sw` values (Kristin xlsx vs CalSim manual)

**Where we are.** Audit script and manual Table 3-7 extract are
committed. Decisions about which source wins per id are deferred until
the team has time and context. Column types stay VARCHAR(5) (R1).

**Files.**

- Walkthrough: [`gw_sw_reconciliation.md`](../database/topic_docs/cws/gw_sw_reconciliation.md)
- Audit script: [`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../etl/tier_data/scripts/reconcile_gw_sw_sources.py)
- CalSim Table 3-7 OR rollup: `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_demand_unit_water_sources.csv`
- Kristin xlsx: `data/reference/cws/Final_M&Idemandunits_withlatlongs.xlsx`
- Latest audit CSV: `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_gw_sw_audit.csv`

**Buckets (May 2026 reconcile run).**

| Bucket | Count | Action |
|---|---|---|
| Seed wrong vs CalSim | 7 rows listed | Roadmap when reconciliation resumes |
| Seed matches CalSim, Kristin differs | 24+ | Deferred - tier rule decision per id |
| Ag SAC + SJR vs seed | 144 / 144 agree | No change |

The 7 seed-vs-CalSim rows: `03_PU1`, `24_NU4`, `26N_NU5`, `26N_PU1`,
`26S_PU2`, `60N_NU2`, `90_PU`. Also `NAPA2` (blank in CalSim, seed and
Kristin disagree).

**Next step.** Choose authority per id with the data team:

- For each seed-vs-CalSim row, confirm the PDF reading and either
  update the seed CSV or leave a note in the audit doc.
- For the 24+ Kristin-vs-CalSim rows, decide whether tier ETL should
  follow CalSim literal or the team xlsx interpretation.
- `03_PU3` needs explicit sign-off from James (Kristin's `Notes` column
  records his earlier guidance about ignoring SW deliveries).

Re-run the audit script after seed edits to confirm convergence.

---

### A4. `cvp_total` aggregate row

**Where we are.** `cws_aggregate_entity` has six rows: `swp_total`,
`swp_nod`, `swp_sod`, `cvp_nod`, `cvp_sod`, `mwd`. There is no
`cvp_total` mirroring `swp_total`. SWP works because CalSim ships a
single `DEL_SWP_PMI` (unsuffixed) variable; CVP only exposes
`DEL_CVP_PMI_N` and `DEL_CVP_PMI_S`.

**Files.**

- Roadmap entry: [`docs/statistics_roadmap.md`](statistics_roadmap.md)
  (`cvp_total` section).
- Existing aggregate definitions: [`etl/statistics/cws_aggregate/calculate_cws_aggregate_statistics.py`](../etl/statistics/cws_aggregate/calculate_cws_aggregate_statistics.py)
  (`CWS_AGGREGATES` dict, lines ~85-136).
- Table + seed: [`database/sql_archive/03_entity_layers/mi/06_create_cws_aggregate_tables.sql`](../database/sql_archive/03_entity_layers/mi/06_create_cws_aggregate_tables.sql).

**Question for the data team.** Three options:

- **A.** Use a single CalSim-native CVP variable if one exists (would
  mirror SWP). Confirm with M&I team.
- **B.** ETL-computed sum of `cvp_nod` + `cvp_sod` (write a new
  `CWS_AGGREGATES` entry that sums the two existing aggregate values).
- **C.** Leave as-is. Treat NOD + SOD as the default CVP split, no
  total row.

Pick one and the implementation is one ON-CONFLICT INSERT plus (if B)
one dict entry.

---

### A5. Reconcile master crosswalk with `du_urban_variable`

**Where we are.** Comparison is computed and committed as a script
(no DB needed. Reads the audit snapshot). Headline:

| Bucket | Count |
|---|---:|
| match | 17 |
| conflict | 54 (all on `delivery_variable`: xlsx `DN_<id>` vs DB `DL_<id>` / `D_<plant>_<id>`) |
| xlsx-only | 15 |
| db-only | 19 (mostly `_PA` ag-suffix ids - worth confirming they belong in the urban table) |

Decisions about which source wins per id are deferred until the team
has time.

**Files.**

- Comparison script: `etl/statistics/scripts/compare_master_crosswalk.py` [NOT YET IMPLEMENTED]
- xlsx: [`data/reference/cws/Updated Master crosswalk SW DUs M&I May7 2026.xlsx`](../data/reference/cws/Updated%20Master%20crosswalk%20SW%20DUs%20M&I%20May7%202026.xlsx)
- Snapshot CSV: `audits/monthly_20260524_143951/layer_exports/04_variable/du_urban_variable.csv`
- Audit CSV (gitignored, regenerate with `--csv-out` once the script lands):
  `etl/statistics/audit_reports/master_crosswalk_audit.csv`

**Next steps.**

1. Implement `etl/statistics/scripts/compare_master_crosswalk.py` (the comparison logic existed in a prior local checkout but was not committed). Then run with `--csv-out` to refresh the audit CSV against the latest audit snapshot.
2. Decide delivery-variable policy (xlsx wins, DB wins, case-by-case).
   `D_<plant>_<id>` codes may encode the correct CalSim variable for
   that specific DU. Check with the M&I team before normalizing to
   `DN_<id>`.
3. Confirm whether the `_PA` rows in `du_urban_variable` are intentional
   (those look like agricultural rows misfiled in the urban table).
4. Apply seed updates and re-run urban DU statistics verification.

---

### A6. Tier location coverage gaps

**Where we are.** Impact and mitigation already documented. CWS_DEL has
about 7 missing attribute rows and 42 missing geometry rows. AG_REV has
1 missing attribute row.

**Files.**

- How it is consumed: [`demand_unit_geometry.md`](../database/topic_docs/demand_unit_geometry.md#how-it-is-consumed)
- Geometry scorecard: [`demand_unit_geometry.md`](../database/topic_docs/demand_unit_geometry.md#coverage)
- Missing-geometry ids: [`demand_unit_geometry.md`](../database/topic_docs/demand_unit_geometry.md#appendix-du_ids-without-geometry)

**Next step.** Decide accept-vs-fix per Pattern C id using the
"Decisions pending" checklist in `demand_unit_geometry.md`.

---

### A7. How statistics corrections behave (operational reference, not a backlog item)

Not a workstream, but worth knowing before kicking off any rerun.

**Pattern (verified in code, May 2026):** statistics ETL writes are
**per-scenario overwrites**, not appends.

Each module starts with one or more

```sql
DELETE FROM <stats_table> WHERE scenario_short_code = %s
```

then uses

```sql
INSERT ... ON CONFLICT (scenario_short_code, ...) DO UPDATE SET ...
```

keyed on a uniqueness constraint that always includes `scenario_short_code`.

| Consequence | Detail |
|---|---|
| Re-running stats for scenario X | Wipes X's rows in those tables, inserts fresh rows |
| Other scenarios | Untouched |
| Previous statistic values | Not preserved (no audit trail per cell) |
| Re-running with same inputs | Idempotent - identical rows |
| Schema migration (e.g. add a column) | Separate code change. Data fills on next stats run |

Modules following this pattern (from `etl/statistics/`):

- `du_urban/calculate_du_statistics_v2.py`
- `ag/calculate_ag_statistics.py`
- `mi/calculate_mi_statistics.py`
- `refuge/calculate_refuge_statistics.py`
- `env_flows/calculate_env_flow_statistics.py`
- `delta/calculate_delta_statistics.py`
- `cws_aggregate/calculate_cws_aggregate_statistics.py`
- `reservoirs/calculate_reservoir_*.py`

**Reference data is different.** Seed CSVs under
`database/seed_tables/` (entity tables, lookups) reload via `psql \copy`
from migration scripts. They are not on the per-scenario overwrite path
and do not have `scenario_short_code`. Corrections to seed data
overwrite the matching row by primary key.

---

## Roadmap detail (R1 and R2)

These two threads were prepped in May 2026, partially scaffolded in
code, and then rolled back when the access window to apply them on RDS
closed. The repo now describes the live RDS state. The intent docs
linked below stay in place as design references for a future developer.

### R1. `gw` / `sw` BOOLEAN migration (rolled back)

**Live RDS state.** `du_urban_entity.gw` and `.sw` are `VARCHAR(5)`
with `'0'` / `'1'` / empty values. Ag and refuge entity tables already
use `BOOLEAN`.

**Why deferred.** Migration prep was committed but never applied on
RDS. The seed CSV, CREATE TABLE script, ERD entry, and an SQL migration
file all assumed BOOLEAN. With the migration unapplied, the prepared
seed would have failed to load. Cleaner to revert the prep until an
developer can run end-to-end.

**Future pickup.** When the team is ready:

1. Reapply the seed conversion (`'0'` / `'1'` / empty → `'true'` /
   `'false'` / empty) in
   `database/seed_tables/04_calsim_data/du_urban_entity.csv`.
2. Write a SQL migration that does an `information_schema`-guarded
   `ALTER COLUMN` from `VARCHAR(5)` to `BOOLEAN NULL` on
   `du_urban_entity.gw` and `.sw`.
3. Update CREATE TABLE in
   `database/sql_archive/03_entity_layers/mi/01_create_du_urban_entity.sql`
   to `BOOLEAN`.
4. Update ERD entry for `du_urban_entity.gw` / `.sw`.
5. Reader audit:
   - `api/coeqwal-api/routes/demand_unit_endpoints.py` returns
     `e.gw`, `e.sw` as-is. PostgreSQL booleans serialize to JSON
     booleans, no code change needed.
   - ETL: `etl/statistics/ag/calculate_ag_statistics.py` and
     `etl/statistics/refuge/calculate_refuge_statistics.py` already
     compare against `'1'` strings. They read from CSV, not DB, so
     they were never affected, but worth re-verifying.
   - Spot-check any DB query in ETL that filters with `gw = '1'`
     instead of `gw IS TRUE`. As of May 2026, the audit found none.

Reconciliation work (A3) is independent and can proceed against the
current VARCHAR seed.

### R2. Move DU polygon geometry into dedicated tables (rolled back)

**Live RDS state.** Migration
[`56_add_du_geometry_columns.sql`](../database/sql_archive/04_scenario/56_add_du_geometry_columns.sql)
added `geom` / `geom_wkt` / `srid` directly to the three
`du_*_entity` tables. Loader
[`load_du_geometries.py`](../database/scripts/data_processing/load_du_geometries.py)
populates those columns from `database/seed_tables/03_GIS/du_4326.gpkg`.
This contradicts the project's "geometry in dedicated tables" rule
but works and is not blocking anything. The migration SQL now lives in
`database/sql_archive/` along with the rest of the one-shot DDL. The DU columns
exist in RDS, so the loader runs as before. If you ever rebuild the DB
from scratch, re-apply the migration from its archive location.

**When this thread is picked up**, the deferred work is to replace the
on-entity columns with dedicated geometry tables. Draft new
`58_create_du_geometry_tables.sql` and `59_migrate_du_geom_off_entity_tables.sql`
files (neither is on disk today), then update the loader and resolver
registry accordingly. The API itself no longer serves DU geometry (see
`database/README.md` "API conventions, geometry"), so this thread is
purely an ETL refactor. The Mapbox tile build is the only consumer of
the new geometry tables.

**Why deferred.** Drafted CREATE-dedicated-tables and migrate-then-drop
SQL plus a refactored loader, resolver registry, and API endpoint. None
of that was applied on RDS. With production untouched, leaving the
half-prep in place was a footgun (loader and API pointing at tables
that did not exist).

**Future pickup.** The current storage and the roadmap pointer are in
[`demand_unit_geometry.md`](../database/topic_docs/demand_unit_geometry.md#roadmap)
under "Roadmap". Roughly:

1. Decide table layout (three tables vs one `demand_unit_geometry`
   with a `du_class` discriminator). Update the ERD.
2. CREATE migration for the new geometry tables (mirror `reservoir`
   DDL style).
3. Refactor `load_du_geometries.py` to write the dedicated tables.
4. Update `etl/common/tier_location_entities.py` `GeometryResolver`
   for `demand_unit` to point at the new tables. The resolver feeds
   the Mapbox tile-build pipeline. The API does not consume it for
   geometry.
5. Data migration: copy any existing rows from `du_*_entity.geom`
   into the new tables.
6. Drop `geom`, `geom_wkt`, `srid`, and the GiST index from the three
   `du_*_entity` tables.
7. **CWS rollout follow-on:** when `cws_entity` lands, put PWS
   **points** in a dedicated geometry table, not on `cws_entity`
   attribute rows. Same rule as DU.

Footprint policy (dissolved gpkg vs multipart vs PWS union vs
centroid) is a separate open question and does not depend on the table
shape. See [`demand_unit_geometry.md`](../database/topic_docs/demand_unit_geometry.md#where-it-lives).

---

## Cross-cutting reference

### Reference data on disk (no AWS required)

| Artifact | Location |
|---|---|
| Latest DB audit | `audits/monthly_20260524_143951/` |
| CalSim PDF extracts | `data/raw/csv_from_CalSim_report_pdf/du+diversion/` |
| CalSim PDFs | `data/raw/pdf_tables_from_CalSim_report/` |
| M&I, CWS, crosswalk xlsx | `data/reference/cws/` |
| Seed CSVs | `database/seed_tables/` |
| SQL migrations | `database/scripts/sql/` |

### Monthly audit cycle

Re-run the audit before any seed reload or schema change:

```bash
python database/audit/run_monthly_audit.py
```

Output lands in `audits/monthly_YYYYMMDD_HHMMSS/`. Compare row counts
and column types in the new `report.md` to the previous snapshot.

### Local sanity checks (no DB required)

```bash
python etl/tier_data/scripts/reconcile_gw_sw_sources.py --csv-out
```

That writes `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_gw_sw_audit.csv`.
If the script reports "PDF flat/OR extract covers 14 du_ids", the
checkout is stale - `git pull` and rerun.

---

## How to pick up a thread

1. Read the section above for the thread.
2. Open the linked detail doc and the linked source file.
3. Read the relevant `audits/monthly_*/layer_exports/` CSV for ground
   truth about what is currently in the DB.
4. Make a small change. Add it to this runbook if scope grows.

---

## Conventions

- Code comments: no em or en dashes, no semicolon-joined clauses, no
  trailing periods on single-fragment comments. Markdown files like
  this one are not bound by those rules.
- Do not commit or push without an explicit request from the team
  lead.
- Verification is by hand or by audit script. Do not drive a browser
  for visual checks. Describe what a developer should look at instead.

---

## Doc index

| Topic | Doc |
|---|---|
| This runbook | [`docs/TEAM_RUNBOOK.md`](TEAM_RUNBOOK.md) |
| Cloud9 procedures (A1, A2) | [`docs/CLOUD9_PROCEDURES.md`](CLOUD9_PROCEDURES.md) |
| Audits + verification | [`etl/verification/README.md`](../etl/verification/README.md) |
| Statistics + M&I roadmap | [`docs/statistics_roadmap.md`](statistics_roadmap.md) |
| gw/sw reconciliation | [`gw_sw_reconciliation.md`](../database/topic_docs/cws/gw_sw_reconciliation.md) |
| Demand-unit geometry (mapping, gap, user impact, target architecture) | [`demand_unit_geometry.md`](../database/topic_docs/demand_unit_geometry.md) |
| Water user categories | [`water_user_categories.md`](../database/topic_docs/cws/water_user_categories.md) |
| Schema ERD | [`database/schema/ERD.md`](../database/schema/ERD.md) |
