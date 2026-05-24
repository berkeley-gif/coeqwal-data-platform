# Statistics and M&I data roadmap

Deferred work for the statistics / model-run pipeline. Not in scope for
the tier-location data quality batch (Section 1).

---

## CVP contractor load (unfinished)

**Current state (verified May 2026 audit):** all 30 `mi_contractor` rows
came from `swp_contractor_perdel_A.wresl` with `project = SWP`. Zero CVP
rows.

**Schema expectation:** [`database/scripts/sql/12_mi_statistics/03_create_mi_contractor_entity_tables.sql`](../database/scripts/sql/12_mi_statistics/03_create_mi_contractor_entity_tables.sql)
comments reference CVP source files (`nodcvpcontract.table`, etc.).

**Work:**
1. Locate CVP contractor source tables / WRESL files.
2. Load CVP rows into `mi_contractor` and `mi_contractor_delivery_arc`.
3. Re-run M&I statistics ETL and Layer 2 verification for a sample scenario.

---

## `cvp_total` aggregate row (decision pending)

**Current state:** `cws_aggregate_entity` has 6 rows. SWP has `swp_total`
plus NOD/SOD splits. CVP has `cvp_nod` and `cvp_sod` only. No `cvp_total`.

**Question for data team:** should a CVP-wide total row exist (mirroring
`swp_total`), or is NOD+SOD sufficient?

**If yes:** add seed row, delivery variables, and ETL path in
[`etl/statistics/cws_aggregate/`](../etl/statistics/cws_aggregate/).

---

## Master crosswalk vs `du_urban_variable`

**File:** [`etl/statistics/reference/Master crosswalk SW DUs M&I.xlsx`](../etl/statistics/reference/Master%20crosswalk%20SW%20DUs%20M&I.xlsx)

86 rows mapping `du_id` to CalSim `UD_*` demand and `DN_*` delivery variables.

**DB table:** `du_urban_variable` (90 rows in May 2026 audit).

**Work (statistics batch):**
1. Cross-reference xlsx ids against `du_urban_variable`.
2. Identify new, matching, and conflicting rows.
3. Update `du_urban_variable` and re-run urban DU statistics verification.

---

## `gw` / `sw` BOOLEAN migration

**Status:** migration and seed CSV updated (May 2026). Apply on RDS:

```bash
psql "$DATABASE_URL" -f database/scripts/sql/57_du_urban_gw_sw_boolean.sql
```

Then reload `database/seed_tables/04_calsim_data/du_urban_entity.csv` (gw/sw are
`true`/`false`/empty). Ag and refuge entity tables already used BOOLEAN.

**Reader audit:** API routes return booleans from DB. ETL statistics scripts
coerce with `== "1"` when reading CSV. No code change required for DB reads.

---

## Urban gw/sw value reconciliation (deferred)

**Do not bulk-update seed gw/sw values** until the team resolves CalSim manual
vs Kristin xlsx vs tier rules. The audit script remains for investigation only.

**Walkthrough:** [`docs/gw_sw_reconciliation.md`](gw_sw_reconciliation.md)

**Reference data on disk:**
- CalSim manual OR rollup: `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_demand_unit_water_sources.csv`
- Kristin xlsx: `etl/tier_data/reference/Final_M&Idemandunits_withlatlongs.xlsx`
- CWS delivery xlsx: [`data/reference/cws/`](../data/reference/cws/) (move from `audits/cws/`)

**Open items:** `03_PU3` James sign-off, 24 Kristin-vs-CalSim ids, `NAPA2` blank cells.

---

## DU polygon geometry (open decision)

Dissolved gpkg footprints on entity tables are **not approved** for
production loading until the team decides footprint policy. See
[`docs/du_polygon_mapping.md`](du_polygon_mapping.md) (Open decision section).
