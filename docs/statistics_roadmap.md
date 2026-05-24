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

**Current:** `du_urban_entity.gw` and `.sw` are `VARCHAR(5)` with `'0'`/`'1'`.

**Target:** `BOOLEAN NULL` with reader audit across ETL and API.

Tracked in Section 1 Phase 1.4a of the finish plan.

---

## Reference data sources for gw/sw

| Source | Location | Role |
|---|---|---|
| Seed CSV | `database/seed_tables/04_calsim_data/du_urban_entity.csv` | Current committed reference |
| CalSim report PDF | `data/raw/pdf_tables_from_CalSim_report/urban_du.pdf` | Upstream source for urban gw/sw |
| Ag PDF extracts | `data/raw/csv_from_CalSim_report_pdf/du+diversion/*.csv` | Upstream for ag gw/sw |
| M&I team xlsx | `etl/tier_data/reference/Final_M&Idemandunits_withlatlongs.xlsx` | Team refresh, may override seed |

Reconciliation script:
[`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../etl/tier_data/scripts/reconcile_gw_sw_sources.py)

---

## Urban gw/sw reconciliation (in progress)

**Walkthrough:** [`docs/gw_sw_reconciliation.md`](gw_sw_reconciliation.md)

**Status (May 2026):**
- Urban seed vs M&I xlsx: 88/120 agree, **32 disagree** (semantic, not format)
- Ag SAC Table 3-3 vs seed: 82/82 agree
- Ag SJR Table 3-6 vs seed: 62/62 agree
- Urban PDF flat extract: **14 du_ids** only (need full Table 3-7, ~123 ids)
- 3 disagreements resolvable now where xlsx and PDF OR agree (`02_PU`, `24_NU1`, `62_NU`)

**Remaining:**
1. Complete `urban_du_calsim_report.csv` from `urban_du.pdf` (9 pages)
2. Case-by-case decisions for other 29 disagreements
3. Update `du_urban_entity.csv` seed
4. Then `gw`/`sw` BOOLEAN migration (Phase 1.4a)

Ag PDF tables 3-4 and 3-5 have no gw/sw columns (diversion arcs only).
Do not compare them to seed gw/sw.
