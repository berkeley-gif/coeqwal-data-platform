# gw/sw reconciliation walkthrough

Step-by-step guide for urban and agricultural demand-unit groundwater (gw) and surface-water (sw) flags.

**Reconciliation script:** [`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../../../etl/tier_data/scripts/reconcile_gw_sw_sources.py)

---

## Step 0: What we are comparing

Each urban DU row carries two binary flags:

| Column | Meaning |
|---|---|
| `gw` | Demand unit has groundwater-supplied systems |
| `sw` | Demand unit has surface-water-supplied systems |

Both can be `1` (mixed sources). Values are stored as `'0'`/`'1'` strings in seed CSVs today. A migration should move them to `BOOLEAN`.

**Sources:**

| Source | File | Notes |
|---|---|---|
| Seed (committed) | `database/seed_tables/04_calsim_data/du_urban_entity.csv` | Current DB reference. Header is uppercase `DU_ID` |
| Team M&I xlsx | `data/reference/cws/Final_M&Idemandunits_withlatlongs.xlsx` | Columns `gw_su`, `sw_du`, optional `Notes` |
| CalSim PDF rollup (primary) | `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_demand_unit_water_sources.csv` | Curated DU-level Table 3-7 rollup. 111 du_ids, columns `du_id,gw_pdf,sw_pdf`. This is the script's authoritative CalSim source |
| CalSim PDF flat (legacy) | `urban_du_calsim_report.csv` | One row per community/system. 14 du_ids, 124 rows. Used only as a fallback when the curated rollup is absent |
| CalSim PDF rollup (legacy) | `urban_du_calsim_report_rollup.csv` | Naive OR of the 14-du flat extract. Superseded by the curated rollup |
| CalSim PDF text | `urban_du_calsim_report_text.txt` | Table 3-7 layout reference |

Ag gw/sw comes from CalSim Tables 3-3 (SAC) and 3-6 (SJR) only. Tables 3-4 and 3-5 list diversion arcs and have **no** gw/sw columns.

**Seed vs live DB (2026-05-24 audit):** Compared against the live-table export `audits/monthly_20260524_143951/layer_exports/03_entity/du_urban_entity.csv`, the committed seed and the live `du_urban_entity` agree on gw/sw for all 125 shared rows. The live table has 20 additional `_P*` project rows (9 urban, 10 agricultural, 1 refuge) that are not in the seed CSV, so the seed is a strict subset of the live table. See the reload caveat in Step 6.

---

## Step 1: Run the reconciliation script

```bash
cd ~/environment/coeqwal-backend   # or local coeqwal-backend root

python etl/tier_data/scripts/reconcile_gw_sw_sources.py

python etl/tier_data/scripts/reconcile_gw_sw_sources.py \
  --csv-out /tmp/urban_gw_sw_audit.csv
```

Open `/tmp/urban_gw_sw_audit.csv` in a spreadsheet. Sort by `seed_xlsx_agree` and `pattern`.

**Expected baseline (May 2026):**

| Check | Result |
|---|---|
| Urban seed vs xlsx overlap | 120 ids |
| Agree | 88 |
| Disagree | 32 |
| Urban seed vs CalSim manual | 107 overlap, 99 agree, 8 disagree |
| Urban xlsx vs CalSim manual | 110 overlap, 82 agree, 28 disagree |
| Ag SAC 3-3 vs seed | 82/82 agree |
| Ag SJR 3-6 vs seed | 62/62 agree |


---

## Step 2: Understand why urban rows disagree

CalSim Table 3-7 lists **multiple communities per demand unit**. Each community row has its own gw/sw dots in the PDF. The seed CSV stores **one** gw/sw pair per DU. That pair may have been taken from:

- a single "primary" community row,
- a summary interpretation, or
- geopackage-only ingest with empty gw/sw (6 rows in the seed CSV, 4 of them inside the xlsx overlap).

The team xlsx often applies different rules, for example:

- OR across communities: if any system is gw, mark `gw=1`
- Tier analysis override: set `sw=0` even when CalSim shows SW delivery
- Explicit Notes override (see `03_PU3`, `71_NU`, `72_NU`)

**Disagreement patterns among the 32 rows:**

| Pattern | Count | Typical meaning |
|---|---|---|
| `xlsx_adds_gw` | 8 | xlsx marks GW where seed had GW=0 |
| `xlsx_clears_sw` | 5 | xlsx sets SW=0 where seed had SW=1 |
| `seed_empty` | 4 | seed gw/sw blank, xlsx fills values |
| `mixed` | 15 | both flags change |

---

## Step 3: Resolve what we can now

The curated Table 3-7 rollup (`urban_demand_unit_water_sources.csv`) is the CalSim authority. Compared against it, the 32 seed-vs-xlsx disagreements fall into four groups.

**Seed differs from CalSim and the team xlsx agrees with CalSim (safe to update seed):**

| du_id | seed | CalSim | xlsx | Action |
|---|---|---|---|---|
| `03_PU1` | (0,1) | (1,1) | (1,1) | Set seed gw=1 |
| `24_NU4` | (0,1) | (1,0) | (1,0) | Set seed gw=1, sw=0 |
| `26N_NU5` | (empty) | (1,0) | (1,0) | Fill seed gw=1, sw=0 |
| `26N_PU1` | (empty) | (1,1) | (1,1) | Fill seed gw=1, sw=1 |
| `26S_PU2` | (empty) | (1,1) | (1,1) | Fill seed gw=1, sw=1 |

**Seed already matches CalSim and only the xlsx differs (tier-rule override territory, not a CalSim-driven seed fix):**

Most of the 32 disagreements sit here, including `02_PU`, `24_NU1`, `62_NU`, `13_NU1`, `21_PU`, `26S_NU2`. The seed value equals the curated CalSim value, and the xlsx applies a different rule (usually OR-ing gw on, or clearing sw for tier analysis). Do not change seed on CalSim grounds. These move only if the team adopts the xlsx tier rule (Step 5).

**Three-way conflicts (seed, CalSim, and xlsx all differ, defer):**

| du_id | seed | CalSim | xlsx |
|---|---|---|---|
| `60N_NU2` | (0,1) | (0,0) | (1,1) |
| `90_PU` | (empty) | (0,1) | (1,1) |

**Team Notes override (needs explicit sign-off, not CalSim alone):**

| du_id | seed | CalSim | xlsx | Note |
|---|---|---|---|---|
| `03_PU3` | (0,1) | (0,1) | (1,0) | James: not represented in CalSim, ignore for SW deliveries |

CalSim agrees with seed for `03_PU3` (0,1). The xlsx sets sw=0 as a deliberate tier-analysis override, so changing seed needs team sign-off rather than a CalSim-driven edit.

`71_NU` and `72_NU` agree between seed and xlsx (so they are not in the 32) but carry xlsx Notes stating the systems are groundwater-dependent and were removed from the SW-delivery analysis despite CalSim showing SW. No seed change unless the team revises gw/sw.

**Ag:** no seed changes needed. SAC 3-3 and SJR 3-6 match seed 100% (82/82 and 62/62).

---

## Step 4: The curated CalSim extract (mostly done)

The manual Table 3-7 extraction this guide used to ask for already exists: `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_demand_unit_water_sources.csv` (**111 du_ids**, columns `du_id,gw_pdf,sw_pdf`). It is the script's authoritative CalSim source and covers all 32 seed-vs-xlsx disagreements. Only a small residual is left (see Roadmap): some seed du_ids are not in it, and `NAPA2` is present but blank.

### 4a. Source PDF and the two extract shapes

The source PDF is `data/raw/pdf_tables_from_CalSim_report/urban_du.pdf` (Table 3-7 and any continuation tables). Two extract shapes exist:

- **Curated DU-level rollup** (authoritative): `urban_demand_unit_water_sources.csv`, one row per `du_id` with the resolved `gw_pdf,sw_pdf`. The script reads this first.
- **Community-level flat extract** (legacy detail): `urban_du_calsim_report.csv`, schema `page,du_id,communities,id_pwsid_like,gw_bool,sw_bool,point_of_diversion`, one row per community. Currently 14 du_ids / 124 rows. The script falls back to OR-ing this only when the curated rollup is missing.

### 4b. Extending the curated rollup

To resolve a residual du_id (for example `NAPA2`), read its row block in the PDF or text layout, decide the DU-level gw/sw (OR across its community rows, or a curated value where the naive OR is wrong, as was done for `02_PU`), and add or edit the `du_id,gw_pdf,sw_pdf` line in `urban_demand_unit_water_sources.csv`. The community-level flat extract can be extended in parallel for provenance, but the script only needs the curated rollup.

### 4c. Re-run reconcile and fill the audit spreadsheet

```bash
python etl/tier_data/scripts/reconcile_gw_sw_sources.py \
  --csv-out /tmp/urban_gw_sw_audit_v2.csv
```

For each remaining disagreement, add a column `decision` with one of:

| decision | Meaning |
|---|---|
| `keep_seed` | Seed matches intended tier rule |
| `use_xlsx` | Apply team xlsx value |
| `use_pdf_or` | Apply PDF OR rollup |
| `override` | Custom value with note (cite xlsx Notes or team thread) |
| `defer` | Still unclear, stays on roadmap |

---

## Step 5: Define rollup rules (document before bulk seed edit)

Write the rule you used for each bucket:

1. **Default for multi-community DUs:** OR across communities (matches CalSim "any system has this source" interpretation).
2. **Team xlsx overrides:** when `Notes` column documents tier-analysis intent (e.g. ignore SW for units not in CalSim SW delivery).
3. **Seed empty rows:** prefer PDF OR if present, else xlsx, else leave null until verified.

Special cases to decide explicitly:

- Cryptic ids: `NAPA2` is the only disagreement still blank in the curated rollup. `CLLPT`, `PLMAS`, and `ELDID_NU3` are resolved (CalSim 1,1) and are now xlsx-only disagreements (xlsx 0,1).
- Rows where xlsx clears SW but CalSim keeps SW=1 (`15N_NU`, `26N_NU4`, `26S_NU2`, `26S_NU4`, `61_NU1`): tier rule vs CalSim literal.

---

## Step 6: Update seed CSV

Edit `database/seed_tables/04_calsim_data/du_urban_entity.csv` only for rows with a recorded `decision`. Keep `source` accurate:

- `calsim_report` when values trace to PDF
- add `mi_team_xlsx` when xlsx override applies (comma-separated if multiple)

Example change for `03_PU1` (seed differs from CalSim, xlsx agrees):

```csv
"03_PU1",...,"1","1",...
```

**Reload caveat:** the committed seed is missing 20 `_P*` project rows that exist in the live `du_urban_entity` (see Step 0). Use an upsert-style reload keyed on `du_id`, not a full table replace, or those rows will be dropped. Better, reconcile the 20 rows into the seed first so the seed and table match.

Reload seed to RDS using your usual seed refresh path, then re-run monthly audit or spot-check `du_urban_entity` gw/sw columns.

---

## Step 7: BOOLEAN type migration (independent of value reconciliation)

The type change and the value reconciliation in Steps 1-6 are independent jobs. The cast is value-preserving (`'0'` to `false`, `'1'` to `true`, `''` to `NULL`), so it does not depend on which rows are finally marked gw/sw. The real prerequisite is the reader audit, not the value decisions, so the type migration can land before reconciliation finishes. Doing it early adds a database-level guardrail (the column rejects non-boolean values) while the remaining value edits are still happening.

This is the same work as the schema-hygiene item in [`SCHEMA_BACKLOG.md` § 6](../../SCHEMA_BACKLOG.md#6-schema-pattern-inconsistencies) ("Align gw/sw column types across DU entity tables"). `du_agriculture_entity` and `du_refuge_entity` are already `BOOLEAN`. Only `du_urban_entity` is `VARCHAR(5)`, so it is the lone outlier.

The migration has five parts:

1. Convert the urban seed CSV values (`'0'`/`'1'`/empty to `true`/`false`/empty) in `database/seed_tables/04_calsim_data/du_urban_entity.csv`.
2. Write an `information_schema`-guarded SQL migration that `ALTER COLUMN`s `du_urban_entity.gw` and `.sw` from `VARCHAR(5)` to `BOOLEAN NULL`.
3. Update the CREATE TABLE in [`01_create_du_urban_entity.sql`](../../sql_archive/03_entity_layers/mi/01_create_du_urban_entity.sql) to `BOOLEAN`.
4. Update the ERD entry for `du_urban_entity.gw` / `.sw`.
5. Reader audit (the real gating step):
   - `api/coeqwal-api/routes/demand_unit_endpoints.py` returns `e.gw` / `e.sw` as-is. PostgreSQL booleans serialize to JSON booleans, so no API code change is needed.
   - ETL `etl/statistics/ag/calculate_ag_statistics.py` and `etl/statistics/refuge/calculate_refuge_statistics.py` already compare against `'1'` strings, but they read from CSV (not the DB), so they were never affected. Re-verify anyway.
   - Spot-check any ETL DB query that filters with `gw = '1'` instead of `gw IS TRUE`. The May 2026 audit found none.

Tracked in [statistics roadmap](../../../etl/statistics/README.md#gw--sw-boolean-migration) and [`SCHEMA_BACKLOG.md` § 6](../../SCHEMA_BACKLOG.md#6-schema-pattern-inconsistencies).

---

## Roadmap (remaining work)

| Item | Owner | Need |
|---|---|---|
| Apply the 5 safe seed fixes (`03_PU1`, `24_NU4`, `26N_NU5`, `26N_PU1`, `26S_PU2`) where seed differs from CalSim and xlsx agrees | Dev | None |
| Tier-rule policy for the ~24 disagreements where seed matches CalSim but the xlsx differs, plus the 2 three-way conflicts (`60N_NU2`, `90_PU`) | Team | Tier-rule decision |
| `03_PU3` override sign-off | James / M&I team | Notes already captured |
| Fill `NAPA2` in the curated rollup (only disagreement still blank) | Manual extract | PDF row lookup |
| `du_urban_entity.csv` seed update | Dev | Decisions spreadsheet |
| `gw`/`sw` BOOLEAN type migration (urban only; SCHEMA_BACKLOG § 6) | Dev | Reader audit (not value reconciliation) |
| xlsx lat/long ingest (separate from gw/sw) | Deferred | Out of scope ? Is there a need? |

