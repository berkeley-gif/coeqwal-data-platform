# gw/sw reconciliation walkthrough

Step-by-step guide for urban and agricultural demand-unit groundwater (gw)
and surface-water (sw) flags. Work case by case until patterns are clear.

**Reconciliation script:** [`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../etl/tier_data/scripts/reconcile_gw_sw_sources.py)

---

## Step 0: What we are comparing

Each urban DU row carries two binary flags:

| Column | Meaning |
|---|---|
| `gw` | Demand unit has groundwater-supplied systems |
| `sw` | Demand unit has surface-water-supplied systems |

Both can be `1` (mixed sources). Values are stored as `'0'`/`'1'` strings in
seed CSVs today. A planned migration will move them to `BOOLEAN`.

**Sources:**

| Source | File | Notes |
|---|---|---|
| Seed (committed) | `database/seed_tables/04_calsim_data/du_urban_entity.csv` | Current DB reference |
| Team M&I xlsx | `data/reference/cws/Final_M&Idemandunits_withlatlongs.xlsx` | `gw_su`, `sw_du`, optional `Notes` |
| CalSim PDF flat | `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_du_calsim_report.csv` | One row per community/system |
| CalSim PDF rollup | `urban_du_calsim_report_rollup.csv` | DU-level OR rollup (partial) |
| CalSim PDF text | `urban_du_calsim_report_text.txt` | 9 pages, Table 3-7 layout reference |

Ag gw/sw comes from CalSim Tables 3-3 (SAC) and 3-6 (SJR) only. Tables 3-4
and 3-5 list diversion arcs and have **no** gw/sw columns.

---

## Step 1: Run the reconciliation script

```bash
cd ~/environment/coeqwal-backend   # or local coeqwal-backend root

python etl/tier_data/scripts/reconcile_gw_sw_sources.py

python etl/tier_data/scripts/reconcile_gw_sw_sources.py \
  --csv-out /tmp/urban_gw_sw_audit.csv
```

Open `/tmp/urban_gw_sw_audit.csv` in a spreadsheet. Sort by `seed_xlsx_agree`
and `pattern`.

**Expected baseline (May 2026):**

| Check | Result |
|---|---|
| Urban seed vs xlsx overlap | 120 ids |
| Agree | 88 |
| Disagree | 32 |
| Ag SAC 3-3 vs seed | 82/82 agree |
| Ag SJR 3-6 vs seed | 62/62 agree |

This is **not** a formatting problem. Both sides use clean `'0'`/`'1'`.

---

## Step 2: Understand why urban rows disagree

CalSim Table 3-7 lists **multiple communities per demand unit**. Each
community row has its own gw/sw dots in the PDF. The seed CSV stores **one**
gw/sw pair per DU. That pair may have been taken from:

- a single "primary" community row,
- a summary interpretation, or
- geopackage-only ingest with empty gw/sw (4 seed rows).

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

## Step 3: Resolve what we can now (evidence-backed)

These rows have **partial PDF extract coverage** where xlsx and PDF OR-rollup
agree and seed differs. Safe candidates to update seed after a quick visual
check against Table 3-7:

| du_id | seed | xlsx | PDF OR | Action |
|---|---|---|---|---|
| `02_PU` | (0,1) | (1,1) | (1,1) | Set seed gw=1 |
| `24_NU1` | (0,1) | (1,1) | (1,1) | Set seed gw=1 |
| `62_NU` | (1,0) | (1,1) | (1,1) | Set seed sw=1 |

**Team Notes override (needs explicit sign-off, not PDF OR alone):**

| du_id | seed | xlsx | Note |
|---|---|---|---|
| `03_PU3` | (0,1) | (1,0) | James: not in CalSim for SW deliveries |

`71_NU` and `72_NU` agree between seed and xlsx but have Notes questioning
CalSim SW attribution. No seed change needed unless the team revises gw/sw.

**Ag:** no seed changes needed. SAC and SJR PDF extracts match seed 100%.

---

## Step 4: Complete the urban PDF extract (your manual task)

The flat CSV currently covers **14 du_ids** (124 community rows). The text
extract spans **9 PDF pages** and lists roughly **123 du_ids**. You need a
complete community-level CSV before most disagreements can be verified.

### 4a. Source PDF

`data/raw/pdf_tables_from_CalSim_report/urban_du.pdf` (Table 3-7 and
continuation tables in other hydrologic regions if present).

If the PDF is not on your machine, copy it from the shared CalSim report
bundle or extract from the committed text file layout.

### 4b. Target CSV schema

Match the existing flat extract columns:

```csv
page,du_id,communities,id_pwsid_like,gw_bool,sw_bool,point_of_diversion
```

- `page`: PDF page number (1-9 for SAC region start)
- `du_id`: demand unit id as printed in the PDF (e.g. `02_PU`, `26N_NU4`)
- `communities`: community / agency label from the row
- `id_pwsid_like`: numeric id from PDF footnote column (PWSID-like, not verified)
- `gw_bool`: `1` if GW dot present, else `0`
- `sw_bool`: `1` if SW dot present, else `0`
- `point_of_diversion`: text from last column

Save as:

`data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_du_calsim_report.csv`

Overwrite or version the partial file once complete.

### 4c. Build the DU-level OR rollup

For each `du_id`:

```
gw_or = 1 if ANY community row has gw_bool=1 else 0
sw_or = 1 if ANY community row has sw_bool=1 else 0
```

Save rollup as `urban_du_calsim_report_rollup.csv`:

```csv
du_id,gw_pdf,sw_pdf,n_systems_pdf
02_PU,1,1,8
```

The reconcile script computes OR from the flat file automatically and merges
any pre-built rollup file.

### 4d. Re-run reconcile and fill the audit spreadsheet

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

1. **Default for multi-community DUs:** OR across communities (matches
   CalSim "any system has this source" interpretation).
2. **Team xlsx overrides:** when `Notes` column documents tier-analysis
   intent (e.g. ignore SW for units not in CalSim SW delivery).
3. **Seed empty rows:** prefer PDF OR if present, else xlsx, else leave null
   until verified.

Special cases to decide explicitly:

- Cryptic ids (`CLLPT`, `NAPA2`, `PLMAS`, `ELDID_NU3`): confirm they appear
  in Table 3-7 or a continuation table.
- Rows where xlsx clears SW but PDF OR keeps SW=1: tier rule vs CalSim literal.

---

## Step 6: Update seed CSV

Edit `database/seed_tables/04_calsim_data/du_urban_entity.csv` only for rows
with a recorded `decision`. Keep `source` accurate:

- `calsim_report` when values trace to PDF
- add `mi_team_xlsx` when xlsx override applies (comma-separated if multiple)

Example change for `02_PU`:

```csv
"02_PU",...,"1","1",...
```

Reload seed to RDS using your usual seed refresh path, then re-run monthly
audit or spot-check `du_urban_entity` gw/sw columns.

---

## Step 7: BOOLEAN migration (after seed is stable)

Tracked in [`docs/statistics_roadmap.md`](statistics_roadmap.md) and as
thread R1 in [`docs/TEAM_RUNBOOK.md`](TEAM_RUNBOOK.md).

1. SQL migration: `VARCHAR(5)` to `BOOLEAN NULL` on `du_urban_entity.gw/sw`
   (and ag/refuge if applicable).
2. Update seed CSVs to `true`/`false` or empty.
3. Reader audit: ETL tier scripts, API serializers, frontend consumers.

Do not start BOOL migration until Step 6 disagreements are resolved or
explicitly deferred with documented defaults.

---

## Roadmap (remaining work)

| Item | Owner | Blocker |
|---|---|---|
| Complete urban Table 3-7 flat CSV (111+ missing du_ids) | Manual extract | PDF access + cleanup time |
| Case-by-case decisions for 29 disagreements without PDF row | Team | Flat CSV |
| `03_PU3` override sign-off | James / M&I team | Notes already captured |
| Cryptic urban ids gw/sw (`CLLPT`, `NAPA2`, etc.) | Team | Locate in PDF region tables |
| `du_urban_entity.csv` seed update | Dev | Decisions spreadsheet |
| `gw`/`sw` BOOLEAN migration | Dev | Stable seed |
| xlsx lat/long ingest (separate from gw/sw) | Deferred | Out of scope here |

---

## Polygon loader clarification (separate from gw/sw)

Migration `database/scripts/sql/.archive/56_add_du_geometry_columns.sql` adds
`geom`, `geom_wkt`, and `srid` to the **existing demand-unit entity tables**:

- `du_urban_entity`
- `du_agriculture_entity`
- `du_refuge_entity`

Polygons are not a new table. The loader matches `du_id` from
`database/seed_tables/03_GIS/du_4326.gpkg` and writes the dissolved footprint
into whichever entity row already exists for that id. See
[`load_du_geometries.py`](../database/scripts/data_processing/load_du_geometries.py).
