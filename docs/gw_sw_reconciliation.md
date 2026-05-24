# gw/sw reconciliation (deferred)

**Resolution (May 2026):** `du_urban_entity.gw` and `.sw` are **BOOLEAN**
(migration `57_du_urban_gw_sw_boolean.sql`). Seed CSV uses `true`/`false`/empty.

**Value reconciliation is deferred.** Do not bulk-change gw/sw to match Kristin
xlsx until the team picks tier rules per id. This doc and the audit script
remain reference material.

**Audit script (informational):**
[`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../etl/tier_data/scripts/reconcile_gw_sw_sources.py)

```bash
# Write audit CSV into the repo (not /tmp):
python etl/tier_data/scripts/reconcile_gw_sw_sources.py --csv-out
```

Output: `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_gw_sw_audit.csv`

**Requires current repo** (includes `urban_demand_unit_water_sources.csv`). If
you see "PDF flat/OR extract covers 14 du_ids", you are on an old revision.
Run `git pull`.

---

## Sources

| Source | File |
|---|---|
| Seed | `database/seed_tables/04_calsim_data/du_urban_entity.csv` |
| CalSim manual (Table 3-7 OR) | `data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_demand_unit_water_sources.csv` |
| Kristin xlsx | `etl/tier_data/reference/Final_M&Idemandunits_withlatlongs.xlsx` |
| CWS delivery xlsx | `data/reference/cws/` |

Ag SAC Table 3-3 and SJR Table 3-6 match seed 100%. Tables 3-4 and 3-5 have no
gw/sw columns.

---

## Baseline (when manual CSV and latest script are present)

| Comparison | Agree | Disagree |
|---|---:|---:|
| CalSim manual vs seed | 99/107 | 8 |
| CalSim manual vs Kristin xlsx | 82/110 | 28 |
| Seed vs Kristin xlsx | 88/120 | 32 |

Seed mostly tracks **CalSim manual**, not Kristin xlsx.

---

## Kristin spreadsheet vs CalSim manual

| Pattern | Kristin vs CalSim | Examples |
|---|---|---|
| Adds gw | gw=1 where CalSim gw=0 | `02_PU`, `13_NU1`, `24_NU1` |
| Clears sw | sw=0 where CalSim sw=1 | `15N_NU`, `71_PU1`, `61_NU1` |
| Clears gw | gw=0 where CalSim gw=1 | `CLLPT`, `PLMAS`, `NAPA2` |
| Both change | mixed | `03_PU3`, `60N_PU` |
| xlsx-only ids | not in Table 3-7 | `ACFC`, `KCWA`, `SBCWD` |

---

## Roadmap (when reconciliation resumes)

### PDF-backed seed fixes (candidate)

`03_PU1`, `24_NU4`, `26N_NU5`, `26N_PU1`, `26S_PU2` (seed wrong vs CalSim).

Triple disagreement: `60N_NU2`, `90_PU`. Blank in CalSim: `NAPA2`.

### Team sign-off

**`03_PU3`:** CalSim/seed `(sw=true)` vs Kristin `(sw=false)`. James note:
ignore SW for tier analysis. Pending M&I team.

### After decisions

Update seed, reload RDS, re-run audit script.

---

## Polygon geometry (separate, action item)

Dedicated geometry tables only. Entity-table geom (migration 56) is
deprecated. See [`docs/database_geometry_pattern.md`](database_geometry_pattern.md).
