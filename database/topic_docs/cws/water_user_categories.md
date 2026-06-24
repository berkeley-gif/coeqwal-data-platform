# Water user categories in the database

How CalSim demand units, M&I contractors, CWS aggregates, and drinking-water utilities relate in the COEQWAL schema. Every claim below is verified against the May 24 2026 monthly audit snapshot (`audits/monthly_20260524_143951/layer_exports/`) unless noted.

Related: [`demand_unit_geometry.md`](../demand_unit_geometry.md)

---

## Quick reference

| Term you might hear | DB table(s) | Grain | Row count (May 2026 audit) |
|---|---|---|---|
| Urban demand unit (DU) | `du_urban_entity` | One CalSim urban `du_id` | 145 |
| Agricultural DU | `du_agriculture_entity` | One CalSim ag `du_id` | 144 |
| Refuge DU | `du_refuge_entity` | One refuge `du_id` | 18 |
| M&I contractor | `mi_contractor` | One SWP/CVP contracting agency | 30 |
| Contractor delivery arc | `mi_contractor_delivery_arc` | One CalSim delivery arc per contractor | 39 |
| DU delivery arc | `du_urban_delivery_arc` | One CalSim arc summed into one urban DU | 57 |
| DU CalSim variables | `du_urban_variable` | One row per DU with delivery/demand variable names | 90 |
| CWS aggregate | `cws_aggregate_entity` | One project-region delivery rollup | 6 |
| Tier focal urban DUs | `du_urban_group_member` where group = `tier` | Subset of urban DUs in tier matrix | 71 members (May 2026 audit) |
| Drinking-water utility name | `du_urban_entity.community_agency` (free text) | Not normalized | 145 rows, text varies |

There is **no** standalone `cws_entity` or `drinking_water_utility` table today.

---

## Urban demand unit (`du_urban_entity`)

**What it is:** CalSim spatial unit for urban (M&I) demand. Identified by codes like `02_PU`, `26N_NU1`, `ACFC`, `MWD`.

**Columns that matter:**
- `du_id` is the primary key for joins
- `gw`, `sw` are groundwater / surface-water flags (`VARCHAR(5)` today, `'0'`/`'1'`)
- `community_agency` is free-text communities and agencies served
- `geom` is the polygon from gpkg loader (when present)
- `primary_contractor_short_code` (`VARCHAR(50)`) references `mi_contractor.short_code` by name but is **not** FK-enforced. It is a soft reference, populated on 21 of the 145 rows

**Source tracking:** The seed CSV holds 125 rows: 78 tagged `geopackage,calsim_report`, 22 `calsim_report`, 6 `geopackage`, 19 `tier_matrix`. The live table has 145 rows. The other 20 were backfilled by a later migration (`01g_add_missing_du_urban_entities.sql`) and carry an empty `source`.

---

## M&I contractor (`mi_contractor`)

**What it is:** A water agency that holds an SWP or CVP contract. Stores contract amount in TAF (`contract_amount_taf`), project (`SWP`/`CVP`), region (`NOD`/`SOD`), and type (`MI`, `MWD`, `AG`, etc.).

**Verified May 2026 state:** all 30 rows have `source_file = swp_contractor_perdel_A.wresl` and `project = SWP`. No CVP contractor rows are loaded yet. See [Statistics roadmap, CVP contractors](../../../etl/statistics/README.md#cvp-contractor-load-unfinished).

**What it is NOT:** This table does not store a contract number column. It stores `contract_amount_taf` (Table A allocation for SWP) and `source_contractor_id` (integer from the WRESL source file).

Example rows (from audit CSV):

| short_code | contractor_name | contract_amount_taf | arc_type in delivery table |
|---|---|---:|---|
| ACWD | ALAMEDA COUNTY WD | 20.88 | PMI |
| MWD | METROPOLITAN WDSC | 134.60 | PMI |
| SCVWD | SANTA CLARA VALLEY WD | 297.33 | PMI |

---

## How contractors connect to demand units (verified)

These are **two separate arc tables**. They do not FK to each other.

### `mi_contractor_delivery_arc` (39 rows)

Maps a **contractor** to CalSim **delivery arc** names.

Example: contractor `ACWD` (id=2) has arc `D_SBA029_ACWD` with `arc_type = PMI`.

### `du_urban_delivery_arc` (57 rows)

Maps an **urban `du_id`** to one or more CalSim arcs that must be summed.

Example: `AMADR` has arcs `D_TBAUD_AMADR_NU` and `D_TGC003_AMADR_NU`.

### `du_urban_variable` (90 rows)

Maps an **urban `du_id`** to CalSim variable names used by the statistics ETL.

Example: `02_PU` has `demand_variable = UD_02_PU`, `delivery_variable = DL_02_PU`.

**Relationship diagram (verified join paths only):**

```
mi_contractor                    du_urban_entity
  |  mi_contractor_delivery_arc      |  du_urban_delivery_arc
  |  (contractor_id -> delivery_arc) |  (du_id -> delivery_arc)
  v                                  v
          CalSim delivery arc name (e.g. D_SBA029_ACWD)
                    |
                    ?  (no explicit FK between the two arc tables;
                        relationship is through CalSim naming only)

du_urban_entity.du_id  --->  du_urban_variable.du_id  (explicit FK)
                         --->  tier_location.location_id (tier catalog)
                         --->  tier_location_result (per-scenario scores)
```

We have **not** verified a complete many-to-many join table linking every contractor to every urban DU. Do not document one until it exists.

---

## CWS aggregate (`cws_aggregate_entity`)

**What it is:** System-level delivery rollups, not individual drinking-water utilities. Each row points to one pair of CalSim aggregate variables (`delivery_variable`, `shortage_variable`).

**Verified rows (6, from audit CSV and seed SQL):**

| short_code | label | project | region | delivery_variable |
|---|---|---|---|---|
| swp_total | SWP Total M&I | SWP | total | DEL_SWP_PMI |
| swp_nod | SWP North | SWP | nod | DEL_SWP_PMI_N |
| swp_sod | SWP South | SWP | sod | DEL_SWP_PMI_S |
| cvp_nod | CVP North | CVP | nod | DEL_CVP_PMI_N |
| cvp_sod | CVP South | CVP | sod | DEL_CVP_PMI_S |
| mwd | Metropolitan Water District | MWD | (null) | DEL_SWP_MWD |

There is **no** `cvp_total` row. Whether one is needed is on the [statistics roadmap](../../../etl/statistics/README.md#cvp_total-aggregate-row-decision-pending).

**What "CWS" means in this codebase (three different uses):**

1. **`cws_aggregate_entity`** is the SWP/CVP/MWD project rollups (table above).
2. **`tier_definition` code `CWS_DEL`** is the tier indicator scoring urban DU deliveries. Locations come from `tier_location`.
3. **Colloquial "CWS focal set"** is the 71 urban DUs in `du_urban_group` group `tier` (May 2026 audit). Subset used for tier matrix scoring.

---

## Drinking-water utilities and (PWSID ?)

There is no normalized utility table. Human-readable utility names live in `du_urban_entity.community_agency` as free text.

Example from seed CSV row `02_PU`:

```
Centerville and Redding Centerville CSD 4510011
```

The trailing 7-digit number (`4510011`) follows the format of a California Public Water System ID (PWSID ?). The CalSim 3 report footnote embedded in seed row `UPANG` states:

> "The California Public Water System identification number (ID) is as used in the DWR Public Water Supply Statistics database."

That footnote comes from the CalSim report PDF, not from a column comment in our schema. We have not validated the numbers against the DWR database.

---

## Tier map uses urban DUs, not contractors

`tier_location` with `location_type = demand_unit` references `du_id` values. CWS_DEL uses urban DUs. AG_REV uses agricultural DUs (via `TIER_ATTRIBUTE_OVERRIDES` / `TIER_GEOMETRY_OVERRIDES` in [`etl/common/tier_location_entities.py`](../../../etl/common/tier_location_entities.py)).

Contractors appear in M&I statistics views, not in the tier location catalog.

---

## Roadmap

This is a `database/` doc about schema entities, but the work for the first three items is executed by the statistics ETL, so each is tracked in the statistics roadmap.

| Item | What it means | Track in |
|---|---|---|
| Load CVP contractors into `mi_contractor` | The M&I statistics pipeline populating `mi_contractor` / `mi_contractor_delivery_arc` | [statistics roadmap](../../../etl/statistics/README.md#cvp-contractor-load-unfinished) |
| Decide on `cvp_total` aggregate row | A seed row plus an ETL path in `etl/statistics/cws_aggregate/` | [statistics roadmap](../../../etl/statistics/README.md#cvp_total-aggregate-row-decision-pending) |
| Master crosswalk vs `du_urban_variable` | Reconciling the statistics input mapping | [statistics roadmap](../../../etl/statistics/README.md#master-crosswalk-vs-du_urban_variable) |
| Normalize PWSID (PWSID ?) into lookup table | | Future schema work |
