# COEQWAL backend team runbook

**Heads up** for priority items for running and developing the website for launch (as opposed to general backend iterative work to keep improving the systems). There are longer timeline tasks to support the database functioning and its role as a queryable resource for researchers at [`database/SCHEMA_BACKLOG.md`](database/SCHEMA_BACKLOG.md). For the full set of improvement work across the backend, see the [Roadmaps section of the root README](README.md#roadmaps), which gathers the deferred and in-progress roadmap kept across section READMEs.

While the website functions as is, there are some improvements that need to be made. These are, in brief:

- Rectify Community Water System (CWS) data so the CWS tier outcomes are fully supported. This means:
  - understanding the final list of cws demand units
  - designing and building the CWS schema to add the water-system layer and link it to the existing urban demand-unit tables (a draft design with open questions lives in the database README Planned tables section)
  - adding the demand units the CWS team has given us that are still missing from the tables, both the `tier_location` ids with no entity row and the rows that still lack geometry
  - reconciling the urban gw/sw flags
  - reconciling the M&I delivery crosswalk
  - rebuilding and republishing the Mapbox tileset via MTS so those polygons render on the map, because the map draws from the tileset and not the database, so a DB-only fix wouldn't reach the map. The MTS workflow is documented in the frontend map package README under "Adding new tile data via Mapbox Tiling Service"
- Get NOD/SOD locations correctly specified in the database and served by the API. Today that membership lives in frontend code instead of coming from the API, due to a quick fix to include NOD/SOD data categories in the radar chart for the last AC meeting.
- Harden the statistics variable lists and calculations. Source the lists from database tables instead of seed tables and lists in scripts, and harden the calculations that read them in collaboration with WAM team.
- Move demand-unit geometry off the entity rows into dedicated geometry tables, the way every other spatial layer already stores it, and stop the monthly audit export from dumping full geometry inline.

---

## Active threads


### A1 Community Water System (CWS) data for tier support

**Goal:** Update drinking-water/CWS delivery data so the CWS_DEL tier outcome is fully supported. The reference spreadsheets are staged in [`data/reference/cws/`](data/reference/cws/), though check with the CWS team to see if they have drifted since May 2026. Four sub-tasks remain:

- **CWS dataset tables:** Finalize and build the CWS schema. A draft design exists in [`database/README.md`, Planned tables](database/README.md#planned-tables-not-yet-in-db) (proposing `cws_entity`, `cws_du_link`, and a list registry), but it has open questions to resolve first, notably whether the CWS list registry should be new tables (`cws_list` / `cws_list_du_member`) or a reuse of the existing `du_urban_group` mechanism, and whether to normalize the utility name / PWSID out of `du_urban_entity.community_agency` free text into its own lookup ([SCHEMA_BACKLOG § 8](database/SCHEMA_BACKLOG.md#8-layer-01-lookup-enforcement)), since the draft `cws_entity` is one row per PWSID. A third is where any CWS geometry (system points or centroids) lives: design it into a dedicated geometry table, not onto the `cws_entity` row, following the pattern in [A5](#a5-demand-unit-geometry-storage-cleanup). To be clear, the CWS tables themselves do get built in the database as part of this task. What is deferred is only the separate migration that moves geometry off the existing `du_*_entity` tables, since the map reads geometry from Mapbox tiles rather than the DB, so that migration is not on the CWS critical path. Resolve the open questions above, then create the CWS tables and load them from the staged spreadsheets.
- **Add the missing demand units, then refresh the map tiles:** Confirm the final CWS demand-unit list with the team first (some entries may also need removing). Then three pieces of work:
  - **Add the ids that have no entity row at all.** A handful of ids appear in the tier map (`tier_location`) but have [no entity row](database/topic_docs/geometry.md#referenced-by-tier_location-but-not-present), so they have neither attributes nor a polygon: the seven CWS_DEL ids (`ACFC`, `KCWA`, `MHILL_NU`, `SBCWD`, `SVWRD`, `TLMNE`, `UNION`) plus the AG_REV id `07S_PA`. Those seven are the same set the database README calls the "master DUs missing from CalSim," listed there next to the `ESB355` HHS-list entry as per-id accept-or-fix decisions ([open questions](database/README.md#planned-tables-not-yet-in-db)). Decide accept vs fix for each, then add the rows.
  - **Load polygons for the rows that have no geometry yet.** The [72 ids that have a row but no polygon](database/topic_docs/geometry.md#appendix-du_ids-without-geometry). This is the same coverage work as A4.
  - **Rebuild and republish the Mapbox demand-units tileset** ([`coeqwal.calsim_demand_units` recipe](scripts/mapbox_recipes/calsim_demand_units.json)) via MTS once the geometry is loaded. The map draws from the tileset and not from the DB `geom`, so a database-only fix never reaches the map. The MTS workflow is in the frontend map package README under "Adding new tile data via Mapbox Tiling Service".
- **gw/sw value reconciliation:** Each urban DU has two flags, `gw` and `sw`, recording whether it draws groundwater, surface water, or both. The same flag pair appears in three places that do not fully agree: the committed seed CSV ([`du_urban_entity.csv`](database/seed_tables/04_calsim_data/du_urban_entity.csv), the values loaded in the DB today), the [curated CalSim Table 3-7 rollup](data/raw/csv_from_CalSim_report_pdf/du+diversion/urban_demand_unit_water_sources.csv), and the [M&I team xlsx](data/reference/cws/Final_M%26Idemandunits_withlatlongs.xlsx). The [reconcile script](etl/tier_data/scripts/reconcile_gw_sw_sources.py) (verified working, it reproduces the May 2026 baseline) and the [walkthrough](database/topic_docs/cws/gw_sw_reconciliation.md) lay the three side by side. It finds 32 seed-vs-xlsx disagreements. Five of them are plain seed errors, rows where both CalSim and the xlsx agree on a value the seed got wrong (`03_PU1`, `24_NU4`, `26N_NU5`, `26N_PU1`, `26S_PU2`); correct those five rows in the seed CSV now. The rest are rows where the seed already matches CalSim and only the xlsx differs, so they are a data-team tier-rule call, not a data fix. Two smaller follow-ups remain on top of that: sign-off on the `03_PU3` override and filling `NAPA2` in the rollup. All of this is itemized in the [walkthrough roadmap](database/topic_docs/cws/gw_sw_reconciliation.md#roadmap-remaining-work); the WAM and Drinking Water/CWS teams are the right reviewers for these decisions. Separately, urban `gw` / `sw` are still stored as `VARCHAR` while the ag and refuge tables are already `BOOLEAN`. That type migration is independent of the value reconciliation ([walkthrough Step 7](database/topic_docs/cws/gw_sw_reconciliation.md#step-7-boolean-type-migration-independent-of-value-reconciliation), [SCHEMA_BACKLOG § 6](database/SCHEMA_BACKLOG.md#6-schema-pattern-inconsistencies)).
- **Master crosswalk vs `du_urban_variable`:** Reconcile the SW-DU M&I delivery-variable crosswalk against `du_urban_variable` ([buckets, script status, per-step detail](etl/statistics/README.md#master-crosswalk-vs-du_urban_variable)), then merge the updated delivery-variable crosswalk in.

**Concepts:** DU vs CWS vs contractor in [`water_user_categories.md`](database/topic_docs/cws/water_user_categories.md).

---

### A2 NOD/SOD demand-unit group membership into the DB and API

**Goal:** NOD/SOD membership for demand units should live in the database and be served by the API. Today the website carries this classification in frontend code instead of reading it from the API.

**Next step:**

- Backfill the 5 empty [`du_urban_group`](database/README.md#demand-unit-group-membership-data-explorer-filters) rows (`nod`, `sod`, `swp_served`, `cvp_served`, `swp_delivery_point`). Urban is already wired: `/demand-units` joins the membership table, defaults to `tier`, and accepts [`?group=`](api/coeqwal-api/README.md#filtering-by-group-cvp--swp-divisions), so backfilling is enough to make `?group=nod` (etc.) return data.
- Add the ag-side [`du_agriculture_group` / `du_agriculture_group_member`](database/README.md#demand-unit-group-membership-data-explorer-filters) tables, mirroring the existing [`du_urban_group` shape in the ERD](database/schema/ERD.md#du_urban_group) (Layer 03). Membership is derivable from `cs3_type` / `provider` / `hydrologic_region_id` on `du_agriculture_entity`. The ag endpoint has no `group` parameter yet, so this also means adding the `?group=` join to `/ag-demand-units`, not just the tables.
- Do the same for refuge if it needs NOD/SOD slicing (no `du_refuge_group` pair or `group` parameter exists yet); otherwise scope this task to urban + ag.
- Optionally, return group-membership flags on the `/demand-units` and `/ag-demand-units` [API](api/coeqwal-api/README.md) rows so the website filters client-side in one fetch, then drop the frontend's hardcoded copy. This is separate from the server-side `?group=` filter.

**Open decisions** (full discussion in the [database roadmap](database/README.md#demand-unit-group-membership-data-explorer-filters)): whether to derive NOD/SOD membership from a rule (`SAC` -> NOD, `SJR` + `TULARE` -> SOD) rather than hand-curating rows, including a WAM-team ruling on the four regions that don't fall cleanly on one side of the Delta (`DELTA`, `SOCAL`, `NC`, `EXPORT`); and whether to consolidate the `du_urban_group` mechanism with the `cws_list` registry so we don't ship two ways to define membership. The reservoir endpoints are the [worked example](api/coeqwal-api/README.md#filtering-by-group-cvp--swp-divisions) of the `*_group` pattern this task extends.

---

### A3 Harden statistics variable lists and calculations

**Goal:** Source the statistics variable lists from database tables (Layer 04) instead of hardcoded Python dicts and seed-CSV prefix derivation, and harden the calculations that read them. Today only DU Urban reads its mapping from SQL (`du_urban_variable`). MI, CWS, Delta, the reservoir thresholds, and the AG aggregate / GW-only sets are Python constants.

**Next step:**

- Build out the [Layer 04 variable tables](database/README.md#layer-04-variable-layer-priority) (`reservoir_variable`, `mi_contractor_variable`, `cws_aggregate_variable`, `ag_variable`, `refuge_variable`, `delta_variable`) and have each statistics module read from the DB ([variable-list migration](etl/statistics/README.md#migrate-module-variable-lists-into-sql-tables)).
- Work the calculation-review items (reservoir spill threshold and volume, and the WAM-team [needs-review backlog](etl/statistics/README.md#needs-review-backlog-wam-team-decisions)).

---

### A4 Tier location coverage gaps (overlaps A1)

**Where we are:** Some tier locations do not fully resolve to an entity row (attributes) and a polygon (geometry). CWS_DEL is the bulk of the gap and is handled in A1. The piece unique to A4 is the AG_REV side: `07S_PA` has no `du_agriculture_entity` row.

**Next step:** Add the `07S_PA` ag row (accept-vs-fix as with any gap). For the CWS_DEL gaps, follow the procedure in [A1](#a1-community-water-system-cws-data-for-tier-support), the same add-rows, load-polygons, republish-tileset flow, tracked together with A1.

**Home:** [`geometry.md`](database/topic_docs/geometry.md): the coverage scorecard ([Coverage](database/topic_docs/geometry.md#coverage)), the tier locations with no geometry ([Referenced by tier_location but not present](database/topic_docs/geometry.md#referenced-by-tier_location-but-not-present)), and the full missing-id list ([Appendix](database/topic_docs/geometry.md#appendix-du_ids-without-geometry)).

---

### A5 Demand-unit geometry storage cleanup

**Now:** DU geometry sits inline on the entity rows. `du_urban_entity`, `du_agriculture_entity`, and `du_refuge_entity` each carry a full `geom` (MultiPolygon) plus a redundant `geom_wkt` text mirror and `srid`. Only `network_gis` and `reservoir` keep geometry in a dedicated table (`wba` and `compliance_station` are inline too, but small and low priority). The DU footprints are the largest and their rows are the most heavily read for non-spatial data, so they are the ones to move. Nothing in the API or frontend reads the DB geometry today, the live map draws polygons from the Mapbox tileset, so it is latent at runtime. See [how geometry is represented now](database/topic_docs/geometry.md#how-geometry-is-represented-now).

**Target:** move DU geometry into a dedicated table keyed by `(du_id, du_class)`, FK-enforced the way `network_gis` links to `network`, drop the `geom_wkt` mirror, and design CWS/PWS geometry the same way. See [how geometry should be represented](database/topic_docs/geometry.md#how-geometry-should-be-represented).

**Next step:**

- Move `geom` / `geom_wkt` / `srid` off the three `du_*_entity` tables into a dedicated geometry table (the pattern `network_gis` and `reservoir` use), then drop the columns. Migration sketch and pickup steps are in [`geometry.md`](database/topic_docs/geometry.md#roadmap) and [SCHEMA_BACKLOG § 6f](database/SCHEMA_BACKLOG.md#6f-demand-unit-geometry-denormalized-on-entity-rows).
- Relocate the misplaced project rows while here: 11 rows whose `du_class` is Agricultural or Refuge (10 ag, 1 refuge) sit in `du_urban_entity`. They got there because the backfill migration [`01g_add_missing_du_urban_entities.sql`](database/sql_archive/03_entity_layers/mi/01g_add_missing_du_urban_entities.sql) misguidedly added missing CalSim project DU (`PU`, `PA`, `PR`) to `du_urban_entity` for completeness (they all have delivery arcs in CalSim), recording each row's `du_class` correctly but leaving the ag and refuge ones in the urban table. Each row self-identifies via `du_class`, so move them to `du_agriculture_entity` / `du_refuge_entity`. While here, also resolve `26N_NA`, which is duplicated across the urban and ag tables.

