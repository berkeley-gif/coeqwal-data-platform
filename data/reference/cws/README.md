# CWS and M&I reference spreadsheets

Primary location for community water system (CWS) delivery, demand unit (DU),
and M&I crosswalk spreadsheets the backend consumes. Everything that used
to live under `etl/tier_data/reference/`, `etl/statistics/reference/`, or
a top-level `reference/` directory has been consolidated here.

## Files

| File | Source | Purpose |
|---|---|---|
| `Final_M&Idemandunits_withlatlongs.xlsx` | Kristin Dobson (May 2026) | M&I demand unit list. Centroids, `gw_su`, `sw_du`, optional `Notes`. Read by `etl/tier_data/scripts/reconcile_gw_sw_sources.py`. |
| `Systems_served_by_DU_systemname_updated.xlsx` | earlier delivery | PWS list keyed by DU system name. |
| `Master list of systems served for sw units updated april 13.xlsx` | spring-2026 CWS delivery | PWS master (~476 systems). |
| `Updated HHS allocations May 6 2026.xlsx` | spring-2026 CWS delivery | HHS allocation list (~76 rows). |
| `Updated Master crosswalk SW DUs M&I May7 2026.xlsx` | spring-2026 CWS delivery | SW DU delivery crosswalk (~75 rows). Read by `etl/statistics/scripts/compare_master_crosswalk.py` (not yet implemented). |

## Schema notes

`du_urban_entity.gw` and `.sw` are `VARCHAR(5)` (`'0'` / `'1'` / empty) on live RDS. The team xlsx uses `0` / `1` integers. A `BOOLEAN` migration is on the roadmap, see [`SCHEMA_BACKLOG.md` § 6](../../../database/SCHEMA_BACKLOG.md#6-schema-pattern-inconsistencies) and [`gw_sw_reconciliation.md` Step 7](../../../database/topic_docs/cws/gw_sw_reconciliation.md#step-7-boolean-type-migration-independent-of-value-reconciliation). Value reconciliation vs the CalSim manual is deferred, see
[`gw_sw_reconciliation.md`](../../../database/topic_docs/cws/gw_sw_reconciliation.md).

## Downstream

- Audit script: [`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../../../etl/tier_data/scripts/reconcile_gw_sw_sources.py)
- Crosswalk comparison script: `etl/statistics/scripts/compare_master_crosswalk.py` [not yet implemented]
- CSV conversion and DB staging are documented in
  [`database/README.md`](../../../database/README.md) (spring-2026 CWS section).
