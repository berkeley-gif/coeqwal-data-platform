# CWS reference spreadsheets

Community water system (CWS) and demand-unit list spreadsheets from the
spring-2026 data delivery.

## Expected files

| File | Purpose |
|---|---|
| `Master demand unit list updated April 13 2026.xlsx` | Project master DU list (~124 rows) |
| `Master list of systems served for sw units updated april 13.xlsx` | PWS master (~476 systems) |
| `Updated HHS allocations May 6 2026.xlsx` | HHS allocation list (~76 rows) |
| `Updated Master crosswalk SW DUs M&I May7 2026.xlsx` | SW DU delivery crosswalk (~75 rows) |

Kristin's urban DU refresh (gw/sw, centroids) stays at
[`etl/tier_data/reference/Final_M&Idemandunits_withlatlongs.xlsx`](../../../etl/tier_data/reference/Final_M&Idemandunits_withlatlongs.xlsx).

Statistics crosswalk duplicate (if present):
[`etl/statistics/reference/Master crosswalk SW DUs M&I.xlsx`](../../../etl/statistics/reference/Master%20crosswalk%20SW%20DUs%20M&I.xlsx).

## Downstream

CSV conversion and DB staging paths are documented in
[`database/README.md`](../../../database/README.md) (spring-2026 CWS section).
gw/sw value reconciliation is deferred. See
[`docs/statistics_roadmap.md`](../../../docs/statistics_roadmap.md).
