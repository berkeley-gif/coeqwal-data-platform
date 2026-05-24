# Tier / M&I reference files

Kristin's urban DU refresh spreadsheet. **CWS delivery xlsx files belong in
[`data/reference/cws/`](../../../data/reference/cws/)**, not here and not under
`audits/`.

## This folder

| File | Purpose |
|---|---|
| `Final_M&Idemandunits_withlatlongs.xlsx` | Centroids, `gw_su`, `sw_du`, optional `Notes` |

gw/sw **column types** on `du_urban_entity` are `VARCHAR(5)` (`'0'` / `'1'` /
empty) on live RDS. BOOLEAN migration is on the roadmap (see
[`docs/TEAM_RUNBOOK.md`](../../../docs/TEAM_RUNBOOK.md) thread R1). **Value**
reconciliation vs CalSim manual is deferred, see
[`docs/gw_sw_reconciliation.md`](../../../docs/gw_sw_reconciliation.md).

## Related

- CWS xlsx: [`data/reference/cws/`](../../../data/reference/cws/)
- Statistics crosswalk: [`etl/statistics/reference/`](../../statistics/reference/)
