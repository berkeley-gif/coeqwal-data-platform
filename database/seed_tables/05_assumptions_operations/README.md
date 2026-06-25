# 05_assumptions_operations

Seed data for the assumption and operation reference tables. These hold the policy assumptions and operational rules that scenarios reference.

## Files

| File | DB table |
|------|----------|
| `assumption_category.csv` | `assumption_category` |
| `assumption_definition.csv` | `assumption_definition` |
| `operation_category.csv` | `operation_category` |
| `operation_definition.csv` | `operation_definition` |

`assumption_category` short codes: `land_use`, `gw_model`. `operation_category` short codes: `comm_delivery`, `delta_outflow`, `carryover`, `regulatory_salinity`, `tucp`, `gw_restrictions`, `infrastructure`, `flow`, `biops`.

`assumption_definition` rows carry `assumptions_version_id`, in the `assumption` version family. `operation_definition` rows carry `operation_version_id`, in the `operation` version family. The category tables are not versioned. See [`../../VERSIONING.md`](../../VERSIONING.md) for how version families work.

## Loading

There is no standalone loader in this folder. The tables were created and populated by the scenario-layer migrations now archived under [`../../sql_archive/`](../../sql_archive/). These CSVs are the committed copy of the reference data.

## Not in this folder

- **Parameter tables.** Earlier designs anticipated per-type parameter CSVs (`assumption_param_*`, `operation_param_*`). None were ever built. Parameter detail lives inline in the `description` and `narrative` columns of the definition rows.
- **Link tables.** The assumption-to-scenario and operation-to-scenario links are seeded from [`../06_scenario/`](../06_scenario/) (`scenario_key_assumption_link.csv`, `scenario_key_operation_link.csv`), not here.

## Pending: align these rows with the frontend

Per-scenario operation and assumption metadata is still hardcoded on the website because the definitions kept shifting while the team settled them. Bringing the DB rows up to date so the API can serve them is roadmap work, not a seed-loading task. The sequenced TODOs (operation-link fixes, missing `operation_definition` rows, the frontend icon-id crosswalk, an `is_renderable` flag) live in [`../../README.md`](../../README.md#scenario-assumptions-and-operations-metadata-align-db-with-the-website) § Roadmap.
