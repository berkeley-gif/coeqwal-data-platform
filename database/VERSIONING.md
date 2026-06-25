# Versioning in the COEQWAL Scenarios Database

This document describes the **versioning subsystem** in Layer 00 of the database: the four tables, the 14 version families, which domain tables actually carry version columns, the authoring audit (`set_audit_fields`), how versioning is *intended* to be bumped, and the gaps that remain.

The information here is sourced from the audit snapshot at `audits/monthly_20260524_143951/schema_snapshot.json` and the layer exports under that same audit folder. Re-run `python database/audit/run_monthly_audit.py` to refresh.

## Status, a working audit core and aspirational versioning

Two things are working today, three things are scaffolding.

| Component | Status |
|---|---|
| `developer` registry + `coeqwal_current_operator()` | **Working:** Every domain row is attributed to a developer or the system account via `set_audit_fields`. |
| `set_audit_fields` BEFORE INSERT/UPDATE trigger | **Working:** Attached to 91 of 96 public tables. Fires reliably. |
| `version_family` + `version` tables | **Catalogued, not exercised:** All 14 families exist and sit at the initial `1.0.0` seed. No family has ever been bumped. |
| `*_version_id` columns on domain tables | **Partial and inconsistent:** 14 tables carry the column out of ~96 public tables. No FK enforcement. See pattern below. |
| `VersioningManager` Python helper | **Dead code:** Defined in `database/utils/versioning_utils.py`. Not imported by any other module in the repository. |

**The intent:** A row in `version` per (family, version_number) acts as a stable handle, and domain rows carry that handle on a `*_version_id` column so historical and current data can coexist. This has not ever been exercised. Until a real version bump happens (most likely on `scenario`, `tier`, or `network`), the system is documentation and scaffolding, not a running version-control mechanism.

## The four Layer 00 tables

| Table | Rows | What it holds |
|---|---:|---|
| `version_family` | 14 | The catalog of versioning domains (`theme`, `scenario`, `assumption`, `operation`, `hydroclimate`, `variable`, `statistics`, `tier`, `geospatial`, `interpretive`, `metadata`, `network`, `entity`, `audit`) |
| `version` | 14 | One row per family. Every row has `version_number = '1.0.0'`, `is_active = true` |
| `developer` | 6 | Developer registry. `id = 1` is the system bootstrap account |
| `domain_family_map` | 93 | Maps every domain table to its `version_family` |

Full column lists, FKs, indexes, and unique constraints for these tables live in `database/schema/ERD.md` under § Layer 00.

## The 14 version families and current versions

Every family sits at `1.0.0` with `is_active = true`. The `updated_at` timestamps reflect metadata touches (description tweaks, activating the family), not version bumps.

| id | short_code | active version | changelog row | created | last touched |
|---:|---|---|---|---|---|
| 1 | `theme` | 1.0.0 | Initial theme version | 2025-09-01 | 2026-03-01 |
| 2 | `scenario` | 1.0.0 | Initial scenario version | 2025-09-01 | 2026-03-01 |
| 3 | `assumption` | 1.0.0 | Initial assumption version | 2025-09-01 | 2026-03-01 |
| 4 | `operation` | 1.0.0 | Initial operation version | 2025-09-01 | 2026-03-01 |
| 5 | `hydroclimate` | 1.0.0 | Initial hydroclimate version | 2025-09-01 | 2026-03-01 |
| 6 | `variable` | 1.0.0 | Initial variable version | 2025-09-01 | 2026-03-01 |
| 7 | `statistics` | 1.0.0 | Initial outcome version | 2025-09-01 | 2026-03-01 |
| 8 | `tier` | 1.0.0 | Initial tier version | 2025-09-01 | 2026-03-01 |
| 9 | `geospatial` | 1.0.0 | Initial geospatial version | 2025-09-01 | 2026-03-01 |
| 10 | `interpretive` | 1.0.0 | Initial interpretive version | 2025-09-01 | 2026-03-01 |
| 11 | `metadata` | 1.0.0 | Initial metadata version | 2025-09-01 | 2026-03-01 |
| 12 | `network` | 1.0.0 | Initial network version | 2025-09-01 | 2026-03-01 |
| 13 | `entity` | 1.0.0 | Initial entity version | 2025-09-04 | 2026-03-14 |
| 14 | `audit` | 1.0.0 | Initial audit version | 2026-02-15 | 2026-03-14 |

## Which domain tables carry a `*_version_id` column

Fourteen columns across fourteen tables. The pattern is **trunk-and-fully-covered-domain**, but applied unevenly.

### The pattern

Three rules describe most of what's there:

1. **Trunk rule:** the single "root" table of each domain family carries `<family>_version_id`. For example, the `scenario` table carries `scenario_version_id`, `theme` carries `theme_version_id`, `hydroclimate` carries `hydroclimate_version_id`. Sub-tables, link tables, group/member tables, and tag tables in the same layer don't carry a version column. They inherit version through their FK to the trunk.
2. **Full-domain rule:** in two families the version column reaches beyond the trunk. The **network** family stamps `network_version_id` on all four Layer 02 tables (`network`, `network_arc`, `network_node`, `network_gis`) because they were loaded together from a single GeoPackage import with one uniform version stamp. The **tier** family stamps `tier_version_id` on the definitions table (`tier_definition`) **and** on both Layer 10 result tables (`tier_result`, `tier_location_result`) so a result row can be joined back to the exact tier definition version that produced it. Tier is the only family where a results-layer table carries a version column.
3. **Lookup-and-link rule:** Layer 01 LOOKUP tables, all link tables (`*_link`, `*_member`), and all author/tag tables (`scenario_author`, `scenario_tag`, `scenario_tag_link`, `theme_scenario_link`) intentionally don't carry a version column. Lookups are considered version-stable. Link tables inherit via FK.

### Per-layer breakdown

| Layer | Tables w/ `*_version_id` | Tables w/o | Notes |
|---|---|---|---|
| 00 VERSIONING | 0 | 5 | n/a (the layer itself) |
| 01 LOOKUP | 0 | 13 | By design (rule 3) |
| 02 NETWORK | **4 of 4** | 0 | Full-domain coverage (rule 2) |
| 03 ENTITY | 2 of 19 (`channel_entity`, `reservoir_entity`) | 17 | Trunk rule applied unevenly. See gap below. |
| 04 VARIABLE | 1 of 5 (`channel_variable`) | 4 | Trunk rule applied unevenly. `du_urban_variable` does not carry one. |
| 05 ASSUMPTIONS+OPERATIONS | 0 | 6 | Not retrofitted. See gap below. |
| 06 SCENARIO | 1 of 4 (`scenario`) | 3 | Clean trunk rule. Author/tag/tag-link inherit via FK. |
| 07 HYDROCLIMATE | 1 of 2 (`hydroclimate`) | 1 | `slr` is treated as lookup-like. |
| 08 THEME | 1 of 2 (`theme`) | 1 | Clean trunk rule. |
| 09 TIER | 1 of 2 (`tier_definition`) | 1 (`tier_location`) | `tier_location` is a foreign-key bridge with no version column. |
| 10+ RESULTS | 2 (`tier_result`, `tier_location_result`) | 33 | Only the tier results carry a version column. All other result tables key by `scenario_id` and inherit version transitively. |

### Where the pattern breaks down

- **Layer 03 ENTITY:** Of the ~17 "trunk" entity tables (one per operational entity type), only `channel_entity` and `reservoir_entity` carry `entity_version_id`. The other entity roots (`du_urban_entity`, `du_agriculture_entity`, `du_refuge_entity`, `mi_contractor`, `ag_aggregate_entity`, `cws_aggregate_entity`, `wba`, `compliance_station`) don't. The two that have it both happen to share the same family (`entity`, id=13) and the same default (1).
- **Layer 04 VARIABLE:** `channel_variable` carries `variable_version_id`, `du_urban_variable` doesn't.
- **Layer 05 ASSUMPTIONS+OPERATIONS:** No table in this layer carries any version column, even though `assumption_definition` and `operation_definition` are the trunk tables of their respective families and have rows in `domain_family_map` mapping them to families 3 and 4.
- **Default values are inconsistent:** Some columns default to `1` (the `version.id` of the first seed, which only matches the row's actual family if the family is family-1, i.e. `theme`). Others default to the family id directly (network = 12, tier = 8). Practically every existing domain row has the same value as its column default. Nothing has been written with a non-default version id. See `database/schema/ERD.md` § Layer 00 for the per-column default table.
- **No FK enforcement:** None of the 14 `*_version_id` columns has a FK constraint pointing at `version.id`. You could write any integer and the database wouldn't object.

## The authoring audit (`set_audit_fields`)

This is the part of the system that **is** working. It is independent of the version-bumping machinery, and it solves the "who touched this row" question.

### How it works

Every public table that opts in carries a trigger named `audit_fields_<table>` that fires `BEFORE INSERT OR UPDATE` and calls the `set_audit_fields()` function. On INSERT it populates `created_at`, `updated_at`, `created_by`, `updated_by`. On UPDATE it refreshes `updated_at` and `updated_by`. The `_by` columns get their value from `coeqwal_current_operator()`, which resolves `session_user` to a `developer.id` by matching on `aws_sso_username`, then email, then name, then display name. If `session_user = 'postgres'`, the function returns 1 (the System bootstrap account). If no match is found at all, the function raises an exception, so misconfigured connections fail loudly rather than silently writing garbage.

The helper `apply_audit_trigger_to_table(text)` is the standard way to attach the trigger to a new table.

### The known superuser issue

The function has one explicit fallback: `session_user = 'postgres'` returns 1 (the System bootstrap account). This is the right call for DDL migrations run as the shared `postgres` role (those should attribute to System), but it has a real consequence for DML. Any write performed while connected as `postgres` attributes to System and the actual operator is lost. This is what produces the `created_by = System` stamp on most CalSim-derived Layer 11 result rows (`ag_*`, `cws_*`, `delta_*`, `du_delivery_monthly`, `du_period_summary`, `du_shortage_monthly`, `mi_*`): those ETL runs connected via the shared postgres credential.

Two paths to address this if better attribution is wanted:

- Run ETL connected as a developer role (e.g. an `etl` service account with its own row in `developer`) rather than as `postgres`.
- Add a session-variable hook to `coeqwal_current_operator()` so a caller running as postgres can declare `SET LOCAL coeqwal.operator = 'elehmer'` (or similar) at the start of a transaction, and the function reads that instead of falling back to System.

### Coverage and attribution at the audit snapshot

- **91 of 96 public tables** carry the trigger. The five that don't: `tier_location` (intentional, link table), and four gaps: `scenario_backup`, `sensitivity_climate`, `sensitivity_operational`, `spatial_ref_sys`. The first three are tracked in `database/SCHEMA_BACKLOG.md` § 4. `spatial_ref_sys` is PostGIS extension-managed.
- **Attribution coverage of Layer 00-09 reference data (~19,400 rows across 60 tables):** 99.97% `created_by = Jill Fantauzza`. The 6 exceptions are 5 rows attributed to System (largely auto-populated rows in `developer` itself) and 1 NULL row in `hydroclimate` that predates the trigger.
- **Layer 11 result tables (the ETL-written tables):** attribution splits cleanly along two channels.
  - Result families with `created_by = System` (id=1): `ag_*`, `cws_*`, `delta_*`, `du_delivery_monthly`, `du_period_summary`, `du_shortage_monthly`, `mi_*`. These were inserted when ETL ran connected as the postgres user, so the trigger fell back to the system account.
  - Result families with `created_by = Jill`: `env_flow_*`, `refuge_du_*`, `tier_*`. These were inserted while connected as a developer account.
  - All four reservoir result tables show both attributions (`created_by = 1, 2`), reflecting partial reloads across auth contexts.
- **Every developer other than Jill has zero rows authored:** Eric, Brian Galey, Brian Kallay, and Meli are registered in `developer` (added 2026-05-23) but no row in any tracked table has a `created_by` matching them yet.

### Is the authoring audit "working well"?

- **Mechanically:** yes. The trigger fires. The function resolves correctly. Coverage is at 99.97% for reference tables and ~100% for result tables that carry the trigger. The only real attribution failure is 1 row in `hydroclimate` and the 4 gap tables that don't carry the trigger at all.
- **The "System fallback" is the biggest practical gap:** When ETL runs as the postgres user, every row written gets attributed to `developer.id = 1` (System) rather than to the human or pipeline that initiated the run. This is correct given the design (postgres is shared), but it means the audit can't tell you which person or pipeline triggered a given ETL load. Either ETL has to connect as a real developer, or `coeqwal_current_operator()` needs an extension hook (e.g. read a session variable set by the caller) to attribute system-account writes to the actual operator.
- **`updated_by` on result tables is uniformly Jill,** which suggests a post-ETL bulk update touched every row. That's a separate signal (something edited every result row in a single transaction), not a problem per se, but worth being aware of when reading the column.

## Helpers, dead code, and what is actually invoked

| Object | Where | Used? |
|---|---|---|
| `set_audit_fields()` | DB function | **Yes**, by 91 trigger attachments |
| `coeqwal_current_operator()` | DB function | **Yes**, called by `set_audit_fields()` |
| `apply_audit_trigger_to_table(text)` | DB function | **Yes**, called in DDL when adding new tables |
| `get_active_version(text)` | DB function | Defined. Usage outside `database/sql_archive/` is limited to a verification script |
| `register_developer(...)` | DB function | **No**, defined but never used in practice. Live `developer` rows came in via the seed migration (`id` 1-2) and direct `INSERT` (`id` 5-8). See `database/schema/ERD.md` § `developer` |
| `VersioningManager` (Python) | `database/utils/versioning_utils.py` | **No live import:** Reference scaffolding only. |
| `TableManager` (Python) | `database/utils/versioning_utils.py` | **No live import:** Reference scaffolding only. |

## How to bump a version (intended workflow, not yet exercised)

The intended workflow is below. It has not been run end-to-end in production.

1. Decide which family is being bumped. Most common candidates: `scenario` when a new release of the scenario set ships, `tier` when the tier rubric changes, `network` when the topology changes.
2. Insert a new row into `version` for that family with the next `version_number` (e.g. `1.1.0` or `2.0.0`) and `is_active = true`.
3. Set the previously active row for that family to `is_active = false`. There is no DB-level constraint enforcing one-active-per-family. The convention is enforced by the load script.
4. Load new domain rows with their `*_version_id` set to the new `version.id`. Old rows keep their original `*_version_id` so historical data stays addressable by version.
5. Update consumers (API, ETL, notebooks) to filter on the active version where appropriate, typically by resolving `get_active_version('family_short_code')` once at session start and using the returned `version.id` as a parameter, rather than hard-coding a value.

For families that don't currently carry a `*_version_id` column on their tables (most of L03, L04, L05), step 4 has nothing to do. Bumping the version there is documentation-only until the column is added.

## Roadmap

See `database/README.md` § Roadmap for the live list. The versioning-specific items are:

- **Decide whether to keep the system as designed or simplify:** Two reasonable paths: (a) finish the design by adding FK constraints, fixing defaults, and exercising a real bump on `scenario` or `tier`. (b) deprecate the `version` / `VersioningManager` pair and keep only `developer`, `domain_family_map`, and `set_audit_fields`, which are the parts that are paying for themselves.
- **If keeping it:** add FK constraints from every `*_version_id` column to `version.id`. Close the L03 entity gap by adding `entity_version_id` to the missing entity roots (or formally exempting them). Close the L05 gap by adding `assumption_version_id` and `operation_version_id` (or formally exempting them). Decide whether ETL-written Layer 11 result tables should carry `scenario_version_id` so a result row can be tied to a specific scenario version.
- **Authoring audit, signal quality:** Decide whether ETL jobs should connect as real developers (so the System fallback fires less often), and whether `coeqwal_current_operator()` should grow a session-variable hook to attribute system-account writes to the actual operator.
- **`VersioningManager` dead code:** Either wire it into a real consumer (the API is the obvious candidate, when it starts caring about active vs historical versions) or remove the file.
