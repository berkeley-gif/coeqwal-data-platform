# COEQWAL Scenarios Database - Schema Audit and Corrections Backlog

Crunch through this for a happy database.

This document accompanies the schema in [`schema/ERD.md`](schema/ERD.md). A database audit was run and analyzed on 2026-05-24 (`audits/monthly_20260524_143951/schema_snapshot.json`) to surface the corrections and refinements collected here. None are urgent to the current API or website, but are part of the evolution of the long-term database. The low-risk, mechanical subset is bundled into one idempotent script, [`scripts/sql/audit_cleanup.sql`](scripts/sql/audit_cleanup.sql). Section 0 opens with that script and details exactly what it changes. Run it (dry-run, then apply). After that section are the careful, judgment-required cleanups (sections 1-10).

**Why these inconsistencies exist:** Many of them are historical. The database grew with the project. A table or pattern was built one way, then conditions or decisions changed and later tables were built differently. The audit surfaces where the older and newer conventions now sit side by side, so they can be reconciled deliberately rather than discovered by surprise.

**Two real bugs live on the README roadmap:** The drifted ID sequences (latent `unique_violation`) and the `watershed.hydrologic_region_id` drift are data-correctness bugs, tracked under [`README.md`](README.md) § Roadmap > "Correctness bugs (do regardless)". They are referenced from sections 4 and 5 below.

**Effort scale:** Roughly, S = single SQL statement or trivial change, M = multi-statement migration with planning, L = cross-cutting refactor.

---

## How to use this document

You do not need to read this end-to-end. Run section 0, then consult a later section only when you action its item. The large per-table tables in sections 1-6 are reference, not required reading.

1. **Start with section 0:** Run `audit_cleanup.sql` (dry-run, then apply) to clear the mechanical drift in one pass.
2. **For a single-issue fix:** find the section, read the evidence row, write the migration, run it, check the box.
3. **For a sweep:** tackle one section at a time. § 2 (duplicate indexes) is highest-volume / lowest-risk. § 1 (FK constraints) is highest-value / medium-risk.
4. **After each fix:** re-run `database/audit/run_monthly_audit.py` and verify the corresponding `validation` block (or index / FK count) drops.

Keep this document and the README roadmap in sync as items land.

---

## Table of contents

0. [Run the cleanup script first](#0-run-the-cleanup-script-first)
1. [FK enforcement gaps](#1-fk-enforcement-gaps)
2. [Duplicate and redundant indexes](#2-duplicate-and-redundant-indexes)
3. [Legacy type drift](#3-legacy-type-drift)
4. [Audit trigger coverage and attribution gaps](#4-audit-trigger-coverage-and-attribution-gaps)
5. [Stale artifacts and seed-CSV triage](#5-stale-artifacts-and-seed-csv-triage)
6. [Schema-pattern inconsistencies](#6-schema-pattern-inconsistencies)
7. [Layer 03 entity-table cleanup](#7-layer-03-entity-table-cleanup)
8. [Layer 01 lookup enforcement](#8-layer-01-lookup-enforcement)
9. [Versioning subsystem](#9-versioning-subsystem)
10. [Docs and tooling](#10-docs-and-tooling)
- [Design background, integer IDs vs short codes](#design-background-integer-ids-vs-short-codes)

---

## 0. Run the cleanup script first

This database is audited monthly. `database/audit/run_monthly_audit.py` writes a fresh `schema_snapshot.json` and a report. The findings split two ways: mechanical drift that this section addresses, and decisions or data work that lives in the sections below.

[`scripts/sql/audit_cleanup.sql`](scripts/sql/audit_cleanup.sql) is the one-shot for the mechanical drift. It runs in two modes: a dry-run that enumerates every drift item with per-item feasibility pre-checks (column types, orphan rows on `created_by` / `updated_by`), and an apply pass that performs the DDL. Per-section `EXCEPTION` handlers and idempotency mean both modes are safe to re-run. Apply is the default. Pass `-v dry_run=true` to preview. Both are optional. If the new owner is building a fresh schema, the script is a specification of corrections to fold into the new DDL, not a script to run.

What it covers, at a glance:

| Section | What it does | New objects | Existing objects touched |
|---------|--------------|-------------|--------------------------|
| 0 | Pre-flight check | 0 | 0 |
| 1 | Drop `scenario_backup` | 0 | 1 table dropped |
| 2 | Drop duplicate FKs on `network_entity_type` | 0 | 2 FKs dropped |
| 3 | Rename `ag_du_delivery_monthly_id_seq` | 0 | 1 sequence, 1 column default |
| 4 | Attach `audit_fields_tier_location` trigger | 1 trigger | 0 |
| 5 | Resync 5 drifted ID sequences | 0 | 5 sequences |
| 6 | Promote 5 manual-id tables to sequence-backed `id` | 5 sequences | 5 column defaults |
| 7 | Drop 41 redundant indexes from exact-duplicate pairs | 0 | 41 indexes dropped |
| 8 | Drop 38 left-prefix duplicate indexes | 0 | 38 indexes dropped |
| 9 | Add 94 `created_by` / `updated_by` FKs to `developer.id` | 94 FKs | 0 |
| 10 | `is_active integer` to `boolean` on 3 tables | 0 | 3 columns |
| 11 | `hydroclimate` numeric audit and version columns to integer | 0 | 3 columns |
| 12 | `has_gis_data integer` to `boolean` on 2 tables (`reservoir_entity`, `channel_entity`) | 0 | 2 columns |
| 13 | Backfill the 1 NULL row in `hydroclimate.created_by` | 0 | 1 row |
| 14 | Add `hydroclimate` developer FKs (depends on § 11 and § 13) | 2 FKs | 0 |

### Cloud9 invocation

This script is DDL-heavy, so use `$SUPERUSER_URL`. Postgres 17.4 on RDS, no extensions needed. Per the `database/README.md` Cloud9 workflow.

Logs go to `audits/audit_cleanup/`, next to the monthly audit snapshots. One-time setup if the directory does not exist yet:

```bash
mkdir -p audits/audit_cleanup
```

The directory is local-only by default (`audits/` gitignores everything except `monthly_*/` and `README.md`), so the logs stay on Cloud9.

Dry-run first. Pass the `dry_run` flag:

```bash
cd ~/environment/coeqwal-backend
git pull
psql $SUPERUSER_URL -v dry_run=true \
  -f database/scripts/sql/audit_cleanup.sql \
  | tee audits/audit_cleanup/audit_dry_run_$(date +%Y%m%d_%H%M%S).log
```

Read the per-section log and aggregate summary at the bottom. Rows with status `would_*` are what the script will actually do when applied. Rows with status `would_partial` carry a `would_fail=N` count in `details` with the per-item reason (e.g. `foo.created_by:type-is-numeric` or `bar.updated_by:3-orphan-values`). Address those items first, or accept that the section will land as `partial` with those items skipped.

Apply for real. Same command, no flag:

```bash
psql $SUPERUSER_URL -f database/scripts/sql/audit_cleanup.sql \
  | tee audits/audit_cleanup/audit_apply_$(date +%Y%m%d_%H%M%S).log
```

Re-run safely. Every section is idempotent. The single signal for "nothing changed" on a clean re-run is `rows_affected = 0` on every row of the per-section log. The `status` column will read `skipped` for single-action sections or `ok` for looping sections, but the count is the authoritative signal in both shapes.

### Running by tier

Sections are grouped into four risk tiers. Pass `-v tier=N` to run one tier, or a comma list like `-v tier=1,2` to run several. Default (`-v tier=all` or unset) runs every section, identical to the pre-tier behavior. Preflight and the final report always run.

- **Tier 1** trivially safe, fully reversible, no data dependency. Sections 1, 5, 7, 8. 89 mechanical fixes.
- **Tier 2** low risk, single-action, contained scope. Sections 2, 3, 4, 13.
- **Tier 3** intrusive but contained, changes future insert behavior. Sections 6, 15.
- **Tier 4** data-dependent, may partial-fail based on data state. Sections 9, 10, 11, 12, 14. Section 12 covers both `reservoir_entity` and `channel_entity`. The dry-run pre-checks in Sections 9 and 14 list any (table, column) pairs whose FK add would fail on the current data (orphan values or wrong column type), so apply shows exactly what will be skipped.

#### Consumer impact audit for the type changes (Sections 10, 11, 12)

The `coeqwal-backend` and `coeqwal-website` repos were grepped on 2026-05-31 for any code that reads the columns whose types change in tier 4. Summary:

- **Backend API (`api/coeqwal-api`):** `is_active` is read on many tables, but only on tables that are already `boolean` (`scenario`, `tier_definition`, `tier_result`, entity tables). The three tables Section 10 converts (`hydroclimate`, `scenario_author`, `theme`) are not queried for `is_active`. `has_gis_data` is not read by any endpoint. `hydroclimate.created_by` / `updated_by` / `hydroclimate_version_id` are not exposed.
- **ETL (`etl/`):** No `INSERT` or `UPDATE` against `hydroclimate`, `reservoir_entity`, or `channel_entity`. Those are seed-loaded reference tables.
- **Frontend:** `has_gis_data` appears once, on `AgDemandUnitListItem`, which maps to `du_agriculture_entity` (already `boolean`). Section 12 does not touch that table. `is_active` appears on `TierListItem` and `ScenarioListItem`, both already sourced from boolean columns. Frontend hooks use truthy filters, not integer equality. No frontend code reads `is_active` from the three Section-10 tables, or `has_gis_data` from `reservoir_entity` / `channel_entity`.

**Net: no consumer should break when tier 4 lands.**

Cautious adoption sequence:

```bash
psql $SUPERUSER_URL -v dry_run=true -f database/scripts/sql/audit_cleanup.sql \
  | tee audits/audit_cleanup/audit_dry_run_$(date +%Y%m%d_%H%M%S).log

psql $SUPERUSER_URL -v tier=1 -f database/scripts/sql/audit_cleanup.sql \
  | tee audits/audit_cleanup/audit_apply_tier1_$(date +%Y%m%d_%H%M%S).log

psql $SUPERUSER_URL -v tier=2 -f database/scripts/sql/audit_cleanup.sql \
  | tee audits/audit_cleanup/audit_apply_tier2_$(date +%Y%m%d_%H%M%S).log
```

Continue with tiers 3 and 4 at your own pace. Within a tier, the new owner can comment out individual `DO $$ ... END $$;` blocks for finer control. Each section is bracketed by `=` separator comments and is independently safe to skip.

If you pass a tier value that is not `all` or a comma list of 1-4 (e.g. `-v tier=5` or `-v tier=foo`), no DDL runs. The preflight row at the top of the report names the bad value and the valid options. Correct the flag and re-run.

### ERD updates after a successful apply

The canonical schema doc ([`schema/ERD.md`](schema/ERD.md)) is hand-maintained and will be out of date in a few places after this script runs. Re-run the monthly audit first to refresh `schema_snapshot.json`, then walk the checklist below against the new snapshot.

```bash
python database/audit/run_monthly_audit.py
```

Checklist of ERD spots to update:

- **Layer 06 - Scenario:** Remove `scenario_backup` from the table listing. It is dropped in Section 1.
- **Layer 02 - Network: `network_entity_type`:** The duplicate `_fkey` FKs go away. The Indexes-and-constraints subsection should drop `network_entity_type_created_by_fkey` and `network_entity_type_updated_by_fkey`. Section 2.
- **Layer 11 - Ag results: `ag_du_demand_monthly`:** Sequence rename note. The `id` default now points at `ag_du_demand_monthly_id_seq`. Section 3.
- **Layer 09 - Tier: `tier_location`:** Triggers subsection gains `audit_fields_tier_location`. The `set_audit_fields` coverage statement on the Layer 00 "Status details" bullet (and the corresponding line in `schema/ERD.md` § Triggers) goes from "91 of 93 audit-bearing tables" to "92 of 92". Section 4.
- **Layer 00 - Versioning: `developer`, `version_family`, `version`:** Sequence `last_value` notes are realigned. Cosmetic only, no schema change. Section 5.
- **Layer 03 - Entity: `reservoir_entity`, `reservoir_group`, `reservoir_group_member`, `du_urban_group`, `mi_contractor_group`:** The `id` column gains `DEFAULT nextval(<table>_id_seq)`. Update the column definition lines in each table's ERD entry. Section 6.
- **All result-table indexes:** Remove the 41 + 38 redundant indexes from each affected table's Indexes subsection. The kept names (canonical `idx_<table>_<columns>`, `<table>_<column>_key`, or `<table>_pkey`) stay. Sections 7 and 8.
- **94 + 2 = 96 new `developer.id` FKs:** Each of the 47 tables in Section 9, plus `hydroclimate` in Section 14, gains FK constraints `fk_<table>_created_by` and `fk_<table>_updated_by` on the corresponding audit columns. Add these to each table's Foreign-keys subsection.
- **`hydroclimate`, `scenario_author`, `theme`:** Column type `is_active`: `integer` becomes `boolean`. Section 10.
- **`hydroclimate`:** Column types `hydroclimate_version_id`, `created_by`, `updated_by`: `numeric` becomes `integer`. Section 11.
- **`reservoir_entity`, `channel_entity`:** Column type `has_gis_data`: `integer` becomes `boolean`, default becomes `false` (a new entity row has no GIS sibling yet). Section 12.
- **Magic-number lookup-ID and version-ID defaults:** 23 columns across 14 tables lose their hardcoded integer defaults. Tables touched: `network`, `network_arc`, `network_gis`, `network_node`, `network_subtype`, `network_type`, `tier_definition`, `tier_location_result`, `tier_result`, `channel_entity`, `reservoir_entity`, `scenario`, `theme`, `channel_variable`. The column definition lines for each `source_id`, `model_source_id`, `network_version_id`, `tier_version_id`, `entity_type_id`, `entity_version_id`, `scenario_version_id`, `theme_version_id`, and `variable_version_id` should drop their `DEFAULT N` clause. Section 15.

The audit snapshot is the source of truth for column types, FKs, indexes, and triggers. After the audit re-runs, every checklist row above is verifiable by reading the new `schema_snapshot.json`.

After a successful apply, move the file into the matching `database/sql_archive/<domain>/` folder per the `database/scripts/sql/README.md` "Adding a new migration" convention.

### If the script is never run

The careful cleanups in sections 1-10 below are the canonical reference. Most can be worked piecemeal as separate PRs. The script is a convenience for the handoff team. Skipping it costs nothing other than the convenience of one-shot application.

What the script does NOT cover, and why those items still need human attention:

- Items that need a data-quality decision first (`reservoir_entity.source_ids` text to array, `hydroclimate.projection_year` text to integer, `channel_entity.hydrologic_region_id` varchar).
- Larger redesigns (`du_wba_link`, sensitivity audit, polymorphic entity FKs). The Layer 04 variable layer and ETL operator-attribution are tracked on the [`README.md`](README.md) § Roadmap > "Data pipeline / ETL", not here.

Effort: M to run the script and verify. The "skipping it" path is split across the individual cleanup items below.

---

## 1. FK enforcement gaps

**Scope:** Every column whose name suggests a foreign key (`*_id`, `*_short_code`, `*_by`, plus the known cross-keys `du_id`, `wba_id`, `calsim_id`, `network_arc_id`, `network_node_id`, `scenario_short_code`, `aggregate_code`, `delivery_arc`, `source`, `location_id`, `tier_short_code`) where no FK constraint exists in `schema_snapshot.json["foreign_keys"]`.

**Why it matters:** Without an FK constraint, Postgres won't catch an orphan write at insert time. A typo in an ETL pipeline (e.g. `du_id = '5_PU_typo'`) silently creates an orphan result row that the website joins to nothing.

### 1a. `created_by` / `updated_by` missing FK to `developer.id`

49 tables carry `created_by` / `updated_by` integer columns (filled by the `set_audit_fields()` trigger) but have no FK constraint linking those columns to `developer.id`. The audit trigger fills these in with whatever `coeqwal_current_operator()` returns. An invalid value would not be caught.

The fix is the same for each, one `ALTER TABLE ... ADD CONSTRAINT` adding the FK to `developer.id` (effort S each), except where noted:

`ag_aggregate_entity`, `ag_aggregate_monthly`, `ag_aggregate_period_summary`, `ag_du_demand_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_period_summary`, `ag_du_shortage_monthly`, `ag_du_sw_delivery_monthly`, `assumption_category`, `assumption_definition`, `cws_aggregate_entity`, `cws_aggregate_monthly`, `cws_aggregate_period_summary`, `delta_monthly`, `delta_period_summary`, `du_agriculture_entity`, `du_delivery_monthly`, `du_period_summary`, `du_shortage_monthly`, `du_urban_delivery_arc`, `du_urban_entity`, `du_urban_group`, `du_urban_group_member`, `du_urban_variable`, `hydroclimate` (also needs the `numeric` to `integer` alter first, see § 3, effort M), `mi_contractor`, `mi_contractor_delivery_arc`, `mi_contractor_group`, `mi_contractor_group_member`, `mi_contractor_period_summary`, `mi_delivery_monthly`, `mi_shortage_monthly`, `operation_category`, `operation_definition`, `refuge_du_delivery_monthly`, `refuge_du_period_summary`, `refuge_du_shortage_monthly`, `reservoir_entity`, `reservoir_group`, `reservoir_group_member`, `reservoir_monthly_percentile`, `reservoir_period_summary`, `reservoir_spill_monthly`, `reservoir_storage_monthly`, `scenario`, `scenario_author`, `scenario_backup` (no FK, drop the table instead, see § 5), `scenario_hydroclimate_sibling`, `theme`.

### 1b. `wba_id` references unenforced

| table | column | type | references (intended) | fix |
|---|---|---|---|---|
| `du_agriculture_entity` | wba_id | varchar | `wba.wba_id` | add FK (varchar-to-varchar, see also §6 WBA roadmap) |
| `du_refuge_entity` | wba_id | varchar | `wba.wba_id` | add FK |
| `du_urban_entity` | wba_id | varchar | `wba.wba_id` | add FK |

### 1c. `*_version_id` not linked to `version.id`

The versioning system catalogs versions but no domain table enforces an FK to them yet. Every `*_version_id` column is unenforced.

| table | column | type | references (intended) | fix |
|---|---|---|---|---|
| `channel_entity` | entity_version_id | integer | `version.id` | add FK or drop (NOT NULL, no default) |
| `channel_variable` | variable_version_id | integer | `version.id` | add FK or drop (nullable, no default) |
| `hydroclimate` | hydroclimate_version_id | numeric | `version.id` | add FK or drop (nullable, no default, also alter to integer, see §3) |
| `reservoir_entity` | entity_version_id | integer | `version.id` | add FK or drop (NOT NULL, no default) |
| `scenario` | scenario_version_id | integer | `version.id` | add FK or drop (nullable, default 1) |
| `theme` | theme_version_id | integer | `version.id` | add FK or drop (NOT NULL, default 1) |

Fix: either add FK constraints to `version.id` per column, or drop the columns if versioning will move to a different model (the versioning system is currently not used, pegged to v 1, see § 9).

### 1d. Soft references in DU and result tables

`du_id varchar` is used as the entity key across 5 DU entity tables and 10 result tables. There is no FK enforcement on any of these. Same pattern for `aggregate_code` (ag aggregate result family) and `network_arc_id` (env-flow result family).

| table | column | type | references (intended) |
|---|---|---|---|
| `du_agriculture_entity` | du_id | varchar | (PK) |
| `du_urban_entity` | du_id | varchar | (PK) |
| `du_refuge_entity` | du_id | varchar | (PK) |
| `ag_du_demand_monthly` | du_id | varchar | du_agriculture_entity.du_id |
| `ag_du_gw_pumping_monthly` | du_id | varchar | du_agriculture_entity.du_id |
| `ag_du_period_summary` | du_id | varchar | du_agriculture_entity.du_id |
| `ag_du_shortage_monthly` | du_id | varchar | du_agriculture_entity.du_id |
| `ag_du_sw_delivery_monthly` | du_id | varchar | du_agriculture_entity.du_id |
| `du_delivery_monthly` | du_id | varchar | du_urban_entity.du_id |
| `du_period_summary` | du_id | varchar | du_urban_entity.du_id |
| `du_shortage_monthly` | du_id | varchar | du_urban_entity.du_id |
| `refuge_du_delivery_monthly` | du_id | varchar | du_refuge_entity.du_id |
| `refuge_du_period_summary` | du_id | varchar | du_refuge_entity.du_id |
| `refuge_du_shortage_monthly` | du_id | varchar | du_refuge_entity.du_id |
| `ag_aggregate_monthly` | aggregate_code | varchar | ag_aggregate_entity.short_code |
| `ag_aggregate_period_summary` | aggregate_code | varchar | ag_aggregate_entity.short_code |
| `env_flow_channel_monthly` | network_arc_id | varchar | channel_entity.network_arc_id |
| `env_flow_channel_period_summary` | network_arc_id | varchar | channel_entity.network_arc_id |
| `env_flow_channel_seasonal` | network_arc_id | varchar | channel_entity.network_arc_id |
| `mi_delivery_monthly` | mi_contractor_code | varchar | mi_contractor.short_code |
| `mi_shortage_monthly` | mi_contractor_code | varchar | mi_contractor.short_code |
| `mi_contractor_period_summary` | mi_contractor_code | varchar | mi_contractor.short_code |

Fix: either add varchar-to-varchar FK constraints or convert these columns to integer FKs and change the ETL load paths. Use `ON DELETE RESTRICT ON UPDATE CASCADE` to match the house convention, same reasoning as § 1e. See §6 for the broader flavor discussion.

### 1e. `scenario_short_code` in 17 result tables not FK-enforced

Every Layer 11 per-scenario statistics table has a `scenario_short_code varchar` column that points at `scenario.short_code`. Currently none of them carry an FK constraint. 17 result tables.

Fix: add `FOREIGN KEY (scenario_short_code) REFERENCES scenario(short_code) ON DELETE RESTRICT ON UPDATE CASCADE` to each, matching the house convention. RESTRICT guards against a single scenario delete silently wiping the ~860k result rows. The ETL already clears results per scenario with `DELETE FROM <table> WHERE scenario_short_code = %s`, so it never relies on a cascade. ON UPDATE CASCADE keeps the result rows in sync if a `short_code` is ever renamed.

### 1f. `scenario.hydroclimate_id`, `hydroclimate.source_id`

| table | column | type | references (intended) | fix |
|---|---|---|---|---|
| `scenario` | hydroclimate_id | integer | `hydroclimate.id` | add FK |
| `hydroclimate` | source_id | integer | `source.id` | add FK |

### 1g. Channel and variable structural columns

`channel_entity` (669 rows) and `channel_variable` carry several notional FKs:

| table | column | type | references (intended) | fix |
|---|---|---|---|---|
| `channel_entity` | network_arc_id | varchar | network_arc.short_code | add FK (varchar-to-varchar) |
| `channel_entity` | entity_type_id | integer | network_entity_type.id | add FK |
| `channel_entity` | schematic_type_id | integer | (unknown) | investigate, this column has no clear target table |
| `channel_entity` | hydrologic_region_id | varchar | hydrologic_region.short_code | add FK (varchar-to-varchar, and alter to integer, see §3) |
| `channel_variable` | calsim_id | varchar | (none, direct CalSim name) | leave as-is, document |
| `channel_variable` | unit_id | integer | unit.id | add FK |
| `channel_variable` | temporal_scale_id | integer | temporal_scale.id | add FK |
| `channel_variable` | variable_id | uuid | (planned `variable.id`) | defer, depends on planned `variable` table |
| `reservoir_entity` | network_node_id | varchar | network_node.short_code | add FK (varchar-to-varchar) |
| `reservoir_entity` | entity_type_id | integer | network_entity_type.id | add FK |
| `reservoir_entity` | schematic_type_id | integer | (unknown) | investigate |
| `reservoir_entity` | hydrologic_region_id | integer | hydrologic_region.id | add FK |

### 1h. Other notional references

| table | column | type | notes |
|---|---|---|---|
| `reservoir` | gnis_id | varchar | external (USGS GNIS), no FK target in this DB |
| `reservoir` | nhd_permanent_id | varchar | external (USGS NHD), no FK target in this DB |
| `reservoir` | calsim_short_code | varchar | should FK to `reservoir_entity.short_code` or vice versa |
| `developer` | aws_sso_user_id | text | external SSO, no FK target in this DB |
| `du_agriculture_entity` | source | varchar | should FK to `source.source` (text-to-text, same pattern as `slr.source`) |
| `du_refuge_entity` | source | varchar | same |
| `du_urban_entity` | source | varchar | same |
| `du_agriculture_entity` | table_id | varchar | unclear meaning, investigate |
| `mi_contractor` | source_contractor_id | integer | unclear target, investigate |
| `du_urban_delivery_arc` | delivery_arc | varchar | (PK component), candidate FK to `network_arc.short_code` |
| `mi_contractor_delivery_arc` | delivery_arc | varchar | same |
| `tier_location` | location_id | varchar | references multiple entity tables depending on `location_type`, polymorphic, can't be a single FK |
| `tier_location_result` | location_id | varchar | same |
| `sensitivity_climate` | entity_id | varchar | polymorphic across modules, can't be a single FK |
| `sensitivity_operational` | entity_id | varchar | same |

The polymorphic `location_id` and `entity_id` columns cannot be FK-enforced without splitting the columns by module, which is a larger refactor.

### Corrections (todo)

- [ ] **Add FK `created_by` / `updated_by` -> `developer.id`** on the 49 tables in § 1a (98 columns). `audit_cleanup.sql` § 9 closes this exact gap. One `ALTER TABLE ... ADD CONSTRAINT` per column, validated on existing data. Effort: M.
- [ ] **Add FK on the 7 remaining `*_version_id` columns to `version.id`** (§ 1c): `theme`, `scenario`, `hydroclimate` (also needs the numeric -> integer fix in § 3), `channel_variable`, `channel_entity`, `reservoir_entity`. Effort: S per column once the active version is settled.
- [ ] **Add FK on result-table entity references** where the target is a single table (§ 1d / 1e): `scenario_short_code` -> `scenario.short_code`, `du_id` -> the DU entity tables, `mi_contractor_code` -> `mi_contractor.short_code`, `network_arc_id` -> `network_arc` / `channel_entity`. Use `ON DELETE RESTRICT ON UPDATE CASCADE`, not CASCADE on delete. The polymorphic `location_id` / `entity_id` cannot be enforced. Effort: M.
- [ ] **Add the channel / reservoir structural FKs** in § 1g (`entity_type_id`, `network_arc_id` / `network_node_id`, `unit_id`, `temporal_scale_id`, etc.) where a clear target exists. Effort: S each.
- [ ] **Settle the `hydroclimate` FKs** (§ 1c / 1f): `created_by` / `updated_by` / `hydroclimate_version_id` via `audit_cleanup.sql` §§ 11 / 13 / 14. The open one is `source_id` -> `source.id`, deferred until the hydroclimate rework lands. Effort: S.
- [ ] **Build the `slr` linkage:** The `slr` catalog (4 rows: `none`, `slr_15`, `slr_30`, `slr_60`) is seeded but unconnected. Add a `hydroclimate_slr_link` junction, or a single `hydroclimate.slr_id` column if each hydroclimate carries exactly one SLR. Register the new table in `domain_family_map`. Effort: M.

---

## 2. Duplicate and redundant indexes

**Scope:** Indexes whose column tuple matches another index on the same table (exact duplicates), and indexes whose column tuple is a strict left-prefix of another index on the same table.

**Why it matters:** Each duplicate index occupies disk, is maintained on every INSERT / UPDATE / DELETE. A single-column index that is a left-prefix of a composite index is redundant for read queries (Postgres can use the composite as a left-prefix scan).

**Note on naming:** Some of these duplicates appear to be Migration 30 era cleanup that renamed indexes without dropping the old ones (e.g. the `idx_cws_agg_*` / `idx_cws_aggregate_*` pairs).

### 2a. Exact duplicates (40 cases)

| table | columns | indexes |
|---|---|---|
| `ag_aggregate_monthly` | (scenario_short_code, aggregate_code) | `idx_ag_agg_monthly_scenario`, `idx_ag_aggregate_monthly_combined` |
| `ag_aggregate_period_summary` | (scenario_short_code, aggregate_code) | `idx_ag_agg_period_scenario`, `uq_ag_aggregate_period_summary` (UNIQUE) |
| `ag_du_demand_monthly` | (scenario_short_code, du_id) | `idx_ag_du_delivery_scenario_du`, `idx_ag_du_demand_monthly_combined` |
| `ag_du_period_summary` | (scenario_short_code, du_id) | `idx_ag_du_period_scenario_du`, `uq_ag_du_period_summary` (UNIQUE) |
| `ag_du_shortage_monthly` | (scenario_short_code, du_id) | `idx_ag_du_shortage_monthly_combined`, `idx_ag_du_shortage_scenario_du` |
| `channel_entity` | (network_arc_id) | `channel_entity_network_arc_id_key` (UNIQUE), `idx_channel_entity_network_arc` |
| `channel_variable` | (calsim_id) | `channel_variable_calsim_id_key` (UNIQUE), `idx_channel_variable_calsim_id` |
| `compliance_station` | (station_code) | `compliance_station_station_code_key` (UNIQUE), `idx_compliance_code` (UNIQUE) |
| `cws_aggregate_entity` | (short_code) | `cws_aggregate_entity_short_code_key` (UNIQUE), `idx_cws_aggregate_entity_short_code` |
| `cws_aggregate_monthly` | (cws_aggregate_id) | `idx_cws_agg_monthly_aggregate`, `idx_cws_aggregate_monthly_entity` |
| `cws_aggregate_monthly` | (scenario_short_code, cws_aggregate_id) | `idx_cws_agg_monthly_combined`, `idx_cws_aggregate_monthly_combined` |
| `cws_aggregate_monthly` | (scenario_short_code) | `idx_cws_agg_monthly_scenario`, `idx_cws_aggregate_monthly_scenario` |
| `cws_aggregate_period_summary` | (cws_aggregate_id) | `idx_cws_agg_period_aggregate`, `idx_cws_aggregate_period_entity` |
| `cws_aggregate_period_summary` | (scenario_short_code) | `idx_cws_agg_period_scenario`, `idx_cws_aggregate_period_scenario` |
| `du_delivery_monthly` | (scenario_short_code, du_id) | `idx_du_delivery_monthly_combined`, `idx_du_delivery_scenario_du` |
| `du_period_summary` | (scenario_short_code, du_id) | `idx_du_period_scenario_du`, `uq_du_period_summary` (UNIQUE) |
| `du_shortage_monthly` | (scenario_short_code, du_id) | `idx_du_shortage_monthly_combined`, `idx_du_shortage_scenario_du` |
| `du_urban_entity` | (du_id) | `du_urban_entity_du_id_key` (UNIQUE), `idx_du_urban_entity_du_id` |
| `du_urban_group` | (short_code) | `du_urban_group_short_code_key` (UNIQUE), `idx_du_urban_group_short_code` |
| `du_urban_variable` | (du_id) | `idx_du_urban_variable_du_id`, `uq_du_urban_variable_du_id` (UNIQUE) |
| `env_flow_channel_period_summary` | (network_arc_id, scenario_short_code) | `idx_env_flow_period_summary_arc_scenario`, `uq_env_flow_period_summary` (UNIQUE) |
| `mi_contractor` | (short_code) | `idx_mi_contractor_short_code`, `mi_contractor_short_code_key` (UNIQUE) |
| `mi_contractor_delivery_arc` | (delivery_arc) | `idx_mi_contractor_delivery_arc_arc`, `uq_delivery_arc` (UNIQUE) |
| `mi_contractor_period_summary` | (scenario_short_code, mi_contractor_code) | `idx_mi_period_scenario_contractor`, `uq_mi_period_summary` (UNIQUE) |
| `mi_delivery_monthly` | (scenario_short_code, mi_contractor_code) | `idx_mi_delivery_monthly_combined`, `idx_mi_delivery_scenario_contractor` |
| `mi_shortage_monthly` | (scenario_short_code, mi_contractor_code) | `idx_mi_shortage_monthly_combined`, `idx_mi_shortage_scenario_contractor` |
| `network_entity_type` | (short_code) | `idx_network_entity_type_short_code`, `network_entity_type_short_code_key` (UNIQUE) |
| `network_type` | (short_code, network_entity_type_id) | `network_type_short_code_entity_key` (UNIQUE), `network_type_short_code_network_entity_type_id_key` (UNIQUE) |
| `refuge_du_period_summary` | (scenario_short_code, du_id) | `idx_refuge_period_summary_scenario_du`, `uq_refuge_period_summary` (UNIQUE) |
| `reservoir` | (calsim_short_code) | `idx_reservoir_calsim_code` (UNIQUE), `reservoir_calsim_short_code_key` (UNIQUE) |
| `reservoir_entity` | (short_code) | `idx_reservoir_entity_short_code`, `reservoir_entity_short_code_key` (UNIQUE) |
| `reservoir_group` | (short_code) | `idx_reservoir_group_short_code`, `reservoir_group_short_code_key` (UNIQUE) |
| `reservoir_monthly_percentile` | (scenario_short_code, reservoir_entity_id) | `idx_reservoir_percentile_combined`, `idx_reservoir_percentile_scenario_entity` |
| `reservoir_monthly_percentile` | (reservoir_entity_id) | `idx_reservoir_percentile_entity`, `idx_reservoir_percentile_reservoir` |
| `reservoir_period_summary` | (dead_pool_prob_all) | `idx_period_summary_dead_pool_prob`, `idx_period_summary_dead_prob` |
| `reservoir_spill_monthly` | (scenario_short_code, reservoir_entity_id) | `idx_reservoir_spill_scenario`, `idx_spill_monthly_combined` |
| `reservoir_storage_monthly` | (scenario_short_code, reservoir_entity_id) | `idx_reservoir_storage_scenario`, `idx_storage_monthly_combined` |
| `theme_scenario_link` | (scenario_id, theme_id) | `idx_theme_scenario_reverse`, `theme_scenario_link_pkey` (PRIMARY) |
| `tier_location_result` | (scenario_short_code, tier_short_code, location_id, tier_version_id) | `idx_tier_location_unique` (UNIQUE), `tier_location_result_scenario_short_code_tier_short_code_lo_key` (UNIQUE), `tier_location_result_unique` (UNIQUE), THREE constraints |
| `wba` | (wba_id) | `idx_wba_id` (UNIQUE), `wba_wba_id_key` (UNIQUE) |

Fix: drop one of each pair (keep the one matching the canonical naming convention `idx_<table>_<columns>` for regular indexes, `<table>_<column>_key` for unique constraints, and `<table>_pkey` for primary keys). For `tier_location_result`, drop two of the three. Effort: S each.

### 2b. Left-prefix duplicates (38 cases)

A single-column index that is the left-prefix of a composite index can be dropped. Queries that need only the leading column can use the composite. The only reason to keep both is if the composite index is much wider and the planner prefers the narrower index for selectivity reasons, in our case all the candidates are 2-3 columns wide, so the composite is fine.

| table | redundant index | covered by |
|---|---|---|
| `ag_aggregate_monthly` | `idx_ag_aggregate_monthly_scenario` | `idx_ag_aggregate_monthly_combined` |
| `ag_aggregate_period_summary` | `idx_ag_aggregate_period_summary_scenario` | `uq_ag_aggregate_period_summary` (UNIQUE) |
| `ag_du_demand_monthly` | `idx_ag_du_demand_monthly_scenario` | `idx_ag_du_demand_monthly_combined` |
| `ag_du_gw_pumping_monthly` | `idx_ag_du_gw_pumping_monthly_scenario` | `idx_ag_du_gw_pumping_monthly_combined` |
| `ag_du_period_summary` | `idx_ag_du_period_summary_scenario` | `uq_ag_du_period_summary` (UNIQUE) |
| `ag_du_shortage_monthly` | `idx_ag_du_shortage_monthly_scenario` | `idx_ag_du_shortage_scenario_du` |
| `ag_du_sw_delivery_monthly` | `idx_ag_du_sw_delivery_monthly_scenario` | `idx_ag_du_sw_delivery_monthly_combined` |
| `cws_aggregate_monthly` | `idx_cws_aggregate_monthly_scenario` | `idx_cws_aggregate_monthly_combined` |
| `cws_aggregate_period_summary` | `idx_cws_aggregate_period_scenario` | `uq_cws_aggregate_period` (UNIQUE) |
| `delta_monthly` | `idx_delta_monthly_scenario` | `uq_delta_monthly` (UNIQUE) |
| `delta_period_summary` | `idx_delta_summary_scenario` | `uq_delta_period_summary` (UNIQUE) |
| `du_delivery_monthly` | `idx_du_delivery_monthly_scenario` | `idx_du_delivery_scenario_du` |
| `du_period_summary` | `idx_du_period_summary_scenario` | `uq_du_period_summary` (UNIQUE) |
| `du_shortage_monthly` | `idx_du_shortage_monthly_scenario` | `idx_du_shortage_scenario_du` |
| `du_urban_delivery_arc` | `idx_du_urban_delivery_arc_du_id` | `uq_du_urban_delivery_arc` (UNIQUE) |
| `du_urban_group_member` | `idx_du_urban_group_member_group` | `uq_du_urban_group_member` (UNIQUE) |
| `env_flow_channel_monthly` | `idx_env_flow_monthly_arc` | `idx_env_flow_monthly_arc_scenario` |
| `env_flow_channel_period_summary` | `idx_env_flow_period_summary_arc` | `uq_env_flow_period_summary` (UNIQUE) |
| `env_flow_channel_seasonal` | `idx_env_flow_seasonal_arc` | `idx_env_flow_seasonal_arc_scenario` |
| `mi_contractor_group_member` | `idx_mi_contractor_group_member_group` | `uq_mi_contractor_group_member` (UNIQUE) |
| `mi_contractor_period_summary` | `idx_mi_period_summary_scenario` | `uq_mi_period_summary` (UNIQUE) |
| `mi_delivery_monthly` | `idx_mi_delivery_monthly_scenario` | `idx_mi_delivery_scenario_contractor` |
| `mi_shortage_monthly` | `idx_mi_shortage_monthly_scenario` | `idx_mi_shortage_scenario_contractor` |
| `network_arc` | `idx_network_arc_from_node` | `idx_network_arc_connectivity` |
| `refuge_du_delivery_monthly` | `idx_refuge_delivery_monthly_scenario` | `idx_refuge_delivery_monthly_scenario_du` |
| `refuge_du_period_summary` | `idx_refuge_period_summary_scenario` | `uq_refuge_period_summary` (UNIQUE) |
| `refuge_du_shortage_monthly` | `idx_refuge_shortage_monthly_scenario` | `idx_refuge_shortage_monthly_scenario_du` |
| `reservoir_group_member` | `idx_reservoir_group_member_group` | `uq_reservoir_group_member` (UNIQUE) |
| `reservoir_monthly_percentile` | `idx_reservoir_percentile_scenario` | `idx_reservoir_percentile_scenario_entity` |
| `reservoir_period_summary` | `idx_period_summary_scenario` | `uq_period_summary` (UNIQUE) |
| `reservoir_spill_monthly` | `idx_spill_monthly_scenario` | `idx_spill_monthly_combined` |
| `reservoir_storage_monthly` | `idx_storage_monthly_scenario` | `idx_storage_monthly_combined` |
| `scenario` | `idx_scenario_active` | `idx_scenario_active_version` |
| `sensitivity_climate` | `idx_sensitivity_climate_sibling` | `sensitivity_climate_sibling_group_module_entity_id_metric_n_key` (UNIQUE) |
| `sensitivity_operational` | `idx_sensitivity_operational_hydro` | `sensitivity_operational_hydroclimate_id_module_entity_id_me_key` (UNIQUE) |
| `theme` | `theme_short_code_key` (UNIQUE) | `idx_theme_short_code_active` |
| `tier_location_result` | `idx_tier_location_scenario` | `idx_tier_location_combined` |
| `tier_result` | `idx_tier_result_scenario` | `idx_tier_result_scenario_tier` |

The `theme` row is the only inverted case. The unique constraint (which we'd normally keep) is covered by a non-unique composite. The right move here is to drop `idx_theme_short_code_active` and keep `theme_short_code_key`, because the uniqueness guarantee on `short_code` is what enforces data integrity. The active-filter index is incidental.

Effort: S each.

### Corrections (todo)

- [ ] **Drop the duplicate indexes** enumerated above: 40 exact duplicates (§ 2a) + 38 left-prefix duplicates (§ 2b), including the three-way constraint on `tier_location_result`. `audit_cleanup.sql` §§ 7-8 drop 41 + 38 in one pass. Otherwise a single migration of `DROP INDEX` statements. Effort: M.

---

## 3. Legacy type drift

**Scope:** Columns whose type doesn't match the modern convention used elsewhere in the schema: `is_active` should be `boolean` (it was originally thought there would be more than two options, including a provisional option to match the Water Allocation Modeling Team designations), audit fields should be `integer`, FK targets should match the referenced PK's type.

**Why it matters:** Type drift can create subtle bugs at the application layer. `is_active integer` accepts any int, not just 0/1. `projection_year text` admits "2043" or more abstract projection text (used previously).

### 3a. `is_active` is integer instead of boolean

| table | type | default | fix |
|---|---|:---:|---|
| `hydroclimate` | integer | 1 | alter to boolean, migrate values |
| `scenario_author` | integer | 1 | alter to boolean, migrate values |
| `theme` | integer | 1 | alter to boolean, migrate values |

The other 89 tables in the schema use `boolean is_active NOT NULL DEFAULT true`. Bring these three onto the standard.

### 3b. `hydroclimate` legacy audit / version columns are `numeric`

| table | column | actual type | should be |
|---|---|---|---|
| `hydroclimate` | hydroclimate_version_id | numeric | integer |
| `hydroclimate` | created_by | numeric | integer (and add FK) |
| `hydroclimate` | updated_by | numeric | integer (and add FK) |

All other tables use `integer` for these columns. The numeric type permits fractional ids (e.g. `1.5`) which makes no semantic sense for FK targets. Pair this fix with §1a (FK addition) and §3c. Effort: M.

### 3c. `hydroclimate.projection_year` is `text`

Stored as text rather than integer. Cast to integer. Data clean-up will be needed for any trailing `.0` values. Effort: M.

### 3d. `channel_entity.hydrologic_region_id` is `varchar`

Other tables use `integer hydrologic_region_id` referencing `hydrologic_region.id`. `channel_entity` stores the `short_code` varchar instead. Either:

- Convert `channel_entity.hydrologic_region_id` to integer and add an FK to `hydrologic_region.id`, or
- Keep it as varchar but add a varchar-to-varchar FK to `hydrologic_region.short_code` (same flavor as `slr.source -> source.source`).

Effort: M.

### Corrections (todo)

- [ ] **`is_active integer` -> `boolean`** on `hydroclimate`, `scenario_author`, `theme` (§ 3a, `audit_cleanup.sql` § 10). Effort: S.
- [ ] **`numeric` -> `integer`** on the `hydroclimate` audit and version columns (§ 3b, `audit_cleanup.sql` § 11). Effort: S.
- [ ] **`projection_year text` -> `integer`** on `hydroclimate` (§ 3c), with cleanup of the trailing `.0` values. Effort: S.
- [ ] **`channel_entity.hydrologic_region_id varchar` -> `integer`** with FK to `hydrologic_region.id` (§ 3d). Effort: M.
- [ ] **Normalize empty-string-as-NULL on `network.name`, `network.riv_sys`, `network.strm_code`:** Some rows carry the literal single-space value `' '` instead of NULL, an artifact of a since-relaxed NOT NULL at load time. One-shot `UPDATE network SET <col> = NULL WHERE <col> = ' '` per column. Effort: S.

---

## 4. Audit trigger coverage and attribution gaps

**Scope:** Tables without the `set_audit_fields` trigger, and tables where every row's `created_by` / `updated_by` equals the system bootstrap id.

**Why it matters:** Audit attribution loses its meaning if every change is "system".

### 4a. Tables missing the `set_audit_fields` trigger

From `schema_snapshot.json["validation"]["tables_missing_audit_trigger"]`:

| table | reason |
|---|---|
| `scenario_backup` | stale snapshot, slated for removal (see §5) |
| `tier_location` | trigger was not attached at table creation. The four audit columns are populated by the load script. Likely an oversight to revisit (tracked in § 4 Corrections below) |

`sensitivity_climate` (306,272 rows) and `sensitivity_operational` (39,702 rows) also lack the `set_audit_fields` trigger and aren't flagged by the audit validation. Both were written with only an `updated_at` column, no `created_at`, no `created_by`, no `updated_by`. The tables themselves are documented in `database/schema/ERD.md` § "Sensitivity result family". Attaching the trigger (and reshaping the audit columns) is tracked in § 4 Corrections below. `spatial_ref_sys` also lacks the trigger but is PostGIS infrastructure and out of scope.

Fix: keep `scenario_backup` out of scope (drop it). For `tier_location`, decide whether the lack of audit trigger is still desired now that the tier system is in production. If not, attach `set_audit_fields`.

### 4b. Result tables attributed to system only

These 17 tables have the `set_audit_fields` trigger attached, but every row was inserted under the system operator (`created_by = updated_by = 1`). The ETL bulk-insert path is not calling `coeqwal_current_operator()` to identify the real developer before writing.

`ag_aggregate_monthly`, `ag_aggregate_period_summary`, `ag_du_demand_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_period_summary`, `ag_du_shortage_monthly`, `ag_du_sw_delivery_monthly`, `cws_aggregate_monthly`, `cws_aggregate_period_summary`, `delta_monthly`, `delta_period_summary`, `du_delivery_monthly`, `du_period_summary`, `du_shortage_monthly`, `mi_contractor_period_summary`, `mi_delivery_monthly`, `mi_shortage_monthly`.

Fix: in the ETL ingestion path (`etl/statistics/*` calculators that write to these tables), `SET LOCAL ROLE` to the operator's role before bulk insert, or explicitly set `coeqwal_current_operator()`-equivalent session state. Pair with §1a so the resulting `created_by` value is also FK-validated. Effort: M.

### Corrections (todo)

- [ ] **Attach `set_audit_fields` to `tier_location`** (§ 4a, `audit_cleanup.sql` § 4). Effort: S.
- [ ] **Attach `set_audit_fields` to `sensitivity_climate` and `sensitivity_operational`**, and reshape their minimal audit columns (just `updated_at` today; add `created_at` / `created_by` / `updated_by`). Effort: S each.
- [ ] **Backfill the 1 NULL row in `hydroclimate.created_by`** (`audit_cleanup.sql` § 13). Effort: S.

The 17 system-attributed result tables (§ 4b) are ETL/operational work, tracked on [`README.md`](README.md) § Roadmap > "ETL operator attribution".

---

## 5. Stale artifacts and seed-CSV triage

**Scope:** Stale tables, columns, and sequences to drop, plus seed CSVs that have no matching DB table and need triage (build the table, keep and document, or remove).

**Why it matters:** Stale artifacts confuse new developers. Seed data staged for tables that are planned but not yet built are easy to lose track of.

### 5a. Tables to drop

| table | reason |
|---|---|
| `scenario_backup` | stale snapshot from `51_reorder_scenario_columns_fix_s0029.sql`, no PK, no trigger, not referenced (already on roadmap) |

### 5b. Sequence name mismatch

| table | column | sequence in default | should be |
|---|---|---|---|
| `ag_du_demand_monthly` | id | `ag_du_delivery_monthly_id_seq` | `ag_du_demand_monthly_id_seq` |

Background: the table was renamed during a migration, but the sequence was not. Fix is an `ALTER SEQUENCE ag_du_delivery_monthly_id_seq RENAME TO ag_du_demand_monthly_id_seq` and a re-alter on the column default. Effort: S.

### 5c. Duplicate FK constraints

| table | column | constraints | fix |
|---|---|---|---|
| `network_entity_type` | created_by | `fk_network_entity_type_created_by` (RESTRICT/CASCADE) + `network_entity_type_created_by_fkey` (NO ACTION / NO ACTION) | drop the NO ACTION variant, keep the RESTRICT/CASCADE variant for consistency with other domain tables |
| `network_entity_type` | updated_by | same pattern | same |

Effort: S.

### 5d. Seed CSVs without a matching table

34 CSVs in `database/seed_tables/` have no matching DB table. Two buckets need a decision. The rest are accounted for.

**Staged for planned tables (build):** the seed data is prepared and waiting on the table to be created. Forward work, tracked with the Layer 04 build-out on [`README.md`](README.md) § Roadmap.

- `04_variable/delta_variable.csv` - 2 rows, waiting on the planned `delta_variable` table
- `04_variable/inflow_variable.csv` - 215 rows, waiting on the planned `inflow_variable` table
- `04_variable/reservoir_variable.csv` - 466 rows, waiting on the planned `reservoir_variable` table

**Abandoned designs (archive or delete):** never built as tables, no code references them. Candidates for `.archive/` or deletion (15 files).

- `03_outcome_framework/outcome_category.csv`
- `03_outcome_framework/outcome_measure_samples.csv`
- `03_outcome_framework/statistic_type.csv` (duplicate of `01_lookup/statistic_type.csv`)
- `02_network/network_after_nodes.csv`, `network_from_schematic.csv` (intermediate network variants)
- `04_calsim_data/diversion_arc_entity.csv` (no `diversion_arc_entity` table, no references)
- `04_calsim_data/inflow_entity.csv` (no `inflow_entity` table, not on the roadmap)
- `06_scenario/scenario_source.csv`, `scenario_source_link.csv` (the planned source-link tables were never built)
- `08_theme/theme_source.csv`, `theme_source_link.csv`, `theme_entity_type_focus.csv`, `theme_focus_entity_link.csv`, `theme_outcome_measure_focus.csv`, `theme_outcome_priority.csv` (theme framework over-design, only `theme` + `theme_scenario_link` exist)

The remaining 16 are not orphans. 12 are inputs that load into existing tables through a script or are CalSim staging data, documented in [`seed_tables/README.md`](seed_tables/README.md). The other 4 are already archived under `database/seed_tables/.archive/` (keep as-is, gitignored).

### Corrections (todo)

- [ ] **Drop `scenario_backup`** (§ 5a, `audit_cleanup.sql` § 1). Stale 75-row snapshot, no PK, no FKs, not referenced. Effort: S.
- [ ] **Rename sequence `ag_du_delivery_monthly_id_seq` -> `ag_du_demand_monthly_id_seq`** (§ 5b, `audit_cleanup.sql` § 3). Effort: S.
- [ ] **Drop the duplicate FK on `network_entity_type.created_by` / `.updated_by`** (§ 5c, `audit_cleanup.sql` § 2). Keep the RESTRICT/CASCADE variant. Effort: S.
- [ ] **Build the planned variable tables and load their staged seed CSVs** (§ 5d). `delta_variable`, `inflow_variable`, `reservoir_variable`. Forward work, tracked with the Layer 04 build-out on [`README.md`](README.md) § Roadmap. Effort: M.
- [ ] **Triage the abandoned-design seed CSVs** (§ 5d, 15 files). Archive to `.archive/` or delete, including `diversion_arc_entity.csv` and `inflow_entity.csv` (no table, no references). Effort: M (the decisions are the work).

Reminder: the drifted ID sequences (`audit_cleanup.sql` § 5) are a real `unique_violation` bug, tracked on [`README.md`](README.md) § Roadmap > "Correctness bugs (do regardless)".

---

## 6. Schema-pattern inconsistencies

**Scope:** Structural patterns that vary across tables that ought to be parallel: entity-suffix conventions, group/group_member trios, result-table key types, column-naming semantics.

**Why it matters:** Pattern inconsistencies make the schema harder to reason about for both humans and code. A consistent shape lets ETL, API, and frontend use the same join template across families.

### 6a. Entity-suffix inconsistency

7 entity tables use the `_entity` suffix. 4 don't:

| with `_entity` suffix | without |
|---|---|
| `ag_aggregate_entity` | `reservoir` |
| `channel_entity` | `compliance_station` |
| `cws_aggregate_entity` | `wba` |
| `du_agriculture_entity` | `mi_contractor` |
| `du_refuge_entity` |  |
| `du_urban_entity` |  |
| `reservoir_entity` |  |

`reservoir` is a split: `reservoir` is the geographic base table (location, geometry, surface area), and `reservoir_entity` is the operational-attributes companion (capacity, dead pool, hydrologic region, tier flags). The DU tables collapse both into a single `du_*_entity` row.

`compliance_station`, `wba`, and `mi_contractor` don't have a base / operational split. They just happen to lack the suffix.

Fix: either rename to `compliance_station_entity`, `wba_entity`, `mi_contractor_entity` for consistency, or document the convention that physical/static lookups don't carry the suffix. Effort: M (rename touches every result-table reference, or document only for S).

### 6b. Entity / group / group_member pattern coverage

3 entity families carry the full trio. 6 don't:

| entity stem | entity | group | group_member |
|---|:---:|:---:|:---:|
| reservoir | YES | YES | YES |
| du_urban | YES | YES | YES |
| mi_contractor | YES | YES | YES |
| channel | YES | NO | NO |
| ag_aggregate | YES | NO | NO |
| cws_aggregate | YES | NO | NO |
| du_agriculture | YES | NO | NO |
| du_refuge | YES | NO | NO |
| wba | YES (in L03) | NO | NO |

The 3 families with the full trio use it for variable categorization, geographic groupings, and rollup definitions. The 6 without don't currently need it. The inconsistency is only worth resolving as new groupings are needed. Effort: defer.

### 6c. Result-table key flavor inconsistency

Different result families key on different types:

| result family | entity key | type | FK enforced |
|---|---|---|:---:|
| reservoir (`reservoir_storage_monthly`, etc.) | `reservoir_entity_id` | integer | NO (notional) |
| cws_aggregate | `cws_aggregate_id` | integer | NO (notional) |
| du urban (`du_delivery_monthly`, etc.) | `du_id` | varchar | NO |
| ag_du | `du_id` | varchar | NO |
| refuge_du | `du_id` | varchar | NO |
| mi | `mi_contractor_code` | varchar | NO |
| ag_aggregate | `aggregate_code` | varchar | NO |
| env_flow | `network_arc_id` | varchar | NO |
| delta | `variable_code` | varchar | NO |
| tier | `scenario_short_code`, `tier_short_code`, `location_id` | varchar | NO |

Two flavors coexist: reservoir / cws_aggregate use integer FKs to the entity table's surrogate `id`. Everything else uses the entity's business-key `varchar`. Either is defensible, but the inconsistency means cross-family joins look different table-by-table.

Picking one and migrating the other:

- Move reservoir + cws_aggregate to use `short_code` (varchar), matches the majority and is human-readable.
- Move everyone else to integer surrogate FKs, requires changing the ETL load to look up the integer id before insert.

The first is the cheaper migration but loses the cheap join to the entity row's `id` column. The second is more orthodox SQL but costlier. Effort: L either direction.

### 6d. `env_flow_channel_monthly` column-naming semantics

This is the only result table where the same column-prefix carries different semantic meanings:

- `q0` .. `q100` and `exc_p5` .. `exc_p95` measure **percent unimpaired** (a fraction, 0-100).
- `flow_q0_cfs` .. `flow_q100_cfs` and `flow_exc_p5_cfs` .. measure absolute flow in CFS.
- `flow_q0_taf` .. `flow_q100_taf` and `flow_exc_p5_taf` .. measure absolute flow in TAF.

Every other result table reserves the `q*` / `exc_*` columns for the single primary metric (delivery / shortage / storage / spill in their respective units). Putting % unimpaired into the unsuffixed `q*` set breaks the pattern.

Fix: rename `q0` .. `exc_p95` to `pct_unimpaired_q0` .. `pct_unimpaired_exc_p95`. Effort: M (requires ETL and API update along with the column rename).

### 6e. Tier-system internal naming

The four tier tables (`tier_definition`, `tier_location`, `tier_result`, `tier_location_result`) use hand-rolled constraint naming (`tier_location_result_unique`, `idx_tier_location_unique`) rather than the `<table>_pkey` / `<table>_<column>_key` / `idx_<table>_<columns>` pattern used elsewhere. Tied to the triple-duplicate constraint issue in §2a. Effort: S to rename, folded into the index-cleanup work.

### 6f. Demand-unit geometry denormalized on entity rows

`du_urban_entity`, `du_agriculture_entity`, and `du_refuge_entity` carry `geom`, `geom_wkt`, and `srid` columns directly on the entity row. Every other spatial layer keeps geometry in a standalone table (`reservoir`, `wba`, `compliance_station`, `network_gis`), so the DU tables are the only place a footprint rides on the attribute row.

The columns were added in anticipation of serving polygons through the API for the get-started animation. That path was never needed, the animation reads polygons straight from the Mapbox vector tiles instead. Nothing in the API or frontend reads the DB geometry today.

Fix: move DU geometry into dedicated geometry tables (the standalone-table pattern the other layers use) and drop the columns from `du_*_entity`. Full context in [topic_docs/geometry.md](topic_docs/geometry.md). Effort: M.

### Corrections (todo)

- [ ] **Decide the entity-suffix convention** (§ 6a): rename `compliance_station` / `wba` / `mi_contractor` to `*_entity`, or document that physical/static lookups don't carry the suffix. Effort: M (rename) or S (document).
- [ ] **Pick one result-table entity-key flavor** (§ 6c): move `reservoir` / `cws_aggregate` to varchar `short_code`, or move everyone else to integer surrogate FKs. Effort: L either direction.
- [ ] **Rename `env_flow_channel_monthly` percent-unimpaired columns** `q0`..`exc_p95` -> `pct_unimpaired_*` (§ 6d). Effort: M (ETL + API + column rename).
- [ ] **Rename the tier-system hand-rolled constraints** to the canonical pattern (§ 6e). Folds into the index cleanup. Effort: S.
- [ ] **Drop magic-number lookup-ID and version-ID defaults:** 23 columns across 14 tables hardcode a lookup / version row's integer id as a column default. If the row is reorganized the default silently points at the wrong row. `audit_cleanup.sql` § 15 drops all 23 (require explicit values on insert). For any subset you prefer a function-call default instead, run § 15 first then add it back. Excludes `srid 4326` (stable EPSG) and `created_by` / `updated_by` defaults (cosmetic, README roadmap). Effort: S each or one-shot via § 15.
- [ ] **Consolidate the overlapping `reservoir_monthly_percentile` / `reservoir_storage_monthly` band columns:** Both per-month tables are keyed identically and carry the same seven `q*` percentile bands. Decide whether to fold `reservoir_monthly_percentile`'s two mean columns into `reservoir_storage_monthly` and retire it, or keep both and document the split. The API (`reservoir_statistics_endpoints.py`) and reservoir ETL both read `reservoir_monthly_percentile`, so update them in lockstep. Effort: M.
- [ ] **Build a trigger to maintain `network.has_gis`:** The boolean mirrors whether a `network_gis` row exists but nothing keeps it in sync. Today's 100% alignment is a bootstrap snapshot artifact. Add `AFTER INSERT/UPDATE/DELETE ON network_gis` triggers, or document the column as snapshot-only. Effort: S.
- [ ] **Reconsider `ON DELETE CASCADE` on 9 entity-aspect FKs** where a single parent-entity DELETE silently destroys child-attribute data: `network.id` -> `network_arc` / `network_node` / `network_gis`, `du_urban_entity.du_id` -> `du_urban_delivery_arc` / `du_urban_group_member` / `du_urban_variable`, `mi_contractor.id` -> `mi_contractor_delivery_arc` / `mi_contractor_group_member`, `reservoir_entity.id` -> `reservoir_group_member`. Decide per parent whether RESTRICT / NO ACTION is safer. The 11 CASCADE FKs on junction / link / group-member tables are conventional and out of scope. Effort: S per parent.
- [ ] **Align `gw` / `sw` column types across DU entity tables:** `du_agriculture_entity` / `du_refuge_entity` use `boolean NOT NULL`. `du_urban_entity` uses `varchar NULL`. Pick boolean and migrate. The cast is value-preserving (`'0'` to `false`, `'1'` to `true`, `''` to `NULL`), so it is independent of the urban gw/sw value reconciliation tracked in [`topic_docs/cws/gw_sw_reconciliation.md`](topic_docs/cws/gw_sw_reconciliation.md). The real prerequisite is the reader audit (ETL, API, frontend). Step-by-step procedure and reader-audit checklist live in [`topic_docs/cws/gw_sw_reconciliation.md` § Step 7](topic_docs/cws/gw_sw_reconciliation.md#step-7-boolean-type-migration-independent-of-value-reconciliation). Effort: S.
- [ ] **Move demand-unit geometry off the entity rows** (§ 6f): migrate `geom` / `geom_wkt` / `srid` from `du_urban_entity` / `du_agriculture_entity` / `du_refuge_entity` into dedicated geometry tables, then drop the columns. No API or frontend consumer reads it. See [topic_docs/geometry.md](topic_docs/geometry.md). Effort: M.
- [ ] **Untangle `du_urban_variable.variable_type` / `variable_type_id`:** two classification axes with confusingly similar names. Decide whether to enforce `variable_type` as an FK on `variable_type.id` (values already match), and whether rows should carry a `calsim_model_variable_type` classification (populate + rename to `calsim_model_variable_type_id`, or drop the column and its FK). Effort: S each.

---

## 7. Layer 03 entity-table cleanup

Layer 03 was built first, while the schema conventions were still being settled. Later layers (network in Layer 02, scenario in Layer 06, theme in Layer 08, tier in Layer 09) inherited the cleaned-up conventions. Layer 03 did not get retrofitted. This section consolidates the per-table Layer 03 work into one place so the handoff team can plan a single migration pass.

Strategy framing and per-table inventory live in [`schema/ERD.md`](schema/ERD.md) § "Layer 03 - ENTITY" intro.

What is and is not in scope:

- **In scope:** Layer 03-specific issues: per-table modernization (`reservoir_entity` and the four other manual-id tables), the WBA redesign, the Layer 03 subset of the developer-FK gap.
- **Not duplicated here, but applies to Layer 03 along with the rest of the schema:** § 1 broader FK additions, § 3 type drift, § 6 ON DELETE CASCADE reconsideration, § 6 naming consistency, § 8 lookup enforcement. The Layer 03 subset of magic-number defaults (`entity_type_id`, `entity_version_id` on `channel_entity` and `reservoir_entity`) is covered by `audit_cleanup.sql` Section 15. The follow-up function-call-default decision lives in § 6.

### `reservoir_entity` modernization

- [ ] **`reservoir_entity` modernization:** One of the earliest Layer 03 tables, several issues consolidated here. Most also appear under broader items elsewhere. Cross-references are noted on each sub-item.
  - **Promote `reservoir_entity.id` to a sequence:** Today `id integer NOT NULL` has no default. The loader inserts explicit ids 1-92 from the seed CSV. Create `reservoir_entity_id_seq`, set the column default to `nextval(...)`, and `setval` to current `MAX(id)`. Existing ids preserved. Brings it in line with every other Layer 03 catalog. Effort: S.
  - **Add the missing FK constraints:** `reservoir_entity` carries zero FKs today: `created_by` / `updated_by` -> `developer.id` (§ 1), `entity_type_id` -> `network_entity_type.id` (§ 8), `hydrologic_region_id` -> `hydrologic_region.id` (§ 8), `entity_version_id` -> `version.id` (§ 1).
  - **Decide on `is_main` and `has_tiers`:** Both 100% `FALSE` across all 92 rows. Populate, document as aspirational, or drop. Effort: S (decision).
  - **Fix `has_gis_data integer` (0/1) -> `boolean`:** Same pattern as the `is_active` fix in § 3. Effort: S.
  - **Fix `source_ids text` -> `integer[]`:** Today the literal string `"1,4"`. Aligns with `network.source_list integer[]`. Effort: S.
  - **Decide on `network_node_id` vs `short_code` redundancy:** Both hold the same value for all 92 rows. Drop `network_node_id`, or formalize it as a real FK and stop carrying both. Effort: S.
  - **Companion `reservoir` table cleanup:** Well-formed but carries a duplicate unique definition on `calsim_short_code` (`idx_reservoir_calsim_code` + `reservoir_calsim_short_code_key`). Covered by the "Drop duplicate indexes" item in § 2. Listed here for completeness.

### Other Layer 03 tables with manual ids

- [ ] **Promote `id` to a sequence on the four remaining Layer 03 manual-id tables:** Same fix as `reservoir_entity`. Tables: `du_urban_group` (11 rows), `mi_contractor_group` (6), `reservoir_group` (4), `reservoir_group_member` (24). `du_urban_group_id_seq` already exists (it is one of the drifted sequences on the README correctness-bugs list) but the column default doesn't reference it. Check and reuse before creating a new one. Per-table SQL pattern:
  ```sql
  CREATE SEQUENCE IF NOT EXISTS <table>_id_seq;
  ALTER TABLE <table> ALTER COLUMN id SET DEFAULT nextval('<table>_id_seq');
  ALTER SEQUENCE <table>_id_seq OWNED BY <table>.id;
  SELECT setval('<table>_id_seq', (SELECT COALESCE(MAX(id), 0) FROM <table>));
  ```
  Effort: S per table, a single migration for the lot.

### Developer-FK subset for Layer 03

- [ ] **Add `created_by` / `updated_by` -> `developer.id` FKs on the 14 Layer 03 tables that lack them:** Subset of the broader § 1 item, listed here so the Layer 03 work can land independently. The 14: `ag_aggregate_entity`, `cws_aggregate_entity`, `du_agriculture_entity`, `du_urban_entity`, `du_urban_delivery_arc`, `du_urban_group`, `du_urban_group_member`, `mi_contractor`, `mi_contractor_delivery_arc`, `mi_contractor_group`, `mi_contractor_group_member`, `reservoir_entity`, `reservoir_group`, `reservoir_group_member`. Effort: S per column, one migration for the lot.

### WBA (Water Budget Areas) gaps

Bring WBA onto the standard entity pattern and model the DU-WBA hierarchy.

- [ ] **Add a `du_wba_link` (or equivalent) crosswalk table** with FKs to the DU entity tables and to `wba`. Today the relationship is a freeform `wba_id varchar` on each DU entity table with no FK. Effort: M.
- [ ] **Decide whether `wba` should follow the `<entity>` / `<entity>_entity` pattern** or remain a typed lookup. Document the choice. Effort: S (decision), M (if implementing).
- [ ] **Decide where groundwater results live:** WBA is the natural carrier for groundwater budgets, but no `wba_*` result family exists in Layer 11. Groundwater data today (`ag_du_gw_pumping_monthly`) is keyed by DU. Effort: M.
- [ ] **Add `wba_variable` to Layer 04** if a groundwater variable list is needed. Effort: M.

---

## 8. Layer 01 lookup enforcement

The Layer 01 lookup tables were built early as a controlled vocabulary for the rest of the schema (units, regions, source attribution, statistic types, scales, network classification, etc.) but the FK plumbing that would make them load-bearing was never finished. Had the bandwidth to design and seed the tables, not to retrofit every consumer to reference them.

**Lookup tables are useful catalogs in their own right.** Some are also useful as keys, but not every lookup is intended to be one. Don't try to enforce every lookup just because it exists.

Lookup linkage is uneven by layer. Layer 02 (network) has high coverage. Layer 03 (entity) is mixed: some columns FK to lookups, others hold the same value as a freeform varchar. Layers 10-12 (results) mostly carry lookup values as denormalized varchar text per row, with nothing enforced. A nice intermediate task is to take one layer at a time and bring its lookup linkage up to a uniform standard, deciding per-lookup whether it should be enforced as a key or stay an informational catalog.

The result today is a three-way split:

- **Well-used:** `source` (12 inbound FKs), `model_source` (7), `hydrologic_region` (5).
- **Orphans:** Six of fourteen have zero inbound FKs despite canonical seeded values: `geometry_type`, `spatial_scale`, `temporal_scale`, `statistic_type`, `network_subtype`, plus `statistic_category` (only referenced from its own child `statistic_type`). `unit` is also FK-orphan but has 4 columns elsewhere that would obviously target it.
- **Partially used:** The same value is a FK in some tables and a freeform `varchar` in others. `hydrologic_region` appears as text in 8 places and as integer-without-FK in 1 (`reservoir_entity.hydrologic_region_id`). `model_source` exists as a `varchar` next to the proper `_id` FK in two `du_*_entity` tables.

This is a per-lookup decision: enforce, retire, or accept-as-catalog. Numbers come from `audits/monthly_20260524_143951/schema_snapshot.json` cross-referenced against the live FK list. Detail and source-of-truth: [`schema/ERD.md`](schema/ERD.md) § Layer 01.

- [ ] **Write a lookup-enforcement audit step:** A Python script that reads `schema_snapshot.json` and reports, per Layer 01 table, (a) inbound FK count and source columns, (b) name-pattern should-be-FK-but-arent matches, (c) value-overlap check against the seeded `short_code` set. Fold into `run_monthly_audit.py`. Effort: S.
- [ ] **`unit`:** Enforce or retire. `channel_variable.unit_id integer` exists (no FK). Three result tables hold `unit` as `varchar`. Effort: M.
- [ ] **`hydrologic_region`:** Standardize to `hydrologic_region_id integer FK` everywhere and migrate the 8 varchar / mistyped-integer columns. Largest single cleanup here. Effort: M.
- [ ] **`model_source`:** Drop the redundant `model_source varchar` on `du_agriculture_entity`, `du_refuge_entity`, `du_urban_entity` (they already carry `model_source_id` FK). Effort: S.
- [ ] **`network_entity_type`:** Add FKs on `channel_entity.entity_type_id` and `reservoir_entity.entity_type_id`. Effort: S each.
- [ ] **`temporal_scale`:** Add FK on `channel_variable.temporal_scale_id`. Effort: S.
- [ ] **`statistic_type` and `statistic_category`:** Decide whether result-table column naming maps to `statistic_type` rows by FK or stays convention-only. Effort: S (decision), M (implementation).
- [ ] **`geometry_type`, `spatial_scale`:** Drop, or build the consumers that should reference them. Effort: S (decision).
- [ ] **`network_subtype`:** Decide the `network` / `network_subtype` relationship (the lookup exists, `network` has no `subtype_id`). Effort: S.
- [ ] **`env_flow_season`:** Already FK'd from `env_flow_channel_seasonal.season_id`. No change unless other seasonal aggregations grow.
- [ ] **`watershed`:** Already a text FK (`channel_entity.watershed_short_code`). Decide whether to migrate to integer FK. Effort: S.
- [ ] **Normalize PWSID / drinking-water utility out of `du_urban_entity.community_agency`:** Utility names and the trailing 7-digit PWSID live as free text in `community_agency` (e.g. `Centerville and Redding Centerville CSD 4510011`). Decide whether to normalize the utility / PWSID into its own lookup. This overlaps the CWS schema build, since the draft `cws_entity` is one row per PWSID, so settle the PWSID representation during that design rather than in isolation (see [`TEAM_RUNBOOK.md` A1](../docs/TEAM_RUNBOOK.md) and [`water_user_categories.md`](topic_docs/cws/water_user_categories.md#roadmap)). Effort: S (decision), M (implementation).

Effort total: roughly two M-sized migrations (`unit`, `hydrologic_region`) plus a string of S-sized decisions and one-shot FK additions. None block current operation. The value is that the next person editing data cannot quietly drift from the controlled vocabulary.

---

## 9. Versioning subsystem

The Layer 00 versioning catalog is **in-progress**: developer attribution works and the catalog is seeded, but the improvement workflow has never been exercised and the audit log is dormant. Full discussion in [`VERSIONING.md`](VERSIONING.md) and [`schema/ERD.md`](schema/ERD.md) § "Layer 00 - VERSIONING".

Versioning has two levels: a family registry (`domain_family_map`, mapping 93 tables across 13 of 14 families) and row-level version tracking (`<family>_version_id` columns that FK to `version.id`). Only seven families have any version-tracked table today (`theme`, `scenario`, `hydroclimate`, `variable`, `tier`, `network`, `entity`), and even within those not every mapped table carries the column. An improvement would insert a new `version` row for the family, flip `is_active` on the prior one, load domain rows stamped with the new `version.id`, and have consumers call `get_active_version('<family>')`. This has never been exercised end-to-end.

- [ ] **Decide: keep the system as designed or simplify:** Keeping = finish the design. Simplifying = deprecate `version` / `VersioningManager` and keep only `developer`, `domain_family_map`, `set_audit_fields`. Effort: S (decision).
- [ ] **Make progress on the implementation:** Open questions: which tables benefit from a `*_version_id` stamp (trunk catalogs yes, result tables per-family, lookups / links usually no), whether to enforce one-active-per-family via a partial unique index, whether to tighten `version.version_number` / `version.changelog` to `NOT NULL`, validate by exercising one real improvement end-to-end. Detail: `VERSIONING.md`. Effort: M-L.
- [ ] **Decide `VersioningManager`: wire into the API, or remove the file:** `database/utils/versioning_utils.py` is not imported by any consumer. Effort: S (decision), M (either path).
- [ ] **Reconcile `domain_family_map` seed CSV with the live table, or archive the seed:** `database/seed_tables/00_versioning/domain_family_map.csv` (39 rows) does not match the live table (93 rows) and is not loaded by any script. Either make the CSV the canonical bootstrap, or archive it and document the live DB + inline SQL migrations as source of truth. Effort: S (decision), M (implementation).

`version_family` polish (none blocks current use):

- [ ] **Tighten `label` and `description` to `NOT NULL`** (all rows populated). Effort: S.
- [ ] **Align `short_code` type** from `text` to `varchar`. Effort: S.
- [ ] **Add `idx_version_family_active` on `is_active`** for consistency. Effort: S.
- [ ] **Add a `CHECK` on `short_code` format** (lowercase, alphanumeric + underscore). Effort: S.
- [ ] **Add `tier_location` to `domain_family_map`** (family 8 `tier`, the other three tier tables are mapped, it is missing). Effort: S.
- [ ] **Resolve the orphan `interpretive` family (id 10):** seeded but no domain table references it. Drop it, or map the sensitivity tables if developed under it. Effort: S (decision).

---

## 10. Docs and tooling

- [ ] **Fix or retire the ERD-vs-DB verifier (`database/audit/verify_erd_against_audit.py`):** Has drifted from the markdown-table ERD and needs the parser rewritten. Two drift points: (1) `run_monthly_audit.py` hardcodes the old filename `COEQWAL_SCENARIOS_DB_ERD.md`, so the comparison silently skips. (2) `parse_erd_tables()` scans for tree lines but `ERD.md` uses Markdown tables, so it parses zero tables and would flag every DB table as missing. Effort: M. The full audit roadmap (this plus the coverage self-check, softening hardcoded counts, and the Layer 09 export gap) lives in [`audit/README.md`](audit/README.md#improving-the-audit).
- [ ] **Decide on the "Referenced by" pattern for inbound references in the ERD:** Unevenly applied. Either document inbound FKs for every table (the `foreign_keys` snapshot block is the source) or drop the pattern entirely. Effort: M.

---

## Design background, integer IDs vs short codes

Context for the new owner, not a correction. The schema is mid-migration from one identity model to another.

**Phase 1 (early layers):** Reference tables (Layer 00 versioning, Layer 01 lookups, parts of Layer 03 entity catalogs) were loaded with explicit `id` values from seed CSVs so the same row carries the same integer on every database. Other tables FK'd to those integers and sometimes embedded them as hardcoded column defaults (`source_id = 4`, `network_version_id = 12`, `created_by = 2`). This was a conscious choice for cross-environment FK stability.

**Phase 2 (later layers):** Starting around the scenario layer and consolidating in Layer 09 tier, new tables use short codes as the actual join key (`scenario.short_code`, `tier_definition.short_code`, `du_id`), both as FK targets and as varchar columns on result tables. Short codes survive sequence resequencing without re-mapping.

Where each shows up (snapshot `audits/monthly_20260524_143951`): 42 of 76 tables have a `short_code`. Of 168 enforced FKs, 162 reference `.id` and 6 reference `.short_code` (clustered in Layer 09 tier plus `channel_entity` and the sibling-group machinery). Every Layer 11 result table keys on `scenario_short_code` + `du_id` varchars. The 23 magic-number defaults and the 5 drifted sequences all cluster in the Phase-1 layers.

**Scope: what to do, defer, leave alone:** The migration is not on the critical path. Targeted cleanup is enough.

- **Apply now** (the script): § 0 / `audit_cleanup.sql` § 5 stops the sequence-drift `unique_violation`, § 15 removes the hardcoded id defaults.
- **Do next:** the sequence-drift check and the result-table short-code FK enforcement (§ 1), and adopt the natural-key convention below for new tables. The `created_by` / `updated_by` default cluster is low priority (cosmetic provenance, tracked on `README.md` § Roadmap > "ETL operator attribution").
- **Probably don't worry about**: mass-converting the 162 integer-id FKs to short-code FKs, migrating the entity-catalog id inconsistency, removing integer-id columns. Revisit if the team grows and there's that time available.

**Convention for new tables:** sequence-default integer `id` (no hardcoded id defaults). A `short_code` natural key on any table with public-facing identity. FK by short code when cross-environment portability matters, integer-id FK only for internal joins. `created_by` / `updated_by` filled by `coeqwal_current_operator()`, not hardcoded defaults.

### Nonsensical patterns and the thinking behind them

A cluster of patterns look arbitrary on first read. Most are the residue of building tables at different times under shifting conventions.

- **Inconsistent `developer.id` enforcement on `created_by` / `updated_by`:** Every audited table has the four audit columns, but only junction / link tables and a few lookups enforce the FK. No semantic reason, an artifact of creation order. Fix: `audit_cleanup.sql` § 9, tracked in § 1.
- **`scenario` references its hydroclimate two ways:** `hydroclimate_sibling` (varchar) enforced FK vs `hydroclimate_id` (integer) unenforced. Phase-1 / Phase-2 overlap. Fix: settle on one, § 1.
- **The sibling group's PK reuses a scenario's `short_code`** (the founding run's code). Cognitive ambiguity only, no DB collision. Low severity, no migration unless other sibling work is done.
- **Magic-number integer-id defaults:** Fix: `audit_cleanup.sql` § 15, follow-up in § 6.
- **Manual explicit `id` on five Layer 03 catalogs:** Fix: § 7 and `audit_cleanup.sql` § 6.
- **Duplicate FK / index on `network_entity_type`:** Fix: § 5 / § 2 and `audit_cleanup.sql` §§ 2 / 7 / 8.
- **Degenerate `variable_type` columns** (single-valued in seed). An aspirational classification axis that was never populated.
- **`hydroclimate` audit / version columns typed `numeric`** while every other table uses integer. Fix: § 3 and `audit_cleanup.sql` §§ 11 / 13 / 14.
