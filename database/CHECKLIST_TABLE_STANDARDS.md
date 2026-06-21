# Table standards checklist

Use this checklist when creating new tables, populating them, or auditing existing tables.

## Table of contents

1. [Table creation checklist](#table-creation-checklist)
2. [Data population checklist](#data-population-checklist)
3. [Quick audit query](#quick-audit-query)

---

## Table creation checklist

### 1. Common columns

Conventions to consider. How many apply depends on the table's role.

**Audit columns:** Carried by essentially every domain table, and the prerequisite for the audit trigger (§ 2). The trigger populates the `_by` columns from the connecting developer, so do not set them by hand.

- [ ] `created_at`: TIMESTAMPTZ DEFAULT NOW()
- [ ] `created_by`: INTEGER REFERENCES developer(id)
- [ ] `updated_at`: TIMESTAMPTZ DEFAULT NOW()
- [ ] `updated_by`: INTEGER REFERENCES developer(id)

**Primary key:**

- [ ] `id`: SERIAL / sequence-backed integer PRIMARY KEY on most tables. A reference table may instead use its `short_code` as the natural key.

**Often used, decide per table:**

- [ ] `is_active`: BOOLEAN DEFAULT TRUE for soft deletes. Common on hand-curated reference and catalog tables. Omit on high-volume result tables (Layers 10-12) where it is not used.
- [ ] `short_code`: the stable, human-readable natural key. See below.

#### `short_code`

A `short_code` is COEQWAL's stable, human-readable identity for a row, a compact string that stays constant across database rebuilds, unlike an integer `id`, which can resequence on a reload. Where a table's rows have a public-facing or cross-environment meaning, give them a `short_code` (`TEXT` / `varchar`, usually `UNIQUE NOT NULL`).

How it is used in this database:

- **It is the cross-environment join key:** Examples: `scenario.short_code` (`s0040`), `tier_definition.short_code` (`AG_REV`), `hydrologic_region.short_code` (`SAC`), DU ids (`02_NA`). Roughly 42 of 76 tables carry one.
- **Newer tables FK on `short_code`, not on the integer `id`:** For example `tier_location_result` / `tier_result` reference `tier_definition.short_code`, and `scenario.hydroclimate_sibling` references `scenario_hydroclimate_sibling.short_code`. The Layer 11 result tables key on `scenario_short_code` (varchar) rather than `scenario.id`. This is the deliberate direction (see `SCHEMA_BACKLOG.md` § "Design background, integer IDs vs short codes").
- **Reference and catalog tables should almost always have one:** Pure result rows and internal-only tables may not need one.
- **One thing to watch:** `scenario_hydroclimate_sibling.short_code` reuses the founding scenario's code (e.g. `s0020`), so the same string can name both a scenario and its sibling group. They live in different tables, so there is no collision, but the value alone is ambiguous.

### 2. Audit trigger

- [ ] Apply the `set_audit_fields` trigger after creating the table:
  ```sql
  SELECT apply_audit_trigger_to_table('your_table_name');
  ```
  Requires the four audit columns from § 1. If any are missing the function returns `SKIPPED: <table> (missing audit columns)` and attaches nothing.

### 3. Domain family mapping

- [ ] Add entry to `domain_family_map`:
  ```sql
  INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note)
  VALUES ('public', 'your_table_name', {version_family_id}, 'Description');
  ```
  The live table also carries `is_active` (defaults to true) and `database_level` (the two-digit layer code, e.g. `'03'`, populated on every existing row, so set it to your table's layer). Then add the table name to the matching layer bucket in the `LAYERS` dict in `database/audit/run_monthly_audit.py`. The audit still finds any new public table in the schema snapshot, but a table missing from `LAYERS` is labelled layer `other` in the report inventory and is skipped from the per-layer CSV exports.

### 4. Foreign key relationships

Once a schema is established and all the lookups understood, please reference the lookup by its key intead of text. The key is often the integer `id`, but newer tables FK directly to a `short_code` where that is the lookup's join target (e.g. `tier_definition.short_code`, `scenario.short_code`).

> Most of these lookup FKs are not enforced in the live database yet, and should be.The lookups were seeded as a controlled vocabulary, but the FK plumbing was never finished. A new table should add every applicable FK below from the start. Bringing the existing tables up to the same standard is a completeness pass tracked in [`SCHEMA_BACKLOG.md`](SCHEMA_BACKLOG.md) § 8. Note that the `database/scripts/sql/audit_cleanup.sql` correction script enforces the `created_by` / `updated_by` -> `developer.id` FKs but not these lookup FKs, so this work is still open for when someone has time to make the database complete. The "Enforced today?" column reflects the latest monthly audit snapshot (`audits/monthly_20260524_143951/`).

| If you need... | Reference this lookup | FK column (recommended) | Enforced today? |
|----------------|----------------------|-------------------------|-----------------|
| Hydrologic region | `hydrologic_region` | `hydrologic_region_id` (integer) | Partial: 5 tables FK'd. `reservoir_entity` (integer) and `channel_entity` (varchar) are unconstrained, plus free-text `region` / `hydrologic_region` copies on `ag_aggregate_entity`, `cws_aggregate_entity`, `mi_contractor`, and the `du_*_entity` tables |
| Data source | `source` | `source_id` (integer) | Yes: 12 FKs |
| Model source | `model_source` | `model_source_id` (integer) | Yes: 7 FKs. Drop the redundant `model_source` varchar still carried on the three `du_*_entity` tables |
| Measurement unit | `unit` | `unit_id` (integer) | No: `channel_variable.unit_id` is unconstrained. `delta_monthly`, `sensitivity_climate`, `sensitivity_operational` hold `unit` as varchar |
| Spatial scale | `spatial_scale` | `spatial_scale_id` (integer) | No, no consumer column exists yet (orphan lookup) |
| Temporal scale | `temporal_scale` | `temporal_scale_id` (integer) | No: `channel_variable.temporal_scale_id` is unconstrained |
| Statistic category | `statistic_category` | `statistic_category_id` (integer) | Yes: 1 FK (from `statistic_type`) |
| Statistic type | `statistic_type` | `statistic_type_id` (integer) | No: result tables name statistics by column convention, with no FK |
| Geometry type | `geometry_type` | `geometry_type_id` (integer) | No, no consumer column exists yet (orphan lookup) |
| Network type | `network_type` | `type_id` (integer) | Yes: 2 FKs |
| Network subtype | `network_subtype` | `subtype_id` (integer) | No: `network.subtype_ids` is an unconstrained array and `channel_entity.subtype` is varchar, so no scalar FK column exists |
| Network entity type | `network_entity_type` | `entity_type_id` (integer) | Partial: `network` and `network_type` FK'd. `channel_entity` and `reservoir_entity` are unconstrained |
| Watershed | `watershed` | `watershed_short_code` (varchar) | Yes: 1 FK (`channel_entity`), short-code key |
| Env flow season | `env_flow_season` | `season_id` (integer) | Yes: 1 FK |
| CalSim technical type | `calsim_model_variable_type` | `calsim_model_variable_type_id` (integer) | Partial: only `du_urban_variable` FK'd, via the legacy column name `variable_type_id` |
| Colloquial variable type | `variable_type` | `variable_type_id` (integer) | No: `channel_variable.variable_type` and `du_urban_variable.variable_type` are varchar copies whose values match the catalog but carry no FK |
| Derived variable type | `derived_variable_type` | `derived_variable_type_id` (integer) | No, no consumer column exists yet (orphan lookup) |

---

## Data population checklist

This covers loading rows into a table that already exists.

### Before populating

- [ ] **Audit fields:** Does the target table have the audit trigger? (If not, `created_*` / `updated_*` won't be tracked. See [Audit trigger](#2-audit-trigger).)
- [ ] **Lookup values:** Do all referenced lookup values exist? (See the Layer 01 lookups in [`schema/ERD.md`](schema/ERD.md).)
- [ ] **FK values:** Are you supplying valid lookup IDs (resolve them via subquery)? Defining the FK *constraint* is a table-creation step (see [Foreign key relationships](#4-foreign-key-relationships)). If the constraint is missing and you have time, add it then.
- [ ] **Attribution:** The audit trigger fills `created_by` / `updated_by` from the connecting developer, so you do not set them by hand. Confirm your database user is registered first (run `SELECT coeqwal_current_operator();`), otherwise writes attribute to System (id 1).

Verify lookup values exist before inserting:
```sql
SELECT id, short_code FROM hydrologic_region WHERE short_code = 'SAC';
SELECT id, source FROM source WHERE source = 'calsim_report';
```

If a needed lookup value doesn't exist, add it first:
```sql
INSERT INTO hydrologic_region (short_code, label, is_active)
VALUES ('NEW_REGION', 'New Region Name', true);
```

### During populating

Use subqueries to resolve lookups to their FK IDs:
```sql

INSERT INTO my_table (name, hydrologic_region_id, source_id)
VALUES ('Example',
        (SELECT id FROM hydrologic_region WHERE short_code = 'SAC'),
        (SELECT id FROM source WHERE source = 'calsim_report'));
```

### After populating

- [ ] **Verify counts:** `SELECT COUNT(*) FROM your_table;`
- [ ] **Check nulls:** `SELECT * FROM your_table WHERE some_fk_id IS NULL;`
- [ ] **Audit trail:** `SELECT created_at, created_by FROM your_table ORDER BY created_at DESC LIMIT 5;`

---

## Quick audit query

Run this to check a table's compliance:

```sql
-- Check table has audit columns and trigger
SELECT 
    t.table_name,
    MAX(CASE WHEN c.column_name = 'created_at' THEN 'Y' ELSE 'N' END) as created_at,
    MAX(CASE WHEN c.column_name = 'created_by' THEN 'Y' ELSE 'N' END) as created_by,
    MAX(CASE WHEN c.column_name = 'updated_at' THEN 'Y' ELSE 'N' END) as updated_at,
    MAX(CASE WHEN c.column_name = 'updated_by' THEN 'Y' ELSE 'N' END) as updated_by,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.triggers tr 
        WHERE tr.event_object_schema = 'public'
        AND tr.event_object_table = t.table_name 
        AND tr.action_statement LIKE '%set_audit_fields%'
    ) THEN 'Y' ELSE 'N' END as has_trigger,
    CASE WHEN EXISTS (
        SELECT 1 FROM domain_family_map dfm 
        WHERE dfm.schema_name = 'public'
        AND dfm.table_name = t.table_name
    ) THEN 'Y' ELSE 'N' END as in_domain_map
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
    ON t.table_name = c.table_name 
    AND c.table_schema = 'public'
    AND c.column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by')
WHERE t.table_schema = 'public' 
AND t.table_name = 'YOUR_TABLE_NAME'
GROUP BY t.table_name;
```
