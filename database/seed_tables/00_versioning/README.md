# 00_versioning seed tables

Bootstrap data for the versioning system. These CSVs are the source of truth for
the small, stable reference tables that define version domains and bootstrap users.

## Files

| File | DB table | CSV rows | DB rows | Status |
|------|----------|----------|---------|--------|
| `version_family.csv` | `version_family` | 14 | 14 | ✅ current |
| `version.csv` | `version` | 14 | 14 | ✅ current |
| `developer.csv` | `developer` | 2 | 2 | ⚠️ see note below |
| `domain_family_map.csv` | `domain_family_map` | 34 | 70 | ❌ out of date — do not use for loading |

## Loading

Seed data is loaded by `database/scripts/sql/.archive/00_versioning/06_load_seed_data.sql`.
Run it after `00_create_versioning_tables.sql`.

`domain_family_map` is **not** loaded from the CSV here — it is populated by
`05_populate_domain_family_map.sql`, which contains the full current 70-row set.

To regenerate `domain_family_map.csv` from the live database:

```bash
psql $DATABASE_URL -c "\copy (
  SELECT
    dfm.schema_name,
    dfm.table_name,
    vf.short_code AS version_family_short_code,
    dfm.note,
    dfm.is_active
  FROM domain_family_map dfm
  JOIN version_family vf ON vf.id = dfm.version_family_id
  ORDER BY dfm.table_name
) TO 'database/seed_tables/00_versioning/domain_family_map.csv' CSV HEADER"
```

## Notes on developer.csv

The CSV is missing the `name` and `aws_sso_username` columns that exist in the
live table. The load script uses inline INSERT values instead of `\copy` to handle
this. If you add a new bootstrap developer, add them to both the CSV and the INSERT
in `06_load_seed_data.sql`.

## Version families (14 total)

| short_code | label | Description |
|------------|-------|-------------|
| `theme` | Theme | Research themes and storylines |
| `scenario` | Scenario | Water management scenarios |
| `assumption` | Assumption | Scenario assumptions and parameters |
| `operation` | Operation | Operational policies and rules |
| `hydroclimate` | Hydroclimate | Hydroclimate conditions and projections |
| `variable` | Variable | CalSim model variables and definitions |
| `statistics` | Statistics | Statistics categories and measurement systems |
| `tier` | Tier | Tier definitions and classification systems |
| `geospatial` | Geospatial | Geographic and spatial data definitions |
| `interpretive` | Interpretive | Analysis and interpretive frameworks |
| `metadata` | Metadata | Data metadata and documentation |
| `network` | Network | CalSim network topology and connectivity |
| `entity` | Entity | Entity data versions |
| `audit` | Audit | Layer 00 system tables: versioning, developer registry, domain mapping, audit log |

## Key versioning functions

### `get_active_version(family_short_code TEXT) → INTEGER`

Returns the active `version.id` for a given family.

```sql
SELECT get_active_version('network');   -- use in table defaults
SELECT get_active_version('scenario');
```

### `coeqwal_current_operator() → INTEGER`

Returns `developer.id` for the session user. Used in audit fields.

```sql
-- Detection order (uses session_user, not current_user):
-- 1. Exact match on aws_sso_username
-- 2. Substring match on email
-- 3. Substring match on name
-- 4. Substring match on display_name
-- Special case: session_user = 'postgres' → returns 1 (system account)
-- Strict: RAISES EXCEPTION if no match found
```

See `database/scripts/sql/.archive/00_create_helper_functions.sql` for the full definition.

## Versioning workflow

1. **New version** — add a row to `version` with `is_active = true`, set previous
   active version to `is_active = false`.
2. **New table** — add a row to `domain_family_map` linking the table to its family,
   and update `05_populate_domain_family_map.sql`.
3. **New developer** — run `register_developer()` (see `04_create_developer_users.sql`)
   and update `developer.csv` and `06_load_seed_data.sql`.
