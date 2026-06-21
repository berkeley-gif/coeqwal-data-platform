# 00_versioning seed tables

Bootstrap data for the versioning system. These CSVs seed the small, stable reference tables that define version domains and bootstrap users.

For how the versioning subsystem works (families, `*_version_id` coverage, the authoring audit, the intended bump workflow), see [`../../VERSIONING.md`](../../VERSIONING.md). This README covers only how the seed files in this folder load.

## Files

| File | DB table | Load source |
|------|----------|-------------|
| `version_family.csv` | `version_family` | Yes, via `06_load_seed_data.sql` |
| `version.csv` | `version` | Yes, via `06_load_seed_data.sql` |
| `developer.csv` | `developer` | Partial, inline `INSERT` (see note below) |
| `domain_family_map.csv` | `domain_family_map` | No. Populated by `05_populate_domain_family_map.sql`. This CSV is a stale export, do not load it |

## Loading

Seed data is loaded by `database/sql_archive/00_versioning/06_load_seed_data.sql`. Run it after `00_create_versioning_tables.sql`.

`domain_family_map` is **not** loaded from the CSV here. It is populated by `05_populate_domain_family_map.sql`, which contains the full current set.

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

The CSV is missing the `name` and `aws_sso_username` columns that exist in the live table. The load script uses inline INSERT values instead of `\copy` to handle this. If you add a new bootstrap developer, add them to both the CSV and the INSERT in `06_load_seed_data.sql`.

## See also

- [`../../VERSIONING.md`](../../VERSIONING.md): the version families, the `set_audit_fields` authoring audit, `get_active_version()` / `coeqwal_current_operator()`, and the intended version-bump workflow.
- `database/sql_archive/00_versioning/`: the migrations that create these tables and helper functions.
