# Table standards checklist

Use this checklist when creating new tables or auditing existing tables.

## Table creation checklist

### 1. Required columns

- [ ] `id` - SERIAL PRIMARY KEY (or appropriate key)
- [ ] `is_active` - BOOLEAN DEFAULT TRUE (for soft deletes)
- [ ] `created_at` - TIMESTAMPTZ DEFAULT NOW()
- [ ] `created_by` - INTEGER REFERENCES developer(id)
- [ ] `updated_at` - TIMESTAMPTZ DEFAULT NOW()
- [ ] `updated_by` - INTEGER REFERENCES developer(id)

### 2. Naming conventions

- [ ] Table name: lowercase, snake_case, singular or descriptive plural
- [ ] Column names: lowercase, snake_case
- [ ] Foreign keys: `{referenced_table}_id` (e.g., `hydrologic_region_id`)
- [ ] Short codes: `short_code` TEXT UNIQUE NOT NULL
- [ ] Display names: `label` TEXT NOT NULL

### 3. Audit trigger

- [ ] Apply `set_audit_fields` trigger after table creation:
  ```sql
  SELECT apply_audit_trigger_to_table('your_table_name');
  ```

### 4. Domain family mapping

- [ ] Add entry to `domain_family_map`:
  ```sql
  INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note)
  VALUES ('public', 'your_table_name', {version_family_id}, 'Description');
  ```

### 5. Foreign key relationships

Use FK IDs, **never store lookup values as text**.

| If you need... | Reference this lookup | Column name |
|----------------|----------------------|-------------|
| Geographic region | `hydrologic_region` | `hydrologic_region_id` |
| Data source | `source` | `source_id` |
| Model source | `model_source` | `model_source_id` |
| Measurement unit | `unit` | `unit_id` |
| Spatial scale | `spatial_scale` | `spatial_scale_id` |
| Temporal scale | `temporal_scale` | `temporal_scale_id` |
| Statistic type | `statistic_type` | `statistic_type_id` |
| Geometry type | `geometry_type` | `geometry_type_id` |
| Variable type | `variable_type` | `variable_type_id` |

Example:
```sql
CREATE TABLE my_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    hydrologic_region_id INTEGER REFERENCES hydrologic_region(id),  -- YES
    -- hydrologic_region TEXT,  -- NO! Never store as text
    source_id INTEGER REFERENCES source(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    created_by INTEGER REFERENCES developer(id),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    updated_by INTEGER REFERENCES developer(id)
);
```

---

## Data population checklist

### 1. Before inserting data

- [ ] Verify all referenced lookup values exist
- [ ] Use lookup IDs, not text values
- [ ] Set `created_by` and `updated_by` (trigger handles this if columns exist)

### 2. Lookup value verification

```sql
-- Verify lookup values exist before inserting
SELECT id, short_code FROM hydrologic_region WHERE short_code = 'SAC';
SELECT id, source FROM source WHERE source = 'calsim_report';
```

### 3. Insert pattern

```sql
-- Good: Use FK IDs
INSERT INTO my_table (name, hydrologic_region_id, source_id)
VALUES ('Example', 
        (SELECT id FROM hydrologic_region WHERE short_code = 'SAC'),
        (SELECT id FROM source WHERE source = 'calsim_report'));

-- Bad: Store text values
INSERT INTO my_table (name, hydrologic_region, source)
VALUES ('Example', 'SAC', 'calsim_report');  -- NO!
```

### 4. Adding new lookup values

If a needed lookup value doesn't exist, add it first:

```sql
-- Add new lookup value
INSERT INTO hydrologic_region (short_code, label, is_active)
VALUES ('NEW_REGION', 'New Region Name', true);

-- Then reference it
INSERT INTO my_table (name, hydrologic_region_id)
VALUES ('Example', (SELECT id FROM hydrologic_region WHERE short_code = 'NEW_REGION'));
```

---

## Lookup tables reference

| Lookup Table | Key Column | Display Column | Values |
|--------------|------------|----------------|--------|
| `hydrologic_region` | `short_code` | `label` | SAC, SJR, DELTA, TULARE, SOCAL, EXTERNAL |
| `source` | `source` | `description` | calsim_report, james_gilbert, etc. |
| `model_source` | `short_code` | `name` | calsim3 |
| `unit` | `short_code` | `full_name` | TAF, CFS, acres, mm, km |
| `spatial_scale` | `short_code` | `label` | system_wide, regional, basin, etc. |
| `temporal_scale` | `short_code` | `label` | daily, weekly, monthly, etc. |
| `statistic_type` | `short_code` | `label` | MEAN, MEDIAN, MIN, MAX, STDEV, CV, Q0, Q10, Q30, Q50, Q70, Q90, Q100 |
| `geometry_type` | `short_code` | `label` | POINT, LINESTRING, POLYGON, MULTIPOLYGON |
| `variable_type` | `short_code` | `label` | output, input, decision |

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
        WHERE tr.event_object_table = t.table_name 
        AND tr.trigger_name LIKE 'audit_fields_%'
    ) THEN 'Y' ELSE 'N' END as has_trigger,
    CASE WHEN EXISTS (
        SELECT 1 FROM domain_family_map dfm 
        WHERE dfm.table_name = t.table_name
    ) THEN 'Y' ELSE 'N' END as in_domain_map
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
    ON t.table_name = c.table_name 
    AND c.column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by')
WHERE t.table_schema = 'public' 
AND t.table_name = 'YOUR_TABLE_NAME'
GROUP BY t.table_name;
```
