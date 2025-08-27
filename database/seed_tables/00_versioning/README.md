# COEQWAL Versioning System

The COEQWAL database includes both versioned and non-versioned tables designed for **controlled evolution** of database schemas and data across different research domains.

## 📁 Directory contents

```
00_versioning/
├── user.csv                    # Bootstrap users and AWS SSO integration
├── version_family.csv          # Version domains (theme, scenario, etc.)
├── version.csv                 # Version instances within families
├── domain_family_map.csv       # Maps tables to version families
└── README.md                   # This comprehensive guide
```

## 🎯 Overview

The versioning system provides:
- **Reproducible research** - Track exactly which data versions were used
- **Parallel development** - Different teams can work on different versions
- **Rollback capability** - Revert to previous versions if needed
- **Impact analysis** - Understand dependencies between domain changes
- **Graceful degradation** - Mixed versioned/non-versioned tables work seamlessly

## 🏗️ Architecture

### 3-Layer structure

1. **Version Family** - Logical domain (theme, scenario, assumption, etc.)
2. **Version** - Specific instance within a family (v1.0.0, v1.1.0, etc.)
3. **Domain Tables** - Tables that belong to each version family

### Key tables

| Table | Purpose |
|-------|---------|
| `version_family` | Define versioning domains (theme, scenario, etc.) |
| `version` | Specific version instances (immutable once created) |
| `domain_family_map` | Map tables to version families |
| `user` | Bootstrap users for system initialization |

## 🔍 Table classification

### ✅ **VERSIONED TABLES** (include in domain_family_map)

Tables that should be versioned have:
- **Domain-specific data** that evolves with research
- **Version-sensitive content** affecting reproducibility  
- **Explicit version_id field** (e.g., `theme_version_id`, `scenario_version_id`)

**Examples:**
```sql
-- Research domain tables
theme (theme_version_id)
scenario (scenario_version_id)
assumption_definition (assumption_version_id)

-- Model-specific tables  
calsim_variable (variable_version_id)
hydroclimate (hydroclimate_version_id)

-- Analysis framework tables
outcome_measure (metrics_version_id)
tier_definition (tier_version_id)
```

### ❌ **NON-VERSIONED TABLES** (exclude from domain_family_map)

Tables that should NOT be versioned:
- **Infrastructure tables** (stable, rarely change)
- **Lookup tables** (reference data)
- **User management** (not research data)
- **No version_id field**

**Examples:**
```sql
-- Infrastructure
user, source, unit, hydrologic_region

-- Lookups
calsim_entity_type, calsim_schematic_type, temporal_scale
statistic_type, analysis_type, geometry_type

-- Links without versioning (inherit from parent)
theme_scenario_link, entity_source_link
```

### Decision tree

```
Does the table contain research/domain data?
├─ NO → DON'T VERSION (infrastructure/lookup table)
└─ YES → Does it have a *_version_id field?
   ├─ NO → DON'T VERSION (link table or missing field)
   └─ YES → VERSION IT (add to domain_family_map)
```

## 📋 Version table schema

### Fields explained

| Field | Type | Purpose |
|-------|------|---------|
| `version_family_id` | int | Which domain this version belongs to |
| `version_number` | text | Semantic version (e.g., "1.2.0") |
| `manifest` | jsonb | **Version metadata** (see below) |
| `changelog` | text | Human-readable changes |
| `is_active` | boolean | Only one version per family can be active |
| `created_by` | int | Who created this version |
| `created_at` | timestamp | When version was created |

### Manifest field (JSONB)

The `manifest` field contains version-specific metadata:

```json
{
  "config": {
    "validation_rules": ["require_gis_data", "validate_capacity_limits"],
    "feature_flags": ["enable_tier_calculations", "use_new_flow_algorithm"]
  },
  "data_sources": {
    "primary": "calsim_variables_v2.csv",
    "secondary": ["gis_data_2024.gpkg", "historical_flows.csv"]
  },
  "migration": {
    "from_version": "1.0.0",
    "scripts": ["migrate_storage_zones.sql"],
    "breaking_changes": true
  },
  "quality": {
    "validation_passed": true,
    "test_suite": "outcome_framework_tests_v1.2",
    "approved_by": "research_team"
  }
}
```

## 🏷️ Version families

### Current families

| ID | Short Code | Description | Example Tables |
|----|------------|-------------|----------------|
| 1 | theme | Research themes | theme, theme_source_link |
| 2 | scenario | Water scenarios | scenario, scenario_variable_statistic |
| 3 | assumption | Model assumptions | assumption_definition, assumption_param_* |
| 4 | operation | Operational rules | operation_definition, operation_param_* |
| 5 | outcome_framework | Outcome metrics | outcome_measure, tier_definition |
| 6 | calsim_variable | CalSim variables | calsim_variable, variable_group |
| 7 | hydroclimate | Climate data | hydroclimate, hydroclimate_source |
| 8 | spatial_data | Geographic data | geometry, calsim_entity |
| 9 | interpretive | Analysis framework | analysis, key_concept |
| 10 | metadata | System metadata | model_source, constant |

## 🗺️ Domain family map

### Purpose
The `domain_family_map` table tells the application **which tables belong to which version families**. This enables:
- Automated version queries
- Consistency checks
- Version-aware data loading

### Example usage
```sql
-- Get all tables in the theme version family
SELECT table_name 
FROM domain_family_map 
WHERE version_family_id = 1;

-- Get latest active version for theme tables
SELECT v.version_number, v.manifest
FROM version v
JOIN version_family vf ON v.version_family_id = vf.id
WHERE vf.short_code = 'theme' AND v.is_active = true;
```

## System design

### Application code pattern

```python
def get_versioned_data(table_name, version_family='theme'):
    """Get data with graceful version handling"""
    try:
        # Check if table is in versioning system
        if table_has_versioning(table_name):
            return get_versioned_table_data(table_name, version_family)
        else:
            # Non-versioned table - just return current data
            return get_table_data(table_name)
    except VersioningError as e:
        # Log warning but don't fail
        logger.warning(f"Versioning issue for {table_name}: {e}")
        return get_table_data(table_name)

def table_has_versioning(table_name):
    """Check if table participates in versioning"""
    return table_name in get_versioned_tables()
```

### Database helper functions

```sql
-- Check if table has versioning
CREATE OR REPLACE FUNCTION table_has_versioning(table_name text) 
RETURNS boolean AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM domain_family_map 
        WHERE domain_family_map.table_name = $1
    );
END;
$$ LANGUAGE plpgsql;

-- Get active version for table (graceful)
CREATE OR REPLACE FUNCTION get_active_version(table_name text)
RETURNS TABLE(version_id int, version_number text) AS $$
BEGIN
    IF table_has_versioning($1) THEN
        RETURN QUERY
        SELECT v.id, v.version_number
        FROM version v
        JOIN version_family vf ON v.version_family_id = vf.id
        JOIN domain_family_map dfm ON dfm.version_family_id = vf.id
        WHERE dfm.table_name = $1 AND v.is_active = true;
    ELSE
        -- Return null for non-versioned tables
        RETURN QUERY SELECT NULL::int, NULL::text;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

## ⚠️ Warning: missing tables

If a table has a `*_version_id` field but **isn't in domain_family_map**:

❌ **Application Failures**
- Version queries will fail
- Data loading scripts break
- Inconsistent version states

❌ **Data Integrity Issues**
- Orphaned version references
- Impossible to track data provenance
- Cannot rollback consistently

❌ **Operational Problems**
- Deployment failures
- Broken version tooling
- Manual intervention required

### Example failure scenario

```sql
-- If 'new_analysis_table' has analysis_version_id but isn't mapped:
SELECT t.*, v.version_number
FROM new_analysis_table t
JOIN version v ON t.analysis_version_id = v.id
WHERE v.is_active = true;
-- ❌ This works

-- But this fails:
SELECT table_name FROM domain_family_map 
WHERE version_family_id = (
  SELECT id FROM version_family WHERE short_code = 'interpretive'
);
-- ❌ Missing 'new_analysis_table' breaks version tooling
```

## ➕ Adding new tables

### Checklist for new tables

1. **Does the table need versioning?**
   - Contains domain-specific data?
   - Changes over time?
   - Affects research reproducibility?

2. **If YES, add to domain_family_map**
   ```sql
   INSERT INTO domain_family_map (table_name, version_family_id, note)
   VALUES ('new_table', 6, 'New table for calsim variable versioning');
   ```

3. **Add version_id field to table**
   ```sql
   ALTER TABLE new_table ADD COLUMN variable_version_id int NOT NULL;
   ALTER TABLE new_table ADD CONSTRAINT fk_version 
     FOREIGN KEY (variable_version_id) REFERENCES version(id);
   ```

4. **Update application code**
   - Version-aware queries
   - Data loading scripts
   - Validation rules

### If NO (infrastructure table)
- **Do nothing!** System will gracefully handle it
- Document why it doesn't need versioning
- Use standard queries (no version awareness needed)

## 🔄 Migration strategy

### Adding versioning to existing table
```sql
-- 1. Add version field
ALTER TABLE existing_table ADD COLUMN some_version_id int;

-- 2. Populate with current active version
UPDATE existing_table SET some_version_id = (
    SELECT id FROM version 
    WHERE version_family_id = X AND is_active = true
);

-- 3. Make it required
ALTER TABLE existing_table ALTER COLUMN some_version_id SET NOT NULL;

-- 4. Add foreign key
ALTER TABLE existing_table ADD CONSTRAINT fk_version
    FOREIGN KEY (some_version_id) REFERENCES version(id);

-- 5. Add to domain_family_map
INSERT INTO domain_family_map (table_name, version_family_id, note)
VALUES ('existing_table', X, 'Added versioning for existing table');
```

## 📊 Monitoring & troubleshooting

### Check version status
```sql
-- Active versions across all families
SELECT vf.short_code, v.version_number, v.created_at
FROM version v
JOIN version_family vf ON v.version_family_id = vf.id
WHERE v.is_active = true
ORDER BY vf.short_code;
```

### Find missing tables
```sql
-- Tables with version_id fields not in domain_family_map
SELECT table_name, column_name
FROM information_schema.columns
WHERE column_name LIKE '%_version_id'
AND table_name NOT IN (SELECT table_name FROM domain_family_map);
```

### Version conflicts
```sql
-- Multiple active versions (should be empty)
SELECT version_family_id, COUNT(*)
FROM version
WHERE is_active = true
GROUP BY version_family_id
HAVING COUNT(*) > 1;
```

## 🚀 Best practices

### Version numbering
- Use semantic versioning: `MAJOR.MINOR.PATCH`
- `MAJOR`: Breaking changes
- `MINOR`: New features
- `PATCH`: Bug fixes

### Use the manifest
- Always include validation status
- Document breaking changes
- Reference data sources
- Include migration instructions

### Version lifecycle
1. **Create** new version (inactive)
2. **Populate** with data
3. **Test** thoroughly
4. **Activate** (deactivates previous)
5. **Archive** old versions (keep for rollback)

## 👥 User management

### Bootstrap users
The system includes bootstrap users for initialization:
- **System User (ID=1)**: Required for database operations
- **Admin User (ID=2)**: Development and administration
- **Research User (ID=3)**: Research operations

### AWS SSO integration
- Production systems integrate with AWS SSO
- Bootstrap users remain for system operations
- See `scripts/utilities/sync_aws_sso_users.py` for integration

## 🔗 Related documentation

- **Implementation**: `scripts/utilities/versioning_utils.py`
- **ERD**: `database/schema/erd.txt`
- **Migration Guide**: `MIGRATION_GUIDE.md`

## ✅ Benefits

### Graceful degradation
- Non-versioned tables don't break the system
- Warnings logged, but system continues
- Easy to add versioning later if needed

### Clear guidelines  
- Explicit criteria for versioning decisions
- Reduces confusion for new developers
- Prevents over-engineering

### Flexible evolution
- Start simple, add versioning when needed
- Mixed versioned/non-versioned tables work fine
- No system-wide failures from missing tables