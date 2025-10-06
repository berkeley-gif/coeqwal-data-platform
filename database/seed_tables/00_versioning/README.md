# COEQWAL database versioning system

The COEQWAL database implements a comprehensive versioning system for controlled evolution of schemas and data. 

## 📁 Directory contents

```
00_versioning/
├── developer.csv               # Bootstrap users and AWS SSO integration
├── version_family.csv          # Version domains (theme, scenario, etc.)
├── version.csv                 # Version instances within families
├── domain_family_map.csv       # Maps tables to version families
└── README.md                   # This documentation
```

## Architecture Overview

### Three-layer structure

1. **Version Family** - Logical domain grouping (theme, scenario, assumption, etc.)
2. **Version** - Specific instance within a family (v1.0.0, v1.1.0, etc.)
3. **Domain Tables** - Tables that belong to each version family

### Core tables

| Table | Purpose | Records |
|-------|---------|---------|
| `version_family` | Define versioning domains | 13 |
| `version` | Specific version instances (immutable) | 13 |
| `domain_family_map` | Map tables to version families | 35 |
| `developer` | Bootstrap users for system initialization | 2 |

## Version families

The system organizes data into 13 version families:

| Family | Description | Example Tables |
|--------|-------------|----------------|
| `theme` | Research themes and storylines | theme |
| `scenario` | Water management scenarios | scenario, scenario_* |
| `assumption` | Scenario assumptions and parameters | assumption_definition |
| `operation` | Operational policies and rules | operation_definition |
| `hydroclimate` | Hydroclimate conditions | hydroclimate |
| `variable` | CalSim model variables | variable, *_variable |
| `outcome` | Outcome measurement systems | outcome_category, outcome_measure |
| `tier` | Tier definitions and systems | tier_definition, variable_tier |
| `geospatial` | Geographic data definitions | geometry |
| `interpretive` | Analysis frameworks | analysis, key_concept |
| `metadata` | Data metadata and documentation | constant, model_value |
| `network` | CalSim network topology | network, network_* |
| `entity` | Entity data versions | *_entity |

## Versioning functions

The system provides several PostgreSQL functions for version management:

### `get_active_version(family_name TEXT) RETURNS INTEGER`

Returns the active version ID for a given version family.

```sql
-- Usage
SELECT get_active_version('network');  -- Returns active network version ID
SELECT get_active_version('scenario'); -- Returns active scenario version ID

-- Example in table defaults
network_version_id INTEGER DEFAULT get_active_version('network')
```

### `coeqwal_current_operator() RETURNS INTEGER`

Returns the current user's developer ID for audit fields.

```sql
-- Usage in audit fields
created_by INTEGER DEFAULT coeqwal_current_operator()
updated_by INTEGER DEFAULT coeqwal_current_operator()

-- Behavior
-- 1. Tries to match current_user to developer.aws_sso_username
-- 2. Tries to match current_user to developer.email pattern
-- 3. Falls back to admin account (ID 2)
```

## Table classification

### Versioned tables

Tables with version-sensitive content that affects reproducibility:

**Characteristics:**
- Domain-specific data that evolves with research
- Include explicit version_id field (e.g., `theme_version_id`, `scenario_version_id`)
- Listed in `domain_family_map.csv`
- Use `get_active_version()` function for defaults

**Examples:**
- `scenario` (scenario_version_id)
- `network` (network_version_id)
- `channel_entity` (entity_version_id)
- `tier_definition` (tier_version_id)

### Non-versioned tables

Infrastructure and lookup tables that remain stable:

**Characteristics:**
- Reference/lookup data
- Infrastructure configuration
- System metadata
- No version_id fields

**Examples:**
- `developer`, `source`, `unit`
- `hydrologic_region`, `geometry_type`
- `temporal_scale`, `spatial_scale`

## Version management workflow

### Creating new versions

1. **Identify scope** - Which version family needs updating?
2. **Create new version** - Add record to `version` table
3. **Update references** - Point tables to new version ID
4. **Validate dependencies** - Ensure referential integrity

### Version queries

```sql
-- Get current active versions
SELECT vf.short_code, v.version_number, v.changelog
FROM version_family vf
JOIN version v ON vf.id = v.version_family_id
WHERE v.is_active = true;

-- Get all tables in a version family
SELECT table_name, note
FROM domain_family_map dfm
JOIN version_family vf ON dfm.version_family_id = vf.id
WHERE vf.short_code = 'network';

-- Get version history for a family
SELECT v.version_number, v.changelog, v.created_at
FROM version v
JOIN version_family vf ON v.version_family_id = vf.id
WHERE vf.short_code = 'scenario'
ORDER BY v.created_at;
```

## Implementation notes

### Database functions

The versioning functions are persistent PostgreSQL objects:
- Created once during initial setup
- Persist across database sessions
- Available to all database users
- Can be audited with: `\df coeqwal_*`

### Best practices

1. **Immutable versions** - Never modify existing version records
2. **Semantic versioning** - Use major.minor.patch format
3. **Descriptive changelogs** - Document what changed and why
4. **Dependency tracking** - Understand cross-family impacts
5. **Regular audits** - Verify version consistency

## Troubleshooting

### Common issues

**Function not found errors:**
```sql
-- Check if functions exist
\df coeqwal_*
\df get_active_version
```

**Version reference errors:**
```sql
-- Check active versions
SELECT * FROM version WHERE is_active = true;

-- Verify domain mappings
SELECT * FROM domain_family_map WHERE table_name = 'your_table';
```

### Schema migration

When updating from old schema:
1. **Backup existing data**
2. **Update table references** (network_topology_id → network_id)
3. **Migrate to new version families**
4. **Validate referential integrity**
5. **Update version mappings**

## 🚀 Status

The versioning system is fully operational with:
- 13 version families covering all domains
- 13 active versions (1 per family)
- 35 table mappings to version families
- Complete function library for version management

All seed tables (stored in the S3 bucket as well as the backend repo) are synchronized with the production database.