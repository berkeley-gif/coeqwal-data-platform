# COEQWAL Database

PostgreSQL database for COEQWAL scenario data, network, tiers, and statistics topology.

## Directory structure

```
database/
├── audit/                     # Audit and verification scripts
│   ├── generate_erd_from_audit.py
│   ├── verify_erd_against_audit.py
│   └── README.md
├── schema/                    # ERD and schema documentation
│   └── COEQWAL_SCENARIOS_DB_ERD.md
├── seed_tables/               # CSV seed data organized by layer
│   ├── 00_versioning/         # Version & developer seeds
│   ├── 01_lookup/             # Reference data
│   ├── 02_network/            # Network infrastructure
│   ├── 03_entity/             # Entity definitions
│   ├── 04_calsim_data/        # CalSim variables & entities
│   ├── 05_themes_scenarios/   # Theme & scenario definitions
│   ├── 06_assumptions_operations/
│   ├── 07_hydroclimate/
│   └── 10_tier/               # Tier results (layer 10)
├── scripts/
│   ├── sql/                   # SQL scripts
│   │   ├── 00_versioning/     # Audit triggers, versioning, verification
│   │   ├── 01_lookup/         # Lookup table verification
│   │   ├── 02_network/        # Network layer verification
│   │   ├── 11_reservoir_statistics/
│   │   ├── 12_mi_statistics/
│   │   ├── 13_ag_statistics/
│   │   └── validate_data_integrity.sql
└── utils/
    └── db_audit_lambda/       # Database audit Lambda function
```

## Schema layers

The database follows a layered architecture separating **foundational data** (00-08) from **derived results** (10+).

Arrows indicate **dependency** - each layer depends on layers above it.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        00_VERSIONING                                │
│  version_family, version, developer, domain_family_map              │
│  Purpose: Version control, audit trails, developer tracking         │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        01_LOOKUP                                    │
│  hydrologic_region, source, model_source, unit, spatial_scale,      │
│  temporal_scale, statistic_type, geometry_type,                     │
│  calsim_variable_type, variable_type                                │
│  Purpose: Reference/lookup tables shared across all layers          │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     02_NETWORK (Infrastructure)                     │
│  network, network_gis, network_arc_attribute, network_node_attribute│
│  network_physical_connectivity, network_operational_connectivity    │
│  network_entity_type, network_arc_type, network_node_type           │
│  Purpose: Physical water infrastructure (arcs, nodes, connectivity) │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     03_ENTITY                          │
│  reservoir_entity, channel_entity, inflow_entity                    │
│  du_urban_entity, du_agriculture_entity, du_refuge_entity           │
│  calsim_entity_type                                                 │
│  Purpose: Operational/management entities built on network          │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     04_CALSIM_DATA                      │
│  reservoir_variable, channel_variable, inflow_variable              │
│  reservoir_group, reservoir_group_member                            │
│  Purpose: CalSim model variables linked to entities                 │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     05_THEMES_SCENARIOS                             │
│  theme, scenario, scenario_author, theme_scenario_link              │
│  Purpose: Scenario definitions and research themes                  │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  06_ASSUMPTIONS_OPERATIONS                          │
│  assumption_category, assumption_definition, operation_category,    │
│  operation_definition, scenario_assumption, scenario_operation      │
│  Purpose: Scenario inputs and operational rules                     │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     07_HYDROCLIMATE                                 │
│  hydroclimate, hydroclimate_source, climate_projection              │
│  Purpose: Environmental boundary conditions (historical, projected) │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  08_TIER_DEFINITIONS (Classification)               │
│  tier_definition, variable_tier                                     │
│  Purpose: Define tier categories and how variables map to tiers     │
└─────────────────────────────────────────────────────────────────────┘
                                  │
══════════════════════════════════════════════════════════════════════
               FOUNDATIONAL (00-08) ▲  │  ▼ DERIVED (10+)
══════════════════════════════════════════════════════════════════════
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  10_TIER_RESULTS (Outcomes)                         │
│  tier_result, tier_location_result                                  │
│  Purpose: Aggregated scenario outcomes (tier levels 1-4)            │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  11_RESERVOIR_STATISTICS                            │
│  reservoir_monthly_percentile, reservoir_storage_monthly,           │
│  reservoir_spill_monthly, reservoir_period_summary                  │
│  Purpose: Reservoir storage and spill statistics from CalSim runs   │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  12_MI_STATISTICS                                   │
│  mi_contractor, mi_contractor_period_summary, du_delivery_monthly,  │
│  cws_aggregate_entity, cws_aggregate_period_summary                 │
│  Purpose: Municipal & Industrial water delivery statistics          │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  13_AG_STATISTICS                                   │
│  ag_aggregate_entity, ag_aggregate_period_summary,                  │
│  ag_delivery_monthly, ag_shortage_monthly                           │
│  Purpose: Agricultural water delivery statistics                    │
└─────────────────────────────────────────────────────────────────────┘
```

## Schema implementation status

| Layer | Key Tables | SQL Scripts | Status |
|-------|------------|-------------|--------|
| 00_VERSIONING | version_family, version, developer | 00_create_helper_functions.sql | Implemented |
| 01_LOOKUP | hydrologic_region, source, model_source | 03_create_and_load_network_lookups.sql | Implemented |
| 02_NETWORK | network, network_gis | seed_tables/02_network/ | Implemented |
| 03_ENTITY | reservoir_entity, channel_entity | seed_tables/03_entity/ | Implemented |
| 04_CALSIM_DATA | calsim_entity_type, *_variable | seed_tables/04_calsim_data/ | Implemented |
| 05_THEMES_SCENARIOS | theme, scenario, hydroclimate | create_scenario_tables.sql | Implemented |
| 06_ASSUMPTIONS_OPERATIONS | assumption_definition, operation_definition | seed_tables/06_assumptions_operations/ | Partial |
| 07_HYDROCLIMATE | hydroclimate | seed_tables/07_hydroclimate/ | Implemented |
| 08_TIER_DEFINITIONS | tier_definition, variable_tier | create_tier_location_result_table.sql | Implemented |
| 10_TIER_RESULTS | tier_result, tier_location_result | upsert_tier_data_from_s3.sql | Implemented |
| 11_RESERVOIR_STATISTICS | reservoir_monthly_percentile, reservoir_period_summary | 11_reservoir_statistics/*.sql | Implemented |
| 12_MI_STATISTICS | mi_contractor, du_delivery_monthly | 12_mi_statistics/*.sql | Implemented |
| 13_AG_STATISTICS | ag_aggregate_entity, ag_aggregate_period_summary | 13_ag_statistics/*.sql | Implemented |

---

## Layer details

### 02_NETWORK: network categorization

The network layer represents CalSim's water infrastructure as connected arcs and nodes.

**Table hierarchy:**
```
network (master registry - 6908 records)
├── network_arc (arc-specific attributes - 2610 records)
└── network_node (node-specific attributes - 1544 records)

Classification tables:
├── network_entity_type (4 types)
├── network_type (21 types)
└── network_subtype (26 subtypes)
```

**Network types and subtypes:**

| Type Code | Description | Networks | Subtypes |
|-----------|-------------|----------|----------|
| STR | Stream | 1310 | 8 |
| CH | Channel | 1139 | 10 |
| D | Delivery | 539 | 0 |
| RT | Return flow | 259 | 0 |
| X | Demand unit | 240 | 4 |
| IN | Inflow | 225 | 0 |
| S | Storage | 94 | 1 |
| WTP | Water treatment plant | 42 | 0 |
| WWTP | Wastewater treatment plant | 22 | 0 |
| SP | Spreading/recharge | 8 | 0 |
| PS | Pump station | 3 | 0 |
| OM | Operations & maintenance | 0 | 2 |
| CH_N | Channel node | 0 | 1 |
| NP, DD, PR, DA, SR, RFS, CT, NULL_A | (unused/deprecated) | 0 | 0 |

**Subtypes by parent type:**

- **CH (Channel):** 10 subtypes for channel classification
- **STR (Storage):** 8 subtypes (e.g., major reservoirs, CVP, SWP)
- **X (Other):** 4 subtypes for miscellaneous elements
- **OM:** 2 subtypes for O&M facilities
- **S (Source):** 1 subtype

Query to explore subtypes:
```sql
-- Get subtypes grouped by parent type
SELECT 
    nt.short_code as type_code,
    nt.label as type_label,
    ns.short_code as subtype_code,
    ns.label as subtype_label
FROM network_type nt
LEFT JOIN network_subtype ns ON ns.type_id = nt.id
WHERE ns.id IS NOT NULL
ORDER BY nt.short_code, ns.short_code;
```

Example output (26 subtypes):
```
 type_code |        type_label        | subtype_code |    subtype_label    
-----------+--------------------------+--------------+---------------------
 CH        | Channel                  | BP           | Bypass
 CH        | Channel                  | CH           | Channel
 CH        | Channel                  | CL           | Canal
 CH        | Channel                  | HIS          | Historical
 CH        | Channel                  | IM           | Imported
 CH        | Channel                  | LI           | Link
 CH        | Channel                  | NA           | Not applicable
 CH        | Channel                  | NS           | Not simulated
 CH        | Channel                  | PRP          | Proposed
 CH        | Channel                  | ST           | Stream
 CH_N      | Channel node             | BYP          | Bypass
 OM        | Operations & maintenance | OMD          | O&M demand
 OM        | Operations & maintenance | OMR          | O&M return
 S         | Storage                  | Reservoir    | Reservoir
 STR       | Stream                   | CNL          | Canal
 STR       | Stream                   | GWO          | Groundwater outflow
 STR       | Stream                   | NA           | Not applicable
 STR       | Stream                   | NSM          | Non-Sacramento
 STR       | Stream                   | PRP          | Proposed
 STR       | Stream                   | SG           | Stream gauge
 STR       | Stream                   | SIM          | Simulated
 STR       | Stream                   | STM          | Stream
 X         | Demand unit              | A            | Agricultural
 X         | Demand unit              | R            | Refuge
 X         | Demand unit              | U            | Urban
 X         | Demand unit              | X            | Other Demand
```

**Source tracking:**
- All network_arc records have `source_id` and `model_source_id` (100% coverage)
- All network_node records have `source_id` and `model_source_id` (100% coverage)
- All network_gis records have `source_id` (100% coverage)
- Primary source: `geopackage` (CalSim GeoSchematic)

**River codes (network_arc.river):**
- 459 distinct CalSim river/waterway codes
- Examples: SAC (Sacramento), SJR (San Joaquin), FTR (Feather), DMC (Delta-Mendota Canal)
- Stored as text (high cardinality makes lookup impractical)

---

## Best practices checklist

### Database best practices

- [ ] **Referential Integrity** - All FKs reference valid PKs
  - Implemented: All tables use explicit FK constraints with `REFERENCES` clause
  - Audit: `validate_data_integrity.sql` checks for orphaned records

- [ ] **Constraints** - CHECK constraints for valid ranges, NOT NULL for required fields
  - Implemented: `water_month BETWEEN 1 AND 12`, `tier_level BETWEEN 1 AND 4`
  - Implemented: `is_active`, `short_code` are NOT NULL where required

- [x] **Audit Fields** - `created_at`, `created_by`, `updated_at`, `updated_by` on all tables
  - Implemented: All domain tables include audit fields
  - Implemented: `set_audit_fields()` trigger auto-populates all audit fields on INSERT/UPDATE
  - Implemented: `coeqwal_current_operator()` function identifies current user via SSO
  - Implemented: `audit_log` table tracks all INSERT/UPDATE/DELETE with old/new values

- [ ] **Indexes** - On FKs, frequently queried columns, unique constraints
  - Implemented: All `short_code` columns have unique indexes
  - Implemented: FK columns indexed for join performance

- [ ] **Naming Conventions** - Consistent table/column naming
  - Implemented: `snake_case` for all tables and columns
  - Implemented: `*_id` suffix for FK columns, `*_entity` suffix for entity tables

### Data integrity best practices

- [ ] **Completeness** - No unexpected NULLs, all required records present
  - Audit: Check record counts match expected (see layer audits)
  - Audit: Check required fields are populated

- [ ] **Consistency** - References match across tables, no orphans
  - Audit: `validate_data_integrity.sql` orphan checks
  - Audit: Version family consistency (each family has exactly 1 active version)

- [ ] **Validity** - Values within expected ranges/enums
  - Implemented: CHECK constraints enforce ranges
  - Audit: Validate `water_month`, `tier_level`, `location_type`

- [ ] **Accuracy** - Data matches source of truth
  - Audit: Compare database records against seed CSVs
  - Audit: ERD verification against actual schema

### API best practices

- [ ] **Validation** - Reject invalid data at API layer before DB
  - Implemented: FastAPI Pydantic models validate input
  - Implemented: Type checking and range validation

- [ ] **Error Handling** - Clear error messages, proper HTTP codes
  - Implemented: Structured error responses with details

- [ ] **Consistency** - Same response format across endpoints
  - Implemented: Standard response envelope with `data`, `meta`, `errors`

---

## Layer 00_VERSIONING schema

The versioning layer provides audit trails and version control for all other layers.

```
┌─────────────────────────────────┐       ┌─────────────────────────────────┐
│        developer                │       │       version_family            │
├─────────────────────────────────┤       ├─────────────────────────────────┤
│ id (PK)                         │       │ id (PK)                         │
│ email (UNIQUE)                  │       │ short_code (UNIQUE, NOT NULL)   │
│ display_name (NOT NULL)         │       │ label                           │
│ role                            │       │ description                     │
│ aws_sso_username (UNIQUE)       │◄──────│ created_by (FK)                 │
│ is_bootstrap                    │       │ updated_by (FK)                 │
│ sync_source                     │       │ is_active                       │
│ is_active                       │       │ created_at, updated_at          │
│ created_at, updated_at          │       └─────────────────────────────────┘
└─────────────────────────────────┘                      │
         ▲                                               │ 1:N
         │                                               ▼
         │                           ┌─────────────────────────────────┐
         │                           │          version                │
         │                           ├─────────────────────────────────┤
         │                           │ id (PK)                         │
         │                           │ version_family_id (FK)          │
         │                           │ version_number                  │
         └───────────────────────────│ created_by (FK)                 │
                                     │ updated_by (FK)                 │
                                     │ manifest (JSONB)                │
                                     │ changelog                       │
                                     │ is_active                       │
                                     └─────────────────────────────────┘
                                                         │
                                                         │ 1:N
                                                         ▼
                                     ┌─────────────────────────────────┐
                                     │     domain_family_map           │
                                     ├─────────────────────────────────┤
                                     │ schema_name (PK)                │
                                     │ table_name (PK)                 │
                                     │ version_family_id (FK)          │
                                     │ target_version_column           │
                                     │ note                            │
                                     └─────────────────────────────────┘
```

**Key functions:**
- `coeqwal_current_operator()` - Returns developer.id for audit fields (SSO-aware)
- `get_active_version(family)` - Returns active version.id for a family
- `set_audit_fields()` - Trigger function for automatic audit field population

**Expected records:**
- `developer`: 2+ (system + admin bootstrap users + SSO users)
- `version_family`: 13 (one per domain, including 'statistics' for id=7)
- `version`: 13 (one active version per family)
- `domain_family_map`: 11+ (maps tables to version families)

---

## Automatic audit triggers

All tables have automatic audit field population via database triggers.

### How it works

| Event | created_at | created_by | updated_at | updated_by |
|-------|------------|------------|------------|------------|
| INSERT | `NOW()` | `coeqwal_current_operator()` | `NOW()` | `coeqwal_current_operator()` |
| UPDATE | preserved | preserved | `NOW()` | `coeqwal_current_operator()` |

### Developer detection (it's strict)

`coeqwal_current_operator()` identifies the current user through multiple strategies:
1. Match `aws_sso_username` column
2. Match email containing database username
3. Match name/display_name containing database username
4. **FAIL with exception if no match** - unregistered users cannot make changes

**Important:** Each developer must have their own database user registered in the `developer` table before making changes.

### Database access

The database is **not publicly accessible**. Access requires:

1. **Network access** - The RDS database is in a private AWS VPC
   - Cloud9 (within AWS) has direct access
   - Local access requires VPN or SSH tunnel through a bastion host

2. **Database credentials** - Host, port, username, password
   - Stored in environment variables or AWS Secrets Manager
   - Never committed to the repository

3. **AWS account access** - Required to use Cloud9 or retrieve credentials

**Access summary:**

| User | Network Access | Query (SELECT) | Modify (INSERT/UPDATE/DELETE) |
|------|----------------|----------------|-------------------------------|
| Berkeley GIF team | Cloud9 | ✅ | ✅ (as registered developer) |
| Registered developer | Cloud9 or VPN | ✅ | ✅ (as themselves) |
| postgres superuser | Cloud9 or VPN | ✅ | ✅ (as system account) |
| Unregistered db user | Cloud9 or VPN | ✅ | ❌ (blocked by trigger) |
| General public | ❌ None | ❌ | ❌ |

**Security layers:**
- AWS VPC (network isolation)
- Security groups (firewall rules)  
- Database authentication (username/password)
- Application-level checks (`coeqwal_current_operator()` for writes)

### Setting up a new developer

Use the `register_developer()` function (run as postgres):

```sql
-- Register a new developer
SELECT register_developer(
    'jdoe',                    -- database username
    'jdoe@berkeley.edu',       -- email
    'Jane Doe',                -- display name
    'secure_password_here',    -- password (change immediately!)
    'developer'                -- role: 'admin' or 'developer'
);

-- List all registered developers
SELECT * FROM list_developers();

-- Change password after first login
ALTER USER jdoe WITH PASSWORD 'new_secure_password';
```

**After registration, connect as your user** (not postgres):

```bash
psql -h <rds-endpoint> -U jdoe -d coeqwal_scenario
```

**Important:** Unregistered users cannot make database changes. The `coeqwal_current_operator()` function will raise an exception.
### audit_log table

All changes are recorded in the `audit_log` table:

```sql
-- Recent changes
SELECT table_name, operation, changed_fields, changed_by, changed_at
FROM audit_log
ORDER BY changed_at DESC
LIMIT 20;

-- Changes to a specific table
SELECT * FROM audit_log WHERE table_name = 'scenario';

-- Changes by a specific user
SELECT * FROM audit_log WHERE changed_by = 2;
```

### Scripts

Audit trigger scripts are in `scripts/sql/00_versioning/`:
- `00_create_audit_trigger_function.sql` - Creates `set_audit_fields()` trigger function
- `01_create_audit_log_table.sql` - Creates `audit_log` table for change tracking
- `03_apply_audit_triggers.sql` - Applies triggers to all tables

### Verification queries

```sql
-- Check triggers are applied
SELECT trigger_name, event_object_table 
FROM information_schema.triggers 
WHERE trigger_name LIKE 'audit_%';

-- Check audit log entries
SELECT table_name, operation, COUNT(*) 
FROM audit_log 
GROUP BY table_name, operation;

-- Enable audit logging on sensitive tables (run once)
SELECT apply_audit_log_trigger_to_table('developer');
SELECT apply_audit_log_trigger_to_table('version');
SELECT apply_audit_log_trigger_to_table('version_family');
SELECT apply_audit_log_trigger_to_table('scenario');
```

---

## Auditing and verification

### Regular audits

Run these audits periodically to ensure database integrity:

#### 1. Database structure audit

Run the Lambda audit function to capture current database state:

```bash
# Via AWS CLI
aws lambda invoke --function-name coeqwal-database-audit --region us-west-2 response.json
cat response.json

# Download reports from S3
aws s3 ls s3://coeqwal-model-run/database_audits/ --recursive | tail -5
aws s3 cp s3://coeqwal-model-run/database_audits/audit_YYYYMMDD_HHMMSS.json ./audits/
aws s3 cp s3://coeqwal-model-run/database_audits/tables_summary_YYYYMMDD_HHMMSS.csv ./audits/
```

See [utils/db_audit_lambda/README.md](utils/db_audit_lambda/README.md) for full documentation.

#### 2. ERD vs database verification

Compare the documented ERD against actual database structure:

```bash
cd database/audit

# Verify ERD matches database
python verify_erd_against_audit.py ../schema/COEQWAL_SCENARIOS_DB_ERD.md ../../audits/latest.json

# Generate fresh ERD from database audit (if updates needed)
python generate_erd_from_audit.py ../../audits/latest.json ../schema/GENERATED_ERD.md
```

See [audit/README.md](audit/README.md) for full documentation.

#### 3. Data integrity checks (after ETL runs)

Run these SQL queries to verify data integrity:

```sql
-- Check for orphaned statistics (no matching scenario)
SELECT 'reservoir_period_summary' as table_name, COUNT(*) as orphans
FROM reservoir_period_summary rps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = rps.scenario_id)
UNION ALL
SELECT 'mi_contractor_period_summary', COUNT(*)
FROM mi_contractor_period_summary mps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = mps.scenario_id)
UNION ALL
SELECT 'ag_aggregate_period_summary', COUNT(*)
FROM ag_aggregate_period_summary aps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = aps.scenario_id);

-- Check scenarios have statistics data
SELECT s.id, s.name,
       (SELECT COUNT(*) FROM reservoir_period_summary rps WHERE rps.scenario_id = s.id) as reservoir_stats,
       (SELECT COUNT(*) FROM mi_contractor_period_summary mps WHERE mps.scenario_id = s.id) as mi_stats,
       (SELECT COUNT(*) FROM ag_aggregate_period_summary aps WHERE aps.scenario_id = s.id) as ag_stats
FROM scenario s
ORDER BY s.id;

-- Check for NULL audit fields (should be 0)
SELECT 'Tables missing created_by' as check_type,
       (SELECT COUNT(*) FROM reservoir_entity WHERE created_by IS NULL) +
       (SELECT COUNT(*) FROM du_urban_entity WHERE created_by IS NULL) +
       (SELECT COUNT(*) FROM mi_contractor WHERE created_by IS NULL) as count;

-- Check water_month values are valid (1-12)
SELECT 'Invalid water_month values' as check_type,
       (SELECT COUNT(*) FROM reservoir_monthly_percentile WHERE water_month NOT BETWEEN 1 AND 12) +
       (SELECT COUNT(*) FROM du_delivery_monthly WHERE water_month NOT BETWEEN 1 AND 12) as count;
```

### Audit schedule

| Audit | Frequency | Owner | Notes |
|-------|-----------|-------|-------|
| Lambda structure audit | Weekly-monthly | Automated | Check S3 for reports |
| ERD verification | Monthly | Developer | Before major releases |
| Data integrity checks | After ETL | ETL pipeline | Run via validation scripts |
| Record count comparison | Weekly-montly | Developer | Compare against expected counts |

### Audit reports location

- **S3**: `s3://coeqwal-model-run/database_audits/`
- **Local**: `./audits/` (gitignored)

### Layer verification scripts

Each database layer has a verification script that checks:

| # | Check | Description |
|---|-------|-------------|
| 1 | Audit columns | created_at, created_by, updated_at, updated_by exist |
| 2 | Audit triggers | `audit_fields_*` trigger applied |
| 3 | Version family mapping | Table registered in domain_family_map |
| 4 | FK relationships | FKs to developer and lookup tables |
| 5 | Row counts | Tables are populated |
| 6 | Schema accuracy | Key columns exist |
| 7 | Data integrity | No orphan FKs, no unexpected duplicates |
| 8 | Naming conventions | No plural table names |
| 9 | Layer-specific | Depends on layer (e.g., network connectivity) |

**Verification script locations:**

```
database/scripts/sql/
├── 00_versioning/09_verify_level00.sql   # Versioning layer
├── 01_lookup/09_verify_level01.sql       # Lookup/reference tables
├── 02_network/09_verify_level02.sql      # Network layer
└── ... (additional layers as audited)
```

**Running verification in Cloud9:**

```sql
-- Connect to database
psql -h $DB_HOST -U $DB_USER -d coeqwal_scenario

-- Run layer verification
\i database/scripts/sql/00_versioning/09_verify_level00.sql
\i database/scripts/sql/01_lookup/09_verify_level01.sql
\i database/scripts/sql/02_network/09_verify_level02.sql
```

**Layer audit modus operandi:**

1. **Discovery** - List tables in layer, check version family mapping
2. **Run verification script** - Identifies issues
3. **Create migration scripts** - Only for issues found (one-time use)
4. **Execute and verify** - Run migrations, re-run verification
5. **Delete migration scripts** - Keep repo clean after execution
6. **Document** - Update ERD/checklist if patterns change

---

## Data validation tools

### ETL validation

CSV validation scripts for verifying ETL output:

```bash
# Compare CSVs
python etl/coeqwal-etl/python-code/validate_csvs.py \
  --reference data/reference/expected.csv \
  --output data/output/actual.csv

# Enhanced validation with reports
python etl/coeqwal-etl/python-code/validate_csvs_improved.py \
  --reference data/reference/ \
  --output data/output/ \
  --report validation_report.json
```

### Database constraints

Tables include CHECK constraints for data validation:
- `water_month` must be 1-12
- `tier_level` must be 1-4 (enforced: `CHECK (tier_level BETWEEN 1 AND 4 OR tier_level IS NULL)`)
- `location_type` must be valid enum value

---

## Development setup

### Cloud9 development workflow

The recommended workflow for database changes:

```
┌──────────────┐     git push     ┌──────────────┐     git pull     ┌──────────────┐
│   Local Dev  │ ───────────────► │    GitHub    │ ◄─────────────── │   Cloud9     │
│   (Cursor)   │                  │  (main repo) │                  │   (AWS)      │
└──────────────┘                  └──────────────┘                  └──────┬───────┘
                                                                          │
                                                                          │ psql
                                                                          ▼
                                                                   ┌──────────────┐
                                                                   │   RDS        │
                                                                   │  (Postgres)  │
                                                                   └──────────────┘
```

1. **Local**: Edit SQL scripts in Cursor
2. **GitHub**: Push changes to main branch
3. **Cloud9**: Pull latest from GitHub
4. **RDS**: Run SQL scripts via psql

### Running SQL scripts in Cloud9

```bash
# Pull latest from GitHub
cd ~/environment/coeqwal-backend
git pull origin main

# Connect to database
psql -h coeqwal-scenario-database-1.xxxxx.us-west-2.rds.amazonaws.com \
     -U postgres -d coeqwal_scenario

# Run scripts
\i database/scripts/sql/00_versioning/00_create_audit_trigger_function.sql
\i database/scripts/sql/00_versioning/01_create_audit_log_table.sql
-- etc.
```

### Connect to production (read-only)

```bash
# Set up SSO credentials
aws sso login --profile coeqwal-dev

# Connect via psql
psql "postgresql://user:pass@coeqwal-db.xxxxx.us-west-2.rds.amazonaws.com:5432/coeqwal"
```

---

## Troubleshooting

### Potential issues

**Missing statistics for scenario**
- Check ETL pipeline completed successfully
- Verify scenario_id exists in scenario table
- Run data integrity checks above

**ERD out of sync**
- Run ERD verification script
- Update COEQWAL_SCENARIOS_DB_ERD.md
- Document changes in schema/.archive/

**Audit Lambda fails**
- Check VPC configuration
- Verify security group allows Lambda → RDS
- See [utils/db_audit_lambda/README.md](utils/db_audit_lambda/README.md)
