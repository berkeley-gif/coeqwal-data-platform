# COEQWAL Database

PostgreSQL database for COEQWAL scenario data, network, tiers, and statistics topology.

## Table of contents

1. [Making changes to the database](#making-changes-to-the-database)
   - [Connection strings](#connection-strings)
   - [Turning on the Cloud9 environment](#turning-on-the-cloud9-environment)
   - [Adding new scenarios](#adding-new-scenarios)
   - [Adding new scenario data (results/statistics)](#adding-new-scenario-data-resultsstatistics)
   - [Adding new tiers (future)](#adding-new-tiers-future)
   - [Adding tier data for new scenarios](#adding-tier-data-for-new-scenarios)
   - [Redeploying the API](#redeploying-the-api)
2. [Running the database audit](#running-the-database-audit)
   - [Running the monthly audit](#running-the-monthly-audit)
   - [What it produces](#what-it-produces)
   - [Report sections](#report-sections)
   - [Other audit tools](#other-audit-tools)
   - [Lambda S3 archive](#lambda-s3-archive)
3. [Getting started](#getting-started)
4. [Directory structure](#directory-structure)
5. [Schema layers](#schema-layers)
6. [Schema implementation status](#schema-implementation-status)
7. [Layer details](#layer-details)
8. [Best practices checklist](#best-practices-checklist)
9. [Layer 00_VERSIONING schema](#layer-00_versioning-schema)
10. [Automatic audit triggers](#automatic-audit-triggers)
11. [Audit and verification strategy](#audit-and-verification-strategy)
12. [Data validation tools](#data-validation-tools)

## Making changes to the database

### Connection strings

All database operations use one of two connection strings stored in `~/.bashrc` on Cloud9:

- **`$DATABASE_URL`** — a developer's own connection (their registered PostgreSQL role). Used for queries, seed loads, ETL, API, audits. Each developer should have their own. See "Setting up a new developer" below.
- **`$SUPERUSER_URL`** — the RDS master (`postgres`) user. Required only for DDL migrations (`ALTER TABLE`, `CREATE INDEX`, `GRANT`). Shared among admins; password is in AWS Secrets Manager.

See "First-time setup" below for how to set these up. See "When to use which" for the full decision table.

### Turning on the Cloud9 environment

Because most of the backend development is finished, the EC2 instance `aws-cloud9-coeqwal-db-admin-48dc921ad0fd48ea93c2a2e218bd8ace` is normally turned off to save costs. To make database changes:

1. Go to the AWS EC2 console and start the instance
2. Open Cloud9 from the AWS console
3. `cd ~/environment/coeqwal-backend && git pull origin main`
4. Run your scripts
5. Stop the instance when done

For bulk operations (e.g., loading batches of scenario statistics data), the ETL scripts support multi-threading. If loads are slow, consider temporarily upgrading the EC2 instance type — see [AWS EC2 instance types](https://aws.amazon.com/ec2/instance-types/) for options.

### Adding new scenarios

1. Prepare scenario metadata (short_code, run_name, name, descriptions, hydroclimate_id, baseline_scenario_id, sibling_group, etc.) — see the [ERD](schema/COEQWAL_SCENARIOS_DB_ERD.md) for the full `scenario` table schema
2. Write a migration SQL script in `database/scripts/sql/migrations/` that INSERTs the new rows into the `scenario` table and populates the link tables (`scenario_tag_link`, `theme_scenario_link`, `scenario_key_assumption_link`, `scenario_key_operation_link`)
3. Run the migration: `psql $SUPERUSER_URL -f database/scripts/sql/migrations/<script>.sql`
4. Verify: `psql $DATABASE_URL -c "SELECT short_code, hydroclimate_id, sibling_group FROM scenario ORDER BY short_code;"`
5. Run a fresh audit: `python database/audit/run_monthly_audit.py`

Reference: Migration 45 (`45_baseline_and_sibling_expansion.sql`) added 48 cc50/cc95 sibling scenarios using this pattern.

### Adding new scenario data (results/statistics)

1. Upload the scenario's CalSim3 DSS output to S3
2. Update the ETL path configuration to include the new scenario's S3 paths
3. Run the ETL pipeline (supports multi-threading for bulk loads)
4. Verify accuracy: `python etl/statistics/verify_all_sections.py --scenario <short_code>`
5. Verify API: `python etl/statistics/verify_api.py --scenario <short_code>`

### Adding new tiers (future)

Tier definitions live in the `tier_definition` table. Adding a new tier requires:

1. INSERT the tier definition row
2. Compute tier results for all scenarios and INSERT into `tier_result`
3. Compute location-level results and INSERT into `tier_location_result`
4. Update the frontend tier configuration to display the new tier

### Adding tier data for new scenarios

After new scenarios are loaded into the `scenario` table and their statistics ETL is complete:

1. Compute tier results for the new scenarios using the existing tier definitions
2. INSERT into `tier_result` and `tier_location_result`
3. Verify with the monthly audit's per-scenario ETL coverage check

### Redeploying the API

If you changed the API endpoint code (anything under `api/`), including query logic, response fields, or CORS settings, you need to redeploy. Push the changes to `main` on GitHub — the CI pipeline handles the rest. See the [API README](../../api/coeqwal-api/README.md) for details and manual deployment fallback.

---

## Getting started

### Prerequisites

- AWS account access (to reach the RDS instance via Cloud9 or VPN)
- Database credentials (ask a team member)

### First-time setup (5 steps)

**1. Set your connection strings** in `~/.bashrc` on Cloud9:

```bash
# Your personal developer connection. Used for everything day-to-day
export DATABASE_URL="postgresql://your_username:password@coeqwal-scenario-database-1.clai4yqcyzxh.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario"

# RDS master user — only needed for DDL migrations (ALTER TABLE, CREATE/DROP INDEX, GRANT)
# Retrieve the postgres password from AWS Secrets Manager (ask an admin or see below)
export SUPERUSER_URL="postgresql://postgres:PASSWORD@coeqwal-scenario-database-1.clai4yqcyzxh.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario"


source ~/.bashrc
```

Alternatively, the `setup_db_connection.sh` script will prompt you for both and test each connection:
```bash
bash database/setup_db_connection.sh
```

**When to use which:**

| Task | Variable |
|---|---|
| Queries, seed loads, inspect scripts, ETL, API | `$DATABASE_URL` |
| DDL migrations (`database/scripts/sql/migrations/`) | `$SUPERUSER_URL` |
| Running the audit (`python database/audit/run_monthly_audit.py`) | `$DATABASE_URL`

**Finding the postgres password:** If you forget the password, it is stored in AWS Secrets Manager. To find it:
```bash
aws secretsmanager list-secrets --query "SecretList[*].Name" --output table
aws secretsmanager get-secret-value --secret-id <secret-name> --query SecretString --output text
```

**2. Get registered** — ask an admin to run `register_developer()` for you (see "Setting up a new developer" below). You need a named PostgreSQL role and a row in the `developer` table before you can write anything.

**3. Verify you're connected as yourself:**

```bash
psql $DATABASE_URL -c "
SELECT session_user AS db_role, coeqwal_current_operator() AS developer_id,
       d.email, d.display_name
FROM developer d WHERE d.id = coeqwal_current_operator();"
```

Your username should appear, with `developer_id` matching your row in the `developer` table. If `developer_id = 1` you are connected as `postgres`  and all your writes will be attributed to the system account. Please contact an admin if there are issues or we need to run corrections (not a big deal). We are working to set up SSO auth but it's still on the TODO list.

**4. Read the ERD** before writing anything:

```
database/schema/COEQWAL_SCENARIOS_DB_ERD.md
```

**5. Run the monthly audit:**

```bash
python database/audit/run_monthly_audit.py
```

This single command produces a comprehensive report covering schema, content, verification, health, and cost — plus CSV exports for all reference tables. Output goes to `audits/monthly_YYYYMMDD_HHMMSS/`. See `database/audit/README.md` for details and options.

Before running, confirm you are connected as yourself:

```bash
psql $DATABASE_URL -c "SELECT session_user, coeqwal_current_operator() AS developer_id;"
```

`developer_id` should be your id (e.g. `2`), not `1`. If it returns `1` you are connected as the system user — check your `DATABASE_URL`.

To inspect a specific layer's full table contents:

```bash
psql $DATABASE_URL -f database/scripts/sql/00_versioning/09_verify_level00.sql
psql $DATABASE_URL -f database/scripts/sql/01_lookup/inspect_layer01.sql
```

### Key resources

| Resource | Location |
|----------|----------|
| **Monthly audit (primary tool)** | `database/audit/run_monthly_audit.py` |
| Audit tool docs | `database/audit/README.md` |
| Schema documentation (ERD) | `database/schema/COEQWAL_SCENARIOS_DB_ERD.md` |
| Quick schema snapshot | `database/run_audit.sh` |
| ERD vs DB comparison | `database/audit/verify_erd_against_audit.py` |
| Reference data export (layers 00–08) | `database/scripts/export_layer_tables.py` |
| Per-layer SQL verification | `database/scripts/sql/NN_layer/09_verify_levelNN.sql` |
| Seed data | `database/seed_tables/<layer>/` |
| Applied migrations | `database/scripts/sql/migrations/` |
| ETL accuracy verification | `etl/statistics/verify_all_sections.py` |
| API accuracy verification | `etl/statistics/verify_api.py` |

---

## Directory structure

```
database/
├── audit/                          # All audit tools and documentation
│   ├── run_monthly_audit.py        #   ★ primary tool — content, verification, health, cost
│   ├── generate_erd_from_audit.py  #   generate draft ERD from live audit snapshot
│   ├── verify_erd_against_audit.py #   diff ERD docs vs. live schema
│   └── README.md                   #   audit documentation and usage guide
├── schema/                         # ERD and schema documentation
│   └── COEQWAL_SCENARIOS_DB_ERD.md
├── seed_tables/                    # CSV seed data, one folder per schema layer
│   ├── 00_versioning/
│   ├── 01_lookup/
│   ├── 02_network/
│   ├── 03_entity/
│   ├── 04_variable/
│   ├── 05_assumptions_operations/
│   ├── 06_scenario/
│   ├── 07_hydroclimate/
│   ├── 08_theme/
│   └── 10_tier/
├── scripts/                        # Runnable scripts (Python + SQL)
│   ├── export_layer_tables.py      #   export layers 00–08 to CSV for content review
│   ├── fix_channel_entity_csv.py   #   one-off data fix utility
│   └── sql/
│       ├── 00_versioning/          #   DDL + audit trigger scripts
│       ├── 01_lookup/              #   lookup table verification
│       ├── 02_network/             #   network layer verification
│       ├── 11_reservoir_statistics/
│       ├── 12_mi_statistics/
│       ├── 13_ag_statistics/
│       ├── migrations/             #   applied one-time ALTER TABLE scripts
│       └── validate_data_integrity.sql
├── run_audit.sh                    # Quick schema-only snapshot to audits/ JSON+CSV
├── run_local_audit.py              # Python: collects schema snapshot (used by run_audit.sh)
├── setup_db_connection.sh          # Interactive connection string setup
└── utils/
    ├── versioning_utils.py
    └── sync_aws_sso_users.py
```

**Audit output** lives at the repo root (not inside `database/`):

```
audits/                             # gitignored — all audit outputs land here
├── monthly_YYYYMMDD_HHMMSS/        #   monthly audit output (from run_monthly_audit.py)
│   ├── report.md                   #     the Markdown report you read
│   ├── schema_snapshot.json        #     full schema snapshot
│   ├── tables_summary.csv          #     per-table row counts + audit field status
│   ├── layer_exports/              #     full CSV exports for layers 00-08
│   └── results_samples/            #     head/tail CSVs for layers 10+
├── audit_YYYYMMDD_HHMMSS.json      #   quick schema snapshot (from run_audit.sh)
├── tables_summary_YYYYMMDD_HHMMSS.csv  # quick per-table summary
├── latest.json                     #   symlink to most recent JSON snapshot
├── verification_reports/           #   ETL accuracy reports (Layer 2 + Layer 3)
│   ├── {scenario_id}_layer2.json
│   └── {scenario_id}_layer3.json
└── validation_mismatches/          #   DSS extraction validation manifests (Layer 1)
    ├── {scenario_id}_manifest.json
    └── {scenario_id}_validation_mismatches.csv

exports/                            # gitignored — layer table CSV exports
└── layer_tables/                   #   from export_layer_tables.py
    ├── 00_versioning/
    ├── 01_lookup/
    ...
    └── summary.csv
```

## Schema layers

The database follows a layered architecture separating **foundational data** (00-08) from **derived results** (10+).
Each layer depends on all layers with a lower number.

```
00  VERSIONING       version_family, version, developer, domain_family_map
01  LOOKUP           hydrologic_region, source, model_source, unit, spatial_scale,
                     temporal_scale, statistic_category, statistic_type, geometry_type,
                     network_type, network_subtype, watershed, wba
02  NETWORK          network, network_gis, network_arc, network_node
03  ENTITY           reservoir, compliance_station,
                     du_agriculture_entity, du_urban_entity, du_refuge_entity
04  VARIABLE         calsim_model_variable_type, derived_variable_type, variable_type
                     channel_variable, reservoir_variable, inflow_variable, derived_variable
05  ASSUMPTIONS      assumption_category, assumption_definition        ← land_use, gw_model only
    + OPERATIONS     operation_category, operation_definition          ← TUCP, SGMA, BiOps, flows,
                     scenario_key_assumption_link                         infrastructure, delta regs,
                     scenario_key_operation_link                          allocation priorities
06  SCENARIO         scenario, scenario_author, scenario_source_link
07  HYDROCLIMATE     hydroclimate, slr
08  THEME            theme, theme_scenario_link, theme_source_link
09  (reserved)
─────────────────────────────────────────────────────────────────────────
10+ RESULTS          tier_definition, tier_result, tier_location_result
                     reservoir_storage_monthly, reservoir_spill_monthly
                     du_delivery_monthly, du_shortage_monthly
                     ag/cws aggregate statistics
```

## Schema implementation status

| Layer | Key Tables | Seed directory | Status |
|-------|------------|----------------|--------|
| 00 VERSIONING | version_family, version, developer | seed_tables/00_versioning/ | Implemented |
| 01 LOOKUP | hydrologic_region, source, unit, network_type, watershed, wba | seed_tables/01_lookup/ | Implemented |
| 02 NETWORK | network, network_gis, network_arc, network_node | seed_tables/02_network/ | Implemented |
| 03 ENTITY | reservoir, compliance_station, du_* | seed_tables/03_entity/ | Implemented |
| 04 VARIABLE | calsim_model_variable_type, derived_variable_type, variable_type, *_variable | seed_tables/04_variable/ | Implemented |
| 05 ASSUMPTIONS + OPS | assumption_definition, operation_definition | seed_tables/05_assumptions_operations/ | Partial |
| 06 SCENARIO | scenario, scenario_author | seed_tables/06_scenario/ | Partial |
| 07 HYDROCLIMATE | hydroclimate, slr | seed_tables/07_hydroclimate/ | Partial |
| 08 THEME | theme, theme_scenario_link | seed_tables/08_theme/ | Implemented |
| 10+ RESULTS | tier_result, reservoir_storage_monthly, du_delivery_monthly | scripts/sql/1*_statistics/ | Implemented |

---

## Layer details

### 02_NETWORK: network topology

The network layer represents CalSim's water infrastructure as connected arcs and nodes.
Classification lookups (`network_entity_type`, `network_type`, `network_subtype`) are in Layer 01.

**Table hierarchy:**
```
network (master registry - 6908 records)
├── network_arc (arc-specific attributes - 2610 records)
└── network_node (node-specific attributes - 1544 records)

Classification tables (Layer 01 lookups):
├── network_entity_type (4 types)
├── network_type (21 types)
└── network_subtype (28 subtypes)
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

The full chain from a SQL write to recorded attribution:

```
INSERT/UPDATE on any table
  BEFORE trigger fires: set_audit_fields()
    calls: coeqwal_current_operator()
      reads: session_user  (PostgreSQL session variable — set at connection time)
                           (NOT current_user: this function is SECURITY DEFINER,
                            so current_user always returns the function owner)
      looks up: developer.id  (4 strategies, in order — see below)
      returns: INTEGER (the developer.id)
    writes:
      NEW.created_by / NEW.updated_by = developer.id
      NEW.created_at / NEW.updated_at = NOW()
```

Field behavior by event:

| Event | `created_at` | `created_by` | `updated_at` | `updated_by` |
|-------|-------------|-------------|-------------|-------------|
| INSERT | `NOW()` | `coeqwal_current_operator()` | `NOW()` | `coeqwal_current_operator()` |
| UPDATE | preserved (from original INSERT) | preserved | `NOW()` | `coeqwal_current_operator()` |

### Developer detection (it's strict)

`coeqwal_current_operator()` resolves `current_user` to a `developer.id` using these strategies in order:

| Priority | Field checked | Match condition |
|----------|--------------|-----------------|
| 0 (special) | — | If `current_user = 'postgres'`, return id=1 (`system@coeqwal.local`) |
| 1 | `aws_sso_username` | Exact match: `aws_sso_username = current_user` |
| 2 | `email` | Substring match: `email LIKE '%current_user%'` |
| 3 | `name` | Case-insensitive substring: `LOWER(name) LIKE '%current_user%'` |
| 4 | `display_name` | Case-insensitive substring: `LOWER(display_name) LIKE '%current_user%'` |
| fail | — | `RAISE EXCEPTION` — unregistered users cannot write to the database |

**Important:** Each developer must have their own database user registered in the `developer` table before making changes.

### Connecting as yourself (getting correct attribution)

The trigger reads `session_user` — the PostgreSQL role you authenticated as at connection time. **You cannot change `session_user` with a session variable.** If your `DATABASE_URL` uses `postgres` as the username, every write is attributed to developer id=1 (system account), regardless of who you are.

**Check who you are** — two ways to do this:

From the bash shell (`$` prompt):

```bash
$ psql $DATABASE_URL -c "SELECT session_user AS db_role, coeqwal_current_operator() AS developer_id;"
```

From inside a psql session (`coeqwal_scenario=>` prompt):

```sql
coeqwal_scenario=> SELECT
    session_user                    AS db_role,
    coeqwal_current_operator()      AS developer_id,
    d.email,
    d.display_name
FROM developer d
WHERE d.id = coeqwal_current_operator();
```

If `developer_id` is `1` you are connected as `postgres` and writes will be attributed to the system account. If the function raises an exception your username is not registered in the `developer` table.

**To get correct attribution, connect as your own registered database user:**

```bash
# Update the username in your connection string
export DATABASE_URL="postgresql://username:password@rds-endpoint:5432/coeqwal_scenario"
psql $DATABASE_URL
```

Strategy 2 (`email LIKE '%username%'`) will match `username@domain.ext`, so no `aws_sso_username` is needed as long as your email is registered.

**Correcting mis-attributed rows (trigger-disable required):**

The trigger preserves `created_by` on every UPDATE (`NEW.created_by := OLD.created_by`), so a normal `UPDATE ... SET created_by = 2` will be silently overwritten back to the old value. To correct attribution you must disable user-defined triggers as postgres.

Step 1 — open a new psql session connected as postgres (bash shell `$` prompt):

```bash
$ psql "postgresql://postgres:password@coeqwal-scenario-database-1.clai4yqcyzxh.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario"
```

Step 2 — once inside psql (`coeqwal_scenario=#` prompt), paste this whole block:

```sql
BEGIN;
ALTER TABLE some_table DISABLE TRIGGER USER;
UPDATE some_table SET created_by = 2, updated_by = 2 WHERE created_by = 1;
ALTER TABLE some_table ENABLE TRIGGER USER;
COMMIT;
```

> **Note:** Use `DISABLE TRIGGER USER`, not `DISABLE TRIGGER ALL`. On AWS RDS the postgres user cannot disable system FK triggers (`RI_ConstraintTrigger_*`), only user-defined ones. `USER` disables only the audit trigger, which is all you need.

**Verifying attribution after corrections:**

Run this to confirm all versioning tables are correctly attributed. The result shows each developer's share of rows per table with a percentage breakdown:

```sql
SELECT
    t.tbl,
    d.display_name      AS attributed_to,
    d.id                AS developer_id,
    COUNT(*)            AS rows,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY t.tbl), 1) AS pct
FROM (
    SELECT 'version_family'    AS tbl, created_by FROM version_family
    UNION ALL
    SELECT 'version',                  created_by FROM version
    UNION ALL
    SELECT 'domain_family_map',        created_by FROM domain_family_map
    UNION ALL
    SELECT 'developer',                created_by FROM developer
) t
LEFT JOIN developer d ON d.id = t.created_by
GROUP BY t.tbl, d.id, d.display_name
ORDER BY t.tbl, developer_id;
```

Expected result after corrections: all rows show your own name/id, except the `developer` table which correctly has one row for the system account (id=1) and one for you.

### Database access

The database is **not publicly accessible**. Access requires:

1. **Network access** - The RDS database is in a private AWS VPC
   - Cloud9 (within AWS) has direct access
   - Local access requires VPN or SSH tunnel through a bastion host

2. **Database credentials** - Host, port, username, password
   - Stored in environment variables or AWS Secrets Manager
   - Never committed to the repository

3. **AWS account access** - Required to use Cloud9 or retrieve credentials

#### Database quick start

Get connection details and store the connection string in shell profile:

```
export DATABASE_URL="postgresql://username:password@your-rds-endpoint:5432/coeqwal_scenario"
psql $DATABASE_URL
```

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

### Cloud9 cheatsheet

**Prompt key:**
- `$` at the end of your prompt to you are in the **bash shell** — use `psql`, `export`, `git`, etc.
- `coeqwal_scenario=>` to you are **inside psql** — only SQL and `\` meta-commands work here. Type `\q` to exit back to bash.

```bash
# Show all environment variables currently set in the session
printenv | sort

# Show only database and AWS connection variables
printenv | grep -E "DATABASE|PG|AWS|DB_"

# Show what is persisted across sessions (saved in shell profile files)
grep -n "export" ~/.bashrc ~/.bash_profile ~/.profile 2>/dev/null
```

`printenv | sort` is the quick sanity check — use it to confirm `DATABASE_URL` is set and see what username it contains.

`printenv | grep -E "DATABASE|PG|AWS|DB_"` narrows to connection-relevant variables only: `DATABASE_URL`, any `PG*` overrides (`PGUSER`, `PGPASSWORD`, `PGHOST`), and AWS credentials.

`grep -n "export" ~/.bashrc ...` shows what is **saved** and will reload on the next login. This is the file to edit when you want to change `DATABASE_URL` permanently. `printenv` only shows what is active right now — edits to profile files require `source ~/.bashrc` to take effect in the current session.

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

Scrips in `scripts/sql/00_versioning/` run in numbered order:

**`00_create_versioning_tables.sql`**
Creates the four foundational versioning tables: `developer`, `version_family`, `version`,
and `domain_family_map`. This must run before any other script in this folder — the audit
trigger functions and all domain tables have FK references to `developer`.
Handles the chicken-and-egg bootstrap: inserts the system user (id=1) before adding the
self-referencing FK constraints on `developer.created_by` and `updated_by`.

**`01_create_audit_trigger_function.sql`**
Defines `set_audit_fields()` — the BEFORE INSERT/UPDATE trigger function that auto-populates
`created_at/by` and `updated_at/by` on every write. This is the core of the audit system.
Also contains the note on why `session_user` must be used instead of `current_user` (SECURITY DEFINER).

**`02_create_audit_log_table.sql`**
Creates the `audit_log` table and its indexes. This table stores a full row-level change history
(old values, new values, changed fields, who, when, from where) as JSONB. The table exists in the
database but is **not active by default** — see `03_` below for how to enable it.

**`03_apply_audit_triggers.sql`**
Does two things:
1. Applies the `set_audit_fields()` trigger to every table that has audit columns — **this runs
   automatically and is always active.**
2. Defines `log_audit_changes()` and the helper `apply_audit_log_trigger_to_table(p_table_name)`,
   which write full change records into `audit_log`. This is **opt-in per table** — it is not
   applied by default because the write volume on bulk data tables would be large. Enable it on
   sensitive tables with:
   ```sql
   SELECT apply_audit_log_trigger_to_table('scenario');
   SELECT apply_audit_log_trigger_to_table('version');
   ```

**`04_create_developer_users.sql`**
Defines `register_developer()` and `list_developers()` — utility functions for creating a new
PostgreSQL role, granting it the right permissions, and registering it in the `developer` table
in one step. See the "Setting up a new developer" section above.

**`05_populate_domain_family_map.sql`**
Seeds the `domain_family_map` table, which maps every database table to a `version_family`. This
is what the versioning system uses to know which version governs each table's data. Contains the
full current set of 70 mappings. The seed CSV (`seed_tables/00_versioning/domain_family_map.csv`)
is out of date (34 rows) and should not be used for loading — see the seed README for how to
regenerate it from the live database.

**`06_load_seed_data.sql`**
Loads bootstrap data into `developer`, `version_family`, and `version`. Uses `ON CONFLICT DO
NOTHING` throughout, so it is safe to re-run on an existing database. `domain_family_map` is
intentionally skipped here — it is populated by `05_populate_domain_family_map.sql`.
Note: `developer` data is inline (not `\copy`) because the seed CSV is missing the `name` and
`aws_sso_username` columns.

**`09_verify_level00.sql`**
Verification queries for the versioning layer — checks that triggers are applied, audit fields
are populated, and domain_family_map entries are present. Run this after any schema changes to
the versioning layer to confirm everything is wired up correctly.

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

## Audit and verification strategy

The database has four distinct audit concerns. Each uses different tools and answers a different question. They are **not redundant** — run them together for full confidence.

| # | Concern | Question | Tools | When |
|---|---------|----------|-------|------|
| A | **Schema structure** | Is the DB shaped the way we documented it? | `run_audit.sh` + `verify_erd_against_audit.py` + `09_verify_level*.sql` | After any schema change |
| B | **Reference data content** | Do layers 00–08 contain the correct records? | `export_layer_tables.py` + manual diff vs `seed_tables/` | When adding or editing seed data |
| C | **ETL statistics accuracy** | Are the computed results (layers 10+) correct? | `verify_all_sections.py` (Layer 2) + `verify_api.py` (Layer 3) | After each ETL run |
| D | **Public verification status** | Is the verified/unverified status visible externally? | `/api/verification/status` + frontend `/verification` page | Ongoing |

---

### A. Schema structure audit

Answers: "Does the live database schema match the documented ERD? Are all triggers, indexes, and FK rules in place?"

**Step 1 — Capture a live schema snapshot** (run from repo root as postgres for full table visibility):

```bash
$ bash database/run_audit.sh
# Writes: audits/audit_YYYYMMDD_HHMMSS.json
#         audits/tables_summary_YYYYMMDD_HHMMSS.csv
#         audits/latest.json  (symlink)
```

**Step 2 — Compare snapshot against ERD documentation:**

```bash
$ python database/audit/verify_erd_against_audit.py \
    database/schema/COEQWAL_SCENARIOS_DB_ERD.md \
    audits/latest.json

# Add --verbose to see full column lists for mismatched tables
$ python database/audit/verify_erd_against_audit.py \
    database/schema/COEQWAL_SCENARIOS_DB_ERD.md \
    audits/latest.json --verbose
```

**Step 3 — Run per-layer structural verification** (checks triggers, FKs, naming conventions, row counts):

```bash
$ psql $DATABASE_URL -f database/scripts/sql/00_versioning/09_verify_level00.sql
$ psql $DATABASE_URL -f database/scripts/sql/01_lookup/09_verify_level01.sql
$ psql $DATABASE_URL -f database/scripts/sql/02_network/09_verify_level02.sql
```

Each `09_verify_level*.sql` script checks:

| # | Check |
|---|-------|
| 1 | Audit columns present (`created_at/by`, `updated_at/by`) |
| 2 | Audit triggers applied (`audit_fields_*`) |
| 3 | Version family mapping registered in `domain_family_map` |
| 4 | FK relationships to `developer` and lookup tables |
| 5 | Row counts match expected |
| 6 | Key columns exist and are correctly typed |
| 7 | No orphaned FK references |
| 8 | Naming conventions (snake_case, no plural table names) |
| 9 | Layer-specific checks (e.g. network connectivity, subtype hierarchy) |

**Layer audit modus operandi:**

1. Run verification script — identify issues
2. Write a migration script in `database/scripts/sql/migrations/` for each issue found
3. Execute migration, re-run verification
4. Delete the migration script after it has run (keep repo clean)
5. Update ERD documentation if the schema changed

**If the ERD is out of sync**, regenerate a draft from the live snapshot and merge manually:

```bash
$ python database/audit/generate_erd_from_audit.py \
    audits/latest.json \
    database/schema/GENERATED_ERD.md
```

See [audit/README.md](audit/README.md) for more detail on these tools.

---

### B. Reference data content audit (layers 00–08)

Answers: "Do the foundational tables contain the correct records — the right scenarios, entities, variables, assumptions, and themes?"

This is distinct from schema structure: the tables can have all the right columns and triggers while still containing incorrect, missing, or stale data. The seed CSVs in `database/seed_tables/` are the source of truth for layers 00–08.

**Export all layer 00–08 tables to CSV:**

```bash
# Export all layers (writes to exports/layer_tables/)
$ python database/scripts/export_layer_tables.py

# Export a single layer
$ python database/scripts/export_layer_tables.py --layer 06

# Custom output directory
$ python database/scripts/export_layer_tables.py --output-dir /tmp/review
```

Output structure:

```
exports/layer_tables/
├── 00_versioning/   developer.csv, version_family.csv, version.csv, domain_family_map.csv
├── 01_lookup/       hydrologic_region.csv, source.csv, unit.csv, ...
├── 02_network/      network.csv, network_arc.csv, network_node.csv, network_gis.csv, ...
├── 03_entity/       reservoir.csv, du_urban_entity.csv, mi_contractor.csv, ...
├── 04_variable/     calsim_model_variable_type.csv, channel_variable.csv, ...
├── 05_assumptions_operations/  assumption_definition.csv, operation_definition.csv, ...
├── 06_scenario/     scenario.csv, scenario_author.csv, scenario_source_link.csv, ...
├── 07_hydroclimate/ hydroclimate.csv, slr.csv
├── 08_theme/        theme.csv, theme_scenario_link.csv, theme_source_link.csv
└── summary.csv      row counts for all tables
```

**Compare exported CSVs against seed files:**

The exported CSVs and seed CSVs won't be identical column-for-column (the live DB has audit columns; seeds may not), but the domain columns should match. Spot-check key fields:

```bash
# Quick row-count comparison using the summary
cat exports/layer_tables/summary.csv

# Diff a specific table (ignore audit columns)
diff \
  <(cut -d, -f1-5 exports/layer_tables/06_scenario/scenario.csv) \
  <(cut -d, -f1-5 database/seed_tables/06_scenario/scenario.csv)
```

> **Note on `domain_family_map`:** The seed CSV at `database/seed_tables/00_versioning/domain_family_map.csv` is out of date (34 rows). The live database has 70+ entries populated by `05_populate_domain_family_map.sql`. Use the export as the current source of truth for this table.

**SQL data integrity checks** (run in psql after ETL runs):

```sql
-- Orphaned statistics (no matching scenario)
SELECT 'reservoir_period_summary' AS table_name, COUNT(*) AS orphans
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

-- Statistics coverage per scenario
SELECT s.id, s.short_code,
       (SELECT COUNT(*) FROM reservoir_period_summary  WHERE scenario_id = s.id) AS reservoir,
       (SELECT COUNT(*) FROM mi_contractor_period_summary WHERE scenario_id = s.id) AS mi,
       (SELECT COUNT(*) FROM ag_aggregate_period_summary  WHERE scenario_id = s.id) AS ag
FROM scenario s ORDER BY s.id;

-- NULL audit fields (should all be 0)
SELECT 'reservoir_entity missing created_by' AS check,
       COUNT(*) FILTER (WHERE created_by IS NULL) AS count
FROM reservoir_entity
UNION ALL
SELECT 'du_urban_entity missing created_by',
       COUNT(*) FILTER (WHERE created_by IS NULL)
FROM du_urban_entity;

-- Invalid water_month values (should be 0)
SELECT COUNT(*) AS invalid_water_months
FROM du_delivery_monthly
WHERE water_month NOT BETWEEN 1 AND 12;
```

---

### C. ETL statistics accuracy verification (layers 10+)

Answers: "Do the computed statistics in the results tables match the source DSS/CSV data?"

This is fully documented in `etl/README.md`. Summary:

```
DSS Files ──► S3 CSVs ──► PostgreSQL ──► JSON API ──► Frontend
  Layer 1      Layer 2      Layer 2b       Layer 3     Layer 4
```

```bash
# Layer 2: verify CSV to DB statistics accuracy (one scenario)
$ python etl/statistics/verify_all_sections.py --scenario s0020

# Layer 2: all scenarios, write JSON reports
$ python etl/statistics/verify_all_sections.py --all-scenarios \
    --report-dir audits/verification_reports

# Layer 3: verify DB to API accuracy
$ python etl/statistics/verify_api.py --scenario s0020
```

Reports land in `audits/verification_reports/{scenario_id}_layer2.json` and `_layer3.json`.

---

### D. Public verification status (Layer 4)

The API serves Layer 2 + Layer 3 report summaries at:

```
GET /api/verification/status              # all scenarios
GET /api/verification/status/{scenario}   # one scenario with per-section breakdown
```

Reports are read from `audits/verification_reports/`. Re-running `verify_all_sections.py` and `verify_api.py` refreshes the data visible on the frontend verification page.

---

### Audit schedule

| Cadence | What to run | Command |
|---------|------------|---------|
| **Monthly** | Full audit (schema, content, health, cost) | `python database/audit/run_monthly_audit.py` |
| **After any schema change** | Per-layer SQL structural checks | `psql $DATABASE_URL -f database/scripts/sql/NN_layer/09_verify_levelNN.sql` |
| **After any seed data edit** | Reference data content export | `python database/scripts/export_layer_tables.py --layer NN` |
| **After every ETL run** | ETL statistics accuracy (Layer 2) | `python etl/statistics/verify_all_sections.py --scenario {id}` |
| **After every ETL run** | API accuracy (Layer 3) | `python etl/statistics/verify_api.py --scenario {id}` |
| **Automated (Lambda)** | Schema snapshot to S3 | CloudWatch scheduled event to `coeqwal-database-audit` Lambda |

### Audit script inventory

All runnable scripts related to audit, verification, and data quality:

| Script | Location | Purpose | Output |
|--------|----------|---------|--------|
| `run_monthly_audit.py` | `database/audit/` | **Primary audit** — schema, content, ERD comparison, health, cost | `audits/monthly_YYYYMMDD_HHMMSS/` |
| `run_audit.sh` | `database/` | Quick schema-only snapshot | `audits/*.json`, `audits/*.csv` |
| `verify_erd_against_audit.py` | `database/audit/` | Diff ERD docs vs. live schema snapshot | stdout / exit code |
| `generate_erd_from_audit.py` | `database/audit/` | Generate draft ERD from live snapshot | `database/schema/GENERATED_ERD.md` |
| `export_layer_tables.py` | `database/scripts/` | Export layers 00–08 reference tables to CSV | `exports/layer_tables/` |
| `09_verify_level*.sql` | `database/scripts/sql/NN_layer/` | Per-layer structural invariants | psql output |
| `validate_data_integrity.sql` | `database/scripts/sql/` | FK orphan checks | psql output |
| `verify_all_sections.py` | `etl/statistics/` | ETL accuracy: CSV to DB (Layer 2) | `audits/verification_reports/*_layer2.json` |
| `verify_api.py` | `etl/statistics/` | API accuracy: DB to API (Layer 3) | `audits/verification_reports/*_layer3.json` |
| `db_audit_lambda.py` | `database/utils/db_audit_lambda/` | Schema snapshot (Lambda / scheduled) | `s3://coeqwal-model-run/database_audits/` |

### Audit output locations

| Output | Location | Gitignored? |
|--------|----------|-------------|
| **Monthly audit folder** | `audits/monthly_YYYYMMDD_HHMMSS/` | Yes |
| — Markdown report | `audits/monthly_.../report.md` | Yes |
| — Schema snapshot | `audits/monthly_.../schema_snapshot.json` | Yes |
| — Table summary | `audits/monthly_.../tables_summary.csv` | Yes |
| — Layer CSV exports | `audits/monthly_.../layer_exports/` | Yes |
| — Results samples | `audits/monthly_.../results_samples/` | Yes |
| Quick schema snapshot | `audits/audit_YYYYMMDD_HHMMSS.json` | Yes |
| Lambda snapshots (archived) | `s3://coeqwal-model-run/database_audits/` | S3 |
| Layer table exports (standalone) | `exports/layer_tables/` | Yes |
| ETL verification reports | `audits/verification_reports/{scenario}_layer2.json` | Yes |
| API verification reports | `audits/verification_reports/{scenario}_layer3.json` | Yes |

---

## Running the database audit

The primary audit tool is `run_monthly_audit.py`. It produces a comprehensive report covering schema structure, ERD comparison, data content, ETL coverage, database health, and cost — all in one run.

See [audit/README.md](audit/README.md) for full documentation of all audit tools.

### Running the monthly audit

From Cloud9, with `DATABASE_URL` set:

```bash
cd ~/environment/coeqwal-backend
source venv/bin/activate
python database/audit/run_monthly_audit.py
```

Running as your own user works for most tables. To get full visibility (including tables where your role lacks SELECT), use the superuser connection:

```bash
DATABASE_URL=$SUPERUSER_URL python database/audit/run_monthly_audit.py
```

### What it produces

A timestamped folder under `audits/`:

```
audits/monthly_YYYYMMDD_HHMMSS/
├── report.md                       Markdown report (the main thing to read)
├── schema_snapshot.json            Full schema snapshot
├── tables_summary.csv              Per-table row counts + audit field status
├── layer_exports/                  Full CSV exports for layers 00-08
│   ├── 00_versioning/
│   └── ...through 08_theme/
└── results_samples/                First 10 + last 10 rows for layers 10+
    ├── reservoir_storage_monthly_head.csv
    └── ...
```

### Report sections

| # | Section | What it checks |
|---|---------|----------------|
| 1a | Table inventory | Every table: name, layer, columns, rows, size |
| 1b | Schema vs. ERD | Tables/columns in DB but not ERD, and vice versa |
| 1c | Row counts vs. expected | Layers 00-08 counts against known targets |
| 1d | Reference data downloads | Full CSV export of layers 00-08 |
| 1e | Results data samples | Head/tail CSV samples of layers 10+ |
| 2a | Data integrity | NULL audit fields, orphaned rows, invalid values |
| 2b | ETL coverage | Per-scenario row counts across all results tables |
| 2c | ETL accuracy summary | Reads existing Layer 2/3 verification reports |
| 3a-d | Database health | Cache hit ratio, connections, dead tuples, bloat |
| 4a-c | Database cost | Table sizes, unused indexes, total storage |
| 5 | Audit summary | PASS/FAIL for each check, with details on failures |

### Skipping sections

```bash
python database/audit/run_monthly_audit.py --skip health
python database/audit/run_monthly_audit.py --skip health --skip cost
```

Valid sections: `content`, `verification`, `health`, `cost`.

### Other audit tools

These standalone tools can be run independently for quick one-off checks, but the monthly audit already calls them internally:

| Tool | Command | Use case |
|------|---------|----------|
| Schema-only snapshot | `bash database/run_audit.sh` | Quick schema check without health/cost/content |
| ERD comparison | `python database/audit/verify_erd_against_audit.py <erd.md> <snapshot.json>` | After editing ERD docs |
| Generate draft ERD | `python database/audit/generate_erd_from_audit.py <snapshot.json> <output.md>` | When ERD needs updating |
| Layer CSV export | `python database/scripts/export_layer_tables.py --layer NN` | Quick spot-check of seed data |

### Lambda S3 archive

An AWS Lambda (`coeqwal-database-audit`) runs on a CloudWatch schedule and archives schema snapshots to S3 independently of anyone being logged in. **Do not decommission it** — its S3 output provides a dated archive.

```bash
# Invoke manually
aws lambda invoke --function-name coeqwal-database-audit --region us-west-2 response.json

# Pull archived snapshots
aws s3 ls s3://coeqwal-model-run/database_audits/ --recursive | tail -5
aws s3 cp s3://coeqwal-model-run/database_audits/audit_YYYYMMDD_HHMMSS.json ./audits/
```

See [utils/db_audit_lambda/README.md](utils/db_audit_lambda/README.md) for Lambda setup details.

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

Step 1 — in the bash shell (`$` prompt), pull latest and connect:

```bash
$ cd ~/environment/coeqwal-backend
$ git pull origin main
$ psql $DATABASE_URL
```

Step 2 — once inside psql (`coeqwal_scenario=>` prompt), run scripts with `\i`:

```sql
coeqwal_scenario=> \i database/scripts/sql/00_create_helper_functions.sql
coeqwal_scenario=> \i database/scripts/sql/00_versioning/00_create_versioning_tables.sql
coeqwal_scenario=> \i database/scripts/sql/00_versioning/01_create_audit_trigger_function.sql
coeqwal_scenario=> \i database/scripts/sql/00_versioning/02_create_audit_log_table.sql
coeqwal_scenario=> \i database/scripts/sql/00_versioning/03_apply_audit_triggers.sql
coeqwal_scenario=> \i database/scripts/sql/00_versioning/04_create_developer_users.sql
coeqwal_scenario=> \i database/scripts/sql/00_versioning/06_load_seed_data.sql
coeqwal_scenario=> \i database/scripts/sql/00_versioning/05_populate_domain_family_map.sql
coeqwal_scenario=> \i database/scripts/sql/00_versioning/09_verify_level00.sql
```

Or from the bash shell, pass the script directly without entering psql:

```bash
$ psql $DATABASE_URL -f database/scripts/sql/00_create_helper_functions.sql
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
- Verify security group allows Lambda to RDS
- See [utils/db_audit_lambda/README.md](utils/db_audit_lambda/README.md)

---

## TODO

Known improvements and cleanup tasks for future work.

### Infrastructure

- **Lambda audit and `run_audit.sh` serve different purposes — keep both.** See "Running the database audit" below for details on when to use each.
- **Update `domain_family_map` seed CSV** — `database/seed_tables/00_versioning/domain_family_map.csv` is stale (34 rows vs. 70+ in the live DB). Use `export_layer_tables.py --layer 00` to export the current state and overwrite the seed file.

### Verification gaps

- **Extend per-layer SQL verification** — only layers 00, 01, and 02 have `09_verify_level*.sql` scripts. Add scripts for layers 03 through 08 following the same pattern.
- **`data_load_log` is still PLANNED** — see "ETL batch tracking" in the ERD Layer 00 section. This table would provide batch-level provenance for bulk ETL loads instead of per-row `created_by` attribution.

### Developer access and authentication

- **SSO user attribution** — currently developers connect to the database using a named PostgreSQL role (e.g. `jfantauzza`). The long-term goal is to use AWS SSO identity for authentication so that the `aws_sso_username` field in the `developer` table is used automatically, without requiring a separate PostgreSQL password per developer.
- **Role-based table permissions** — instead of granting permissions individually to each developer, create a `coeqwal_developer` role with `SELECT, INSERT, UPDATE, DELETE` on all tables (including future ones via `ALTER DEFAULT PRIVILEGES`), and have `register_developer()` grant that role. This prevents permission gaps like the `variable_type` issue encountered during auditing. See the "Connecting as yourself" section for background.

### Review indices and compare to API