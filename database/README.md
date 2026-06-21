# COEQWAL Database

PostgreSQL database for COEQWAL scenario data, network, tiers, and statistics topology.

## Table of contents

1. [Getting started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Connection strings](#connection-strings)
   - [First-time setup](#first-time-setup-6-steps)
   - [Turning on the Cloud9 environment](#turning-on-the-cloud9-environment-to-run-scripts-on-the-database)
   - [Cloud9 development workflow](#cloud9-development-workflow)
   - [Running SQL scripts in Cloud9](#running-sql-scripts-in-cloud9)
   - [Database access](#database-access)
   - [Setting up a new developer](#setting-up-a-new-developer)
   - [Cloud9 cheatsheet](#cloud9-cheatsheet)
   - [Key resources](#key-resources)
2. [Making changes to the database](#making-changes-to-the-database)
   - [Creating a new table](#creating-a-new-table)
   - [Adding new scenarios](#adding-new-scenarios)
   - [Adding new scenario data (statistics)](#adding-new-scenario-data-statistics)
   - [Adding tiers and tier outcome data](#adding-tiers-and-tier-outcome-data)
3. [Schema layers](#schema-layers)
4. [Schema implementation status](#schema-implementation-status)
5. [Layer details](#layer-details)
   - [02_NETWORK](#02_network-network-topology)
   - [03_ENTITY](#03_entity-entity-tables-and-the-entity-attribute-pattern)
   - [00_VERSIONING](#00_versioning-audit-trails-and-version-control)
6. [Audit and verification](#audit-and-verification)
   - [Standalone tools](#standalone-tools)
   - [Lambda S3 archive](#lambda-s3-archive)
7. [Running the database audit](#running-the-database-audit)
   - [Running the monthly audit](#running-the-monthly-audit)
   - [Report sections](#report-sections)
   - [After a run, what to read](#after-a-run-what-to-read)
   - [Skipping sections](#skipping-sections)
8. [Roadmap](#roadmap)
   - [Product-driven](#product-driven)
   - [Data pipeline / ETL](#data-pipeline--etl)
   - [Correctness bugs](#correctness-bugs-do-regardless)

## Getting started

### Prerequisites

- AWS account access (to reach the RDS instance via Cloud9 or VPN)
- Database credentials (ask a team member)

### Connection strings

Database operations use one of two connection strings stored in `~/.bashrc` on Cloud9:

- **`$DATABASE_URL`:** a developer's own connection (their personal PostgreSQL login role). Used for queries, seed loads, ETL, API, audits. Each developer has their own. See "Setting up a new developer" below.
- **`$SUPERUSER_URL`:** the RDS master (`postgres`) user. Required only for DDL migrations (`ALTER TABLE`, `CREATE INDEX`, `GRANT`). Shared among admins. Password is in AWS Secrets Manager.

The **dev** database (`coeqwal-scenario-database-dev`) has its own pair, **`$DEVDB_URL`** and **`$DEVSU_URL`** (the developer and superuser connections to the dev RDS, same roles as above). If you set up your identity before the dev database existed, repeat the first-login password step against `$DEVDB_URL`.

See "First-time setup" below for how to set these up.

**The two URLs at a glance**

| URL | Who they connect as | What it's for | Audit attribution? |
|---|---|---|---|
| `$DATABASE_URL` | Their own named role (e.g. `alice`) | Daily work: queries, seed loads, INSERT/UPDATE, ETL, API, running the monthly audit | Yes, attributed to you |
| `$SUPERUSER_URL` | Shared `postgres` account (everyone uses the same password) | DDL only: `CREATE TABLE`, `ALTER`, `GRANT`, migrations (`database/sql_archive/`) | No, all writes attributed to `id=1` (system) |

Practical rule: if you are writing DML, use `$DATABASE_URL` so your name lands in the audit log. If you are writing DDL, use `$SUPERUSER_URL`.

### First-time setup

**1. Get registered:** ask an admin to set up your personal database identity: a login role, a row in the `developer` table, `coeqwal_developer` group membership, and a per-user env file (`~/.coeqwal-env-<name>`, mode 600) seeded with a temporary password on the Cloud9 EC2. See [Setting up a new developer](#setting-up-a-new-developer) below. You need this before your writes are attributed to you by the audit trigger.

**2. Activate your personal connection:** Run `become<name>` at the start of each Cloud9 session to load your personal `DATABASE_URL` from your `~/.coeqwal-env-<name>` file, then `whoami_coeqwal` to confirm which identity is active. Use `$DATABASE_URL` for queries, ETL, and inserts (audited to you) and `$SUPERUSER_URL` for DDL and migrations (audited to system).

```bash
become<name>          # load your personal DATABASE_URL for this session
whoami_coeqwal        # confirm DATABASE_URL shows your username
```

On your first login, rotate the temporary password your admin set (see [Setting up a new developer](#setting-up-a-new-developer)).

**Finding the postgres password:** `SUPERUSER_URL` uses the shared `postgres` master password. If it is not already set in your shell, retrieve it from AWS Secrets Manager and export it:

```bash
aws secretsmanager list-secrets --query "SecretList[*].Name" --output table
aws secretsmanager get-secret-value --secret-id <secret-name> --query SecretString --output text
```

```bash
export SUPERUSER_URL="postgresql://postgres:PASSWORD@coeqwal-scenario-database-1.clai4yqcyzxh.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario"
```

Alternatively, on the Cloud9 instance `SUPERUSER_URL` is already exported, so you can read the password straight from the environment instead of going to Secrets Manager:

```bash
echo "$SUPERUSER_URL"
```

**3. Verify you're connected as yourself:**

```bash
psql $DATABASE_URL -c "
SELECT session_user AS db_role, coeqwal_current_operator() AS developer_id,
       d.email, d.display_name
FROM developer d WHERE d.id = coeqwal_current_operator();"
```

Your username should appear, with `developer_id` matching your row in the `developer` table. If `developer_id = 1` you are connected as `postgres`  and all your writes will be attributed to the system account. Please contact an admin if there are issues. Setting up SSO auth is still on the TODO list.

**4. (optional) Reference the ERD**:

```
database/schema/ERD.md
```

**5. Activate the Python virtual environment** before running any Python script. On Cloud9, the shared venv lives at the repo root:

```bash
cd ~/environment/coeqwal-backend
source venv/bin/activate
```

It already has the project dependencies installed (`psycopg2-binary`, `pandas`, `numpy`, `boto3`, ...). Re-run `source venv/bin/activate` in each new terminal session.

**6. (optional/test) Run the monthly audit:**

```bash
python database/audit/run_monthly_audit.py
```

This command produces a comprehensive report covering schema, content, verification, health, and cost, plus CSV exports for all reference tables. Output goes to `audits/monthly_YYYYMMDD_HHMMSS/`. See `database/audit/README.md` for details and options.

### Turning on the Cloud9 environment to run scripts

The EC2 instance `aws-cloud9-coeqwal-db-admin-48dc921ad0fd48ea93c2a2e218bd8ace` is normally turned off to save costs. If you want to use it to make database changes:

1. Go to the AWS EC2 console and start the instance
2. Open Cloud9 from the AWS console
3. `cd ~/environment/coeqwal-backend && git pull origin main`
4. Activate the Python virtual environment (needed for Python scripts like the audit): `source venv/bin/activate`
5. Run your scripts
6. Stop the instance when done

For bulk operations (e.g., loading batches of scenario statistics data), the ETL scripts support multi-threading. If loads are slow, consider temporarily upgrading the EC2 instance type. See [AWS EC2 instance types](https://aws.amazon.com/ec2/instance-types/) for options.

### Cloud9 development workflow

The Cloud9 workflow for running scripts:

```
┌──────────────┐     git push     ┌──────────────┐     git pull     ┌──────────────┐
│   Local Dev  │ ───────────────> │    GitHub    │ <─────────────── │   Cloud9     │
│  (your IDE)  │                  │  (main repo) │                  │   (AWS)      │
└──────────────┘                  └──────────────┘                  └──────┬───────┘
                                                                          │
                                                                          │ psql
                                                                          v
                                                                   ┌──────────────┐
                                                                   │   RDS        │
                                                                   │  (Postgres)  │
                                                                   └──────────────┘
```

1. **Local**: Edit SQL scripts in your editor
2. **GitHub**: Push changes to main branch
3. **Cloud9**: Pull latest from GitHub
4. **RDS**: Run SQL scripts via psql

### Running SQL scripts in Cloud9

Step 1: in the bash shell (`$` prompt), pull latest and connect:

```bash
$ cd ~/environment/coeqwal-backend
$ git pull origin main
$ psql $DATABASE_URL
```

Step 2: once inside psql (`coeqwal_scenario=>` prompt), run a script with `\i`. This read-only integrity check runs fine as your own role:

```sql
coeqwal_scenario=> \i database/scripts/sql/validate_data_integrity.sql
```

Or from the bash shell, pass the script directly without entering psql:

```bash
$ psql $DATABASE_URL -f database/scripts/sql/validate_data_integrity.sql
```

### Database access

The database is **not publicly accessible**. Access requires:

1. **Network access:** The RDS database is in a private AWS VPC
   - Cloud9 (within AWS) has direct access
   - Local access requires VPN or SSH tunnel through a bastion host

2. **Database credentials:** Host, port, username, password
   - Stored in environment variables or AWS Secrets Manager
   - Never committed to the repository

3. **AWS account access:** Required to use Cloud9 or retrieve credentials

**Security layers:**
- AWS VPC (network isolation)
- Security groups (firewall rules)
- Database authentication (username/password)
- Application-level checks (`coeqwal_current_operator()` for writes)

### Setting up a new developer

Each developer gets a **personal PostgreSQL identity** (a login role) on the RDS instance, so their writes are attributed to them in the audit log. There are two parts: an admin provisions the identity, then the developer claims it on first login.

**Admin: provision the identity:** Create the developer's login role, register them in the `developer` table, and add them to the `coeqwal_developer` group, which grants RW on `public` (including future tables) via the `ALTER DEFAULT PRIVILEGES` installed by [`57_install_coeqwal_developer_role.sql`](sql_archive/04_scenario/57_install_coeqwal_developer_role.sql). Set a temporary password and load it into the developer's `~/.coeqwal-env-<name>` file on the Cloud9 EC2. The `developer` row's `aws_sso_username` must match the login role name so `coeqwal_current_operator()` can resolve `session_user` to their `developer.id`. Despite the name, `aws_sso_username` currently just holds the PostgreSQL login role name. There is no SSO integration yet. The column name is forward-looking.

> The helper function `register_developer()` (in `04_create_developer_users.sql`) does the role-plus-row creation in one step. `list_developers()` is a read-only listing of the `developer` table. In practice the live `developer` rows were created by direct `INSERT`. Either path produces the same result.

> **The `role` column is informational:** It has no `CHECK` constraint and is not read by any authorization logic, RLS policy, or privilege check (`list_developers()` only returns it for display). Its documented values are `admin`, `user`, and `system`. Actual privileges come from the login role plus membership in `coeqwal_developer`.

**Developer: first login:** In a Cloud9 terminal, replacing `<name>` with your username and choosing a new password:

```bash
become<name>
psql $DATABASE_URL -c "ALTER USER <name> WITH PASSWORD 'YOUR_NEW_PASSWORD'"
sed -i "s|YOUR_TEMP_PASSWORD|YOUR_NEW_PASSWORD|" /home/ec2-user/.coeqwal-env-<name>
become<name> && whoami_coeqwal && psql $DATABASE_URL -c "SELECT session_user, coeqwal_current_operator();"
```

The last command should print your username and your `developer_id`. If it shows `developer_id = 1`, you are connected as `postgres` (System). Check your `DATABASE_URL`.

**Choosing a password:** Stick to characters bash does not interpret, or the commands above will fail or misbehave:

```
Allowed:  letters  digits  -  .  _  +  =
Avoid:    !  $  `  "  '  \  ;  &  |  *  <  >  and spaces
```

**Attribution mechanics:** Every write performed via `$DATABASE_URL` resolves through `coeqwal_current_operator()`, which matches `session_user` to a `developer.id` (by `aws_sso_username`, then email, then name, then display_name). Writes performed as `postgres` (the `$SUPERUSER_URL` path) attribute to `developer.id = 1` (System). That is the right call for DDL migrations but means DML run as `postgres` loses the operator. SSO-based auth is planned but not yet in place. See [`database/VERSIONING.md`](VERSIONING.md) for the full attribution model and its gaps.

### Cloud9 cheatsheet

**Prompt key:**
- `$` at the end of your prompt means you are in the **bash shell**. Use `psql`, `export`, `git`, etc.
- `coeqwal_scenario=>` means you are **inside psql**. Only SQL and `\` meta-commands work here. Type `\q` to exit back to bash.

```bash
# Show all environment variables currently set in the session
printenv | sort

# Show only database and AWS connection variables
printenv | grep -E "DATABASE|PG|AWS|DB_"

# Show what is persisted across sessions (saved in shell profile and env files)
grep -n "export" ~/.bashrc ~/.bash_profile ~/.profile ~/.coeqwal-env-* 2>/dev/null
```

`printenv | sort` is a quick check to confirm `DATABASE_URL` is set and see what username it contains.

`printenv | grep -E "DATABASE|PG|AWS|DB_"` narrows to connection-relevant variables only: `DATABASE_URL`, any `PG*` overrides (`PGUSER`, `PGPASSWORD`, `PGHOST`), and AWS credentials.

`grep -n "export" ~/.bashrc ...` shows what is saved and will reload on the next login. Your personal `DATABASE_URL` lives in `~/.coeqwal-env-<name>` (loaded by `become<name>`), and the shared `SUPERUSER_URL` lives in `~/.bashrc`. Edit those files to change a value permanently. `printenv` only shows what is active right now. Edits to env files require re-running `become<name>` (or `source`) to take effect in the current session.

**Python virtual environment:** Activate it before running any Python script (the monthly audit, ETL, utilities):

```bash
cd ~/environment/coeqwal-backend
source venv/bin/activate   # prompt gains a (venv) prefix
which python               # confirm it points into venv/
deactivate                 # leave the venv when done
```

Re-run `source venv/bin/activate` in each new terminal session.

### Key resources

| Resource | Location |
|----------|----------|
| **Database audit (primary tool)** | `database/audit/run_monthly_audit.py` |
| Audit tool docs | `database/audit/README.md` |
| Schema documentation (ERD) | `database/schema/ERD.md` |
| Seed data | `database/seed_tables/<layer>/` |
| Applied migrations | `database/sql_archive/` |
| ETL accuracy verification | `etl/statistics/verify_all_sections.py` |
| API accuracy verification | `etl/statistics/verify_api.py` |

---

## Making changes to the database

All database work runs from Cloud9 using the connection strings described in [Getting started](#getting-started). Quick rule: **DML** (INSERT/UPDATE of data) runs against `$DATABASE_URL` so your name lands in the audit log. **DDL** (CREATE/ALTER/GRANT) runs against `$SUPERUSER_URL`. See [Connection strings](#connection-strings).

### Creating a new table

Use [`database/CHECKLIST_TABLE_STANDARDS.md`](CHECKLIST_TABLE_STANDARDS.md) as the authoritative checklist. The essential steps, in order:

1. **Pick the layer and version family:** Decide where the table lives in the schema layer scheme (see [`schema/ERD.md`](schema/ERD.md) § Layer scheme) and which `version_family` it belongs to (see [`database/VERSIONING.md`](VERSIONING.md)). If no existing family fits, talk to the team before inventing a new one.
2. **Include the standard audit columns:** Every COEQWAL domain table carries `created_at TIMESTAMPTZ`, `created_by INTEGER REFERENCES developer(id)`, `updated_at TIMESTAMPTZ`, `updated_by INTEGER REFERENCES developer(id)`. Most tables also carry `is_active BOOLEAN DEFAULT TRUE` for soft deletes and `short_code TEXT UNIQUE NOT NULL` if the table is reference data with a stable human-readable key. Default-quality column types: `text` (not `VARCHAR(n)`), `timestamptz` (not `TIMESTAMP`), `integer` for FKs.
3. **Reference lookups by their key:** The key is sometimes the integer `id` (`hydrologic_region(id)`, `source(id)`, `unit(id)`), but newer tables FK on a `short_code` where that is the lookup's join target. The [CHECKLIST](CHECKLIST_TABLE_STANDARDS.md) has the full lookup list.
4. **Attach the audit trigger** with `SELECT apply_audit_trigger_to_table('your_table_name');`. This wires up `set_audit_fields` so `created_*` and `updated_*` auto-populate from the connecting developer. See [`database/VERSIONING.md`](VERSIONING.md) § "The authoring audit" for how attribution actually resolves.
5. **Register the table in `domain_family_map`** with the appropriate `version_family_id` and `database_level` (the two-digit layer code, e.g. `'03'`, populated on every existing row). Then add the table name to the matching layer bucket in the hard-coded `LAYERS` dict in [`database/audit/run_monthly_audit.py`](audit/run_monthly_audit.py). The audit still finds any new public table in the schema snapshot, but a table missing from `LAYERS` is labelled layer `other` in the report inventory and is skipped from the per-layer CSV exports.
6. **Document the table in [`schema/ERD.md`](schema/ERD.md):** Add the column table, FK list, indexes, unique constraints, and a short narrative paragraph.
7. **Verify with the audit:** Run `python database/audit/run_monthly_audit.py` and confirm the new table shows up in `tables_summary.csv` with `has_audit_trigger = True` and a sensible `created_by_values` after your first load.

The CHECKLIST also covers data-population patterns (FK subqueries vs literal IDs), the standard developer-attribution sanity check (`SELECT session_user, coeqwal_current_operator();`), and the audit query you can run against an existing table to confirm it follows the conventions.

### Adding new scenarios

Every scenario belongs to a **hydroclimate sibling group** (`scenario_hydroclimate_sibling`): one operational configuration run under each hydroclimate (historical, cc50, cc95, ...). Each `scenario` row carries `hydroclimate_sibling`, a varchar FK to `scenario_hydroclimate_sibling.short_code` (the founding scenario's code, e.g. `s0011`). The shared display `name`, descriptions, and `baseline_group` lineage live on the group row, not on the scenario. See the [ERD](schema/ERD.md) `scenario` and `scenario_hydroclimate_sibling` sections for the full schema.

1. Prepare scenario metadata: `short_code`, `run_name`, `is_active`, `hydroclimate_id`, `hydroclimate_sibling`, `scenario_version_id`, `scenario_author_id`, `model_source_id`. Insert new scenarios with `is_active = FALSE`. They stay hidden from the API until an operator flips the flag with [`etl/ingestion/tools/set_scenario_active.py`](../etl/ingestion/tools/set_scenario_active.py) after their ETL completes.
2. If the scenario starts a **new** operational configuration, first `INSERT` its `scenario_hydroclimate_sibling` row (PK `short_code` = the founding scenario's code, plus `name`, descriptions, and `baseline_group`). The `hydroclimate_sibling` FK is enforced, so the group row must exist before any scenario can point at it. If the scenario is just another hydroclimate variant of an existing config, reuse that group's `short_code`.
3. `INSERT` the scenario row(s) with `hydroclimate_sibling` set to the group code. This is DML, so run it against `$DATABASE_URL` with `psql -f <path>` (your name lands in the audit log). See [`database/scripts/sql/actions/3_add_additional_scenarios.sql`](scripts/sql/actions/3_add_additional_scenarios.sql) for the current pattern. The historical archive lives under [`database/sql_archive/04_scenario/`](sql_archive/04_scenario/) for naming precedents.
4. Verify: `psql $DATABASE_URL -c "SELECT short_code, hydroclimate_id, hydroclimate_sibling FROM scenario ORDER BY short_code;"`
5. Run a fresh audit: `python database/audit/run_monthly_audit.py`

### Adding new scenario data (statistics)

We load scenario's results (statistics) via the ETL. The full pipeline (pull from Drive, stage to S3, AWS Batch extraction, statistics, verification, activation) lives in [`etl/README.md`](../etl/README.md). After it completes, the new rows land in the Layer 10-12 result tables. Confirm per-scenario coverage with the monthly audit (`python database/audit/run_monthly_audit.py`).

### Adding tiers and tier outcome data

Tier definitions live in the `tier_definition` table, seeded from [`database/seed_tables/10_tier/tier_definition.csv`](seed_tables/10_tier/tier_definition.csv). The per-scenario `tier_result` and per-location `tier_location_result` rows are loaded by the tier ETL pipeline. The full workflow for adding a new tier or backfilling tier data for new scenarios lives in [`etl/tier_data/README.md`](../etl/tier_data/README.md). Confirm coverage with the monthly audit's per-scenario tier check.

> **Tier location membership is owned by the tier-team staging CSVs:** `tier_location` is a narrow database catalog (`tier_short_code`, `location_type`, `location_id`, `display_order`, `is_active`). The tier teams' staging CSVs in `etl/tier_data/staging/` are the source of truth for membership. There is no seed CSV. To reconcile, run [`etl/tier_data/scripts/diff_tier_locations.py`](../etl/tier_data/scripts/diff_tier_locations.py) for the diff and [`etl/tier_data/scripts/sync_tier_locations_from_staging.py`](../etl/tier_data/scripts/sync_tier_locations_from_staging.py) to apply (inserts active rows, soft-deletes rows that left staging). Display names are resolved at query time by joining `location_id` to the entity tables in the registry at [`etl/common/tier_location_entities.py`](../etl/common/tier_location_entities.py). The public API uses the same join map in [`/api/tiers/scenarios/{scenario_short_code}/locations`](../api/coeqwal-api/routes/tier_endpoints.py) for per-location tier assignments. Geometry is not served from the API. The frontend joins `location_id` to Mapbox vector tile features created vis MTS (Mapbox Tile Service) from the COEQWAL Geopackage for map rendering (see "API conventions, geometry" in the TODO section).

---

## Schema layers

The database follows a layered architecture separating **foundational data** (00-09) from **derived results** (10+).
Each layer depends on all layers with a lower number.

The full table inventory per layer:

- **00 VERSIONING:** `version_family`, `version`, `developer`, `domain_family_map`
- **01 LOOKUP:** `hydrologic_region`, `source`, `model_source`, `unit`, `spatial_scale`, `temporal_scale`, `statistic_category`, `statistic_type`, `geometry_type`, `network_type`, `network_subtype`, `network_entity_type`, `watershed`, `env_flow_season`
- **02 NETWORK:** `network`, `network_arc`, `network_node`, `network_gis`
- **03 ENTITY:** `reservoir`, `reservoir_entity`, `reservoir_group`, `reservoir_group_member`, `channel_entity`, `du_agriculture_entity`, `du_refuge_entity`, `du_urban_entity`, `du_urban_delivery_arc`, `du_urban_group`, `du_urban_group_member`, `mi_contractor`, `mi_contractor_delivery_arc`, `mi_contractor_group`, `mi_contractor_group_member`, `ag_aggregate_entity`, `cws_aggregate_entity`, `compliance_station`, `wba`
- **04 VARIABLE:** `calsim_model_variable_type`, `derived_variable_type`, `variable_type`, `channel_variable`, `du_urban_variable` (planned: `variable`, `delta_variable`, `reservoir_variable`, `inflow_variable`)
- **05 ASSUMPTIONS + OPS:** `assumption_category`, `assumption_definition` (land_use, gw_model), `operation_category`, `operation_definition` (TUCP, SGMA, BiOps, flows, infrastructure, delta regs, allocation priorities)
- **06 SCENARIO:** `scenario`, `scenario_hydroclimate_sibling`, `scenario_author`, `scenario_key_assumption_link`, `scenario_key_operation_link`, `scenario_tag`, `scenario_tag_link`
- **07 HYDROCLIMATE:** `hydroclimate`, `slr`
- **08 THEME:** `theme`, `theme_scenario_link`
- **09 TIER:** `tier_definition`, `tier_location`
- **10 TIER RESULTS:** `tier_result`, `tier_location_result`
- **11 PER-SCENARIO STATISTICS:** `reservoir_storage_monthly`, `reservoir_spill_monthly`, `reservoir_monthly_percentile`, `reservoir_period_summary`, `delta_monthly`, `delta_period_summary`, `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary`, `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary`, `cws_aggregate_monthly`, `cws_aggregate_period_summary`, `ag_du_demand_monthly`, `ag_du_sw_delivery_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary`, `ag_aggregate_monthly`, `ag_aggregate_period_summary`, `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary`, `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary`
- **12 CROSS-SCENARIO ANALYSIS:** `sensitivity_climate`, `sensitivity_operational`

`wba` lives in Layer 03 because Water Budget Areas are entities with their own groundwater variables and data, not a pure lookup. See the WBA gaps in [`database/SCHEMA_BACKLOG.md`](SCHEMA_BACKLOG.md) § 7 for the issues that follow from this placement.

## Schema implementation status

| Layer | Key Tables | Primary seed / source location | Status |
|-------|------------|----------------|--------|
| 00 VERSIONING | version_family, version, developer | seed_tables/00_versioning/ | Implemented |
| 01 LOOKUP | hydrologic_region, source, unit, network_type, watershed, env_flow_season | seed_tables/01_lookup/ | Implemented |
| 02 NETWORK | network, network_gis, network_arc, network_node | seed_tables/02_network/ | Implemented |
| 03 ENTITY | reservoir, channel_entity, compliance_station, du_*, mi_contractor, ag_aggregate_entity, cws_aggregate_entity, wba | seed_tables/03_GIS/ + seed_tables/04_calsim_data/. The `*_aggregate_entity` tables are SQL-seeded under sql_archive/03_entity_layers/ (no seed CSV) | Implemented (see WBA roadmap) |
| 04 VARIABLE | calsim_model_variable_type, derived_variable_type, variable_type, channel_variable, du_urban_variable | seed_tables/04_variable/ | Partial (see Roadmap) |
| 05 ASSUMPTIONS + OPS | assumption_definition, operation_definition | seed_tables/05_assumptions_operations/ | Partial |
| 06 SCENARIO | scenario, scenario_author | seed_tables/06_scenario/ | Partial |
| 07 HYDROCLIMATE | hydroclimate, slr | seed_tables/07_hydroclimate/ | Partial |
| 08 THEME | theme, theme_scenario_link | seed_tables/08_theme/ | Implemented |
| 09 TIER | tier_definition, tier_location | seed_tables/10_tier/ (legacy directory numbering, holds `tier_definition` only, `tier_location` is loaded via `etl/tier_data/`) | Implemented |
| 10+ RESULTS | tier_result, reservoir_storage_monthly, du_delivery_monthly | loaded by ETL (`etl/statistics/`, tiers via `etl/tier_data/`) | Implemented |

> **"Implemented" means the tables exist and are populated, not that every layer is free of known issues:** A backlog of schema-hygiene corrections (missing audit triggers and FKs, drifted ID sequences, duplicate indexes, stale artifacts) is batched in [`database/scripts/sql/audit_cleanup.sql`](scripts/sql/audit_cleanup.sql). See [`database/SCHEMA_BACKLOG.md`](SCHEMA_BACKLOG.md) for the full list.

---

## Layer details

Deep dives on the layers with the most involved structure. Other layers are covered by the [Schema layers](#schema-layers) inventory and the [ERD](schema/ERD.md).

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

### 03_ENTITY: entity tables and the entity-attribute pattern

The entity layer holds the version-controlled list of every "thing" the model and the website talk about: every reservoir, every channel, every demand unit, every M&I contractor, every community water system, every water budget area. Statistics tables in layer 10+ all reference rows in this layer by `<entity>_id`.

#### The entity-attribute pattern

Every domain in the database follows the same five-piece shape. Use this pattern when adding a new domain (e.g. `community_water_system`, future `inflow_entity`, etc.) so the audit, ETL, and API layers all behave consistently.

```
Layer 01 - lookups        hydrologic_region, source, model_source, unit, statistic_type, ...
                              ^
                              │ FK
Layer 03 - entity         <domain>_entity                 (the "thing": one row per real-world object)
                          <domain>_group                   (optional; analytical groupings)
                          <domain>_group_member            (M:N membership)
                          <domain>_delivery_arc            (optional; multi-arc CalSim sums)
                          <related>_<domain>_link          (M:N to other entities, e.g. CWS<->DU)
                              ^                                  ^
                              │ FK (<entity>_id)                 │ FK (<entity>_id) + scenario_short_code
                              │                                  │
Layer 04 - variables      <domain>_variable          Layer 11 - statistics   <domain>_<period>
                          (CalSim variable names              (e.g. *_monthly, *_period_summary)
                           per entity; consumed by
                           the ETL, NOT referenced
                           by the statistics tables)
```
**Common columns on entity tables** (per `database/CHECKLIST_TABLE_STANDARDS.md`):
- `id SERIAL PRIMARY KEY`
- `short_code TEXT UNIQUE NOT NULL`: stable, machine-readable code used by ETL and API
- Domain attributes (FK IDs to lookup tables)
- `is_active BOOLEAN NOT NULL DEFAULT TRUE`: soft delete
- `created_at`, `created_by`, `updated_at`, `updated_by`: populated automatically by the `set_audit_fields()` trigger
- A row in `domain_family_map` so the versioning system knows which `version_family` governs the table

**The `aggregate` adjective:** Several entity tables carry the suffix `_aggregate_entity` (`ag_aggregate_entity`, `cws_aggregate_entity`). It denotes a CalSim **project-level rollup**, i.e. one row per pre-computed CalSim variable that already sums many demand units (`DEL_SWP_PMI`, `DEL_CVP_PAG_N`, `DEL_SWP_MWD`, etc.). It does NOT mean "an aggregation of community water systems". It means "this table holds the entities CalSim itself reports as aggregates rather than per-DU."

**Concept guide:** how urban DUs, M&I contractors, CWS aggregates, and drinking-water utility names relate in the live schema is documented in [`water_user_categories.md`](topic_docs/cws/water_user_categories.md) (verified against monthly audit CSVs).

#### Implemented entity tables

Counts are from the most recent monthly audit (run `python database/audit/run_monthly_audit.py` to refresh).

| Domain | Entity table | Records | Variable / link / group tables | Statistics tables (Layer 11) |
|---|---|---:|---|---|
| Reservoirs | `reservoir_entity` | 92 | `reservoir_group`, `reservoir_group_member`, `reservoir_variable` (planned) | `reservoir_storage_monthly`, `reservoir_spill_monthly`, `reservoir_monthly_percentile`, `reservoir_period_summary` |
| Channels | `channel_entity` | 669 | `channel_variable` | `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary` |
| Compliance stations | `compliance_station` | 2 | - | (Delta tables, indirectly) |
| Water budget areas | `wba` | 42 | (referenced by DU tables via `wba_id`) | - |
| Agricultural DUs | `du_agriculture_entity` | 144 | - | `ag_du_demand_monthly`, `ag_du_sw_delivery_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary` |
| Refuge DUs | `du_refuge_entity` | 18 | - | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` |
| CWS DUs (urban) | `du_urban_entity` | 145 | `du_urban_variable`, `du_urban_delivery_arc`, `du_urban_group`, `du_urban_group_member` | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |
| CWS contractors (M&I) | `mi_contractor` | 30 | `mi_contractor_delivery_arc`, `mi_contractor_group`, `mi_contractor_group_member` | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |
| Agricultural project aggregates | `ag_aggregate_entity` | 9 | (delivery variable on entity row) | `ag_aggregate_monthly`, `ag_aggregate_period_summary` |
| CWS project aggregates | `cws_aggregate_entity` | 6 | (delivery + shortage variables on entity row) | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |
| Reservoir (legacy) | `reservoir` | 7 | - | (predates `reservoir_entity`. Kept for FK compatibility) |

**Project-level aggregates currently in the DB:**

NOD/SOD and SWP/CVP are not yet first-class per-DU attributes. NOD/SOD lives in three places, none of them a clean per-DU classification:

- As free-text `region` values on the two aggregate tables: `cws_aggregate_entity.region` (`total` / `nod` / `sod` / empty) and `ag_aggregate_entity.region` (`TOTAL` / `NOD` / `SOD`). These are rollups, not per-DU. (Note the case mismatch between the two tables 😜.)
- Empty placeholder rows in `du_urban_group` (`nod`, `sod`) that are defined but have no members (see the group breakdown below).
- Implicitly, via `hydrologic_region` on the DU entity tables.

**NOD/SOD and hydrologic-region convention.** Where NOD/SOD is derived from `hydrologic_region`, the rule is **NOD = `SAC`** and **SOD = `SJR` + `TULARE`**. That covers only three of the seven regions in the `hydrologic_region` lookup. The remaining four (`DELTA`, `SOCAL`, `NC`, `EXPORT`) have no defined NOD/SOD assignment. Ask the Water Allocation Modeling Team what to do about those.

`cws_aggregate_entity` (6 rows), CWS / M&I rollups:

| short_code | label | project | region | delivery_var | shortage_var |
|---|---|---|---|---|---|
| `swp_total` | SWP Total M&I | SWP | total | `DEL_SWP_PMI` | `SHORT_SWP_PMI` |
| `swp_nod` | SWP North | SWP | nod | `DEL_SWP_PMI_N` | `SHORT_SWP_PMI_N` |
| `swp_sod` | SWP South | SWP | sod | `DEL_SWP_PMI_S` | `SHORT_SWP_PMI_S` |
| `cvp_nod` | CVP North | CVP | nod | `DEL_CVP_PMI_N` | `SHORT_CVP_PMI_N` |
| `cvp_sod` | CVP South | CVP | sod | `DEL_CVP_PMI_S` | `SHORT_CVP_PMI_S` |
| `mwd` | Metropolitan WD | MWD | - | `DEL_SWP_MWD` | `SHORT_SWP_MWD` |

`ag_aggregate_entity` (9 rows), agricultural rollups:

| short_code | label | project | region | delivery_var |
|---|---|---|---|---|
| `swp_pag` | SWP Project AG | SWP | TOTAL | `DEL_SWP_PAG` |
| `swp_pag_n` | SWP Project AG North | SWP | NOD | `DEL_SWP_PAG_N` |
| `swp_pag_s` | SWP Project AG South | SWP | SOD | `DEL_SWP_PAG_S` |
| `cvp_pag_n` | CVP Project AG North | CVP | NOD | `DEL_CVP_PAG_N` |
| `cvp_pag_s` | CVP Project AG South | CVP | SOD | `DEL_CVP_PAG_S` |
| `cvp_psc_n` | CVP Settlement Contractors NOD | CVP | NOD | `DEL_CVP_PSC_N` |
| `cvp_pex_s` | CVP Exchange Contractors SOD | CVP | SOD | `DEL_CVP_PEX_S` |
| `nod_ag` | Total NOD AG | - | NOD | `COMPUTED` (sum) |
| `sod_ag` | Total SOD AG | - | SOD | `COMPUTED` (sum) |

`mi_contractor_group` (6 rows), contractor-level groupings: `swp`, `cvp_nod`, `cvp_sod`, `all_mi`, `swp_mi`, `swp_ag`.

`du_urban_group` (11 rows), per-DU groupings: `tier` (71 members, the existing focal set), `nod` (0), `sod` (0), `swp_served` (0), `cvp_served` (0), `swp_delivery_point` (0), `var_wba` (40), `var_gw_only` (3), `var_swp_contractor` (11), `var_named_locality` (15), `var_missing` (2). The 5 zero-member groups need to be backfilled.

> **No other aggregate-style tables exist:** Reservoir / channel / refuge / wba domains do not have `*_aggregate_entity` tables. Their roll-ups are computed at query / API time when needed. Talk to the Water Allocation Modeling Team.

#### Planned tables (not yet in DB)

These are documented or partially designed but not implemented in the database. The **In ERD?** column reflects the current `database/schema/ERD.md`.

| Domain | Planned table | In ERD? | Notes |
|---|---|---|---|
| Inflows | `inflow_entity` | Yes | Watershed inflow nodes. See ERD for column list. Variable side already partly designed (`inflow_variable.csv` seed exists). |
| Reservoirs | `reservoir_variable` | Yes | Variable mapping for reservoirs (storage, spill, levels). Currently the ETL uses hardcoded `S_*`, `C_*_FLOOD`, `S_*LEVELxDV` patterns. Promoting these to a table would match the channel / DU pattern. A `reservoir_variable.csv` seed exists. |
| Watershed | `river_watershed` | No | Watershed<->river crosswalk. |
| Hydroclimate | `hydroclimate_source` | Yes | Source attribution for hydroclimate scenarios. |
| Themes | `theme_source_link` | No | Source attribution for themes (parallel to `scenario_source_link`). A `theme_source_link.csv` seed exists. |
| Outcomes | `outcome_category`, `outcome_measure` | Partial (`outcome_category` only) | Outcome-framework tables. Partial seed in `database/seed_tables/03_outcome_framework/` (`outcome_category.csv`, `outcome_measure_samples.csv`). |

**Forward-looking design note:** The tables and columns below are what the team plans to add to support the Community Water Systems dataset. **None of it exists in the database yet:** The source spreadsheets are staged in [`data/reference/cws/`](../data/reference/cws/) (the spring-2026 delivery: `Master list of systems served for sw units updated april 13.xlsx`, `Updated HHS allocations May 6 2026.xlsx`, `Updated Master crosswalk SW DUs M&I May7 2026.xlsx`, plus `Final_M&Idemandunits_withlatlongs.xlsx`). See [`data/reference/cws/README.md`](../data/reference/cws/README.md). No seed CSVs have been converted into `seed_tables/` yet. Check with the CWS team to make sure this is the latest before proceeding.

**"CWS" vs "DU":**

- **CWS = Community Water System:** a drinking-water utility, identified by a **PWSID** (Public Water System ID). Think "City of Anderson water department."
- **DU = Demand Unit:** CalSim's modeling abstraction. The model does not know about individual utilities. It lumps water demand into ~145 urban demand units (`du_urban_entity`).

Today the database only has the CalSim DUs (classified as "urban" demand units). The four planned tables plus the new `du_urban_entity` columns add the real-world water-system layer and relate it to the model's DUs:

| Planned table | Layer | Purpose |
|---|---|---|
| `cws_entity` | 03_entity | One row per California Public Water System (PWSID): `pwsid`, `system_name`, `pop_served`, `system_lat`, `system_lon`, `hydrologic_region_id`, `source_id`. ~476 systems from the 2026-04-13 master list. |
| `cws_du_link` | 03_entity | M:N junction `cws_entity` <-> `du_urban_entity` (a system may serve multiple DUs and a DU may be served by multiple systems). One row per system-DU pair (~586 rows). |
| `cws_list` | 03_entity | List/registry catalog (e.g. `coeqwal_master_du`, `coeqwal_focal_sw_du`, `calsim_urban_du`, `tier_matrix`, `hhs_allocation`). One row per named list. A grouping/registry entity (referenced only by `cws_list_du_member`), so it sits in Layer 03 next to the DUs it groups, matching `du_urban_group`, not a broadly shared Layer 01 lookup. |
| `cws_list_du_member` | 03_entity | M:N junction `cws_list` <-> `du_urban_entity`: which list(s) each DU belongs to. Carries optional per-membership values (e.g. an `allocation_taf` column for the `hhs_allocation` list). See the open questions below. |

Plus an updated delivery-variable crosswalk merged into `du_urban_variable`.

The CWS delivery also carries per-DU attributes from the master / crosswalk spreadsheets. In this plan they are deliberately **not** added as new wide columns on `du_urban_entity`:

- **SW / GW flags:** reuse the existing `du_urban_entity.gw` / `.sw` columns (pending their `VARCHAR` -> `BOOLEAN` migration). Do not add `is_sw_du` / `is_gw_du`, which would duplicate them.
- **HHS allocation (TAF):** store on the `cws_list_du_member` row for the `hhs_allocation` list (an `allocation_taf` column), not on the entity. Additional allocation programs then become new lists rather than new entity columns.
- **Centroids:** geographic, so they belong in the GIS layer.

**Open questions to resolve before staging:**

- **List-registry consolidation:** `cws_list` / `cws_list_du_member` and the existing `du_urban_group` / `du_urban_group_member` are structurally identical (a named set of DUs plus M:N membership). Pick one mechanism rather than shipping both. Either generalize `du_urban_group` into a single DU-list registry and fold the CWS lists into it, or keep `du_urban_group` for the ETL `var_*` classification groups and use `cws_list` only for project lists with clearly disjoint purposes. Either way requires a migration and an ETL update, and the 5 empty `du_urban_group` rows (`nod`, `sod`, `swp_served`, `cvp_served`, `swp_delivery_point`) should be resolved in the same pass.
- **`gw` / `sw` boolean migration:** (roadmap R1 in `data/reference/cws/README.md`) must land before those columns can serve as the SW/GW flags.
- **GIS home for centroids:** Decide which GIS table holds `largest_system_centroid_*`, and confirm `calsim_centroid_*` is computed from `geom` rather than stored.
- **Data reconciliation:** 7 master DUs missing from CalSim (`ACFC`, `KCWA`, `MHILL_NU`, `SBCWD`, `SVWRD`, `TLMNE`, `UNION`) and the `ESB355` entry in the HHS list must be reconciled before load.
- **`cws_du_link`:** Confirm the system-to-DU mapping (~586 pairs) against `Systems_served_by_DU_systemname_updated.xlsx` / the master list.

**Related downstream work:** merging in the M&I delivery-variable crosswalk means changing the `du_urban_variable` list, or creating a new list for the statistic etl and re-running `etl/statistics/du_urban/run_all.py` for each active scenario. You can run it only for the `du_urban` ETL module or other module that you create.

### 00_VERSIONING: audit trails and version control

The versioning layer provides audit trails and version control for all other layers.

```
┌─────────────────────────────────┐       ┌─────────────────────────────────┐
│        developer                │       │       version_family            │
├─────────────────────────────────┤       ├─────────────────────────────────┤
│ id (PK)                         │       │ id (PK)                         │
│ email (UNIQUE)                  │       │ short_code (UNIQUE, NOT NULL)   │
│ display_name (NOT NULL)         │       │ label                           │
│ role                            │       │ description                     │
│ aws_sso_username (UNIQUE)       │<──────│ created_by (FK)                 │
│ is_bootstrap                    │       │ updated_by (FK)                 │
│ sync_source                     │       │ is_active                       │
│ is_active                       │       │ created_at, updated_at          │
│ created_at, updated_at          │       └─────────────────────────────────┘
└─────────────────────────────────┘                      │
         ^                                               │ 1:N
         │                                               v
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
                                                         v
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
- `coeqwal_current_operator()`: Returns developer.id for audit fields (SSO-aware)
- `get_active_version(family)`: Returns active version.id for a family
- `set_audit_fields()`: Trigger function for automatic audit field population

**Expected records:**
- `developer`: 2+ (system + admin bootstrap users + SSO users)
- `version_family`: 13 (one per domain, including 'statistics' for id=7)
- `version`: 13 (one active version per family)
- `domain_family_map`: 11+ (maps tables to version families)

**Audit field behavior:** The `set_audit_fields()` trigger fires `BEFORE INSERT OR UPDATE` on every table with audit columns and fills the four audit fields from `coeqwal_current_operator()`. See [`VERSIONING.md`](VERSIONING.md) for the full operator-resolution chain.

| Event | `created_at` | `created_by` | `updated_at` | `updated_by` |
|-------|-------------|-------------|-------------|-------------|
| INSERT | `NOW()` | `coeqwal_current_operator()` | `NOW()` | `coeqwal_current_operator()` |
| UPDATE | preserved (from original INSERT) | preserved | `NOW()` | `coeqwal_current_operator()` |

---

## Audit and verification

The database has three audit concerns. **`run_monthly_audit.py` is the only tool you need to run by hand:** One command rolls up all three concerns, adds database health and cost checks, and writes everything to a single `audits/monthly_<ts>/report.md`.

| # | Concern | Question | Covered by `run_monthly_audit.py` | When |
|---|---------|----------|-----------------------------------|------|
| A | **Schema structure** | Is the DB shaped the way we documented it? | §1 | After any schema change |
| B | **Reference data content** | Do layers 00-08 contain the correct records? | §1c-1e | When adding or editing seed data |
| C | **ETL statistics accuracy** | Are the computed results (layers 10+) correct? | §2c, which reads back the Layer 2/3 verification reports produced during each ETL run | After each ETL run |

See [Running the database audit](#running-the-database-audit) for how to run it and read the report. The standalone scripts behind each concern exist for one-off use but are not required for a normal audit. They are listed under [Standalone tools](#standalone-tools).

---

### Standalone tools

The monthly audit is the primary audit tool and is self-contained. These scripts can also be run on their own. Many of them were developed to check the database while it was in its early development stages, and are still around and helpful when needed.

| Tool | Command | In monthly audit? | Use case |
|------|---------|-------------------|----------|
| `run_audit.sh` | `bash database/run_audit.sh` | No (the audit takes its own snapshot) | Quick schema-only snapshot (`audits/*.json` + `*.csv`) |
| `verify_erd_against_audit.py` | `python database/audit/verify_erd_against_audit.py database/schema/ERD.md audits/latest.json` | Imported, but **has drifted** (see [SCHEMA_BACKLOG](SCHEMA_BACKLOG.md) § 10) | ERD docs vs live schema diff |
| `generate_erd_from_audit.py` | `python database/audit/generate_erd_from_audit.py audits/latest.json database/schema/GENERATED_ERD.md` | No | Draft an ERD from a live snapshot |
| `export_layer_tables.py` | `python database/scripts/export_layer_tables.py [--layer NN]` | No (the audit has its own export) | Standalone CSV export of layers 00-08 |
| `validate_data_integrity.sql` | `psql $DATABASE_URL -f database/scripts/sql/validate_data_integrity.sql` | No | FK orphan checks |
| `verify_all_sections.py` | `python etl/statistics/verify_all_sections.py --scenario {id}` | Reports read back (§2c) | ETL accuracy, CSV -> DB (Layer 2) |
| `verify_api.py` | `python etl/statistics/verify_api.py --scenario {id}` | Reports read back (§2c) | API accuracy, DB -> API (Layer 3) |
| `db_audit_lambda.py` | scheduled (see [Lambda S3 archive](#lambda-s3-archive)) | Separate (Lambda) | Schema snapshot to `s3://coeqwal-model-run/database_audits/` |

### Lambda S3 archive

`coeqwal-database-audit` is a bare-bones AWS Lambda that snapshots the schema to `s3://coeqwal-model-run/database_audits/` as dated JSON. It is manually invoked, there is no schedule, and `run_monthly_audit.py` produces a more complete audit. The Lambda is useful mainly for pulling a historical dated snapshot from S3.

```bash
# Invoke manually
aws lambda invoke --function-name coeqwal-database-audit --region us-west-2 response.json

# Pull archived snapshots
aws s3 ls s3://coeqwal-model-run/database_audits/ --recursive | tail -5
aws s3 cp s3://coeqwal-model-run/database_audits/audit_YYYYMMDD_HHMMSS.json ./audits/
```

See [utils/db_audit_lambda/README.md](utils/db_audit_lambda/README.md) for setup details.

---

## Running the database audit

The primary audit tool is `run_monthly_audit.py`. It produces a comprehensive report covering schema structure, ERD comparison, data content, ETL coverage, database health, and cost.

The standalone scripts are listed under [Standalone tools](#standalone-tools). See [audit/README.md](audit/README.md) for full documentation of all audit tools.

### Running the monthly audit

From Cloud9, with `DATABASE_URL` set:

```bash
cd ~/environment/coeqwal-backend
source venv/bin/activate
python database/audit/run_monthly_audit.py
```

To get full visibility (including tables where your role lacks SELECT), use the superuser connection:

```bash
DATABASE_URL=$SUPERUSER_URL python database/audit/run_monthly_audit.py
```

### Report sections

| # | Section | What it checks |
|---|---------|----------------|
| 1a | Table inventory | Every table: name, layer, columns, rows, size |
| 1b | Schema vs. ERD | Tables/columns in DB but not ERD, and vice versa. **Currently skipped**, the ERD comparator is broken (see [SCHEMA_BACKLOG](SCHEMA_BACKLOG.md) § 10) |
| 1c | Row counts vs. expected | Layers 00-08 counts against known targets |
| 1d | Reference data downloads | Full CSV export of layers 00-08 |
| 1e | Results data samples | Head/tail CSV samples of layers 10+ |
| 2a | Data integrity | NULL audit fields, orphaned rows, invalid values |
| 2b | ETL coverage | Per-scenario row counts across all results tables |
| 2c | ETL accuracy summary | Reads existing Layer 2/3 verification reports |
| 3a-d | Database health | Cache hit ratio, connections, dead tuples, bloat |
| 4a-c | Database cost | Table sizes, unused indexes, total storage |
| 5 | Audit summary | PASS/FAIL for each check, with details on failures |

### After a run, what to read

Read top-down, and stop at the first level that gives you your answer:

| Level | Where | What it tells you |
|---|---|---|
| **Console** | terminal | Ends with a `MONTHLY AUDIT COMPLETE` block naming the output directory and the report filename. |
| **Digest** | `audits/monthly_<ts>/report.md` | The top-level summary: row counts, schema-vs-ERD diff, audit-field checks, ETL coverage, health, and cost. The §5 audit summary flags any PASS/FAIL. |
| **Forensic** | `audits/monthly_<ts>/layer_exports/`, `results_samples/` | Open the per-layer CSVs only when the report flags a section. |

### Skipping sections

```bash
python database/audit/run_monthly_audit.py --skip health
python database/audit/run_monthly_audit.py --skip health --skip cost
```

Valid sections: `content`, `verification`, `health`, `cost`.

---

## Roadmap


Schema-level hygiene and audit-derived corrections live in [`SCHEMA_BACKLOG.md`](SCHEMA_BACKLOG.md).

### Product-driven

#### Community water systems (CWS) dataset

The design, planned tables, design decisions, and open questions can be found in [Planned tables (not yet in DB)](#planned-tables-not-yet-in-db) above.

#### Demand-unit group membership (Data Explorer filters)

The `du_*_group` / `du_*_group_member` tables let the website filter demand units by membership (NOD/SOD, SWP/CVP served, hydrologic region). The plumbing is partly in place but not enough to query, for example, for tier outcome and data in depth statistics.

- **Backfill the 5 empty `du_urban_group` rows** (`nod`, `sod`, `swp_served`, `cvp_served`, `swp_delivery_point`) so the existing per-DU SWP/CVP/NOD/SOD memberships are queryable. (These are the per-DU relatives of the project-level rollups in `cws_aggregate_entity`.)
- **Add the ag-side counterpart:** There is no `du_agriculture_group` / `du_agriculture_group_member` yet. Create them and populate `nod`, `sod`, `swp_served`, `cvp_served`, `cvp_settlement`, `cvp_exchange`, `non_district`, plus per-hydrologic-region groups (`sac`, `sjr`, `tulare`), all derivable from the existing `cs3_type` / `provider` / `hydrologic_region_id` columns on `du_agriculture_entity`.
- **Expose these in the API:** Surfacing group membership in the `/demand-units` (urban) and `/ag-demand-units` responses is one join in the existing FastAPI route handlers, and lets the website filter without per-group queries.

#### Scenario assumptions and operations metadata, align DB with the website

Metadata for operations, assumptions, and themes was hardcoded on the frontend while those values were still settling with the team (the same was true of other data, such as scenario and tier definitions). Because the definitions kept shifting, the final database entries and API shapes were never locked in. So today the per-scenario operation/assumption icons live in `apps/main/app/features/scenarios/components/shared/opsIcons.tsx` (`ICON_REGISTRY` + `SCENARIO_ICONS`), and that file is the current source of truth for which icons belong to which scenario. The database already has the parallel tables and fields (`operation_definition`, `assumption_definition`, `scenario_key_operation_link`, `scenario_key_assumption_link`), but its rows need to catch up to the frontend before the website can read them from the API instead of the hardcoded json.

Equivalently, the per-scenario theme is currently inferred from `sibling_group` via a hardcoded `scenarioMetadata` map in `apps/main/app/content/scenarios.ts`. The DB has a `theme` table linked through `theme_scenario_link`.

**Prerequisite, build a short_code to icon-id crosswalk:** The frontend icon ids are a parallel vocabulary that does not match the DB short_codes (`cws_hhs` vs `comm_delivery_HHS`, `unimpaired_45` vs `delta_outflow_45`, `tunnel` vs `DCP_6000`, `biops_2019` vs `biops_standard`, and so on). A mapping is the prerequisite for the steps below. Some frontend icon ids (`reduced_sj_ag`, `functional_flows_salmon`, etc.) have no `operation_definition` row yet, so the crosswalk doubles as a gap list of definitions to add. The frontend is authoritative, so reconciliation is one-directional. The corrected frontend values flow back into the DB as a data-correction migration (not a structural change).

Sequenced TODOs:

1. **Fix wrong/missing operation links in `scenario_key_operation_link`:**
   - `s0046`: add `functional_flows` (currently linked only to `no_min_flow`. Website shows both `functional_flows` and `no_delta_flow`).
   - `s0046`: change `biops_standard` -> `biops_modified_2019`.
   - `s0065`: change `biops_standard` -> `biops_modified_2019`.
   - Decide whether `s0026 / s0028 / s0032 / s0033` should link to a new "reduced ag acreage" operation (see step 2) or stay linked to `SGMA_SJV` / `SGMA_CV`. Today the DB collapses "pumping limits" and "reduced acreage" into the SGMA op even though the scenario descriptions distinguish them and the website renders them as `reduced_sj_ag` / `reduced_cv_ag`.

2. **Add missing operation rows in `operation_definition`** for the icons the website uses but the DB doesn't model:
   - `usbr_alt2v1`: USBR 2024 LTO Alt2V1 framework (`s0023`, `s0024`)
   - `usbr_alt3`: USBR 2024 LTO Alt3 framework (`s0039`-`s0042`)
   - `limit_delta_exports`: Delta export limits (`s0039`-`s0042`)
   - `dwr_adapt_2025`: DWR 2025 climate adaptation framework (`s0065`)
   - `reduced_sj_ag`, `reduced_cv_ag`: reduced agricultural acreage, or a single regional-scope op (`s0026`, `s0028`, `s0032`, `s0033`). Decide naming with the modeling team before adding rows.

3. **Add an `is_renderable` (or `is_baseline_no_op`) column on `operation_definition`** so API consumers can filter out the "standard / no-op" rows that every baseline scenario links to: `gw_none`, `infra_standard`, `flow_standard`, `delta_regs_standard`, `alloc_standard`. The website never shows these. Without a flag the API has to return them and the client has to know which ones to drop. `ALTER TABLE operation_definition ADD COLUMN is_renderable BOOLEAN NOT NULL DEFAULT TRUE` and set the five rows above to `FALSE`.

4. **Populate `key_operations` for the CWS scenarios (`s0035`, `s0036`, `s0037`):** They are seeded as inactive "Coming soon" placeholders with zero rows in `scenario_key_operation_link`. The matching operations (`comm_delivery_HHS`, `comm_delivery_functional`, `comm_delivery_full`) already exist. Link them when the scenarios are activated.

5. **Reconcile the assumption vs. operation distinction:** The website's icon track mixes both (e.g. `land_use_2020` is a visual icon next to `tucp`/`biops_*`). In the DB, land-use lives in `assumption_definition` (`lu_2020_landiq`, `lu_2004_2013`, etc.) and is linked via `scenario_key_assumption_link`. Two options:
   - Keep the schemas separate and have the API consumer merge `key_assumptions` + `key_operations` into one visual list (current frontend behavior).
   - Add a unified `scenario_badge` view (or one materialized response field) that returns the combined list in display order, with a `category` discriminator.

6. **Re-run the audit** (`database/audit/run_monthly_audit.py`) after each seed/schema change and confirm the link tables still pass referential integrity.

Once steps 1-4 land, the API needs to re-add `key_operations` and `key_assumptions` to `GET /api/scenarios` (and the detail endpoint) and the website can drop the hardcoded `SCENARIO_ICONS` mapping, keying `ICON_REGISTRY` by `operation.short_code` / `assumption.short_code` returned from the API instead. SVG payloads and colors stay client-side. The per-scenario list of which icons to show comes from the API.

#### Developer access and authentication (not urgent)

- **SSO user attribution:** currently developers connect to the database using a named PostgreSQL role (e.g. `jfantauzza`). The long-term goal is to use AWS SSO identity for authentication so that the `aws_sso_username` field in the `developer` table is used automatically, without requiring a separate PostgreSQL password per developer.
- **Role-based table permissions:** shipped in [`database/sql_archive/04_scenario/57_install_coeqwal_developer_role.sql`](sql_archive/04_scenario/57_install_coeqwal_developer_role.sql). The `coeqwal_developer` group role holds `SELECT, INSERT, UPDATE, DELETE` on every table in `public`, and `ALTER DEFAULT PRIVILEGES FOR ROLE postgres` makes that grant auto-extend to any future table created via `$SUPERUSER_URL`. New developers get RW on everything via `GRANT coeqwal_developer TO <username>` (see "Setting up a new developer" above). This closed the permission-gap class of bugs surfaced by the `variable_type` issue during auditing.

#### API conventions, geometry

**Policy:** Geometry of any kind, points, lines, or polygons, must not flow through `coeqwal-api`. All geometry reaches the browser exclusively through Mapbox vector tilesets registered in [apps/main/app/features/map/config/tilesetSources.ts](../../coeqwal-website/apps/main/app/features/map/config/tilesetSources.ts) on the website side. The API returns identifiers (`short_code`, `du_id`, `location_id`, `wba_id`) which the frontend joins to tile features at render time.

**Rationale:** Vector tiles are dramatically more bandwidth-efficient than GeoJSON over HTTP. Tiles are pre-quantized to integer grid coordinates, zoom-level-aware (the browser fetches only the resolution it currently needs), served from Mapbox's global CDN with aggressive caching, and consumed natively by Mapbox GL without per-render JSON parsing. A full demand-unit polygon `FeatureCollection` from the API is multiple MB. The equivalent tile request at zoom 8 is typically tens of KB. The DB retains the `geom` columns on entity tables (`du_urban_entity`, `du_agriculture_entity`, `wba`, `reservoir`, `network_node`, `compliance_station`, `network_arc`) because the ETL needs them to build tilesets, but those columns never leave the DB except through the tile pipeline.

**Cross-reference:** See [coeqwal-website/packages/map/README.md](../../coeqwal-website/packages/map/README.md) for the workflow for uploading new tile data via Mapbox Tiling Service.

#### Frontend-hardcoded data that should move into the DB

Each item below is a static TS or TSX data literal in the website repo whose source of truth is, or should be, the database. The website hardcodes today because the data was either never API-exposed or because the API contract was simpler when the frontend owned the strings. As schemas stabilize, each entry should migrate to a DB column or table and be served via the existing API. The frontend remains the source of truth in the meantime.

- **Outcome names, definitions, ordering, and tier value descriptions:** [apps/main/app/content/outcomes.ts](../../coeqwal-website/apps/main/app/content/outcomes.ts) (`OUTCOME_NAMES`, `OUTCOME_CODE_ORDER`, `OUTCOME_DEFINITIONS`, `OUTCOME_TIER_VALUES`, `NOD_SOD_DEFINITIONS`, `METRIC_ID_TO_OUTCOME_CODE`). Target: `tier_definition` already holds tier descriptions. Add display and ordering columns for `OUTCOME_CODE_ORDER` / `OUTCOME_NAMES`. Verify it covers all 9 outcomes plus NOD/SOD variants. Goal is for the website to consume something like `GET /api/tiers/definitions`.

- **Tier labels:** [apps/main/app/content/tiers.ts](../../coeqwal-website/apps/main/app/content/tiers.ts) (`TIER_LABELS` = Optimal, Acceptable, At-risk, Critical). Small but conceptually metadata. Either add `tier_level_label` to a lookup table or accept this as cosmetic and leave in code.

- **Hydroclimate metadata:** [apps/main/app/content/scenarios.ts](../../coeqwal-website/apps/main/app/content/scenarios.ts) (`HYDROCLIMATE_ID_MAP`, `HYDROCLIMATE_LABEL_MAP`, `HYDROCLIMATE_SHORT_LABELS`, `HYDROCLIMATE_LABELS_BY_VALUE`, `HYDROCLIMATE_DESCRIPTIONS_BY_VALUE`). Target: extend the `hydroclimate` table with `short_label`, `long_label`, and `description` columns. Re-add the `JOIN hydroclimate h` to `/api/scenarios` (it was dropped in the 2026-05-26 trim) and expose the new columns there.

- **Sibling-group / scenario UI metadata:** [apps/main/app/content/scenarios.ts](../../coeqwal-website/apps/main/app/content/scenarios.ts) (`SIBLING_GROUP_METADATA` and `scenarioMetadata`, theme plus `iconPath` and `shortLabel` per sibling group, ~30 entries). Target: `scenario_hydroclimate_sibling` already holds the group. Add `theme_short_code` and `icon_path` columns. Themes are already keyed in the `theme` table, so the join would let the API return the mapping directly.

- **Environmental flow station names:** [apps/main/app/features/map/config/outcomeLocations.ts](../../coeqwal-website/apps/main/app/features/map/config/outcomeLocations.ts) (`ENV_FLOWS_NAMES`, 17 entries). Target: add `display_name` column on the `env_flow_channel` table. Surface it in `/api/env-flows/channels`.

- **Compliance station and pumping plant names:** Same file (`STATION_NAMES`, 4 entries: `EM`, `JP`, `CAA003`, `DMC000`). Target: `compliance_station.display_name` already exists in many cases. Confirm and then remove from the frontend.

- **Reservoir display-name mapping:** [apps/main/app/features/map/config/outcomeLayerRegistry.ts](../../coeqwal-website/apps/main/app/features/map/config/outcomeLayerRegistry.ts) (`RESERVOIR_CALSIM_TO_GNISIDLABEL`, 8 entries). The file already has an inline TODO: "Update the california-reservoir Mapbox tileset to include a `calsim_id` property... then remove this mapping." Promote that inline TODO into this roadmap. Two-step fix. First, regenerate the reservoir tileset with `calsim_id` properties (tile side). Then drop the TS mapping (frontend side).

- **Hardcoded point coordinates:** [apps/main/app/features/map/config/outcomeLocations.ts](../../coeqwal-website/apps/main/app/features/map/config/outcomeLocations.ts) (`AG_REV_COORDINATES` 132 pts, `CWS_DEL_COORDINATES` ~100 pts, `ENV_FLOWS_COORDINATES` 17 pts, `STATION_COORDINATES` 4 pts, `RESERVOIR_CONFIGS` 7 pts, `SALMON_RIVER_CENTROID` 1 pt). The file header says: "Coordinates are hardcoded because the tier API returns location IDs and tier levels but not geometry." Per the geometry policy above these coordinates belong in Mapbox tiles, not the API and not in hardcoded TS. Two paths: (a) Add a `coeqwal-locations-points` tileset that consolidates all point and centroid coordinates with `outcome_code` and `short_code` properties. (b) Compute DU centroids on-the-fly from the existing polygon tiles. Either way the static TS file goes away. This is a follow-on to the geometry policy above.

#### Tile work (Mapbox MTS, no DB writes required)

The publishing path is Mapbox Tiling Service. See the workflow in [coeqwal-website/packages/map/README.md](../../coeqwal-website/packages/map/README.md) under "Adding new tile data via Mapbox Tiling Service" and the existing recipe layout in `coeqwal-backend/scripts/mapbox_recipes/`.

- **Regenerate the reservoir tileset with a `calsim_id` property:** Cross-reference for the "Reservoir display-name mapping" bullet above. Re-export `reservoir` rows to include `calsim_short_code` as `calsim_id` in each feature's properties, write or update a recipe in `scripts/mapbox_recipes/`, then `tilesets upload-source` + `tilesets create` + `tilesets publish` under the `coeqwal` account. Once the property is on every feature, drop `RESERVOIR_CALSIM_TO_GNISIDLABEL` from [apps/main/app/features/map/config/outcomeLayerRegistry.ts](../../coeqwal-website/apps/main/app/features/map/config/outcomeLayerRegistry.ts) and set the layer's `idProperty` to `calsim_id`.

- **Publish a consolidated `coeqwal-locations-points` tileset:** Cross-reference for the "Hardcoded point coordinates" bullet above. One NDGeoJSON feed per outcome, joined into a single tileset whose features carry `outcome_code` (one of `AG_REV`, `CWS_DEL`, `ENV_FLOWS`, etc.) and `short_code` (the per-feature identifier the API uses). Register as a new source in [apps/main/app/features/map/config/tilesetSources.ts](../../coeqwal-website/apps/main/app/features/map/config/tilesetSources.ts), wire it into the affected outcome layers, then delete the coordinate tables from `outcomeLocations.ts`. The animation in `useTierAnimationData.ts` currently joins tier results to `AG_REV_COORDINATES` in TS. Once the tileset exists the animation can read centroids out of the tile instead.

- **Add `du_id` parity to whatever demand-unit tilesets still rely on legacy keys:** `coeqwal-demand-units` (per `tilesetSources.ts`) is keyed by `DU_ID` already, so this is mostly a sanity-check pass to confirm every DU referenced by the API has a matching tile feature, and to log the gaps already enumerated in `database/topic_docs/demand_unit_geometry.md`. Output: a small script in `scripts/mapbox_recipes/` that diffs API DU IDs against tile feature properties.

### Data pipeline / ETL

These improve the ETL and its provenance, but the API and website work without them.

#### Layer 04 variable layer (priority)

Layer 04 should serve as the single source of truth for the variable lists the ETL uses to compute statistics. The statistics the website shows are computed today from hardcoded Python dicts and CSV reads. The one Layer 04 table that already feeds the API, `du_urban_variable` (consumed by `/demand-units` in the API and by `etl/statistics/du_urban/calculate_du_statistics_v2.py`), is built as an example. Building out the rest is a key operation. It makes Layer 04 the single source of truth for the variable lists and removes the hardcoded Python dicts and CSV reads the statistics currently depend on. Table shapes and the reference pattern (`du_urban_variable`) are documented in [`schema/ERD.md`](schema/ERD.md) § Layer 04.

> **Planning source:** The variable lists each table needs are driven by the statistics in the **Outcomes tab** of the [COEQWAL Platform Content Summary](https://docs.google.com/spreadsheets/d/1xcQIR_J96-cs7BuCrXjznwkinLgxl-Pf9tA3mJ2GiyA) sheet. Develop the list of variables and calculations in collaboration with the Water Allocation Modeling Team.

> **Registration step (every new table):** Each new Layer 04 table needs a row in `domain_family_map` mapping it to the `variable` family (`version_family.id = 6`). See `database/README.md` § "Creating a new table" step 5 and [`schema/ERD.md`](schema/ERD.md) § `domain_family_map`.

Backlog (but use your own judgement):

- [ ] **Build `delta_variable`** (~8 vars). 2 rows already in `database/seed_tables/04_variable/delta_variable.csv` (NDO, X2 regulatory metadata). Proposed columns in `schema/ERD.md` § Layer 04. Effort: S.
- [ ] **Add `umhos_cm` to the `unit` catalog:** Microsiemens per centimeter is the native unit for the EC salinity variables the delta calc reads (Emmaton, Jersey Point, Rock Slough, Collinsville, Banks, Tracy). Prerequisite for adding EC rows to `delta_variable`. Effort: S.
- [ ] **Build `reservoir_variable`:** Seed CSV `database/seed_tables/04_variable/reservoir_variable.csv` curated. Replaces two current mechanisms (storage variables by naming convention from `reservoir_entity.csv`, and the hardcoded `RESERVOIR_THRESHOLDS` dict in `etl/statistics/reservoirs/reservoir_metrics.py`) with one DB lookup. Requires deciding where to encode the threshold-role metadata. Effort: M.
- [ ] **Build `inflow_entity` first, then `inflow_variable`:** Seed CSV `database/seed_tables/04_variable/inflow_variable.csv` is shape-correct but has content gaps (all rows `variable_type='inflow'`, empty `description`). No inflow ETL exists yet, so this is "ready when an inflow calculation is written." Requires building the parent `inflow_entity` table first (Layer 03 future work). Effort: L.

Deferred (dict-driven domains, need content curation against the Outcomes tab):

- [ ] **`mi_contractor_variable`:** lift `MI_CONTRACTOR_VARIABLES` out of `calculate_mi_statistics.py` (~35 contractors x 4 var lists). FK target `mi_contractor.id`. Effort: M.
- [ ] **`cws_aggregate_variable`:** lift `CWS_AGGREGATES` out of `calculate_cws_aggregate_statistics.py` (~10 aggregates). FK target `cws_aggregate_entity.id`. Effort: S.
- [ ] **`ag_variable`:** FK target `du_agriculture_entity.id`. Has a WBA-region branch (`SHRTG_` vs `GW_SHORT_`) needing a column-shape decision. Effort: M.
- [ ] **`refuge_variable`:** FK target `du_refuge_entity.id`. Same WBA-region branch question. Effort: M.

Related ETL cleanup:

- [ ] **Retire or refactor `etl/statistics/generate_release_variables.py`:** One-shot helper that generated the 276 release-variable rows now in `reservoir_variable.csv`. Not imported or invoked by `run_all.py`. Effort: S.

#### Audit tooling maintenance

The monthly audit's table coverage is hand-maintained and drifts as the schema grows. Layer 09 (TIER) currently falls through the cracks. `tier_location` is exported nowhere and `tier_definition` is sampled as a result rather than exported in full. Details and the suggested fix are in [`audit/README.md`](audit/README.md) § "Maintaining the audit". The standing task is to update the audit's layer lists and ERD whenever a new layer or reference table is added. Effort: S.

#### ETL operator attribution (low priority)

Make `created_by` / `updated_by` reflect the real operator, not the System fallback. Low priority: the cost is not super consequential, at this point.

- [ ] **Run ETL pipelines as a developer role** (e.g. an `etl` service account with its own `developer` row) rather than the shared `postgres` superuser. Most CalSim-derived result rows attribute to System (`developer.id = 1`) because ETL connects via `$SUPERUSER_URL`. Effort: M.
- [ ] **Add a session-variable hook to `coeqwal_current_operator()`** so a caller connected as `postgres` can `SET LOCAL coeqwal.operator = 'alicelalala'` at the start of a transaction and have the function read that instead of falling back to System. Effort: M.
- [ ] **Backfill the 1 NULL row in `hydroclimate.created_by`:** Single UPDATE. Effort: S.

### Correctness bugs

The schema-level detail for these also live in [`SCHEMA_BACKLOG.md`](SCHEMA_BACKLOG.md).

#### Resync drifted ID sequences, and add a drift check

- [ ] **Resync 5 drifted ID sequences** (`developer_id_seq`, `version_family_id_seq`, `version_id_seq`, `cws_aggregate_entity_id_seq`, `du_urban_group_id_seq`). Real bug, not debt: `last_value < MAX(id)` means the next insert into these tables collides on the PK and raises `unique_violation`. One-shot `setval(..., MAX(id))`. Also bundled in [`scripts/sql/audit_cleanup.sql`](scripts/sql/audit_cleanup.sql) § 5. Effort: S.
- [ ] **Add a sequence-drift check to `run_monthly_audit.py`** that flags any `*_id_seq` where `last_value < MAX(id)`, so future drift is caught at audit time. Effort: S.

#### Correct `watershed.hydrologic_region_id` for 4 drifted rows with the wrong hydrologic region assignment

- [ ] Data-correctness bug: the wrong region assignment quietly skews any region-grouped query against `watershed`. Live DB has `BEAR_RIVER`, `UPPER_AMERICAN`, `UPPER_FEATHER`, and `YUBA_RIVER` assigned to `hydrologic_region_id = 2` (SJR). I believe this is incorrect and all four drain to the Sacramento system. After the fix, the `watershed` Values table in [`schema/ERD.md`](schema/ERD.md) (the `#### watershed` section) needs 4 region values flipped from `SJR` to `SAC`. Effort: S.