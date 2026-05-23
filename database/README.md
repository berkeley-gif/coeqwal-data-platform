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

**The two URLs at a glance**

| URL | Who they connect as | What it's for | Audit attribution? |
|---|---|---|---|
| `$DATABASE_URL` | Their own named role (e.g. `alice`) | Daily work: SELECTs, INSERTs, UPDATEs, running ETL scripts | Yes - works correctly |
| `$SUPERUSER_URL` | Shared `postgres` account (everyone uses the same password) | DDL only: `CREATE TABLE`, `ALTER`, `GRANT`, migrations | No - all writes attributed to `id=1` (system). That's fine for migrations because the relevant info is "the migration ran," not "who typed `psql`." |

Practical rule: if you are typing DML, use `$DATABASE_URL` so your name lands in the audit log. If you are typing DDL, use `$SUPERUSER_URL` and don't worry that it shows up as `id=1` - that's the intended trade-off.

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

Tier definitions live in the `tier_definition` table (seeded from
[`database/seed_tables/10_tier/tier_definition.csv`](../database/seed_tables/10_tier/tier_definition.csv)).
The per-scenario tier values and per-location tier rows are project
data, populated by the ETL pipeline. Adding a new tier requires:

1. INSERT the tier definition row (update `tier_definition.csv` and re-seed, or
   `INSERT` directly)
2. Add a `staging/<TIER_SHORT_CODE>.csv` file under [`etl/tier_data/staging/`](../etl/tier_data/staging/) and
   wire a loader function into
   [`etl/tier_data/scripts/load_all_tier_results.py`](../etl/tier_data/scripts/load_all_tier_results.py)
3. Run the loader to populate `tier_result` and `tier_location_result`. See
   [`etl/tier_data/README.md`](../etl/tier_data/README.md) for the full workflow
4. Update the frontend tier configuration to display the new tier

### Adding tier data for new scenarios

After new scenarios are loaded into the `scenario` table and their statistics ETL is complete:

1. Update the tier-team staging CSVs in [`etl/tier_data/staging/`](../etl/tier_data/staging/) with
   rows for the new scenarios
2. Run the loader (see [`etl/tier_data/README.md`](../etl/tier_data/README.md)) to UPSERT into
   `tier_result` and `tier_location_result`
3. Verify with the monthly audit's per-scenario ETL coverage check

### Redeploying the API

If you changed the API endpoint code (anything under `api/`), including query logic, response fields, or CORS settings, you need to redeploy. Push the changes to `main` on GitHub — the CI pipeline handles the rest. See the [API README](../../api/coeqwal-api/README.md) for details and manual deployment fallback.

---

## Getting started

### Two paths

| Path | Use it for | Connection |
|---|---|---|
| **Cloud9 / VPN -> production RDS** | Developer work, monthly audits, real seed loads, DDL migrations on the live DB | `DATABASE_URL` set per "First-time setup" below |
| **Linux dev host -> local Postgres** _(unsupported)_ | Offline schema work, query development, iterating on migrations before they touch RDS | `DATABASE_URL=postgresql://coeqwal:coeqwal@localhost:5432/coeqwal_scenario` |

> **Local Postgres is unsupported.** The bootstrapper at [`scripts/setup_dev_env.sh`](../scripts/setup_dev_env.sh) (which brings up the local DB via [`docker-compose.yml`](../docker-compose.yml) and applies schema + seeds via [`scripts/load_local_seeds.sh`](../scripts/load_local_seeds.sh)) is best-effort on Linux and not maintained for macOS or Windows. Cloud9 is the supported environment for everything that touches production data.

> **Seed CSV `is_active` is bootstrap-only for `scenario.csv`.** [`database/seed_tables/06_scenario/scenario.csv`](seed_tables/06_scenario/scenario.csv) introduces new scenario rows into the DB via [`database/scripts/sql/upsert_scenario_data.sql`](scripts/sql/upsert_scenario_data.sql). Once a row exists, flip `is_active` with [`etl/ingestion/tools/set_scenario_active.py`](../etl/ingestion/tools/set_scenario_active.py), not by editing the CSV and re-upserting. The seed CSV's `is_active` value is allowed to drift from the live `scenario` table by design. The DB is the source of truth for live publication state, exposed by the API as [`/api/scenarios`](../api/coeqwal-api/routes/scenario_endpoints.py) and cached for ETL consumers in [`etl/common/active_scenarios.py`](../etl/common/active_scenarios.py).

> **Tier location membership is owned by the tier-team staging CSVs.** `tier_location` is a narrow database catalog (`tier_short_code`, `location_type`, `location_id`, `display_order`, `is_active`). The tier teams' staging CSVs in `etl/tier_data/staging/` are the source of truth for membership. There is no seed CSV. To reconcile, run [`etl/tier_data/scripts/diff_tier_locations.py`](../etl/tier_data/scripts/diff_tier_locations.py) for the diff and [`etl/tier_data/scripts/sync_tier_locations_from_staging.py`](../etl/tier_data/scripts/sync_tier_locations_from_staging.py) to apply (inserts active rows, soft-deletes rows that left staging). Display names and geometry are resolved at query time by joining `location_id` to the entity tables in the registry at [`etl/common/tier_location_entities.py`](../etl/common/tier_location_entities.py); the public API uses the same join map for [`/api/tier-map/{scenario}/{tier}`](../api/coeqwal-api/routes/tier_map_endpoints.py) GeoJSON output.

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

### Sharing the Cloud9 environment across developers

Cloud9 backs a single EC2 instance that all collaborators share as the OS user `ec2-user`. That means `~/.bashrc`, `~/.gitconfig`, `~/.ssh/`, and shell history are shared. AWS Cloud9 also injects `$C9_USER` into every shell, but it reflects whichever IAM principal most recently attached via the browser IDE - it does not change per concurrent collaborator and cannot be used to identify the current user. The default Cloud9 PS1 renders `$C9_USER`, which is why every collaborator sees the same name in their prompt regardless of who is actually typing.

Two more things to know before onboarding:

- `ec2-user` has passwordless `sudo`. Every collaborator can become root on this EC2. That is acceptable for a small trusted team but it is not a security boundary; treat the EC2 as collectively administered.
- `aws sts get-caller-identity` from inside the shell returns the EC2's instance profile role (`AWSCloud9SSMAccessRole/<instance-id>`), the same string for every collaborator. It does not identify the human user. The AWS CLI on this EC2 has no `iam:*` perms anyway; for AWS admin commands use CloudShell from the AWS console (see `docs/INFRASTRUCTURE.md` §9.3).

To keep each developer's `DATABASE_URL` (and audit attribution) correct without overwriting each other:

1. **Shared `~/.bashrc`** (set up once by the lead) defines a `become<name>` alias for each collaborator and exports the shared `SUPERUSER_URL`.
2. **Each developer creates `~/.coeqwal-env-<their_name>`** (mode 600) with their personal `DATABASE_URL`, `COEQWAL_USER`, and a `PS1` that uses their name.
3. **At the start of each session, the developer runs `become<their_name>`.** That sources their env file and gives them the correct prompt, `DATABASE_URL`, and identity for that shell.

#### A) Shared `~/.bashrc` addition (one-time, by the lead)

Append to `/home/ec2-user/.bashrc` after the existing Cloud9 default block:

```bash
# --- COEQWAL multi-developer setup ---
# Each developer runs their own 'become<name>' alias at the start of a session.
# That sources ~/.coeqwal-env-<name>, which exports their personal DATABASE_URL,
# sets COEQWAL_USER, and overrides PS1 so the prompt matches the active identity.

export SUPERUSER_URL="postgresql://postgres:PASSWORD@coeqwal-scenario-database-1.clai4yqcyzxh.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario"

alias becomemelijimenez='source ~/.coeqwal-env-melijimenez'
alias becomebkallay='source ~/.coeqwal-env-bkallay'
alias becomeelehmer='source ~/.coeqwal-env-elehmer'
alias becomebgaley='source ~/.coeqwal-env-bgaley'
alias becomejfantauzza='source ~/.coeqwal-env-jfantauzza'

whoami_coeqwal() {
  echo "OS user:        $(whoami)"
  echo "Cloud9 C9_USER: ${C9_USER:-<unset>} (last IDE attacher, ignore)"
  echo "COEQWAL_USER:   ${COEQWAL_USER:-<not set - run becomeXXXX>}"
  if [ -n "$DATABASE_URL" ]; then
    echo "DATABASE_URL:   $(echo "$DATABASE_URL" | sed -E 's|^(postgresql://)([^:]+):[^@]*@|\1\2:***@|')"
  else
    echo "DATABASE_URL:   <not set>"
  fi
}
# --- end COEQWAL ---
```

`SUPERUSER_URL` lives in shared `~/.bashrc` because the master password is intentionally shared by all five collaborators (anyone with `sudo` could read it from any per-user file anyway). Personal Postgres passwords stay in each dev's own env file (`mode 600`) to keep them at least notionally compartmented from casual `cat ~/.bashrc` reads.

#### B) Per-user env file (one-time, by each developer)

Each developer creates one file in their home directory on Cloud9. Example for `melijimenez`:

```bash
# ~/.coeqwal-env-melijimenez
export COEQWAL_USER="melijimenez"
export DATABASE_URL="postgresql://melijimenez:HER_PASSWORD@coeqwal-scenario-database-1.clai4yqcyzxh.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario"
export PS1='\[\033[01;32m\]'"${COEQWAL_USER}"'\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]$(__git_ps1 " (%s)" 2>/dev/null) $ '
```

Then `chmod 600 ~/.coeqwal-env-melijimenez` so the password is owner-readable only.

#### C) Per-session use (every developer, every Cloud9 session)

Open a Cloud9 terminal tab. The prompt initially shows whoever was the last IDE attacher (currently `elehmer`); ignore it. Run:

```bash
becomejfantauzza      # or your own become<name>
whoami_coeqwal        # confirm the identity
```

After that the prompt switches to your name, `DATABASE_URL` is your role, and `coeqwal_current_operator()` in Postgres will return your developer id. If you forget to run your `become<name>`, `whoami_coeqwal` will say `COEQWAL_USER: <not set - run becomeXXXX>`.

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
└── validation_mismatches/          #   DSS extraction records (Layer 1)
    ├── {scenario_id}_extract_record.json
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

### 03_ENTITY: entity tables and the entity-attribute pattern

The entity layer holds the canonical, version-controlled list of every "thing" the model and the website talk about: every reservoir, every channel, every demand unit, every M&I contractor, every community water system, every water budget area. Statistics tables in layer 10+ all reference rows in this layer by `<entity>_id`.

#### The entity-attribute pattern

Every domain in the database follows the same five-piece shape. Use this pattern when adding a new domain (e.g. `community_water_system`, future `inflow_entity`, etc.) so the audit, ETL, and API layers all behave consistently.

```
Layer 01 — lookups        hydrologic_region, source, model_source, unit, statistic_type, ...
                              ▲
                              │ FK
Layer 03 — entity         <domain>_entity                 (the "thing": one row per real-world object)
                          <domain>_group                   (optional; analytical groupings)
                          <domain>_group_member            (M:N membership)
                          <domain>_delivery_arc            (optional; multi-arc CalSim sums)
                          <related>_<domain>_link          (M:N to other entities, e.g. CWS↔DU)
                              ▲
                              │ FK
Layer 04 — variables      <domain>_variable                (CalSim variable names per entity)
                              ▲
                              │ FK (entity_id) + scenario_short_code
Layer 10+ — statistics    <domain>_<period>                (e.g. *_monthly, *_period_summary)
```

**Required columns on every entity table** (per `database/CHECKLIST_TABLE_STANDARDS.md`):
- `id SERIAL PRIMARY KEY`
- `short_code TEXT UNIQUE NOT NULL` — stable, machine-readable code used by ETL and API
- Domain attributes (FK IDs to lookup tables — never store text values for things that have a lookup)
- `is_active BOOLEAN NOT NULL DEFAULT TRUE` — soft delete
- `created_at`, `created_by`, `updated_at`, `updated_by` — populated automatically by the `set_audit_fields()` trigger
- A row in `domain_family_map` so the versioning system knows which `version_family` governs the table

**The `aggregate` adjective.** Several entity tables carry the suffix `_aggregate_entity` (`ag_aggregate_entity`, `cws_aggregate_entity`). It denotes a CalSim **project-level rollup** — one row per pre-computed CalSim variable that already sums many demand units (`DEL_SWP_PMI`, `DEL_CVP_PAG_N`, `DEL_SWP_MWD`, etc.). It does NOT mean "an aggregation of community water systems"; it means "this table holds the entities CalSim itself reports as aggregates rather than per-DU." The naming is accurate and stays as-is.

**Standard prefixes (canonical).** The COEQWAL team uses these short prefixes everywhere — in tables, columns, ETL, API routes, and frontend hooks. Pick the right prefix for any new table in this domain.

| Prefix | Meaning | Example tables |
|---|---|---|
| `cws_` | Community water system domain (urban / M&I / drinking water) | `cws_aggregate_entity`, `cws_entity` (planned), `cws_du_link` (planned), `cws_list` (planned) |
| `du_urban_` | Per-CalSim-DU rows in the CWS domain | `du_urban_entity`, `du_urban_variable`, `du_urban_group` |
| `mi_` | Per-M&I-contractor rows (subset of CWS domain) | `mi_contractor`, `mi_delivery_monthly` |
| `ag_` | Agricultural domain | `ag_aggregate_entity`, `ag_du_demand_monthly` |
| `du_agriculture_`, `du_refuge_` | Per-DU rows in the ag and refuge domains | `du_agriculture_entity`, `du_refuge_entity` |
| `reservoir_` | Reservoir domain | `reservoir_entity`, `reservoir_storage_monthly` |
| `channel_` | Channel domain | `channel_entity`, `env_flow_channel_monthly` |

> **Rule of thumb for new tables in the CWS domain:** use `cws_*` for things that are scoped to a community water system (e.g. PWSID-keyed entity, lookup, link). Keep `du_urban_*` for things scoped to a CalSim urban demand unit, and `mi_*` for things scoped to an M&I contractor. Do not introduce new prefixes.

#### Implemented entity tables

Counts are from the most recent monthly audit (run `python database/audit/run_monthly_audit.py` to refresh).

| Domain | Entity table | Records | Variable / link / group tables | Statistics tables (Layer 10+) |
|---|---|---:|---|---|
| Reservoirs | `reservoir_entity` | 92 | `reservoir_group`, `reservoir_group_member`; `reservoir_variable` (planned) | `reservoir_storage_monthly`, `reservoir_spill_monthly`, `reservoir_monthly_percentile`, `reservoir_period_summary` |
| Channels | `channel_entity` | 669 | `channel_variable` | `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary` |
| Compliance stations | `compliance_station` | 2 | — | (Delta tables, indirectly) |
| Water budget areas | `wba` | 42 | (referenced by DU tables via `wba_id`) | — |
| Agricultural DUs | `du_agriculture_entity` | 144 | — | `ag_du_demand_monthly`, `ag_du_sw_delivery_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary` |
| Refuge DUs | `du_refuge_entity` | 18 | — | `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary` |
| CWS DUs (urban) | `du_urban_entity` | 145 | `du_urban_variable`, `du_urban_delivery_arc`, `du_urban_group`, `du_urban_group_member` | `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary` |
| CWS contractors (M&I) | `mi_contractor` | 30 | `mi_contractor_delivery_arc`, `mi_contractor_group`, `mi_contractor_group_member` | `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary` |
| Agricultural project aggregates | `ag_aggregate_entity` | 9 | (delivery variable on entity row) | `ag_aggregate_monthly`, `ag_aggregate_period_summary` |
| CWS project aggregates | `cws_aggregate_entity` | 6 | (delivery + shortage variables on entity row) | `cws_aggregate_monthly`, `cws_aggregate_period_summary` |
| Reservoir (legacy) | `reservoir` | 7 | — | (predates `reservoir_entity`; kept for FK compatibility) |

**Project-level aggregates currently in the DB.** These are CalSim's pre-computed project-level totals — useful for "which DUs are SWP vs CVP" and "NOD vs SOD" questions at the **project** level. (Per-DU SWP/CVP/NOD/SOD membership lives in `du_urban_group_member` for urban DUs and is derivable from `du_agriculture_entity` for ag DUs.)

`cws_aggregate_entity` (6 rows) — CWS / M&I rollups:

| short_code | label | project | region | delivery_var | shortage_var |
|---|---|---|---|---|---|
| `swp_total` | SWP Total M&I | SWP | total | `DEL_SWP_PMI` | `SHORT_SWP_PMI` |
| `swp_nod` | SWP North | SWP | nod | `DEL_SWP_PMI_N` | `SHORT_SWP_PMI_N` |
| `swp_sod` | SWP South | SWP | sod | `DEL_SWP_PMI_S` | `SHORT_SWP_PMI_S` |
| `cvp_nod` | CVP North | CVP | nod | `DEL_CVP_PMI_N` | `SHORT_CVP_PMI_N` |
| `cvp_sod` | CVP South | CVP | sod | `DEL_CVP_PMI_S` | `SHORT_CVP_PMI_S` |
| `mwd` | Metropolitan WD | MWD | — | `DEL_SWP_MWD` | `SHORT_SWP_MWD` |

`ag_aggregate_entity` (9 rows) — agricultural rollups:

| short_code | label | project | region | delivery_var |
|---|---|---|---|---|
| `swp_pag` | SWP Project AG | SWP | TOTAL | `DEL_SWP_PAG` |
| `swp_pag_n` | SWP Project AG North | SWP | NOD | `DEL_SWP_PAG_N` |
| `swp_pag_s` | SWP Project AG South | SWP | SOD | `DEL_SWP_PAG_S` |
| `cvp_pag_n` | CVP Project AG North | CVP | NOD | `DEL_CVP_PAG_N` |
| `cvp_pag_s` | CVP Project AG South | CVP | SOD | `DEL_CVP_PAG_S` |
| `cvp_psc_n` | CVP Settlement Contractors NOD | CVP | NOD | `DEL_CVP_PSC_N` |
| `cvp_pex_s` | CVP Exchange Contractors SOD | CVP | SOD | `DEL_CVP_PEX_S` |
| `nod_ag` | Total NOD AG | — | NOD | `COMPUTED` (sum) |
| `sod_ag` | Total SOD AG | — | SOD | `COMPUTED` (sum) |

`mi_contractor_group` (6 rows) — contractor-level groupings: `swp`, `cvp_nod`, `cvp_sod`, `all_mi`, `swp_mi`, `swp_ag`.

`du_urban_group` (11 rows) — per-DU groupings: `tier` (71 members — the existing canonical focal set), `nod` (0), `sod` (0), `swp_served` (0), `cvp_served` (0), `swp_delivery_point` (0), `var_wba` (40), `var_gw_only` (3), `var_swp_contractor` (11), `var_named_locality` (15), `var_missing` (2). The 5 zero-member groups need to be backfilled.

> **No other aggregate-style tables exist.** Reservoir / channel / refuge / wba domains do not have `*_aggregate_entity` tables; their roll-ups are computed at query / API time when needed.

#### Planned entity tables (in ERD, not yet in DB)

Per the audit's "Tables in ERD but NOT in DB" section, these are documented but not implemented:

| Domain | Planned table | Notes |
|---|---|---|
| Inflows | `inflow_entity` | Watershed inflow nodes; see ERD for column list. Variable side already partly designed (`inflow_variable.csv` seed exists). |
| Reservoirs | `reservoir_variable` | Variable mapping for reservoirs (storage, spill, levels). Currently the ETL uses hardcoded `S_*`, `C_*_FLOOD`, `S_*LEVELxDV` patterns; promoting these to a table would match the channel / DU pattern. |
| Network attribution | `network_arc_attribute`, `network_node_attribute`, `network_source_attribution` | Per-network typed attribute extensions. |
| Network connectivity | `network_physical_connectivity`, `network_computational_connectivity`, `network_operational_connectivity` | Three connectivity perspectives planned in the ERD. |
| Network variables | `network_variable`, `variable_prefix` | Variable catalog at the network level. |
| Watershed | `river_watershed` | Watershed↔river crosswalk. |
| Hydroclimate | `hydroclimate_source` | Source attribution for hydroclimate scenarios. |
| Themes | `theme_source_link` | Source attribution for themes (parallel to `scenario_source_link`). |
| Outcomes | `outcome_category`, `outcome_statistic` | Outcome-framework tables (see `database/seed_tables/03_outcome_framework/` for partial seed). |
| ETL provenance | `data_load_log` | Batch-level load tracking; flagged as TODO in this README. |

New tables coming from the spring-2026 CWS data delivery (see plan in `database/seed_tables/03_entity/cws/` once staged):

| Planned table | Layer | Purpose |
|---|---|---|
| `cws_entity` | 03_entity | One row per California Public Water System (PWSID): `pwsid`, `system_name`, `pop_served`, `system_lat`, `system_lon`, `hydrologic_region_id`, `source_id`. ~476 systems from the 2026-04-13 master list. |
| `cws_du_link` | 03_entity | M:N junction `cws_entity` ↔ `du_urban_entity` (a system may serve multiple DUs and a DU may be served by multiple systems). One row per system-DU pair (~586 rows). |
| `cws_list` | 01_lookup | List/registry catalog (e.g. `coeqwal_master_du`, `coeqwal_focal_sw_du`, `calsim_urban_du`, `tier_matrix`, `hhs_allocation`). One row per named list. |
| `cws_list_du_member` | 03_entity | M:N junction `cws_list` ↔ `du_urban_entity`. Indicates which list(s) each DU belongs to (replaces / generalizes `du_urban_group_member` for CWS lists; see *Project list vs CalSim list* below). |

Plus new attribute columns on `du_urban_entity`: `is_sw_du`, `is_gw_du`, `largest_system_centroid_lat`, `largest_system_centroid_lon`, `calsim_centroid_lat`, `calsim_centroid_lon`, `hhs_allocation_taf`. And an updated delivery-variable crosswalk merged into `du_urban_variable`.

#### Project list vs CalSim list (community water systems)

The COEQWAL project's CWS focus list (delivered in `reference/community_water_systems/`) is **not** identical to the full CalSim urban DU list now in the database. The team needs to know which list any given DU belongs to. Numbers below are from the audit + reference CSVs as of 2026-05-11:

| List | Source | DUs |
|---|---|---:|
| `calsim_urban_du` | `du_urban_entity` (current DB) | 145 |
| `coeqwal_master_du` | `Master demand unit list updated April 13 2026.xlsx` | 124 |
| `coeqwal_focal_sw_du` | SW DUs in master list | 75 |
| `coeqwal_focal_gw_du` | GW DUs in master list | 83 |
| `hhs_allocation` | `Updated HHS allocations May 6 2026.xlsx` | 76 |
| `mi_delivery_crosswalk` | `Updated Master crosswalk SW DUs M&I May7 2026.xlsx` | 75 |
| `tier_matrix` | `du_urban_group` row `tier` | 71 |

**Set differences:**
- **In project master AND CalSim DB:** 117 DUs (the overlap)
- **In project master but NOT in CalSim DB (7):** `ACFC`, `KCWA`, `MHILL_NU`, `SBCWD`, `SVWRD`, `TLMNE`, `UNION` — these need to be **added to `du_urban_entity`** (or reconciled to existing rows under different `du_id`s).
- **In CalSim DB but NOT in project master (28):** `26N_NA`, `26N_NU513`, `50_PA1`, `50_PA2`, `60N_PA`, `60N_PU1`, `60S_PA`, `60S_PU`, `61_PA`, `61_PU1`, `61_PU2`, `63_PA`, `63_PR`, `64_PA`, `64_PU`, `65_PA`, `65_PU`, `70_PA`, `70_PU1`, `71_PA`, `72_PU1`, `72_PU2`, `90_PU5`, `CCWDI`, `CLLPT`, `CWD`, `ESB415`, `PINES` — flagged as **out-of-scope for the CWS focus** but kept for full CalSim compatibility.
- **In HHS list but NOT in project master (1):** `ESB355` — needs reconciliation (typo? `ESB315`/`ESB415`?).
- **HHS vs M&I crosswalk:** identical except `ESB355`.

The project master and the M&I crosswalk also carry **per-DU attributes that are not in CalSim** (`is_sw_du`, `is_gw_du`, two centroid pairs, HHS allocation in TAF). Those become columns on `du_urban_entity`.

**How to record list membership in the DB.** Two compatible patterns:

1. **Lookup + junction (recommended for the CWS list registry):**

   ```
   01_lookup/cws_list                        — id, short_code, label, description, source_id, is_active, audit fields
   03_entity/cws_list_du_member              — cws_list_id (FK), du_id (FK to du_urban_entity), is_active, audit fields
                                              UNIQUE (cws_list_id, du_id)
   ```

   This generalizes nicely if PWSID-level lists arrive later (`cws_list_system_member` keyed by `cws_entity_id`).

2. **Existing `du_urban_group` pattern:** the four new lists could just be added as new rows in `du_urban_group` and populated in `du_urban_group_member`. That is the lowest-friction path because the table already exists, but the prefix `du_urban_group` reads as "groupings of DUs" rather than "registries of CWS lists" — fine if the team accepts that. Note that 5 of the 11 existing `du_urban_group` rows (`nod`, `sod`, `swp_served`, `cvp_served`, `swp_delivery_point`) currently have **0 members** and need to be backfilled either way.

> **Recommendation.** Use option 1 (`cws_list` + `cws_list_du_member`) for the new project lists, and backfill the legacy `du_urban_group` rows in the same migration. Both tables are queryable, both reference `du_urban_entity` by `du_id`, and the registry is explicit about its purpose.

#### How the database represents lists, subsets, and group memberships

Eight distinct patterns are in use today. New work should pick the one that best fits the cardinality and stability of the relationship.

| # | Pattern | Where it's used | Populated? |
|---|---|---|---|
| 1 | **`X_group` + `X_group_member` junction** (M:N) | `reservoir_group` (4 rows) + `reservoir_group_member` (24 rows) — `major`, `cvp`, `swp`, `tier`. `du_urban_group` (11 rows) + `du_urban_group_member` (142 rows) — `tier`, `nod`, `sod`, `swp_served`, `cvp_served`, `swp_delivery_point`, `var_*`. `mi_contractor_group` (6 rows) + `mi_contractor_group_member` (60 rows) — `swp`, `cvp_nod`, `cvp_sod`, `all_mi`, `swp_mi`, `swp_ag`. | Reservoirs: fully populated. Urban DUs: 6 of 11 groups populated (5 geographic/project groups `nod`, `sod`, `swp_served`, `cvp_served`, `swp_delivery_point` are **empty**). MI: 3 of 6 groups populated (`swp` 30, `swp_mi` 23, `swp_ag` 7); `cvp_nod`, `cvp_sod` empty because no CVP contractors are loaded yet; `all_mi` empty (missed seed step). |
| 2 | **Aggregate-entity table** (one row per pre-summed CalSim variable) | `cws_aggregate_entity` (6 rows: `swp_total`, `swp_nod`, `swp_sod`, `cvp_nod`, `cvp_sod`, `mwd`). `ag_aggregate_entity` (9 rows: SWP/CVP PAG NOD/SOD, settlement, exchange, two computed totals). | Yes. **No** companion junction table mapping individual DUs to the aggregate they roll up into — that mapping is implicit in CalSim variable naming today. |
| 3 | **Lookup table + FK column on entity** | `hydrologic_region` (7 rows) → `*_entity.hydrologic_region_id`; `source` (12) → `*.source_id`; `model_source` (1) → `*.model_source_id`; `network_type` (21) / `network_subtype` (28) → `network.type_id`/`subtype_ids[]`; `geometry_type`, `network_entity_type`, `statistic_category`, `statistic_type`, `temporal_scale`, `spatial_scale`, `unit`, `variable_type`, `derived_variable_type`, `calsim_model_variable_type`, `assumption_category`, `operation_category`, `env_flow_season`, `slr`. | Yes — this is the most common subset pattern in the DB. |
| 4 | **Tag + tag-link junction** (M:N) | `scenario_tag` (10 rows) + `scenario_tag_link` (109 rows) — free-form labels like `baseline`, `dcr`, `dcp`. | Yes. |
| 5 | **Direct M:N link table** (named relation, no separate "list" lookup) | `theme_scenario_link` (79 rows), `scenario_key_assumption_link` (73), `scenario_key_operation_link` (514), `scenario_hydroclimate_sibling` (27). | Yes. |
| 6 | **Boolean / categorical column on entity** | `channel_entity.has_mif`, `.has_eflows`, `.has_tiers`, `.has_gis_data`, `.is_main`, `.boundary_condition`, `.channel_class`. `du_urban_entity.gw`/`.sw`. `du_agriculture_entity.gw`/`.sw`/`.cs3_type`/`.bank`/`.agency`/`.provider`. `du_refuge_entity.refuge_or_wildlife_area`/`.managed_by`/`.provider`. `reservoir_entity.is_main`/`.has_tiers`/`.operational_purpose`. | Yes — denormalized. Fast to query, but multi-membership / re-grouping is awkward. |
| 7 | **Free-text "registry" column** (no FK) | `du_urban_entity.community_agency`, `du_agriculture_entity.agency`/`.provider`/`.river_reach`/`.demand_unit`, `du_refuge_entity.refuge_or_wildlife_area`/`.managed_by`. `network_arc.river` (459 distinct strings). | Yes but un-normalized. Good candidates for promotion to lookup tables. |
| 8 | **Multi-arc sub-entity** (one entity expanded into N arcs) | `du_urban_delivery_arc` (57 rows for 145 DUs), `mi_contractor_delivery_arc` (39 rows for 30 contractors). | Yes. Used when one entity sums multiple CalSim arcs. |

**Where this maps to recurring user questions:**

| User question | Pattern | Today |
|---|---|---|
| "Which reservoirs are SWP / CVP / major?" | (1) | Live: `reservoir_group` |
| "Which urban DUs are NOD/SOD/SWP-served?" | (1) | Designed but **5 groups empty** in `du_urban_group_member` |
| "Which contractors are SWP M&I vs CVP NOD?" | (1) | Live: `mi_contractor_group` (CVP groups empty because no CVP contractors loaded) |
| "What's the SWP total CWS delivery this scenario?" | (2) | Live: `cws_aggregate_entity.swp_total` |
| "Which ag DUs roll up into SWP PAG South?" | (2) needs companion (1) | **Implicit only** — no junction. Either add `ag_du_aggregate_member` (1) or compute from CalSim variable names at ETL time. |
| "Which urban DUs are in the COEQWAL focal SW list?" | (1) — proposed `cws_list` | **Planned** (`cws_list` + `cws_list_du_member`). |
| "Which channels have MIF?" | (6) | Live: `channel_entity.has_mif` flag. |
| "Which ag DUs are in the SAC region?" | (3) | Live: `du_agriculture_entity.hydrologic_region_id`. |
| "Which ag DUs are 'project-ag'?" | (6) | Implicit via `du_agriculture_entity.cs3_type` (`PA`/`SA`/`NA`/`PR`) — not normalized. |
| "Which ag DUs are CVP vs SWP service area?" | (6) | Implicit via `du_agriculture_entity.provider` text — not normalized. |

#### Per-sector aggregates: tying the DB to the website's Data Explorer

The site's `apps/main/app/features/scenarioExplorer/dataExplorer/` already has the entity-level toggle wired up for CWS (project totals / contractors / DUs) and AG (project totals / DUs / region filter — coded but currently suppressed in the UI). Endpoints used today:

- CWS: `/api/statistics/cws-aggregates`, `/api/statistics/mi-contractors`, `/api/statistics/demand-units`
- AG: `/api/statistics/ag-aggregates`, `/api/statistics/ag-demand-units`
- Refuge: `/api/statistics/refuge-demand-units`
- Reservoir: `/api/statistics/reservoirs`, `/api/statistics/scenarios/{id}/storage-monthly?group=major`
- Channels: `/api/statistics/channels`
- Delta: `/api/statistics/scenarios/{id}/delta/monthly`

**What we can already report per-sector without any new DB work:**

| Sector | Available aggregates | API + table |
|---|---|---|
| CWS — SWP total | `swp_total` | `/cws-aggregates` → `cws_aggregate_entity` |
| CWS — SWP NOD vs SOD | `swp_nod`, `swp_sod` | same |
| CWS — CVP NOD vs SOD | `cvp_nod`, `cvp_sod` | same |
| CWS — MWD | `mwd` | same |
| CWS — per-contractor (30 — all SWP today) | M&I contractor table | `/mi-contractors` → `mi_contractor` |
| CWS — contractor groups (3 populated, 3 empty) | Populated: `swp` (30), `swp_mi` (23), `swp_ag` (7). Empty: `cvp_nod`, `cvp_sod` (no CVP contractors loaded yet), `all_mi` (missed seed step — should equal SWP-MI + CVP-MI when CVP contractors land). | `mi_contractor_group(_member)` — needs API exposure |
| Ag — SWP NOD/SOD/total | `swp_pag`, `swp_pag_n`, `swp_pag_s` | `/ag-aggregates` |
| Ag — CVP NOD/SOD | `cvp_pag_n`, `cvp_pag_s` | same |
| Ag — Settlement / Exchange | `cvp_psc_n`, `cvp_pex_s` | same |
| Ag — total NOD / SOD | `nod_ag`, `sod_ag` (computed) | same |
| Reservoir — major / CVP / SWP / tier | 4 groups, fully populated | `?group=...` parameter on storage endpoints |

**Gaps to fill so the data explorer can show richer per-sector views (do this in three small tranches):**

1. **Backfill the 5 empty `du_urban_group_member` rows.** Once `nod`, `sod`, `swp_served`, `cvp_served`, `swp_delivery_point` have members, the website can offer "Show me only NOD CWS DUs" / "Show me only SWP-served CWS DUs" without any API change beyond exposing `du_urban_group_member` membership on the existing `/demand-units` endpoint.
2. **Add ag DU groups to mirror the urban side.** Create `du_agriculture_group` + `du_agriculture_group_member` with at least: `nod`, `sod`, `swp_served`, `cvp_served`, `cvp_settlement`, `cvp_exchange`, `non_district`, plus per-region groups (`sac`, `sjr`, `tulare`). All can be populated by a one-time SQL pass over the `cs3_type`/`provider`/`hydrologic_region_id` columns already on `du_agriculture_entity`. This unlocks the suppressed Ag region filter in the UI without any frontend change.
3. **Add `cws_list` + `cws_list_du_member` (already in the planned-tables section).** Lets the data explorer expose the focal-SW-DU list, the HHS-allocation list, and the M&I crosswalk list as filters.

After steps 1-3, the data explorer can offer per-sector aggregate views (NOD/SOD, SWP/CVP, M&I/Ag, Project/Contractor/DU, region) directly from the DB without any client-side hardcoding. No new statistics tables are needed — the existing `du_*_monthly` tables can be filtered by membership.

#### Per-layer verification

`database/scripts/sql/02_network/09_verify_level02.sql` is the template. A `09_verify_level03.sql` is on the TODO list (see "Verification gaps" below) and should cover, at minimum:
1. Every entity table has the standard audit columns and the `set_audit_fields` trigger applied.
2. Every entity row resolves to a real `developer.id` for `created_by` / `updated_by`.
3. Every `<domain>_variable.<entity>_id` and every `<domain>_group_member.<entity>_id` resolves to a real entity row (no orphans).
4. Every `<domain>_entity.short_code` is unique and non-empty.
5. Row counts match the expected targets in `database/audit/run_monthly_audit.py` (these targets live in the `EXPECTED_COUNTS` dict).

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

Run as postgres (`$SUPERUSER_URL`). Two SQL steps, then one EC2 step.

```sql
-- (1) Create the PostgreSQL login role + register them in the developer table.
SELECT register_developer(
    'jdoe',                    -- database username (matches their EC2 alias)
    'jdoe@berkeley.edu',       -- email
    'Jane Doe',                -- display name
    'secure_password_here',    -- temp password (they ALTER USER on first login)
    'developer'                -- role: 'admin' or 'developer'
);

-- (2) Add them to the coeqwal_developer group so they inherit RW on every
--     table, including future ones (via the ALTER DEFAULT PRIVILEGES installed
--     by 57_install_coeqwal_developer_role.sql). Skip this and they will hit
--     "permission denied" on tables created after their registration.
GRANT coeqwal_developer TO jdoe;

-- Verify
SELECT * FROM list_developers();
```

> **About the `role` value:** `'admin'` vs `'developer'` is a **placeholder
> label only**. The `developer.role` column has no `CHECK` constraint and is
> not read by any SQL function, RLS policy, or application code today. Actual
> privileges come from three independent things:
>
> 1. The PostgreSQL `LOGIN` role created by `register_developer()`
> 2. Membership in `coeqwal_developer` (granted in step 2 above) -- this is
>    what gates table reads and writes
> 3. Knowledge of the shared `postgres` password used by `$SUPERUSER_URL`
>    for DDL / migrations (exported globally on the Cloud9 EC2)
>
> So setting `role='admin'` today is purely informational. Use it to flag
> developers who are intended to be power users, on the assumption that a
> future role-based check (e.g. `WHERE role='admin'`) may be added. Switching
> between `'admin'` and `'developer'` changes no current behavior.

Then on the Cloud9 EC2, as `ec2-user`, create their personal env file so the
`becomejdoe` alias (already wired into `/home/ec2-user/.bashrc`) points at
their connection string:

```bash
cat > /home/ec2-user/.coeqwal-env-jdoe <<'COEQWAL_ENV_EOF'
export COEQWAL_USER="jdoe"
export DATABASE_URL="postgresql://jdoe:secure_password_here@coeqwal-scenario-database-1.clai4yqcyzxh.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario"
export PS1='\[\033[01;32m\]'"${COEQWAL_USER}"'\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]$(__git_ps1 " (%s)" 2>/dev/null) $ '
COEQWAL_ENV_EOF
chmod 600 /home/ec2-user/.coeqwal-env-jdoe
```

On first login they should rotate the temp password and update their env file:

```bash
becomejdoe
psql $DATABASE_URL -c "ALTER USER jdoe WITH PASSWORD 'their_new_password';"
# then edit /home/ec2-user/.coeqwal-env-jdoe to use the new password

# Sanity check: their writes will land in audit logs as their developer.id, not 1
psql $DATABASE_URL -c "SELECT session_user AS db_role, coeqwal_current_operator() AS developer_id;"
```

**Important:** Unregistered users cannot make database changes. The
`coeqwal_current_operator()` function will raise an exception. Users who are
registered (step 1) but not granted `coeqwal_developer` (step 2) will be able
to connect but will hit "permission denied" on tables created after their
registration.

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

This is distinct from schema structure: the tables can have all the right columns and triggers while still containing incorrect, missing, or stale data. The seed CSVs in `database/seed_tables/` are the source of truth for layers 00–08, with one carve-out: `scenario.is_active` is owned by the live DB after a row's initial bootstrap (see the "Seed CSV `is_active` is bootstrap-only" callout in [Getting started](#getting-started)).

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

CSV validation script for verifying ETL output:

```bash
python etl/batch-container/python-code/validate_csvs.py \
  --ref data/reference/expected.csv \
  --file data/output/actual.csv \
  --out-json validation_summary.json \
  --out-csv validation_mismatches.csv \
  --show-unmatched
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
- **Section 1b "Tables with column mismatches" was a noisy false positive** — the verifier now skips tables whose ERD entry is just a stub (no column tree) and ignores the six implicit standard columns (`id`, `is_active`, `created_at`, `created_by`, `updated_at`, `updated_by`). Anything still flagged in section 1b after the next audit is real drift. The follow-up is to flesh out the ~60 stub tables in `database/schema/COEQWAL_SCENARIOS_DB_ERD.md` so they get column-level checks too — listed separately under "ERD documentation gaps" below.
- **ERD documentation gaps.** Roughly 60 of the ~96 tables in the live DB are documented in stub form in `COEQWAL_SCENARIOS_DB_ERD.md` (just `Table:` / `Records:` / `Columns:` and no column tree). They pass the ERD-DB synchronization check trivially. Filling them in would let `verify_erd_against_audit.py` catch column-level drift for those tables too. Track per-table progress against the latest audit report's section 1b "stub" list.

### Community water systems (CWS) — spring-2026 work

Source data is in `reference/community_water_systems/`. See the entity-pattern section above for the full plan. Sequenced TODOs:

1. **Stage and reconcile new CSVs** under `database/seed_tables/03_entity/cws/` and `database/seed_tables/01_lookup/cws_list/`. Strip the trailing newlines in `DWUC_*` headers and resolve the `ESB355` discrepancy (HHS list contains it but project master does not).
2. **Add the 7 missing project DUs to `du_urban_entity`** (`ACFC`, `KCWA`, `MHILL_NU`, `SBCWD`, `SVWRD`, `TLMNE`, `UNION`) — or document why each one maps to an existing DU under a different `du_id`.
3. **`ALTER TABLE du_urban_entity` to add the 7 new attribute columns** (`is_sw_du`, `is_gw_du`, `largest_system_centroid_lat/lon`, `calsim_centroid_lat/lon`, `hhs_allocation_taf`).
4. **Reload `du_urban_variable`** with the M&I delivery-variable crosswalk for the 75 SW DUs, then **re-run `etl/statistics/du_urban/run_all.py`** for every active scenario (only this ETL module is affected).
5. **Create `cws_entity` (Layer 03)** with one row per PWSID; load the ~476 systems.
6. **Create `cws_du_link` (Layer 03)** as M:N junction `cws_entity` ↔ `du_urban_entity`; load the ~586 system-DU rows.
7. **Create `cws_list` (Layer 01) + `cws_list_du_member` (Layer 03)** to hold the registry of named CWS lists. Initial seed rows: `coeqwal_master_du`, `coeqwal_focal_sw_du`, `coeqwal_focal_gw_du`, `calsim_urban_du`, `tier_matrix`, `hhs_allocation`, `mi_delivery_crosswalk`. Populate `cws_list_du_member` from the reference CSVs.
8. **Backfill the 5 zero-member `du_urban_group` rows** (`nod`, `sod`, `swp_served`, `cvp_served`, `swp_delivery_point`) so the existing per-DU SWP/CVP/NOD/SOD memberships are queryable. (These are the per-DU twin of the project-level rollups already in `cws_aggregate_entity`.)
9. **Mirror the group pattern on the ag side** — create `du_agriculture_group` + `du_agriculture_group_member` and populate `nod`, `sod`, `swp_served`, `cvp_served`, `cvp_settlement`, `cvp_exchange`, `non_district`, plus per-region groups (`sac`, `sjr`, `tulare`) from the existing `cs3_type` / `provider` / `hydrologic_region_id` columns on `du_agriculture_entity`. Unlocks the suppressed Ag region filter in the website's Data Explorer without frontend changes.
10. **Expose group-membership in the `/demand-units` and `/ag-demand-units` API responses** so the website can filter by membership without re-issuing per-group queries. (One join in the existing FastAPI route handlers.)
11. **Add `09_verify_level03.sql`** — verify `cws_*` integrity, `du_urban_entity ↔ cws_du_link ↔ cws_entity` referential integrity, and that every `cws_list` and `du_*_group` row has at least one member.
12. **Re-run `database/audit/run_monthly_audit.py` and `verify_erd_against_audit.py`** to confirm zero drift.

### Developer access and authentication

- **SSO user attribution** — currently developers connect to the database using a named PostgreSQL role (e.g. `jfantauzza`). The long-term goal is to use AWS SSO identity for authentication so that the `aws_sso_username` field in the `developer` table is used automatically, without requiring a separate PostgreSQL password per developer.
- **Role-based table permissions** — shipped in [`database/scripts/sql/57_install_coeqwal_developer_role.sql`](scripts/sql/57_install_coeqwal_developer_role.sql). The `coeqwal_developer` group role holds `SELECT, INSERT, UPDATE, DELETE` on every table in `public`, and `ALTER DEFAULT PRIVILEGES FOR ROLE postgres` makes that grant auto-extend to any future table created via `$SUPERUSER_URL`. New developers get RW on everything via `GRANT coeqwal_developer TO <username>` (see "Setting up a new developer" above). This closed the permission-gap class of bugs surfaced by the `variable_type` issue during auditing.

### Review indices and compare to API