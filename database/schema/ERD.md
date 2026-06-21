# COEQWAL Scenarios Database ERD

To see the current state of the live database, run the monthly audit tool:

```bash
python database/audit/run_monthly_audit.py
```

It produces a timestamped folder under `audits/` with the schema snapshot, per-table row counts, layer-by-layer CSV exports, ETL coverage, and database health checks.

This schema was set up, presented to the team, and reviewed in June/July 2025. Database development continued sporadically, often interrupted for front-end tasks. Project data was evolving while database was evolving, leading to some broken threads.

This version includes notes for developers.

## Layer scheme

Conceptual layer scheme to organize the tables. Two top-level categories:

- **Reference (layers 00-09):** things the COEQWAL world consists of. Entities, lookups, definitions, link tables, tier rubrics. Largely loaded from seed files, with updates.
- **Results (layers 10-12):** populated by the ETL pipeline and by team-curated loads. L10 holds the team-curated tier rollups, L11 holds the per-scenario statistics computed from CalSim output, and L12 holds rollups that aggregate ACROSS scenarios.

**Schema layer conventions:**

- Layers typically depend on layers that come earlier in the sequence.
- Link tables (M:N junctions, foreign-key bridges) that join two layers live in the higher of the two layers. For example, `scenario_key_assumption_link` joins `scenario` (L06) and `assumption_definition` (L05). It sits in L06.
- Most joins go from a higher-layer table down to its lower-layer attributes (e.g. `scenario JOIN scenario_key_operation_link JOIN operation_definition`).

### Layers

```
00  VERSIONING (audit + versioning infrastructure)
    version_family, version, developer, domain_family_map

01  LOOKUP (shared lookups and code lists)
    hydrologic_region, source, model_source, unit, spatial_scale, temporal_scale,
    statistic_category, statistic_type, geometry_type, network_type,
    network_subtype, network_entity_type, watershed, env_flow_season

02  NETWORK (CalSim topology)
    network, network_arc, network_node, network_gis

03  ENTITY (operational entities)
    reservoir, reservoir_entity, reservoir_group, reservoir_group_member
    channel_entity
    du_agriculture_entity, du_refuge_entity, du_urban_entity,
        du_urban_delivery_arc, du_urban_group, du_urban_group_member
    mi_contractor, mi_contractor_delivery_arc,
        mi_contractor_group, mi_contractor_group_member
    ag_aggregate_entity, cws_aggregate_entity
    compliance_station, wba

04  VARIABLE (CalSim variable definitions and type classifications)
    calsim_model_variable_type, derived_variable_type, variable_type
    channel_variable, du_urban_variable
    (planned) variable, delta_variable, reservoir_variable, inflow_variable

05  ASSUMPTIONS + OPERATIONS (scenario configuration dimensions)
    assumption_category, assumption_definition
    operation_category, operation_definition

06  SCENARIO (scenario definitions + linking tables)
    scenario, scenario_hydroclimate_sibling, scenario_author
    scenario_key_assumption_link, scenario_key_operation_link
    scenario_tag, scenario_tag_link

07  HYDROCLIMATE (hydrology + sea-level-rise scenarios)
    hydroclimate, slr

08  THEME (research themes)
    theme, theme_scenario_link

09  TIER (tier rubric: definitions and location membership)
    tier_definition, tier_location

10  TIER RESULTS (per-scenario tier rollups, loaded from team-curated CSVs)
    tier_result, tier_location_result

11  PER-SCENARIO STATISTICS (statistics computed from CalSim DV/SV output)
    reservoir_storage_monthly, reservoir_spill_monthly,
        reservoir_monthly_percentile, reservoir_period_summary
    delta_monthly, delta_period_summary
    du_delivery_monthly, du_shortage_monthly, du_period_summary
    mi_delivery_monthly, mi_shortage_monthly, mi_contractor_period_summary
    cws_aggregate_monthly, cws_aggregate_period_summary
    ag_du_demand_monthly, ag_du_sw_delivery_monthly, ag_du_gw_pumping_monthly,
        ag_du_shortage_monthly, ag_du_period_summary
    ag_aggregate_monthly, ag_aggregate_period_summary
    refuge_du_delivery_monthly, refuge_du_shortage_monthly,
        refuge_du_period_summary
    env_flow_channel_monthly, env_flow_channel_seasonal,
        env_flow_channel_period_summary

12  CROSS-SCENARIO ANALYSIS (rollups across scenarios, computed from L11)
    sensitivity_climate, sensitivity_operational
```

**Coverage:** This ERD documents 96 tables (including one extension-managed table, `spatial_ref_sys`) plus 4 planned tables. `scenario_backup` is slated for removal and not included. Planned tables use the same column-table format with a `[PLANNED]` label and a paragraph explaining intent.

---

## Contents

- [Triggers](#triggers)
- [Layer 00 - VERSIONING](#layer-00---versioning)
- [Layer 01 - LOOKUP](#layer-01---lookup)
- [Layer 02 - NETWORK](#layer-02---network)
- [Layer 03 - ENTITY](#layer-03---entity)
- [Layer 04 - VARIABLE](#layer-04---variable)
- [Layer 05 - ASSUMPTIONS + OPERATIONS](#layer-05---assumptions--operations)
- [Layer 06 - SCENARIO](#layer-06---scenario)
- [Layer 07 - HYDROCLIMATE](#layer-07---hydroclimate)
- [Layer 08 - THEME](#layer-08---theme)
- [Layer 09 - TIER](#layer-09---tier)
- [Layer 10 - TIER RESULTS](#layer-10---tier-results)
- [Layer 11 - PER-SCENARIO STATISTICS](#layer-11---per-scenario-statistics)
- [Layer 12 - CROSS-SCENARIO ANALYSIS](#layer-12---cross-scenario-analysis)
- [Database functions](#database-functions)
- [Extension-managed tables](#extension-managed-tables)

---

## Triggers

The database has one application-level trigger function in active use, plus a second one that is defined but not attached.

### `set_audit_fields` (BEFORE INSERT OR UPDATE, row-level)

**Status:** active. Attached to 91 of the 93 tables that carry the audit columns (`created_at`, `updated_at`, `created_by`, `updated_by`). The two current gaps are the obsolete `scenario_backup` (can be dropped by `database/scripts/sql/audit_cleanup.sql` § 1) and `tier_location` (relatively new table that I forgot to attach the trigger to).

Fires on every row-level INSERT or UPDATE, including UPSERTs, `INSERT ... SELECT`, `COPY`, and `MERGE`. Reads the current developer via `coeqwal_current_operator()` and writes it to the audit columns.

**Known issue:** ETL pipelines connect via the shared postgres role. When `session_user = 'postgres'`, `coeqwal_current_operator()` returns `1` (System). That is correct for DDL migrations run via `$SUPERUSER_URL`, but for DML it means any write performed as `postgres` attributes to System and the operator is lost. See `database/README.md` § "Connecting as yourself" for how to switch to your own registered role. Resolution strategies are detailed in `database/VERSIONING.md`.

---

## Layer 00 - VERSIONING

The versioning layer tracks:

- Which domain tables belong to which "version family" (e.g. "theme", "scenario", "network", "entity").
- The active version per family.
- The developer registry that anchors the audit-attribution convention (`created_by` / `updated_by` FKs to `developer.id`).

**Status:** in-progress. Non-blocking for current operations, but the subsystem needs further development to be useful. Tracked in `database/SCHEMA_BACKLOG.md` § 9. The full discussion lives in `database/VERSIONING.md`.

**Status details:**

- **Developer registry and audit attribution:** `coeqwal_current_operator()` resolves `session_user` to a `developer.id`, and the `set_audit_fields` trigger uses it to populate `created_by` / `updated_by` on 91 of the 93 audit-bearing tables (see § Triggers above for the two gaps).
- **Version catalog seeded:** All 14 families have an initial row in `version` at `1.0.0` with `is_active = true`.
- **`*_version_id` coverage:** Domain tables that participate in row-level version tracking carry a `<family>_version_id` FK to `version.id`. `domain_family_map` records family membership for every domain table. See `database/SCHEMA_BACKLOG.md` § 9 for which families and tables carry the column.

**What could be developed:**

- **The version bump workflow has never been exercised:** No family has been bumped past 1.0.0. The intended workflow (insert new `version` row, flip `is_active` on the old one, load new domain rows with the new `version.id`, update consumers to use `get_active_version('<family>')`) is documented but not exercised end-to-end. There is no DB-level constraint enforcing one-active-per-family.
- **`VersioningManager` is not wired up:** `database/utils/versioning_utils.py` defines two Python classes (`VersioningManager` and `TableManager`) that resolve the active version for a given table. Neither is imported by any other code in the repository. Reference scaffolding for now (see `database/VERSIONING.md` for the design discussion).

### Table schema

#### `version_family`

The catalog of version families. Each family is a logical group of domain tables that version together.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('version_family_id_seq')` | PK |
| `short_code` | text | NO |  | unique, e.g. `scenario`, `network`, `theme` |
| `label` | text | YES |  | display name |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `version_family_pkey` (id) [PRIMARY]
- `version_family_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `version_family_short_code_key` (short_code)

**Values:**

Only two families touch result tables: `statistics` (results-only) and `tier` (mixed: rubric + results, sharing one version). All others are attribute / lookup / link tables.

| short_code | label | description |
|---|---|---|
| `theme` | Theme | Research themes and storylines |
| `scenario` | Scenario | Water management scenarios |
| `assumption` | Assumption | Scenario assumptions and parameters |
| `operation` | Operation | Operational policies and rules |
| `hydroclimate` | Hydroclimate | Hydroclimate conditions and projections |
| `variable` | Variable | CalSim model variables and definitions |
| `statistics` | Statistics | Outcome categories and measurement systems |
| `tier` | Tier | Tier definitions and classification systems |
| `geospatial` | Geospatial | Geographic and spatial data definitions |
| `interpretive` | Interpretive | Analysis and interpretive frameworks |
| `metadata` | Metadata | Data metadata and documentation |
| `network` | Network | CalSim network topology and connectivity |
| `entity` | Entity | Entity version family for tracking entity data versions |
| `audit` | Audit | Layer 00 system tables: versioning, developer registry, domain mapping, audit log |

#### `version`

A version instance under a family.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('version_id_seq')` | PK |
| `version_family_id` | integer | NO |  | FK `version_family.id` |
| `version_number` | text | YES |  |  |
| `changelog` | text | YES |  |  |
| `is_active` | boolean | NO | `false` | |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `version_family_id` -> `version_family.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `version_pkey` (id) [PRIMARY]
- `version_version_family_id_version_number_key` (version_family_id, version_number) [UNIQUE]

**Unique constraints:**

- `version_version_family_id_version_number_key` (version_family_id, version_number)

The composite unique index doubles as the `version_family_id` lookup path (left-prefix usable).

`is_active` defaults to `false`. New version rows must be explicitly opted in. The "one active version per family" rule is convention, not enforced by the DB.

#### `domain_family_map`

Maps every tracked domain table to its `version_family`. The PK is the `(schema_name, table_name)` pair, so each table appears at most once. One known gap: `tier_location` is not yet mapped (tracked in `database/SCHEMA_BACKLOG.md` § 9).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `schema_name` | text | NO | `'public'` | part of PK |
| `table_name` | text | NO |  | part of PK |
| `version_family_id` | integer | NO |  | FK `version_family.id` |
| `note` | text | YES |  |  |
| `database_level` | varchar | YES |  | two-digit layer code. **not up-to-date** Updated layer bucketing for audit reports lives in the `LAYERS` dict in `database/audit/run_monthly_audit.py`, but right now all of this is too manual and probably not worth it. Existing values are stale and disagree with the ERD's current layer scheme |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `version_family_id` -> `version_family.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `domain_family_map_new_pkey` (schema_name, table_name) [PRIMARY]
- `idx_domain_family_map_version_family` (version_family_id)

#### `developer`

Developer registry. Domain rows' `created_by` / `updated_by` columns FK here. FK coverage is incomplete in the live DB. `audit_cleanup.sql` § 9 completes it by adding the `created_by` / `updated_by` -> `developer.id` These adds are data-dependent. Each column is pre-checked for integer type and for orphan values (any non-NULL audit id not present in `developer.id`), and a column failing either check is skipped and reported rather than erroring the run, so the section can land partially. The "intended FK `developer.id`, not enforced" notes throughout this ERD mark columns still awaiting this step. `id = 1` is the system bootstrap account, used when DDL is applied as the shared `postgres` superuser. Other rows are individual developers added by direct `INSERT`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('developer_id_seq')` | PK |
| `email` | text | YES |  | unique |
| `name` | text | YES |  | legacy, prefer `display_name` |
| `display_name` | text | NO |  |  |
| `affiliation` | text | YES |  |  |
| `role` | text | YES |  | `admin`, `user`, `system` |
| `aws_sso_user_id` | text | YES |  | unique, unused |
| `aws_sso_username` | text | YES |  | unique, resolved against `session_user` |
| `is_bootstrap` | boolean | NO | `false` | true for the two bootstrap rows (`id = 1` System, `id = 2` lead-dev) |
| `sync_source` | text | YES | `'manual'` | `manual`, `sso`, `seed` |
| `is_active` | boolean | NO | `true` |  |
| `last_login` | timestamptz | YES |  |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `updated_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | YES |  | FK `developer.id` |
| `updated_by` | integer | YES |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `developer_pkey` (id) [PRIMARY]
- `developer_aws_sso_user_id_key` (aws_sso_user_id) [UNIQUE]
- `developer_aws_sso_username_key` (aws_sso_username) [UNIQUE]
- `developer_email_key` (email) [UNIQUE]

**Unique constraints:**

- `developer_aws_sso_user_id_key` (aws_sso_user_id)
- `developer_aws_sso_username_key` (aws_sso_username)
- `developer_email_key` (email)

---

## Layer 01 - LOOKUP

Shared reference data and code lists: `hydrologic_region`, `source`, `model_source`, `unit`, the classification scales (`spatial_scale`, `temporal_scale`, `statistic_category`, `statistic_type`, `geometry_type`), the network catalogs (`network_type`, `network_subtype`, `network_entity_type`), `watershed`, and `env_flow_season`. All 14 tables are small and stable, loaded once at bootstrap and rarely written to after that. Every lookup table carries the standard audit columns and FKs to `developer.id`.

Layer 01 tables play two roles. Some are **catalogs**, useful as reference documentation of a controlled vocabulary that a researcher, analyst or developer can consult, with no FK enforcement needed. Some are **keys**, referenced by FK from other layers. Not every lookup must be a key.

Lookup linkage is uneven by layer. Layer 02 (network) has high coverage: arcs, nodes, types, subtypes, regions, sources, and model sources are all wired up to the appropriate Layer 01 catalog. Layer 03 (entity) is mixed. Layers 10-12 (results) less so. A nice intermediate-stage task is to take one layer at a time and bring its lookup linkage up to a uniform standard, deciding per-lookup along the way whether the lookup is a key or stays as an informational catalog. See `database/SCHEMA_BACKLOG.md` § 8.

Tables whose data originated from a specific external source (geopackage, NHD, CalSim report) carry a `source_id` FK to `source.id`.

### Table schema

#### `hydrologic_region`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('hydrologic_region_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `label` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `hydrologic_region_pkey` (id) [PRIMARY]
- `hydrologic_region_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `hydrologic_region_short_code_key` (short_code)

**Values:**

| short_code | label |
|---|---|
| `SAC` | Sacramento River Basin |
| `SJR` | San Joaquin River Basin |
| `DELTA` | Sacramento-San Joaquin Delta |
| `TULARE` | Tulare Basin |
| `SOCAL` | Southern California |
| `NC` | North Coast |
| `EXPORT` | Export region |

#### `source`

Catalog of data sources.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('source_id_seq')` | PK |
| `source` | text | NO |  | unique short identifier |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `source_pkey` (id) [PRIMARY]
- `source_source_key` (source) [UNIQUE]

**Unique constraints:**

- `source_source_key` (source)

**Values:**

| source | description |
|---|---|
| `calsim_report` | CalSim-3 report `final.pdf` |
| `james_gilbert` | James Gilbert |
| `calsim_variables` | CalSim variables from output and SV data |
| `geopackage` | `CalSim3_GeoSchematic_20221227_COEQWAL_Revisions2024_corrected.gpkg` |
| `trend_report` | Variables extracted from Gilbert team trend reports |
| `metadata` | Scenario metadata |
| `cvm_docs` | Central Valley Model documentation |
| `network_schematic` | Network schematic |
| `manual` | Manual insertion |
| `NHD` | National Hydrography Dataset |
| `DWR_CDEC` | DWR California Data Exchange Center |
| `wietske_medema` | Wietske Medema |

#### `model_source`

Catalog of upstream model providers. Currently a single row for CalSim-3. Designed to support future model integrations.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('model_source_id_seq')` | PK |
| `short_code` | text | NO |  | unique, e.g. `calsim3` |
| `name` | text | NO |  | unique, e.g. `CalSim3` |
| `description` | text | YES |  |  |
| `contact` | text | YES |  |  |
| `notes` | text | YES |  |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `model_source_pkey` (id) [PRIMARY]
- `model_source_name_key` (name) [UNIQUE]
- `model_source_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `model_source_name_key` (name)
- `model_source_short_code_key` (short_code)

**Values:**

| short_code | name | description |
|---|---|---|
| `calsim3` | CalSim3 | California Central Valley water system allocation simulation model |

#### `unit`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('unit_id_seq')` | PK |
| `short_code` | text | NO |  | unique, e.g. `TAF`, `CFS`, `acres` |
| `full_name` | text | YES |  | e.g. `thousand acre-feet` |
| `canonical_group` | text | YES |  | e.g. `volume`, `flow`, `area` |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `unit_pkey` (id) [PRIMARY]
- `unit_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `unit_short_code_key` (short_code)

**Values:**

| short_code | full_name | canonical_group |
|---|---|---|
| `TAF` | thousand acre-feet | volume |
| `CFS` | cubic feet per second | flow |
| `acres` | acres | area |
| `mm` | millimeters | length |
| `km` | kilometers | length |

**Status:** Catalog covers TAF, CFS, acres, mm, km. Missing `umhos_cm` (microsiemens per centimeter), the unit for the EC salinity variables that `etl/statistics/delta/calculate_delta_statistics.py` reads (Emmaton, Jersey Point, Rock Slough, Collinsville, Banks, Tracy). Tracked in `database/README.md` § Roadmap > "Layer 04 variable layer".

#### `spatial_scale`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('spatial_scale_id_seq')` | PK |
| `short_code` | text | NO |  | unique, e.g. `system_wide`, `regional`, `basin` |
| `label` | text | YES |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `spatial_scale_pkey` (id) [PRIMARY]
- `spatial_scale_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `spatial_scale_short_code_key` (short_code)

#### `temporal_scale`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('temporal_scale_id_seq')` | PK |
| `short_code` | text | NO |  | unique, e.g. `daily`, `monthly`, `seasonal` |
| `label` | text | NO |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `temporal_scale_pkey` (id) [PRIMARY]
- `temporal_scale_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `temporal_scale_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `daily` | Daily | Daily |
| `weekly` | Weekly | Weekly |
| `monthly` | Monthly | Monthly |
| `seasonal` | Seasonal | Seasonal |
| `flow_seasonal` | Flow-seasonal | Flow-based seasonal periods |
| `annual` | Annual | Annual |
| `por` | Period of record | Period of record |
| `long_term` | Long-term | Long-term planning horizon |

#### `statistic_category`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('statistic_category_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `label` | text | NO |  |  |
| `description` | text | YES |  |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `statistic_category_pkey` (id) [PRIMARY]
- `statistic_category_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `statistic_category_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `summary` | Summary | Aggregate summary statistics (mean, median, min, max, cv, stdev) |
| `percentile_band` | Percentile Band | Standard quantile bands aligned with DWR water year types |
| `exceedance` | Exceedance | Exceedance percentiles for flow duration and reliability analysis |

**Status:** The `percentile_band` description ("aligned with DWR water year types") states the intended goal, not the current implementation. The ETL computes evenly spaced quantiles `[0, 10, 30, 50, 70, 90, 100]` and the band labels borrow water-year terminology (dry / below normal / above normal / wet). The DWR wet/dry water-year thresholds the bands are meant to map to still need to be researched and reconciled with these breakpoints. See the matching Status note under `statistic_type`.

#### `statistic_type`

Registry of statistics the system produces.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('statistic_type_id_seq')` | PK |
| `short_code` | text | NO |  | unique, e.g. `MEAN`, `Q90`, `EXC_P90` |
| `label` | text | NO |  |  |
| `description` | text | YES |  |  |
| `statistic_category_id` | integer | NO |  | FK `statistic_category.id` |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `statistic_category_id` -> `statistic_category.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `statistic_type_new_pkey` (id) [PRIMARY]
- `statistic_type_new_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `statistic_type_new_short_code_key` (short_code)

**Values:**

*Summary (statistic_category_id = 1, 6 rows):* `MEAN`, `MEDIAN`, `MIN`, `MAX`, `CV` (coefficient of variation, stdev/mean), `STDEV` (standard deviation).

*Percentile Band (statistic_category_id = 2, 7 rows):* `Q0`, `Q10`, `Q30`, `Q50`, `Q70`, `Q90`, `Q100`. Breakpoints `[0, 10, 30, 50, 70, 90, 100]` are intended to align with water management conventions, but this need verification and may need refinement.

*Exceedance (statistic_category_id = 3, 7 rows):* `EXC_P5`, `EXC_P10`, `EXC_P25`, `EXC_P50`, `EXC_P75`, `EXC_P90`, `EXC_P95`. Breakpoints `[5, 10, 25, 50, 75, 90, 95]` follow standard hydrologic convention. `EXC_P(n) =` "value exceeded n% of the time". `EXC_P90` is a LOW value (dry). In other words, `EXC_P90` = the flow/value that is exceeded 90% of the time, so that is a relatively low number, so represents low flow/dry years. Relationship: `EXC_P(n) = Q(100 - n)`, so `EXC_P90 = Q10`. The band and exceedance values are independently computed by the ETL (different breakpoint sets), so most exceedance values cannot be derived from band percentiles.

**Status:** The Percentile Band breakpoints are intended to align with DWR water year type thresholds (wet / above normal / below normal / dry), and the band labels reflect that goal (`Q10` dry, `Q30` below normal, `Q70` above normal, `Q90` wet). As implemented they are quantiles of the distribution across water years (`STORAGE_PERCENTILES` in `etl/statistics/reservoirs/calculate_reservoir_statistics.py`), independent of any official year-type classification. The actual DWR wet/dry year thresholds the breakpoints should map to still need to be researched and reconciled with these breakpoints. The same goal applies to the `statistic_category.percentile_band` description.

#### `geometry_type`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('geometry_type_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `label` | text | YES |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | YES |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | YES |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `geometry_type_pkey` (id) [PRIMARY]
- `geometry_type_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `geometry_type_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `POINT` | Point | Single coordinate |
| `LINESTRING` | LineString | Linear feature |
| `POLYGON` | Polygon | Area polygon |
| `MULTIPOLYGON` | MultiPolygon | Multiple polygons |

#### `network_entity_type`

The top of the three-level network classification hierarchy. Distinguishes arcs from nodes. Used as the parent FK for `network_type` and directly stamped on every `network` row via `entity_type_id`. See database/README.md § "02_NETWORK: network topology" for the type-and-subtype hierarchy table.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('network_entity_type_id_seq')` | PK |
| `short_code` | text | NO |  | unique, one of `arc`, `node`, `null`, `unimpaired_flows` |
| `label` | text | NO |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE): duplicate of the constraint below, clean up
- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE): duplicate of the constraint below, clean up
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `network_entity_type_pkey` (id) [PRIMARY]
- `network_entity_type_short_code_key` (short_code) [UNIQUE]
- `idx_network_entity_type_active` (is_active, short_code)
- `idx_network_entity_type_short_code` (short_code): duplicates the unique index's left-prefix, cleanup

**Unique constraints:**

- `network_entity_type_short_code_key` (short_code)

The duplicate FK constraints on `created_by` / `updated_by` (one RESTRICT/CASCADE, one NO ACTION/NO ACTION) and the redundant `idx_network_entity_type_short_code` are schema-cleanup candidates.

#### `network_type`

The middle of the three-level network classification hierarchy. Subdivides arcs and nodes into specific types. See database/README.md § "02_NETWORK: network topology" for the type-and-subtype hierarchy table.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('network_type_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique within `network_entity_type_id` |
| `label` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `network_entity_type_id` | integer | NO |  | FK `network_entity_type.id` |
| `model_source_id` | integer | YES | `1` | FK `model_source.id` |
| `source_id` | integer | YES | `4` | FK `source.id` |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `model_source_id` -> `model_source.id` (delete: RESTRICT, update: CASCADE)
- `network_entity_type_id` -> `network_entity_type.id` (delete: RESTRICT, update: CASCADE)
- `source_id` -> `source.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `network_type_pkey` (id) [PRIMARY]
- `network_type_short_code_entity_key` (short_code, network_entity_type_id) [UNIQUE] - duplicate of the row below, drop it
- `network_type_short_code_network_entity_type_id_key` (short_code, network_entity_type_id) [UNIQUE]
- `idx_network_type_active` (is_active)
- `idx_network_type_entity_type` (network_entity_type_id)

**Unique constraints:**

- `network_type_short_code_network_entity_type_id_key` (short_code, network_entity_type_id)

The two unique indexes on `(short_code, network_entity_type_id)` are duplicates, cleanup candidate.

#### `network_subtype`

The most detailed of the three-level network classification hierarchy. Further partitions select `network_type`s. Most types have no subtypes. See database/README.md § "02_NETWORK: network topology" for the type-and-subtype hierarchy table.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('network_subtype_id_seq')` | PK |
| `short_code` | varchar | NO |  |  |
| `label` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `type_id` | integer | NO |  | FK `network_type.id` |
| `model_source_id` | integer | YES | `1` | FK `model_source.id` |
| `source_id` | integer | YES | `4` | FK `source.id` |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `model_source_id` -> `model_source.id` (delete: RESTRICT, update: CASCADE)
- `source_id` -> `source.id` (delete: RESTRICT, update: CASCADE)
- `type_id` -> `network_type.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `network_subtype_pkey` (id) [PRIMARY]
- `idx_network_subtype_active` (is_active)
- `idx_network_subtype_short_code` (short_code)
- `idx_network_subtype_type` (type_id)

#### `watershed`

The watersheds used as the channel-entity classification anchor. The Sacramento mainstem is split at Bend Bridge (rm 257) into `SAC_UPPER` and `SAC_LOWER`. Different from hydrologic region, whose membership is via `hydrologic_region_id` FK.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('watershed_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique |
| `name` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `hydrologic_region_id` | integer | YES |  | FK `hydrologic_region.id` |
| `unimp_sv_variable` | varchar | YES |  | CalSim SV `UNIMP_*` variable, NULL when no SV reference exists |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `hydrologic_region_id` -> `hydrologic_region.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `watershed_new_pkey` (id) [PRIMARY]
- `watershed_new_short_code_key` (short_code) [UNIQUE]
- `idx_watershed_hydrologic_region` (hydrologic_region_id)

**Unique constraints:**

- `watershed_new_short_code_key` (short_code)

Referenced by `channel_entity.watershed_short_code` -> `watershed.short_code` (Layer 03).

**Values:**

| short_code | region | UNIMP_SV variable |
|---|---|---|
| `BEAR_RIVER` | SJR | (none) |
| `CLEAR_CREEK` | SAC | `UNIMP_WH` |
| `SAC_UPPER` | SAC | `UNIMP_SHAS` |
| `SAC_LOWER` | SAC | `UNIMP_SRBB` |
| `SAN_JOAQUIN` | SJR | `UNIMP_SJ` |
| `TRINITY_RIVER` | NC | `UNIMP_TRIN` |
| `UPPER_AMERICAN` | SJR | `UNIMP_FOLS` |
| `UPPER_FEATHER` | SJR | `UNIMP_OROV` |
| `UPPER_MERCED` | SJR | `UNIMP_ME` |
| `UPPER_MOKELUMNE` | SJR | (none) |
| `UPPER_STANISLAUS` | SJR | `UNIMP_ST` |
| `UPPER_TUOLUMNE` | SJR | `UNIMP_TU` |
| `YUBA_RIVER` | SJR | `UNIMP_YUBA` |

Sacramento mainstem channels at or below rm 257 use `SAC_LOWER` / `UNIMP_SRBB`. Channels above (`SAC289`, `KSWCK`, `SHSTA`) use `SAC_UPPER` / `UNIMP_SHAS`. `UPPER_MOKELUMNE` has no `UNIMP_MOK` in CalSim SV. The % unimpaired metric is not computable for MOK reaches without a natural flow reference.

**Status:** The `region` column reflects the current live DB. Four rows are mislabeled `SJR` and should be `SAC`: `BEAR_RIVER`, `UPPER_AMERICAN`, `UPPER_FEATHER`, and `YUBA_RIVER` are all Sacramento-basin drainages. The `channel_entity`'s per-channel region tags both place these in `SAC`, so the live values drifted from manual edits. `UPPER_MOKELUMNE` is `SJR` in both seed and live (eastside-Delta tributary) and is not part of the drift. Tracked in `database/README.md` § Roadmap > "Correctness bugs (do regardless)". The corrective one-shot:

```sql
UPDATE watershed SET hydrologic_region_id = 1
WHERE short_code IN ('BEAR_RIVER', 'UPPER_AMERICAN', 'UPPER_FEATHER', 'YUBA_RIVER');
```

#### `env_flow_season`

Water-year season catalog used by env-flow seasonal results.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('env_flow_season_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique |
| `name` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `wy_months` | ARRAY | NO |  | smallint[] of WY month numbers |
| `calendar_months` | text | NO |  | human-readable label |
| `sort_order` | smallint | NO |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `updated_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `env_flow_season_pkey` (id) [PRIMARY]
- `env_flow_season_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `env_flow_season_short_code_key` (short_code)

**Values (ordered by `sort_order`):**

| short_code | name | wy_months | calendar_months |
|---|---|---|---|
| `wet_peak` | Wet Season Peak | {3,4,5} | December, January, February |
| `wet_base` | Wet Season Base | {6,7} | March, April |
| `spring_recession` | Spring Recession | {8,9} | May, June |
| `dry` | Dry Season | {10,11,12,1} | July, August, September, October |
| `fall_pulse` | Fall Pulse | {2} | November |

`wy_months` are water-year month numbers (1 = October), which is why `wet_peak = {3,4,5}` maps to December-February.

**Status:** These values need review by a domain expert before being treated as authoritative. They were authored to drive the env-flow seasonal results and have not been validated against an agreed hydrologic / ecological definition of the water-year seasons.

Referenced by `env_flow_channel_seasonal.season_id` -> `env_flow_season.id` (Layer 11).

---

## Layer 02 - NETWORK

The CalSim3 water infrastructure as a directed graph. `network` is the master element registry. `network_arc` and `network_node` carry arc-specific and node-specific attributes and both have `source_id` and `model_source_id` FKs (geopackage / CalSim3 model). `network_gis` holds PostGIS geometry and has `source_id` only. `network` itself has neither.

### Table schema

#### `network`

The master element registry. Every CalSim infrastructure element has exactly one row here (hopefully, if we got them all), identified by `short_code`. `entity_type_id` distinguishes arcs from nodes. `network_arc` / `network_node` / `network_gis` all reference this table.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('network_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique CalSim code, e.g. `C_SAC296`, `D_FOLSM` |
| `name` | varchar | YES |  |  |
| `description` | text | YES |  |  |
| `comment` | text | YES |  |  |
| `entity_type_id` | integer | NO |  | FK `network_entity_type.id` |
| `type_id` | integer | YES |  | FK `network_type.id` |
| `subtype_ids` | ARRAY | YES |  | integer[] of `network_subtype.id` values |
| `model_list` | ARRAY | NO |  | integer[] of `model_source.id` values |
| `source_list` | ARRAY | NO |  | integer[] of `source.id` values |
| `has_gis` | boolean | YES | `false` | TRUE when a `network_gis` row exists |
| `hydrologic_region_id` | integer | YES |  | FK `hydrologic_region.id` |
| `riv_sys` | varchar | YES |  | river-system code |
| `strm_code` | varchar | YES |  | stream code |
| `network_version_id` | integer | NO |  | FK `version.id` |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `entity_type_id` -> `network_entity_type.id` (delete: RESTRICT, update: CASCADE)
- `hydrologic_region_id` -> `hydrologic_region.id` (delete: RESTRICT, update: CASCADE)
- `network_version_id` -> `version.id` (delete: RESTRICT, update: CASCADE)
- `type_id` -> `network_type.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `network_pkey` (id) [PRIMARY]
- `network_short_code_key` (short_code) [UNIQUE]
- `idx_network_entity_type` (entity_type_id)
- `idx_network_has_gis` (has_gis)
- `idx_network_hydrologic_region` (hydrologic_region_id)
- `idx_network_model_list` (model_list): GIN, array containment
- `idx_network_source_list` (source_list): GIN, array containment
- `idx_network_strm_code` (strm_code)
- `idx_network_type` (type_id)
- `idx_network_version` (network_version_id)

**Unique constraints:**

- `network_short_code_key` (short_code)

#### `network_arc`

Arc-specific attributes. Each row references its parent `network` row via `network_id`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('network_arc_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique, matches `network.short_code` |
| `network_id` | integer | NO |  | FK `network.id`, ON DELETE CASCADE |
| `river` | varchar | YES |  | CalSim waterway code, e.g. `SAC`, `SJR`, `DMC` |
| `from_node` | varchar | YES |  | upstream node `short_code` |
| `to_node` | varchar | YES |  | downstream node `short_code` |
| `shape_length_m` | numeric | YES |  | arc length in meters |
| `model_source_id` | integer | YES | `1` | FK `model_source.id` |
| `source_id` | integer | YES | `4` | FK `source.id` |
| `network_version_id` | integer | NO |  | FK `version.id` |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `model_source_id` -> `model_source.id` (delete: RESTRICT, update: CASCADE)
- `network_id` -> `network.id` (delete: CASCADE, update: CASCADE)
- `network_version_id` -> `version.id` (delete: RESTRICT, update: CASCADE)
- `source_id` -> `source.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `network_arc_pkey` (id) [PRIMARY]
- `network_arc_short_code_key` (short_code) [UNIQUE]
- `idx_network_arc_connectivity` (from_node, to_node): topology traversal
- `idx_network_arc_from_node` (from_node)
- `idx_network_arc_network_id` (network_id)
- `idx_network_arc_river` (river)
- `idx_network_arc_to_node` (to_node)
- `idx_network_arc_version` (network_version_id)

**Unique constraints:**

- `network_arc_short_code_key` (short_code)

#### `network_node`

Node-specific attributes.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('network_node_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique, matches `network.short_code` |
| `network_id` | integer | NO |  | FK `network.id`, ON DELETE CASCADE |
| `riv_mi` | numeric | YES |  | river mile |
| `c2vsim_gw` | varchar | YES |  | C2VSim groundwater cell link |
| `c2vsim_sw` | varchar | YES |  | C2VSim surface-water subregion link |
| `nrest_gage` | varchar | YES |  | NRCS stream gauge identifier |
| `strm_code` | varchar | YES |  | stream code |
| `rm_ii` | varchar | YES |  | river mile (alternate representation) |
| `model_source_id` | integer | YES | `1` | FK `model_source.id` |
| `source_id` | integer | YES | `4` | FK `source.id` |
| `network_version_id` | integer | NO |  | FK `version.id` |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `model_source_id` -> `model_source.id` (delete: RESTRICT, update: CASCADE)
- `network_id` -> `network.id` (delete: CASCADE, update: CASCADE)
- `network_version_id` -> `version.id` (delete: RESTRICT, update: CASCADE)
- `source_id` -> `source.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `network_node_pkey` (id) [PRIMARY]
- `network_node_short_code_key` (short_code) [UNIQUE]
- `idx_network_node_network_id` (network_id)
- `idx_network_node_strm_code` (strm_code)
- `idx_network_node_version` (network_version_id)

**Unique constraints:**

- `network_node_short_code_key` (short_code)

#### `network_gis`

PostGIS geometry per network element. The unique index on `network_id` enforces one-to-one with `network` (an element either has one geometry row or none). `network.has_gis` mirrors this. `source` is typically the CalSim3 GeoSchematic geopackage.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('network_gis_id_seq')` | PK |
| `short_code` | varchar | NO |  | matches `network.short_code` |
| `network_id` | integer | NO |  | FK `network.id`, ON DELETE CASCADE, also unique |
| `precision_level` | varchar | NO | `'precise'` | one of `precise`, `approximate`, `schematic` |
| `geom_wkt` | text | NO |  | WKT geometry (human-readable copy) |
| `srid` | integer | YES | `4326` | EPSG:4326 (WGS84) |
| `geom` | geometry | YES |  | PostGIS binary, GiST index |
| `estimated_accuracy_meters` | numeric | YES |  |  |
| `source_id` | integer | NO | `4` | FK `source.id` |
| `network_version_id` | integer | NO |  | FK `version.id` |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `network_id` -> `network.id` (delete: CASCADE, update: CASCADE)
- `network_version_id` -> `version.id` (delete: RESTRICT, update: CASCADE)
- `source_id` -> `source.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `network_gis_pkey` (id) [PRIMARY]
- `idx_network_gis_network_id` (network_id) [UNIQUE] - enforces 1:1 with network
- `idx_network_gis_geom` (geom): GiST spatial index
- `idx_network_gis_precision` (precision_level)
- `idx_network_gis_short_code` (short_code)
- `idx_network_gis_version` (network_version_id)

---

## Layer 03 - ENTITY

Lists of real-world "things" in the model: reservoirs, channels, demand units, M&I contractors, water budget areas, compliance stations, project-level aggregate categories. Statistics tables in Layers 10-12 reference rows in this layer by `<entity>_id` or `<entity>_short_code`. The entity-attribute pattern is documented in [`database/README.md`](../README.md) § "The entity-attribute pattern".

**On Layer 03 inconsistencies:** Layer 03 was built first, table by table, while the schema conventions were still being settled. Later layers (network in Layer 02, scenario in Layer 06, theme in Layer 08, tier in Layer 09) were built after the conventions stabilized, so they show fewer legacy patterns. Layer 03 carries the heavier load. The patterns the rest of the ERD documents as conventions were learned on these tables.

Specific gaps that recur across Layer 03:

- **Manual `id` assignment** on five tables (`reservoir_entity`, `reservoir_group`, `reservoir_group_member`, `du_urban_group`, `mi_contractor_group`). Every other entity-catalog in the schema uses a sequence-backed default. Explicit `id` values are required at insert time, until that is changed. These represent early tables where the thought was that catalog entries were canonical and static, which didn't prove to be true.
- **Unenforced FK-style columns:** 14 of the 19 Layer 03 tables lack the `created_by` / `updated_by` -> `developer.id` FKs that the audit trigger relies on. The trigger populates the values. No constraint validates them. Several tables also carry `entity_type_id`, `hydrologic_region_id`, and `entity_version_id` integer columns that point at a Layer 01 / Layer 00 row by id but aren't FK-enforced.
- **Type drift:** `has_gis_data integer (0/1)` on `reservoir_entity`, `source_ids text` (comma-separated string) instead of `integer[]` on `reservoir_entity`, `hydrologic_region_id varchar` on `channel_entity` where every peer uses `integer`.

None of this is broken in the operational sense. The API and ETL work today. The consolidated cleanup work for this layer is tracked in `database/SCHEMA_BACKLOG.md` § 7.

**Developer-FK reminder:** The unenforced `created_by` / `updated_by` -> `developer.id` FKs noted above are added by `audit_cleanup.sql` § 9 (see the `developer` table note in Layer 00 for the mechanism). The live Layer 03 data is clean (audit ids fall within `developer.id`, integer type), so the constraints add without orphans.

### Table schema

#### Reservoir family

##### `reservoir`

The NHD polygon footprint for the CalSim reservoirs that the National Hydrography Dataset covers (Shasta, Trinity, Oroville, Folsom, New Melones, Millerton, San Luis). Each row holds a `MULTIPOLYGON Z` boundary, surface area in km^2, elevation in meters, and the NHD / GNIS provenance ids. We needed this data to get the reservoir polygons, which the CalSim geoschematic doesn't provide.

This table exists to carry geometry the CalSim3 GeoSchematic does not. The geoschematic represents every CalSim reservoir as a single `POINT` node in `network` / `network_gis`. It does not carry the polygon boundary. NHD does. `reservoir` is therefore the authoritative source for reservoir extent and is kept as a separate table because its geometry type, schema, and provenance all differ from the geoschematic.

The companion `reservoir_entity` table (below) holds the operational attributes such as capacity, dead pool, hydrologic region, tier flags, and covers all CalSim reservoirs, including those that NHD does not provide a polygon for. The two tables join on `reservoir.calsim_short_code` = `reservoir_entity.short_code`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('reservoir_id_seq')` | PK |
| `calsim_short_code` | varchar | NO |  | unique, e.g. `SHSTA`, `OROVL` |
| `reservoir_name` | varchar | NO |  |  |
| `geom_wkt` | text | NO |  |  |
| `srid` | integer | YES | `4326` |  |
| `geom` | geometry | YES |  | PostGIS, GiST index |
| `area_sqkm` | numeric | YES |  | surface area in km^2 |
| `elevation_m` | numeric | YES |  | elevation in meters |
| `gnis_id` | varchar | YES |  | GNIS identifier |
| `nhd_permanent_id` | varchar | YES |  | NHD Permanent Identifier |
| `data_source` | varchar | YES | `'NHD'` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |
| `source_id` | integer | YES |  | FK `source.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `source_id` -> `source.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `reservoir_pkey` (id) [PRIMARY]
- `idx_reservoir_calsim_code` (calsim_short_code) [UNIQUE] - duplicate of below, cleanup candidate
- `reservoir_calsim_short_code_key` (calsim_short_code) [UNIQUE]
- `idx_reservoir_geom` (geom): GiST spatial index

**Unique constraints:**

- `reservoir_calsim_short_code_key` (calsim_short_code)

**Values:**

| calsim_short_code | reservoir_name | area_sqkm |
|---|---|---|
| `SHSTA` | Shasta Lake | 50.25 |
| `TRNTY` | Trinity Lake | 63.08 |
| `OROVL` | Lake Oroville | 42.94 |
| `FOLSM` | Folsom Lake | 27.68 |
| `MELON` | New Melones Lake | 36.25 |
| `MLRTN` | Millerton Lake | 13.41 |
| `SLUIS` | San Luis Reservoir | 51.93 |

All seven rows come from NHD (`data_source = 'NHD'`, `source_id = 10`). `elevation_m` is unreliable: `FOLSM` and `MELON` carry `0.0` placeholders rather than true elevations.

##### `reservoir_entity`

Catalog of CalSim reservoirs with operational attributes: capacity (TAF), dead pool storage, surface area, hydrologic region, operational purpose (CVP / SWP / Local), and entity-classification fields. Joins to the `reservoir` table by `short_code` for the rows that have an NHD polygon. Joins to the network by `network_node_id` (which currently mirrors `short_code` on every row).

This was one of the earliest Layer 03 tables and predates several conventions the rest of the schema later settled on. It carries **no foreign-key constraints in the live schema**, despite having columns that look like FKs (`entity_type_id`, `hydrologic_region_id`, `entity_version_id`, `created_by`, `updated_by`). It uses a manually-assigned `id integer NOT NULL` instead of the sequence-backed default used by every other entity-catalog table (`channel_entity`, `du_*_entity`, `mi_contractor`, etc.). New inserts must supply an `id` explicitly. Both gaps and other table-specific items are tracked in `database/SCHEMA_BACKLOG.md` § 7 under "`reservoir_entity` modernization".

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO |  | PK, manually assigned |
| `network_node_id` | varchar | NO |  | matching network node short_code |
| `short_code` | varchar | NO |  | unique reservoir identifier |
| `name` | varchar | YES |  |  |
| `description` | text | YES |  |  |
| `associated_river` | varchar | YES |  |  |
| `entity_type_id` | integer | NO |  | intended FK to `network_entity_type.id`, not enforced |
| `schematic_type_id` | integer | YES |  | internal type classifier |
| `hydrologic_region_id` | integer | YES |  | intended FK to `hydrologic_region.id`, not enforced |
| `capacity_taf` | numeric | YES |  | total capacity in TAF |
| `dead_pool_taf` | numeric | YES |  | dead-pool storage in TAF |
| `surface_area_acres` | numeric | YES |  |  |
| `operational_purpose` | varchar | YES |  | e.g. `CVP`, `SWP`, `Local` |
| `has_tiers` | boolean | YES | `false` | true when tier results exist for this entity |
| `is_main` | boolean | YES | `false` | true for the top-level entry in a storage-zone family |
| `has_gis_data` | integer | YES | `1` | legacy integer flag (0/1) |
| `entity_version_id` | integer | NO |  | intended FK to `version.id`, not enforced |
| `source_ids` | text | YES |  | comma-separated source IDs |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Indexes:**

- `reservoir_entity_pkey` (id) [PRIMARY]
- `reservoir_entity_short_code_key` (short_code) [UNIQUE]
- `idx_reservoir_entity_has_tiers` (has_tiers)
- `idx_reservoir_entity_is_main` (is_main)
- `idx_reservoir_entity_region` (hydrologic_region_id)
- `idx_reservoir_entity_short_code` (short_code): duplicates left-prefix of unique index

**Unique constraints:**

- `reservoir_entity_short_code_key` (short_code)

##### `reservoir_group`

Analytical groupings of reservoirs (e.g. `major` for the CVP/SWP storage reservoirs used in dashboard percentile-band charts).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO |  | PK |
| `short_code` | varchar | NO |  | unique |
| `label` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `display_order` | integer | YES | `0` |  |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `reservoir_group_pkey` (id) [PRIMARY]
- `reservoir_group_short_code_key` (short_code) [UNIQUE]
- `idx_reservoir_group_short_code` (short_code): duplicates left-prefix of unique index

**Unique constraints:**

- `reservoir_group_short_code_key` (short_code)

##### `reservoir_group_member`

M:N membership between `reservoir_group` and `reservoir_entity`. Both FKs cascade on delete.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO |  | PK |
| `reservoir_group_id` | integer | NO |  | FK `reservoir_group.id`, CASCADE |
| `reservoir_entity_id` | integer | NO |  | FK `reservoir_entity.id`, CASCADE |
| `display_order` | integer | YES | `0` |  |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `reservoir_entity_id` -> `reservoir_entity.id` (delete: CASCADE, update: NO ACTION)
- `reservoir_group_id` -> `reservoir_group.id` (delete: CASCADE, update: NO ACTION)

**Indexes:**

- `reservoir_group_member_pkey` (id) [PRIMARY]
- `uq_reservoir_group_member` (reservoir_group_id, reservoir_entity_id) [UNIQUE]
- `idx_reservoir_group_member_group` (reservoir_group_id)
- `idx_reservoir_group_member_reservoir` (reservoir_entity_id)

**Unique constraints:**

- `uq_reservoir_group_member` (reservoir_group_id, reservoir_entity_id)

#### Channel family

##### `channel_entity`

Per-arc channel entity. `network_arc_id` is the unique join key into `network_arc.short_code`, but stored as `varchar` with no FK constraint (cleanup candidate). Channels can carry minimum-instream-flow (`has_mif`) and environmental-flow (`has_eflows`) flags that gate the env-flow Layer 11 tables.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('channel_entity_id_seq')` | PK |
| `network_arc_id` | varchar | NO |  | unique, matches `network_arc.short_code` (not FK-enforced) |
| `short_code` | varchar | YES |  |  |
| `name` | varchar | YES |  |  |
| `description` | text | YES |  |  |
| `subtype` | varchar | YES |  |  |
| `entity_type_id` | integer | NO |  |  |
| `schematic_type_id` | integer | YES |  |  |
| `hydrologic_region_id` | varchar | YES |  | varchar here, cleanup candidate (other tables use integer FK) |
| `boundary_condition` | varchar | YES |  |  |
| `from_node` | varchar | YES |  |  |
| `to_node` | varchar | YES |  |  |
| `length_m` | numeric | YES |  |  |
| `has_tiers` | boolean | YES | `false` |  |
| `is_main` | boolean | YES | `false` |  |
| `has_gis_data` | integer | YES | `1` |  |
| `entity_version_id` | integer | NO |  |  |
| `source_ids` | text | YES |  | postgres-array-encoded text |
| `watershed_short_code` | varchar | YES |  | FK `watershed.short_code` |
| `unimp_sv_variable` | varchar | YES |  | SV unimpaired value read directly by the env-flow calc (e.g. `UNIMP_{watershed}`) |
| `has_mif` | boolean | NO | `false` | true gates env_flow MIF rows |
| `has_eflows` | boolean | NO | `false` | true gates env_flow seasonal/period rows |
| `channel_class` | varchar | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `watershed_short_code` -> `watershed.short_code` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `channel_entity_pkey` (id) [PRIMARY]
- `channel_entity_network_arc_id_key` (network_arc_id) [UNIQUE]
- `idx_channel_entity_channel_class` (channel_class)
- `idx_channel_entity_has_eflows` (has_eflows)
- `idx_channel_entity_has_mif` (has_mif)
- `idx_channel_entity_network_arc` (network_arc_id): duplicates left-prefix of unique index
- `idx_channel_entity_watershed` (watershed_short_code)

**Unique constraints:**

- `channel_entity_network_arc_id_key` (network_arc_id)

#### Demand-unit families (agriculture, urban, refuge)

All three DU tables carry a `wba_id varchar` column that stores the matching `wba.wba_id` code. **No foreign key is enforced.** Adding a FK (or replacing the varchar with an integer `wba_entity_id`) is tracked as a roadmap item in [`database/README.md`](../README.md).

##### `du_agriculture_entity`

Per-CalSim agricultural demand unit. 144 rows.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_agriculture_entity_id_seq')` | PK |
| `du_id` | varchar | NO |  | unique CalSim DU code, e.g. `02_PA1`, `50_PU` |
| `wba_id` | varchar | YES |  | matches `wba.wba_id`, no FK enforcement |
| `hydrologic_region` | varchar | NO |  | text (legacy), prefer `hydrologic_region_id` |
| `dups` | integer | YES |  | number of demand-unit polygons |
| `du_class` | varchar | YES |  | `Agriculture` |
| `cs3_type` | varchar | YES |  | CalSim3 type code, e.g. `CVP_PAG`, `SWP_PAG` |
| `total_acres` | numeric | YES |  |  |
| `polygon_count` | integer | YES |  |  |
| `source` | varchar | YES |  |  |
| `model_source` | varchar | YES |  |  |
| `agency` | varchar | YES |  |  |
| `provider` | varchar | YES |  | water provider name |
| `gw` | boolean | YES | `true` | groundwater supply |
| `sw` | boolean | YES | `true` | surface-water supply |
| `point_of_diversion` | text | YES |  |  |
| `diversion_arc` | varchar | YES |  | CalSim arc code |
| `river_reach` | varchar | YES |  |  |
| `river_mile_start` | numeric | YES |  |  |
| `river_mile_end` | numeric | YES |  |  |
| `bank` | varchar | YES |  | `L` or `R` |
| `area_acres` | numeric | YES |  | operational area |
| `annual_diversion_taf` | numeric | YES |  |  |
| `demand_unit` | varchar | YES |  |  |
| `table_id` | varchar | YES |  |  |
| `has_gis_data` | boolean | YES | `true` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `hydrologic_region_id` | integer | YES |  | FK `hydrologic_region.id` |
| `model_source_id` | integer | YES |  | FK `model_source.id` |
| `geom_wkt` | text | YES |  |  |
| `srid` | integer | YES |  |  |
| `geom` | geometry | YES |  | PostGIS, GiST index |

**Foreign keys:**

- `hydrologic_region_id` -> `hydrologic_region.id` (delete: NO ACTION, update: NO ACTION)
- `model_source_id` -> `model_source.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `du_agriculture_entity_pkey` (id) [PRIMARY]
- `du_agriculture_entity_du_id_key` (du_id) [UNIQUE]
- `idx_du_ag_provider` (provider)
- `idx_du_ag_region` (hydrologic_region)
- `idx_du_ag_type` (cs3_type)
- `idx_du_ag_wba` (wba_id)
- `idx_du_agriculture_entity_geom` (geom)

**Unique constraints:**

- `du_agriculture_entity_du_id_key` (du_id)

##### `du_refuge_entity`

Wildlife-refuge demand units. 18 rows. Unlike the other two DU tables, `created_by` / `updated_by` here DO have FK constraints (audit-trigger discipline is inconsistent across the DU family).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_refuge_entity_id_seq')` | PK |
| `du_id` | varchar | NO |  | unique CalSim DU code |
| `wba_id` | varchar | YES |  | matches `wba.wba_id`, no FK enforcement |
| `hydrologic_region` | varchar | NO |  | text (legacy) |
| `dups` | integer | YES |  |  |
| `du_class` | varchar | YES | `'Refuge'` |  |
| `cs3_type` | varchar | YES |  |  |
| `total_acres` | numeric | YES |  |  |
| `polygon_count` | integer | YES | `1` |  |
| `refuge_or_wildlife_area` | text | YES |  | official refuge name |
| `managed_by` | varchar | YES |  | e.g. `USFWS`, `DFW` |
| `provider` | varchar | YES |  |  |
| `gw` | boolean | NO | `false` | refuges typically SW only |
| `sw` | boolean | NO | `true` |  |
| `point_of_diversion_conveyance` | text | YES |  |  |
| `source` | varchar | YES |  |  |
| `model_source` | varchar | YES | `'calsim3'` |  |
| `has_gis_data` | boolean | YES | `true` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |
| `geom_wkt` | text | YES |  |  |
| `srid` | integer | YES |  |  |
| `geom` | geometry | YES |  | PostGIS, GiST index |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `du_refuge_entity_pkey` (id) [PRIMARY]
- `du_refuge_entity_du_id_key` (du_id) [UNIQUE]
- `idx_du_refuge_entity_cs3_type` (cs3_type)
- `idx_du_refuge_entity_geom` (geom)
- `idx_du_refuge_entity_hydrologic_region` (hydrologic_region)

**Unique constraints:**

- `du_refuge_entity_du_id_key` (du_id)

##### `du_urban_entity`

Per-CalSim urban / community-water-system demand unit. 145 rows. Each row may FK to a primary `mi_contractor` via `primary_contractor_short_code` (not DB-enforced as a FK, but referenced by name).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_urban_entity_id_seq')` | PK |
| `du_id` | varchar | NO |  | unique CalSim DU code, e.g. `ACWD`, `MWD` |
| `wba_id` | varchar | YES |  | matches `wba.wba_id`, no FK enforcement |
| `hydrologic_region` | varchar | YES |  | text (legacy) |
| `dups` | integer | YES | `0` |  |
| `du_class` | varchar | YES | `'Urban'` |  |
| `cs3_type` | varchar | YES |  | e.g. `CVP_PMI`, `SWP_PMI` |
| `total_acres` | numeric | YES |  |  |
| `polygon_count` | integer | YES | `1` |  |
| `community_agency` | text | YES |  | water-agency name |
| `gw` | varchar | YES |  | groundwater-supply indicator (text, not boolean) |
| `sw` | varchar | YES |  | surface-water-supply indicator (text, not boolean) |
| `point_of_diversion` | text | YES |  |  |
| `source` | varchar | YES |  |  |
| `model_source` | varchar | YES |  |  |
| `has_gis_data` | boolean | YES | `true` |  |
| `primary_contractor_short_code` | varchar | YES |  | references `mi_contractor.short_code`, not FK-enforced |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `hydrologic_region_id` | integer | YES |  | FK `hydrologic_region.id` |
| `model_source_id` | integer | YES |  | FK `model_source.id` |
| `geom_wkt` | text | YES |  |  |
| `srid` | integer | YES |  |  |
| `geom` | geometry | YES |  | PostGIS, GiST index |

**Foreign keys:**

- `hydrologic_region_id` -> `hydrologic_region.id` (delete: NO ACTION, update: NO ACTION)
- `model_source_id` -> `model_source.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `du_urban_entity_pkey` (id) [PRIMARY]
- `du_urban_entity_du_id_key` (du_id) [UNIQUE]
- `idx_du_urban_entity_contractor` (primary_contractor_short_code)
- `idx_du_urban_entity_du_id` (du_id): duplicates left-prefix of unique index
- `idx_du_urban_entity_geom` (geom)
- `idx_du_urban_entity_region` (hydrologic_region)
- `idx_du_urban_entity_type` (cs3_type)
- `idx_du_urban_entity_wba_id` (wba_id)

**Unique constraints:**

- `du_urban_entity_du_id_key` (du_id)

##### `du_urban_delivery_arc`

When a CalSim urban DU is fed by multiple delivery arcs, this table lists them. Some downstream calculations sum the matching CalSim arc-flow variables across all rows for the DU.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_urban_delivery_arc_id_seq')` | PK |
| `du_id` | varchar | NO |  | FK `du_urban_entity.du_id`, CASCADE |
| `delivery_arc` | varchar | NO |  | CalSim arc code |
| `arc_order` | integer | YES | `1` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `du_id` -> `du_urban_entity.du_id` (delete: CASCADE, update: NO ACTION)

**Indexes:**

- `du_urban_delivery_arc_pkey` (id) [PRIMARY]
- `uq_du_urban_delivery_arc` (du_id, delivery_arc) [UNIQUE]
- `idx_du_urban_delivery_arc_du_id` (du_id)

**Unique constraints:**

- `uq_du_urban_delivery_arc` (du_id, delivery_arc)

##### `du_urban_group`

Analytical groupings of urban DUs (e.g. SWP/CVP rollups).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO |  | PK |
| `short_code` | varchar | NO |  | unique |
| `label` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `display_order` | integer | YES | `0` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `du_urban_group_pkey` (id) [PRIMARY]
- `du_urban_group_short_code_key` (short_code) [UNIQUE]
- `idx_du_urban_group_short_code` (short_code): duplicates left-prefix

**Unique constraints:**

- `du_urban_group_short_code_key` (short_code)

##### `du_urban_group_member`

M:N membership between `du_urban_group` and `du_urban_entity`. Joined on `du_id` text (cascades from `du_urban_entity.du_id`).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_urban_group_member_id_seq')` | PK |
| `du_urban_group_id` | integer | NO |  | FK `du_urban_group.id`, CASCADE |
| `du_id` | varchar | NO |  | FK `du_urban_entity.du_id`, CASCADE |
| `display_order` | integer | YES | `0` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `du_id` -> `du_urban_entity.du_id` (delete: CASCADE, update: NO ACTION)
- `du_urban_group_id` -> `du_urban_group.id` (delete: CASCADE, update: NO ACTION)

**Indexes:**

- `du_urban_group_member_pkey` (id) [PRIMARY]
- `uq_du_urban_group_member` (du_urban_group_id, du_id) [UNIQUE]
- `idx_du_urban_group_member_du` (du_id)
- `idx_du_urban_group_member_group` (du_urban_group_id)

**Unique constraints:**

- `uq_du_urban_group_member` (du_urban_group_id, du_id)

#### M&I contractor family

##### `mi_contractor`

SWP/CVP municipal-and-industrial contractor catalog.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('mi_contractor_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique contractor code, e.g. `ACWD`, `MWD`, `SCVWD` |
| `contractor_name` | varchar | NO |  |  |
| `project` | varchar | NO |  | `SWP`, `CVP`, or `Both` |
| `region` | varchar | YES |  |  |
| `contractor_type` | varchar | NO |  | `Urban`, `Agricultural`, `Mixed` |
| `contract_amount_taf` | numeric | YES |  | Table A / contract allocation in TAF/yr |
| `source_contractor_id` | integer | YES |  | external contractor ID from source data |
| `source_file` | varchar | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `mi_contractor_pkey` (id) [PRIMARY]
- `mi_contractor_short_code_key` (short_code) [UNIQUE]
- `idx_mi_contractor_project` (project)
- `idx_mi_contractor_region` (region)
- `idx_mi_contractor_short_code` (short_code): duplicates left-prefix of unique index
- `idx_mi_contractor_type` (contractor_type)

**Unique constraints:**

- `mi_contractor_short_code_key` (short_code)

##### `mi_contractor_delivery_arc`

Per-contractor delivery-arc list. Multiple arcs may sum into a contractor's delivery. `arc_type` distinguishes the arc kind (e.g. main canal, branch).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('mi_contractor_delivery_arc_id_seq')` | PK |
| `mi_contractor_id` | integer | NO |  | FK `mi_contractor.id`, CASCADE |
| `delivery_arc` | varchar | NO |  | globally unique CalSim arc code |
| `arc_type` | varchar | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `mi_contractor_id` -> `mi_contractor.id` (delete: CASCADE, update: NO ACTION)

**Indexes:**

- `mi_contractor_delivery_arc_pkey` (id) [PRIMARY]
- `uq_delivery_arc` (delivery_arc) [UNIQUE]
- `idx_mi_contractor_delivery_arc_arc` (delivery_arc): duplicates left-prefix
- `idx_mi_contractor_delivery_arc_contractor` (mi_contractor_id)

**Unique constraints:**

- `uq_delivery_arc` (delivery_arc)

The global uniqueness on `delivery_arc` (vs scoping to `(mi_contractor_id, delivery_arc)`) implies each arc belongs to at most one contractor.

##### `mi_contractor_group`

Analytical groupings of M&I contractors.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO |  | PK |
| `short_code` | varchar | NO |  | unique |
| `label` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `display_order` | integer | YES | `0` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `mi_contractor_group_pkey` (id) [PRIMARY]
- `mi_contractor_group_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `mi_contractor_group_short_code_key` (short_code)

##### `mi_contractor_group_member`

M:N membership between `mi_contractor_group` and `mi_contractor`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('mi_contractor_group_member_id_seq')` | PK |
| `mi_contractor_group_id` | integer | NO |  | FK `mi_contractor_group.id`, CASCADE |
| `mi_contractor_id` | integer | NO |  | FK `mi_contractor.id`, CASCADE |
| `display_order` | integer | YES | `0` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `mi_contractor_group_id` -> `mi_contractor_group.id` (delete: CASCADE, update: NO ACTION)
- `mi_contractor_id` -> `mi_contractor.id` (delete: CASCADE, update: NO ACTION)

**Indexes:**

- `mi_contractor_group_member_pkey` (id) [PRIMARY]
- `uq_mi_contractor_group_member` (mi_contractor_group_id, mi_contractor_id) [UNIQUE]
- `idx_mi_contractor_group_member_contractor` (mi_contractor_id)
- `idx_mi_contractor_group_member_group` (mi_contractor_group_id)

**Unique constraints:**

- `uq_mi_contractor_group_member` (mi_contractor_group_id, mi_contractor_id)

#### Project-level aggregate entities

CalSim emits some demand totals at the project (SWP/CVP) level rather than per DU. These tables catalog those project-level aggregates and the CalSim variables that carry their data.

##### `ag_aggregate_entity`

Agricultural project-level rollups. 9 rows (SWP/CVP PAG totals and NOD/SOD splits, plus CVP settlement and exchange contractors). The "aggregate" suffix denotes that CalSim itself reports these as aggregates, not that they aggregate community water systems.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('ag_aggregate_entity_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique, e.g. `swp_pag`, `cvp_pex_s` |
| `label` | varchar | NO |  |  |
| `project` | varchar | YES |  | `SWP` or `CVP` |
| `region` | varchar | YES |  | `TOTAL`, `NOD`, `SOD` |
| `delivery_variable` | varchar | NO |  | CalSim variable name, e.g. `DEL_SWP_PAG_N` |
| `description` | text | YES |  |  |
| `display_order` | integer | YES | `0` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `ag_aggregate_entity_pkey` (id) [PRIMARY]
- `ag_aggregate_entity_short_code_key` (short_code) [UNIQUE]
- `idx_ag_agg_project` (project)

**Unique constraints:**

- `ag_aggregate_entity_short_code_key` (short_code)

##### `cws_aggregate_entity`

Community-water-system (M&I) project-level rollups. 6 rows: SWP and CVP NOD/SOD totals plus MWD. Carries both a delivery variable and a shortage variable per row.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('cws_aggregate_entity_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique, e.g. `swp_total`, `cvp_nod`, `mwd` |
| `label` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `project` | varchar | NO |  |  |
| `region` | varchar | YES |  |  |
| `delivery_variable` | varchar | YES |  |  |
| `shortage_variable` | varchar | YES |  |  |
| `display_order` | integer | YES | `0` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `cws_aggregate_entity_pkey` (id) [PRIMARY]
- `cws_aggregate_entity_short_code_key` (short_code) [UNIQUE]
- `idx_cws_aggregate_entity_project` (project)
- `idx_cws_aggregate_entity_short_code` (short_code): duplicates left-prefix

**Unique constraints:**

- `cws_aggregate_entity_short_code_key` (short_code)

#### Compliance and water-budget-area entities

##### `compliance_station`

Compliance monitoring stations used for delta-region tier indicators (e.g. `FW_DELTA_USES`). 2 rows.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('compliance_station_id_seq')` | PK |
| `station_code` | varchar | NO |  | unique, e.g. `JP`, `EX2` |
| `station_name` | varchar | NO |  |  |
| `latitude` | numeric | NO |  |  |
| `longitude` | numeric | NO |  |  |
| `srid` | integer | YES | `4326` |  |
| `geom_wkt` | text | NO |  |  |
| `geom` | geometry | YES |  | PostGIS, GiST index |
| `tier_use` | varchar | YES |  | which tier indicator uses this station |
| `data_source` | varchar | YES |  |  |
| `notes` | text | YES |  |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |
| `source_id` | integer | YES |  | FK `source.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `source_id` -> `source.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `compliance_station_pkey` (id) [PRIMARY]
- `compliance_station_station_code_key` (station_code) [UNIQUE]
- `idx_compliance_code` (station_code) [UNIQUE] - duplicate of above, cleanup candidate
- `idx_compliance_geom` (geom): GiST spatial index
- `idx_compliance_tier` (tier_use)

**Unique constraints:**

- `compliance_station_station_code_key` (station_code)

##### `wba`

Water budget areas. 42 rows. **Non-standard entity pattern:** uses `wba_id varchar` as the human-readable code (instead of `short_code TEXT UNIQUE NOT NULL` per `CHECKLIST_TABLE_STANDARDS.md`), and has no `is_active` column. WBAs are the unit for groundwater data. The planned `wba_variable` table and the missing DU-WBA foreign-key crosswalk are tracked in [`database/README.md`](../README.md) § Roadmap.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('wba_id_seq')` | PK |
| `wba_id` | varchar | NO |  | unique, e.g. `DETAW`, `02N`, `06S`, used as human-readable code |
| `wba_name` | varchar | NO |  |  |
| `geom_wkt` | text | NO |  |  |
| `srid` | integer | YES | `4326` |  |
| `geom` | geometry | YES |  | PostGIS, GiST index |
| `area_acres` | numeric | YES |  |  |
| `comments` | text | YES |  |  |
| `data_source` | varchar | YES | `'CalSim_Geopackage'` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |
| `hydrologic_region_id` | integer | YES |  | FK `hydrologic_region.id` |
| `source_id` | integer | YES |  | FK `source.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `hydrologic_region_id` -> `hydrologic_region.id` (delete: NO ACTION, update: NO ACTION)
- `source_id` -> `source.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `wba_pkey` (id) [PRIMARY]
- `idx_wba_id` (wba_id) [UNIQUE] - duplicate of below, cleanup candidate
- `wba_wba_id_key` (wba_id) [UNIQUE]
- `idx_wba_geom` (geom): GiST spatial index

**Unique constraints:**

- `wba_wba_id_key` (wba_id)

The three DU entity tables (`du_agriculture_entity`, `du_urban_entity`, `du_refuge_entity`) carry a `wba_id varchar` column that matches `wba.wba_id`, but no FK constraint enforces it.

---

## Layer 04 - VARIABLE

CalSim variable definitions and type classifications. The intent is one `*_variable` table per ETL calculation domain [from the Content Platform Outcomes tab](https://docs.google.com/spreadsheets/d/1xcQIR_J96-cs7BuCrXjznwkinLgxl-Pf9tA3mJ2GiyA/edit?gid=1094338461#gid=1094338461), each carrying an FK to its Layer 03 entity table. Then the ETL can also use these lists to calculate statistics for the outcomes instead of the current ad hoc methods.

**Status today:** `du_urban_variable` is built and actively consumed by the production ETL. `etl/statistics/du_urban/calculate_du_statistics_v2.py` loads one row per `du_id` and uses `delivery_variable`, `shortage_variable`, `demand_variable`, `demand_mode`, `demand_params`, and `requires_sum` to drive per-DU demand-calculation logic. `channel_variable` is also built but no ETL calc consumes it yet. As an intermediate step, the env-flow calc instead reads the seed table `channel_entity.csv` directly, treating it as a per-entity variable-mapping table. See [`database/README.md`](../README.md) § Roadmap > "Layer 04 variable layer"

It is a key roadmap item to figure out the proper variable lists for the ETL along with the project team and Water Allocation Modeling Team, load them into the database, use them in the ETL, and eventually apply FK's to link those variables with definition and attribute data to create a rich and FK-traceable variable graph that links each statistic back to the CalSim variable, entity, unit, and version it relates to. 

### Table schema

#### Tables we currently have

Five tables exist today: three classification catalogs (`calsim_model_variable_type`, `variable_type`, `derived_variable_type`) and two domain variable tables (`channel_variable`, `du_urban_variable`). The classifiers are intended as shared vocabularies.

##### `calsim_model_variable_type`

Catalog of CalSim model variable behaviors.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('calsim_model_variable_type_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `label` | text | NO |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `calsim_model_variable_type_pkey` (id) [PRIMARY]
- `calsim_model_variable_type_short_code_key` (short_code) [UNIQUE]
- `idx_calsim_model_variable_type_active` (is_active, short_code)

**Unique constraints:**

- `calsim_model_variable_type_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `output` | Output | Standard CalSim model output variables |
| `control` | Control | Operational control indicators and binary flags |
| `decision` | Decision | Model decision variables |
| `state` | State | State variables |
| `input` | Input | External inputs and boundary conditions |
| `intermediate` | Intermediate | Calculated intermediate values used in model logic |
| `aggregate` | Aggregate | Variables that sum or combine multiple reservoir/system components |
| `index` | Index | Index variables |

##### `derived_variable_type`

Catalog of derived variable categories.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('derived_variable_type_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `label` | text | NO |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `derived_variable_type_pkey` (id) [PRIMARY]
- `derived_variable_type_short_code_key` (short_code) [UNIQUE]
- `idx_derived_variable_type_active` (is_active, short_code)

**Unique constraints:**

- `derived_variable_type_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `sector_aggregate` | Sector Aggregate | Variables that aggregate across a sector |
| `delta_variable` | Delta | Variables specific to Delta conditions and operations |
| `environmental_indicator` | Environmental Indicator | Environmental metrics and indicators |
| `regional_summary` | Regional Summary | Variables that aggregate across a region |

##### `variable_type`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('variable_type_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `label` | text | NO |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `variable_type_pkey` (id) [PRIMARY]
- `variable_type_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `variable_type_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `delivery` | delivery | water delivery |
| `gw_pumping` | groundwater pumping | groundwater pumping |
| `PA` | project agricultural | project agricultural water use |
| `PR` | project wildlife refuge | project wildlife refuge water use |
| `PU` | project community water system | project community water system (M&I) |
| `unknown` | unknown | unknown or unclassified variable type |

##### `channel_variable`

CalSim variable definitions for channel arcs. Each row maps a CalSim variable name (`calsim_id`) to its channel-entity context and type classification. 1,352 rows including 20 `C_*_MIF` regulatory minimum-instream-flow variables.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('channel_variable_id_seq')` | PK |
| `calsim_id` | varchar | NO |  | unique CalSim variable name |
| `name` | varchar | YES |  |  |
| `description` | text | YES |  |  |
| `channel_entity_id` | integer | YES |  | FK `channel_entity.id` |
| `variable_type` | varchar | YES |  | text (not FK to variable_type) |
| `unit_id` | integer | YES |  | FK `unit.id` (not currently enforced) |
| `temporal_scale_id` | integer | YES |  | FK `temporal_scale.id` (not currently enforced) |
| `variable_version_id` | integer | YES |  | FK `version.id` (not currently enforced) |
| `is_regulatory` | boolean | NO | `false` | TRUE for `C_*_MIF` |
| `regulatory_authority` | varchar | YES |  | e.g. `CalSim-III` for MIF |
| `is_aggregate` | boolean | NO | `false` |  |
| `aggregated_variable_ids` | text | YES |  | postgres-array-encoded text |
| `variable_id` | uuid | YES |  | legacy column, see "Legacy column" note below |
| `source_ids` | text | YES |  | postgres-array-encoded text |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_by` | integer | YES |  | FK `developer.id` |
| `created_at` | timestamptz | NO | `now()` |  |
| `updated_at` | timestamptz | NO | `now()` |  |

**Foreign keys:**

- `channel_entity_id` -> `channel_entity.id` (delete: NO ACTION, update: NO ACTION)
- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `channel_variable_pkey` (id) [PRIMARY]
- `channel_variable_calsim_id_key` (calsim_id) [UNIQUE]
- `idx_channel_variable_calsim_id` (calsim_id): duplicates left-prefix of unique index
- `idx_channel_variable_entity` (channel_entity_id)
- `idx_channel_variable_is_regulatory` (is_regulatory)
- `idx_channel_variable_type` (variable_type)

**Unique constraints:**

- `channel_variable_calsim_id_key` (calsim_id)

**Legacy column:** `variable_id uuid` is a nullable column from an earlier design that proposed a master `variable` registry to give every row a stable cross-domain identifier. The master registry was not built and the column has no FK target. All rows carry populated UUIDs in case the design is ever revived. Safe to ignore for current use. Safe to drop if the design is abandoned.

The 20 MIF variables (migration 23): `AMR004`, `FTR003`, `FTR029`, `FTR059`, `KSWCK`, `MCD005`, `MOK028`, `NTOMA`, `SAC049`, `SAC122`, `SAC148`, `SAC257`, `SAC289`, `SJR070`, `SJR127`, `STS011`, `STS059`, `TRN111`, `TUO003`, `YUB002`. `C_SAC000_MIF` is absent from CalSim DV (no MIF for the delta confluence reach).

##### `du_urban_variable`

CalSim variable mapping per urban demand unit. Read by the urban DU calc (`etl/statistics/du_urban/calculate_du_statistics_v2.py`), which loads one row per `du_id` and uses it to resolve `delivery_variable`, `shortage_variable`, and `demand_variable` names, plus `demand_mode` / `demand_params jsonb` / `requires_sum` for per-DU demand-calculation logic.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_urban_variable_id_seq')` | PK |
| `du_id` | varchar | NO |  | unique, FK `du_urban_entity.du_id`, CASCADE |
| `delivery_variable` | varchar | NO |  | CalSim delivery variable name |
| `shortage_variable` | varchar | YES |  | CalSim shortage variable name |
| `variable_type` | varchar | NO | `'delivery'` | water-supply classifier |
| `requires_sum` | boolean | YES | `false` | TRUE when delivery is summed across multiple arcs |
| `notes` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  |  |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  |  |
| `demand_variable` | varchar | YES |  |  |
| `variable_type_id` | integer | YES |  | FK `calsim_model_variable_type.id` |
| `demand_mode` | varchar | YES |  | per-DU demand calculation mode |
| `demand_params` | jsonb | YES |  | demand-calculation parameters |

**Foreign keys:**

- `du_id` -> `du_urban_entity.du_id` (delete: CASCADE, update: NO ACTION)
- `variable_type_id` -> `calsim_model_variable_type.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `du_urban_variable_pkey` (id) [PRIMARY]
- `uq_du_urban_variable_du_id` (du_id) [UNIQUE]
- `idx_du_urban_variable_du_id` (du_id): duplicates left-prefix of unique index
- `idx_du_urban_variable_type` (variable_type)

**Unique constraints:**

- `uq_du_urban_variable_du_id` (du_id)

#### Tables we have seed data for

Three planned tables have curated content in `database/seed_tables/04_variable/` but their DDL has not been applied. Per-table proposals below.

The seed CSVs for these three tables include a populated `variable_id uuid` column inherited from the parked master-registry design (see `channel_variable` > "Legacy column" above). The proposed table columns below omit `variable_id`. When the tables are built, either strip the column from the CSV at load time or carry it forward as a nullable legacy column matching `channel_variable.variable_id`.

##### `delta_variable` (planned)

Delta-specific derived variables (regulatory thresholds, salinity positions, system-scope indicators). The two seeded rows (NDO and X2) are `is_regulatory=true` under SWRCB authority.

[PLANNED] Proposed columns:

| column | type | nullable | notes |
|---|---|---|---|
| `id` | integer | NO | PK |
| `calsim_id` | varchar | NO | unique, e.g. `NDO`, `X2` |
| `name` | varchar | YES |  |
| `description` | text | YES |  |
| `variable_type_id` | integer | YES | FK `derived_variable_type.id`, e.g. `delta_variable` (= id 2) |
| `system_scope` | varchar | YES | e.g. `delta` |
| `calculation_method` | varchar | YES | e.g. `complex_formula`, `spatial_interpolation` |
| `hydrologic_region_id` | integer | YES | FK `hydrologic_region.id` |
| `spatial_description` | text | YES |  |
| `unit_id` | integer | YES | FK `unit.id` |
| `temporal_scale_id` | integer | YES | FK `temporal_scale.id` |
| `is_aggregate` | boolean | NO | DEFAULT `false` |
| `aggregation_rule` | varchar | YES | e.g. `sum`, `spatial_interpolation` |
| `is_regulatory` | boolean | NO | DEFAULT `false`, TRUE for NDO, X2 |
| `regulatory_authority` | varchar | YES | e.g. `SWRCB` |
| `regulatory_threshold` | numeric | YES | e.g. 11400 for NDO minimum, 81 for X2 |
| `outcome_category_short_code` | varchar | YES |  |
| `source_ids` | integer[] | YES | text-encoded array of source IDs |
| `variable_version_id` | integer | NO | FK `version.id` |
| `is_active` | boolean | NO | DEFAULT `true` |
| `created_at` | timestamptz | NO | DEFAULT `now()` |
| `created_by` | integer | NO | FK `developer.id` |
| `updated_at` | timestamptz | NO | DEFAULT `now()` |
| `updated_by` | integer | NO | FK `developer.id` |

##### `reservoir_variable` (planned)

Per-reservoir CalSim variable catalog (storage `S_*`, release `R_*`, etc.). Mirrors the `channel_variable` pattern with an `<entity>_id` FK to `reservoir_entity.id`.

[PLANNED] Proposed columns:

| column | type | nullable | notes |
|---|---|---|---|
| `id` | integer | NO | PK |
| `calsim_id` | varchar | NO | unique, e.g. `S_SHSTA`, `C_OROVL` |
| `name` | varchar | YES |  |
| `description` | text | YES |  |
| `reservoir_entity_id` | integer | YES | FK `reservoir_entity.id` |
| `variable_type` | varchar | YES | e.g. `storage`, `release`, `spill` |
| `is_aggregate` | boolean | NO | DEFAULT `false` |
| `trigger_threshold` | numeric | YES | optional alarm/trigger threshold |
| `unit_id` | integer | YES | FK `unit.id` |
| `temporal_scale_id` | integer | YES | FK `temporal_scale.id` |
| `variable_version_id` | integer | NO | FK `version.id` |
| `source_ids` | integer[] | YES | text-encoded array of source IDs |
| `is_active` | boolean | NO | DEFAULT `true` |
| `created_at` | timestamptz | NO | DEFAULT `now()` |
| `created_by` | integer | NO | FK `developer.id` |
| `updated_at` | timestamptz | NO | DEFAULT `now()` |
| `updated_by` | integer | NO | FK `developer.id` |

##### `inflow_variable` (planned)

CalSim inflow boundary-condition variables (`I_*` series). Depends on a not-yet-built `inflow_entity` entity table (Layer 03 future work) that `inflow_entity_id` would FK to. For now an integer placeholder is used.

[PLANNED] Proposed columns:

| column | type | nullable | notes |
|---|---|---|---|
| `id` | integer | NO | PK |
| `calsim_id` | varchar | NO | unique, e.g. `I_ALD002`, `I_SHSTA` |
| `name` | varchar | YES |  |
| `description` | text | YES |  |
| `inflow_entity_id` | integer | YES | intended FK to planned `inflow_entity.id` |
| `variable_type` | varchar | YES | e.g. `inflow` |
| `is_aggregate` | boolean | NO | DEFAULT `false` |
| `unit_id` | integer | YES | FK `unit.id` |
| `temporal_scale_id` | integer | YES | FK `temporal_scale.id` |
| `variable_version_id` | integer | NO | FK `version.id` |
| `source_ids` | integer[] | YES | text-encoded array of source IDs |
| `is_active` | boolean | NO | DEFAULT `true` |
| `created_at` | timestamptz | NO | DEFAULT `now()` |
| `created_by` | integer | NO | FK `developer.id` |
| `updated_at` | timestamptz | NO | DEFAULT `now()` |
| `updated_by` | integer | NO | FK `developer.id` |

---

## Layer 05 - ASSUMPTIONS + OPERATIONS

Scenario configuration dimensions:

- **Assumptions** = broad model-input context (land use, groundwater model). Sea-level rise was previously here but now lives in Layer 07, Hydroclimate (`slr`).
- **Operations** = active policy / regulatory actions applied to a scenario: TUCP, SGMA, BiOps, infrastructure, flows, allocation priorities, delta regs, etc.

Each type has a category table and a definition table. The scenario -> definition link tables (`scenario_key_assumption_link`, `scenario_key_operation_link`) live in **Layer 06** because they belong to the scenario layer (see [`database/README.md`](../README.md) § "Schema layers").

**Status:** This layer is populated and wired (`assumption_definition` and `operation_definition` carry the vocabulary, and the Layer 06 link tables connect scenarios to both), but the database-populated values are provisional. They were assumed before the water-allocation modeling team produced the [metadata](https://docs.google.com/document/d/1D5mkb7sxZDSk-JZgh4Hhh_Q6vj0mgHCaWK06EYhYoBI/edit?tab=t.0#heading=h.qgbml6o5wma9). To get data onto the website quickly, the per-scenario assumptions and operations were authored directly in the frontend (`coeqwal-website`) as a hardcoded map in `apps/main/app/features/scenarios/components/shared/opsIcons.tsx` (`ICON_REGISTRY` plus `SCENARIO_ICONS`, keyed by hydroclimate sibling-group id in the frontend). In the database these attach per scenario through the Layer 06 link tables, not to the sibling group. A next step is to load that data into these tables and use the API to deliver to the website.

That frontend map is the current source of truth and has been hand-checked for accuracy as of March 6, 2026. That said, this is a working document and the Water Allocation Modeling Team may have altered the metadata since. Worth a check in Google versioning and a check-in with the WAM team. Note that the frontend uses its own icon-id vocabulary that does not match the DB short_codes (`cws_hhs` vs `comm_delivery_HHS`, `tunnel` vs `DCP_6000`, and so on), is this has to be sorted. The API doesn't expose opperations or assumptions yet. Tracked in `database/README.md` § Roadmap > "Scenario assumptions and operations metadata".

**Developer-FK reminder:** All four tables carry `created_by` / `updated_by` columns whose `developer.id` FK is not yet enforced in the live DB. `audit_cleanup.sql` § 9 adds them (see the `developer` table note in Layer 00 for the mechanism).

### Table schema

#### `assumption_category`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('assumption_category_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `label` | text | YES |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | YES |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | YES |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `assumption_category_pkey` (id) [PRIMARY]
- `assumption_category_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `assumption_category_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `land_use` | Land Use | Agricultural and urban land use assumptions |
| `gw_model` | Groundwater Model | Groundwater model coupling assumptions (e.g. C2VSimFG) |

**Status:** The `land_use` description was written prematurely. I'm not sure it covers urban. Check with WAM team.

#### `assumption_definition`

Specific assumption instances

**Frontend-data note:** The corrected assumption metadata and per-scenario assumption assignments currently live in the frontend (`coeqwal-website`, `opsIcons.tsx`), which is the present source of truth (see the Layer 05 description) as of March 6, 2026. They should migrate into this table and `scenario_key_assumption_link`. See `database/README.md` § Roadmap > "Scenario assumptions and operations metadata".

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('assumption_definition_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique |
| `name` | varchar | YES |  |  |
| `short_title` | varchar | YES |  |  |
| `assumption_category_id` | integer | NO |  | FK `assumption_category.id` |
| `description` | text | YES |  |  |
| `source_id` | integer | YES |  | FK `source.id` |
| `is_active` | boolean | NO | `true` |  |
| `notes` | text | YES |  |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `created_at` | timestamptz | NO | `now()` |  |
| `updated_at` | timestamptz | NO | `now()` |  |

**Foreign keys:**

- `assumption_category_id` -> `assumption_category.id` (delete: RESTRICT, update: CASCADE)
- `source_id` -> `source.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `assumption_definition_pkey` (id) [PRIMARY]
- `assumption_definition_short_code_key` (short_code) [UNIQUE]
- `idx_assumption_definition_active` (is_active)
- `idx_assumption_definition_category_id` (assumption_category_id)

**Unique constraints:**

- `assumption_definition_short_code_key` (short_code)

**Values:**

| short_code | name | category |
|---|---|---|
| `lu_2004_2013` | 2004-2013 land use model | land_use |
| `lu_updated` | updated land use model | land_use |
| `lu_proj_reductions` | updated land use model with projected land use reductions | land_use |
| `gw_model` | groundwater model | gw_model |
| `lu_2020_landiq` | 2020 LandIQ land use | land_use |
| `lu_2020_landiq_reduced_ag` | 2020 LandIQ land use with reduced agricultural acreage | land_use |

#### `operation_category`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('operation_category_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `name` | text | YES |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | YES |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | YES |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `operation_category_pkey` (id) [PRIMARY]
- `operation_category_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `operation_category_short_code_key` (short_code)

**Values:**

| short_code | name | description |
|---|---|---|
| `comm_delivery` | Community Water Delivery | Community water-delivery prioritization |
| `delta_outflow` | Delta Outflow | Delta outflow requirements |
| `carryover` | Reservoir Carryover | Reservoir carryover storage requirements |
| `regulatory_salinity` | Regulatory Salinity | Delta salinity standards (X2) |
| `tucp` | TUCP / TUCO | Temporary Urgency Change Petitions and Orders |
| `gw_restrictions` | Groundwater Restrictions | Groundwater pumping restrictions (SGMA-type) |
| `infrastructure` | Infrastructure | Water-infrastructure configuration (tunnels, reservoirs) |
| `flow` | Flow Requirements | Instream flow and minimum-flow objectives |
| `biops` | Biological Opinions | NMFS / USFWS biological opinions for USBR LTO |

#### `operation_definition`

Specific operational policies

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('operation_definition_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique |
| `name` | varchar | YES |  |  |
| `short_title` | varchar | YES |  |  |
| `operation_category_id` | integer | NO |  | FK `operation_category.id` |
| `description` | text | YES |  |  |
| `source_id` | integer | YES |  | FK `source.id` |
| `is_active` | boolean | NO | `true` |  |
| `notes` | text | YES |  |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `created_at` | timestamptz | NO | `now()` |  |
| `updated_at` | timestamptz | NO | `now()` |  |

**Foreign keys:**

- `operation_category_id` -> `operation_category.id` (delete: RESTRICT, update: CASCADE)
- `source_id` -> `source.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `operation_definition_pkey` (id) [PRIMARY]
- `operation_definition_short_code_key` (short_code) [UNIQUE]
- `idx_operation_definition_active` (is_active)
- `idx_operation_definition_category_id` (operation_category_id)

**Unique constraints:**

- `operation_definition_short_code_key` (short_code)

**Values:**

| short_code | name | category |
|---|---|---|
| `biops_2024` | 2024 USBR Biological Opinions (LTO Alt2V1) | biops |
| `biops_modified_2019` | Modified versions of 2019 Biological Opinions (2020 ITP for SWP) | biops |
| `biops_standard` | 2019 Biological Opinions / 2020 ITP for SWP | biops |
| `increase_Shasta_co` | Increased Shasta Reservoir carryover requirements | carryover |
| `alloc_standard` | Standard CVP/SWP allocation priorities | comm_delivery |
| `comm_delivery_HHS` | Prioritize deliveries for minimum human health and safety | comm_delivery |
| `comm_delivery_full` | Prioritize full community water demands | comm_delivery |
| `comm_delivery_functional` | Prioritize deliveries for functional community needs | comm_delivery |
| `cvp_settlement_to_zero` | CVP Settlement contractor allocations reduced to 0% | comm_delivery |
| `delta_outflow_35` | Modified Delta outflow requirements at 35% of unimpaired flow | delta_outflow |
| `delta_outflow_45` | Current Baseline Delta outflow requirements | delta_outflow |
| `delta_outflow_55` | Modified Delta outflow requirements at 55% of unimpaired flow | delta_outflow |
| `delta_outflow_65` | Modified Delta outflow requirements at 65% of unimpaired flow | delta_outflow |
| `delta_regs_standard` | Standard Delta regulations, D1641 requirements | delta_outflow |
| `flow_standard` | Standard minimum flow requirements | flow |
| `functional_flows` | functional flows | flow |
| `no_min_flow` | minimum flow requirements removed | flow |
| `salmon_flows` | modified functional flows adjusted for Winter-run Chinook salmon | flow |
| `SGMA_CV` | Central Valley-wide SGMA-type groundwater pumping limits | gw_restrictions |
| `SGMA_SAC` | Sacramento Valley SGMA-type groundwater pumping limits | gw_restrictions |
| `SGMA_SJV` | San Joaquin Valley SGMA groundwater pumping limits | gw_restrictions |
| `gw_none` | No groundwater restrictions | gw_restrictions |
| `DCP_6000` | Delta Conveyance Project (The Tunnel) transporting at 6000 CFS | infrastructure |
| `DCP_Bethany` | Delta Conveyance Project (The Tunnel) with the proposed Bethany Reservoir alignment | infrastructure |
| `infra_standard` | Standard infrastructure, no Delta Conveyance Project | infrastructure |
| `delta_salinity_standards` | Relaxed Delta salinity standards | regulatory_salinity |
| `TUCP_TUCO` | TUCP's and TUCO's | tucp |
| `tucp_not_active` | Temporary Urgency Change Petitions and Orders not active | tucp |

**Status:** These are the values currently in the DB. They are provisional (see the Layer 05 description above). They were assumed before the water-allocation modeling team produced the metadata. The corrected per-scenario operations were authored in the frontend to meet a frontend deadline (`coeqwal-website`, `opsIcons.tsx` `ICON_REGISTRY` plus `SCENARIO_ICONS`), are accurate as of March 6, 2026, and are the current source of truth. The frontend uses its own icon-id vocabulary that does not match these `short_code`s (`tunnel` vs `DCP_6000`, and so on), so this needs to be sorted. The API does not expose operations yet. A next step is to reconcile the frontend metadata back into this table and deliver it through the API. Tracked in `database/README.md` § Roadmap > "Scenario assumptions and operations metadata".

---

## Layer 06 - SCENARIO

The scenario layer defines individual model runs and the linkages from each scenario to its operational assumptions, operations, and classification tags. Theme definitions and theme-scenario links are in Layer 08.

This is a catalog layer. It holds each scenario's identity and metadata, not its model run results. The computed outputs keyed to a scenario live in later layers. Per-scenario statistics are in Layer 11 and cross-scenario analysis is in Layer 12. Similarly, tier definitions are in Layer 09 and tier results in Layer 10. Those downstream tables reference a scenario by `scenario_short_code`.

**`is_active` semantics:** The `scenario.is_active` boolean controls API visibility, and therefore website visibility, for every scenario. `GET /api/scenarios` filters for `is_active = TRUE`. `GET /api/tiers/{code}` returns 404 for inactive scenarios. There is **no automated process that flips this flag**. After ETL completes for a new scenario, an operator manually flips it via `etl/ingestion/tools/set_scenario_active.py`. The ETL orchestrator (`etl/statistics/run_all.py`) does not touch it. Treat the flag as the deploy gate. Nothing the website surfaces about a scenario is visible until someone runs the tool.

**Developer-FK reminder:** `scenario`, `scenario_author`, and `scenario_hydroclimate_sibling` carry `created_by` / `updated_by` columns whose `developer.id` FK is not yet enforced. `audit_cleanup.sql` § 9 adds them (see the `developer` table note in Layer 00). The live data is clean (integer type, audit ids within `developer.id`), so the constraints add without orphans.

### Table schema

#### `scenario`

The catalog of individual model runs. Each scenario is one model run for one hydroclimate. This is a catalog layer. It holds each scenario's identity and per-run attributes, not its results, and not its operations and assumptions. Operations and assumptions attach to each individual scenario through the Layer 06 link tables (`scenario_key_operation_link`, `scenario_key_assumption_link`), not to the sibling group. So far it has been the case that the operations and assumptions are consistent between siblings, but it may not always be the case.

How the scenario catalog connects to its sibling group and to operations and assumptions (read left to right):

```text
                          +-------------------------------+
                          | scenario_hydroclimate_sibling |   shared metadata:
                          |  PK short_code (varchar)      |   name, descriptions,
                          +-------------------------------+   baseline_group (self-ref)
                                        |  1
                                        |  hydroclimate_sibling = short_code  (ENFORCED)
                                        v  N
   operation_category        +----------------------+        assumption_category
   PK id                     |       scenario       |        PK id
        | 1                  |  PK id               |             | 1
        | N                  +----------------------+             | N
        v                      | 1              1 |               v
   operation_definition        | N              N |          assumption_definition
   PK id  (cat_id, source_id)  v                  v          PK id  (cat_id, source_id)
        | 1            +------------------+  +------------------+        | 1
        | N            | scenario_key_    |  | scenario_key_    |        | N
        v              | operation_link   |  | assumption_link  |        v
   (operation_id) <----| scenario_id  FK  |  | scenario_id  FK  |----> (assumption_id)
                       | operation_id FK  |  | assumption_id FK |
                       +------------------+  +------------------+
                          514 rows               73 rows
                          (PK scenario_id,       (PK scenario_id,
                           operation_id)          assumption_id)
```

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('scenario_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique, e.g. `s0011` |
| `run_name` | varchar | YES |  | full technical run name, e.g. `s0011_adjBL_wTUCP` |
| `is_active` | boolean | NO | `true` | website visibility gate (see semantics above) |
| `hydroclimate_id` | integer | YES |  | intended FK to `hydroclimate.id`, not enforced |
| `hydroclimate_sibling` | varchar | YES |  | FK `scenario_hydroclimate_sibling.short_code` |
| `scenario_version_id` | integer | YES |  | intended FK to `version.id`, not enforced |
| `scenario_author_id` | integer | YES |  | FK `scenario_author.id` |
| `model_source_id` | integer | YES |  | FK `model_source.id` |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `created_at` | timestamptz | NO | `now()` |  |
| `updated_at` | timestamptz | NO | `now()` |  |

**Foreign keys:**

- `hydroclimate_sibling` -> `scenario_hydroclimate_sibling.short_code` (delete: RESTRICT, update: CASCADE)
- `model_source_id` -> `model_source.id` (delete: RESTRICT, update: CASCADE)
- `scenario_author_id` -> `scenario_author.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `scenario_pkey` (id) [PRIMARY]
- `scenario_short_code_key` (short_code) [UNIQUE]
- `idx_scenario_active` (is_active)
- `idx_scenario_active_version` (is_active, scenario_version_id)
- `idx_scenario_hydro_sibling` (hydroclimate_sibling)
- `idx_scenario_hydroclimate` (hydroclimate_id)
- `idx_scenario_run_name_active` (run_name, is_active)

**Unique constraints:**

- `scenario_short_code_key` (short_code)

#### `scenario_hydroclimate_sibling`

Connects the hydroclimate siblings of an operational configuration. A set of hydroclimate siblings is the same or similar operational configuration run under each hydroclimate (historical, cc50, cc95, etc.), differing in hydroclimate inputs. This table groups those siblings and holds the values common to them: the display `name`, `short_description`, `long_description`, and the `baseline_group` lineage. These names and descriptions are intended to surface on the website via the API (see the left scenario sidebar in the Tools section). Each `scenario` points here through its `hydroclimate_sibling` code. The PK is `short_code` (varchar), which reuses the founding scenario's `short_code` (e.g. `s0020`). Operations and assumptions are not stored or linked here. They attach to each individual scenario through the Layer 06 link tables, the thought being that some details could potentially be different between siblings.

**Frontend-data note:** Per-sibling display extras (`shortLabel`, `iconPath`) currently live in the frontend (`coeqwal-website`, `apps/main/app/content/scenarios.ts` `scenarioMetadata`). Somem may belong here alongside `name` and the descriptions, and all should migrate once they settle. See `database/README.md` § Roadmap > "Frontend-hardcoded data that should move into the DB".

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `short_code` | varchar | NO |  | PK, matches hist_adj scenario short_code |
| `name` | varchar | YES |  | display name, e.g. "Current operations" |
| `short_description` | text | YES |  |  |
| `long_description` | text | YES |  | full multi-paragraph description |
| `baseline_group` | varchar | YES |  | self-FK to short_code, NULL for root baselines |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `created_at` | timestamptz | NO | `now()` |  |
| `updated_at` | timestamptz | NO | `now()` |  |

**Foreign keys:**

- `baseline_group` -> `scenario_hydroclimate_sibling.short_code` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `sibling_group_pkey` (short_code) [PRIMARY]
- `idx_hydro_sibling_baseline` (baseline_group)

Root baselines (those with `baseline_group IS NULL`) include `s0011`, `s0022`, `s0038`, `s0065`.

#### `scenario_author`

Authoring organization for a scenario.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('scenario_author_id_seq')` | PK |
| `short_code` | text | NO |  | unique, e.g. `dwr`, `usbr`, `coeqwal` |
| `name` | text | NO |  |  |
| `email` | text | YES |  |  |
| `organization` | text | YES |  |  |
| `affiliation` | text | YES |  |  |
| `is_active` | integer | NO | `1` | integer flag, not boolean (legacy) |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | YES |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `scenario_author_pkey` (id) [PRIMARY]
- `scenario_author_short_code_key` (short_code) [UNIQUE]
- `idx_scenario_author_active` (is_active, short_code)

**Unique constraints:**

- `scenario_author_short_code_key` (short_code)

**Values:**

| short_code | name |
|---|---|
| `dwr` | California Department of Water Resources |
| `usbr` | US Bureau of Reclamation |
| `coeqwal` | COEQWAL modeling team based on model files provided by USBR and DWR |

#### `scenario_tag`

Fine-grained scenario classification tags, distinct from the 6 broad research themes in `theme` (Layer 08). Tags are derived from scenario metadata and used for filtering on the website.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('scenario_tag_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique |
| `label` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `scenario_tag_pkey` (id) [PRIMARY]
- `scenario_tag_short_code_key` (short_code) [UNIQUE]

**Unique constraints:**

- `scenario_tag_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `baseline` | Baseline | Reference baseline scenarios |
| `groundwater` | Groundwater | Groundwater pumping and sustainability scenarios |
| `agriculture` | Agriculture | Agricultural land use and irrigation demand scenarios |
| `flows` | Flows | Instream flow and minimum flow requirement scenarios |
| `drinking_water` | Drinking Water | Community water system delivery prioritization scenarios |
| `infrastructure` | Infrastructure | Water infrastructure modification scenarios |
| `delta` | Delta | Sacramento-San Joaquin Delta regulation scenarios |
| `reservoir` | Reservoir | Reservoir storage and carryover scenarios |
| `salmon` | Salmon | Salmon habitat and survival flow scenarios |
| `environment` | Environment | Environmental and ecological flow scenarios |

#### `scenario_tag_link`

M:N membership between `scenario` and `scenario_tag`. Composite PK `(scenario_id, tag_id)`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `scenario_id` | integer | NO |  | FK `scenario.id`, CASCADE |
| `tag_id` | integer | NO |  | FK `scenario_tag.id`, CASCADE |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `scenario_id` -> `scenario.id` (delete: CASCADE, update: CASCADE)
- `tag_id` -> `scenario_tag.id` (delete: CASCADE, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `scenario_tag_link_pkey` (scenario_id, tag_id) [PRIMARY]
- `idx_scenario_tag_link_reverse` (tag_id, scenario_id)

#### `scenario_key_assumption_link`

Link table: which scenarios carry which assumptions. Composite PK `(scenario_id, assumption_id)`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `scenario_id` | integer | NO |  | FK `scenario.id`, CASCADE |
| `assumption_id` | integer | NO |  | FK `assumption_definition.id`, CASCADE |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | YES |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | YES |  | FK `developer.id` |

**Foreign keys:**

- `assumption_id` -> `assumption_definition.id` (delete: CASCADE, update: CASCADE)
- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `scenario_id` -> `scenario.id` (delete: CASCADE, update: CASCADE)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `scenario_key_assumption_link_pkey` (scenario_id, assumption_id) [PRIMARY]
- `idx_scenario_assumption_reverse` (assumption_id, scenario_id)

#### `scenario_key_operation_link`

Link table: which scenarios carry which operations. Composite PK `(scenario_id, operation_id)`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `scenario_id` | integer | NO |  | FK `scenario.id`, CASCADE |
| `operation_id` | integer | NO |  | FK `operation_definition.id`, CASCADE |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | YES |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | YES |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `operation_id` -> `operation_definition.id` (delete: CASCADE, update: CASCADE)
- `scenario_id` -> `scenario.id` (delete: CASCADE, update: CASCADE)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `scenario_key_operation_link_pkey` (scenario_id, operation_id) [PRIMARY]
- `idx_scenario_operation_reverse` (operation_id, scenario_id)

---

## Layer 07 - HYDROCLIMATE

Hydroclimate conditions (historical and projected) plus a companion sea-level-rise catalog (`slr`). The `slr` table is built and seeded but not yet linked to scenarios or hydroclimates. A planned `hydroclimate_slr_link` table would connect sea-level rise to hydroclimates (see the SLR status and the planned-table note below).

The previously planned `hydroclimate_source` table (a dedicated catalog of hydroclimate data sources) was never built. `hydroclimate.source_id` still FKs notionally to the generic `source` table.

**Developer-FK reminder:** `hydroclimate` is the one table in the whole schema whose audit-FK fix needs pre-work. Its `created_by` / `updated_by` are `numeric` (not integer) and one `created_by` row is NULL, so it is excluded from `audit_cleanup.sql` § 9 and handled by the § 11 (numeric -> integer) -> § 13 (NULL backfill) -> § 14 (add FK) chain instead. See the `developer` table note in Layer 00.

### Table schema

#### `hydroclimate`

**Frontend-data note:** The hydroclimate UI labels and id maps (`HYDROCLIMATE_ID_MAP`, `HYDROCLIMATE_LABEL_MAP`) currently live in the frontend (`coeqwal-website`, `apps/main/app/content/scenarios.ts`). They should migrate into this table. See `database/README.md` § Roadmap > "Frontend-hardcoded data that should move into the DB".

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('hydroclimate_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `name` | text | YES |  | display name |
| `subtitle` | text | YES |  |  |
| `short_title` | text | YES |  |  |
| `simple_description` | text | YES |  |  |
| `description` | text | YES |  |  |
| `is_active` | integer | NO | `1` |  |
| `narrative` | jsonb | YES |  |  |
| `projection_year` | text | YES |  | May be an obsolete column, not sure what to use this for as hydroclimate descriptions have drifted |
| `source_id` | integer | YES |  | notional FK to `source.id`, not enforced |
| `notes` | text | YES |  |  |
| `hydroclimate_version_id` | numeric | YES |  | numeric not integer (legacy), notional FK to `version.id` |
| `created_by` | numeric | YES |  | numeric not integer (legacy), notional FK to `developer.id` |
| `updated_by` | numeric | YES |  | numeric not integer (legacy), notional FK to `developer.id` |
| `created_at` | timestamptz | YES | `now()` |  |
| `updated_at` | timestamptz | YES | `now()` |  |

**Foreign keys:** none yet. `source_id`, `hydroclimate_version_id`, `created_by`, and `updated_by` are notional FK columns whose constraints were deferred while the table is being settled (see Status).

**Status:** Tracked in `database/SCHEMA_BACKLOG.md` § 1 (hydroclimate FKs and the `slr` linkage).

**Indexes:**

- `hydroclimate_pkey` (id) [PRIMARY]
- `hydroclimate_short_code_key` (short_code) [UNIQUE]
- `idx_hydroclimate_active` (is_active, short_code)
- `idx_hydroclimate_source` (source_id)

**Unique constraints:**

- `hydroclimate_short_code_key` (short_code)

**Values:**

| short_code | name | description |
|---|---|---|
| `dwr_hist` | DWR historical | Historical hydrology (1922-2021) |
| `dwr_hist_adj` | DWR historically-adjusted | Adjusted for 20th-century climate warming |
| `cc50` | Warmer and Drier I | 50% exceedance, median future (+1.5C, -3% precip, 2043) |
| `cc95` | Warmer and Drier II | 95% exceedance, extreme hot/dry (+1.8C, -9% precip, 2043) |
| `CMIP6_TaiESM1_SSP370` | Warmer and Drier III | +1.9C, -7% precip, 2043 |
| `CMIP6_CESM2-LENS_SSP370` | Warmer and Drier IV | +1.4C, -12% precip, 2043 |

Earlier columns `slr_value` and `slr_unit_id` were dropped from this table. SLR is now in the dedicated `slr` table.

#### `slr`

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('slr_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `label` | text | NO |  |  |
| `slr_value_mm` | numeric | YES |  | SLR amount in mm (0, 15, 30, 60) |
| `description` | text | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |
| `source` | text | YES |  | FK `source.source` (text-to-text FK, not standard pattern) |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `source` -> `source.source` (delete: NO ACTION, update: NO ACTION): text-to-text FK
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `slr_pkey` (id) [PRIMARY]
- `slr_short_code_key` (short_code) [UNIQUE]
- `idx_slr_active` (is_active)

**Unique constraints:**

- `slr_short_code_key` (short_code)

**Values:**

| short_code | label | description |
|---|---|---|
| `none` | No sea level rise | Baseline, no sea level rise applied |
| `slr_15` | 15mm sea level rise | 15mm sea level rise scenario |
| `slr_30` | 30mm sea level rise | 30mm sea level rise scenario |
| `slr_60` | 60mm sea level rise | 60mm sea level rise scenario |

**Status:** The `slr` catalog is built and seeded (4 rows) with enforced audit and `source` FKs, but nothing links to it yet (no scenario, hydroclimate, ETL, API, or frontend reference). The plan is to connect sea-level-rise conditions to hydroclimates through a new `hydroclimate_slr_link` table, documented below as planned. Tracked in `database/SCHEMA_BACKLOG.md` § 1 (hydroclimate FKs and the `slr` linkage).

#### `hydroclimate_slr_link` (planned, not yet built)

Planned junction table to attach sea-level-rise conditions to hydroclimates. It does not exist in the live schema yet. Intended shape, one row per hydroclimate / SLR pairing:

| column | type | nullable | notes |
|---|---|---|---|
| `hydroclimate_id` | integer | NO | FK `hydroclimate.id` |
| `slr_id` | integer | NO | FK `slr.id` |
| `created_by` | integer | NO | FK `developer.id` |
| `updated_by` | integer | NO | FK `developer.id` |
| `created_at` | timestamptz | NO | `now()` |
| `updated_at` | timestamptz | NO | `now()` |

Intended PK `(hydroclimate_id, slr_id)`. The cardinality is being settled with the current `hydroclimate` rework. If each hydroclimate carries exactly one SLR condition, a single `hydroclimate.slr_id` column would replace the link table. Tracked in `database/SCHEMA_BACKLOG.md` § 1 (hydroclimate FKs and the `slr` linkage).

---

## Layer 08 - THEME

Research themes organize the scenarios. Each theme links to one or more scenarios.

**Status:** These tables were built before the themes were finalized. The themes themselves, along with their names and descriptions, are still being developed, so the current rows and the theme-to-scenario assignments are provisional. The working copy currently lives in the frontend (see the Frontend-data note on `theme`).

**Developer-FK reminder:** `theme` carries `created_by` / `updated_by` columns whose `developer.id` FK is not yet enforced. `audit_cleanup.sql` § 9 adds them (see the `developer` table note in Layer 00). The live data is clean (integer type, audit ids within `developer.id`), so the constraints add without orphans.

### Table schema

#### `theme`

**Frontend-data note:** Theme narrative copy and the theme-to-scenario assignment currently live in the frontend (`coeqwal-website`, `apps/main/app/content/themes.ts` `WATER_THEMES` and `apps/main/app/content/scenarios.ts` `scenarioMetadata[...].theme`). No theme API endpoint exists today. They should migrate into this table and `theme_scenario_link`. See `database/README.md` § Roadmap > "Frontend-hardcoded data that should move into the DB".

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('theme_id_seq')` | PK |
| `short_code` | text | NO |  | unique |
| `is_active` | integer | NO | `1` |  |
| `name` | text | NO |  |  |
| `subtitle` | text | YES |  |  |
| `short_title` | text | YES |  |  |
| `simple_description` | text | YES |  |  |
| `description` | text | YES |  |  |
| `description_next` | text | YES |  |  |
| `narrative` | text | YES |  |  |
| `outcome_description` | text | YES |  |  |
| `outcome_narrative` | text | YES |  |  |
| `theme_version_id` | integer | NO |  | notional FK to `version.id`, not enforced |
| `created_by` | integer | NO |  | notional FK to `developer.id`, not enforced |
| `updated_by` | integer | YES |  | notional FK to `developer.id`, not enforced |
| `created_at` | timestamptz | YES | `now()` |  |
| `updated_at` | timestamptz | YES | `now()` |  |
| `source` | text | YES |  | FK `source.source`  |

**Foreign keys:**

- `source` -> `source.source` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `theme_pkey` (id) [PRIMARY]
- `theme_short_code_key` (short_code) [UNIQUE]
- `idx_theme_active` (is_active)
- `idx_theme_short_code_active` (short_code, is_active): redundant against the unique `short_code` index, dropped by `audit_cleanup.sql` § 8

**Unique constraints:**

- `theme_short_code_key` (short_code)

**Values:**

| short_code | name |
|---|---|
| `cws` | Community water systems |
| `ag_gw` | Farms, groundwater & food systems |
| `eco` | Rivers, salmon & ecosystems |
| `delta` | The Delta as a living place |
| `climate` | Drought, climate risk, and resilience |
| `governance` | Operations and impacts |

#### `theme_scenario_link`

M:N membership between `theme` and `scenario`. Composite PK `(scenario_id, theme_id)`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `theme_id` | integer | NO |  | FK `theme.id`, CASCADE |
| `scenario_id` | integer | NO |  | FK `scenario.id`, CASCADE |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | YES |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | YES |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)
- `scenario_id` -> `scenario.id` (delete: CASCADE, update: CASCADE)
- `theme_id` -> `theme.id` (delete: CASCADE, update: CASCADE)
- `updated_by` -> `developer.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `theme_scenario_link_pkey` (scenario_id, theme_id) [PRIMARY]
- `idx_theme_scenario_reverse` (scenario_id, theme_id): redundant, its columns match the PK exactly. The name implies a reverse `(theme_id, scenario_id)` lookup, but it was created in PK column order, so it never served that purpose. Dropped by `audit_cleanup.sql` § 8

---

## Layer 09 - TIER

The tier rubric. The tier system summarizes scenario performance against key outcome categories (community water systems, environmental flows, etc.). Two tables make up the rubric:

- `tier_definition` defines each tier indicator.
- `tier_location` lists which Layer 03 entities each tier indicator considers.

The per-scenario tier outputs (`tier_result`, `tier_location_result`) live in Layer 10. On the website, what the team broadly calls "tiers" is called "key outcomes".

`tier_location` is populated by tier results ETL. The locations in the latest tier results csv's in `etl/tier_data/staging` are the source of truth. These locations can shift and drift as the tier teams develop their results. The writer is `etl/tier_data/scripts/sync_tier_locations_from_staging.py`, which upserts active rows and soft-deletes (is_active = FALSE) anything that left staging (it never hard-deletes, in case we want to switch it back on again in the future). A validation gate refuses any `location_id` that does not resolve in the entity tables named by `etl/common/tier_location_entities.py` (the script exits with code 2) unless `--allow-unresolved` is passed, and `--dry-run` prints the plan without committing.

### Table schema

#### `tier_definition`

The catalog of tier indicators.

**Frontend-data note:** What the frontend calls a *key outcome* is one tier indicator, that is, one `tier_definition` row (on the website "tiers" are surfaced as "key outcomes", see the layer description above). The frontend's outcome codes are this table's `short_code` values. `outcomes.ts` labels `OUTCOME_NAMES` as a map from "API short codes" to display names, and its nine codes (`CWS_DEL`, `AG_REV`, `ENV_FLOWS`, `RES_STOR`, `GW_STOR`, `DELTA_ECO`, `FW_EXP`, `FW_DELTA_USES`, `WRC_SALMON_AB`) are exactly this table's nine rows. The vocabulary differs but the entities are the same. By column: `OUTCOME_NAMES[code]` is the display name, which already exists here as `name`, though the frontend uses shorter wording (e.g. DB `Community water system deliveries` vs frontend `Community deliveries`). `OUTCOME_CODE_ORDER` is the display order, which has no column here yet. Migrating means reconciling `name` with the frontend labels and adding a display-order column. See `database/README.md` § Roadmap > "Frontend-hardcoded data that should move into the DB".

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('tier_definition_id_seq')` | PK |
| `short_code` | varchar | NO |  | unique |
| `name` | varchar | NO |  |  |
| `description` | text | YES |  |  |
| `tier_type` | varchar | NO |  | indicator domain, e.g. `reservoir`, `compliance`, `wba` |
| `tier_count` | integer | NO |  | number of tier levels for this indicator, typically 4, but salmon abundance seems a little different? |
| `tier_version_id` | integer | NO |  | FK `version.id` |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `tier_version_id` -> `version.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `tier_definition_pkey` (id) [PRIMARY]
- `tier_definition_short_code_key` (short_code) [UNIQUE]
- `idx_tier_definition_active` (is_active)
- `idx_tier_definition_tier_type` (tier_type)
- `idx_tier_definition_version` (tier_version_id)

**Unique constraints:**

- `tier_definition_short_code_key` (short_code)

#### `tier_location`

Catalog of (tier, location) pairs, i.e. which Layer 03 entities each tier indicator applies to and aggregates. **Does not carry the `set_audit_fields` trigger**, a known gap (the trigger was never attached when the table was recently added :P). The four audit columns are present and populated by the sync script. `audit_cleanup.sql` § 4 attaches `audit_fields_tier_location`. Also tracked in `database/SCHEMA_BACKLOG.md` § 4 Corrections.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('tier_location_id_seq')` | PK |
| `tier_short_code` | varchar | NO |  | FK `tier_definition.short_code` |
| `location_type` | varchar | NO |  | one of `reservoir`, `compliance_station`, `wba`, ... |
| `location_id` | varchar | NO |  | the matching entity's natural key (e.g. `reservoir.calsim_short_code`, `compliance_station.station_code`, `wba.wba_id`) |
| `display_order` | integer | NO | `1` |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `tier_short_code` -> `tier_definition.short_code` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `tier_location_pkey` (id) [PRIMARY]
- `tier_location_tier_short_code_location_id_key` (tier_short_code, location_id) [UNIQUE]
- `idx_tier_location_active` (is_active)

**Unique constraints:**

- `tier_location_tier_short_code_location_id_key` (tier_short_code, location_id)

---

## Layer 10 - TIER RESULTS

Per-scenario tier rollups. Two tables. The values are computed by the tier team and loaded from CSV staging via the tier-data ETL (see `etl/tier_data/README.md`). Both tables reference the rubric in Layer 09 (`tier_definition.short_code`).

### Table schema

#### `tier_location_result`

Per-scenario, per-location tier values. **Three unique indexes/constraints cover `(scenario_short_code, tier_short_code, location_id, tier_version_id)`**: one unique constraint plus two redundant standalone unique indexes. `audit_cleanup.sql` § 8 drops the two duplicates (`idx_tier_location_unique`, `tier_location_result_unique`) and keeps the constraint.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('tier_location_result_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  | keys to `scenario.short_code` |
| `tier_short_code` | varchar | NO |  | FK `tier_definition.short_code` |
| `location_type` | varchar | NO |  |  |
| `location_id` | varchar | NO |  |  |
| `location_name` | varchar | YES |  |  |
| `tier_level` | integer | YES |  |  |
| `tier_value` | integer | YES |  |  |
| `display_order` | integer | YES | `1` |  |
| `tier_version_id` | integer | NO |  | FK `version.id` |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `tier_short_code` -> `tier_definition.short_code` (delete: RESTRICT, update: CASCADE)
- `tier_version_id` -> `version.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `tier_location_result_pkey` (id) [PRIMARY]
- `idx_tier_location_unique` (scenario_short_code, tier_short_code, location_id, tier_version_id) [UNIQUE] - duplicate of the constraint below, dropped by `audit_cleanup.sql` § 8
- `tier_location_result_scenario_short_code_tier_short_code_lo_key` (scenario_short_code, tier_short_code, location_id, tier_version_id) [UNIQUE]
- `tier_location_result_unique` (scenario_short_code, tier_short_code, location_id, tier_version_id) [UNIQUE] - duplicate of the constraint above, dropped by `audit_cleanup.sql` § 8
- `idx_tier_location_combined` (scenario_short_code, tier_short_code)
- `idx_tier_location_level` (tier_level)
- `idx_tier_location_scenario` (scenario_short_code): left-prefix duplicate of `idx_tier_location_combined`, dropped by `audit_cleanup.sql` § 8
- `idx_tier_location_tier` (tier_short_code)
- `idx_tier_location_type` (location_type)

**Unique constraints:**

- `tier_location_result_scenario_short_code_tier_short_code_lo_key` (scenario_short_code, tier_short_code, location_id, tier_version_id)

#### `tier_result`

Per-scenario, per-tier rolled-up tier results.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('tier_result_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  | keys to `scenario.short_code` |
| `tier_short_code` | varchar | NO |  | FK `tier_definition.short_code` |
| `tier_1_value` | integer | YES |  | count of locations at tier 1 |
| `tier_2_value` | integer | YES |  |  |
| `tier_3_value` | integer | YES |  |  |
| `tier_4_value` | integer | YES |  |  |
| `norm_tier_1` | numeric | YES |  | normalized fraction at tier 1 |
| `norm_tier_2` | numeric | YES |  |  |
| `norm_tier_3` | numeric | YES |  |  |
| `norm_tier_4` | numeric | YES |  |  |
| `total_value` | integer | YES |  |  |
| `single_tier_level` | integer | YES |  | for indicators that produce a single overall tier |
| `tier_version_id` | integer | NO |  | FK `version.id` |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | YES | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | YES | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `tier_short_code` -> `tier_definition.short_code` (delete: RESTRICT, update: CASCADE)
- `tier_version_id` -> `version.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `tier_result_pkey` (id) [PRIMARY]
- `tier_result_scenario_short_code_tier_short_code_tier_versio_key` (scenario_short_code, tier_short_code, tier_version_id) [UNIQUE]
- `idx_tier_result_active` (is_active)
- `idx_tier_result_scenario` (scenario_short_code)
- `idx_tier_result_scenario_tier` (scenario_short_code, tier_short_code)
- `idx_tier_result_tier` (tier_short_code)
- `idx_tier_result_version` (tier_version_id)

**Unique constraints:**

- `tier_result_scenario_short_code_tier_short_code_tier_versio_key` (scenario_short_code, tier_short_code, tier_version_id)

---

## Layer 11 - PER-SCENARIO STATISTICS

Statistics computed by the ETL pipeline from CalSim DV/SV files, one row per `(scenario, entity, time-bucket)`. [Reference](https://docs.google.com/spreadsheets/d/1xcQIR_J96-cs7BuCrXjznwkinLgxl-Pf9tA3mJ2GiyA/edit?gid=1094338461#gid=1094338461). The website reads these directly through the API. The same shape appears across most tables:

- `scenario_short_code varchar` keys to `scenario.short_code` (text key, no DB-enforced FK).
- An entity reference
- For monthly tables: `water_month integer` (1-12 starting at October).
- For period_summary tables: `simulation_start_year`, `simulation_end_year`, `total_years`.
- Statistics columns named per the `statistic_type` convention: `mean`, `median`, `cv`, `stdev`, `q0`..`q100` for percentile bands, `exc_p5`..`exc_p95` for exceedances.
- Standard audit columns (`created_at`, `created_by`, `updated_at`, `updated_by`), populated by the `set_audit_fields` trigger.

The website-facing API endpoints in `api/coeqwal-api/routes/` filter by `scenario.is_active = TRUE` before returning result rows. None of the result tables carry their own `is_active` per-row except a small set noted in their entries below.

**Developer-FK reminder:** `audit_cleanup.sql` § 9 adds the missing `created_by` / `updated_by` -> `developer.id` FKs to the result tables that lack them (see the `developer` table note in Layer 00).

**Redundant indexes:** Each result table was created with overlapping indexes: a combined/unique index on `(scenario_short_code, <entity>)` plus a left-prefix index on `(scenario_short_code)` alone, and in a few cases an abbreviated exact-duplicate index. `audit_cleanup.sql` drops both the exact and the left-prefix duplicates.

**Coverage:** Every modeled demand/supply sector has a result family here, and each maps to one ETL module in `etl/statistics/` and one route module in `api/coeqwal-api/routes/`. Two domains are intentionally absent: `compliance_station` results surface through tier indicators (Layer 09/10, e.g. `FW_DELTA_USES`) rather than a statistics table, and WBA / groundwater results have no family yet. Groundwater appears only as DU-level `ag_du_gw_pumping_monthly`. The planned `wba_variable` table and the DU-WBA crosswalk are tracked in [`database/README.md`](../README.md) § Roadmap.

### Table schema

#### Reservoir result family

Four tables, all keyed by `(scenario_short_code, reservoir_entity_id, water_month)` or by `(scenario_short_code, reservoir_entity_id)` for period summaries. `reservoir_entity_id` is the only FK. The rest of the columns are statistics.

##### `reservoir_storage_monthly`

Per-month storage statistics with both raw and TAF-denominated percentile/exceedance columns

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('reservoir_storage_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  | keys to `scenario.short_code` |
| `reservoir_entity_id` | integer | NO |  | FK `reservoir_entity.id` |
| `water_month` | integer | NO |  | 1-12 starting Oct |
| `storage_avg_taf` | numeric | YES |  |  |
| `storage_cv` | numeric | YES |  | coefficient of variation |
| `storage_pct_capacity` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands (raw, % of capacity scale) |
| `q0_taf`..`q100_taf` | numeric | YES |  | percentile bands in TAF |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles (raw) |
| `exc_p5_taf`..`exc_p95_taf` | numeric | YES |  | exceedance percentiles in TAF |
| `capacity_taf` | numeric | YES |  | denormalized capacity (matches `reservoir_entity.capacity_taf`) |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  |  |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  |  |

**Foreign keys:**

- `reservoir_entity_id` -> `reservoir_entity.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `reservoir_storage_monthly_pkey` (id) [PRIMARY]
- `uq_storage_monthly` (scenario_short_code, reservoir_entity_id, water_month) [UNIQUE]
- `idx_reservoir_storage_scenario` (scenario_short_code, reservoir_entity_id)
- `idx_storage_monthly_active` (is_active)
- `idx_storage_monthly_combined` (scenario_short_code, reservoir_entity_id)
- `idx_storage_monthly_entity` (reservoir_entity_id)
- `idx_storage_monthly_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_storage_monthly` (scenario_short_code, reservoir_entity_id, water_month)

The percentile-bands columns are expanded inline rather than collapsed to a row range because each is independently consumed by the API. See `statistic_type` in Layer 01 for the naming convention.

##### `reservoir_spill_monthly`

Per-month spill statistics.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('reservoir_spill_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `reservoir_entity_id` | integer | NO |  | FK `reservoir_entity.id` |
| `water_month` | integer | NO |  |  |
| `spill_months_count` | integer | YES |  |  |
| `total_months` | integer | YES |  |  |
| `spill_frequency_pct` | numeric | YES |  |  |
| `spill_avg_cfs` | numeric | YES |  |  |
| `spill_max_cfs` | numeric | YES |  |  |
| `spill_q50`, `spill_q90`, `spill_q100` | numeric | YES |  | spill percentile values |
| `storage_at_spill_avg_pct` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `reservoir_entity_id` -> `reservoir_entity.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `reservoir_spill_monthly_pkey` (id) [PRIMARY]
- `uq_spill_monthly` (scenario_short_code, reservoir_entity_id, water_month) [UNIQUE]
- `idx_reservoir_spill_scenario` (scenario_short_code, reservoir_entity_id)
- `idx_spill_monthly_active` (is_active)
- `idx_spill_monthly_combined` (scenario_short_code, reservoir_entity_id)
- `idx_spill_monthly_entity` (reservoir_entity_id)
- `idx_spill_monthly_frequency` (spill_frequency_pct)
- `idx_spill_monthly_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_spill_monthly` (scenario_short_code, reservoir_entity_id, water_month)

##### `reservoir_monthly_percentile`

Per-month storage percentile bands (raw % of capacity and TAF) plus a mean (`mean_value` / `mean_taf`). Its seven `q*` bands (raw and TAF) and `capacity_taf` duplicate the same columns in `reservoir_storage_monthly`. It omits that table's exceedance, `storage_cv`, `storage_pct_capacity`, `storage_avg_taf`, and `sample_count` columns, and uniquely adds the two mean columns. The overlapping band columns are a historical redundancy from the two tables being built separately, tracked for consolidation in `database/SCHEMA_BACKLOG.md` § 6 Corrections.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('reservoir_monthly_percentile_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `reservoir_entity_id` | integer | NO |  | FK `reservoir_entity.id` |
| `water_month` | integer | NO |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands (raw) |
| `mean_value` | numeric | YES |  |  |
| `q0_taf`..`q100_taf` | numeric | YES |  | percentile bands in TAF |
| `mean_taf` | numeric | YES |  |  |
| `capacity_taf` | numeric | YES |  | denormalized |
| `is_active` | boolean | YES | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `reservoir_entity_id` -> `reservoir_entity.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `reservoir_monthly_percentile_pkey` (id) [PRIMARY]
- `uq_reservoir_percentile` (scenario_short_code, reservoir_entity_id, water_month) [UNIQUE]
- `idx_reservoir_percentile_active` (is_active)
- `idx_reservoir_percentile_combined` (scenario_short_code, reservoir_entity_id)
- `idx_reservoir_percentile_entity` (reservoir_entity_id)
- `idx_reservoir_percentile_reservoir` (reservoir_entity_id)
- `idx_reservoir_percentile_scenario` (scenario_short_code)
- `idx_reservoir_percentile_scenario_entity` (scenario_short_code, reservoir_entity_id)

**Unique constraints:**

- `uq_reservoir_percentile` (scenario_short_code, reservoir_entity_id, water_month)

##### `reservoir_period_summary`

Per-scenario, per-reservoir whole-simulation-period summary. Carries flood-pool probability (overall, September, and April), dead-pool probability (overall and September), per-period CV, and per-period storage-exceedance percentiles.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('reservoir_period_summary_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `reservoir_entity_id` | integer | NO |  | FK `reservoir_entity.id` |
| `simulation_start_year`, `simulation_end_year`, `total_years` | integer | NO |  |  |
| `storage_exc_p5`..`storage_exc_p95` | numeric | YES |  | storage exceedance percentiles |
| `dead_pool_taf`, `dead_pool_pct` | numeric | YES |  | `dead_pool_taf` denormalized from `reservoir_entity`, `dead_pool_pct` computed as `dead_pool_taf / capacity_taf * 100` |
| `spill_threshold_pct`, `spill_years_count`, `spill_frequency_pct` | various | YES |  |  |
| `spill_mean_cfs`, `spill_peak_cfs` | numeric | YES |  |  |
| `annual_spill_avg_taf`, `annual_spill_cv`, `annual_spill_max_taf` | numeric | YES |  |  |
| `annual_max_spill_q50`, `annual_max_spill_q90`, `annual_max_spill_q100` | numeric | YES |  |  |
| `capacity_taf` | numeric | YES |  | denormalized |
| `flood_pool_prob_all`, `flood_pool_prob_september`, `flood_pool_prob_april` | numeric | YES |  | probability of being in flood pool |
| `dead_pool_prob_all`, `dead_pool_prob_september` | numeric | YES |  |  |
| `storage_cv_all`, `storage_cv_april`, `storage_cv_september` | numeric | YES |  |  |
| `annual_avg_taf`, `april_avg_taf`, `september_avg_taf` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `reservoir_entity_id` -> `reservoir_entity.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `reservoir_period_summary_pkey` (id) [PRIMARY]
- `uq_period_summary` (scenario_short_code, reservoir_entity_id) [UNIQUE]
- `idx_period_summary_active` (is_active)
- `idx_period_summary_cv` (storage_cv_all)
- `idx_period_summary_dead_pool_prob` (dead_pool_prob_all)
- `idx_period_summary_dead_prob` (dead_pool_prob_all)
- `idx_period_summary_entity` (reservoir_entity_id)
- `idx_period_summary_flood_prob` (flood_pool_prob_all)
- `idx_period_summary_scenario` (scenario_short_code)
- `idx_period_summary_spill_freq` (spill_frequency_pct)

**Unique constraints:**

- `uq_period_summary` (scenario_short_code, reservoir_entity_id)

#### Delta result family

Two tables holding per-scenario delta outflow and X2 statistics. Keyed by `(scenario_short_code, variable_code)` rather than by a Layer-03 entity FK. `variable_code` is the CalSim/derived variable name (e.g. `NDO`, `X2`). These two tables do **not** carry an `is_active` column.

##### `delta_monthly`

Per-scenario, per-variable monthly statistics for Delta variables.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('delta_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `variable_code` | varchar | NO |  | e.g. `NDO`, `X2` |
| `water_month` | integer | NO |  |  |
| `avg`, `cv` | numeric | YES |  |  |
| `unit` | varchar | YES |  |  |
| `sample_count` | integer | YES |  |  |
| `avg_cfs` | numeric | YES |  | denormalized to CFS |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `delta_monthly_pkey` (id) [PRIMARY]
- `uq_delta_monthly` (scenario_short_code, variable_code, water_month) [UNIQUE]
- `idx_delta_monthly_scenario` (scenario_short_code)
- `idx_delta_monthly_variable` (variable_code)

**Unique constraints:**

- `uq_delta_monthly` (scenario_short_code, variable_code, water_month)

##### `delta_period_summary`

Per-scenario, per-variable whole-simulation-period summary. Uses a single `jsonb` column for statistic payload rather than expanding the percentile / exceedance columns.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('delta_period_summary_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `variable_code` | varchar | NO |  |  |
| `label` | varchar | YES |  |  |
| `category` | varchar | YES |  |  |
| `native_unit` | varchar | YES |  |  |
| `simulation_start_year`, `simulation_end_year`, `total_years` | integer | YES |  |  |
| `summary_data` | jsonb | NO | `'{}'` | per-variable stat payload |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `delta_period_summary_pkey` (id) [PRIMARY]
- `uq_delta_period_summary` (scenario_short_code, variable_code) [UNIQUE]
- `idx_delta_summary_category` (category)
- `idx_delta_summary_scenario` (scenario_short_code)
- `idx_delta_summary_variable` (variable_code)

**Unique constraints:**

- `uq_delta_period_summary` (scenario_short_code, variable_code)

#### Urban demand-unit result family

The DU urban result tables are keyed by `(scenario_short_code, du_id, water_month)` (monthly) or `(scenario_short_code, du_id)` (period). `du_id varchar` references `du_urban_entity.du_id` (or any DU domain) but **no FK is enforced**. The columns are textual. M&I and refuge result tables share the same shape.

##### `du_delivery_monthly`

Per-DU monthly delivery statistics.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_delivery_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  | matches `du_urban_entity.du_id`, no FK |
| `water_month` | integer | NO |  |  |
| `delivery_avg_taf`, `delivery_cv` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `demand_avg_taf`, `percent_of_demand_avg` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `du_delivery_monthly_pkey` (id) [PRIMARY]
- `uq_du_delivery_monthly` (scenario_short_code, du_id, water_month) [UNIQUE]
- `idx_du_delivery_monthly_combined` (scenario_short_code, du_id)
- `idx_du_delivery_monthly_du_id` (du_id)
- `idx_du_delivery_monthly_scenario` (scenario_short_code)
- `idx_du_delivery_scenario_du` (scenario_short_code, du_id)

**Unique constraints:**

- `uq_du_delivery_monthly` (scenario_short_code, du_id, water_month)

##### `du_shortage_monthly`

Per-DU monthly shortage statistics. Same shape as `du_delivery_monthly` plus a `shortage_frequency_pct` column.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_shortage_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  | matches `du_urban_entity.du_id`, no FK |
| `water_month` | integer | NO |  |  |
| `shortage_avg_taf`, `shortage_cv`, `shortage_frequency_pct` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `du_shortage_monthly_pkey` (id) [PRIMARY]
- `uq_du_shortage_monthly` (scenario_short_code, du_id, water_month) [UNIQUE]
- `idx_du_shortage_monthly_combined` (scenario_short_code, du_id)
- `idx_du_shortage_monthly_du_id` (du_id)
- `idx_du_shortage_monthly_scenario` (scenario_short_code)
- `idx_du_shortage_scenario_du` (scenario_short_code, du_id)

**Unique constraints:**

- `uq_du_shortage_monthly` (scenario_short_code, du_id, water_month)

##### `du_period_summary`

Per-DU period summary. Carries reliability and per-period delivery+shortage exceedance percentiles.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('du_period_summary_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  | matches `du_urban_entity.du_id`, no FK |
| `simulation_start_year`, `simulation_end_year`, `total_years` | integer | NO |  |  |
| `annual_delivery_avg_taf`, `annual_delivery_cv` | numeric | YES |  |  |
| `delivery_exc_p5`..`delivery_exc_p95` | numeric | YES |  |  |
| `annual_shortage_avg_taf` | numeric | YES |  |  |
| `shortage_years_count` | integer | YES |  |  |
| `shortage_frequency_pct` | numeric | YES |  |  |
| `shortage_exc_p5`..`shortage_exc_p95` | numeric | YES |  |  |
| `reliability_pct`, `avg_pct_demand_met`, `annual_demand_avg_taf` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `du_period_summary_pkey` (id) [PRIMARY]
- `uq_du_period_summary` (scenario_short_code, du_id) [UNIQUE]
- `idx_du_period_scenario_du` (scenario_short_code, du_id)
- `idx_du_period_summary_du_id` (du_id)
- `idx_du_period_summary_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_du_period_summary` (scenario_short_code, du_id)

#### M&I contractor result family

Same shape as the DU result family but keyed by `mi_contractor_code varchar` (matches `mi_contractor.short_code`, no FK enforced).

##### `mi_delivery_monthly`

Per-contractor monthly delivery statistics.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('mi_delivery_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `mi_contractor_code` | varchar | NO |  | matches `mi_contractor.short_code`, no FK |
| `water_month` | integer | NO |  |  |
| `delivery_avg_taf`, `delivery_cv` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `demand_avg_taf`, `percent_of_demand_avg` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `mi_delivery_monthly_pkey` (id) [PRIMARY]
- `uq_mi_delivery_monthly` (scenario_short_code, mi_contractor_code, water_month) [UNIQUE]
- `idx_mi_delivery_monthly_combined` (scenario_short_code, mi_contractor_code)
- `idx_mi_delivery_monthly_contractor` (mi_contractor_code)
- `idx_mi_delivery_monthly_scenario` (scenario_short_code)
- `idx_mi_delivery_scenario_contractor` (scenario_short_code, mi_contractor_code)

**Unique constraints:**

- `uq_mi_delivery_monthly` (scenario_short_code, mi_contractor_code, water_month)

##### `mi_shortage_monthly`

Per-contractor monthly shortage statistics. Same shape as `du_shortage_monthly`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('mi_shortage_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `mi_contractor_code` | varchar | NO |  |  |
| `water_month` | integer | NO |  |  |
| `shortage_avg_taf`, `shortage_cv`, `shortage_frequency_pct` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `mi_shortage_monthly_pkey` (id) [PRIMARY]
- `uq_mi_shortage_monthly` (scenario_short_code, mi_contractor_code, water_month) [UNIQUE]
- `idx_mi_shortage_monthly_combined` (scenario_short_code, mi_contractor_code)
- `idx_mi_shortage_monthly_contractor` (mi_contractor_code)
- `idx_mi_shortage_monthly_scenario` (scenario_short_code)
- `idx_mi_shortage_scenario_contractor` (scenario_short_code, mi_contractor_code)

**Unique constraints:**

- `uq_mi_shortage_monthly` (scenario_short_code, mi_contractor_code, water_month)

##### `mi_contractor_period_summary`

Same shape as `du_period_summary` plus a denormalized `contract_amount_taf` (matches `mi_contractor.contract_amount_taf`).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('mi_contractor_period_summary_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `mi_contractor_code` | varchar | NO |  |  |
| `simulation_start_year`, `simulation_end_year`, `total_years` | integer | NO |  |  |
| `annual_delivery_avg_taf`, `annual_delivery_cv` | numeric | YES |  |  |
| `delivery_exc_p5`..`delivery_exc_p95` | numeric | YES |  |  |
| `annual_shortage_avg_taf` | numeric | YES |  |  |
| `shortage_years_count` | integer | YES |  |  |
| `shortage_frequency_pct` | numeric | YES |  |  |
| `shortage_exc_p5`..`shortage_exc_p95` | numeric | YES |  |  |
| `reliability_pct`, `avg_pct_demand_met`, `annual_demand_avg_taf` | numeric | YES |  |  |
| `contract_amount_taf` | numeric | YES |  | denormalized from mi_contractor |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `mi_contractor_period_summary_pkey` (id) [PRIMARY]
- `uq_mi_period_summary` (scenario_short_code, mi_contractor_code) [UNIQUE]
- `idx_mi_period_scenario_contractor` (scenario_short_code, mi_contractor_code)
- `idx_mi_period_summary_contractor` (mi_contractor_code)
- `idx_mi_period_summary_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_mi_period_summary` (scenario_short_code, mi_contractor_code)

#### CWS aggregate result family

Same shape as the DU/MI families but keyed by `cws_aggregate_id integer` (proper FK to `cws_aggregate_entity.id`). These tables are the project-level community-water-system rollups (SWP/CVP NOD/SOD totals, MWD).

##### `cws_aggregate_monthly`

Carries both delivery and shortage statistics in the same row (rather than splitting into two tables like the DU/MI families).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('cws_aggregate_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `cws_aggregate_id` | integer | NO |  | FK `cws_aggregate_entity.id` |
| `water_month` | integer | NO |  |  |
| `delivery_avg_taf`, `delivery_cv` | numeric | YES |  |  |
| `delivery_q0`..`delivery_q100` | numeric | YES |  | delivery percentile bands |
| `delivery_exc_p5`..`delivery_exc_p95` | numeric | YES |  | delivery exceedance percentiles |
| `shortage_avg_taf`, `shortage_cv`, `shortage_frequency_pct` | numeric | YES |  |  |
| `shortage_q0`..`shortage_q100` | numeric | YES |  | shortage percentile bands |
| `shortage_exc_p5`..`shortage_exc_p95` | numeric | YES |  | shortage exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `demand_avg_taf`, `percent_of_demand_avg` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `cws_aggregate_id` -> `cws_aggregate_entity.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `cws_aggregate_monthly_pkey` (id) [PRIMARY]
- `uq_cws_aggregate_monthly` (scenario_short_code, cws_aggregate_id, water_month) [UNIQUE]
- `idx_cws_agg_monthly_aggregate` (cws_aggregate_id)
- `idx_cws_agg_monthly_combined` (scenario_short_code, cws_aggregate_id)
- `idx_cws_agg_monthly_scenario` (scenario_short_code)
- `idx_cws_aggregate_monthly_combined` (scenario_short_code, cws_aggregate_id)
- `idx_cws_aggregate_monthly_entity` (cws_aggregate_id)
- `idx_cws_aggregate_monthly_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_cws_aggregate_monthly` (scenario_short_code, cws_aggregate_id, water_month)

##### `cws_aggregate_period_summary`

Per-aggregate-category period summary.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('cws_aggregate_period_summary_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `cws_aggregate_id` | integer | NO |  | FK `cws_aggregate_entity.id` |
| `simulation_start_year`, `simulation_end_year`, `total_years` | integer | NO |  |  |
| `annual_delivery_avg_taf`, `annual_delivery_cv`, `annual_delivery_min_taf`, `annual_delivery_max_taf` | numeric | YES |  |  |
| `delivery_exc_p5`..`delivery_exc_p95` | numeric | YES |  |  |
| `annual_shortage_avg_taf` | numeric | YES |  |  |
| `shortage_years_count` | integer | YES |  |  |
| `shortage_frequency_pct` | numeric | YES |  |  |
| `shortage_exc_p5`..`shortage_exc_p95` | numeric | YES |  |  |
| `reliability_pct`, `avg_pct_allocation_met`, `annual_demand_avg_taf`, `avg_pct_demand_met` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:**

- `cws_aggregate_id` -> `cws_aggregate_entity.id` (delete: NO ACTION, update: NO ACTION)

**Indexes:**

- `cws_aggregate_period_summary_pkey` (id) [PRIMARY]
- `uq_cws_aggregate_period` (scenario_short_code, cws_aggregate_id) [UNIQUE]
- `idx_cws_agg_period_aggregate` (cws_aggregate_id)
- `idx_cws_agg_period_scenario` (scenario_short_code)
- `idx_cws_aggregate_period_entity` (cws_aggregate_id)
- `idx_cws_aggregate_period_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_cws_aggregate_period` (scenario_short_code, cws_aggregate_id)

#### Agriculture DU result family

The agriculture DU pipeline splits the supply / demand breakdown across five tables (demand, surface-water delivery, groundwater pumping, shortage, period summary). All keyed by `du_id varchar` (matches `du_agriculture_entity.du_id`, no FK).

##### `ag_du_demand_monthly`

Per-DU monthly demand statistics. Note the sequence is named `ag_du_delivery_monthly_id_seq`. The table was renamed during a migration but the sequence was not.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('ag_du_delivery_monthly_id_seq')` | PK, sequence retains old name |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  | matches `du_agriculture_entity.du_id`, no FK |
| `water_month` | integer | NO |  |  |
| `demand_avg_taf`, `demand_cv` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `ag_du_delivery_monthly_pkey` (id) [PRIMARY] - PK index name matches the legacy table name
- `uq_ag_du_demand_monthly` (scenario_short_code, du_id, water_month) [UNIQUE]
- `idx_ag_du_delivery_scenario_du` (scenario_short_code, du_id): legacy index name
- `idx_ag_du_demand_monthly_combined` (scenario_short_code, du_id)
- `idx_ag_du_demand_monthly_du` (du_id)
- `idx_ag_du_demand_monthly_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_ag_du_demand_monthly` (scenario_short_code, du_id, water_month)

##### `ag_du_sw_delivery_monthly`

Per-DU monthly surface-water-delivery statistics.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('ag_du_sw_delivery_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  |  |
| `water_month` | integer | NO |  |  |
| `sw_delivery_avg_taf`, `sw_delivery_cv` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `ag_du_sw_delivery_monthly_pkey` (id) [PRIMARY]
- `uq_ag_du_sw_delivery_monthly` (scenario_short_code, du_id, water_month) [UNIQUE]
- `idx_ag_du_sw_delivery_monthly_combined` (scenario_short_code, du_id)
- `idx_ag_du_sw_delivery_monthly_du` (du_id)
- `idx_ag_du_sw_delivery_monthly_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_ag_du_sw_delivery_monthly` (scenario_short_code, du_id, water_month)

##### `ag_du_gw_pumping_monthly`

Per-DU monthly groundwater-pumping statistics. Carries an `is_calculated boolean NOT NULL DEFAULT true` flag.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('ag_du_gw_pumping_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  |  |
| `water_month` | integer | NO |  |  |
| `gw_pumping_avg_taf`, `gw_pumping_cv` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `is_calculated` | boolean | NO | `true` |  |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `ag_du_gw_pumping_monthly_pkey` (id) [PRIMARY]
- `uq_ag_du_gw_pumping_monthly` (scenario_short_code, du_id, water_month) [UNIQUE]
- `idx_ag_du_gw_pumping_monthly_combined` (scenario_short_code, du_id)
- `idx_ag_du_gw_pumping_monthly_du` (du_id)
- `idx_ag_du_gw_pumping_monthly_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_ag_du_gw_pumping_monthly` (scenario_short_code, du_id, water_month)

##### `ag_du_shortage_monthly`

Per-DU monthly shortage statistics. Adds `shortage_pct_of_demand_avg`.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('ag_du_shortage_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  |  |
| `water_month` | integer | NO |  |  |
| `shortage_avg_taf`, `shortage_cv`, `shortage_frequency_pct`, `shortage_pct_of_demand_avg` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `ag_du_shortage_monthly_pkey` (id) [PRIMARY]
- `uq_ag_du_shortage_monthly` (scenario_short_code, du_id, water_month) [UNIQUE]
- `idx_ag_du_shortage_monthly_combined` (scenario_short_code, du_id)
- `idx_ag_du_shortage_monthly_du` (du_id)
- `idx_ag_du_shortage_monthly_scenario` (scenario_short_code)
- `idx_ag_du_shortage_scenario_du` (scenario_short_code, du_id)

**Unique constraints:**

- `uq_ag_du_shortage_monthly` (scenario_short_code, du_id, water_month)

##### `ag_du_period_summary`

Per-DU period summary. Carries annual rollups of demand, surface-water delivery, groundwater pumping, and shortage, plus their exceedance-percentile tails.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('ag_du_period_summary_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  |  |
| `simulation_start_year`, `simulation_end_year`, `total_years` | integer | NO |  |  |
| `annual_demand_avg_taf`, `annual_demand_cv` | numeric | YES |  |  |
| `demand_exc_p5`..`demand_exc_p95` | numeric | YES |  |  |
| `annual_shortage_avg_taf` | numeric | YES |  |  |
| `shortage_years_count` | integer | YES |  |  |
| `shortage_frequency_pct` | numeric | YES |  |  |
| `annual_shortage_pct_of_demand` | numeric | YES |  |  |
| `reliability_pct`, `avg_pct_demand_met` | numeric | YES |  |  |
| `annual_sw_delivery_avg_taf`, `annual_sw_delivery_cv` | numeric | YES |  |  |
| `annual_gw_pumping_avg_taf`, `annual_gw_pumping_cv`, `gw_pumping_pct_of_demand` | numeric | YES |  |  |
| `sw_delivery_exc_p5`..`sw_delivery_exc_p95` | numeric | YES |  |  |
| `gw_pumping_exc_p5`..`gw_pumping_exc_p95` | numeric | YES |  |  |
| `shortage_exc_p5`..`shortage_exc_p95` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `ag_du_period_summary_pkey` (id) [PRIMARY]
- `uq_ag_du_period_summary` (scenario_short_code, du_id) [UNIQUE]
- `idx_ag_du_period_scenario_du` (scenario_short_code, du_id)
- `idx_ag_du_period_summary_du` (du_id)
- `idx_ag_du_period_summary_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_ag_du_period_summary` (scenario_short_code, du_id)

#### Agriculture aggregate result family

Project-level agriculture rollups keyed by `aggregate_code varchar` (matches `ag_aggregate_entity.short_code`, no FK enforced).

##### `ag_aggregate_monthly`

Per-aggregate-category monthly delivery and shortage statistics. Both delivery and shortage stats live in the same row (similar to `cws_aggregate_monthly`).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('ag_aggregate_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `aggregate_code` | varchar | NO |  | matches `ag_aggregate_entity.short_code`, no FK |
| `water_month` | integer | NO |  |  |
| `delivery_avg_taf`, `delivery_cv` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | delivery percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | delivery exceedance percentiles |
| `shortage_avg_taf`, `shortage_cv`, `shortage_frequency_pct` | numeric | YES |  |  |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `ag_aggregate_monthly_pkey` (id) [PRIMARY]
- `uq_ag_aggregate_monthly` (scenario_short_code, aggregate_code, water_month) [UNIQUE]
- `idx_ag_agg_monthly_scenario` (scenario_short_code, aggregate_code)
- `idx_ag_aggregate_monthly_code` (aggregate_code)
- `idx_ag_aggregate_monthly_combined` (scenario_short_code, aggregate_code)
- `idx_ag_aggregate_monthly_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_ag_aggregate_monthly` (scenario_short_code, aggregate_code, water_month)

##### `ag_aggregate_period_summary`

Per-aggregate-category period summary.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('ag_aggregate_period_summary_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `aggregate_code` | varchar | NO |  |  |
| `simulation_start_year`, `simulation_end_year`, `total_years` | integer | NO |  |  |
| `annual_delivery_avg_taf`, `annual_delivery_cv` | numeric | YES |  |  |
| `delivery_exc_p5`..`delivery_exc_p95` | numeric | YES |  |  |
| `annual_shortage_avg_taf` | numeric | YES |  |  |
| `shortage_years_count` | integer | YES |  |  |
| `shortage_frequency_pct` | numeric | YES |  |  |
| `shortage_exc_p5`..`shortage_exc_p95` | numeric | YES |  |  |
| `reliability_pct` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `ag_aggregate_period_summary_pkey` (id) [PRIMARY]
- `uq_ag_aggregate_period_summary` (scenario_short_code, aggregate_code) [UNIQUE]
- `idx_ag_agg_period_scenario` (scenario_short_code, aggregate_code)
- `idx_ag_aggregate_period_summary_code` (aggregate_code)
- `idx_ag_aggregate_period_summary_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_ag_aggregate_period_summary` (scenario_short_code, aggregate_code)

#### Refuge DU result family

Same shape as DU urban result family but keyed by `du_id` matching `du_refuge_entity.du_id`. Adds shortage-pct columns (refuges are often quoted in % shortage rather than absolute TAF).

##### `refuge_du_delivery_monthly`

Per-refuge-DU monthly delivery statistics.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('refuge_du_delivery_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  | matches `du_refuge_entity.du_id`, no FK |
| `water_month` | integer | NO |  |  |
| `delivery_avg_taf`, `delivery_cv` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `refuge_du_delivery_monthly_pkey` (id) [PRIMARY]
- `uq_refuge_delivery_monthly` (scenario_short_code, du_id, water_month) [UNIQUE]
- `idx_refuge_delivery_monthly_du_id` (du_id)
- `idx_refuge_delivery_monthly_scenario` (scenario_short_code)
- `idx_refuge_delivery_monthly_scenario_du` (scenario_short_code, du_id)

**Unique constraints:**

- `uq_refuge_delivery_monthly` (scenario_short_code, du_id, water_month)

##### `refuge_du_shortage_monthly`

Per-refuge-DU monthly shortage statistics. Adds `shortage_pct_avg` / `shortage_pct_cv` (shortage expressed as % of refuge demand).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('refuge_du_shortage_monthly_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  |  |
| `water_month` | integer | NO |  |  |
| `shortage_avg_taf`, `shortage_cv` | numeric | YES |  |  |
| `shortage_pct_avg`, `shortage_pct_cv` | numeric | YES |  | shortage as % of demand |
| `shortage_frequency_pct` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | exceedance percentiles |
| `sample_count` | integer | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `refuge_du_shortage_monthly_pkey` (id) [PRIMARY]
- `uq_refuge_shortage_monthly` (scenario_short_code, du_id, water_month) [UNIQUE]
- `idx_refuge_shortage_monthly_du_id` (du_id)
- `idx_refuge_shortage_monthly_scenario` (scenario_short_code)
- `idx_refuge_shortage_monthly_scenario_du` (scenario_short_code, du_id)

**Unique constraints:**

- `uq_refuge_shortage_monthly` (scenario_short_code, du_id, water_month)

##### `refuge_du_period_summary`

Per-refuge-DU period summary. Uses `reliability_pct_95` (reliability at the 95% threshold) rather than the plain `reliability_pct` used by other domains.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('refuge_du_period_summary_id_seq')` | PK |
| `scenario_short_code` | varchar | NO |  |  |
| `du_id` | varchar | NO |  |  |
| `simulation_start_year`, `simulation_end_year`, `total_years` | integer | YES |  |  |
| `annual_delivery_avg_taf`, `annual_delivery_cv` | numeric | YES |  |  |
| `delivery_exc_p5`..`delivery_exc_p95` | numeric | YES |  |  |
| `annual_shortage_avg_taf`, `annual_shortage_cv` | numeric | YES |  |  |
| `annual_shortage_pct_avg`, `annual_shortage_pct_cv` | numeric | YES |  |  |
| `reliability_pct_95` | numeric | YES |  | reliability at 95% threshold |
| `shortage_exc_p5`..`shortage_exc_p95` | numeric | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | intended FK `developer.id`, not enforced |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | intended FK `developer.id`, not enforced |

**Foreign keys:** none.

**Indexes:**

- `refuge_du_period_summary_pkey` (id) [PRIMARY]
- `uq_refuge_period_summary` (scenario_short_code, du_id) [UNIQUE]
- `idx_refuge_period_summary_du_id` (du_id)
- `idx_refuge_period_summary_scenario` (scenario_short_code)
- `idx_refuge_period_summary_scenario_du` (scenario_short_code, du_id)

**Unique constraints:**

- `uq_refuge_period_summary` (scenario_short_code, du_id)

#### Env-flow result family

Per-channel-arc environmental-flow statistics. Three tables: monthly (12 buckets), seasonal (5 buckets via `env_flow_season`), and period summary. Keyed by `network_arc_id varchar` (matches `channel_entity.network_arc_id`, no FK enforced). These three tables carry FK constraints on the audit-trigger columns (the only per-scenario statistics tables in Layer 11 that do).

##### `env_flow_channel_monthly`

Per-arc, per-water-month flow + unimpaired-flow statistics.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('env_flow_channel_monthly_id_seq')` | PK |
| `network_arc_id` | varchar | NO |  | matches `channel_entity.network_arc_id`, no FK |
| `scenario_short_code` | varchar | NO |  |  |
| `water_month` | smallint | NO |  |  |
| `flow_avg_cfs`, `flow_cv` | numeric | YES |  |  |
| `unimp_avg_cfs` | numeric | YES |  |  |
| `pct_unimpaired_avg`, `pct_unimpaired_cv` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | percent-unimpaired percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | percent-unimpaired exceedance |
| `flow_avg_taf` | numeric | YES |  |  |
| `flow_q0_cfs`..`flow_q100_cfs` | numeric | YES |  | flow percentile bands in CFS |
| `flow_exc_p5_cfs`..`flow_exc_p95_cfs` | numeric | YES |  | flow exceedance in CFS |
| `flow_q0_taf`..`flow_q100_taf` | numeric | YES |  | flow percentile bands in TAF |
| `flow_exc_p5_taf`..`flow_exc_p95_taf` | numeric | YES |  | flow exceedance in TAF |
| `sample_count` | smallint | YES |  |  |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `env_flow_channel_monthly_pkey` (id) [PRIMARY]
- `uq_env_flow_monthly` (network_arc_id, scenario_short_code, water_month) [UNIQUE]
- `idx_env_flow_monthly_arc` (network_arc_id)
- `idx_env_flow_monthly_arc_scenario` (network_arc_id, scenario_short_code)
- `idx_env_flow_monthly_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_env_flow_monthly` (network_arc_id, scenario_short_code, water_month)

The `q*` / `exc_*` columns without `_cfs` / `_taf` suffixes carry the % unimpaired distribution. The `flow_q*_cfs` / `flow_q*_taf` columns carry the absolute flow distribution. This is the only result table where the same percentile name (e.g. `q50`) refers to a fraction.

##### `env_flow_channel_seasonal`

Per-arc, per-season statistics. `season_id` FKs to `env_flow_season.id` (Layer 01).

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('env_flow_channel_seasonal_id_seq')` | PK |
| `network_arc_id` | varchar | NO |  |  |
| `scenario_short_code` | varchar | NO |  |  |
| `season_id` | integer | NO |  | FK `env_flow_season.id` |
| `pct_ff_avg`, `pct_ff_cv` | numeric | YES |  | percent functional flow |
| `deviation_avg` | numeric | YES |  |  |
| `q0`..`q100` | numeric | YES |  | % FF percentile bands |
| `exc_p5`..`exc_p95` | numeric | YES |  | % FF exceedance |
| `target_met_pct` | numeric | YES |  |  |
| `sample_count` | smallint | YES |  |  |
| `flow_avg_cfs`, `flow_cv` | numeric | YES |  |  |
| `flow_q0`..`flow_q100` | numeric | YES |  | flow percentile bands |
| `flow_exc_p5`..`flow_exc_p95` | numeric | YES |  | flow exceedance |
| `unimp_avg_cfs` | numeric | YES |  |  |
| `pct_unimpaired_avg`, `pct_unimpaired_cv` | numeric | YES |  |  |
| `unimp_q0`..`unimp_q100` | numeric | YES |  | unimpaired-flow percentile bands |
| `unimp_exc_p5`..`unimp_exc_p95` | numeric | YES |  | unimpaired-flow exceedance |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `season_id` -> `env_flow_season.id` (delete: NO ACTION, update: NO ACTION)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `env_flow_channel_seasonal_pkey` (id) [PRIMARY]
- `uq_env_flow_seasonal` (network_arc_id, scenario_short_code, season_id) [UNIQUE]
- `idx_env_flow_seasonal_arc` (network_arc_id)
- `idx_env_flow_seasonal_arc_scenario` (network_arc_id, scenario_short_code)
- `idx_env_flow_seasonal_scenario` (scenario_short_code)
- `idx_env_flow_seasonal_season` (season_id)

**Unique constraints:**

- `uq_env_flow_seasonal` (network_arc_id, scenario_short_code, season_id)

##### `env_flow_channel_period_summary`

Per-arc whole-simulation summary. Carries Pearson r / p-value (against unimpaired-flow regression), MIF-met percent, and per-period CV / averages of % unimpaired and % functional-flow.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('env_flow_channel_period_summary_id_seq')` | PK |
| `network_arc_id` | varchar | NO |  |  |
| `scenario_short_code` | varchar | NO |  |  |
| `simulation_start_year`, `simulation_end_year` | smallint | YES |  |  |
| `total_months` | smallint | YES |  |  |
| `pearson_r`, `p_value` | numeric | YES |  | regression statistics |
| `avg_pct_unimpaired`, `annual_cv_pct_unimpaired` | numeric | YES |  |  |
| `avg_pct_ff`, `annual_cv_pct_ff` | numeric | YES |  |  |
| `mif_met_pct` | numeric | YES |  | only meaningful when channel has `has_mif = true` |
| `flow_exc_p5_cfs`..`flow_exc_p95_cfs` | numeric | YES |  | flow exceedance in CFS |
| `flow_exc_p5_taf`..`flow_exc_p95_taf` | numeric | YES |  | flow exceedance in TAF |
| `is_active` | boolean | NO | `true` |  |
| `created_at` | timestamptz | NO | `now()` |  |
| `created_by` | integer | NO |  | FK `developer.id` |
| `updated_at` | timestamptz | NO | `now()` |  |
| `updated_by` | integer | NO |  | FK `developer.id` |

**Foreign keys:**

- `created_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)
- `updated_by` -> `developer.id` (delete: RESTRICT, update: CASCADE)

**Indexes:**

- `env_flow_channel_period_summary_pkey` (id) [PRIMARY]
- `uq_env_flow_period_summary` (network_arc_id, scenario_short_code) [UNIQUE]
- `idx_env_flow_period_summary_arc` (network_arc_id)
- `idx_env_flow_period_summary_arc_scenario` (network_arc_id, scenario_short_code)
- `idx_env_flow_period_summary_scenario` (scenario_short_code)

**Unique constraints:**

- `uq_env_flow_period_summary` (network_arc_id, scenario_short_code)

---

## Layer 12 - CROSS-SCENARIO ANALYSIS

Rollups that aggregate ACROSS scenarios. Today this is one family: the sensitivity tables, computed from the Layer 11 per-scenario statistics. Layer 12 depends on L11 and L07 (`hydroclimate`).

### Table schema

#### Sensitivity result family

Cross-scenario sensitivity analysis produced by `etl/statistics/sensitivity/calculate_sensitivity.py`. The script reads from the database (not S3) and must run AFTER all Layer 11 per-scenario statistics modules have completed. It writes two tables:

- **`sensitivity_climate`** holds climate sensitivity within each hydroclimate sibling group (scenarios sharing identical operations). For each `(sibling_group, module, entity, metric, water_month)` it stores the historical / cc50 / cc95 values plus absolute and percent change for the two climate-change levels.
- **`sensitivity_operational`** holds operational sensitivity within each hydroclimate level (e.g. all historical-hydrology scenarios). For each `(hydroclimate_id, module, entity, metric, water_month)` it stores min / max / mean / std / range / `pct_range` across the operational variants in that level, along with `scenario_count`.

Both tables are flagged "Experimental" in the calculation script. Neither carries a `set_audit_fields` trigger. The trigger was never attached at table creation, and the tables use a minimal audit shape: a single `updated_at` column and no `created_at` / `created_by` / `updated_by`. Attaching the trigger and reshaping the audit columns is tracked in `database/SCHEMA_BACKLOG.md` § 4 Corrections.

**Redundant indexes:** As in Layer 11, each table carries a left-prefix index covered by its unique constraint (`idx_sensitivity_climate_sibling`, `idx_sensitivity_operational_hydro`). `audit_cleanup.sql` drops them.

##### `sensitivity_climate`

Climate-sensitivity rollups across the three hydroclimate variants of each sibling group.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('sensitivity_climate_id_seq')` | PK |
| `sibling_group` | varchar | NO |  | scenario sibling-group code (operations held constant across hydroclimate variants) |
| `module` | varchar | NO |  | calculation domain, e.g. `reservoir`, `du_urban`, `delta` |
| `entity_id` | varchar | NO |  | entity reference within the module, polymorphic across modules |
| `metric_name` | varchar | NO |  | metric label, e.g. `mean_storage`, `delivery_cv` |
| `water_month` | smallint | NO |  | 1-12 starting at October |
| `unit` | varchar | YES |  | unit of `*_value` columns |
| `hist_value` | double precision | YES |  | historical-hydrology value |
| `cc50_value` | double precision | YES |  | cc50 (mid climate change) value |
| `cc95_value` | double precision | YES |  | cc95 (high climate change) value |
| `cc50_abs_change` | double precision | YES |  | `cc50_value - hist_value` |
| `cc95_abs_change` | double precision | YES |  | `cc95_value - hist_value` |
| `cc50_pct_change` | double precision | YES |  | percent change from historical to cc50 |
| `cc95_pct_change` | double precision | YES |  | percent change from historical to cc95 |
| `updated_at` | timestamptz | NO | `now()` |  |

**Foreign keys:**

- none. `sibling_group`, `module`, and `entity_id` are unenforced text references. The audit trigger is not attached.

**Indexes:**

- `sensitivity_climate_pkey` (id) [PRIMARY]
- `sensitivity_climate_sibling_group_module_entity_id_metric_n_key` (sibling_group, module, entity_id, metric_name, water_month) [UNIQUE]
- `idx_sensitivity_climate_module_month` (module, water_month)
- `idx_sensitivity_climate_sibling` (sibling_group)

**Unique constraints:**

- `sensitivity_climate_sibling_group_module_entity_id_metric_n_key` (sibling_group, module, entity_id, metric_name, water_month). The natural key for one cross-climate row.

##### `sensitivity_operational`

Operational-sensitivity rollups across the operational variants within each hydroclimate level.

| column | type | nullable | default | notes |
|---|---|---|---|---|
| `id` | integer | NO | `nextval('sensitivity_operational_id_seq')` | PK |
| `hydroclimate_id` | integer | NO |  | FK `hydroclimate.id` |
| `module` | varchar | NO |  | calculation domain, e.g. `reservoir`, `du_urban`, `delta` |
| `entity_id` | varchar | NO |  | entity reference within the module, polymorphic across modules |
| `metric_name` | varchar | NO |  | metric label |
| `water_month` | smallint | NO |  | 1-12 starting at October |
| `unit` | varchar | YES |  | unit of statistic columns |
| `scenario_count` | integer | NO |  | number of scenarios contributing to the rollup at this hydroclimate level |
| `min_value` | double precision | YES |  | minimum across operational variants |
| `max_value` | double precision | YES |  | maximum across operational variants |
| `mean_value` | double precision | YES |  | arithmetic mean across operational variants |
| `std_value` | double precision | YES |  | standard deviation across operational variants |
| `range_value` | double precision | YES |  | `max_value - min_value` |
| `pct_range` | double precision | YES |  | range as a percent of mean (or another base, see calc script) |
| `updated_at` | timestamptz | NO | `now()` |  |

**Foreign keys:**

- `hydroclimate_id` -> `hydroclimate.id` (delete: NO ACTION, update: NO ACTION). The only enforced FK on either sensitivity table.
- `module` and `entity_id` are unenforced text references. The audit trigger is not attached.

**Indexes:**

- `sensitivity_operational_pkey` (id) [PRIMARY]
- `sensitivity_operational_hydroclimate_id_module_entity_id_me_key` (hydroclimate_id, module, entity_id, metric_name, water_month) [UNIQUE]
- `idx_sensitivity_operational_hydro` (hydroclimate_id)
- `idx_sensitivity_operational_module_month` (module, water_month)

**Unique constraints:**

- `sensitivity_operational_hydroclimate_id_module_entity_id_me_key` (hydroclimate_id, module, entity_id, metric_name, water_month). The natural key for one cross-scenario row at one hydroclimate level.

---

## Database functions

Beyond what PostGIS and pgcrypto provide, the database owns 11 `postgres`-role functions. They split into four groups: audit-trigger infrastructure, versioning helpers, developer registration, and network-topology walks. The trigger function `set_audit_fields` is wired by `apply_audit_trigger_to_table` and fires on every tracked table. It is not called directly.

### Audit-trigger infrastructure

| function | language | security definer | purpose |
|---|---|---|---|
| `set_audit_fields()` returns trigger | plpgsql | no | Trigger function. Fills `created_by` / `updated_by` (from `coeqwal_current_operator()`) and `created_at` / `updated_at` on INSERT / UPDATE. |
| `apply_audit_trigger_to_table(p_table_name text) returns text` | plpgsql | no | One-shot helper. Drops and re-creates a `BEFORE INSERT OR UPDATE` trigger on `p_table_name` that calls `set_audit_fields()`. Used during migrations. |

### Operator / versioning helpers

| function | language | security definer | purpose |
|---|---|---|---|
| `coeqwal_current_operator() returns integer` | plpgsql | yes | Returns the `developer.id` matching the current `session_user`. Called by `set_audit_fields()` and by ETL writes. SECURITY DEFINER so non-superuser roles can resolve their own id. |
| `get_active_version(family_id integer) returns integer` | plpgsql | no | Returns the active `version.id` for a `version_family.id`. |
| `get_active_version(family_short_code text) returns integer` | plpgsql | yes | Overload that looks up the family by short_code first, then defers to the integer form. SECURITY DEFINER so app roles can read `version_family` without direct grants. |
| `get_source_id(source_short_code text) returns integer` | plpgsql | yes | Resolves `source.id` from a `short_code`. SECURITY DEFINER so app roles can resolve sources without direct `source` grants. |

### Developer registration

| function | language | security definer | purpose |
|---|---|---|---|
| `register_developer(p_username text, p_email text, p_display_name text, p_password text, p_role text DEFAULT 'developer') returns text` | plpgsql | yes | Wraps the `CREATE ROLE` + `INSERT INTO developer` pattern: creates a Postgres role with the supplied password, GRANTs the standard role membership, and inserts the matching `developer` row. SECURITY DEFINER so an existing developer can onboard a new one without superuser. |
| `list_developers() returns TABLE(id integer, email text, display_name text, db_username text, role text, is_active boolean)` | plpgsql | no | Read-only convenience view of the `developer` table. |

### Network-topology walks

These are pure `LANGUAGE sql` functions used by network exploration tooling. They walk the `network_arc` / `network_node` edge set. They are unrelated to the result-layer tables in Layers 10-12.

| function | language | security definer | purpose |
|---|---|---|---|
| `get_connected_arcs(node_id integer)` | sql | no | Returns all `network_arc` rows incident on `node_id`, with the opposite node decorated. Direction (`inbound` / `outbound`) is computed from the arc's `from_node_id` / `to_node_id`. |
| `get_upstream_nodes(start_node_id integer, max_depth integer DEFAULT 10)` | sql | no | Recursive walk that returns nodes reachable from `start_node_id` by following arcs upstream, with depth and the arc-id path. |
| `get_downstream_nodes(start_node_id integer, max_depth integer DEFAULT 10)` | sql | no | Symmetric walk for the downstream direction. |

These three are the only `LANGUAGE sql` user functions. Everything else in this section is plpgsql.

---

## Extension-managed tables

These tables live in the `public` schema but are not COEQWAL domain tables. They are owned by an admin role and managed by a PostgreSQL extension. They do not follow COEQWAL conventions (no audit columns, no audit trigger, not registered in `domain_family_map` under a COEQWAL family).

### `spatial_ref_sys` (PostGIS extension, owned by `rdsadmin`)

PostGIS spatial reference systems catalog. Installed by the PostGIS extension. Maps SRID values (e.g. 4326, 26910) to the proj4 / WKT definitions used by PostGIS geometry columns to interpret coordinates. Read-only from the COEQWAL application's perspective. We never INSERT, UPDATE, or DELETE rows here.

- Record count at audit snapshot: 8500
- Audit trigger: none (extension-managed)
- `domain_family_map` classifies it under family `geospatial`, but it is included here purely for completeness. It is not part of the layered COEQWAL schema.

`spatial_ref_sys` is the only `rdsadmin`-owned table in the database. All other 96 public tables are owned by `postgres`.
