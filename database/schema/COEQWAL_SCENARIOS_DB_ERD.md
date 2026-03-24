# COEQWAL SCENARIOS DATABASE ERD

## **ARCHITECTURE OVERVIEW**

### **Database Layer Structure:**
```
00  VERSIONING (audit)
    version_family, version, developer, domain_family_map, audit_log

01  LOOKUP (shared reference data)
    hydrologic_region, source, model_source, unit, spatial_scale, temporal_scale,
    statistic_category, statistic_type, geometry_type, network_entity_type,
    network_type, network_subtype, watershed

02  NETWORK (physical infrastructure)
    network, network_arc, network_node, network_gis

03  ENTITY (operational entities)
    reservoir, compliance_station, du_agriculture_entity, du_urban_entity,
    du_refuge_entity, reservoir_entity, mi_contractor, wba

04  VARIABLE (CalSim variable definitions + type classifications)
    calsim_model_variable_type, derived_variable_type, variable_type,
    channel_variable

05  ASSUMPTIONS + OPERATIONS (scenario configuration dimensions)
    assumption_category, assumption_definition       ← land use, groundwater model
    operation_category, operation_definition         ← TUCP, SGMA, BiOps, flows,
                                                        infrastructure, delta regs,
                                                        allocation priorities

06  SCENARIO (scenario definitions)
    scenario, scenario_author
    scenario_key_assumption_link, scenario_key_operation_link
    scenario_tag, scenario_tag_link

07  HYDROCLIMATE (hydrology + SLR)
    hydroclimate                                     ← historical + projected records
    slr                                              ← sea level rise scenarios

08  THEME (research themes)
    theme, theme_scenario_link

09  (reserved)

10+ RESULTS / TIERS
    tier_definition, tier_result, tier_location_result
    reservoir_storage_monthly, reservoir_spill_monthly, reservoir_monthly_percentile,
        reservoir_period_summary
    du_delivery_monthly, du_period_summary, du_shortage_monthly
    ag_du_demand_monthly, ag_du_sw_delivery_monthly, ag_du_gw_pumping_monthly,
        ag_du_shortage_monthly, ag_du_period_summary
    ag_aggregate_monthly, ag_aggregate_period_summary
    mi_delivery_monthly, mi_shortage_monthly, mi_contractor_period_summary
    cws_aggregate_monthly, cws_aggregate_period_summary
    refuge_du_delivery_monthly, refuge_du_shortage_monthly, refuge_du_period_summary
    env_flow_season (lookup), env_flow_channel_monthly, env_flow_channel_seasonal,
        env_flow_channel_period_summary
    delta_monthly, delta_period_summary

VIEWS
    scenario_full          ← wide pivot of scenario + hydroclimate_sibling + operations + assumptions
    refuge_du_full         ← denormalized refuge demand units with decoded cs3_type label
    env_flow_channel_full  ← denormalized channel entities with watershed + env-flow attributes
```

## **Layer 00 — VERSIONING SYSTEM**

### **1. version_family (version categories)**
```
Table: version_family
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "theme", "scenario", "network", "entity", etc.
├── label                TEXT                       -- "Theme", "Scenario", "Network", "Entity", etc.
├── description          TEXT                       -- Purpose description
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (14 total):
├── theme: Research themes and storylines
├── scenario: Water management scenarios
├── assumption: Scenario assumptions and parameters
├── operation: Operational policies and rules
├── hydroclimate: Hydroclimate conditions and projections
├── variable: CalSim model variables and definitions
├── statistics: Statistics categories and measurement systems
├── tier: Tier definitions and classification systems
├── geospatial: Geographic and spatial data definitions
├── interpretive: Analysis and interpretive frameworks
├── metadata: Data metadata and documentation
├── network: CalSim network topology and connectivity
├── entity: Entity version family for tracking entity data versions
└── audit: Layer 00 system tables: versioning, developer registry, domain mapping, audit log

Indexes:
└── version_family_short_code_key (short_code) -- For version family lookups
```

### **2. version (version instances)**
```
Table: version
├── id                   SERIAL PRIMARY KEY
├── version_family_id    INTEGER NOT NULL           -- FK to version_family.id
├── version_number       TEXT                       -- "1.0.0" (semantic versioning)
├── changelog            TEXT                       -- Change description
├── is_active            BOOLEAN DEFAULT FALSE      -- Only one active per family
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Indexes:
└── version_version_family_id_version_number_key (version_family_id, version_number)
    -- Also handles version_family_id prefix queries (left-prefix usable);
    -- idx_version_family was dropped as redundant.
```

### **3. developer (audits)**
```
Table: developer
├── id                   SERIAL PRIMARY KEY
├── email                TEXT UNIQUE                -- "jfantauzza@berkeley.edu"
├── name                 TEXT                       -- "Jill"
├── display_name         TEXT NOT NULL              -- "Jill Fantauzza"
├── affiliation          TEXT                       -- Organization
├── role                 TEXT                       -- "admin", "user", "system"
├── aws_sso_user_id      TEXT                       -- AWS SSO integration (optional)
├── aws_sso_username     TEXT UNIQUE                -- AWS SSO username (primary SSO identifier)
├── is_bootstrap         BOOLEAN DEFAULT FALSE      -- System bootstrap user
├── sync_source          TEXT DEFAULT 'manual'      -- "manual", "sso", "seed"
├── is_active            BOOLEAN DEFAULT TRUE
├── last_login           TIMESTAMP WITH TIME ZONE
├── created_at           TIMESTAMP DEFAULT NOW()
├── updated_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER                    -- FK developer.id (self-referencing)
└── updated_by           INTEGER                    -- FK developer.id (self-referencing)

Records: 2 (bootstrap system user + admin)
```

### **4. data_load_log (ETL batch tracking)**

Status: PLANNED — not yet created in the database.
Intended to replace per-row `created_by`/`updated_by` on bulk statistics tables,
providing batch-level ETL provenance (who loaded, when, from what source file).

```
Table: data_load_log   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── table_name           TEXT NOT NULL              -- Target statistics table
├── scenario_short_code  TEXT                       -- Scenario loaded (if applicable)
├── source_file          TEXT                       -- S3 path or local file loaded
├── record_count         INTEGER                    -- Rows inserted/updated
├── loaded_by            INTEGER NOT NULL           -- FK developer.id
├── loaded_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── notes                TEXT
└── status               TEXT DEFAULT 'success'     -- "success", "error", "partial"
```

### **6. audit_log (change tracking)**
```
Table: audit_log
├── id                   SERIAL PRIMARY KEY
├── table_name           TEXT NOT NULL              -- Name of the table where change occurred
├── record_id            INTEGER                    -- Primary key of the changed record
├── record_key           JSONB                      -- Natural key of the record (e.g. {"short_code": "s0020"})
├── operation            TEXT NOT NULL              -- "INSERT", "UPDATE", "DELETE"
├── old_values           JSONB                      -- Row state before change
├── new_values           JSONB                      -- Row state after change
├── changed_fields       TEXT[]                     -- Array of column names that changed
├── changed_by           INTEGER                    -- FK developer.id (who made the change)
├── changed_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── session_user_name    TEXT                       -- Database session user
├── application_name     TEXT                       -- Application/tool that made the change
└── client_addr          TEXT                       -- Client IP address

Records: 0 (table created, triggers not yet deployed to production tables)
Purpose: Row-level change tracking for key domain tables (theme, scenario, etc.)

Indexes:
├── idx_audit_log_changed_at  (changed_at)              -- time-range queries
├── idx_audit_log_changed_by  (changed_by)              -- "what did user X change?"
└── idx_audit_log_record      (table_name, record_id)   -- "what happened to record X?"
    -- idx_audit_log_table_name and idx_audit_log_operation were dropped:
    --   table_name  is covered by idx_audit_log_record left-prefix
    --   operation   has only 3 distinct values; too low-cardinality to be useful
```

### **7. domain_family_map (table-to-version mapping)**
```
Table: domain_family_map
├── schema_name          TEXT NOT NULL              -- "public"
├── table_name           TEXT NOT NULL              -- Table name
├── version_family_id    INTEGER NOT NULL           -- FK version_family.id
├── database_level       TEXT                       -- Two-digit layer code ("00", "01", ... "15")
├── note                 TEXT                       -- Purpose note
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK developer.id

Records: ~88 entries (one per domain table tracked)
Primary key: (schema_name, table_name)

Indexes:
└── idx_domain_family_map_version_family (version_family_id) -- FK lookup: all tables in a family
```

## **Layer 01 — LOOKUP TABLES**

> **Provenance convention:** All lookup tables carry `created_by`, `updated_by`, `created_at`, `updated_at`
> audit fields (FK to `developer.id`). Tables whose data originates from a specific external source
> (e.g., geopackage, NHD, CalSim report) additionally carry a `source_id` FK to `source.id`.
> Current tables using `source_id`: `network_type`, `network_subtype`, `reservoir`, `compliance_station`, `wba`.
>
> Note: variable type classification tables (`calsim_model_variable_type`, `derived_variable_type`,
> `variable_type`) have been moved to **Layer 04 — VARIABLE**.

### **1. hydrologic_region**
```
Table: hydrologic_region
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "SAC", "SJR", "NC", "DELTA", "TULARE", "SOCAL", "EXPORT"
├── label                TEXT                       -- "Sacramento River Basin", etc.
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (7 total):
├── SAC: Sacramento River Basin
├── SJR: San Joaquin River Basin
├── NC: North Coast
├── DELTA: Sacramento–San Joaquin Delta
├── TULARE: Tulare Basin
├── SOCAL: Southern California
└── EXPORT: Export region

Indexes:
└── hydrologic_region_short_code_key (short_code) -- For region lookups
```

### **2. source (data sources)**
```
Table: source
├── id                   SERIAL PRIMARY KEY
├── source               TEXT UNIQUE NOT NULL       -- "calsim_report", "geopackage", etc.
├── description          TEXT                       -- Source description
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (12 total, IDs 1–12 sequential):
├──  1 calsim_report: CalSim-3 report final.pdf
├──  2 james_gilbert: James Gilbert
├──  3 calsim_variables: CalSim variables from output and sv data
├──  4 geopackage: CalSim3_GeoSchematic_20221227_COEQWAL_Revisions2024_corrected.gpkg
├──  5 trend_report: Variables extracted from Gilbert team trend reports
├──  6 metadata: Scenario metadata
├──  7 cvm_docs: Central Valley Model documentation
├──  8 network_schematic: Network schematic
├──  9 manual: Manual insertion
├── 10 NHD: National Hydrography Dataset
├── 11 DWR_CDEC: DWR California Data Exchange Center
└── 12 wietske_medema: Wietske Medema
```

### **3. model_source**
```
Table: model_source
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "calsim3"
├── name                 TEXT UNIQUE NOT NULL       -- "CalSim3"
├── description          TEXT                       -- Model description
├── contact              TEXT                       -- Contact information
├── notes                TEXT                       -- Additional notes
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (1 total):
└── calsim3: California Central Valley water system allocation simulation model
```

### **4. geometry_type (GIS)**
```
Table: geometry_type
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "point", "linestring", "polygon", "multipolygon"
├── label                TEXT                       -- "Point", "LineString", etc.
├── description          TEXT                       -- Geometry description
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK developer.id

Values (4 total):
├── point: Point geometry
├── linestring: LineString geometry
├── polygon: Polygon geometry
└── multipolygon: MultiPolygon geometry
```

### **5. spatial_scale (geographic scales)**
```
Table: spatial_scale
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "system_wide", "regional", "basin", etc.
├── label                TEXT                       -- "System-wide", "Regional", etc.
├── description          TEXT                       -- Scale description
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (11 total):
├── system_wide: Entire CalSim system
├── regional: Hydrologic region
├── basin: Watershed or hydrologic basin
└── ... (8 more scales)
```

### **6. temporal_scale (time scales)**
```
Table: temporal_scale
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "daily", "weekly", "monthly", etc.
├── label                TEXT NOT NULL              -- "Daily", "Weekly", etc.
├── description          TEXT                       -- Scale description
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (8 total):
├── daily: Daily
├── weekly: Weekly
├── monthly: Monthly
└── ... (5 more scales)
```

### **7. statistic_category**
```
Table: statistic_category
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "summary", "percentile_band", "exceedance"
├── label                TEXT NOT NULL              -- "Summary", "Percentile Band", "Exceedance"
├── description          TEXT
├── created_at           TIMESTAMPTZ DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMPTZ DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (3 total):
├── summary:         Summary         — Aggregate summary statistics (mean, median, min, max, cv, stdev)
├── percentile_band: Percentile Band — Standard quantile bands aligned with DWR water year types
└── exceedance:      Exceedance      — Exceedance percentiles for flow duration and reliability analysis
```

### **8. statistic_type**
```
Table: statistic_type
├── id                     SERIAL PRIMARY KEY
├── short_code             TEXT UNIQUE NOT NULL       -- "MEAN", "Q90", "EXC_P90", etc.
├── label                  TEXT NOT NULL              -- "Mean", "90th percentile", etc.
├── description            TEXT                       -- Statistic description
├── statistic_category_id  INTEGER NOT NULL           -- FK to statistic_category.id
├── created_at             TIMESTAMPTZ DEFAULT NOW()
├── created_by             INTEGER NOT NULL           -- FK to developer.id
├── updated_at             TIMESTAMPTZ DEFAULT NOW()
└── updated_by             INTEGER NOT NULL           -- FK to developer.id

Note: no is_active column — all statistic types are always active.

Column naming convention:
  Results tables (Layer 10+) store statistics as columns named LOWER(short_code).
  Example: statistic_type.short_code = 'Q90'  to  column name = 'q90'
           statistic_type.short_code = 'EXC_P90' to  column name = 'exc_p90'
  This table is the authoritative registry of all statistics the system produces.
  Future schema versions may restructure results tables to use statistic_type_id as FK.

Values (20 total):

Summary (statistic_category_id = 1):
├── MEAN: Mean (Average value)
├── MEDIAN: Median (Middle value of the distribution)
├── MIN: Minimum (Minimum value)
├── MAX: Maximum (Maximum value)
├── CV: Coefficient of variation (Relative variability: stdev/mean)
└── STDEV: Standard deviation (Absolute spread around the mean)

Percentile Band (statistic_category_id = 2) — water year type classification:
  Q(n) = "the value at the nth percentile" — Q90 is a HIGH value (wet).
  Bands: [0, 10, 30, 50, 70, 90, 100] align with DWR water year types.
  Both band and exceedance values are independently computed by the ETL
  and stored as separate columns. They use different breakpoint sets, so
  most exceedance values CANNOT be derived from band percentiles.
├── Q0: 0th percentile (Minimum in band context)
├── Q10: 10th percentile (Dry conditions)
├── Q30: 30th percentile (Below normal)
├── Q50: 50th percentile (Median in band context)
├── Q70: 70th percentile (Above normal)
├── Q90: 90th percentile (Wet conditions)
└── Q100: 100th percentile (Maximum in band context)

Exceedance (statistic_category_id = 3) — flow duration / reliability analysis:
  EXC_P(n) = "the value exceeded n% of the time" — EXC_P90 is a LOW value (dry).
  Relationship: EXC_P(n) = Q(100−n). So EXC_P90 = Q10, EXC_P50 = Q50, EXC_P10 = Q90.
  Breakpoints: [5, 10, 25, 50, 75, 90, 95] follow standard hydrologic convention.
├── EXC_P5: 5th exceedance (Very wet — exceeded only 5% of time = Q95)
├── EXC_P10: 10th exceedance (Wet = Q90)
├── EXC_P25: 25th exceedance (Above average = Q75)
├── EXC_P50: 50th exceedance (Median = Q50)
├── EXC_P75: 75th exceedance (Below average = Q25)
├── EXC_P90: 90th exceedance (Dry = Q10)
└── EXC_P95: 95th exceedance (Very dry — exceeded 95% of time = Q5)
```

### **9. unit**
```
Table: unit
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "TAF", "CFS", "acres", etc.
├── full_name            TEXT                       -- "thousand acre-feet", etc.
├── canonical_group      TEXT                       -- "volume", "flow", "area", etc.
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (5 total):
├── TAF: thousand acre-feet (volume)
├── CFS: cubic feet per second (flow)
├── acres: acres (area)
└── ... (2 more units)
```

### **10. network_type**
```
Table: network_type
├── id                      SERIAL PRIMARY KEY
├── short_code              TEXT UNIQUE NOT NULL       -- "CH", "CT", "D", "CH_N", "S", etc.
├── label                   TEXT NOT NULL              -- "Channel", "Cross transfer", etc.
├── description             TEXT                       -- Network type description
├── network_entity_type_id  INTEGER NOT NULL           -- FK to network_entity_type.id (1=arc, 2=node)
├── model_source_id         INTEGER                    -- FK to model_source.id
├── source_id               INTEGER                    -- FK to source.id
├── is_active               BOOLEAN DEFAULT TRUE
├── created_at              TIMESTAMP DEFAULT NOW()
├── created_by              INTEGER NOT NULL           -- FK to developer.id
├── updated_at              TIMESTAMP DEFAULT NOW()
└── updated_by              INTEGER NOT NULL           -- FK to developer.id

Records: 21 types (10 arc types, 11 node types)
Seed: seed_tables/01_lookup/network_type.csv
```

### **11. network_subtype**
```
Table: network_subtype
├── id                      SERIAL PRIMARY KEY
├── short_code              TEXT NOT NULL              -- "BP", "CH", "CL", etc.
├── label                   TEXT NOT NULL              -- "Bypass", "Channel", etc.
├── description             TEXT
├── type_id                 INTEGER NOT NULL           -- FK to network_type.id (entity type derivable via network_type)
├── model_source_id         INTEGER                    -- FK to model_source.id
├── source_id               INTEGER                    -- FK to source.id
├── is_active               BOOLEAN DEFAULT TRUE
├── created_at              TIMESTAMP DEFAULT NOW()
├── created_by              INTEGER NOT NULL           -- FK to developer.id
├── updated_at              TIMESTAMP DEFAULT NOW()
└── updated_by              INTEGER NOT NULL           -- FK to developer.id

Records: 28 subtypes
Seed: seed_tables/01_lookup/network_subtype.csv
```

### **12. watershed**

```
Table: watershed
├── id                    SERIAL PRIMARY KEY
├── short_code            VARCHAR UNIQUE NOT NULL    -- Watershed identifier
├── name                  VARCHAR NOT NULL           -- Full watershed name
├── description           TEXT                       -- Watershed description
├── hydrologic_region_id  INTEGER                    -- FK to hydrologic_region.id
├── unimp_sv_variable     VARCHAR                    -- CalSim SV UNIMP_* variable for this watershed
│                                                    -- (NULL if no SV reference exists, e.g. UPPER_MOKELUMNE)
├── is_active             BOOLEAN NOT NULL DEFAULT TRUE
├── created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by            INTEGER NOT NULL           -- FK to developer.id
├── updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by            INTEGER NOT NULL           -- FK to developer.id

Records: 13 watersheds (migration 23 added CLEAR_CREEK, SAC_LOWER, SAC_UPPER, TRINITY_RIVER, UPPER_MERCED;
         replaced SAC_RIVER with SAC_UPPER + SAC_LOWER split at Bend Bridge rm 257)
Seed: seed_tables/01_lookup/watershed.csv

Foreign keys:
├── Ref: watershed.hydrologic_region_id > hydrologic_region.id [delete: restrict, update: cascade]
├── Ref: watershed.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: watershed.updated_by > developer.id [delete: restrict, update: cascade]

Referenced by:
└── channel_entity.watershed_short_code > watershed.short_code

Indexes:
├── watershed_short_code_key (short_code) -- Unique constraint
└── idx_watershed_hydrologic_region (hydrologic_region_id) -- Region lookups

Values (13 total):
├── BEAR_RIVER:       Bear River Watershed (SJR)                          — no UNIMP variable
├── CLEAR_CREEK:      Clear Creek / Whiskeytown Watershed (SAC)           — UNIMP_WH
├── SAC_UPPER:        Sacramento River above Bend Bridge (SAC)            — UNIMP_SHAS
├── SAC_LOWER:        Sacramento River at/below Bend Bridge (SAC)         — UNIMP_SRBB
├── SAN_JOAQUIN:      San Joaquin River Hydrologic Region (SJR)           — UNIMP_SJ
├── TRINITY_RIVER:    Trinity River Watershed (NC)                        — UNIMP_TRIN
├── UPPER_AMERICAN:   Upper American River Watershed (SJR)                — UNIMP_FOLS
├── UPPER_FEATHER:    Upper Feather River Watershed (SJR)                 — UNIMP_OROV
├── UPPER_MERCED:     Upper Merced River Watershed (SJR)                  — UNIMP_ME
├── UPPER_MOKELUMNE:  Upper Mokelumne River Watershed (SJR)               — no UNIMP variable
├── UPPER_STANISLAUS: Upper Stanislaus River (SJR)                        — UNIMP_ST
├── UPPER_TUOLUMNE:   Upper Tuolumne River Watershed (SJR)                — UNIMP_TU
└── YUBA_RIVER:       Yuba River Watershed (SJR)                          — UNIMP_YUBA

Notes:
- Migration 33 normalized hydrologic_region_short_code (text) to hydrologic_region_id (FK).
- SAC_RIVER was split at Bend Bridge (rm 257) into SAC_UPPER and SAC_LOWER (migration 23).
  All Sacramento mainstem channels at or below rm 257 use SAC_LOWER / UNIMP_SRBB.
  Channels above rm 257 (SAC289, KSWCK, SHSTA) use SAC_UPPER / UNIMP_SHAS.
- UPPER_MOKELUMNE has no UNIMP_MOK in CalSim SV. % unimpaired metric cannot be computed
  for MOK reaches without a proper natural flow reference.
```

### **13. network_entity_type (element type classification)**
```
Table: network_entity_type
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "arc", "node", "null", "unimpaired_flows"
├── label                TEXT
├── description          TEXT
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER                    -- FK to developer.id (RESTRICT)
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER                    -- FK to developer.id (RESTRICT)

Records: 4
Indexes:
├── network_entity_type_pkey (id)
├── network_entity_type_short_code_key (short_code) UNIQUE
└── idx_network_entity_type_active (is_active, short_code)
```

---

## **Layer 02 — NETWORK LAYER** *(CalSim network topology and physical infrastructure)*

> Represents the CalSim3 water infrastructure as a directed graph: `network` is the master
> element registry; `network_arc` and `network_node` carry arc/node-specific attributes;
> `network_gis` holds PostGIS geometry.
>
> Seed data: `seed_tables/02_network/`. Primary source: CalSim3 GeoSchematic geopackage.
> All arc/node/gis records carry `source_id` and `model_source_id` (100% coverage).

### **1. network (master element registry)**
```
Table: network
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- CalSim code (e.g. "C_SAC296", "D_FOLSM")
├── name                 VARCHAR
├── description          TEXT
├── comment              TEXT
├── entity_type_id       INTEGER                    -- FK to network_entity_type.id (RESTRICT)
├── type_id              INTEGER                    -- FK to network_type.id (RESTRICT)
├── subtype_ids          INTEGER[]                  -- Array of FK to network_subtype.id
├── model_list           TEXT[]                     -- CalSim models containing this element
├── source_list          TEXT[]                     -- Data source identifiers
├── has_gis              BOOLEAN DEFAULT FALSE       -- TRUE when network_gis entry exists
├── hydrologic_region_id INTEGER                    -- FK to hydrologic_region.id (RESTRICT)
├── riv_sys              VARCHAR                    -- River system code
├── strm_code            VARCHAR                    -- Stream code
├── network_version_id   INTEGER DEFAULT 12         -- FK to version.id (RESTRICT)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER                    -- FK to developer.id (RESTRICT)
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER                    -- FK to developer.id (RESTRICT)

Records: 6,908
Indexes:
├── network_pkey (id)
├── network_short_code_key (short_code) UNIQUE
├── idx_network_type (type_id)
├── idx_network_entity_type (entity_type_id)
├── idx_network_has_gis (has_gis)
├── idx_network_hydrologic_region (hydrologic_region_id)
├── idx_network_model_list (model_list)         -- GIN index for array containment
├── idx_network_source_list (source_list)       -- GIN index for array containment
├── idx_network_strm_code (strm_code)
└── idx_network_version (network_version_id)
```

### **3. network_arc (arc-specific attributes)**
```
Table: network_arc
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- Matches network.short_code
├── network_id           INTEGER                    -- FK to network.id (CASCADE delete)
├── river                VARCHAR                    -- CalSim waterway code (e.g. "SAC", "SJR", "DMC")
├── from_node            VARCHAR                    -- Upstream node short_code
├── to_node              VARCHAR                    -- Downstream node short_code
├── shape_length_m       NUMERIC                    -- Arc length in meters
├── model_source_id      INTEGER DEFAULT 1          -- FK to model_source.id (RESTRICT)
├── source_id            INTEGER DEFAULT 4          -- FK to source.id (RESTRICT)
├── network_version_id   INTEGER DEFAULT 12         -- FK to version.id (RESTRICT)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER                    -- FK to developer.id (RESTRICT)
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER                    -- FK to developer.id (RESTRICT)

Records: 2,610
Note: CASCADE on network_id — arcs are deleted when their parent network element is removed.
      459 distinct river/waterway codes.
Indexes:
├── network_arc_pkey (id)
├── network_arc_short_code_key (short_code) UNIQUE
├── idx_network_arc_network_id (network_id)
├── idx_network_arc_from_node (from_node)
├── idx_network_arc_to_node (to_node)
├── idx_network_arc_connectivity (from_node, to_node) -- topology traversal
├── idx_network_arc_river (river)
└── idx_network_arc_version (network_version_id)
```

### **4. network_node (node-specific attributes)**
```
Table: network_node
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- Matches network.short_code
├── network_id           INTEGER                    -- FK to network.id (CASCADE delete)
├── riv_mi               NUMERIC                    -- River mile
├── c2vsim_gw            VARCHAR                    -- C2VSim groundwater cell link
├── c2vsim_sw            VARCHAR                    -- C2VSim surface water subregion link
├── nrest_gage           VARCHAR                    -- NRCS stream gauge identifier
├── strm_code            VARCHAR                    -- Stream code
├── rm_ii                VARCHAR                    -- River mile (alternate representation)
├── model_source_id      INTEGER DEFAULT 1          -- FK to model_source.id (RESTRICT)
├── source_id            INTEGER DEFAULT 4          -- FK to source.id (RESTRICT)
├── network_version_id   INTEGER DEFAULT 12         -- FK to version.id (RESTRICT)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER                    -- FK to developer.id (RESTRICT)
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER                    -- FK to developer.id (RESTRICT)

Records: 1,544
Note: CASCADE on network_id — nodes are deleted when their parent network element is removed.
Indexes:
├── network_node_pkey (id)
├── network_node_short_code_key (short_code) UNIQUE
├── idx_network_node_network_id (network_id)
├── idx_network_node_strm_code (strm_code)
└── idx_network_node_version (network_version_id)
```

### **5. network_gis (PostGIS geometry for network elements)**
```
Table: network_gis
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR                    -- Matches network.short_code
├── network_id           INTEGER UNIQUE             -- FK to network.id (CASCADE delete)
│                                                   -- UNIQUE enforces one-to-one with network
├── precision_level      VARCHAR DEFAULT 'precise'  -- "precise", "approximate", "schematic"
├── geom_wkt             TEXT                       -- WKT geometry string (human-readable copy)
├── srid                 INTEGER DEFAULT 4326       -- EPSG:4326 (WGS84)
├── geom                 GEOMETRY                   -- PostGIS binary (GiST spatial index)
├── estimated_accuracy_meters NUMERIC
├── source_id            INTEGER DEFAULT 4          -- FK to source.id (RESTRICT)
├── network_version_id   INTEGER DEFAULT 12         -- FK to version.id (RESTRICT)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER                    -- FK to developer.id (RESTRICT)
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER                    -- FK to developer.id (RESTRICT)

Records: 4,154 (not all 6,908 network elements have GIS geometry; network.has_gis tracks this)
Note: CASCADE on network_id. source = geopackage (CalSim3 GeoSchematic).
Indexes:
├── network_gis_pkey (id)
├── idx_network_gis_network_id (network_id) UNIQUE
├── idx_network_gis_short_code (short_code)
├── idx_network_gis_precision (precision_level)
├── idx_network_gis_version (network_version_id)
└── idx_network_gis_geom (geom)  -- GiST spatial index
```

---

## **Layer 03 — ENTITY LAYER** *(GIS and operational entity tables)*

### **1. reservoir (reservoir geographic base table)**
```
Table: reservoir
├── id                   SERIAL PRIMARY KEY
├── calsim_short_code    VARCHAR(20)                -- CalSim identifier (e.g., "SHSTA", "OROVL")
├── reservoir_name       TEXT                       -- Full reservoir name
├── geom_wkt             TEXT                       -- WKT geometry
├── srid                 INTEGER DEFAULT 4326
├── geom                 GEOMETRY (computed)        -- PostGIS binary (STORED)
├── area_sqkm            NUMERIC                    -- Surface area in square km
├── elevation_m          NUMERIC                    -- Elevation in meters
├── gnis_id              TEXT                       -- GNIS identifier
├── nhd_permanent_id     TEXT                       -- NHD Permanent Identifier
├── data_source          TEXT                       -- Original data source description
├── source_id            INTEGER                    -- FK source.id
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK developer.id

Records: 7 major reservoirs
Note: Geographic base table. The related `reservoir_entity` table holds operational attributes
(capacity_taf, dead_pool_taf). Both use `calsim_short_code` / `short_code` as the join key.
Used by: tier_location_result (location_type = 'reservoir', location_id = reservoir.calsim_short_code)
```

### **2. compliance_station (compliance monitoring stations)**
```
Table: compliance_station
├── id                   SERIAL PRIMARY KEY
├── station_code         VARCHAR(20)                -- Station identifier (e.g., "JP", "EX2")
├── station_name         TEXT                       -- Full station name
├── latitude             NUMERIC
├── longitude            NUMERIC
├── srid                 INTEGER DEFAULT 4326
├── geom_wkt             TEXT                       -- WKT geometry
├── geom                 GEOMETRY (computed)        -- PostGIS binary (STORED)
├── tier_use             TEXT                       -- Which tier indicator uses this station
├── data_source          TEXT                       -- Original data source description
├── notes                TEXT
├── source_id            INTEGER                    -- FK source.id
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK developer.id

Records: 2 compliance stations
Used by: tier_location_result (location_type = 'compliance_station', location_id = station_code)
         for FW_DELTA_USES tier monitoring
```

### **3. du_agriculture_entity (agricultural demand units)**
```
Table: du_agriculture_entity
├── id                   SERIAL PRIMARY KEY
├── du_id                VARCHAR UNIQUE NOT NULL    -- CalSim demand unit ID (e.g. "02_PA1", "50_PU")
├── wba_id               VARCHAR                    -- Water budget area ID
├── hydrologic_region    VARCHAR                    -- Region name (text, legacy — prefer hydrologic_region_id)
├── dups                 INTEGER                    -- Number of demand unit polygons
├── du_class             VARCHAR                    -- "Agriculture"
├── cs3_type             VARCHAR                    -- CalSim3 type code (e.g. "CVP_PAG", "SWP_PAG")
├── total_acres          NUMERIC                    -- Total acreage across all polygons
├── polygon_count        INTEGER
├── source               VARCHAR                    -- Source agency abbreviation
├── model_source         VARCHAR                    -- "calsim3"
├── agency               VARCHAR
├── provider             VARCHAR                    -- Water provider name
├── gw                   BOOLEAN DEFAULT TRUE       -- Has groundwater supply
├── sw                   BOOLEAN DEFAULT TRUE       -- Has surface water supply
├── point_of_diversion   TEXT
├── diversion_arc        VARCHAR                    -- CalSim arc code
├── river_reach          VARCHAR
├── river_mile_start     NUMERIC
├── river_mile_end       NUMERIC
├── bank                 VARCHAR                    -- "L" (left), "R" (right)
├── area_acres           NUMERIC                    -- Operational area
├── annual_diversion_taf NUMERIC                    -- Annual average diversion in TAF
├── demand_unit          VARCHAR
├── table_id             VARCHAR
├── has_gis_data         BOOLEAN DEFAULT TRUE
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER DEFAULT 1          -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
├── updated_by           INTEGER DEFAULT 1          -- FK to developer.id
├── hydrologic_region_id INTEGER                    -- FK to hydrologic_region.id
└── model_source_id      INTEGER                    -- FK to model_source.id

Records: 144
Seed: seed_tables/04_calsim_data/du_agriculture_entity.csv
Indexes:
├── du_agriculture_entity_pkey (id)
├── du_agriculture_entity_du_id_key (du_id) UNIQUE
├── idx_du_ag_region (hydrologic_region)
├── idx_du_ag_wba (wba_id)
├── idx_du_ag_type (cs3_type)
└── idx_du_ag_provider (provider)
```

### **4. du_urban_entity (urban/community water system demand units)**
```
Table: du_urban_entity
├── id                   SERIAL PRIMARY KEY
├── du_id                VARCHAR UNIQUE NOT NULL    -- CalSim demand unit ID (e.g. "ACWD", "MWD")
├── wba_id               VARCHAR                    -- Water budget area ID
├── hydrologic_region    VARCHAR                    -- Region name (text, legacy)
├── dups                 INTEGER DEFAULT 0
├── du_class             VARCHAR DEFAULT 'Urban'
├── cs3_type             VARCHAR                    -- CalSim3 type code (e.g. "CVP_PMI", "SWP_PMI")
├── total_acres          NUMERIC
├── polygon_count        INTEGER DEFAULT 1
├── community_agency     TEXT                       -- Water agency name
├── gw                   VARCHAR                    -- Groundwater supply indicator
├── sw                   VARCHAR                    -- Surface water supply indicator
├── point_of_diversion   TEXT
├── source               VARCHAR
├── model_source         VARCHAR
├── has_gis_data         BOOLEAN DEFAULT TRUE
├── primary_contractor_short_code VARCHAR          -- FK-style reference to mi_contractor.short_code
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER DEFAULT 1          -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
├── updated_by           INTEGER DEFAULT 1          -- FK to developer.id
├── hydrologic_region_id INTEGER                    -- FK to hydrologic_region.id
└── model_source_id      INTEGER                    -- FK to model_source.id

Records: 145
Seed: seed_tables/04_calsim_data/du_urban_entity.csv
Indexes:
├── du_urban_entity_pkey (id)
├── du_urban_entity_du_id_key (du_id) UNIQUE
├── idx_du_urban_entity_du_id (du_id)
├── idx_du_urban_entity_region (hydrologic_region)
├── idx_du_urban_entity_wba_id (wba_id)
├── idx_du_urban_entity_type (cs3_type)
└── idx_du_urban_entity_contractor (primary_contractor_short_code)
```

### **5. du_refuge_entity (wildlife refuge demand units)**
```
Table: du_refuge_entity
├── id                   SERIAL PRIMARY KEY
├── du_id                VARCHAR UNIQUE NOT NULL    -- CalSim demand unit ID
├── wba_id               VARCHAR                    -- Water budget area ID
├── hydrologic_region    VARCHAR                    -- Region name (text)
├── dups                 INTEGER
├── du_class             VARCHAR DEFAULT 'Refuge'
├── cs3_type             VARCHAR                    -- CalSim3 type code
├── total_acres          NUMERIC
├── polygon_count        INTEGER DEFAULT 1
├── refuge_or_wildlife_area TEXT                   -- Official refuge name
├── managed_by           VARCHAR                    -- Managing agency (e.g. "USFWS", "DFW")
├── provider             VARCHAR
├── gw                   BOOLEAN DEFAULT FALSE      -- Refuges typically SW only
├── sw                   BOOLEAN DEFAULT TRUE
├── point_of_diversion_conveyance TEXT
├── source               VARCHAR
├── model_source         VARCHAR DEFAULT 'calsim3'
├── has_gis_data         BOOLEAN DEFAULT TRUE
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER DEFAULT 1          -- FK to developer.id (RESTRICT)
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER DEFAULT 1          -- FK to developer.id (RESTRICT)

Records: 18
Seed: seed_tables/04_calsim_data/du_refuge_entity.csv (or seed_tables/03_entity/)
Indexes:
├── du_refuge_entity_pkey (id)
├── du_refuge_entity_du_id_key (du_id) UNIQUE
├── idx_du_refuge_entity_cs3_type (cs3_type)
└── idx_du_refuge_entity_hydrologic_region (hydrologic_region)
```

### **6. reservoir_entity (reservoir operational attributes)**
```
Table: reservoir_entity
├── id                   INTEGER PRIMARY KEY        -- Manually assigned (not SERIAL)
├── network_node_id      VARCHAR                    -- Matching network node short_code
├── short_code           VARCHAR UNIQUE             -- Reservoir identifier (e.g. "SHSTA", "OROVL")
├── name                 VARCHAR                    -- Full reservoir name
├── description          TEXT
├── associated_river     VARCHAR                    -- River name
├── entity_type_id       INTEGER DEFAULT 1          -- FK to network_entity_type.id
├── schematic_type_id    INTEGER                    -- Internal type classifier
├── hydrologic_region_id INTEGER                    -- FK to hydrologic_region.id
├── capacity_taf         NUMERIC                    -- Total capacity in TAF
├── dead_pool_taf        NUMERIC                    -- Dead pool storage in TAF
├── surface_area_acres   NUMERIC
├── operational_purpose  VARCHAR                    -- "CVP", "SWP", "Local", etc.
├── has_tiers            BOOLEAN DEFAULT FALSE      -- Whether tier results exist for this reservoir
├── is_main              BOOLEAN DEFAULT FALSE      -- Primary reservoir in a multi-level system
├── has_gis_data         INTEGER DEFAULT 1          -- Presence of GIS record (legacy INTEGER flag)
├── entity_version_id    INTEGER DEFAULT 1          -- FK to version.id
├── source_ids           TEXT                       -- Comma-separated source IDs
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER DEFAULT 1          -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER DEFAULT 1          -- FK to developer.id

Records: 92 (includes main reservoirs + storage zone sub-entries)
Note: Companion to `reservoir` (GIS base). Join on short_code / calsim_short_code.
      See ETL capacity override constants in calculate_reservoir_statistics.py.
Seed: seed_tables/04_calsim_data/reservoir_entity.csv (and reservoir_sublayer/)
Indexes:
├── reservoir_entity_pkey (id)
├── reservoir_entity_short_code_key (short_code) UNIQUE
├── idx_reservoir_entity_region (hydrologic_region_id)
├── idx_reservoir_entity_has_tiers (has_tiers)
└── idx_reservoir_entity_is_main (is_main)
```

### **7. mi_contractor (M&I SWP/CVP contractors)**
```
Table: mi_contractor
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- Contractor code (e.g. "ACWD", "MWD", "SCVWD")
├── contractor_name      VARCHAR                    -- Full contractor name
├── project              VARCHAR                    -- "SWP", "CVP", or "Both"
├── region               VARCHAR                    -- Geographic region
├── contractor_type      VARCHAR                    -- "Urban", "Agricultural", "Mixed"
├── contract_amount_taf  NUMERIC                    -- Table A / Contract allocation in TAF/yr
├── source_contractor_id INTEGER                    -- External contractor ID from source data
├── source_file          VARCHAR                    -- Source document/file reference
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER DEFAULT 1          -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER DEFAULT 1          -- FK to developer.id

Records: 30
Seed: seed_tables/04_calsim_data/mi_contractor.csv
Used by: du_urban_entity.primary_contractor_short_code, mi_delivery_monthly, mi_contractor_period_summary
Indexes:
├── mi_contractor_pkey (id)
├── mi_contractor_short_code_key (short_code) UNIQUE
├── idx_mi_contractor_project (project)
├── idx_mi_contractor_region (region)
├── idx_mi_contractor_type (contractor_type)
└── idx_mi_contractor_short_code (short_code)
```

### **8. wba (Water Budget Areas)**
```
Table: wba
├── id                   SERIAL PRIMARY KEY
├── wba_id               VARCHAR(10)                -- WBA identifier (e.g., "DETAW", "02N", "06S")
├── wba_name             TEXT                       -- Full WBA name
├── hydrologic_region_id INTEGER                    -- FK to hydrologic_region.id
├── source_id            INTEGER                    -- FK to source.id
├── geom_wkt             TEXT                       -- WKT geometry
├── srid                 INTEGER DEFAULT 4326
├── geom                 GEOMETRY (computed)        -- PostGIS binary (STORED)
├── area_acres           NUMERIC                    -- Area in acres
├── comments             TEXT
├── data_source          TEXT                       -- Original data source description
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK developer.id

Records: 42 Water Budget Areas
Used by: tier_location_result (location_type = 'wba', location_id = wba.wba_id) for GW_STOR tier mapping
```

---

## **Layer 04 — VARIABLE LAYER**

> CalSim variable definitions and type classifications. Tables physically reside in the `public` schema.
> Seed data is in `seed_tables/04_variable/`.

### **1. calsim_model_variable_type**
```
Table: calsim_model_variable_type
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "output", "control", "decision", etc.
├── label                TEXT NOT NULL              -- "Output", "Control", etc.
├── description          TEXT                       -- Classification description
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMPTZ DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMPTZ DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (8 total — CalSim model variable behavior):
├── output:       Standard CalSim model output variables (flows, diversions, storage)
├── control:      Operational control indicators and binary flags
├── decision:     Model decision variables and optimization targets
├── state:        State variables including storage zones and bookkeeping accounts
├── input:        External inputs and boundary conditions
├── intermediate: Calculated intermediate values used in model logic
├── aggregate:    Variables that sum or combine multiple reservoir/system components
└── index:        Index variables

Seed: seed_tables/04_variable/calsim_model_variable_type.csv
```

### **2. derived_variable_type**
```
Table: derived_variable_type
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "sector_aggregate", "delta_variable", etc.
├── label                TEXT NOT NULL              -- "Sector Aggregate", etc.
├── description          TEXT                       -- Classification description
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMPTZ DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMPTZ DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (4 total — derived/computed variable categories):
├── sector_aggregate:        Variables that aggregate across a sector
├── delta_variable:          Variables specific to Delta conditions and operations
├── environmental_indicator: Environmental metrics and indicators
└── regional_summary:        Variables that aggregate across a region

Note: INCOMPLETE — additional types expected as derived variable pipeline expands.
Seed: seed_tables/04_variable/derived_variable_type.csv
```

### **3. variable_type**
```
Table: variable_type
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "delivery", "gw_pumping", "PA", etc.
├── label                TEXT NOT NULL              -- "delivery", "groundwater pumping", etc.
├── description          TEXT                       -- Variable type description
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (6 total — water use classification):
├── delivery: water delivery
├── gw_pumping: groundwater pumping
├── PA: project agricultural
├── PR: project wildlife refuge
├── PU: project community water system (M&I)
└── unknown: unknown or unclassified

Used by: du_urban_variable (variable_type_id)
Seed: seed_tables/04_variable/variable_type.csv
```

### **4. channel_variable (and planned: reservoir_variable, inflow_variable, derived_variable)**
```
These tables hold CalSim variable definitions (model output names).
Each maps a CalSim variable name to its type classification and entity association.

channel_variable      -- flow/diversion arc variables   (FK to channel_entity; includes MIF regulatory vars)
reservoir_variable    -- storage/release variables       [PLANNED — not yet created]
inflow_variable       -- inflow boundary conditions      [PLANNED — not yet created]
derived_variable      -- computed / post-processed vars  [PLANNED — not yet created]

channel_variable notable fields (migration 23):
├── is_regulatory       BOOLEAN    -- TRUE for C_*_MIF variables (binding regulatory minimums)
├── regulatory_authority VARCHAR   -- "CalSim-III" for MIF variables
└── channel_entity_id   INTEGER FK -- links variable to its physical channel entity

channel_variable records: ~1352 total (migration 23 added 20 MIF + 1 ISF001_OMR027)
  - 20 C_*_MIF variables (FLOW-MIN-INSTREAM, is_regulatory=true):
    AMR004, FTR003, FTR029, FTR059, KSWCK, MCD005, MOK028, NTOMA,
    SAC049, SAC122, SAC148, SAC257, SAC289, SJR070, SJR127,
    STS011, STS059, TRN111, TUO003, YUB002
  - NOTE: C_SAC000_MIF is absent from CalSim DV — no MIF for delta confluence reach

channel_entity new columns (migration 23):
├── watershed_short_code  VARCHAR FK to watershed.short_code  -- geographic watershed grouping
├── unimp_sv_variable     VARCHAR    -- specific UNIMP_* SV variable for % unimpaired calc
│                                    -- may differ from watershed.unimp_sv_variable (e.g. SAC mainstem)
├── has_mif               BOOLEAN    -- companion C_*_MIF exists in DV
├── has_eflows            BOOLEAN    -- companion EFLOWS_* exists in SV (confirmed for original 17 reaches)
└── channel_class         VARCHAR    -- 'stream' | 'canal' | 'reservoir_release'

Seed: seed_tables/04_variable/*.csv
      seed_tables/04_calsim_data/channel_entity.csv (updated migration 23)
```

---

## **Layer 06 — SCENARIO LAYER**

> Theme definitions and theme-scenario links are in **Layer 08 — THEME LAYER**.

### **1. scenario_hydroclimate_sibling (operational configuration families)**

```
Table: scenario_hydroclimate_sibling
├── short_code          VARCHAR PRIMARY KEY        -- Hist_adj scenario code (e.g. 's0020')
├── name                VARCHAR                    -- Display name: "Current operations"
├── short_description   TEXT                       -- Brief 1-2 sentence description
├── long_description    TEXT                       -- Full multi-paragraph description
├── baseline_group      VARCHAR                    -- FK to scenario_hydroclimate_sibling.short_code (NULL for roots)
├── scenario_author_id  INTEGER                    -- FK to scenario_author.id
├── model_source_id     INTEGER                    -- FK to model_source.id
├── created_by          INTEGER NOT NULL DEFAULT 2 -- FK to developer.id
├── updated_by          INTEGER NOT NULL DEFAULT 2 -- FK to developer.id
├── created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()

Records: 26 (24 active operational configs + 2 inactive agency baselines: s0022, s0038)

Note: Each row represents an operational configuration that may be run across multiple
      hydroclimates. The short_code matches the hist_adj scenario that first defined
      this configuration. All scenarios with the same hydroclimate_sibling share the
      same operational assumptions and differ only in hydroclimate inputs.

      baseline_group points to the sibling this config was derived from.
      Root baselines (s0011, s0022, s0038, s0065) have baseline_group = NULL.

Foreign keys:
├── Ref: scenario_hydroclimate_sibling.baseline_group > scenario_hydroclimate_sibling.short_code [delete: restrict, update: cascade]
├── Ref: scenario_hydroclimate_sibling.scenario_author_id > scenario_author.id [delete: restrict, update: cascade]
└── Ref: scenario_hydroclimate_sibling.model_source_id > model_source.id [delete: restrict, update: cascade]

Indexes:
└── idx_hydro_sibling_baseline (baseline_group)
```

### **2. scenario (water management scenarios)**

```
Table: scenario
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR NOT NULL UNIQUE    -- Friendly identifier like "s0011"
├── run_name             VARCHAR                    -- Full technical run name like "s0011_adjBL_wTUCP"
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── hydroclimate_id      INTEGER                    -- FK to hydroclimate.id
├── hydroclimate_sibling  VARCHAR                    -- FK to scenario_hydroclimate_sibling.short_code
├── scenario_version_id  INTEGER DEFAULT 1          -- FK to version.id (scenario family)
├── created_by           INTEGER NOT NULL DEFAULT 2 -- FK to developer.id
├── updated_by           INTEGER NOT NULL DEFAULT 2 -- FK to developer.id
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()

Records: 72 active scenarios across 3 hydroclimates (24 dwr_hist_adj + 24 cc50 + 24 cc95)
         3 inactive scenarios (s0022 USBR Alt2V1, s0029 eflows v1, s0038 USBR Alt3)

Note: Shared operational configuration attributes (name, descriptions, baseline,
      scenario_author, model_source) live in the scenario_hydroclimate_sibling table.
      Each hydroclimate_sibling groups operationally identical scenarios across hydroclimates.
      The short_code is the hist_adj scenario's short_code (e.g. 's0020').

Columns moved to sibling_group table (migration 47):
  name, short_description, long_description, baseline_scenario_id,
  scenario_author_id, model_source_id

Dropped columns (migration 35): subtitle, narrative, source_scenario_id, slr_id

Foreign keys:
└── Ref: scenario.hydroclimate_sibling > scenario_hydroclimate_sibling.short_code [delete: restrict, update: cascade]

Note: hydroclimate_id, scenario_version_id, created_by, updated_by
      do not have explicit FK constraints in the database.

Indexes:
├── scenario_short_code_key (short_code) -- Unique constraint
├── idx_scenario_run_name_active (run_name, is_active)
├── idx_scenario_active (is_active)
├── idx_scenario_hydroclimate (hydroclimate_id)
├── idx_scenario_active_version (is_active, scenario_version_id)
└── idx_scenario_hydro_sibling (hydroclimate_sibling)
```

### **3. scenario_author (scenario authors/groups)**

```
Table: scenario_author
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "dwr", "usbr", "coeqwal"
├── name                 TEXT NOT NULL              -- Full name / description
├── email                TEXT
├── organization         TEXT
├── affiliation          TEXT
├── is_active            INTEGER DEFAULT 1
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Foreign keys:
├── Ref: scenario_author.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: scenario_author.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── scenario_author_short_code_key (short_code) -- Unique constraint
└── idx_scenario_author_active (is_active, short_code)

Records: 3 authors
├── 1  dwr     California Department of Water Resources
├── 2  usbr    US Bureau of Reclamation
└── 3  coeqwal COEQWAL modeling team based on model files provided by USBR and DWR
```

### **4. scenario_tag (fine-grained scenario classification tags)**

```
Table: scenario_tag
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR NOT NULL UNIQUE    -- "baseline", "groundwater", "flows", etc.
├── label                VARCHAR NOT NULL           -- "Baseline", "Groundwater", "Flows"
├── description          TEXT
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (10): baseline, groundwater, agriculture, flows, drinking_water,
             infrastructure, delta, reservoir, salmon, environment

Foreign keys:
├── Ref: scenario_tag.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: scenario_tag.updated_by > developer.id [delete: restrict, update: cascade]

Note: Distinct from the 6 broad research themes in the theme table (Layer 08).
      Tags are fine-grained classifications derived from scenario metadata.
```

### **5. scenario_tag_link (scenario-tag relationships)**

```
Table: scenario_tag_link
├── scenario_id          INTEGER NOT NULL           -- FK to scenario.id
├── tag_id               INTEGER NOT NULL           -- FK to scenario_tag.id
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Primary key: (scenario_id, tag_id)

Foreign keys:
├── Ref: scenario_tag_link.scenario_id > scenario.id [delete: cascade, update: cascade]
├── Ref: scenario_tag_link.tag_id > scenario_tag.id [delete: cascade, update: cascade]
├── Ref: scenario_tag_link.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: scenario_tag_link.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── scenario_tag_link_pkey (scenario_id, tag_id)
└── idx_scenario_tag_link_reverse (tag_id, scenario_id)
```

---

## **Layer 05 — ASSUMPTIONS + OPERATIONS LAYER**

> **Classification:**
> - **Assumptions** = model inputs that represent broad context (land use, groundwater model).
>   SLR (sea level rise) is now its own table in Layer 07.
> - **Operations** = active policy/regulatory actions applied to a scenario:
>   TUCP actions, SGMA / GW restrictions, Delta regulations, BiOps, Infrastructure,
>   Flows, Allocation / Priorities.

### **1. assumption_category (assumption categories)**

Note: `assumption_definition.assumption_category_id` is an integer FK to `assumption_category.id`
(normalized from TEXT in migration 36). IDs resequenced in migration 42.

```
Table: assumption_category
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "land_use", "gw_model"
├── label                TEXT
├── description          TEXT
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL

Values (2):
├── land_use (id=1):  Land use scenario (LandIQ vintages)
└── gw_model (id=2):  Groundwater model used (C2VSimFG, etc.)

Indexes:
├── assumption_category_pkey (id) UNIQUE PRIMARY
└── assumption_category_short_code_key (short_code) UNIQUE
```

### **2. assumption_definition (assumption definitions)**

```
Table: assumption_definition
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR NOT NULL UNIQUE    -- "lu_2020_landiq", "gw_model", etc.
├── name                 VARCHAR                    -- "2020 LandIQ Land Use"
├── short_title          VARCHAR
├── assumption_category_id INTEGER NOT NULL         -- FK to assumption_category.id
├── description          TEXT
├── source_id            INTEGER                    -- FK to source.id (james_gilbert = 2)
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── notes                TEXT
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_by           INTEGER NOT NULL           -- FK to developer.id
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()

Records: 6 (IDs 1-6, resequenced in migration 42)
  1  lu_2004_2013               land_use (1)   2004-2013 average land use
  2  lu_updated                 land_use (1)   Updated land use
  3  lu_proj_reductions         land_use (1)   Projected reductions in land use
  4  gw_model                   gw_model (2)   Groundwater model
  5  lu_2020_landiq             land_use (1)   2020 LandIQ land use
  6  lu_2020_landiq_reduced_ag  land_use (1)   2020 LandIQ with reduced ag acreage

Note: Only land_use + gw_model assumptions remain here; operational policies
      (TUCP, SGMA, BiOps, flows, etc.) are in operation_definition.

Dropped columns (migration 42): subtitle, simple_description, narrative,
    source_access_date, file, assumptions_version_id
Changed: source (TEXT) to source_id (INTEGER FK to source.id)
         is_active: INTEGER to BOOLEAN

Foreign keys:
├── Ref: assumption_definition.assumption_category_id > assumption_category.id [delete: restrict, update: cascade]
└── Ref: assumption_definition.source_id > source.id [delete: restrict, update: cascade]

Indexes:
├── assumption_definition_short_code_key (short_code)
├── idx_assumption_definition_category_id (assumption_category_id)
└── idx_assumption_definition_active (is_active)
```

### **3. scenario_key_assumption_link (scenario-assumption relationships)**

```
Table: scenario_key_assumption_link
├── scenario_id          INTEGER NOT NULL           -- FK to scenario.id
├── assumption_id        INTEGER NOT NULL           -- FK to assumption_definition.id
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Primary key: (scenario_id, assumption_id)

Foreign keys:
├── Ref: scenario_key_assumption_link.scenario_id > scenario.id [delete: cascade, update: cascade]
└── Ref: scenario_key_assumption_link.assumption_id > assumption_definition.id [delete: cascade, update: cascade]

Indexes:
├── scenario_key_assumption_link_pkey (scenario_id, assumption_id)
└── idx_scenario_assumption_reverse (assumption_id, scenario_id)
```

### **4. operation_category (operation categories)**

```
Table: operation_category
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL
├── name                 TEXT
├── description          TEXT
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL            -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL            -- FK to developer.id

Values (9 total, IDs 1-9 — resequenced in migration 42):
├── 1  comm_delivery:       Community water delivery prioritization
├── 2  delta_outflow:       Delta outflow requirements
├── 3  carryover:           Reservoir carryover storage requirements
├── 4  regulatory_salinity: Delta salinity standards (X2)
├── 5  tucp:                Temporary Urgency Change Petitions and Orders
├── 6  gw_restrictions:     Groundwater pumping restrictions (SGMA-type)
├── 7  infrastructure:      Water infrastructure configuration (tunnels, reservoirs)
├── 8  flow:                Instream flow and minimum flow objectives
└── 9  biops:               Biological Opinions (NMFS / USFWS for USBR LTO)

Foreign keys:
├── Ref: operation_category.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: operation_category.updated_by > developer.id [delete: restrict, update: cascade]
```

### **5. operation_definition (operation definitions)**

```
Table: operation_definition
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR NOT NULL UNIQUE    -- see Values section below
├── name                 VARCHAR
├── short_title          VARCHAR
├── operation_category_id INTEGER NOT NULL          -- FK to operation_category.id
├── description          TEXT                       -- merged from simple_description + narrative
├── source_id            INTEGER                    -- FK to source.id (james_gilbert = 2)
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── notes                TEXT
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_by           INTEGER NOT NULL           -- FK to developer.id
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()

Dropped columns (migration 42): subtitle, simple_description, narrative, operation_version_id
Changed: source (TEXT) to source_id (INTEGER FK to source.id)
         is_active: INTEGER to BOOLEAN
         Category IDs remapped: old 27-31 to 5-9

Values (28 rows, IDs 1-28 | short_code | category_id to category):
  -- comm_delivery (1) — CVP/SWP allocation priorities and community delivery
   1  comm_delivery_HHS           1   Prioritize human health & safety deliveries
   2  comm_delivery_functional    1   Prioritize functional community water needs
   3  comm_delivery_full          1   Prioritize full community water demands
  27  alloc_standard              1   Standard CVP/SWP allocation (no modification)
  28  cvp_settlement_to_zero      1   CVP Settlement allocations reduced to 0% (Alt3)
  -- delta_outflow (2) — Delta outflow requirements
   4  delta_outflow_35            2   35% of unimpaired flow (Alt3)
   5  delta_outflow_45            2   45% of unimpaired flow (Alt3)
   6  delta_outflow_55            2   55% of unimpaired flow (Alt3)
   7  delta_outflow_65            2   65% of unimpaired flow (Alt3)
  26  delta_regs_standard         2   Standard D1641 delta regulations
  -- carryover (3) — reservoir carryover storage
   8  increase_Shasta_co          3   Increase Shasta carryover target by 20%
  -- regulatory_salinity (4) — Delta salinity standards
   9  delta_salinity_standards    4   Relax Fall X2 salinity standard
  -- tucp (5) — Temporary Urgency Change Petitions/Orders
  10  TUCP_TUCO                   5   TUCPs/TUCOs active
  22  tucp_not_active             5   TUCPs/TUCOs not active
  -- gw_restrictions (6) — SGMA-type groundwater restrictions
  11  SGMA_SJV                    6   SGMA pumping limits — San Joaquin Valley
  12  SGMA_SAC                    6   SGMA pumping limits — Sacramento Valley
  13  SGMA_CV                     6   SGMA pumping limits — entire Central Valley
  23  gw_none                     6   No GW restrictions
  -- infrastructure (7) — water conveyance infrastructure
  14  DCP_6000                    7   Delta Conveyance Project at 6000 CFS
  15  DCP_Bethany                 7   Delta Conveyance Project — Bethany alignment
  24  infra_standard              7   Standard infrastructure (no DCP)
  -- flow (8) — instream flow requirements
  16  no_min_flow                 8   Remove CV tributary min flow requirements
  17  functional_flows            8   Functional flow requirements at 17 locations
  18  salmon_flows                8   Salmon-friendly flows (Sacramento R.)
  25  flow_standard               8   Standard/existing min flow requirements
  -- biops (9) — Biological Opinions
  19  biops_2024                  9   2024 USBR LTO proposed action BiOps
  20  biops_standard              9   2019 BiOps / 2020 ITP for SWP (standard)
  21  biops_modified_2019         9   Modified versions of 2019 BiOps (Alt3, s0044/s0045)

Foreign keys:
├── Ref: operation_definition.operation_category_id > operation_category.id [delete: restrict, update: cascade]
└── Ref: operation_definition.source_id > source.id [delete: restrict, update: cascade]

Indexes:
├── operation_definition_short_code_key (short_code)
├── idx_operation_definition_category_id (operation_category_id)
└── idx_operation_definition_active (is_active)
```

### **6. scenario_key_operation_link (scenario-operation relationships)**

```
Table: scenario_key_operation_link
├── scenario_id          INTEGER NOT NULL           -- FK to scenario.id
├── operation_id         INTEGER NOT NULL           -- FK to operation_definition.id
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Primary key: (scenario_id, operation_id)

Foreign keys:
├── Ref: scenario_key_operation_link.scenario_id > scenario.id [delete: cascade, update: cascade]
└── Ref: scenario_key_operation_link.operation_id > operation_definition.id [delete: cascade, update: cascade]

Indexes:
├── scenario_key_operation_link_pkey (scenario_id, operation_id)
└── idx_scenario_operation_reverse (operation_id, scenario_id)
```

---

## **Layer 07 — HYDROCLIMATE LAYER**

### **1. hydroclimate (hydroclimate conditions)**

```
Table: hydroclimate
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "historical", "2040_central", "2070_dry", etc.
├── name                 TEXT                       -- "Historical (1922-2021)"
├── subtitle             TEXT
├── short_title          TEXT
├── simple_description   TEXT
├── description          TEXT
├── narrative            JSONB
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── projection_year      INTEGER                    -- 2040, 2070, etc.
├── source_id            INTEGER                    -- FK source.id
├── notes                TEXT
├── hydroclimate_version_id INTEGER                 -- FK to version.id (hydroclimate family)
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Note: slr_value and slr_unit_id removed — sea level rise is now in the slr table (see below).
      Use scenario.slr_id to slr.id to link a scenario to its SLR condition.

Foreign keys:
├── Ref: hydroclimate.hydroclimate_version_id > version.id [delete: restrict, update: cascade]
├── Ref: hydroclimate.source_id > source.id [delete: restrict, update: cascade]
├── Ref: hydroclimate.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: hydroclimate.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── hydroclimate_short_code_key (short_code) -- Unique constraint
├── idx_hydroclimate_active (is_active, short_code)
└── idx_hydroclimate_source (source_id)

Values (6 total):
├── dwr_hist (id=1): DWR historical (1922-2021)
├── dwr_hist_adj (id=2): DWR historically-adjusted (adjusted for 20th century climate warming)
├── cc50 (id=3): Warmer and Drier I — 50% exceedance, median future (+1.5C, -3% precip, 2043)
├── cc95 (id=4): Warmer and Drier II — 95% exceedance, extreme hot/dry (+1.8C, -9% precip, 2043)
├── CMIP6_TaiESM1_SSP370 (id=5): Warmer and Drier III (+1.9C, -7% precip, 2043)
└── CMIP6_CESM2-LENS_SSP370 (id=6): Warmer and Drier IV (+1.4C, -12% precip, 2043)

Records: 6 hydroclimate conditions
```

### **2. slr (sea level rise scenarios)**

```
Table: slr
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "none", "slr_15", "slr_30", "slr_60"
├── label                TEXT NOT NULL              -- "No sea level rise", "15mm SLR", etc.
├── slr_value_mm         NUMERIC                    -- SLR amount in millimetres (0, 15, 30, 60)
├── description          TEXT
├── source               TEXT                       -- FK to source.source (james_gilbert, etc.)
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMPTZ DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMPTZ DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (4 total):
├── none:   No sea level rise (0mm)
├── slr_15: 15mm sea level rise
├── slr_30: 30mm sea level rise
└── slr_60: 60mm sea level rise

Foreign keys:
├── Ref: slr.source > source.source [delete: restrict, update: cascade]
├── Ref: slr.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: slr.updated_by > developer.id [delete: restrict, update: cascade]

Referenced by: scenario.slr_id

Seed: seed_tables/07_hydroclimate/slr.csv
```

### **3. hydroclimate_source (hydroclimate data sources)**

Status: PLANNED — not yet created in the database.
`hydroclimate.source_id` currently points to the generic `source` table (FK source.id).
When this dedicated table is created, `hydroclimate.source_id` should be migrated to reference it.

```
Table: hydroclimate_source   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "dwr_cctag", "usgs", etc.
├── name                 TEXT                       -- "DWR Climate Change Technical Advisory Group"
├── description          TEXT
├── citation             TEXT
├── url                  TEXT
├── notes                TEXT
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK developer.id
```

---

## **Layer 08 — THEME LAYER**

> Research themes organize the frontend storylines. Each theme links to one or more scenarios.
> Seed data is in `seed_tables/08_theme/`.

### **1. theme (research themes)**

```
Table: theme
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "cws", "ag_gw", "eco", "delta", "climate", "governance"
├── is_active            INTEGER NOT NULL DEFAULT 1
├── name                 TEXT NOT NULL              -- "Community water systems"
├── subtitle             TEXT
├── short_title          TEXT
├── simple_description   TEXT
├── description          TEXT
├── description_next     TEXT
├── narrative            JSONB
├── outcome_description  TEXT
├── outcome_narrative    TEXT
├── source               TEXT                       -- FK to source.source (wietske_medema, etc.)
├── theme_version_id     INTEGER NOT NULL DEFAULT 1 -- FK to version.id (theme family)
├── created_by           INTEGER NOT NULL DEFAULT 1 -- FK to developer.id
├── updated_by           INTEGER                    -- FK to developer.id
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()

Foreign keys:
├── Ref: theme.source > source.source [delete: restrict, update: cascade]
├── Ref: theme.theme_version_id > version.id [delete: restrict, update: cascade]
├── Ref: theme.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: theme.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── theme_short_code_key (short_code) -- Unique constraint
├── idx_theme_short_code_active (short_code, is_active)
└── idx_theme_active (is_active)

Values (6 total):
├── cws:        Community water systems
├── ag_gw:      Farms, groundwater & food systems
├── eco:        Rivers, salmon & ecosystems
├── delta:      The Delta as a living place
├── climate:    Drought, climate risk, and resilience
└── governance: Operations and impacts

Seed: seed_tables/08_theme/theme.csv
```

### **2. theme_scenario_link (many-to-many relationship)**

```
Table: theme_scenario_link
├── theme_id             INTEGER NOT NULL           -- FK to theme.id
├── scenario_id          INTEGER NOT NULL           -- FK to scenario.id
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Primary key: (theme_id, scenario_id)

Foreign keys:
├── Ref: theme_scenario_link.theme_id > theme.id [delete: cascade, update: cascade]
└── Ref: theme_scenario_link.scenario_id > scenario.id [delete: cascade, update: cascade]

Indexes:
├── theme_scenario_link_pkey (theme_id, scenario_id) -- Primary key
└── idx_theme_scenario_reverse (scenario_id, theme_id) -- Reverse lookup

Seed: seed_tables/08_theme/theme_scenario_link.csv
```

### **3. theme_source_link (theme provenance)**

Status: PLANNED — not yet created in the database.

```
Table: theme_source_link   [PLANNED]
├── theme_id             INTEGER NOT NULL           -- FK to theme.id
├── source_id            INTEGER NOT NULL           -- FK to source.id
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Primary key: (theme_id, source_id)

Seed: seed_tables/08_theme/theme_source_link.csv
```

---

## **Layer 09+ — STATISTICS / RESULTS**

Pre-calculated statistics and outcome metrics derived from scenario model runs. Provides aggregated data for frontend visualization (percentile bands, time series summaries).

### **1. outcome_category (outcome types)**

Status: PLANNED — not yet created in the database.

```
Table: outcome_category   [PLANNED]
├── id                    SERIAL PRIMARY KEY
├── short_code            TEXT UNIQUE NOT NULL       -- "reservoir_storage", "groundwater_storage", etc.
├── label                 TEXT                       -- "Reservoir Storage", etc.
├── description           TEXT                       -- Detailed description
├── outcome_version_id    INTEGER NOT NULL           -- FK to version.id (statistics family, version_family_id=7)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK to developer.id (1 = system)
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Foreign keys:
├── Ref: outcome_category.outcome_version_id > version.id [delete: restrict, update: cascade]
├── Ref: outcome_category.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: outcome_category.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
└── outcome_category_short_code_key (short_code)

Values (10 total):
├── 1: community_water - Community Water Systems delivery performance
├── 2: agricultural_water - Agricultural water supply and economic outcomes
├── 3: environmental_water - River flows and ecosystem function indicators
├── 4: delta_outflow - Delta to San Francisco Bay flows
├── 5: delta_salinity - Salinity levels including X2 position
├── 6: delta_water_quality - In-Delta water quality for uses
├── 7: delta_exports - Water exported from Delta via pumping facilities
├── 8: reservoir_storage - Major Central Valley reservoir storage
├── 9: groundwater_storage - Central Valley aquifer storage
└── 10: salmon_population - Winter Run Chinook Salmon abundance
```

### **2. variable_prefix (CalSim variable naming convention)**

Status: PLANNED — not yet created in the database.

```
Table: variable_prefix   [PLANNED]
├── id                    SERIAL PRIMARY KEY
├── prefix                VARCHAR(10) UNIQUE NOT NULL -- "S", "C", "I", "E", "D", "A", "X", etc.
├── label                 VARCHAR NOT NULL           -- "Storage", "Channel Flow", "Inflow", etc.
├── description           TEXT                       -- What this variable type represents
├── unit_id               INTEGER                    -- FK to unit.id (default unit for prefix)
├── applies_to_entity     TEXT[]                     -- ["reservoir", "channel", "node", "demand_unit"]
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK to developer.id (system)
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Foreign keys:
├── Ref: variable_prefix.unit_id > unit.id [delete: restrict, update: cascade]
├── Ref: variable_prefix.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: variable_prefix.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── variable_prefix_pkey (id)
└── variable_prefix_prefix_key (prefix)

Values:
├── S: Storage (TAF) - applies to ["reservoir"]
├── C: Channel Flow (CFS) - applies to ["reservoir", "channel"]
├── I: Inflow (TAF) - applies to ["reservoir", "inflow"]
├── E: Evaporation (TAF) - applies to ["reservoir"]
├── D: Diversion (CFS) - applies to ["demand_unit", "node"]
├── A: Area (acres) - applies to ["reservoir"]
├── X: Transfer (CFS) - applies to ["demand_unit"]
└── DLT: Delivery (TAF/CFS) - applies to ["demand_unit"]

Usage: CalSim variable names follow pattern {prefix}_{entity_short_code}[_{suffix}]
Example: S_SHSTA = variable_prefix.prefix "S" + "_" + reservoir_entity.short_code "SHSTA"
```

### **3. outcome_statistic (statistics type per category)**

Status: PLANNED — not yet created in the database.

```
Table: outcome_statistic   [PLANNED]
├── id                    SERIAL PRIMARY KEY
├── outcome_category_id   INTEGER NOT NULL           -- FK to outcome_category.id
├── short_code            VARCHAR(50) NOT NULL       -- "monthly_percentile", "annual_exceedance", etc.
├── label                 VARCHAR NOT NULL           -- "Monthly Percentile Bands"
├── description           TEXT                       -- What this statistic measures
├── variable_prefix_id    INTEGER                    -- FK to variable_prefix.id (e.g., "S" for storage)
├── percentile_scheme     TEXT[]                     -- ['p0','p10','p30','p50','p70','p90','p100']
├── time_resolution       VARCHAR(20)                -- "monthly", "annual", "daily"
├── unit                  VARCHAR(50)                -- "percent_capacity", "taf", "cfs"
├── data_table            VARCHAR(100)               -- "reservoir_monthly_percentile" (target table)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK to developer.id (system)
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Foreign keys:
├── Ref: outcome_statistic.outcome_category_id > outcome_category.id [delete: restrict, update: cascade]
├── Ref: outcome_statistic.variable_prefix_id > variable_prefix.id [delete: restrict, update: cascade]
├── Ref: outcome_statistic.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: outcome_statistic.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── outcome_statistic_pkey (id)
├── uq_outcome_statistic (outcome_category_id, short_code)
├── idx_outcome_statistic_category (outcome_category_id)
└── idx_outcome_statistic_prefix (variable_prefix_id)

Constraints:
└── Unique: (outcome_category_id, short_code)

Values (initial):
├── outcome_category_id=8 (reservoir_storage):
│   └── short_code="monthly_percentile"
│       label="Monthly Percentile Bands"
│       variable_prefix_id to "S" (storage)
│       percentile_scheme=['p0','p10','p30','p50','p70','p90','p100']
│       time_resolution="monthly"
│       unit="percent_capacity"
│       data_table="reservoir_monthly_percentile"
```

### **4. reservoir_monthly_percentile (storage distribution by water month)**

```
Table: reservoir_monthly_percentile
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL       -- Scenario identifier (s0011, s0020, etc.)
├── reservoir_entity_id   INTEGER NOT NULL           -- FK reservoir_entity.id (SHSTA, OROVL, etc.)
├── water_month           INTEGER NOT NULL           -- 1-12 (Oct=1, Nov=2, ..., Sep=12)
│
├── -- Percentiles (% of reservoir capacity, using water management scheme)
├── q0                    NUMERIC(6,2)               -- min (0th percentile)
├── q10                   NUMERIC(6,2)               -- dry
├── q30                   NUMERIC(6,2)               -- below normal
├── q50                   NUMERIC(6,2)               -- median
├── q70                   NUMERIC(6,2)               -- above normal
├── q90                   NUMERIC(6,2)               -- wet
├── q100                  NUMERIC(6,2)               -- max (100th percentile)
├── mean_value            NUMERIC(6,2)               -- mean (% of capacity)
├── mean_taf              NUMERIC(10,2)              -- mean storage in TAF
├── capacity_taf          NUMERIC(10,2)              -- Denormalized capacity for convenience
│
├── -- Percentiles in absolute TAF values
├── q0_taf                NUMERIC(10,2)
├── q10_taf               NUMERIC(10,2)
├── q30_taf               NUMERIC(10,2)
├── q50_taf               NUMERIC(10,2)
├── q70_taf               NUMERIC(10,2)
├── q90_taf               NUMERIC(10,2)
├── q100_taf              NUMERIC(10,2)
│
├── -- Audit fields (ETL uses developer.id = 1 "system")
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK developer.id (system)
├── updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Note: scenario_short_code is a logical reference to scenario.scenario_id, not a strict FK.
This allows percentile data to be loaded independently for ETL flexibility.
Note: outcome_statistic_id was part of the original design (referencing the PLANNED
outcome_statistic table) but was not implemented in the current schema.

Foreign keys:
├── Ref: reservoir_monthly_percentile.reservoir_entity_id > reservoir_entity.id [delete: restrict, update: cascade]
├── Ref: reservoir_monthly_percentile.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: reservoir_monthly_percentile.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── reservoir_monthly_percentile_pkey (id)
├── uq_reservoir_percentile (scenario_short_code, reservoir_entity_id, water_month)
├── idx_reservoir_percentile_scenario (scenario_short_code)
├── idx_reservoir_percentile_reservoir (reservoir_entity_id)
├── idx_reservoir_percentile_combined (scenario_short_code, reservoir_entity_id)
└── idx_reservoir_percentile_active (is_active) WHERE is_active = TRUE

Constraints:
├── water_month CHECK (water_month BETWEEN 1 AND 12)
└── Unique: (scenario_short_code, reservoir_entity_id, water_month)

Records: 20,520 rows (92 reservoirs × 12 months × 8+ scenarios)

Source: CalSim scenario CSV from S3 (s3://coeqwal-model-run/scenario/{id}/csv/)
ETL: etl/statistics/calculate_reservoir_percentiles.py
```

### **5. reservoir_variable (CalSim variables linked to reservoirs)**

Status: PLANNED — not yet created in the database.

```
Table: reservoir_variable   [PLANNED]
├── id                    SERIAL PRIMARY KEY
├── calsim_id             TEXT NOT NULL              -- "S_SHSTA", "C_SHSTA", "C_SHSTA_FLOOD", etc.
├── name                  TEXT NOT NULL              -- "Shasta Storage", "Shasta Total Release", etc.
├── description           TEXT                       -- Detailed description
├── reservoir_entity_id   INTEGER                    -- FK to reservoir_entity.id (NULL for aggregates)
├── variable_type         TEXT NOT NULL              -- "storage", "storage_level", "release_total", "release_normal", "release_flood"
├── is_aggregate          BOOLEAN DEFAULT FALSE      -- TRUE for composite variables
├── aggregated_variable_ids INTEGER[]                -- IDs of component variables if aggregate
├── trigger_threshold     NUMERIC                    -- Threshold for alerts/triggers
├── unit_id               INTEGER NOT NULL           -- FK to unit.id (1=TAF, 2=CFS)
├── temporal_scale_id     INTEGER NOT NULL           -- FK to temporal_scale.id (3=monthly)
├── variable_version_id   INTEGER NOT NULL           -- FK to version.id (variable family)
├── variable_id           UUID UNIQUE NOT NULL       -- External system identifier
├── source_ids            INTEGER[]                  -- FK array to data_source.id
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK to developer.id
└── updated_by            INTEGER NOT NULL DEFAULT 1

Variable Types:
├── storage: S_{code} - Reservoir storage volume (TAF)
├── storage_level: S_{code}LEVEL* - Storage zone decision variables (TAF)
├── release_total: C_{code} - Total release from reservoir (CFS)
├── release_normal: C_{code}_NCF - Normal controlled release ≤ release capacity (CFS)
└── release_flood: C_{code}_FLOOD - Flood spill above release capacity (CFS)

CalSim Release Logic (from constraints-FloodSpill.wresl):
├── C_{code}_NCF + C_{code}_FLOOD = C_{code} (total release equation)
├── Normal release ≤ RelCap (release capacity, function of storage)
└── Flood spill is penalized heavily in optimization (-900000 weight)

Foreign keys:
├── Ref: reservoir_variable.reservoir_entity_id > reservoir_entity.id [delete: restrict, update: cascade]
├── Ref: reservoir_variable.unit_id > unit.id [delete: restrict, update: cascade]
├── Ref: reservoir_variable.temporal_scale_id > temporal_scale.id [delete: restrict, update: cascade]
├── Ref: reservoir_variable.variable_version_id > version.id [delete: restrict, update: cascade]
├── Ref: reservoir_variable.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: reservoir_variable.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── reservoir_variable_pkey (id)
├── idx_reservoir_variable_calsim_id (calsim_id)
├── idx_reservoir_variable_entity (reservoir_entity_id)
├── idx_reservoir_variable_type (variable_type)
└── idx_reservoir_variable_uuid (variable_id)

Expected Records: ~466 rows
├── storage: ~100 rows (92 base + variants like DELTA, EBMUD)
├── storage_level: ~90 rows (level decision variables)
├── release_total: 92 rows (one per reservoir)
├── release_normal: 92 rows (one per reservoir)
└── release_flood: 92 rows (one per reservoir)

Seed CSV: database/seed_tables/04_calsim_data/reservoir_variable.csv
```

### **6. reservoir_storage_monthly (monthly storage statistics)**

```
Table: reservoir_storage_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL       -- Scenario identifier (s0020, etc.)
├── reservoir_entity_id   INTEGER NOT NULL           -- FK to reservoir_entity.id
├── water_month           INTEGER NOT NULL           -- 1-12 (Oct=1, Sep=12)
│
├── -- Storage statistics (TAF)
├── storage_avg_taf       NUMERIC(10,2)              -- Mean storage
├── storage_cv            NUMERIC(6,4)               -- Coefficient of variation
├── storage_pct_capacity  NUMERIC(6,2)               -- Mean as % of capacity
│
├── -- Storage percentiles (% of capacity)
├── q0                    NUMERIC(6,2)               -- min (0th percentile)
├── q10                   NUMERIC(6,2)
├── q30                   NUMERIC(6,2)
├── q50                   NUMERIC(6,2)               -- median
├── q70                   NUMERIC(6,2)
├── q90                   NUMERIC(6,2)
├── q100                  NUMERIC(6,2)               -- max
│
├── -- Storage percentiles (TAF volume) - aligned with COEQWAL research notebooks
├── q0_taf                NUMERIC(10,2)              -- Minimum storage in TAF
├── q10_taf               NUMERIC(10,2)              -- 10th percentile in TAF
├── q30_taf               NUMERIC(10,2)              -- 30th percentile in TAF
├── q50_taf               NUMERIC(10,2)              -- Median storage in TAF
├── q70_taf               NUMERIC(10,2)              -- 70th percentile in TAF
├── q90_taf               NUMERIC(10,2)              -- 90th percentile in TAF
├── q100_taf              NUMERIC(10,2)              -- Maximum storage in TAF
│
├── -- Storage exceedance percentiles (% of capacity)
├── exc_p5                NUMERIC(6,2)               -- Exceeded 95% of time
├── exc_p10               NUMERIC(6,2)               -- Exceeded 90% of time
├── exc_p25               NUMERIC(6,2)               -- Exceeded 75% of time
├── exc_p50               NUMERIC(6,2)               -- Exceeded 50% of time (median)
├── exc_p75               NUMERIC(6,2)               -- Exceeded 25% of time
├── exc_p90               NUMERIC(6,2)               -- Exceeded 10% of time
├── exc_p95               NUMERIC(6,2)               -- Exceeded 5% of time
│
├── -- Storage exceedance percentiles (TAF volume)
├── exc_p5_taf            NUMERIC(10,2)
├── exc_p10_taf           NUMERIC(10,2)
├── exc_p25_taf           NUMERIC(10,2)
├── exc_p50_taf           NUMERIC(10,2)
├── exc_p75_taf           NUMERIC(10,2)
├── exc_p90_taf           NUMERIC(10,2)
├── exc_p95_taf           NUMERIC(10,2)
│
├── -- Metadata
├── capacity_taf          NUMERIC(10,2)              -- Denormalized for convenience
├── sample_count          INTEGER                    -- Number of months in sample
│
├── -- Audit fields (ERD standard)
├── is_active             BOOLEAN NOT NULL DEFAULT TRUE
├── created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK to developer.id
├── updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Note: scenario_short_code is a logical reference (not strict FK) for ETL flexibility.
Reservoir lookup via reservoir_entity join to get short_code, capacity, dead_pool.

Foreign keys:
├── Ref: reservoir_storage_monthly.reservoir_entity_id > reservoir_entity.id [delete: restrict, update: cascade]
├── Ref: reservoir_storage_monthly.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: reservoir_storage_monthly.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── reservoir_storage_monthly_pkey (id)
├── uq_storage_monthly (scenario_short_code, reservoir_entity_id, water_month)
├── idx_storage_monthly_scenario (scenario_short_code)
├── idx_storage_monthly_entity (reservoir_entity_id)
├── idx_storage_monthly_combined (scenario_short_code, reservoir_entity_id)
└── idx_storage_monthly_active (is_active) WHERE is_active = TRUE

Constraints:
├── water_month CHECK (water_month BETWEEN 1 AND 12)
└── Unique: (scenario_short_code, reservoir_entity_id, water_month)

Expected Records: 8,832 rows (92 reservoirs × 12 months × 8 scenarios)

DDL: database/scripts/sql/11_reservoir_statistics/04_create_reservoir_storage_monthly.sql
     database/scripts/sql/11_reservoir_statistics/09_add_taf_percentile_columns.sql (ALTER)
ETL: etl/statistics/calculate_reservoir_statistics.py

Note: TAF percentile columns (q0_taf through q100_taf) added to support COEQWAL research
notebook verification and provide absolute storage values alongside % of capacity.
```

### **7. reservoir_spill_monthly (monthly spill statistics)**

```
Table: reservoir_spill_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL       -- Scenario identifier
├── reservoir_entity_id   INTEGER NOT NULL           -- FK to reservoir_entity.id
├── water_month           INTEGER NOT NULL           -- 1-12 (Oct=1, Sep=12)
│
├── -- Spill frequency this month
├── spill_months_count    INTEGER                    -- Count of months with spill > 0
├── total_months          INTEGER                    -- Total months in sample
├── spill_frequency_pct   NUMERIC(5,2)               -- % of months with spill
│
├── -- Spill magnitude when spilling (CFS)
├── spill_avg_cfs         NUMERIC(10,2)              -- Mean spill when > 0
├── spill_max_cfs         NUMERIC(10,2)              -- Max spill this month
│
├── -- Spill exceedance percentiles (CFS) - of non-zero values
├── spill_q50             NUMERIC(10,2)              -- Median when spilling
├── spill_q90             NUMERIC(10,2)              -- 90th percentile
├── spill_q100            NUMERIC(10,2)              -- Max (same as spill_max_cfs)
│
├── -- Storage threshold for spill context
├── storage_at_spill_avg_pct NUMERIC(6,2)            -- Avg storage % when spilling
│
├── -- Audit fields (ERD standard)
├── is_active             BOOLEAN NOT NULL DEFAULT TRUE
├── created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1
├── updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Note: Spill data from C_{short_code}_FLOOD variable (flood release above release capacity).
From constraints-FloodSpill.wresl: C_{res}_NCF + C_{res}_Flood = C_{res}
ETL maps reservoir_entity.short_code to CalSim variable C_{short_code}_FLOOD

Foreign keys:
├── Ref: reservoir_spill_monthly.reservoir_entity_id > reservoir_entity.id [delete: restrict, update: cascade]
├── Ref: reservoir_spill_monthly.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: reservoir_spill_monthly.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── reservoir_spill_monthly_pkey (id)
├── uq_spill_monthly (scenario_short_code, reservoir_entity_id, water_month)
├── idx_spill_monthly_scenario (scenario_short_code)
├── idx_spill_monthly_entity (reservoir_entity_id)
├── idx_spill_monthly_combined (scenario_short_code, reservoir_entity_id)
├── idx_spill_monthly_frequency (spill_frequency_pct DESC)
└── idx_spill_monthly_active (is_active) WHERE is_active = TRUE

Constraints:
├── water_month CHECK (water_month BETWEEN 1 AND 12)
└── Unique: (scenario_short_code, reservoir_entity_id, water_month)

Expected Records: 8,832 rows (92 reservoirs × 12 months × 8 scenarios)

DDL: database/scripts/sql/11_reservoir_statistics/05_create_reservoir_spill_monthly.sql
ETL: etl/statistics/calculate_reservoir_statistics.py
```

### **8. reservoir_period_summary (period-of-record summary)**

```
Table: reservoir_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL       -- Scenario identifier
├── reservoir_entity_id   INTEGER NOT NULL           -- FK to reservoir_entity.id
│
├── -- Simulation period
├── simulation_start_year INTEGER NOT NULL           -- First water year
├── simulation_end_year   INTEGER NOT NULL           -- Last water year
├── total_years           INTEGER NOT NULL           -- Number of years
│
├── -- Storage exceedance (% capacity exceeded X% of time) - for full exceedance curves
├── storage_exc_p5        NUMERIC(6,2)               -- Exceeded 95% of time (5th percentile)
├── storage_exc_p10       NUMERIC(6,2)               -- Exceeded 90% of time
├── storage_exc_p25       NUMERIC(6,2)               -- Exceeded 75% of time
├── storage_exc_p50       NUMERIC(6,2)               -- Exceeded 50% of time (median)
├── storage_exc_p75       NUMERIC(6,2)               -- Exceeded 25% of time
├── storage_exc_p90       NUMERIC(6,2)               -- Exceeded 10% of time
├── storage_exc_p95       NUMERIC(6,2)               -- Exceeded 5% of time (95th percentile)
│
├── -- Threshold markers (for horizontal lines on charts)
├── dead_pool_taf         NUMERIC(10,2)              -- Dead pool volume (from reservoir_entity)
├── dead_pool_pct         NUMERIC(6,2)               -- Dead pool as % of capacity
├── spill_threshold_pct   NUMERIC(6,2)               -- Avg storage % when spill begins
│
├── -- Annual spill frequency
├── spill_years_count     INTEGER                    -- Years with any spill
├── spill_frequency_pct   NUMERIC(5,2)               -- % of years with spill
│
├── -- Spill magnitude summary (CFS)
├── spill_mean_cfs        NUMERIC(10,2)              -- Mean when spilling (all events)
├── spill_peak_cfs        NUMERIC(10,2)              -- Maximum ever observed
│
├── -- Annual spill volume (TAF)
├── annual_spill_avg_taf  NUMERIC(10,2)              -- Mean annual volume
├── annual_spill_cv       NUMERIC(6,4)               -- CV of annual volume
├── annual_spill_max_taf  NUMERIC(10,2)              -- Max annual volume
│
├── -- Annual max spill distribution (worst event each year)
├── annual_max_spill_q50  NUMERIC(10,2)              -- Median of annual peaks
├── annual_max_spill_q90  NUMERIC(10,2)              -- 90th percentile of annual peaks
├── annual_max_spill_q100 NUMERIC(10,2)              -- Max (same as spill_peak_cfs)
│
├── -- Probability metrics (aligned with COEQWAL research notebooks)
├── -- Flood pool: P(storage >= flood control level)
├── flood_pool_prob_all       NUMERIC(6,4)           -- Flood probability, all months (0-1)
├── flood_pool_prob_september NUMERIC(6,4)           -- Flood probability, September
├── flood_pool_prob_april     NUMERIC(6,4)           -- Flood probability, April
│
├── -- Dead pool: P(storage <= dead pool level)
├── dead_pool_prob_all        NUMERIC(6,4)           -- Dead pool probability, all months (0-1)
├── dead_pool_prob_september  NUMERIC(6,4)           -- Dead pool probability, September
│
├── -- Coefficient of variation: CV = std / mean
├── storage_cv_all            NUMERIC(6,4)           -- CV of storage, all months
├── storage_cv_april          NUMERIC(6,4)           -- CV of storage, April
├── storage_cv_september      NUMERIC(6,4)           -- CV of storage, September
│
├── -- Average storage (aligned with notebook metrics)
├── annual_avg_taf            NUMERIC(10,2)          -- Mean of annual mean storage
├── april_avg_taf             NUMERIC(10,2)          -- Mean April storage
├── september_avg_taf         NUMERIC(10,2)          -- Mean September storage
│
├── -- Metadata
├── capacity_taf          NUMERIC(10,2)              -- Denormalized for convenience
│
├── -- Audit fields (ERD standard)
├── is_active             BOOLEAN NOT NULL DEFAULT TRUE
├── created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1
├── updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Note: ETL maps reservoir_entity.short_code to CalSim variables:
├── Storage: S_{short_code} (e.g., S_SHSTA)
└── Spill: C_{short_code}_FLOOD (e.g., C_SHSTA_FLOOD)

Storage Exceedance Interpretation:
├── storage_exc_p10 = 60% means "90% of the time, storage ≥ 60% of capacity"
├── storage_exc_p50 = 75% means "50% of the time, storage ≥ 75% of capacity"
└── storage_exc_p90 = 95% means "10% of the time, storage ≥ 95% of capacity"

Threshold Markers for Charts:
├── dead_pool_pct: horizontal line at bottom (physical minimum)
├── spill_threshold_pct: horizontal line near top (where spill typically begins)
└── Example chart with thresholds:
    100% ─┬────────────────── Capacity
          │    ╱╲
      90% │   ╱  ╲   ← spill_threshold_pct
          │  ╱    ╲
      50% │ ╱      ╲  ← Percentile bands
          │╱        ╲
      10% ├──────────── dead_pool_pct
          │
       0% ─┴──────────────────

Use Cases:
├── Spill risk assessment: spill_frequency_pct shows probability of annual spill
├── Infrastructure planning: annual_max_spill_q90 indicates 90th percentile worst case
├── Climate comparison: compare spill patterns across scenarios
├── Volume impacts: annual_spill_avg_taf quantifies water "lost" to spill
├── Exceedance curves: storage_exc_* enables full period storage duration curves
└── Chart thresholds: dead_pool_pct and spill_threshold_pct for visual markers

Probability Metrics (COEQWAL Research Notebooks Alignment):
├── Source: coeqwal/notebooks/coeqwalpackage/metrics.py
├── Verified against: all_metrics_output.csv (Metrics.ipynb output)
│
├── Flood Pool Probability:
│   ├── Formula: P = count(storage >= flood_control_level) / total_count
│   ├── Threshold source: S_{res}LEVEL5DV variable (dynamic) or constant
│   ├── Example SHSTA flood_pool_prob_all: 0.3117 (31.17% of months at flood control)
│   └── Reservoirs with variable thresholds: SHSTA, OROVL, TRNTY, FOLSM, MELON, SLUIS_CVP, SLUIS_SWP
│
├── Dead Pool Probability:
│   ├── Formula: P = count(storage <= dead_pool_level) / total_count
│   ├── Threshold source: S_{res}LEVEL1DV variable (dynamic) or constant (reservoir_entity.dead_pool_taf)
│   ├── Example: Most reservoirs have 0.0000 dead pool probability under normal conditions
│   └── Key drought indicator: increasing dead_pool_prob signals storage stress
│
├── Coefficient of Variation (CV):
│   ├── Formula: CV = standard_deviation / mean
│   ├── Higher CV = more variability = less predictable operations
│   └── Separate CV for all months, April (spring), September (end of dry season)
│
└── Metric Naming (matching notebook output):
    ├── All_Prob_S_{RES}_flood to flood_pool_prob_all
    ├── Sep_Prob_S_{RES}_flood to flood_pool_prob_september
    ├── All_Prob_S_{RES}_dead to dead_pool_prob_all
    ├── Sep_Avg_S_{RES}_TAF to september_avg_taf
    └── Sep_S_{RES}_CV to storage_cv_september

Reservoir Thresholds Reference:
├── SHSTA: floodVar=S_SHSTALEVEL5DV, deadVar=S_SHSTALEVEL1DV
├── OROVL: floodVar=S_OROVLLEVEL5DV, deadVar=S_OROVLLEVEL1DV
├── TRNTY: floodVar=S_TRNTYLEVEL5DV, deadVar=S_TRNTYLEVEL1DV
├── FOLSM: floodVar=S_FOLSMLEVEL5DV, deadVar=S_FOLSMLEVEL1DV
├── MELON: floodVar=S_MELONLEVEL4DV, deadPool=80 TAF (constant)
├── MLRTN: floodPool=524 TAF (constant), deadPool=135 TAF (constant)
├── SLUIS_CVP: floodVar=S_SLUIS_CVPLEVEL5DV, deadVar=S_SLUIS_CVPLEVEL1DV
└── SLUIS_SWP: floodVar=S_SLUIS_SWPLEVEL5DV, deadVar=S_SLUIS_SWPLEVEL1DV

Foreign keys:
├── Ref: reservoir_period_summary.reservoir_entity_id > reservoir_entity.id [delete: restrict, update: cascade]
├── Ref: reservoir_period_summary.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: reservoir_period_summary.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── reservoir_period_summary_pkey (id)
├── uq_period_summary (scenario_short_code, reservoir_entity_id)
├── idx_period_summary_scenario (scenario_short_code)
├── idx_period_summary_entity (reservoir_entity_id)
├── idx_period_summary_spill_freq (spill_frequency_pct DESC)
├── idx_period_summary_flood_prob (flood_pool_prob_all DESC) -- For flood risk queries
├── idx_period_summary_dead_prob (dead_pool_prob_all DESC) -- For drought risk queries
├── idx_period_summary_cv (storage_cv_all DESC) -- For variability queries
└── idx_period_summary_active (is_active) WHERE is_active = TRUE

Constraints:
└── Unique: (scenario_short_code, reservoir_entity_id)

Expected Records: 736 rows (92 reservoirs × 8 scenarios)

DDL: database/scripts/sql/11_reservoir_statistics/06_create_reservoir_period_summary.sql
     database/scripts/sql/11_reservoir_statistics/08_add_probability_metrics_to_period_summary.sql (ALTER)
ETL: etl/statistics/calculate_reservoir_statistics.py
```

---

### **M&I (Municipal & Industrial) Statistics**

The M&I statistics layer provides delivery and shortage statistics at two levels:
1. **Urban Demand Units (du_urban_entity)** - 126 geographic demand units
2. **M&I Contractors (mi_contractor)** - 30 SWP water agency contractors

#### **du_urban_group (demand unit groupings)**
```
Table: du_urban_group
├── id                    INTEGER PRIMARY KEY
├── short_code            VARCHAR(50) UNIQUE NOT NULL  -- "tier", "var_wba", etc.
├── label                 VARCHAR(100) NOT NULL        -- "Tier Matrix DUs"
├── description           TEXT                         -- Purpose of this grouping
├── display_order         INTEGER DEFAULT 0
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1            -- FK to developer.id
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Records: 11 groups in two categories:

Analytical/Geographic Groups (IDs 1-6):
├── tier                  Tier matrix DUs (71 members)
├── nod                   North of Delta
├── sod                   South of Delta
├── swp_served            Receives SWP water
├── cvp_served            Receives CVP water
└── swp_delivery_point    Has SWP delivery point

Variable Extraction Category Groups (IDs 7-11):
├── var_wba               WBA-style DL_* delivery (40 members)
├── var_gw_only           Groundwater only, no surface (3 members)
├── var_swp_contractor    SWP contractor D_*_PMI (11 members)
├── var_named_locality    Named locality D_* arcs (15 members)
└── var_missing           No CalSim variables found (2 members)

DDL: database/scripts/sql/12_mi_statistics/02b_create_du_urban_group_tables.sql
Seed (analytical): database/scripts/sql/12_mi_statistics/02c_load_du_urban_group_from_s3.sql
Seed (variable): database/scripts/sql/12_mi_statistics/02d_load_du_variable_groups.sql
```

#### **du_urban_group_member (demand unit group memberships)**
```
Table: du_urban_group_member
├── id                    SERIAL PRIMARY KEY
├── du_urban_group_id     INTEGER NOT NULL             -- FK to du_urban_group.id
├── du_id                 VARCHAR(20) NOT NULL         -- FK to du_urban_entity.du_id
├── display_order         INTEGER DEFAULT 0
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
└── Unique: (du_urban_group_id, du_id)

Records: ~142 rows (71 tier + 71 variable category memberships)

Note: A demand unit can belong to multiple groups. For example, 02_PU is in:
├── tier (analytical group)
└── var_wba (variable extraction category)

DDL: database/scripts/sql/12_mi_statistics/02b_create_du_urban_group_tables.sql
```

#### **mi_contractor_group (M&I contractor groupings)**
```
Table: mi_contractor_group
├── id                    INTEGER PRIMARY KEY
├── short_code            VARCHAR(50) UNIQUE NOT NULL  -- "swp", "cvp_nod", "cvp_sod", "all_mi"
├── label                 VARCHAR(100) NOT NULL
├── description           TEXT
├── display_order         INTEGER DEFAULT 0
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Records: 6 groups (swp, cvp_nod, cvp_sod, all_mi, swp_mi, swp_ag)

DDL: database/scripts/sql/12_mi_statistics/03_create_mi_contractor_entity_tables.sql
```

#### **mi_contractor (SWP/CVP water agency contractors)**
```
Table: mi_contractor
├── id                    SERIAL PRIMARY KEY
├── short_code            VARCHAR(50) UNIQUE NOT NULL  -- "ACWD", "MWD", "YUBA"
├── contractor_name       VARCHAR(100) NOT NULL        -- "ALAMEDA COUNTY WD"
├── project               VARCHAR(10) NOT NULL         -- "SWP" or "CVP"
├── region                VARCHAR(10)                  -- "NOD" or "SOD"
├── contractor_type       VARCHAR(10) NOT NULL         -- "MI", "MWD", "AG"
├── contract_amount_taf   NUMERIC(10,2)                -- Table A contract amount
├── source_contractor_id  INTEGER                      -- Original ID from wresl files
├── source_file           VARCHAR(100)                 -- "swp_contractor_perdel_A.wresl"
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Records: 30 SWP contractors (22 MI, 1 MWD, 7 AG)

Source: /data/raw/model_run/.../DeliveryLogic/SWP/Allocation/swp_contractor_perdel_A.wresl

DDL: database/scripts/sql/12_mi_statistics/03_create_mi_contractor_entity_tables.sql
```

#### **mi_contractor_group_member (contractor group memberships)**
```
Table: mi_contractor_group_member
├── id                    SERIAL PRIMARY KEY
├── mi_contractor_group_id INTEGER NOT NULL            -- FK to mi_contractor_group.id
├── mi_contractor_id      INTEGER NOT NULL             -- FK to mi_contractor.id
├── display_order         INTEGER DEFAULT 0
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
└── Unique: (mi_contractor_group_id, mi_contractor_id)

Records: 60 memberships

DDL: database/scripts/sql/12_mi_statistics/03_create_mi_contractor_entity_tables.sql
```

#### **mi_contractor_delivery_arc (delivery variable mappings)**
```
Table: mi_contractor_delivery_arc
├── id                    SERIAL PRIMARY KEY
├── mi_contractor_id      INTEGER NOT NULL             -- FK to mi_contractor.id
├── delivery_arc          VARCHAR(50) NOT NULL         -- "D_SBA029_ACWD", "D_PRRIS_MWDSC"
├── arc_type              VARCHAR(20)                  -- "PMI", "PAG"
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
└── Unique: (delivery_arc)

Records: 39 delivery arcs

DDL: database/scripts/sql/12_mi_statistics/03_create_mi_contractor_entity_tables.sql
```

#### **du_urban_variable (demand unit variable mappings)**
```
Table: du_urban_variable
├── id                    SERIAL PRIMARY KEY
├── du_id                 VARCHAR(20) NOT NULL         -- FK du_urban_entity.du_id
├── delivery_variable     VARCHAR(100) NOT NULL        -- CalSim variable (DL_*, D_*, GP_*)
├── demand_variable       VARCHAR(100)                 -- CalSim variable for demand
├── shortage_variable     VARCHAR(100)                 -- CalSim variable (SHRTG_*, GW_SHORT_*)
├── variable_type         VARCHAR(20) DEFAULT 'delivery' -- Type of water supply measurement
├── variable_type_id      INTEGER                      -- FK variable_type.id
├── requires_sum          BOOLEAN DEFAULT FALSE        -- TRUE if multiple arcs need summing
├── demand_mode           VARCHAR                      -- How demand is determined (e.g. "static", "dynamic")
├── demand_params         JSONB                        -- Additional demand calculation parameters
├── notes                 TEXT                         -- Mapping context
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
├── FK: du_id to du_urban_entity.du_id
└── Unique: (du_id)

Records: 71 mappings (canonical CWS demand units from tier matrix)

Variable types (type of water supply measurement):
├── delivery: Surface water delivery (68 units)
├── gw_pumping: Groundwater pumping, no surface delivery (3 units: 71_NU, 72_NU, 72_PU)
├── diversion: Water diversion (future use)
└── unknown: No CalSim variable found (2 units: JLIND, UPANG)

Note: The extraction category (how to find the CalSim variable) is determined by
group membership in du_urban_group (var_wba, var_swp_contractor, etc.).

DDL: database/scripts/sql/12_mi_statistics/01c_create_du_urban_variable.sql
Seed: database/scripts/sql/12_mi_statistics/01d_load_du_urban_variable.sql
```

#### **du_urban_delivery_arc (multi-arc delivery mappings)**
```
Table: du_urban_delivery_arc
├── id                    SERIAL PRIMARY KEY
├── du_id                 VARCHAR(20) NOT NULL         -- FK to du_urban_entity.du_id
├── delivery_arc          VARCHAR(100) NOT NULL        -- CalSim arc variable (D_*)
├── arc_order             INTEGER DEFAULT 1            -- Order for summing
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
├── FK: du_id to du_urban_entity.du_id
└── Unique: (du_id, delivery_arc)

Records: 10 arcs for 5 multi-arc units (AMADR, AMCYN, ANTOC, FRFLD, GRSVL)

Purpose: For demand units requiring sum of multiple delivery arcs.
Example: FRFLD = D_WTPNBR_FRFLD + D_WTPWMN_FRFLD

DDL: database/scripts/sql/12_mi_statistics/01c_create_du_urban_variable.sql
Seed: database/scripts/sql/12_mi_statistics/01d_load_du_urban_variable.sql
```

#### **du_delivery_monthly (urban demand unit delivery statistics)**
```
Table: du_delivery_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL         -- FK to du_urban_entity.du_id
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── delivery_avg_taf      NUMERIC(10,2)
├── delivery_cv           NUMERIC(6,4)
├── q0                    NUMERIC(10,2)                -- Percentiles for box plots
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── demand_avg_taf        NUMERIC(10,2)                -- Average monthly demand
├── percent_of_demand_avg NUMERIC(5,2)                 -- Average percent of demand met
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
├── Unique: (scenario_short_code, du_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/12_mi_statistics/02_create_du_statistics_tables.sql
```

#### **du_shortage_monthly (urban demand unit shortage statistics)**
```
Table: du_shortage_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL
├── water_month           INTEGER NOT NULL
├── shortage_avg_taf      NUMERIC(10,2)
├── shortage_cv           NUMERIC(6,4)
├── shortage_frequency_pct NUMERIC(5,2)                -- % months with shortage > 0
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
├── Unique: (scenario_short_code, du_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/12_mi_statistics/02_create_du_statistics_tables.sql
```

#### **du_period_summary (urban demand unit period summary)**
```
Table: du_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL
├── simulation_start_year INTEGER NOT NULL
├── simulation_end_year   INTEGER NOT NULL
├── total_years           INTEGER NOT NULL
├── annual_delivery_avg_taf NUMERIC(10,2)
├── annual_delivery_cv    NUMERIC(6,4)
├── delivery_exc_p5       NUMERIC(10,2)                -- Exceedance percentiles
├── delivery_exc_p10      NUMERIC(10,2)
├── delivery_exc_p25      NUMERIC(10,2)
├── delivery_exc_p50      NUMERIC(10,2)
├── delivery_exc_p75      NUMERIC(10,2)
├── delivery_exc_p90      NUMERIC(10,2)
├── delivery_exc_p95      NUMERIC(10,2)
├── annual_shortage_avg_taf NUMERIC(10,2)
├── shortage_years_count  INTEGER
├── shortage_frequency_pct NUMERIC(5,2)
├── shortage_exc_p5       NUMERIC(10,2)                -- Shortage exceedance percentiles
├── shortage_exc_p10      NUMERIC(10,2)
├── shortage_exc_p25      NUMERIC(10,2)
├── shortage_exc_p50      NUMERIC(10,2)
├── shortage_exc_p75      NUMERIC(10,2)
├── shortage_exc_p90      NUMERIC(10,2)
├── shortage_exc_p95      NUMERIC(10,2)
├── reliability_pct       NUMERIC(5,2)                 -- % months meeting full demand
├── avg_pct_demand_met    NUMERIC(5,2)
├── annual_demand_avg_taf NUMERIC(10,2)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Records: 874 rows

Constraints:
└── Unique: (scenario_short_code, du_id)

DDL: database/scripts/sql/12_mi_statistics/02_create_du_statistics_tables.sql
```

#### **mi_delivery_monthly (contractor delivery statistics)**
```
Table: mi_delivery_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── mi_contractor_code    VARCHAR(50) NOT NULL         -- FK mi_contractor.short_code
├── water_month           INTEGER NOT NULL
├── delivery_avg_taf      NUMERIC(10,2)
├── delivery_cv           NUMERIC(6,4)
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── demand_avg_taf        NUMERIC(10,2)                -- Average monthly demand
├── percent_of_demand_avg NUMERIC(5,2)                 -- Avg percent of demand met
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Records: 3,588 rows (30 contractors × 12 months × ~10 scenarios)

Note: mi_contractor_code contains BOTH individual contractor codes (referencing mi_contractor.short_code)
AND aggregate rollup codes (CVP_PMI_N, CVP_PMI_S, KERN, SWP_PMI_N, SWP_PMI_S, SWP_PMI_TOTAL).
A strict FK constraint cannot be enforced on this column as-is.

Constraints:
├── Unique: (scenario_short_code, mi_contractor_code, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/12_mi_statistics/05_create_mi_statistics_tables.sql
```

#### **mi_shortage_monthly (contractor shortage statistics)**
```
Table: mi_shortage_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── mi_contractor_code    VARCHAR(50) NOT NULL         -- FK mi_contractor.short_code
├── water_month           INTEGER NOT NULL
├── shortage_avg_taf      NUMERIC(10,2)
├── shortage_cv           NUMERIC(6,4)
├── shortage_frequency_pct NUMERIC(5,2)
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Records: 3,588 rows

Note: mi_contractor_code contains both individual and aggregate rollup codes — see mi_delivery_monthly note.
FK constraint not enforced. See mi_delivery_monthly for full explanation.

Constraints:
├── Unique: (scenario_short_code, mi_contractor_code, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/12_mi_statistics/05_create_mi_statistics_tables.sql
```

#### **mi_contractor_period_summary (contractor period summary)**
```
Table: mi_contractor_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── mi_contractor_code    VARCHAR(50) NOT NULL         -- FK mi_contractor.short_code
├── simulation_start_year INTEGER NOT NULL
├── simulation_end_year   INTEGER NOT NULL
├── total_years           INTEGER NOT NULL
├── annual_delivery_avg_taf NUMERIC(10,2)
├── annual_delivery_cv    NUMERIC(6,4)
├── delivery_exc_p5       NUMERIC(10,2)                -- Exceedance percentiles
├── delivery_exc_p10      NUMERIC(10,2)
├── delivery_exc_p25      NUMERIC(10,2)
├── delivery_exc_p50      NUMERIC(10,2)
├── delivery_exc_p75      NUMERIC(10,2)
├── delivery_exc_p90      NUMERIC(10,2)
├── delivery_exc_p95      NUMERIC(10,2)
├── annual_shortage_avg_taf NUMERIC(10,2)
├── shortage_years_count  INTEGER
├── shortage_frequency_pct NUMERIC(5,2)
├── shortage_exc_p5       NUMERIC(10,2)
├── shortage_exc_p10      NUMERIC(10,2)
├── shortage_exc_p25      NUMERIC(10,2)
├── shortage_exc_p50      NUMERIC(10,2)
├── shortage_exc_p75      NUMERIC(10,2)
├── shortage_exc_p90      NUMERIC(10,2)
├── shortage_exc_p95      NUMERIC(10,2)
├── reliability_pct       NUMERIC(5,2)
├── avg_pct_demand_met    NUMERIC(5,2)
├── contract_amount_taf   NUMERIC(10,2)                -- Table A amount
├── annual_demand_avg_taf NUMERIC(10,2)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Records: 299 rows

Note: mi_contractor_code contains both individual and aggregate rollup codes — FK constraint not enforced.
See mi_delivery_monthly for full explanation.

Constraints:
└── Unique: (scenario_short_code, mi_contractor_code)

DDL: database/scripts/sql/12_mi_statistics/05_create_mi_statistics_tables.sql
```

### **CWS (Community Water Systems) Aggregate Statistics**

System-level aggregate statistics for SWP, CVP, and MWD deliveries using pre-calculated CalSim variables.

#### **cws_aggregate_entity (aggregate definitions)**
```
Table: cws_aggregate_entity
├── id                    INTEGER PRIMARY KEY
├── short_code            VARCHAR(50) UNIQUE NOT NULL  -- "swp_total", "cvp_nod", "cvp_sod", "mwd"
├── label                 VARCHAR(100) NOT NULL        -- "SWP Total M&I"
├── description           TEXT
├── project               VARCHAR(10)                  -- "SWP", "CVP", "MWD"
├── region                VARCHAR(10)                  -- "total", "nod", "sod", NULL
├── delivery_variable     VARCHAR(50) NOT NULL         -- CalSim variable (DEL_SWP_PMI)
├── shortage_variable     VARCHAR(50)                  -- CalSim variable (SHORT_SWP_PMI), NULL for MWD
├── display_order         INTEGER DEFAULT 0
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1            -- FK to developer.id
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Records: 6 aggregates (swp_total, swp_nod, swp_sod, cvp_nod, cvp_sod, mwd)

Indexes:
├── cws_aggregate_entity_pkey (id)
├── cws_aggregate_entity_short_code_key (short_code) UNIQUE
└── idx_cws_aggregate_entity_project (project)

DDL: database/scripts/sql/12_mi_statistics/06_create_cws_aggregate_tables.sql
```

#### **cws_aggregate_monthly (monthly delivery/shortage statistics)**
```
Table: cws_aggregate_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── cws_aggregate_id      INTEGER NOT NULL             -- FK to cws_aggregate_entity.id
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── delivery_avg_taf      NUMERIC(10,2)
├── delivery_cv           NUMERIC(6,4)
├── delivery_q0           NUMERIC(10,2)                -- Percentiles for box plots
├── delivery_q10          NUMERIC(10,2)
├── delivery_q30          NUMERIC(10,2)
├── delivery_q50          NUMERIC(10,2)
├── delivery_q70          NUMERIC(10,2)
├── delivery_q90          NUMERIC(10,2)
├── delivery_q100         NUMERIC(10,2)
├── shortage_avg_taf      NUMERIC(10,2)
├── shortage_cv           NUMERIC(6,4)
├── shortage_frequency_pct NUMERIC(5,2)                -- % months with shortage > 0
├── shortage_q0           NUMERIC(10,2)
├── shortage_q10          NUMERIC(10,2)
├── shortage_q30          NUMERIC(10,2)
├── shortage_q50          NUMERIC(10,2)
├── shortage_q70          NUMERIC(10,2)
├── shortage_q90          NUMERIC(10,2)
├── shortage_q100         NUMERIC(10,2)
├── shortage_exc_p5       NUMERIC(10,2)                -- Shortage exceedance percentiles
├── shortage_exc_p10      NUMERIC(10,2)
├── shortage_exc_p25      NUMERIC(10,2)
├── shortage_exc_p50      NUMERIC(10,2)
├── shortage_exc_p75      NUMERIC(10,2)
├── shortage_exc_p90      NUMERIC(10,2)
├── shortage_exc_p95      NUMERIC(10,2)
├── delivery_exc_p5       NUMERIC(10,2)                -- Delivery exceedance percentiles
├── delivery_exc_p10      NUMERIC(10,2)
├── delivery_exc_p25      NUMERIC(10,2)
├── delivery_exc_p50      NUMERIC(10,2)
├── delivery_exc_p75      NUMERIC(10,2)
├── delivery_exc_p90      NUMERIC(10,2)
├── delivery_exc_p95      NUMERIC(10,2)
├── demand_avg_taf        NUMERIC(12,2)                -- Average monthly demand
├── percent_of_demand_avg NUMERIC(5,2)                 -- Average percent of demand met
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
├── FK: cws_aggregate_id to cws_aggregate_entity.id
├── Unique: (scenario_short_code, cws_aggregate_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

Indexes:
├── idx_cws_agg_monthly_scenario (scenario_short_code)
├── idx_cws_agg_monthly_aggregate (cws_aggregate_id)
└── idx_cws_agg_monthly_combined (scenario_short_code, cws_aggregate_id)

Expected Records: 72 rows per scenario (6 aggregates × 12 months)

DDL: database/scripts/sql/12_mi_statistics/06_create_cws_aggregate_tables.sql
ETL: etl/statistics/cws_aggregate/calculate_cws_aggregate_statistics.py
```

#### **cws_aggregate_period_summary (period-of-record summary)**
```
Table: cws_aggregate_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── cws_aggregate_id      INTEGER NOT NULL             -- FK to cws_aggregate_entity.id
├── simulation_start_year INTEGER NOT NULL
├── simulation_end_year   INTEGER NOT NULL
├── total_years           INTEGER NOT NULL
├── annual_delivery_avg_taf NUMERIC(10,2)
├── annual_delivery_cv    NUMERIC(6,4)
├── annual_delivery_min_taf NUMERIC(10,2)              -- Minimum annual delivery
├── annual_delivery_max_taf NUMERIC(10,2)              -- Maximum annual delivery
├── delivery_exc_p5       NUMERIC(10,2)                -- Exceedance percentiles
├── delivery_exc_p10      NUMERIC(10,2)
├── delivery_exc_p25      NUMERIC(10,2)
├── delivery_exc_p50      NUMERIC(10,2)
├── delivery_exc_p75      NUMERIC(10,2)
├── delivery_exc_p90      NUMERIC(10,2)
├── delivery_exc_p95      NUMERIC(10,2)
├── annual_shortage_avg_taf NUMERIC(10,2)
├── shortage_years_count  INTEGER
├── shortage_frequency_pct NUMERIC(5,2)                -- % of years with any shortage
├── shortage_exc_p5       NUMERIC(10,2)
├── shortage_exc_p10      NUMERIC(10,2)
├── shortage_exc_p25      NUMERIC(10,2)
├── shortage_exc_p50      NUMERIC(10,2)
├── shortage_exc_p75      NUMERIC(10,2)
├── shortage_exc_p90      NUMERIC(10,2)
├── shortage_exc_p95      NUMERIC(10,2)
├── reliability_pct       NUMERIC(5,2)                 -- % months meeting full demand
├── avg_pct_allocation_met NUMERIC(5,2)                -- avg delivery/allocation across period
├── annual_demand_avg_taf NUMERIC(12,2)                -- Average annual demand
├── avg_pct_demand_met    NUMERIC(5,2)                 -- Average percent of demand met
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER DEFAULT 1

Constraints:
├── FK: cws_aggregate_id to cws_aggregate_entity.id
└── Unique: (scenario_short_code, cws_aggregate_id)

Indexes:
├── idx_cws_agg_period_scenario (scenario_short_code)
└── idx_cws_agg_period_aggregate (cws_aggregate_id)

Expected Records: 6 rows per scenario (6 aggregates)

DDL: database/scripts/sql/12_mi_statistics/06_create_cws_aggregate_tables.sql
ETL: etl/statistics/cws_aggregate/calculate_cws_aggregate_statistics.py
```

---

### **Agriculture (AG) Statistics**

Pre-calculated delivery and shortage statistics for agricultural demand units and aggregate water balance areas.
Mirrors the M&I/Urban DU layer in structure.

#### **ag_aggregate_entity (agriculture aggregate definitions)**
```
Table: ag_aggregate_entity
├── id                    SERIAL PRIMARY KEY
├── short_code            VARCHAR(50) UNIQUE NOT NULL  -- Aggregate identifier
├── label                 VARCHAR(100) NOT NULL
├── project               VARCHAR(10)                  -- "CVP", "SWP", etc.
├── region                VARCHAR(10)                  -- "NOD", "SOD", etc.
├── delivery_variable     VARCHAR(100) NOT NULL         -- CalSim variable for delivery
├── description           TEXT
├── display_order         INTEGER DEFAULT 0
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1   -- FK developer.id
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Records: 5 aggregates
```

#### **ag_aggregate_monthly (agriculture aggregate monthly statistics)**
```
Table: ag_aggregate_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL         -- Scenario identifier
├── aggregate_code        VARCHAR(50) NOT NULL         -- FK ag_aggregate_entity.short_code
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── delivery_avg_taf      NUMERIC(10,2)
├── delivery_cv           NUMERIC(6,4)
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── shortage_avg_taf      NUMERIC(10,2)
├── shortage_cv           NUMERIC(6,4)
├── shortage_frequency_pct NUMERIC(5,2)
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Records: 480 rows (5 aggregates × 12 months × 8 scenarios)

Constraints:
├── Unique: (scenario_short_code, aggregate_code, water_month)
└── Check: water_month BETWEEN 1 AND 12
```

#### **ag_aggregate_period_summary (agriculture aggregate period summary)**
```
Table: ag_aggregate_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── aggregate_code        VARCHAR(50) NOT NULL         -- FK ag_aggregate_entity.short_code
├── simulation_start_year INTEGER NOT NULL
├── simulation_end_year   INTEGER NOT NULL
├── total_years           INTEGER NOT NULL
├── annual_delivery_avg_taf NUMERIC(10,2)
├── annual_delivery_cv    NUMERIC(6,4)
├── delivery_exc_p5       NUMERIC(10,2)                -- Exceedance percentiles
├── delivery_exc_p10      NUMERIC(10,2)
├── delivery_exc_p25      NUMERIC(10,2)
├── delivery_exc_p50      NUMERIC(10,2)
├── delivery_exc_p75      NUMERIC(10,2)
├── delivery_exc_p90      NUMERIC(10,2)
├── delivery_exc_p95      NUMERIC(10,2)
├── annual_shortage_avg_taf NUMERIC(10,2)
├── shortage_years_count  INTEGER
├── shortage_frequency_pct NUMERIC(5,2)
├── shortage_exc_p5       NUMERIC(10,2)
├── shortage_exc_p10      NUMERIC(10,2)
├── shortage_exc_p25      NUMERIC(10,2)
├── shortage_exc_p50      NUMERIC(10,2)
├── shortage_exc_p75      NUMERIC(10,2)
├── shortage_exc_p90      NUMERIC(10,2)
├── shortage_exc_p95      NUMERIC(10,2)
├── reliability_pct       NUMERIC(5,2)                -- % months meeting full demand
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Records: 40 rows (5 aggregates × 8 scenarios)

Constraints:
└── Unique: (scenario_short_code, aggregate_code)
```

#### **ag_du_demand_monthly (agriculture demand unit demand statistics)**

Note: Originally named `ag_du_delivery_monthly`, renamed in migration 04 when delivery
was split into separate surface water and groundwater tables.

```
Table: ag_du_demand_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL         -- FK du_agriculture_entity.du_id
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── demand_avg_taf        NUMERIC(10,2)
├── demand_cv             NUMERIC(6,4)
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 25,200 rows

Constraints:
├── Unique: (scenario_short_code, du_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/13_ag_statistics/02_create_ag_statistics_tables.sql (original)
     database/scripts/sql/migrations/04_add_sw_delivery_gw_pumping_tables.sql (rename)
```

#### **ag_du_sw_delivery_monthly (agriculture demand unit surface water delivery statistics)**
```
Table: ag_du_sw_delivery_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL         -- FK du_agriculture_entity.du_id
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── sw_delivery_avg_taf   NUMERIC(10,2)
├── sw_delivery_cv        NUMERIC(6,4)
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 21,060 rows

Constraints:
├── Unique: (scenario_short_code, du_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/migrations/04_add_sw_delivery_gw_pumping_tables.sql
```

#### **ag_du_gw_pumping_monthly (agriculture demand unit groundwater pumping statistics)**
```
Table: ag_du_gw_pumping_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL         -- FK du_agriculture_entity.du_id
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── gw_pumping_avg_taf    NUMERIC(10,2)
├── gw_pumping_cv         NUMERIC(6,4)
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 23,400 rows

Constraints:
├── Unique: (scenario_short_code, du_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/migrations/04_add_sw_delivery_gw_pumping_tables.sql
```

#### **ag_du_shortage_monthly (agriculture demand unit shortage statistics)**
```
Table: ag_du_shortage_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL         -- FK du_agriculture_entity.du_id
├── water_month           INTEGER NOT NULL
├── shortage_avg_taf      NUMERIC(10,2)
├── shortage_cv           NUMERIC(6,4)
├── shortage_frequency_pct NUMERIC(5,2)
├── shortage_pct_of_demand_avg NUMERIC(5,2)
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Records: 4,728 rows

Constraints:
├── Unique: (scenario_short_code, du_id, water_month)
└── Check: water_month BETWEEN 1 AND 12
```

#### **ag_du_period_summary (agriculture demand unit period summary)**
```
Table: ag_du_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL         -- FK du_agriculture_entity.du_id
├── simulation_start_year INTEGER NOT NULL
├── simulation_end_year   INTEGER NOT NULL
├── total_years           INTEGER NOT NULL
├── annual_demand_avg_taf NUMERIC(10,2)
├── annual_demand_cv      NUMERIC(6,4)
├── demand_exc_p5         NUMERIC(10,2)                -- Exceedance percentiles
├── demand_exc_p10        NUMERIC(10,2)
├── demand_exc_p25        NUMERIC(10,2)
├── demand_exc_p50        NUMERIC(10,2)
├── demand_exc_p75        NUMERIC(10,2)
├── demand_exc_p90        NUMERIC(10,2)
├── demand_exc_p95        NUMERIC(10,2)
├── annual_sw_delivery_avg_taf NUMERIC(10,2)
├── annual_sw_delivery_cv NUMERIC(6,4)
├── annual_gw_pumping_avg_taf NUMERIC(10,2)
├── annual_gw_pumping_cv  NUMERIC(6,4)
├── gw_pumping_pct_of_demand NUMERIC(5,2)
├── annual_shortage_avg_taf NUMERIC(10,2)
├── shortage_years_count  INTEGER
├── shortage_frequency_pct NUMERIC(5,2)
├── annual_shortage_pct_of_demand NUMERIC(5,2)
├── reliability_pct       NUMERIC(5,2)
├── avg_pct_demand_met    NUMERIC(5,2)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 2,100 rows

Constraints:
└── Unique: (scenario_short_code, du_id)
```

#### **refuge_du_delivery_monthly (wildlife refuge demand unit delivery statistics)**
```
Table: refuge_du_delivery_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL
├── du_id                 VARCHAR(20) NOT NULL         -- References du_refuge_entity.du_id
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── delivery_avg_taf      NUMERIC(10,2)                -- Mean monthly SW delivery (TAF)
├── delivery_cv           NUMERIC(10,4)                -- CV of monthly delivery
├── q0                    NUMERIC(10,2)                -- Percentile bands
├── q10                   NUMERIC(10,2)
├── q30                   NUMERIC(10,2)
├── q50                   NUMERIC(10,2)
├── q70                   NUMERIC(10,2)
├── q90                   NUMERIC(10,2)
├── q100                  NUMERIC(10,2)
├── exc_p5                NUMERIC(10,2)                -- Exceedance percentiles
├── exc_p10               NUMERIC(10,2)
├── exc_p25               NUMERIC(10,2)
├── exc_p50               NUMERIC(10,2)
├── exc_p75               NUMERIC(10,2)
├── exc_p90               NUMERIC(10,2)
├── exc_p95               NUMERIC(10,2)
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Records: ~4,752 rows (18 DUs × 12 months × ~22 active scenarios)

Constraints:
├── Unique: (scenario_short_code, du_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

Source: DN_{DU_ID} from deliveries CSV, TAF block (Units row = 'TAF').
ETL: etl/statistics/refuge/calculate_refuge_statistics.py
```

#### **refuge_du_shortage_monthly (wildlife refuge demand unit shortage statistics)**
```
Table: refuge_du_shortage_monthly
├── id                        SERIAL PRIMARY KEY
├── scenario_short_code       VARCHAR(20) NOT NULL
├── du_id                     VARCHAR(20) NOT NULL     -- References du_refuge_entity.du_id
├── water_month               INTEGER NOT NULL         -- 1-12 (Oct=1, Sep=12)
├── shortage_avg_taf          NUMERIC(10,2)            -- Mean monthly shortage (TAF)
├── shortage_cv               NUMERIC(10,4)            -- CV of monthly shortage (TAF)
├── shortage_pct_avg          NUMERIC(10,4)            -- Mean shortage as % of demand
├── shortage_pct_cv           NUMERIC(10,4)            -- CV of shortage %
├── shortage_frequency_pct    NUMERIC(10,4)            -- Fraction of months with shortage > 0.1 TAF
├── q0                        NUMERIC(10,2)            -- Percentile bands of monthly shortage TAF
├── q10                       NUMERIC(10,2)
├── q30                       NUMERIC(10,2)
├── q50                       NUMERIC(10,2)
├── q70                       NUMERIC(10,2)
├── q90                       NUMERIC(10,2)
├── q100                      NUMERIC(10,2)
├── exc_p5                    NUMERIC(10,2)            -- Exceedance percentiles
├── exc_p10                   NUMERIC(10,2)
├── exc_p25                   NUMERIC(10,2)
├── exc_p50                   NUMERIC(10,2)
├── exc_p75                   NUMERIC(10,2)
├── exc_p90                   NUMERIC(10,2)
├── exc_p95                   NUMERIC(10,2)
├── sample_count              INTEGER
├── is_active                 BOOLEAN DEFAULT TRUE
├── created_at                TIMESTAMPTZ DEFAULT NOW()
├── created_by                INTEGER NOT NULL DEFAULT 1
├── updated_at                TIMESTAMPTZ DEFAULT NOW()
└── updated_by                INTEGER NOT NULL DEFAULT 1

Records: ~4,752 rows (18 DUs × 12 months × ~22 active scenarios)

Constraints:
├── Unique: (scenario_short_code, du_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

Note: Shortage is DERIVED — no native CalSim shortage variable exists for refuge DUs.
      shortage_taf = max(AWO_{DU_ID} - DN_{DU_ID}, 0)
Source: AWO_{DU_ID} from SV input CSV (TAF); DN_{DU_ID} from deliveries CSV (TAF block).
ETL: etl/statistics/refuge/calculate_refuge_statistics.py
```

#### **refuge_du_period_summary (wildlife refuge demand unit period summary)**
```
Table: refuge_du_period_summary
├── id                          SERIAL PRIMARY KEY
├── scenario_short_code         VARCHAR(20) NOT NULL
├── du_id                       VARCHAR(20) NOT NULL     -- References du_refuge_entity.du_id
├── simulation_start_year       INTEGER                  -- First water year (e.g., 1922)
├── simulation_end_year         INTEGER                  -- Last water year (e.g., 2021)
├── total_years                 INTEGER                  -- Total simulated years
├── annual_delivery_avg_taf     NUMERIC(10,2)            -- Mean of annual delivery totals
├── annual_delivery_cv          NUMERIC(10,4)            -- CV of annual delivery
├── delivery_exc_p5             NUMERIC(10,2)            -- Annual delivery exceedance curve
├── delivery_exc_p10            NUMERIC(10,2)
├── delivery_exc_p25            NUMERIC(10,2)
├── delivery_exc_p50            NUMERIC(10,2)
├── delivery_exc_p75            NUMERIC(10,2)
├── delivery_exc_p90            NUMERIC(10,2)
├── delivery_exc_p95            NUMERIC(10,2)
├── annual_shortage_avg_taf     NUMERIC(10,2)            -- Mean of annual shortage totals
├── annual_shortage_cv          NUMERIC(10,4)            -- CV of annual shortage
├── annual_shortage_pct_avg     NUMERIC(10,4)            -- Mean annual shortage as % of demand
├── annual_shortage_pct_cv      NUMERIC(10,4)            -- CV of annual shortage %
├── reliability_pct_95          NUMERIC(10,4)            -- 95th pct of annual shortage %
│                                                        -- "In 95 of 100 years, shortage ≤ this value"
├── is_active                   BOOLEAN DEFAULT TRUE
├── created_at                  TIMESTAMPTZ DEFAULT NOW()
├── created_by                  INTEGER NOT NULL DEFAULT 1
├── updated_at                  TIMESTAMPTZ DEFAULT NOW()
└── updated_by                  INTEGER NOT NULL DEFAULT 1

Records: ~396 rows (18 DUs × ~22 active scenarios)

Constraints:
└── Unique: (scenario_short_code, du_id)

ETL: etl/statistics/refuge/calculate_refuge_statistics.py
```

#### **env_flow_season (environmental flow season definitions)**
```
Table: env_flow_season
├── id                    SERIAL PRIMARY KEY
├── short_code            VARCHAR UNIQUE NOT NULL      -- "fall", "winter", "spring", "summer", "annual"
├── label                 VARCHAR NOT NULL             -- Display name
├── description           TEXT
├── start_month           INTEGER                      -- Water month (Oct=1)
├── end_month             INTEGER                      -- Water month (Oct=1)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 5 seasons (fall, winter, spring, summer, annual)

DDL: database/scripts/sql/migrations/24_create_env_flow_statistics_tables.sql
```

#### **env_flow_channel_monthly (environmental flow monthly statistics)**
```
Table: env_flow_channel_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR NOT NULL
├── network_arc_id        INTEGER NOT NULL             -- FK to channel_entity.id
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── flow_avg_cfs          NUMERIC                      -- Mean regulated flow (CFS)
├── flow_cv               NUMERIC                      -- CV of regulated flow
├── unimp_avg_cfs         NUMERIC                      -- Mean unimpaired flow (CFS)
├── pct_unimpaired_avg    NUMERIC                      -- Mean % of unimpaired flow
├── pct_unimpaired_cv     NUMERIC                      -- CV of % unimpaired flow
├── q0 – q100             NUMERIC (7 cols)             -- Percentile bands (regulated flow)
├── exc_p5 – exc_p95      NUMERIC (7 cols)             -- Exceedance percentiles (regulated flow)
├── unimp_q0 – unimp_q100 NUMERIC (7 cols)            -- Percentile bands (unimpaired flow)
├── unimp_exc_p5 – unimp_exc_p95 NUMERIC (7 cols)     -- Exceedance percentiles (unimpaired flow)
├── mif_avg_cfs           NUMERIC                      -- Mean minimum instream flow (CFS)
├── mif_cv                NUMERIC                      -- CV of MIF
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 13,452 rows

Constraints:
├── Unique: (scenario_short_code, network_arc_id, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/migrations/24_create_env_flow_statistics_tables.sql
```

#### **env_flow_channel_seasonal (environmental flow seasonal statistics)**
```
Table: env_flow_channel_seasonal
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR NOT NULL
├── network_arc_id        INTEGER NOT NULL             -- FK to channel_entity.id
├── season_id             INTEGER NOT NULL             -- FK to env_flow_season.id
├── flow_avg_cfs          NUMERIC                      -- Mean regulated flow (CFS)
├── flow_cv               NUMERIC
├── unimp_avg_cfs         NUMERIC                      -- Mean unimpaired flow (CFS)
├── pct_unimpaired_avg    NUMERIC
├── pct_unimpaired_cv     NUMERIC
├── q0 – q100             NUMERIC (7 cols)             -- Percentile bands (regulated + unimpaired)
├── exc_p5 – exc_p95      NUMERIC (7 cols)             -- Exceedance percentiles
├── unimp_q0 – unimp_q100 NUMERIC (7 cols)
├── unimp_exc_p5 – unimp_exc_p95 NUMERIC (7 cols)
├── mif_avg_cfs           NUMERIC
├── mif_cv                NUMERIC
├── eflow_avg_cfs         NUMERIC                      -- Mean functional flow target (CFS)
├── eflow_cv              NUMERIC
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 5,605 rows

Constraints:
└── Unique: (scenario_short_code, network_arc_id, season_id)

DDL: database/scripts/sql/migrations/24_create_env_flow_statistics_tables.sql
```

#### **env_flow_channel_period_summary (environmental flow period summary)**
```
Table: env_flow_channel_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR NOT NULL
├── network_arc_id        INTEGER NOT NULL             -- FK to channel_entity.id
├── simulation_start_year INTEGER NOT NULL
├── simulation_end_year   INTEGER NOT NULL
├── total_years           INTEGER NOT NULL
├── annual_flow_avg_cfs   NUMERIC
├── annual_flow_cv        NUMERIC
├── annual_unimp_avg_cfs  NUMERIC
├── annual_pct_unimpaired_avg NUMERIC
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 1,121 rows

Constraints:
└── Unique: (scenario_short_code, network_arc_id)

DDL: database/scripts/sql/migrations/24_create_env_flow_statistics_tables.sql
```

#### **delta_monthly (Delta monthly statistics)**
```
Table: delta_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR NOT NULL
├── variable_name         VARCHAR NOT NULL             -- Delta variable (outflow, X2, etc.)
├── water_month           INTEGER NOT NULL             -- 1-12 (Oct=1, Sep=12)
├── avg                   NUMERIC
├── cv                    NUMERIC
├── avg_cfs               NUMERIC
├── q0                    NUMERIC                      -- Percentile bands
├── q10                   NUMERIC
├── q30                   NUMERIC
├── q50                   NUMERIC
├── q70                   NUMERIC
├── q90                   NUMERIC
├── q100                  NUMERIC
├── exc_p5                NUMERIC                      -- Exceedance percentiles
├── exc_p10               NUMERIC
├── exc_p25               NUMERIC
├── exc_p50               NUMERIC
├── exc_p75               NUMERIC
├── exc_p90               NUMERIC
├── exc_p95               NUMERIC
├── sample_count          INTEGER
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 1,248 rows

Constraints:
├── Unique: (scenario_short_code, variable_name, water_month)
└── Check: water_month BETWEEN 1 AND 12

DDL: database/scripts/sql/migrations/29_create_delta_statistics_tables.sql
```

#### **delta_period_summary (Delta period summary)**
```
Table: delta_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR NOT NULL
├── variable_name         VARCHAR NOT NULL
├── simulation_start_year INTEGER NOT NULL
├── simulation_end_year   INTEGER NOT NULL
├── total_years           INTEGER NOT NULL
├── annual_avg            NUMERIC
├── annual_cv             NUMERIC
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL

Records: 104 rows

Constraints:
└── Unique: (scenario_short_code, variable_name)

DDL: database/scripts/sql/migrations/29_create_delta_statistics_tables.sql
```

---

## **10_TIER LAYER**

### **1. tier_definition**

```
Table: tier_definition
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- Tier identifier (ENV_FLOWS, DELTA_ECO, etc.)
├── name                 VARCHAR NOT NULL           -- Display name (Environmental flows, Delta ecology)
├── description          TEXT                       -- Detailed description of the indicator
├── tier_type            VARCHAR NOT NULL           -- 'multi_value' or 'single_value'
├── tier_count           INTEGER NOT NULL           -- Number of tier values (1 or 4)
├── tier_version_id      INTEGER NOT NULL DEFAULT 8 -- FK to version.id (tier family)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL DEFAULT coeqwal_current_operator() -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL DEFAULT coeqwal_current_operator() -- FK to developer.id

Records: 9 tier indicators

Foreign keys:
├── Ref: tier_definition.tier_version_id > version.id [delete: restrict, update: cascade]
├── Ref: tier_definition.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: tier_definition.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── tier_definition_pkey (id) -- Primary key
├── tier_definition_short_code_key (short_code) -- Unique constraint
├── idx_tier_definition_tier_type (tier_type) -- Type filtering
├── idx_tier_definition_version (tier_version_id) -- Version lookups
└── idx_tier_definition_active (is_active) -- Active status filtering

Constraints:
├── tier_type CHECK (tier_type IN ('multi_value', 'single_value'))
└── tier_count CHECK (tier_count IN (1, 4))

Values (9 total):
├── ENV_FLOWS: Environmental flows (multi_value, 4 tiers)
├── RES_STOR: Reservoir storage (multi_value, 4 tiers)
├── GW_STOR: Groundwater storage (multi_value, 4 tiers)
├── DELTA_ECO: Delta ecology (single_value, 1 tier)
├── FW_DELTA_USES: Freshwater for in-Delta uses (single_value, 1 tier)
├── FW_EXP: Freshwater for Delta exports (single_value, 1 tier)
├── WRC_SALMON_AB: Salmon abundance (single_value, 1 tier)
├── CWS_DEL: Community water system deliveries (multi_value, future)
└── AG_REV: Agricultural revenue (multi_value, future)
```

### **2. tier_location_result (tier values by location)**

```
Table: tier_location_result
├── id                      SERIAL PRIMARY KEY
├── scenario_short_code     VARCHAR NOT NULL           -- Scenario identifier (s0011, s0020, etc.) - logical ref to scenario.scenario_id
├── tier_short_code         VARCHAR NOT NULL           -- FK to tier_definition.short_code
├── location_type           VARCHAR NOT NULL           -- 'network_node', 'wba', 'reservoir', 'compliance_station', 'region'
├── location_id             VARCHAR NOT NULL           -- ID in respective table (e.g., SAC232, 08N, SHSTA, JP, DELTA)
├── location_name           VARCHAR                    -- Display name for map tooltip
├── tier_level              INTEGER                    -- 1, 2, 3, or 4 (tier assignment for this location)
├── tier_value              INTEGER                    -- Optional: count or value at this location (usually 1)
├── display_order           INTEGER DEFAULT 1          -- For consistent map marker ordering
├── tier_version_id         INTEGER NOT NULL DEFAULT 8 -- FK to version.id (tier family)
├── created_at              TIMESTAMP DEFAULT NOW()
├── created_by              INTEGER NOT NULL           -- FK to developer.id
├── updated_at              TIMESTAMP DEFAULT NOW()
└── updated_by              INTEGER NOT NULL           -- FK to developer.id

Foreign Keys:
├── Ref: tier_location_result.tier_short_code > tier_definition.short_code [delete: restrict, update: cascade]
├── Ref: tier_location_result.tier_version_id > version.id [delete: restrict, update: cascade]
├── Ref: tier_location_result.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: tier_location_result.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── tier_location_result_unique (scenario_short_code, tier_short_code, location_id, tier_version_id)
├── idx_tier_location_scenario (scenario_short_code)
├── idx_tier_location_tier (tier_short_code)
├── idx_tier_location_type (location_type)
├── idx_tier_location_level (tier_level)
└── idx_tier_location_combined (scenario_short_code, tier_short_code)

Constraints:
├── location_type CHECK (location_type IN ('network_node', 'wba', 'reservoir', 'compliance_station', 'region'))
└── tier_level CHECK (tier_level BETWEEN 1 AND 4 OR tier_level IS NULL)

Location Type Reference:
├── 'network_node' to network.short_code (ENV_FLOWS, FW_EXP evaluation points)
├── 'wba' to wba.wba_id (GW_STOR aquifer polygons)
├── 'reservoir' to reservoir.calsim_short_code (RES_STOR lake polygons)  
├── 'compliance_station' to compliance_station.station_code (FW_DELTA_USES monitoring)
└── 'region' to hydrologic_region.short_code (DELTA_ECO, WRC_SALMON_AB regional)

Example: ENV_FLOWS s0011 has 17 location records (one per evaluation node) with tier_levels 2-3
```

### **3. tier_result (aggregated tier values by scenario)**

```
Table: tier_result
├── id                   SERIAL PRIMARY KEY
├── scenario_short_code  VARCHAR NOT NULL           -- Scenario identifier (s0011, etc.) - logical ref to scenario.scenario_id
├── tier_short_code      VARCHAR NOT NULL           -- FK to tier_definition.short_code
├── tier_1_value         INTEGER                    -- Count in Tier 1 (best performance)
├── tier_2_value         INTEGER                    -- Count in Tier 2 (good performance)
├── tier_3_value         INTEGER                    -- Count in Tier 3 (moderate performance)
├── tier_4_value         INTEGER                    -- Count in Tier 4 (poor performance)
├── norm_tier_1          NUMERIC(5,3)               -- Normalized Tier 1 (0-1 scale for D3)
├── norm_tier_2          NUMERIC(5,3)               -- Normalized Tier 2 (0-1 scale for D3)
├── norm_tier_3          NUMERIC(5,3)               -- Normalized Tier 3 (0-1 scale for D3)
├── norm_tier_4          NUMERIC(5,3)               -- Normalized Tier 4 (0-1 scale for D3)
├── total_value          INTEGER                    -- Sum of tier values (for multi-value)
├── single_tier_level    INTEGER                    -- Single tier level 1-4 (for single-value)
├── tier_version_id      INTEGER NOT NULL DEFAULT 8 -- FK to version.id (tier family)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL DEFAULT coeqwal_current_operator() -- FK to developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL DEFAULT coeqwal_current_operator() -- FK to developer.id

Records: 64 tier results (8 scenarios × ~8 indicators)

Note: scenario_short_code is a logical reference to scenario.scenario_id, not a strict FK.
This allows tier results to exist independently for flexibility during data loading.

Foreign keys:
├── Ref: tier_result.tier_short_code > tier_definition.short_code [delete: restrict, update: cascade]
├── Ref: tier_result.tier_version_id > version.id [delete: restrict, update: cascade]
├── Ref: tier_result.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: tier_result.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── tier_result_pkey (id) -- Primary key
├── tier_result_scenario_short_code_tier_short_code_tier_versio_key (scenario_short_code, tier_short_code, tier_version_id) -- Unique constraint
├── idx_tier_result_scenario (scenario_short_code) -- Scenario lookups
├── idx_tier_result_tier (tier_short_code) -- Tier lookups
├── idx_tier_result_scenario_tier (scenario_short_code, tier_short_code) -- Combined lookups
├── idx_tier_result_version (tier_version_id) -- Version lookups
└── idx_tier_result_active (is_active) -- Active status filtering

Constraints:
├── Mutual exclusion: (tier_1_value IS NOT NULL AND single_tier_level IS NULL) OR (tier_1_value IS NULL AND single_tier_level IS NOT NULL)
└── Tier level bounds: single_tier_level BETWEEN 1 AND 4 OR single_tier_level IS NULL

D3 Visualization Data:
├── Multi-value tiers: Use norm_tier_1 through norm_tier_4 (pre-calculated 0-1 scale)
├── Single-value tiers: Use single_tier_level (1-4)
├── Color scheme: Tier 1=#2cc83b, Tier 2=#2064d4, Tier 3=#f89740, Tier 4=#f96262
└── Comparable bar charts enabled through normalization

Sample data:
├── ENV_FLOWS s0011: [0,5,12,0] to normalized [0, 0.294, 0.706, 0]
├── GW_STOR s0020: [7,14,15,6] to normalized [0.167, 0.333, 0.357, 0.143]
└── DELTA_ECO s0011: single_tier_level = 4
```

## **DATABASE FUNCTIONS**

### **Helper functions**
```sql
-- Get current operator for audit fields
FUNCTION coeqwal_current_operator() RETURNS INTEGER
├── Tries to find developer by database user or email
├── Falls back to admin account (ID 2: jfantauzza@berkeley.edu)
└── Used in DEFAULT values for created_by/updated_by

-- Get active version for a family  
FUNCTION get_active_version(family_id INTEGER) RETURNS INTEGER
├── Returns the active version ID for a version family
└── Used for default version references

-- Network analysis functions (todo: refine with new network schemat)
FUNCTION get_connected_arcs(node_id INTEGER) RETURNS SETOF RECORD
FUNCTION get_downstream_nodes(node_id INTEGER) RETURNS SETOF RECORD  
FUNCTION get_upstream_nodes(node_id INTEGER) RETURNS SETOF RECORD
└── Advanced network connectivity analysis
```

## **NETWORK TABLES**

### **NETWORK TYPE HIERARCHY**

#### **Tier 1: network_entity_type (Top Level)**
```
Table: network_entity_type
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- "arc", "node", "null", "unimpaired_flows"
├── label                VARCHAR NOT NULL           -- "Arc", "Node", "None", "Unimpaired Flows"
├── description          TEXT                       -- Purpose description
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

```

#### **Tier 2: Type table (unified arc + node types)**
```
Table: network_type
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- "CH", "CT", "D", "STR", etc.
├── label                VARCHAR NOT NULL           -- "Channel", "Cross transfer", "Storage", etc.
├── description          TEXT
├── network_entity_type_id INTEGER NOT NULL         -- FK to network_entity_type.id (1=arc, 2=node)
├── model_source_id      INTEGER DEFAULT 1          -- FK to model_source.id (calsim3)
├── source_id            INTEGER DEFAULT 4          -- FK to source.id (geopackage)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (21 total):
├── IDs 1-10: Arc types (CH, CT, D, DA, DD, IN, RT, SP, SR, NULL)
└── IDs 11-21: Node types (CH, NP, OM, PR, PS, RFS, S, STR, WTP, WWTP, X)

```

#### **Tier 3: Subtype table (unified arc + node subtypes)**
```
Table: network_subtype
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- "ST", "CL", "RES", "A", "STM", etc.
├── label                VARCHAR NOT NULL           -- "Stream", "Canal", "Reservoir", "Agricultural", etc.
├── description          TEXT
├── type_id              INTEGER NOT NULL           -- FK to network_type.id (parent type)
├── model_source_id      INTEGER DEFAULT 1          -- FK to model_source.id (calsim3)
├── source_id            INTEGER DEFAULT 4          -- FK to source.id (geopackage)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Values (28 total):
├── IDs 1-10:  Arc subtypes (BP, CH, CL, HIS, IM, LI, NA, NS, PRP, ST)
└── IDs 11-28: Node subtypes (A, BYP, CNL, GWO, NA, NSM, OMD, OMR, PRP, R, Reservoir, SG, SIM, STM, U, X, PR, NR)
              PR (id=27) = Project Refuge — CVP (Central Valley Project) contract deliveries
              NR (id=28) = Non-project Refuge — water rights only, no CVP deliveries
              R  (id=20) = Generic refuge (legacy; all 18 DUs now use PR or NR)

Note: 9 refuge nodes (08N_PR1, 08N_PR2, and all SJR refuges except 91_PR) were
incorrectly tagged with subtype U (Urban, id=25) in the original seed data.
Migration 22 corrects all 18 refuge nodes to use PR (id=27) or NR (id=28).
```

#### **Views**
```
View: v_network_arc_types_complete
├── Combines all arc type hierarchy levels
├── Shows: full_code, type_code, type_name, subtype_code, subtype_name
└── Ordered by type_code, subtype_code

View: v_network_node_types_complete  
├── Combines all node type hierarchy levels
├── Shows: full_code, type_code, type_name, subtype_code, subtype_name
└── Ordered by type_code, subtype_code
```

### **1. network (master registry)**
```
Table: network
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- "AMR006", "C_AMR006", "UNIMP_OROV"
├── name                 VARCHAR                    -- Display name from geopackage, CalSim manual, or other sources
├── description          TEXT                       -- Description from XML schematic or other sources
├── comment              TEXT                       -- Additional notes or source comments
├── entity_type_id       INTEGER                    -- FK to network_entity_type.id (arc=1, node=2, null=3, unimpaired_flows=4)
├── type_id              INTEGER                    -- FK to network_type.id
├── subtype_ids          INTEGER[]                  -- Array of network_subtype.id values (e.g., {25,23})
├── model_list           INTEGER[]                  -- Array of model_source.id (e.g., {1} for CalSim3)
├── source_list          INTEGER[]                  -- Array of source.id (e.g., {1,4,8,9} for report+geopackage+schematic+manual)
├── has_gis              BOOLEAN DEFAULT FALSE      -- Spatial data available
├── hydrologic_region_id INTEGER                    -- FK to hydrologic_region.id (1=SAC, 2=SJR, 3=DELTA, 4=TL, 5=CC)
├── riv_sys              VARCHAR                    -- River system name from geopackage (e.g., "Sacramento River", "San Joaquin River")
├── strm_code            VARCHAR                    -- Stream code from geopackage (e.g., "SAC", "SJR", "DMC")
├── network_version_id   INTEGER NOT NULL           -- FK to version.id (network family, default=12)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Records: 6,908 total (2,610 arcs + 4,298 nodes)
Data sources: XML schematic (6,466) + geopackage nodes (1,548) + geopackage arcs (2,619)

Foreign Keys:
├── Ref: network.entity_type_id > network_entity_type.id [delete: restrict, update: cascade]
├── Ref: network.type_id > network_type.id [delete: restrict, update: cascade]
├── Ref: network.hydrologic_region_id > hydrologic_region.id [delete: restrict, update: cascade]
├── Ref: network.network_version_id > version.id [delete: restrict, update: cascade]
├── Ref: network.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: network.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── network_short_code_key (short_code) -- Unique constraint (PRIMARY for lookups)
├── idx_network_entity_type (entity_type_id) -- Filter by arcs vs nodes
├── idx_network_type (type_id) -- Type filtering
├── idx_network_source_list (source_list) USING GIN -- Multi-source queries
├── idx_network_model_list (model_list) USING GIN -- Multi-model queries
├── idx_network_has_gis (has_gis) -- Filter for spatial data availability
├── idx_network_hydrologic_region (hydrologic_region_id) -- Regional queries
├── idx_network_strm_code (strm_code) -- Stream code lookups
└── idx_network_version (network_version_id) -- Version filtering

Constraints:
├── model_list CHECK (array_length(model_list, 1) > 0) -- At least one model
└── source_list CHECK (array_length(source_list, 1) > 0) -- At least one source

```

### **2. network_arc (arc-specific physical attributes)**

```
Table: network_arc
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- Arc identifier (matches network.short_code for safety)
├── network_id           INTEGER NOT NULL           -- FK to network.id (populated during DB load via short_code lookup)
├── river                VARCHAR                    -- River identifier for watershed connection (AMR, CCH, ELD)
├── from_node            VARCHAR                    -- From node identifier
├── to_node              VARCHAR                    -- To node identifier  
├── shape_length_m       NUMERIC                    -- Arc length in meters
├── model_source_id      INTEGER DEFAULT 1          -- FK to model_source.id (CalSim3)
├── source_id            INTEGER DEFAULT 4          -- FK to source.id (geopackage)
├── network_version_id   INTEGER NOT NULL           -- FK to version.id (network family)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Records: 2,118 arcs from geopackage

Foreign Keys:
├── Ref: network_arc.network_id > network.id [delete: cascade, update: cascade]
├── Ref: network_arc.model_source_id > model_source.id [delete: restrict, update: cascade]
├── Ref: network_arc.source_id > source.id [delete: restrict, update: cascade]
├── Ref: network_arc.network_version_id > version.id [delete: restrict, update: cascade]
├── Ref: network_arc.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: network_arc.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── network_arc_short_code_key (short_code) -- Unique constraint
├── idx_network_arc_network_id (network_id) -- FK performance
├── idx_network_arc_river (river) -- For watershed lookups
└── idx_network_arc_connectivity (from_node, to_node) -- Connectivity queries

Note: shape_length_m units are meters
```

### **3. river_watershed (river-to-watershed mapping)**

Status: PLANNED — not yet created in the database. The `watershed` lookup table exists.
`network_node.strm_code` currently stores string codes without a FK.

```
Table: river_watershed   [PLANNED]
├── id                    SERIAL PRIMARY KEY
├── river_prefix          VARCHAR UNIQUE NOT NULL    -- River identifier (AMR, CCH, ELD, etc.)
├── river_name            VARCHAR NOT NULL           -- Full river name (American River, Cache Creek)
├── watershed_short_code  VARCHAR NOT NULL           -- FK reference to watersheds.short_code
├── source_id             INTEGER DEFAULT 1          -- FK to source.id (CalSim report)
├── network_version_id    INTEGER NOT NULL           -- FK to version.id (network family)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMP DEFAULT NOW()
├── created_by            INTEGER NOT NULL           -- FK to developer.id
├── updated_at            TIMESTAMP DEFAULT NOW()
└── updated_by            INTEGER NOT NULL           -- FK to developer.id

Records: 268 river-watershed mappings from CalSim report
Note: No model_source_id - rivers/watersheds are geographic features, not model-specific

Foreign keys:
├── Ref: river_watershed.watershed_short_code > watersheds.short_code [delete: restrict, update: cascade]
├── Ref: river_watershed.source_id > source.id [delete: restrict, update: cascade]
├── Ref: river_watershed.network_version_id > version.id [delete: restrict, update: cascade]
├── Ref: river_watershed.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: river_watershed.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── river_watershed_river_prefix_key (river_prefix) -- Unique constraint
└── idx_river_watershed_watershed (watershed_short_code) -- Watershed lookups


Values (268 total, 259 unique prefixes):
├── AMR to SAC_RIVER (American River to Sacramento River Hydrologic Region)
├── CCH to SAC_RIVER (Cache Creek to Sacramento River Hydrologic Region)
├── ELD to UPPER_AMERICAN (Eldorado to Upper American River Watershed)
├── SFA to UPPER_AMERICAN (South Fork American to Upper American River Watershed)
├── TRN to SAN_JOAQUIN (Tuolumne River to San Joaquin River Hydrologic Region)
└── ... (263 more river mappings)

Distribution by watershed:
├── SAC_RIVER: 86 rivers
├── SAN_JOAQUIN: 56 rivers
├── UPPER_AMERICAN: 44 rivers
├── UPPER_FEATHER: 26 rivers
├── YUBA_RIVER: 17 rivers
├── UPPER_STANISLAUS: 16 rivers
├── UPPER_TUOLUMNE: 11 rivers
├── UPPER_MOKELUMNE: 10 rivers
└── BEAR_RIVER: 2 rivers
```

### **4. network_node (node-specific physical attributes)**

```
Table: network_node
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- Node identifier (matches network.short_code for safety)
├── network_id           INTEGER NOT NULL           -- FK to network.id (populated during DB load via short_code lookup)
├── riv_mi               NUMERIC                    -- River mile location
├── c2vsim_gw            VARCHAR                    -- C2VSIM groundwater connection
├── c2vsim_sw            VARCHAR                    -- C2VSIM surface water connection
├── nrest_gage           VARCHAR                    -- Nearest stream gauge
├── strm_code            VARCHAR                    -- Stream/river code (links to river_watershed)
├── rm_ii                VARCHAR                    -- River mile II designation
├── model_source_id      INTEGER DEFAULT 1          -- FK to model_source.id (CalSim3)
├── source_id            INTEGER DEFAULT 4          -- FK to source.id (geopackage)
├── network_version_id   INTEGER NOT NULL           -- FK to version.id (network family)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

Records: 1,400 nodes from geopackage

Foreign keys:
├── Ref: network_node.strm_code > river_watershed.river_prefix [delete: restrict, update: cascade]
├── Ref: network_node.model_source_id > model_source.id [delete: restrict, update: cascade]
├── Ref: network_node.source_id > source.id [delete: restrict, update: cascade]
├── Ref: network_node.network_version_id > version.id [delete: restrict, update: cascade]
├── Ref: network_node.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: network_node.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── network_node_short_code_key (short_code) -- Unique constraint
└── idx_network_node_strm_code (strm_code) -- River system lookups

Top stream codes by node count:
├── SAC: 89 nodes (Sacramento River)
├── SJR: 64 nodes (San Joaquin River)
├── DMC: 24 nodes (Delta-Mendota Canal)
├── FTR: 24 nodes (Feather River)
├── CAA: 22 nodes (California Aqueduct)
├── MCD: 18 nodes (Mokelumne River)
├── BRR: 16 nodes (Bear River)
└── ... (229 more stream codes)

Note: strm_code links nodes to river systems via river_watershed.river_prefix
```

Query examples:
```sql
-- Find all streams (requires JOIN for readable results)
SELECT n.*, nt.short_code as type_name, array_agg(ns.short_code) as subtype_names
FROM network n
JOIN network_type nt ON n.type_id = nt.id
LEFT JOIN network_subtype ns ON ns.id = ANY(n.subtype_ids)
WHERE 25 = ANY(n.subtype_ids)  -- STM subtype_id
GROUP BY n.id, nt.short_code;

-- Find all gauges (monitoring classification)
SELECT * FROM network WHERE 23 = ANY(subtype_ids);  -- SG subtype_id

-- Find stream gauges (dual purpose) 
SELECT * FROM network WHERE subtype_ids @> array[25, 23];  -- STM + SG

-- Find any gauge (active or discontinued)
SELECT * FROM network WHERE subtype_ids && array[23, 28];  -- SG or SG_DISC

-- Find nodes with multiple subtypes
SELECT * FROM network WHERE array_length(subtype_ids, 1) > 1;
```

### **2. network_gis (multi-precision-level spatial data)**

```
Table: network_gis
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR NOT NULL           -- Network element identifier (matches network.short_code for safety)
├── network_id           INTEGER NOT NULL           -- FK to network.id (populated during DB load via short_code lookup)
├── precision_level      VARCHAR NOT NULL           -- "precise", "mapping_efficient", "regional"
├── geom_wkt             TEXT NOT NULL              -- Primary geometry storage
├── srid                 INTEGER DEFAULT 4326
├── geom                 GEOMETRY (computed)        -- PostGIS binary (STORED)
├── estimated_accuracy_meters NUMERIC               -- Actual accuracy estimate
├── source_id            INTEGER NOT NULL           -- FK to source.id
├── network_version_id   INTEGER NOT NULL           -- FK to version.id (network family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id
```

### **3. network_arc_attribute (Arc network attribute)**

Status: PLANNED — not yet created in the database.

```
Table: network_arc_attribute   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK to network.id
├── name                 VARCHAR                    -- Arc name
├── calsim_id_stream     VARCHAR                    -- Stream/canal identifier (not unique)
├── arc_id_short_code    VARCHAR                    -- Arc identifier (in most cases matches network.short_code)
├── type_id              INTEGER                    -- FK to network_arc_type.id
├── sub_type_id          INTEGER                    -- FK to network_arc_subtype.id
├── shape_length         NUMERIC                    -- Arc length in meters
├── attribute_source     JSONB NOT NULL             -- {"name": {"source": "geopackage", "column": "NAME"}, "calsim_id_stream": {"source": "geopackage", "column": "CalSim_ID"}, "shape_length": {"source": "geopackage", "column": "Shape_Leng"}}
├── network_version_id   INTEGER NOT NULL           -- FK to version.id (network family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

```

### **4. network_node_attribute (Node network attribute)**

Status: PLANNED — not yet created in the database.

```
Table: network_node_attribute   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK to network.id
├── calsim_id            VARCHAR                    -- CalSim node identifier
├── riv_mi               NUMERIC                    -- River mile
├── riv_name             VARCHAR                    -- River name
├── comment              TEXT                       -- Node comment
├── c2vsim_gw            VARCHAR                    -- C2VSIM groundwater ID
├── c2vsim_sw            VARCHAR                    -- C2VSIM surface water ID
├── type_id              INTEGER                    -- FK to network_node_type.id
├── sub_type_id          INTEGER                    -- FK to network_node_subtype.id
├── nrest_gage           VARCHAR                    -- Nearest gage
├── strm_code            VARCHAR                    -- Stream code
├── rm_ii                VARCHAR                    -- River mile indicator
├── attribute_source     JSONB NOT NULL             -- {"calsim_id": {"source": "geopackage", "column": "CalSim_ID"}, "riv_mi": {"source": "geopackage", "column": "Riv_Mi"}, "type_id": {"source": "calsim_model", "column": "derived"}}
├── network_version_id   INTEGER NOT NULL           -- FK to version.id (network family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id

```

### **5. network_physical_connectivity (Geopackage Connectivity)**

Status: PLANNED — not yet created in the database.

```
Table: network_physical_connectivity   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── arc_network_id       INTEGER NOT NULL           -- FK to network.id (arc)
├── from_node_network_id INTEGER NOT NULL           -- FK to network.id (from node)
├── to_node_network_id   INTEGER NOT NULL           -- FK to network.id (to node)
├── source_id            INTEGER NOT NULL           -- FK to source.id
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id
```

### **6. network_operational_connectivity (XML Connectivity)**

Status: PLANNED — not yet created in the database.

```
Table: network_operational_connectivity   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── from_network_id      INTEGER NOT NULL           -- FK to network.id
├── to_network_id        INTEGER NOT NULL           -- FK to network.id
├── via_arc_network_id   INTEGER                    -- FK to network.id (connecting arc, if applicable)
├── source_id            INTEGER NOT NULL           -- FK to source.id
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id
```

### **7. network_computational_connectivity (CalSim Connectivity)**

Status: PLANNED — not yet created in the database.

```
Table: network_computational_connectivity   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── from_network_id      INTEGER NOT NULL           -- FK to network.id
├── to_network_id        INTEGER NOT NULL           -- FK to network.id
├── equation_name        VARCHAR                    -- "continuityAMR006"
├── wresl_context_list   JSONB NOT NULL             -- [{"file": "SystemTables_Sac/constraints-Connectivity.wresl", "context": "Sac"}, {"file": "SystemTables_LowerAmerican/constraints-Connectivity.wresl", "context": "LowerAmerican"}]
├── source_id            INTEGER NOT NULL           -- FK to source.id
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id
```

### **8. network_variable (future variable relationships)**

Status: PLANNED — not yet created in the database.

```
Table: network_variable   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK to network.id
├── variable_id          INTEGER NOT NULL           -- FK to variable.id
├── variable_role        VARCHAR                    -- "flow", "storage", "diversion"
├── units                VARCHAR
├── source_id            INTEGER NOT NULL           -- FK to source.id
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id
```

### **9. network_source_attribution**

Status: PLANNED — not yet created in the database.

```
Table: network_source_attribution   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK to network.id
├── source_id            INTEGER NOT NULL           -- FK to source.id
├── note                 TEXT
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id
```

### **10. variable_tier (Many-to-many variable-tier relationship)**

Status: PLANNED — not yet created in the database.

## **ENTITY LAYER TABLES**

### **Entity tables reference network layer:**

#### **channel_entity (channel management)**

Status: **IMPLEMENTED** — created by `14_channel_entity/01_create_channel_entity_variable_tables.sql`,
loaded from `seed_tables/04_calsim_data/channel_entity.csv` (669 rows).
Env-flow attribute columns added by migration 23; developer FKs + domain_family_map by migration 25.

```
Table: channel_entity   [IMPLEMENTED — migration 25 complete]
├── id                   SERIAL PRIMARY KEY
├── network_arc_id       VARCHAR(30) NOT NULL UNIQUE -- CalSim arc ID, e.g. C_SAC049
├── short_code           VARCHAR(100)               -- human label (may be full name)
├── name                 VARCHAR(200)
├── description          TEXT
├── subtype              VARCHAR(50)
├── entity_type_id       INTEGER NOT NULL DEFAULT 1  -- FK to calsim_entity_type.id
├── schematic_type_id    INTEGER
├── hydrologic_region_id VARCHAR(10)                 -- SAC, SJR, DELTA, etc.
├── boundary_condition   VARCHAR(50)
├── from_node            VARCHAR(30)
├── to_node              VARCHAR(30)
├── length_m             NUMERIC(14,4)
├── has_tiers            BOOLEAN DEFAULT FALSE
├── is_main              BOOLEAN DEFAULT FALSE
├── has_gis_data         INTEGER DEFAULT 1
├── entity_version_id    INTEGER NOT NULL DEFAULT 1
├── source_ids           TEXT
├── watershed_short_code VARCHAR(30)                 -- FK to watershed.short_code
├── unimp_sv_variable    VARCHAR(30)                 -- CalSim SV unimpaired variable (override)
├── has_mif              BOOLEAN NOT NULL DEFAULT FALSE
├── has_eflows           BOOLEAN NOT NULL DEFAULT FALSE
├── channel_class        VARCHAR(30) CHECK IN ('stream','canal','reservoir_release')
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL DEFAULT 1   -- FK to developer.id
├── updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL DEFAULT 1   -- FK to developer.id

Indexes:
├── idx_channel_entity_network_arc   (network_arc_id)
├── idx_channel_entity_watershed     (watershed_short_code)
├── idx_channel_entity_has_mif       (has_mif) WHERE has_mif = TRUE
├── idx_channel_entity_has_eflows    (has_eflows) WHERE has_eflows = TRUE
└── idx_channel_entity_channel_class (channel_class)

domain_family_map: version_family = 'entity'
Developer attribution: created_by = 2 (jfantauzza) — set by migration 25
```

#### **reservoir_entity (reservoir management)**
```
Table: reservoir_entity
├── id                   INTEGER PRIMARY KEY
├── network_node_id      VARCHAR(20) NOT NULL       -- Network node identifier (e.g., "SHSTA")
├── short_code           VARCHAR(20) UNIQUE NOT NULL -- Short identifier (SHSTA, OROVL, etc.)
├── name                 VARCHAR(100)               -- Full reservoir name
├── description          TEXT                       -- Detailed description
├── associated_river     VARCHAR(100)               -- River system
├── entity_type_id       INTEGER NOT NULL DEFAULT 1 -- FK to calsim_entity_type.id
├── schematic_type_id    INTEGER                    -- FK to schematic type lookup
├── hydrologic_region_id INTEGER                    -- FK to hydrologic_region.id (1=SAC, 2=SJR, 4=Tulare)
├── capacity_taf         NUMERIC(10,2)              -- Maximum capacity in TAF
├── dead_pool_taf        NUMERIC(10,2)              -- Dead pool storage in TAF
├── surface_area_acres   NUMERIC(12,2)              -- Surface area in acres
├── operational_purpose  VARCHAR(50)                -- Primary operational purpose
├── has_tiers            BOOLEAN DEFAULT FALSE      -- Whether tier analysis covers this reservoir
├── is_main              BOOLEAN DEFAULT FALSE      -- Whether this is a primary (major) reservoir
├── has_gis_data         INTEGER DEFAULT 1          -- Whether GIS data exists (1=yes, 0=no)
├── entity_version_id    INTEGER NOT NULL DEFAULT 1 -- FK to version.id (entity family)
├── source_ids           TEXT                       -- Comma-separated source IDs
├── is_active            BOOLEAN DEFAULT TRUE       -- Soft delete flag
├── created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL DEFAULT 1 -- FK to developer.id
├── updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL DEFAULT 1 -- FK to developer.id

Indexes:
├── idx_reservoir_entity_short_code (short_code)
└── idx_reservoir_entity_region (hydrologic_region_id)

Comments:
├── Table: Reservoir management entities with capacity and operational attributes. Part of ENTITY LAYER.
├── short_code: Short identifier (SHSTA, OROVL, etc.) - matches network.short_code
├── capacity_taf: Maximum reservoir capacity in thousand acre-feet (TAF)
├── dead_pool_taf: Dead pool storage in TAF - unusable storage at bottom
└── hydrologic_region_id: FK to hydrologic_region: 1=SAC(NOD), 2=SJR(SOD), 4=Tulare(SOD)
```

#### **inflow_entity (inflow management)**

Status: PLANNED — not yet created in the database.

```
Table: inflow_entity   [PLANNED]
├── id                   SERIAL PRIMARY KEY
├── network_arc_id       INTEGER NOT NULL           -- FK to network.id (inflow arc)
├── short_code           VARCHAR UNIQUE NOT NULL
├── name                 VARCHAR
├── description          TEXT
├── to_node_id           INTEGER                    -- FK to network.id (specific to entity role)
├── entity_type_id       INTEGER NOT NULL           -- FK to calsim_entity_type.id
├── entity_version_id    INTEGER NOT NULL           -- FK to version.id
├── attribute_source     JSONB NOT NULL             -- {"name": "entity_system", "to_node_id": "operational"}
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK to developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK to developer.id
```

#### **du_urban_entity (community demand unit management)**
```
Table: du_urban_entity
├── id                   SERIAL PRIMARY KEY
├── du_id                VARCHAR(20) UNIQUE NOT NULL -- Demand unit identifier (e.g., "16_PU", "AMCYN")
├── wba_id               VARCHAR(10)                -- Water Budget Area ID
├── hydrologic_region    VARCHAR(10)                -- SAC, SJR, TULARE
├── hydrologic_region_id INTEGER                    -- FK hydrologic_region.id
├── dups                 VARCHAR(10)                -- Duplicate indicator
├── du_class             VARCHAR DEFAULT 'Urban'
├── cs3_type             VARCHAR(10)                -- NU, PU, SU (Non-project, Project, Settlement Urban)
├── total_acres          NUMERIC(15,10)
├── polygon_count        INTEGER DEFAULT 0
├── community_agency     TEXT                       -- Community/agency description
├── gw                   VARCHAR(10)                -- Groundwater indicator (0/1)
├── sw                   VARCHAR(10)                -- Surface water indicator (0/1)
├── point_of_diversion   TEXT                       -- Water source description
├── source               VARCHAR(50)                -- Data source (geopackage, calsim_report, tier_matrix)
├── model_source         VARCHAR(20) DEFAULT 'calsim3'
├── model_source_id      INTEGER                    -- FK model_source.id
├── has_gis_data         BOOLEAN DEFAULT FALSE
├── primary_contractor_short_code VARCHAR(20)       -- FK mi_contractor.short_code (for SWP-served units)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMPTZ DEFAULT NOW()
├── created_by           INTEGER DEFAULT 1          -- FK developer.id
├── updated_at           TIMESTAMPTZ DEFAULT NOW()
└── updated_by           INTEGER DEFAULT 1

Records: 145 urban demand units (107 original + 19 tier matrix additions + extras)

Relationships:
├── du_urban_group_member.du_id to du_urban_entity.du_id (group memberships)
└── primary_contractor_short_code to mi_contractor.short_code (optional SWP contractor link)

DDL: database/scripts/sql/12_mi_statistics/01_create_du_urban_entity.sql
Seed: s3://coeqwal-seeds-dev/04_calsim_data/du_urban_entity.csv
```

#### **du_agriculture_entity (agriculture demand unit management)**
```
Table: du_agriculture_entity
├── id                   SERIAL PRIMARY KEY
├── du_id                VARCHAR UNIQUE NOT NULL    -- Demand unit identifier
├── wba_id               VARCHAR(10)                -- Water Budget Area ID
├── hydrologic_region    VARCHAR(10)                -- SAC, SJR, TULARE
├── hydrologic_region_id INTEGER                    -- FK hydrologic_region.id
├── dups                 VARCHAR(10)                -- Duplicate indicator
├── du_class             VARCHAR DEFAULT 'Agriculture'
├── cs3_type             VARCHAR(10)                -- CalSim3 demand unit type
├── total_acres          NUMERIC(15,10)
├── polygon_count        INTEGER DEFAULT 1
├── source               VARCHAR(50)                -- Data source
├── model_source         VARCHAR(20) DEFAULT 'calsim3'
├── model_source_id      INTEGER                    -- FK model_source.id
├── agency               TEXT                       -- Water agency
├── provider             TEXT                       -- Water provider
├── gw                   VARCHAR(10)                -- Groundwater indicator
├── sw                   VARCHAR(10)                -- Surface water indicator
├── point_of_diversion   TEXT                       -- Water source description
├── diversion_arc        TEXT                       -- CalSim diversion arc variable
├── river_reach          TEXT                       -- River reach identifier
├── river_mile_start     NUMERIC                    -- Start river mile
├── river_mile_end       NUMERIC                    -- End river mile
├── bank                 TEXT                       -- River bank (left/right)
├── area_acres           NUMERIC                    -- Geographic area in acres
├── annual_diversion_taf NUMERIC                    -- Annual diversion volume
├── demand_unit          TEXT                       -- Demand unit description
├── table_id             TEXT                       -- Source table identifier
├── has_gis_data         BOOLEAN DEFAULT FALSE
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMPTZ DEFAULT NOW()
├── created_by           INTEGER NOT NULL DEFAULT 1 -- FK developer.id
├── updated_at           TIMESTAMPTZ DEFAULT NOW()
└── updated_by           INTEGER NOT NULL DEFAULT 1

Records: 144 agricultural demand units
```

#### **du_refuge_entity (refuge demand unit management)**

Seed file: `database/seed_tables/04_calsim_data/du_refuge_entity.csv` (18 rows)
Migration: `database/scripts/sql/migrations/20_create_refuge_entity_table.sql`

Statistics tables: `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary`
(see Layer 09+ — STATISTICS / RESULTS section above)

Source: CalSim 3 Main Report Tables 3-9 (SAC region) and 3-10 (SJR/Tulare region).
`gw` and `sw` flags indicate whether the demand unit has access to groundwater and surface water
respectively, as defined in the report, and should be surfaced in the frontend interface.

```
Table: du_refuge_entity
├── id                            SERIAL PRIMARY KEY
├── du_id                         VARCHAR(20) UNIQUE NOT NULL    -- e.g. 08N_PR1, 91_PR
├── wba_id                        VARCHAR(10)                    -- Water Budget Area ID
├── hydrologic_region             VARCHAR(20) NOT NULL           -- SAC, SJR, TULARE
├── dups                          INTEGER                        -- -1 = aggregated DU, 0 = single unit
├── du_class                      VARCHAR(50) DEFAULT 'Refuge'
├── cs3_type                      VARCHAR(10)                    -- PR = Project Refuge (CVP), NR = Non-project Refuge
├── total_acres                   NUMERIC(14,4)
├── polygon_count                 INTEGER DEFAULT 1
├── refuge_or_wildlife_area       TEXT                           -- Refuge name(s) within this DU
├── managed_by                    VARCHAR(200)                   -- Managing agency: USFWS, CDFW, Private, …
├── provider                      VARCHAR(200)                   -- Water provider/contractor (blank = drainage-supplied)
├── gw                            BOOLEAN NOT NULL DEFAULT FALSE -- Access to groundwater (from CalSim 3 report)
├── sw                            BOOLEAN NOT NULL DEFAULT TRUE  -- Access to surface water (from CalSim 3 report)
├── point_of_diversion_conveyance TEXT                          -- Point of diversion description
├── source                        VARCHAR(100)                   -- e.g. geopackage,calsim_report
├── model_source                  VARCHAR(50) DEFAULT 'calsim3'
├── has_gis_data                  BOOLEAN DEFAULT TRUE
├── is_active                     BOOLEAN NOT NULL DEFAULT TRUE
├── created_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by                    INTEGER NOT NULL DEFAULT 1     -- FK to developer.id
├── updated_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by                    INTEGER NOT NULL DEFAULT 1     -- FK to developer.id

Foreign keys:
├── Ref: du_refuge_entity.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: du_refuge_entity.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── du_refuge_entity_pkey (id)
├── du_refuge_entity_du_id_key (du_id)
├── idx_du_refuge_entity_hydrologic_region (hydrologic_region)
└── idx_du_refuge_entity_cs3_type (cs3_type)
```

### **Entity Grouping Tables**

#### **reservoir_group (reservoir subset definitions)**
```
Table: reservoir_group
├── id                    SERIAL PRIMARY KEY
├── short_code            VARCHAR(50) UNIQUE NOT NULL -- "major_8", "cvp_primary", "swp_primary", "tier_analysis"
├── label                 VARCHAR NOT NULL           -- "8 Major Reservoirs"
├── description           TEXT                       -- Purpose of this grouping
├── display_order         INTEGER DEFAULT 0          -- For UI ordering
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK to developer.id (system)
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Foreign keys:
├── Ref: reservoir_group.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: reservoir_group.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── reservoir_group_pkey (id)
└── reservoir_group_short_code_key (short_code)

Values:
├── major: Major Reservoirs - Primary CVP/SWP storage reservoirs for statistics dashboards
├── cvp: CVP Storage - Central Valley Project reservoirs
├── swp: SWP Storage - State Water Project reservoirs
└── tier: Tier Reservoirs - Reservoirs included in tier result analysis

Note: Regional aggregation (NOD/SOD) uses reservoir_entity.hydrologic_region_id:
├── NOD (North of Delta): hydrologic_region_id = 1 (Sacramento)
└── SOD (South of Delta): hydrologic_region_id IN (2, 4) (San Joaquin, Tulare)
```

#### **reservoir_group_member (reservoir-to-group junction)**
```
Table: reservoir_group_member
├── id                    SERIAL PRIMARY KEY
├── reservoir_group_id    INTEGER NOT NULL           -- FK to reservoir_group.id
├── reservoir_entity_id   INTEGER NOT NULL           -- FK to reservoir_entity.id
├── display_order         INTEGER DEFAULT 0          -- Order within group for UI
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK to developer.id (system)
├── updated_at            TIMESTAMPTZ DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Foreign keys:
├── Ref: reservoir_group_member.reservoir_group_id > reservoir_group.id [delete: cascade, update: cascade]
├── Ref: reservoir_group_member.reservoir_entity_id > reservoir_entity.id [delete: cascade, update: cascade]
├── Ref: reservoir_group_member.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: reservoir_group_member.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── reservoir_group_member_pkey (id)
├── uq_reservoir_group_member (reservoir_group_id, reservoir_entity_id)
├── idx_reservoir_group_member_group (reservoir_group_id)
└── idx_reservoir_group_member_reservoir (reservoir_entity_id)

Constraints:
└── Unique: (reservoir_group_id, reservoir_entity_id)

Example memberships (reservoirs can be in multiple groups):

major group (id=1):
├── SHSTA (66), TRNTY (79), OROVL (56), FOLSM (26)
├── MELON (49), MLRTN (51), SLUIS_CVP (70), SLUIS_SWP (71)

cvp group (id=2):
├── SHSTA (66), TRNTY (79), FOLSM (26), MELON (49), MLRTN (51), SLUIS_CVP (70)

swp group (id=3):
├── OROVL (56), SLUIS_SWP (71)

tier group (id=4):
└── Same as major group

---

## **VIEWS**

### **scenario_full**

Wide, human-readable view of **active** scenario configurations (`is_active = TRUE` only).
Pivots the normalized `scenario_key_operation_link` and `scenario_key_assumption_link`
junction tables into named columns grouped by category. Each row represents one
active scenario. To query inactive scenarios, query the `scenario` table directly.

```
View: scenario_full
Filter: WHERE scenario.is_active = TRUE
├── id                   INTEGER                    -- scenario.id (for ordering)
├── short_code           TEXT                       -- e.g. "s0011"
├── run_name             TEXT                       -- e.g. "s0011_adjBL_wTUCP"
├── name                 TEXT                       -- e.g. "DWR Historical Adjusted Baseline with TUCPs"
├── is_active            BOOLEAN
├── author               TEXT                       -- scenario_author.short_code
├── hydroclimate         TEXT                       -- hydroclimate.short_code
│
│   ── operations (one column per operation_category, NULL if not linked) ──
├── biops                TEXT                       -- e.g. "biops_standard"
├── tucp                 TEXT                       -- e.g. "TUCP_TUCO"
├── gw_restrictions      TEXT                       -- e.g. "gw_none", "SGMA_CV"
├── infrastructure       TEXT                       -- e.g. "infra_standard"
├── flow                 TEXT                       -- e.g. "flow_standard"
├── delta_outflow        TEXT                       -- e.g. "delta_regs_standard"
├── comm_delivery        TEXT                       -- e.g. "alloc_standard"
├── regulatory_salinity  TEXT                       -- NULL for most scenarios
├── carryover            TEXT                       -- NULL for most scenarios
│
│   ── assumptions (one column per assumption_category, NULL if not linked) ──
├── land_use             TEXT                       -- e.g. "lu_2020_landiq"
└── gw_model             TEXT                       -- NULL until gw_model links added

NULL in a category column = no link for that category
(e.g. s0046 has no delta_outflow; most scenarios have no regulatory_salinity)

Source: Recreated in migrations 42 and 43 (current definition).
        Originally created in migration 17, filtered in 18.
```

---

### **refuge_du_full**

Denormalized, human-readable view of **active** wildlife refuge demand units.
Decodes `cs3_type` into a plain-language label (`PR` to `Project Refuge`, `NR` to `Non-project Refuge`).
Use this view for API responses and frontend attribute panels. The `gw` and `sw` columns
should be surfaced in the frontend (tooltip or attribute panel).

```
View: refuge_du_full
Filter: WHERE du_refuge_entity.is_active = TRUE
├── du_id                         TEXT     -- e.g. "08N_PR1"
├── wba_id                        TEXT     -- Water Budget Area ID
├── hydrologic_region             TEXT     -- SAC, SJR, TULARE
├── cs3_type                      TEXT     -- PR, NR (raw)
├── cs3_type_label                TEXT     -- "Project Refuge" or "Non-project Refuge"
├── refuge_or_wildlife_area       TEXT     -- Refuge name(s) within this DU
├── managed_by                    TEXT     -- Managing agency: USFWS, CDFW, Private, …
├── provider                      TEXT     -- Water provider/contractor (NULL = drainage-supplied)
├── gw                            BOOLEAN  -- Access to groundwater (surface in frontend)
├── sw                            BOOLEAN  -- Access to surface water (surface in frontend)
├── total_acres                   NUMERIC
├── polygon_count                 INTEGER
├── point_of_diversion_conveyance TEXT     -- Point of diversion description
└── has_gis_data                  BOOLEAN

Source: database/scripts/sql/migrations/20_create_refuge_entity_table.sql
```

---

### **env_flow_channel_full**

Denormalized, human-readable view of **active** channel entities with watershed linkage
and environmental flow attributes. Use for API responses and frontend channel selectors.
Analogous to `refuge_du_full` — one row per channel; stats queried separately by
`network_arc_id × scenario_short_code`.

```
View: env_flow_channel_full
Filter: WHERE channel_entity.is_active = TRUE
├── network_arc_id       TEXT     -- e.g. "C_SAC049"
├── label                TEXT     -- channel_entity.short_code (human label)
├── channel_class        TEXT     -- 'stream', 'canal', or 'reservoir_release'
├── channel_class_label  TEXT     -- 'Natural stream or river reach', etc.
├── watershed_short_code TEXT     -- FK to watershed.short_code
├── watershed_name       TEXT     -- watershed.name
├── hydrologic_region    TEXT     -- hydrologic_region.short_code (via watershed.hydrologic_region_id)
├── unimp_sv_variable    TEXT     -- CalSim SV unimpaired baseline variable
├── has_mif              BOOLEAN  -- TRUE if C_*_MIF companion variable exists
├── has_eflows           BOOLEAN  -- TRUE if EFLOWS_* functional flow target exists
├── from_node            TEXT     -- upstream node
├── to_node              TEXT     -- downstream node
├── hydrologic_region_id TEXT     -- channel_entity.hydrologic_region_id
└── is_active            BOOLEAN

Source: database/scripts/sql/migrations/25_env_flow_audit_and_views.sql
```