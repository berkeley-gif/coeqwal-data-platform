# COEQWAL SCENARIOS DATABASE ERD

## **ARCHITECTURE OVERVIEW**

### **Database Layer Structure:**
```
00_VERSIONING SYSTEM
├── Version families and instances
├── Developer management and SSO
├── Domain-to-version mappings
└── Purpose: track all data versions and changes

01_LOOKUP TABLES (reference data)
├── Geographic regions and scales
├── Data sources and model sources  
├── Units, statistics, and geometry types
└── Purpose: provide consistent reference values

05_THEMES_SCENARIOS LAYER
├── Research themes (storylines for exploring scenarios)
├── Scenarios (water management configurations)
├── Theme-scenario relationships
├── Scenario authors and sources
└── Purpose: organize and describe what-if scenarios

06_ASSUMPTIONS_OPERATIONS LAYER
├── Assumption definitions and categories
├── Operation definitions and categories
├── Scenario-assumption and scenario-operation links
└── Purpose: define inputs and rules for each scenario

07_HYDROCLIMATE LAYER
├── Hydroclimate conditions (historical, projected)
├── Climate projections and sea level rise
└── Purpose: define environmental boundary conditions

09_STATISTICS LAYER
├── Outcome categories (types of outcomes being measured)
├── Outcome statistics (types of statistics per category)
├── Variable prefixes (S_, C_, I_, E_, D_, etc.)
├── Reservoir variables (CalSim storage/release variables linked to entities)
├── Reservoir monthly percentiles (storage distribution by water month)
├── Reservoir storage monthly (storage statistics for all 92 reservoirs)
├── Reservoir spill monthly (spill/flood release statistics)
├── Reservoir period summary (period-of-record spill metrics)
└── Purpose: pre-calculated statistics for frontend visualization

10_TIER LAYER
├── Tier definitions (outcome indicators)
├── Tier results (aggregated scenario outcomes)
├── Tier location results (spatially-detailed outcomes)
└── Purpose: evaluate and compare scenario performance

NETWORK LAYER (infrastructure/physical)
├── Master registry of all physical network elements
├── Spatial data and engineering attributes
├── Multi-source connectivity (geopackage, XML, CalSim)
└── Purpose: what exists physically and how is it connected?

ENTITY LAYER (management/operational)  
├── Management and operational perspectives on network elements
└── Purpose: how are network elements used and managed?
```

## **00_VERSIONING SYSTEM TABLES**

### **1. version_family (version categories)**
```
Table: version_family
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "theme", "scenario", "network", "entity", etc.
├── label                TEXT                       -- "Theme", "Scenario", "Network", "Entity", etc.
├── description          TEXT                       -- Purpose description
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Values (13 total):
├── theme: Research themes and storylines
├── scenario: Water management scenarios
├── assumption: Scenario assumptions and parameters
├── operation: Operational policies and rules
├── hydroclimate: Hydroclimate conditions and projections
├── variable: CalSim model variables and definitions
├── outcome: Outcome categories and measurement systems
├── tier: Tier definitions and classification systems
├── geospatial: Geographic and spatial data definitions
├── interpretive: Analysis and interpretive frameworks
├── metadata: Data metadata and documentation
├── network: CalSim network topology and connectivity
└── entity: Entity version family for tracking entity data versions

Indexes:
└── version_family_short_code_key (short_code) -- For version family lookups
```

### **2. version (version instances)**
```
Table: version
├── id                   SERIAL PRIMARY KEY
├── version_family_id    INTEGER NOT NULL           -- FK → version_family.id
├── version_number       TEXT                       -- "1.0.0" (semantic versioning)
├── manifest             JSONB                      -- Version metadata
├── changelog            TEXT                       -- Change description
├── is_active            BOOLEAN DEFAULT FALSE      -- Only one active per family
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Indexes:
├── version_version_family_id_version_number_key (version_family_id, version_number)
└── idx_version_family (version_family_id) -- FK performance
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
└── updated_at           TIMESTAMP DEFAULT NOW()
```

### **4. domain_family_map (table-to-version mapping)**
```
Table: domain_family_map
├── schema_name          TEXT NOT NULL              -- "public"
├── table_name           TEXT NOT NULL              -- Table name
├── version_family_id    INTEGER NOT NULL           -- FK → version_family.id
└── note                 TEXT                       -- Purpose note
```

## **01_LOOKUP TABLES**

### **1. hydrologic_region**
```
Table: hydrologic_region
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "SAC", "SJR", "DELTA", "TULARE", "SOCAL", "EXTERNAL"
├── label                TEXT                       -- "Sacramento River Basin", etc.
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Values (6 total):
├── SAC: Sacramento River Basin
├── SJR: San Joaquin River Basin  
├── DELTA: Sacramento–San Joaquin Delta
├── TULARE: Tulare Basin
├── SOCAL: Southern California
└── EXTERNAL: External areas

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
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Values (9 total):
├── calsim_report: CalSim-3 report final.pdf
├── james_gilbert: James Gilbert
├── calsim_variables: CalSim variables from output and sv data
├── geopackage: CalSim3_GeoSchematic_20221227_COEQWAL_Revisions2024_corrected.gpkg
├── trend_report: Variables extracted from Gilbert team trend reports
├── metadata: Scenario metadata
├── cvm_docs: Central Valley Model documentation
├── network_schematic: Network schematic
└── manual: Manual insertion
```

### **3. model_source**
```
Table: model_source
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "calsim3"
├── name                 TEXT UNIQUE NOT NULL       -- "CalSim3"
├── version_family_id    INTEGER NOT NULL           -- FK → version_family.id (variable family)
├── description          TEXT                       -- Model description
├── contact              TEXT                       -- Contact information
├── notes                TEXT                       -- Additional notes
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

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
└── is_active            BOOLEAN DEFAULT TRUE

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
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

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
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Values (8 total):
├── daily: Daily
├── weekly: Weekly
├── monthly: Monthly
└── ... (5 more scales)
```

### **7. statistic_type**
```
Table: statistic_type
├── id                   SERIAL PRIMARY KEY
├── code                 TEXT UNIQUE NOT NULL       -- "MEAN", "MEDIAN", "MIN", "MAX", etc.
├── name                 TEXT NOT NULL              -- "Mean", "Median", etc.
├── description          TEXT                       -- Statistic description
├── is_percentile        BOOLEAN DEFAULT FALSE      -- Whether this is a percentile measure
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Values (6 total):
├── MEAN: Mean (Average value)
├── MEDIAN: Median (50th percentile)
├── MIN: Minimum (Minimum value)
└── ... (3 more statistics)
```

### **8. unit**
```
Table: unit
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "TAF", "CFS", "acres", etc.
├── full_name            TEXT                       -- "thousand acre-feet", etc.
├── canonical_group      TEXT                       -- "volume", "flow", "area", etc.
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Values (5 total):
├── TAF: thousand acre-feet (volume)
├── CFS: cubic feet per second (flow)
├── acres: acres (area)
└── ... (2 more units)
```

### **9. watersheds (watershed regions)**

```
Table: watersheds
├── id                              SERIAL PRIMARY KEY
├── short_code                      VARCHAR UNIQUE NOT NULL    -- Watershed identifier (UPPER_AMERICAN, SAC_RIVER)
├── name                            VARCHAR NOT NULL           -- Full watershed name (Upper American River Watershed)
├── description                     TEXT                       -- Watershed description
├── hydrologic_region_short_code    VARCHAR                    -- Connection to hydrologic regions (SAC, SJR)
├── is_active                       BOOLEAN DEFAULT TRUE
├── created_at                      TIMESTAMP DEFAULT NOW()
├── created_by                      INTEGER NOT NULL           -- FK → developer.id
├── updated_at                      TIMESTAMP DEFAULT NOW()
└── updated_by                      INTEGER NOT NULL           -- FK → developer.id

Records: 9 watersheds from CalSim report

Foreign keys:
├── Ref: watersheds.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: watersheds.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── watersheds_short_code_key (short_code) -- Unique constraint
└── idx_watersheds_hydrologic_region (hydrologic_region_short_code) -- Region lookups

Values (9 total):
├── BEAR_RIVER: Bear River Watershed
├── SAC_RIVER: Sacramento River Hydrologic Region
├── SAN_JOAQUIN: San Joaquin River Hydrologic Region
├── UPPER_AMERICAN: Upper American River Watershed
├── UPPER_FEATHER: Upper Feather River Watershed
├── UPPER_MOKELUMNE: Upper Mokelumne River Watershed
├── UPPER_STANISLAUS: Upper Stanislaus River
├── UPPER_TUOLUMNE: Upper Tuolumne River Watershed
└── YUBA_RIVER: Yuba River Watershed
```

---

## **05_THEMES_SCENARIOS LAYER**

### **1. theme (research themes)**

```
Table: theme
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "baseline", "community_water", "flow", etc.
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── name                 TEXT NOT NULL              -- "Current operations for California water"
├── subtitle             TEXT
├── short_title          TEXT                       -- "Current operations"
├── simple_description   TEXT
├── description          TEXT
├── description_next     TEXT
├── narrative            JSONB                      -- Structured narrative data
├── outcome_description  TEXT
├── outcome_narrative    TEXT
├── theme_version_id     INTEGER NOT NULL DEFAULT 1 -- FK → version.id (theme family)
├── created_by           INTEGER NOT NULL DEFAULT 1 -- FK → developer.id
└── updated_by           INTEGER                    -- FK → developer.id

Records: 7 themes

Foreign keys:
├── Ref: theme.theme_version_id > version.id [delete: restrict, update: cascade]
├── Ref: theme.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: theme.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── theme_short_code_key (short_code) -- Unique constraint
├── idx_theme_short_code_active (short_code, is_active)
└── idx_theme_active (is_active)

Values (7 total):
├── baseline: Current operations for California water
├── community_water: Prioritizing community water systems
├── flow: Enhancing river flows for the environment
├── gw_ag: Managing groundwater in a changing agricultural landscape
├── delta_flow: Improving flows for the health of the Bay Delta estuary
├── delta_uses: Sustaining uses in the Delta for communities and farms
└── delta_export_reliability: Improving reliability of Delta exports for farms and cities
```

### **2. scenario (water management scenarios)**

```
Table: scenario
├── id                   SERIAL PRIMARY KEY
├── scenario_id          TEXT NOT NULL UNIQUE       -- Friendly identifier like "s0011"
├── short_code           TEXT NOT NULL              -- Full technical code like "s0011_adjBL_wTUCP"
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── name                 TEXT NOT NULL              -- "Baseline - adjusted with TUCP/TUCO"
├── subtitle             TEXT
├── short_title          TEXT
├── simple_description   TEXT
├── description          TEXT
├── narrative            JSONB                      -- Structured narrative data
├── baseline_scenario_id INTEGER                    -- FK → scenario.id (self-referencing, NULL for baseline scenarios)
├── hydroclimate_id      INTEGER                    -- FK → hydroclimate.id
├── scenario_author_id   INTEGER                    -- FK → scenario_author.id
├── scenario_version_id  INTEGER NOT NULL           -- FK → version.id (scenario family)
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Records: 8 scenarios (s0011, s0020, s0021, s0023, s0024, s0025, s0027, s0029)

Foreign keys:
├── Ref: scenario.baseline_scenario_id > scenario.id [delete: set null, update: cascade]
├── Ref: scenario.hydroclimate_id > hydroclimate.id [delete: restrict, update: cascade]
├── Ref: scenario.scenario_author_id > scenario_author.id [delete: restrict, update: cascade]
├── Ref: scenario.scenario_version_id > version.id [delete: restrict, update: cascade]
├── Ref: scenario.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: scenario.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── scenario_scenario_id_key (scenario_id) -- Unique constraint
├── idx_scenario_short_code_active (short_code, is_active)
├── idx_scenario_active (is_active)
├── idx_scenario_baseline (baseline_scenario_id)
├── idx_scenario_hydroclimate (hydroclimate_id)
└── idx_scenario_active_version (is_active, scenario_version_id)

Baseline Derivations:
├── s0011 (id=1): baseline_scenario_id = 1 (self, is the baseline)
├── s0020 (id=2): baseline_scenario_id = 2 (self, is a baseline)
├── s0021 (id=3): baseline_scenario_id = 2 (derived from s0020)
├── s0023 (id=4): baseline_scenario_id = NULL (no baseline yet)
├── s0024 (id=5): baseline_scenario_id = NULL (no baseline yet)
├── s0025 (id=6): baseline_scenario_id = 2 (derived from s0020)
├── s0027 (id=7): baseline_scenario_id = 2 (derived from s0020)
└── s0029 (id=8): baseline_scenario_id = 2 (derived from s0020)
```

### **3. theme_scenario_link (many-to-many relationship)**

```
Table: theme_scenario_link
├── theme_id             INTEGER NOT NULL           -- FK → theme.id
└── scenario_id          INTEGER NOT NULL           -- FK → scenario.id

Primary key: (theme_id, scenario_id)

Foreign keys:
├── Ref: theme_scenario_link.theme_id > theme.id [delete: restrict, update: cascade]
└── Ref: theme_scenario_link.scenario_id > scenario.id [delete: restrict, update: cascade]

Indexes:
├── theme_scenario_link_pkey (theme_id, scenario_id) -- Primary key
└── idx_theme_scenario_reverse (scenario_id, theme_id) -- Reverse lookup

Records: 8 links
```

### **4. scenario_author (scenario authors/groups)**

```
Table: scenario_author
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "UC_DAVIS", "DWR", etc.
├── name                 TEXT NOT NULL              -- "UC Davis Center for Watershed Sciences"
├── email                TEXT
├── organization         TEXT
├── affiliation          TEXT
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Foreign keys:
├── Ref: scenario_author.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: scenario_author.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── scenario_author_short_code_key (short_code) -- Unique constraint
└── idx_scenario_author_active (is_active, short_code)

Records: 3 authors
```

---

## **06_ASSUMPTIONS_OPERATIONS LAYER**

### **1. assumption_category (assumption categories)**

```
Table: assumption_category
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "hydrology", "land_use", "regulations", etc.
├── label                TEXT NOT NULL              -- "Hydrology", "Land Use", "Regulations"
├── description          TEXT
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Foreign keys:
├── Ref: assumption_category.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: assumption_category.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── assumption_category_short_code_key (short_code)
└── idx_assumption_category_active (is_active, short_code)

Values (8 total):
├── hydrology: Hydrology
├── hydroclimate: Hydroclimate
├── land_use: Land Use
├── future_land_use: Future Land Use
├── gw_model: Groundwater Model
├── regulations: Regulations
├── demand: Demand
└── infrastructure: Infrastructure

Records: 8 categories
```

### **2. assumption_definition (assumption definitions)**

```
Table: assumption_definition
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "tucp_tuco", "land_use_2030", etc.
├── name                 TEXT NOT NULL              -- "Temporary Urgency Change Petition / Order"
├── short_title          TEXT
├── subtitle             TEXT
├── simple_description   TEXT
├── description          TEXT
├── narrative            JSONB
├── category_id          INTEGER                    -- FK → assumption_category.id (nullable for flexibility)
├── source               TEXT                       -- Source citation
├── source_access_date   DATE                       -- When source was accessed
├── file                 TEXT                       -- Associated file path/name
├── assumptions_version_id INTEGER NOT NULL         -- FK → version.id (assumptions family)
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── notes                TEXT
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Foreign keys:
├── Ref: assumption_definition.category_id > assumption_category.id [delete: restrict, update: cascade]
├── Ref: assumption_definition.assumptions_version_id > version.id [delete: restrict, update: cascade]
├── Ref: assumption_definition.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: assumption_definition.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── assumption_definition_short_code_key (short_code)
├── idx_assumption_definition_category (category_id, short_code)
└── idx_assumption_definition_active (is_active)

Records: 17 assumption definitions
```

### **3. scenario_key_assumption_link (scenario-assumption relationships)**

```
Table: scenario_key_assumption_link
├── scenario_id          INTEGER NOT NULL           -- FK → scenario.id
└── assumption_id        INTEGER NOT NULL           -- FK → assumption_definition.id

Primary key: (scenario_id, assumption_id)

Foreign keys:
├── Ref: scenario_key_assumption_link.scenario_id > scenario.id [delete: restrict, update: cascade]
└── Ref: scenario_key_assumption_link.assumption_id > assumption_definition.id [delete: restrict, update: cascade]

Indexes:
├── scenario_key_assumption_link_pkey (scenario_id, assumption_id)
└── idx_scenario_assumption_reverse (assumption_id, scenario_id)
```

### **4. operation_category (operation categories)**

```
Table: operation_category
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "allocation", "regulatory", "infrastructure"
├── name                 TEXT                       -- "Allocation", "Regulatory", "Infrastructure"
├── description          TEXT
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Foreign keys:
├── Ref: operation_category.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: operation_category.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── operation_category_short_code_key (short_code)
└── idx_operation_category_active (is_active, short_code)

Values (4 total):
├── allocation: Allocation
├── carryover: Carryover Storage
├── infrastructure: Infrastructure
└── regulatory: Regulatory

Records: 4 categories
```

### **5. operation_definition (operation definitions)**

```
Table: operation_definition
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "priority_allocation", "min_flow_req", etc.
├── name                 TEXT NOT NULL              -- "Priority Allocation Rules"
├── short_title          TEXT
├── subtitle             TEXT
├── simple_description   TEXT
├── description          TEXT
├── narrative            JSONB
├── category_id          INTEGER                    -- FK → operation_category.id (nullable for flexibility)
├── is_active            BOOLEAN NOT NULL DEFAULT TRUE
├── notes                TEXT
├── operation_version_id INTEGER NOT NULL           -- FK → version.id (operations family)
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Foreign keys:
├── Ref: operation_definition.category_id > operation_category.id [delete: restrict, update: cascade]
├── Ref: operation_definition.operation_version_id > version.id [delete: restrict, update: cascade]
├── Ref: operation_definition.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: operation_definition.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── operation_definition_short_code_key (short_code)
├── idx_operation_definition_category (category_id, short_code)
└── idx_operation_definition_active (is_active)

Records: 10 operation definitions
```

### **6. scenario_key_operation_link (scenario-operation relationships)**

```
Table: scenario_key_operation_link
├── scenario_id          INTEGER NOT NULL           -- FK → scenario.id
└── operation_id         INTEGER NOT NULL           -- FK → operation_definition.id

Primary key: (scenario_id, operation_id)

Foreign keys:
├── Ref: scenario_key_operation_link.scenario_id > scenario.id [delete: restrict, update: cascade]
└── Ref: scenario_key_operation_link.operation_id > operation_definition.id [delete: restrict, update: cascade]

Indexes:
├── scenario_key_operation_link_pkey (scenario_id, operation_id)
└── idx_scenario_operation_reverse (operation_id, scenario_id)
```

---

## **07_HYDROCLIMATE LAYER**

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
├── slr_value            NUMERIC                    -- Sea level rise value
├── slr_unit_id          INTEGER                    -- FK → unit.id
├── source_id            INTEGER                    -- FK → hydroclimate_source.id
├── notes                TEXT
├── hydroclimate_version_id INTEGER                 -- FK → version.id (hydroclimate family)
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Foreign keys:
├── Ref: hydroclimate.hydroclimate_version_id > version.id [delete: restrict, update: cascade]
├── Ref: hydroclimate.source_id > hydroclimate_source.id [delete: restrict, update: cascade]
├── Ref: hydroclimate.slr_unit_id > unit.id [delete: restrict, update: cascade]
├── Ref: hydroclimate.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: hydroclimate.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── hydroclimate_short_code_key (short_code) -- Unique constraint
├── idx_hydroclimate_active (is_active, short_code)
└── idx_hydroclimate_source (source_id)

Values (7 total):
├── historical: Historical (1922-2021)
├── 2040_central: 2040 Central Tendency
├── 2040_dry: 2040 Dry Extreme
├── 2040_wet: 2040 Wet Extreme
├── 2070_central: 2070 Central Tendency
├── 2070_dry: 2070 Dry Extreme
└── 2070_wet: 2070 Wet Extreme

Records: 7 hydroclimate conditions
```

### **2. hydroclimate_source (hydroclimate data sources)**

```
Table: hydroclimate_source
├── id                   SERIAL PRIMARY KEY
├── short_code           TEXT UNIQUE NOT NULL       -- "dwr_cctag", "usgs", etc.
├── name                 TEXT                       -- "DWR Climate Change Technical Advisory Group"
├── description          TEXT
├── citation             TEXT
├── url                  TEXT
├── notes                TEXT
├── created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Foreign keys:
├── Ref: hydroclimate_source.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: hydroclimate_source.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
└── hydroclimate_source_short_code_key (short_code)
```

---

## **09_STATISTICS LAYER**

Pre-calculated statistics and outcome metrics derived from scenario model runs. Provides aggregated data for frontend visualization (percentile bands, time series summaries).

### **1. outcome_category (outcome types)**

```
Table: outcome_category
├── id                    SERIAL PRIMARY KEY
├── short_code            TEXT UNIQUE NOT NULL       -- "reservoir_storage", "groundwater_storage", etc.
├── label                 TEXT                       -- "Reservoir Storage", etc.
├── description           TEXT                       -- Detailed description
├── outcome_version_id    INTEGER NOT NULL           -- FK → version.id (outcome family, version_family_id=7)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK → developer.id (1 = system)
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

```
Table: variable_prefix
├── id                    SERIAL PRIMARY KEY
├── prefix                VARCHAR(10) UNIQUE NOT NULL -- "S", "C", "I", "E", "D", "A", "X", etc.
├── label                 VARCHAR NOT NULL           -- "Storage", "Channel Flow", "Inflow", etc.
├── description           TEXT                       -- What this variable type represents
├── unit_id               INTEGER                    -- FK → unit.id (default unit for prefix)
├── applies_to_entity     TEXT[]                     -- ["reservoir", "channel", "node", "demand_unit"]
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK → developer.id (system)
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

```
Table: outcome_statistic
├── id                    SERIAL PRIMARY KEY
├── outcome_category_id   INTEGER NOT NULL           -- FK → outcome_category.id
├── short_code            VARCHAR(50) NOT NULL       -- "monthly_percentile", "annual_exceedance", etc.
├── label                 VARCHAR NOT NULL           -- "Monthly Percentile Bands"
├── description           TEXT                       -- What this statistic measures
├── variable_prefix_id    INTEGER                    -- FK → variable_prefix.id (e.g., "S" for storage)
├── percentile_scheme     TEXT[]                     -- ['p0','p10','p30','p50','p70','p90','p100']
├── time_resolution       VARCHAR(20)                -- "monthly", "annual", "daily"
├── unit                  VARCHAR(50)                -- "percent_capacity", "taf", "cfs"
├── data_table            VARCHAR(100)               -- "reservoir_monthly_percentile" (target table)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK → developer.id (system)
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
│       variable_prefix_id → "S" (storage)
│       percentile_scheme=['p0','p10','p30','p50','p70','p90','p100']
│       time_resolution="monthly"
│       unit="percent_capacity"
│       data_table="reservoir_monthly_percentile"
```

### **4. reservoir_monthly_percentile (storage distribution by water month)**

```
Table: reservoir_monthly_percentile
├── id                    SERIAL PRIMARY KEY
├── outcome_statistic_id  INTEGER NOT NULL           -- FK → outcome_statistic.id
├── scenario_short_code   VARCHAR(20) NOT NULL       -- Scenario identifier (s0011, s0020, etc.)
├── reservoir_entity_id   INTEGER NOT NULL           -- FK → reservoir_entity.id (SHSTA, OROVL, etc.)
├── water_month           INTEGER NOT NULL           -- 1-12 (Oct=1, Nov=2, ..., Sep=12)
│
├── -- Percentiles (% of reservoir capacity, using water management scheme)
├── p0                    NUMERIC(6,2)               -- min (0th percentile)
├── p10                   NUMERIC(6,2)               -- dry
├── p30                   NUMERIC(6,2)               -- below normal
├── p50                   NUMERIC(6,2)               -- median
├── p70                   NUMERIC(6,2)               -- above normal
├── p90                   NUMERIC(6,2)               -- wet
├── p100                  NUMERIC(6,2)               -- max (100th percentile)
├── mean_value            NUMERIC(6,2)               -- mean for reference
│
├── -- Audit fields (ETL uses developer.id = 1 "system")
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK → developer.id (system)
├── updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Note: scenario_short_code is a logical reference to scenario.scenario_id, not a strict FK.
This allows percentile data to be loaded independently for ETL flexibility.

Note: Capacity lookup via reservoir_entity.capacity_taf (no duplication).
Variable reconstruction: variable_prefix.prefix "S" + "_" + reservoir_entity.short_code = "S_SHSTA"

Foreign keys:
├── Ref: reservoir_monthly_percentile.outcome_statistic_id > outcome_statistic.id [delete: restrict, update: cascade]
├── Ref: reservoir_monthly_percentile.reservoir_entity_id > reservoir_entity.id [delete: restrict, update: cascade]
├── Ref: reservoir_monthly_percentile.created_by > developer.id [delete: restrict, update: cascade]
└── Ref: reservoir_monthly_percentile.updated_by > developer.id [delete: restrict, update: cascade]

Indexes:
├── reservoir_monthly_percentile_pkey (id)
├── uq_reservoir_percentile (outcome_statistic_id, scenario_short_code, reservoir_entity_id, water_month)
├── idx_reservoir_percentile_statistic (outcome_statistic_id)
├── idx_reservoir_percentile_scenario (scenario_short_code)
├── idx_reservoir_percentile_reservoir (reservoir_entity_id)
├── idx_reservoir_percentile_combined (scenario_short_code, reservoir_entity_id)
└── idx_reservoir_percentile_active (is_active) WHERE is_active = TRUE

Constraints:
├── water_month CHECK (water_month BETWEEN 1 AND 12)
└── Unique: (outcome_statistic_id, scenario_short_code, reservoir_entity_id, water_month)

Target Reservoirs (8 major):
├── SHSTA: Shasta (4,552 TAF capacity)
├── TRNTY: Trinity (2,448 TAF)
├── OROVL: Oroville (3,537 TAF)
├── FOLSM: Folsom (975 TAF)
├── MELON: New Melones (2,400 TAF)
├── MLRTN: Millerton (520 TAF)
├── SLUIS_CVP: San Luis CVP (1,062 TAF)
└── SLUIS_SWP: San Luis SWP (979 TAF)

Expected Records: 96 rows per scenario (8 reservoirs × 12 months)
Total: 768 rows for 8 scenarios

Frontend Use:
├── Band charts showing storage distribution across water year months
├── Outer band: p10-p90 (lightest color)
├── Inner bands: p30-p70 (darker)
├── Center line: p50 (median)
└── Min/max bounds: p0-p100

Source: CalSim scenario CSV from S3 (s3://coeqwal-model-run/scenario/{id}/csv/)
ETL: etl/statistics/calculate_reservoir_percentiles.py
```

### **5. reservoir_variable (CalSim variables linked to reservoirs)**

```
Table: reservoir_variable
├── id                    SERIAL PRIMARY KEY
├── calsim_id             TEXT NOT NULL              -- "S_SHSTA", "C_SHSTA", "C_SHSTA_FLOOD", etc.
├── name                  TEXT NOT NULL              -- "Shasta Storage", "Shasta Total Release", etc.
├── description           TEXT                       -- Detailed description
├── reservoir_entity_id   INTEGER                    -- FK → reservoir_entity.id (NULL for aggregates)
├── variable_type         TEXT NOT NULL              -- "storage", "storage_level", "release_total", "release_normal", "release_flood"
├── is_aggregate          BOOLEAN DEFAULT FALSE      -- TRUE for composite variables
├── aggregated_variable_ids INTEGER[]                -- IDs of component variables if aggregate
├── trigger_threshold     NUMERIC                    -- Threshold for alerts/triggers
├── unit_id               INTEGER NOT NULL           -- FK → unit.id (1=TAF, 2=CFS)
├── temporal_scale_id     INTEGER NOT NULL           -- FK → temporal_scale.id (3=monthly)
├── variable_version_id   INTEGER NOT NULL           -- FK → version.id (variable family)
├── variable_id           UUID UNIQUE NOT NULL       -- External system identifier
├── source_ids            INTEGER[]                  -- FK array → data_source.id
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK → developer.id
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
├── reservoir_entity_id   INTEGER NOT NULL           -- FK → reservoir_entity.id
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
├── -- Metadata
├── capacity_taf          NUMERIC(10,2)              -- Denormalized for convenience
├── sample_count          INTEGER                    -- Number of months in sample
│
├── -- Audit fields (ERD standard)
├── is_active             BOOLEAN NOT NULL DEFAULT TRUE
├── created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK → developer.id
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

DDL: database/scripts/sql/09_statistics/04_create_reservoir_storage_monthly.sql
ETL: etl/statistics/calculate_reservoir_statistics.py
```

### **7. reservoir_spill_monthly (monthly spill statistics)**

```
Table: reservoir_spill_monthly
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL       -- Scenario identifier
├── reservoir_entity_id   INTEGER NOT NULL           -- FK → reservoir_entity.id
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
ETL maps reservoir_entity.short_code → CalSim variable C_{short_code}_FLOOD

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

DDL: database/scripts/sql/09_statistics/05_create_reservoir_spill_monthly.sql
ETL: etl/statistics/calculate_reservoir_statistics.py
```

### **8. reservoir_period_summary (period-of-record summary)**

```
Table: reservoir_period_summary
├── id                    SERIAL PRIMARY KEY
├── scenario_short_code   VARCHAR(20) NOT NULL       -- Scenario identifier
├── reservoir_entity_id   INTEGER NOT NULL           -- FK → reservoir_entity.id
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
├── -- Metadata
├── capacity_taf          NUMERIC(10,2)              -- Denormalized for convenience
│
├── -- Audit fields (ERD standard)
├── is_active             BOOLEAN NOT NULL DEFAULT TRUE
├── created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1
├── updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
└── updated_by            INTEGER NOT NULL DEFAULT 1

Note: ETL maps reservoir_entity.short_code → CalSim variables:
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
└── idx_period_summary_active (is_active) WHERE is_active = TRUE

Constraints:
└── Unique: (scenario_short_code, reservoir_entity_id)

Expected Records: 736 rows (92 reservoirs × 8 scenarios)

DDL: database/scripts/sql/09_statistics/06_create_reservoir_period_summary.sql
ETL: etl/statistics/calculate_reservoir_statistics.py
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
├── tier_version_id      INTEGER NOT NULL DEFAULT 8 -- FK → version.id (tier family)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL DEFAULT coeqwal_current_operator() -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL DEFAULT coeqwal_current_operator() -- FK → developer.id

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
├── tier_short_code         VARCHAR NOT NULL           -- FK → tier_definition.short_code
├── location_type           VARCHAR NOT NULL           -- 'network_node', 'wba', 'reservoir', 'compliance_station', 'region'
├── location_id             VARCHAR NOT NULL           -- ID in respective table (e.g., SAC232, 08N, SHSTA, JP, DELTA)
├── location_name           VARCHAR                    -- Display name for map tooltip
├── tier_level              INTEGER                    -- 1, 2, 3, or 4 (tier assignment for this location)
├── tier_value              INTEGER                    -- Optional: count or value at this location (usually 1)
├── display_order           INTEGER DEFAULT 1          -- For consistent map marker ordering
├── tier_version_id         INTEGER NOT NULL DEFAULT 8 -- FK → version.id (tier family)
├── created_at              TIMESTAMP DEFAULT NOW()
├── created_by              INTEGER NOT NULL           -- FK → developer.id
├── updated_at              TIMESTAMP DEFAULT NOW()
└── updated_by              INTEGER NOT NULL           -- FK → developer.id

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
├── 'network_node' → network.short_code (ENV_FLOWS, FW_EXP evaluation points)
├── 'wba' → wba.wba_id (GW_STOR aquifer polygons)
├── 'reservoir' → reservoirs.calsim_short_code (RES_STOR lake polygons)  
├── 'compliance_station' → compliance_stations.station_code (FW_DELTA_USES monitoring)
└── 'region' → hydrologic_region.short_code (DELTA_ECO, WRC_SALMON_AB regional)

Example: ENV_FLOWS s0011 has 17 location records (one per evaluation node) with tier_levels 2-3
```

### **3. tier_result (aggregated tier values by scenario)**

```
Table: tier_result
├── id                   SERIAL PRIMARY KEY
├── scenario_short_code  VARCHAR NOT NULL           -- Scenario identifier (s0011, etc.) - logical ref to scenario.scenario_id
├── tier_short_code      VARCHAR NOT NULL           -- FK → tier_definition.short_code
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
├── tier_version_id      INTEGER NOT NULL DEFAULT 8 -- FK → version.id (tier family)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
├── created_by           INTEGER NOT NULL DEFAULT coeqwal_current_operator() -- FK → developer.id
├── updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
└── updated_by           INTEGER NOT NULL DEFAULT coeqwal_current_operator() -- FK → developer.id

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
├── ENV_FLOWS s0011: [0,5,12,0] → normalized [0, 0.294, 0.706, 0]
├── GW_STOR s0020: [7,14,15,6] → normalized [0.167, 0.333, 0.357, 0.143]
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
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

```

#### **Tier 2: Type table (unified arc + node types)**
```
Table: network_type
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- "CH", "CT", "D", "STR", etc.
├── label                VARCHAR NOT NULL           -- "Channel", "Cross transfer", "Storage", etc.
├── description          TEXT
├── network_entity_type_id INTEGER NOT NULL         -- FK → network_entity_type.id (1=arc, 2=node)
├── model_source_id      INTEGER DEFAULT 1          -- FK → model_source.id (calsim3)
├── source_id            INTEGER DEFAULT 4          -- FK → source.id (geopackage)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

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
├── network_entity_type_id INTEGER NOT NULL         -- FK → network_entity_type.id (1=arc, 2=node)
├── type_id              INTEGER NOT NULL           -- FK → network_type.id (parent type)
├── model_source_id      INTEGER DEFAULT 1          -- FK → model_source.id (calsim3)
├── source_id            INTEGER DEFAULT 4          -- FK → source.id (geopackage)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Values (27 total):
├── IDs 1-10: Arc subtypes (BP, CH, CL, HIS, IM, LI, NA, NS, PRP, ST)
└── IDs 11-27: Node subtypes (A, BYP, CNL, GWO, NA, NSM, OMD, OMR, PRP, R, Reservoir, SG, SIM, STM, U, X)
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
├── entity_type_id       INTEGER                    -- FK → network_entity_type.id (arc=1, node=2, null=3, unimpaired_flows=4)
├── type_id              INTEGER                    -- FK → network_type.id
├── subtype_ids          INTEGER[]                  -- Array of network_subtype.id values (e.g., {25,23})
├── model_list           INTEGER[]                  -- Array of model_source.id (e.g., {1} for CalSim3)
├── source_list          INTEGER[]                  -- Array of source.id (e.g., {1,4,8,9} for report+geopackage+schematic+manual)
├── has_gis              BOOLEAN DEFAULT FALSE      -- Spatial data available
├── hydrologic_region_id INTEGER                    -- FK → hydrologic_region.id (1=SAC, 2=SJR, 3=DELTA, 4=TL, 5=CC)
├── riv_sys              VARCHAR                    -- River system name from geopackage (e.g., "Sacramento River", "San Joaquin River")
├── strm_code            VARCHAR                    -- Stream code from geopackage (e.g., "SAC", "SJR", "DMC")
├── network_version_id   INTEGER NOT NULL           -- FK → version.id (network family, default=12)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

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
├── network_id           INTEGER NOT NULL           -- FK → network.id (populated during DB load via short_code lookup)
├── river                VARCHAR                    -- River identifier for watershed connection (AMR, CCH, ELD)
├── from_node            VARCHAR                    -- From node identifier
├── to_node              VARCHAR                    -- To node identifier  
├── shape_length_m       NUMERIC                    -- Arc length in meters
├── model_source_id      INTEGER DEFAULT 1          -- FK → model_source.id (CalSim3)
├── source_id            INTEGER DEFAULT 4          -- FK → source.id (geopackage)
├── network_version_id   INTEGER NOT NULL           -- FK → version.id (network family)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

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

```
Table: river_watershed
├── id                    SERIAL PRIMARY KEY
├── river_prefix          VARCHAR UNIQUE NOT NULL    -- River identifier (AMR, CCH, ELD, etc.)
├── river_name            VARCHAR NOT NULL           -- Full river name (American River, Cache Creek)
├── watershed_short_code  VARCHAR NOT NULL           -- FK reference to watersheds.short_code
├── source_id             INTEGER DEFAULT 1          -- FK → source.id (CalSim report)
├── network_version_id    INTEGER NOT NULL           -- FK → version.id (network family)
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMP DEFAULT NOW()
├── created_by            INTEGER NOT NULL           -- FK → developer.id
├── updated_at            TIMESTAMP DEFAULT NOW()
└── updated_by            INTEGER NOT NULL           -- FK → developer.id

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
├── AMR → SAC_RIVER (American River → Sacramento River Hydrologic Region)
├── CCH → SAC_RIVER (Cache Creek → Sacramento River Hydrologic Region)
├── ELD → UPPER_AMERICAN (Eldorado → Upper American River Watershed)
├── SFA → UPPER_AMERICAN (South Fork American → Upper American River Watershed)
├── TRN → SAN_JOAQUIN (Tuolumne River → San Joaquin River Hydrologic Region)
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
├── network_id           INTEGER NOT NULL           -- FK → network.id (populated during DB load via short_code lookup)
├── riv_mi               NUMERIC                    -- River mile location
├── c2vsim_gw            VARCHAR                    -- C2VSIM groundwater connection
├── c2vsim_sw            VARCHAR                    -- C2VSIM surface water connection
├── nrest_gage           VARCHAR                    -- Nearest stream gauge
├── strm_code            VARCHAR                    -- Stream/river code (links to river_watershed)
├── rm_ii                VARCHAR                    -- River mile II designation
├── model_source_id      INTEGER DEFAULT 1          -- FK → model_source.id (CalSim3)
├── source_id            INTEGER DEFAULT 4          -- FK → source.id (geopackage)
├── network_version_id   INTEGER NOT NULL           -- FK → version.id (network family)
├── is_active            BOOLEAN DEFAULT TRUE
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

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
├── network_id           INTEGER NOT NULL           -- FK → network.id (populated during DB load via short_code lookup)
├── precision_level      VARCHAR NOT NULL           -- "precise", "mapping_efficient", "regional"
├── geom_wkt             TEXT NOT NULL              -- Primary geometry storage
├── srid                 INTEGER DEFAULT 4326
├── geom                 GEOMETRY (computed)        -- PostGIS binary (STORED)
├── center_latitude      NUMERIC (computed)         -- Arc midpoint ON line (STORED)
├── center_longitude     NUMERIC (computed)         -- Arc midpoint ON line (STORED)
├── estimated_accuracy_meters NUMERIC               -- Actual accuracy estimate
├── source_id            INTEGER NOT NULL           -- FK → source.id
├── network_version_id   INTEGER NOT NULL           -- FK → version.id (network family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

### **3. network_arc_attribute (Arc network attribute)**
```
Table: network_arc_attribute
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK → network.id
├── name                 VARCHAR                    -- Arc name
├── calsim_id_stream     VARCHAR                    -- Stream/canal identifier (not unique)
├── arc_id_short_code    VARCHAR                    -- Arc identifier (in most cases matches network.short_code)
├── type_id              INTEGER                    -- FK → network_arc_type.id
├── sub_type_id          INTEGER                    -- FK → network_arc_subtype.id
├── shape_length         NUMERIC                    -- Arc length in meters
├── attribute_source     JSONB NOT NULL             -- {"name": {"source": "geopackage", "column": "NAME"}, "calsim_id_stream": {"source": "geopackage", "column": "CalSim_ID"}, "shape_length": {"source": "geopackage", "column": "Shape_Leng"}}
├── network_version_id   INTEGER NOT NULL           -- FK → version.id (network family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

```

### **4. network_node_attribute (Node network attribute)**
```
Table: network_node_attribute
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK → network.id
├── calsim_id            VARCHAR                    -- CalSim node identifier
├── riv_mi               NUMERIC                    -- River mile
├── riv_name             VARCHAR                    -- River name
├── comment              TEXT                       -- Node comment
├── c2vsim_gw            VARCHAR                    -- C2VSIM groundwater ID
├── c2vsim_sw            VARCHAR                    -- C2VSIM surface water ID
├── type_id              INTEGER                    -- FK → network_node_type.id
├── sub_type_id          INTEGER                    -- FK → network_node_subtype.id
├── nrest_gage           VARCHAR                    -- Nearest gage
├── strm_code            VARCHAR                    -- Stream code
├── rm_ii                VARCHAR                    -- River mile indicator
├── attribute_source     JSONB NOT NULL             -- {"calsim_id": {"source": "geopackage", "column": "CalSim_ID"}, "riv_mi": {"source": "geopackage", "column": "Riv_Mi"}, "type_id": {"source": "calsim_model", "column": "derived"}}
├── network_version_id   INTEGER NOT NULL           -- FK → version.id (network family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

```

### **5. network_physical_connectivity (Geopackage Connectivity)**
```
Table: network_physical_connectivity
├── id                   SERIAL PRIMARY KEY
├── arc_network_id       INTEGER NOT NULL           -- FK → network.id (arc)
├── from_node_network_id INTEGER NOT NULL           -- FK → network.id (from node)
├── to_node_network_id   INTEGER NOT NULL           -- FK → network.id (to node)
├── source_id            INTEGER NOT NULL           -- FK → source.id
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

### **6. network_operational_connectivity (XML Connectivity)**
```
Table: network_operational_connectivity
├── id                   SERIAL PRIMARY KEY
├── from_network_id      INTEGER NOT NULL           -- FK → network.id
├── to_network_id        INTEGER NOT NULL           -- FK → network.id
├── via_arc_network_id   INTEGER                    -- FK → network.id (connecting arc, if applicable)
├── source_id            INTEGER NOT NULL           -- FK → source.id
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

### **7. network_computational_connectivity (CalSim Connectivity)**
```
Table: network_computational_connectivity
├── id                   SERIAL PRIMARY KEY
├── from_network_id      INTEGER NOT NULL           -- FK → network.id
├── to_network_id        INTEGER NOT NULL           -- FK → network.id
├── equation_name        VARCHAR                    -- "continuityAMR006"
├── wresl_context_list   JSONB NOT NULL             -- [{"file": "SystemTables_Sac/constraints-Connectivity.wresl", "context": "Sac"}, {"file": "SystemTables_LowerAmerican/constraints-Connectivity.wresl", "context": "LowerAmerican"}]
├── source_id            INTEGER NOT NULL           -- FK → source.id
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

### **8. network_variable (future variable relationships)**
```
Table: network_variable
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK → network.id
├── variable_id          INTEGER NOT NULL           -- FK → variable.id
├── variable_role        VARCHAR                    -- "flow", "storage", "diversion"
├── units                VARCHAR
├── source_id            INTEGER NOT NULL           -- FK → source.id
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

### **9. network_source_attribution**
```
Table: network_source_attribution
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK → network.id
├── source_id            INTEGER NOT NULL           -- FK → source.id
├── note                 TEXT
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

### **10. tier_definition**
```
Table: tier_definition
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- "community_water", "agricultural_revenue", etc.
├── label                VARCHAR NOT NULL           -- "Community Water Systems", "Agricultural Revenue"
├── description          TEXT
├── tier_category        TEXT[]                     -- ["water_supply", "environmental"] (can belong to multiple categories)
├── measurement_unit     VARCHAR                    -- "acre_feet", "people_served", "temperature_f"
├── is_active            BOOLEAN DEFAULT TRUE
├── tier_version_id      INTEGER NOT NULL           -- FK → version.id (tier family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER                    -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER                    -- FK → developer.id

```

### **11. variable_tier (Many-to-many variable-tier relationship)**
```
Table: variable_tier
├── id                   SERIAL PRIMARY KEY
├── variable_id          INTEGER NOT NULL           -- FK → variable.id
├── tier_definition_id   INTEGER NOT NULL           -- FK → tier_definition.id
├── tier_value           NUMERIC                    -- Value in base unit
├── base_unit            VARCHAR NOT NULL           -- "TAF", "CFS", "people", "temperature_f" (authoritative unit)
├── supported_unit_list  TEXT[]                     -- ["TAF", "CFS", "acre_feet"] (units this can be converted to)
├── note                 TEXT
├── tier_version_id      INTEGER NOT NULL           -- FK → version.id (tier family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Index:
├── idx_variable_tier_variable
└──  idx_variable_tier_definition
```

## **ENTITY LAYER TABLES**

### **Entity tables reference network layer:**

#### **channel_entity (channel management)**
```
Table: channel_entity
├── id                   SERIAL PRIMARY KEY
├── network_arc_id       INTEGER NOT NULL           -- FK → network.id
├── short_code           VARCHAR UNIQUE NOT NULL
├── name                 VARCHAR
├── description          TEXT
├── subtype              VARCHAR
├── entity_type_id       INTEGER NOT NULL           -- FK → calsim_entity_type.id
├── boundary_condition   VARCHAR
├── from_node            VARCHAR
├── to_node_id           INTEGER                    -- FK → network.id (specific to entity role)
├── length_m             NUMERIC
├── entity_version_id    INTEGER NOT NULL           -- FK → version.id (entity family)
├── attribute_source     JSONB NOT NULL             -- {"name": {"source": "entity_system", "column": "name"}, "boundary_condition": {"source": "management", "column": "boundary_type"}}
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

#### **reservoir_entity (reservoir management)**
```
Table: reservoir_entity
├── id                   SERIAL PRIMARY KEY
├── network_node_id      INTEGER NOT NULL           -- FK → network.id
├── short_code           VARCHAR UNIQUE NOT NULL
├── name                 VARCHAR
├── description          TEXT
├── associated_river     VARCHAR
├── entity_type_id       INTEGER NOT NULL           -- FK → calsim_entity_type.id
├── capacity_taf         NUMERIC
├── dead_pool_taf        NUMERIC
├── surface_area_acres   NUMERIC
├── operational_purpose  VARCHAR
├── entity_version_id    INTEGER NOT NULL           -- FK → version.id (entity family)
├── attribute_source     JSONB NOT NULL             -- {"capacity_taf": "entity_system", "operational_purpose": "management"}
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

#### **inflow_entity (inflow management)**
```
Table: inflow_entity
├── id                   SERIAL PRIMARY KEY
├── network_arc_id       INTEGER NOT NULL           -- FK → network.id (inflow arc)
├── short_code           VARCHAR UNIQUE NOT NULL
├── name                 VARCHAR
├── description          TEXT
├── to_node_id           INTEGER                    -- FK → network.id (specific to entity role)
├── entity_type_id       INTEGER NOT NULL           -- FK → calsim_entity_type.id
├── entity_version_id    INTEGER NOT NULL           -- FK → version.id
├── attribute_source     JSONB NOT NULL             -- {"name": "entity_system", "to_node_id": "operational"}
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

#### **du_urban_entity (community demand unit management)**
```
Table: du_urban_entity
├── id                   SERIAL PRIMARY KEY
├── du_id                VARCHAR UNIQUE NOT NULL    -- Demand unit identifier
├── network_node_id      INTEGER NOT NULL           -- FK → network.id (service location)
├── wba_id               VARCHAR
├── du_class             VARCHAR DEFAULT 'Urban'
├── total_acre           NUMERIC
├── polygon_count        INTEGER DEFAULT 1
├── community_agency     VARCHAR                    -- Urban specific
├── gw                   VARCHAR                    -- Urban specific
├── sw                   VARCHAR                    -- Urban specific
├── point_of_diversion   VARCHAR                    -- Urban specific
├── entity_type_id       INTEGER NOT NULL           -- FK → calsim_entity_type.id
├── entity_version_id    INTEGER NOT NULL           -- FK → version.id (entity family)
├── attribute_source     JSONB NOT NULL             -- {"community_agency": "du_system", "gw": "operational"}
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

#### **du_agriculture_entity (dgriculture demand unit management)**
```
Table: du_agriculture_entity
├── id                   SERIAL PRIMARY KEY
├── du_id                VARCHAR UNIQUE NOT NULL
├── network_node_id      INTEGER NOT NULL           -- FK → network.id
├── wba_id               VARCHAR
├── du_class             VARCHAR DEFAULT 'Agriculture'
├── total_acre           NUMERIC
├── polygon_count        INTEGER DEFAULT 1
├── crop_type            VARCHAR                    -- Agriculture specific
├── irrigation_method    VARCHAR                    -- Agriculture specific
├── water_right_type     VARCHAR                    -- Agriculture specific
├── entity_type_id       INTEGER NOT NULL           -- FK → calsim_entity_type.id
├── entity_version_id    INTEGER NOT NULL           -- FK → version.id (entity family)
├── attribute_source     JSONB NOT NULL
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
```

#### **du_refuge_entity (refuge demand unit management)**
```
Table: du_refuge_entity
├── id                   SERIAL PRIMARY KEY
├── du_id                VARCHAR UNIQUE NOT NULL
├── network_node_id      INTEGER NOT NULL           -- FK → network.id
├── wba_id               VARCHAR
├── du_class             VARCHAR DEFAULT 'Refuge'
├── total_acre           NUMERIC
├── polygon_count        INTEGER DEFAULT 1
├── refuge_or_wildlife_area VARCHAR                 -- Refuge specific
├── managed_by           VARCHAR                    -- Refuge specific
├── provider             VARCHAR                    -- Refuge specific
├── habitat_type         VARCHAR                    -- Refuge specific
├── entity_type_id       INTEGER NOT NULL           -- FK → calsim_entity_type.id
├── entity_version_id    INTEGER NOT NULL           -- FK → version.id (entity family)
├── attribute_source     JSONB NOT NULL
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id
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
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK → developer.id (system)
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
├── reservoir_group_id    INTEGER NOT NULL           -- FK → reservoir_group.id
├── reservoir_entity_id   INTEGER NOT NULL           -- FK → reservoir_entity.id
├── display_order         INTEGER DEFAULT 0          -- Order within group for UI
├── is_active             BOOLEAN DEFAULT TRUE
├── created_at            TIMESTAMPTZ DEFAULT NOW()
├── created_by            INTEGER NOT NULL DEFAULT 1 -- FK → developer.id (system)
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
```