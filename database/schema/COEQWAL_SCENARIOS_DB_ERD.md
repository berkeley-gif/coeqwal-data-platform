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
├── scenario_short_code     VARCHAR NOT NULL           -- Scenario identifier (s0011, s0020, s0021)
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
├── scenario_short_code  VARCHAR NOT NULL           -- Scenario identifier (s0011, s0020, s0021)
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

Records: 21 tier results (3 scenarios × 7 indicators)

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