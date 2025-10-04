# 🏗️ NETWORK & ENTITY LAYER ERD

## **🎯 ARCHITECTURE OVERVIEW**

### **Two-Layer Design:**
```
NETWORK LAYER (Infrastructure/Physical)
├── Master registry of all physical network elements
├── Spatial data and engineering attributes
├── Multi-source connectivity (geopackage, XML, CalSim)
└── Purpose: "What exists physically and how is it connected?"

ENTITY LAYER (Management/Operational)  
├── Management and operational perspectives on network elements
├── Multiple entity roles can reference same network element
├── Business logic and operational attributes
└── Purpose: "How are network elements used and managed?"
```

## **📊 NETWORK LAYER TABLES**

### **1. network (Master Registry)**
```
Table: network
├── id                   SERIAL PRIMARY KEY
├── short_code           VARCHAR UNIQUE NOT NULL    -- "AMR006", "C_AMR006"
├── entity_type_id       INTEGER NOT NULL           -- FK → entity_type.id (arc/node)
├── model_list           INTEGER[]                  -- Array of model_source.id
├── source_list          INTEGER[]                  -- Array of source.id  
├── has_gis              BOOLEAN DEFAULT FALSE
├── hydrologic_region_id INTEGER                    -- FK → hydrologic_region.id (fundamental)
├── network_version_id   INTEGER NOT NULL           -- FK → version.id (network family)
├── created_at           TIMESTAMP DEFAULT NOW()
├── created_by           INTEGER NOT NULL           -- FK → developer.id
├── updated_at           TIMESTAMP DEFAULT NOW()
└── updated_by           INTEGER NOT NULL           -- FK → developer.id

Constraints:
├── UNIQUE(short_code)
├── CHECK(array_length(model_list, 1) > 0)
├── CHECK(array_length(source_list, 1) > 0)
└── FK validation functions for arrays

Indexes:
├── idx_network_short_code
├── idx_network_entity_type  
├── idx_network_source_list (GIN)
├── idx_network_model_list (GIN)
└── idx_network_has_gis
```

### **2. network_gis (Multi-precision spatial data)**
```
Table: network_gis
├── id                   SERIAL PRIMARY KEY
├── network_id           INTEGER NOT NULL           -- FK → network.id
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

Constraints:
├── CHECK precision_level IN ('precise', 'mapping_efficient', 'regional')
└── CASCADE DELETE with network

Indexes:
├── idx_network_gis_network_id
├── idx_network_gis_precision_level
├── idx_network_gis_precision_network (network_id, precision_level)
└── idx_network_gis_geom (GIST)                     -- Spatial index
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

Constraints:
└── CASCADE DELETE with network

Indexes:
├── idx_network_arc_attr_network_id
├── idx_network_arc_attr_sources (GIN)             -- JSONB index
├── idx_network_arc_attr_type_id                   -- Arc type filtering
├── idx_network_arc_attr_type_subtype (type_id, sub_type_id) -- Type/subtype combination
├── idx_network_arc_attr_calsim_stream (calsim_id_stream)    -- Stream grouping
└── idx_network_arc_attr_arc_id (arc_id)                     -- Arc ID lookup (matches network.short_code)
```

### **4. network_node_attribute (Node nertwork attribute)**
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

Constraints:
└── CASCADE DELETE with network

Indexes:
├── idx_network_node_attr_network_id
├── idx_network_node_attr_sources (GIN)             -- JSONB index
├── idx_network_node_attr_type_id                   -- Node type filtering
├── idx_network_node_attr_type_subtype (type_id, sub_type_id) -- Type/subtype combination
└── idx_network_node_attr_calsim_id                 -- CalSim ID lookup
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

Indexes:
├── idx_phys_conn_arc
├── idx_phys_conn_from
└── idx_phys_conn_to
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

Indexes:
├── idx_op_conn_from
├── idx_op_conn_to
└── idx_op_conn_via_arc
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

Indexes:
├── idx_comp_conn_from
├── idx_comp_conn_to
├── idx_comp_conn_wresl_context (GIN)               -- JSONB search on wresl_context_list
└── idx_comp_conn_equation_name                     -- Equation name lookup

Note: Boundary elements appear in multiple regional WRESL files
Example: continuityAMR006 in both Sac and LowerAmerican contexts
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

Constraints:
└── UNIQUE(network_id, variable_id, variable_role)

Indexes:
├── idx_network_var_network
└── idx_network_var_variable
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

Constraints:
└── UNIQUE(network_id, source_id)

Indexes:
├── idx_source_attr_network
└── idx_source_attr_source
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

Sample Data:
├── community_water: Community Water Systems (water_supply)
├── agricultural_revenue: Agricultural Revenue (economic)
├── environmental_water: Environmental Water (environmental)
├── delta_salinity: Delta Salinity (environmental)
├── reservoir_storage: Reservoir Storage (water_supply)
├── groundwater_storage: Groundwater Storage (water_supply)
└── winter_run_salmon: Winter-run Chinook Salmon Sacramento Abundance (environmental)

Indexes:
├── idx_tier_def_short_code (unique)
├── idx_tier_def_category
└── idx_tier_def_active
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

Constraint:
└── UNIQUE(variable_id, tier_definition_id)

Index:
├── idx_variable_tier_variable
└──  idx_variable_tier_definition
```

## **🏢 ENTITY LAYER TABLES**

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

Purpose: Management/operational perspective on channel arc
Note: entity_type vs schematic_type distinction needed

New Indexes:
├── idx_channel_entity_hydrologic_region (from network.hydrologic_region_id)
├── idx_channel_entity_type_subtype (entity_type_id, subtype)
└── idx_channel_entity_calsim_id_stream (for stream grouping)
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

Purpose: Management/operational perspective on reservoir node
Note: Removed hydrologic_region_id (now in network table), has_gis_data (redundant), is_main (hard to maintain)

New Indexes:
├── idx_reservoir_entity_type_subtype (entity_type_id, schematic_type_id)
└── idx_reservoir_entity_operational_purpose
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

Purpose: Management/operational perspective on inflow arc

Indexes:
├── idx_inflow_entity_type_subtype (entity_type_id, schematic_type_id)
└── idx_inflow_entity_to_node_id
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

Indexes:
├── idx_du_urban_entity_type_subtype (entity_type_id, schematic_type_id)
├── idx_du_urban_entity_community_agency
└── idx_du_urban_entity_du_class
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

New Indexes:
├── idx_du_agriculture_entity_type_subtype (entity_type_id, schematic_type_id)
├── idx_du_agriculture_entity_crop_type
└── idx_du_agriculture_entity_irrigation_method
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

New Indexes:
├── idx_du_refuge_entity_type_subtype (entity_type_id, schematic_type_id)
├── idx_du_refuge_entity_managed_by
└── idx_du_refuge_entity_habitat_type
```

## **🔗 RELATIONSHIPS & FOREIGN KEYS**

### **Network Layer Internal Relationships:**
```
network.id ← network_gis.network_id (1:1)
network.id ← network_arc_attribute.network_id (1:1)
network.id ← network_node_attribute.network_id (1:1)
network.id ← network_physical_connectivity.arc_network_id (1:many)
network.id ← network_physical_connectivity.from_node_network_id (1:many)
network.id ← network_physical_connectivity.to_node_network_id (1:many)
network.id ← network_operational_connectivity.from_network_id (1:many)
network.id ← network_operational_connectivity.to_network_id (1:many)
network.id ← network_computational_connectivity.from_network_id (1:many)
network.id ← network_computational_connectivity.to_network_id (1:many)
network.id ← network_source_attribution.network_id (1:many)
```

### **Entity Layer to Network Layer Relationships:**
```
network.id ← channel_entity.network_arc_id (1:many)
network.id ← inflow_entity.network_arc_id (1:many)
network.id ← diversion_arc_entity.network_arc_id (1:many)
network.id ← reservoir_entity.network_node_id (1:many)
network.id ← du_urban_entity.network_node_id (1:many)
network.id ← du_agriculture_entity.network_node_id (1:many)
network.id ← du_refuge_entity.network_node_id (1:many)
```

### **Lookup Table Relationships:**
```
entity_type.id ← network.entity_type_id
network_arc_type.id ← network_arc_attribute.type_id
network_node_type.id ← network_node_attribute.type_id
network_arc_subtype.id ← network_arc_attribute.sub_type_id
network_node_subtype.id ← network_node_attribute.sub_type_id
hydrologic_region.id ← network_arc_attribute.hr_id
hydrologic_region.id ← network_node_attribute.hr_id
source.id ← network_gis.source_id
source.id ← network_arc_attribute.primary_source_id
source.id ← network_node_attribute.primary_source_id
model_source.id ← network.model_list[] (array FK)
source.id ← network.source_list[] (array FK)
developer.id ← network.created_by
developer.id ← network.updated_by
```

## **🎯 KEY ARCHITECTURAL PATTERNS**

### **1. One-to-Many: Network to Entities**
```
Example: Network Node AMR006
├── network.id = 123, short_code = "AMR006"
├── network_gis: POINT(-121.4232, 38.5688)
├── network_node_attribute: riv_name="American River"
└── Referenced by multiple entities:
    ├── diversion_entity.network_node_id = 123
    ├── du_urban_entity.network_node_id = 123
    └── monitoring_entity.network_node_id = 123
```

### **2. Shared GIS Coordinates:**
```
Multiple entities at same location:
├── Physical: One network node with one set of coordinates
├── Logical: Multiple entity purposes at that location
├── Efficient: No duplicate spatial data
└── Realistic: Real-world facilities serve multiple purposes
```

### **3. Attribute Separation:**
```
Network Attributes (Infrastructure):
├── shape_length, elevation, junction_type
├── Source: Engineering/spatial data
└── Purpose: "What is it physically?"

Entity Attributes (Management):
├── operational_purpose, capacity_taf, service_population
├── Source: Management/operational systems
└── Purpose: "How is it used/managed?"
```

## **📊 EXAMPLE SCENARIOS**

### **Scenario 1: Channel Arc with Multiple Roles**
```
Network Element: C_SAC287
├── network: short_code="C_SAC287", entity_type="arc"
├── network_gis: MULTILINESTRING(...)
├── network_arc_attribute: shape_length=5000m, type="channel"
└── Entity roles:
    ├── channel_entity: operational_purpose="conveyance"
    └── monitoring_entity: monitoring_type="flow_gauge"
```

### **Scenario 2: Node with Multiple Entity Purposes**
```
Network Element: AMR006
├── network: short_code="AMR006", entity_type="node"
├── network_gis: POINT(-121.4232, 38.5688)
├── network_node_attribute: riv_name="American River", elevation=50ft
└── Entity roles:
    ├── diversion_entity: capacity=500 CFS, purpose="municipal"
    ├── du_urban_entity: service_pop=50000, demand_type="residential"
    ├── return_entity: return_capacity=200 CFS, treatment="secondary"
    └── monitoring_entity: station_type="flow_temp_quality"
```

### **Scenario 3: Shared Coordinates, Different Entities**
```
Physical Location: (-121.4232, 38.5688)
├── Network elements at this location:
│   ├── AMR006 (main river node)
│   ├── D_AMR006_CITY (diversion arc)
│   └── R_AMR006_WWTP (return arc)
└── All share same coordinates but serve different functions
```

## **🚀 BENEFITS OF THIS DESIGN**

### **✅ Handles All Your Challenges:**
1. **Attribute overlap**: Clear separation (infrastructure vs management)
2. **Multiple entity roles**: One network element, many entity purposes
3. **Shared coordinates**: Efficient spatial data usage
4. **Source attribution**: Granular tracking with JSONB
5. **Scalability**: Easy to add new entity types

### **✅ Query Flexibility:**
```sql
-- Infrastructure view
SELECT n.short_code, ng.center_latitude, naa.shape_length
FROM network n
JOIN network_gis ng ON n.id = ng.network_id
JOIN network_arc_attribute naa ON n.id = naa.network_id;

-- Management view
SELECT n.short_code, ce.operational_purpose, ce.management_agency
FROM network n  
JOIN channel_entity ce ON n.id = ce.network_arc_id;

-- Complete view
SELECT n.short_code, ng.center_latitude, naa.shape_length, ce.operational_purpose
FROM network n
JOIN network_gis ng ON n.id = ng.network_id
JOIN network_arc_attribute naa ON n.id = naa.network_id
LEFT JOIN channel_entity ce ON n.id = ce.network_arc_id;
```

## **📋 IMPLEMENTATION SUMMARY**

### **Network Layer: 9 Tables**
- Infrastructure-focused
- Multi-source integration
- Physical/engineering attributes
- Connectivity from all sources

### **Entity Layer: 7+ Tables (Existing)**
- Management-focused  
- Operational attributes
- Multiple roles per network element
- Business logic and operations

### **Relationship Pattern:**
```
network.id ← entity_table.network_id (1:many)
One infrastructure element, multiple management roles
```

**This ERD provides complete separation of concerns while enabling rich multi-perspective queries on your water network!** 🎯



### Notes: Source attribution in JSONB**

```
-- Example in attribute_source JSONB:
{"name": "geopackage.NAME", "calsim_id_stream": "geopackage.CalSim_ID", "type_id": "calsim_model"}
```



### **10. entity_type vs schematic_type**
❓ **CLARIFICATION NEEDED**: 
- `entity_type_id`: Management classification (reservoir, channel, inflow)
- `schematic_type_id`: Schematic representation type
- Network table tracks arc/node (infrastructure type)
- Entity table tracks management type (operational classification)

## **📋 SUMMARY OF KEY IMPROVEMENTS**

## **🔍 FINAL ANSWERS TO REMAINING QUESTIONS**

### **1. Do we need data_types at all?**
❌ **NO** - `data_types` field not found in any seed tables
✅ **REMOVE**: `data_types TEXT[]` from `network_source_attribution` table
✅ **REASON**: Not used in actual data, adds unnecessary complexity

### **2. entity_type vs schematic_type Values Found:**

#### **calsim_entity_type (Management Classification):**
```
reservoir          → schematic_type: node
channel            → schematic_type: arc  
inflow             → schematic_type: arc
demand_unit_agriculture → schematic_type: node
demand_unit_urban  → schematic_type: node
demand_unit_refuge → schematic_type: node
groundwater        → schematic_type: node
salinity_node      → schematic_type: node
delta_outflow      → schematic_type: arc
```

#### **calsim_schematic_type (Infrastructure Type):**
```
arc    -- Infrastructure arcs (channels, diversions, inflows)
node   -- Infrastructure nodes (junctions, reservoirs)
none   -- Non-infrastructure entities
```

### **3. Arc/Node Field Analysis:**
✅ **FOUND**: `schematic_type_label` in `calsim_entity_type` indicates "arc" or "node"
❌ **POOR NAMING**: `schematic_type_label` is confusing
✅ **RENAME**: Call it `network_entity_type` (clearer distinction)

### **4. Entity Type Usage:**
**Tables using entity_type_id (all reference calsim_entity_type.id):**
- `reservoir_entity.entity_type_id`
- `inflow_entity.entity_type_id`
- `channel_entity.entity_type_id`
- `du_agriculture_entity.entity_type_id`
- `du_urban_entity.entity_type_id`
- `du_refuge_entity.entity_type_id`
- `diversion_arc_entity.entity_type_id`
- `theme_entity_type_focus.entity_type_id`

**NO basic `entity_type` table found - all use `calsim_entity_type`**

### **5. Schematic Type Redundancy:**
✅ **YOU'RE RIGHT**: `schematic_type_id` is redundant!
✅ **REASON**: Network table already tracks arc/node via `entity_type_id`
✅ **REMOVE**: `schematic_type_id` from all entity tables

### **6. Updated Table Design:**

#### **Remove data_types field:**
```sql
-- SIMPLIFIED network_source_attribution:
network_source_attribution (
    network_id INTEGER,
    source_id INTEGER,
    notes TEXT
    -- REMOVED: data_types TEXT[] (not used)
);
```

#### **Remove schematic_type_id from entity tables:**
```sql
-- REMOVE from all entity tables:
schematic_type_id INTEGER  -- Redundant with network.entity_type_id

-- Keep only:
entity_type_id INTEGER  -- Management classification (reservoir, channel, etc.)
```

#### **Rename field in calsim_entity_type:**
```sql
-- UPDATE calsim_entity_type table:
-- RENAME: schematic_type_label → network_entity_type
-- VALUES: "arc", "node", "none"
-- PURPOSE: Indicates whether this entity type maps to network arcs or nodes
```

### **🎯 FINAL CLARIFICATIONS:**
- **entity_type_id**: Management classification (reservoir, channel, demand_unit, etc.)
- **network_entity_type**: Infrastructure mapping ("arc", "node", "none") - RENAMED from schematic_type_label
- **network.entity_type_id**: Basic arc/node classification for network infrastructure
- **schematic_type_id**: REMOVED (redundant with network table)
- **data_types**: REMOVED (not used in actual data)

**ERD is now complete and ready for implementation!** 🎯
