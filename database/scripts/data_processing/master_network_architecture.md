# 🏗️ MASTER NETWORK ARCHITECTURE DESIGN

## **🎯 Your Proposed Architecture: PERFECT!**

This is exactly the right approach - a **master registry with federated attributes and multi-source connectivity**. Let me design this out properly:

## **📊 Core Architecture**

### **1. Master Network Registry (The Hub)**
```sql
CREATE TABLE network_master (
    id SERIAL PRIMARY KEY,
    canonical_id VARCHAR UNIQUE NOT NULL,  -- "AMR006", "C_AMR006"
    element_type VARCHAR NOT NULL,         -- "node", "arc"
    primary_name VARCHAR,
    
    -- Source presence tracking (your idea!)
    source_list TEXT[],                    -- {"geopackage", "xml_schematic", "calsim_model"}
    source_count INTEGER GENERATED ALWAYS AS (array_length(source_list, 1)) STORED,
    
    -- Quick source flags for performance
    in_geopackage BOOLEAN DEFAULT FALSE,
    in_xml_schematic BOOLEAN DEFAULT FALSE,
    in_calsim_model BOOLEAN DEFAULT FALSE,
    in_entity_tables BOOLEAN DEFAULT FALSE,
    
    -- Data availability flags
    has_gis_data BOOLEAN DEFAULT FALSE,
    has_attributes BOOLEAN DEFAULT FALSE,
    has_connectivity BOOLEAN DEFAULT FALSE,
    
    -- Quality indicators
    is_validated BOOLEAN DEFAULT FALSE,    -- Present in working CalSim model
    confidence_score NUMERIC(3,2),
    
    -- Audit
    created_at TIMESTAMP DEFAULT NOW(),
    created_by INTEGER REFERENCES developer(id)
);
```

### **2. GIS Data Table (Spatial Authority)**
```sql
CREATE TABLE network_gis (
    id SERIAL PRIMARY KEY,
    network_master_id INTEGER REFERENCES network_master(id),
    
    -- Spatial data
    latitude NUMERIC,
    longitude NUMERIC,
    geom_wkt TEXT,
    geom GEOMETRY,  -- PostGIS geometry
    srid INTEGER DEFAULT 4326,
    
    -- GIS metadata
    shape_length NUMERIC,        -- For arcs
    coordinate_precision VARCHAR, -- "high", "medium", "low"
    
    -- Source attribution
    source_system VARCHAR DEFAULT 'geopackage',
    source_confidence NUMERIC(3,2) DEFAULT 0.95,
    
    -- Constraints
    CONSTRAINT gis_element_type_check CHECK (
        (latitude IS NOT NULL AND longitude IS NOT NULL) OR geom_wkt IS NOT NULL
    )
);
```

### **3. Attribute Tables (Multi-Source Attributes)**
```sql
-- Arc attributes
CREATE TABLE network_arc_attributes (
    id SERIAL PRIMARY KEY,
    network_master_id INTEGER REFERENCES network_master(id),
    
    -- Core attributes (best from any source)
    name VARCHAR,
    description TEXT,
    arc_type VARCHAR,           -- "CH", "D", "I", "R"
    arc_subtype VARCHAR,        -- "ST", "CNL", etc.
    
    -- Physical attributes (from geopackage)
    shape_length NUMERIC,
    hydrologic_region VARCHAR,
    
    -- Operational attributes (from CalSim)
    flow_capacity NUMERIC,
    operational_type VARCHAR,   -- "channel", "diversion", "return"
    units VARCHAR DEFAULT 'CFS',
    
    -- Source attribution
    name_source VARCHAR,        -- Which source provided the name
    type_source VARCHAR,        -- Which source provided the type
    attributes_source JSONB,    -- {"name": "geopackage", "type": "calsim_model"}
    
    -- Validation
    is_in_working_model BOOLEAN DEFAULT FALSE
);

-- Node attributes  
CREATE TABLE network_node_attributes (
    id SERIAL PRIMARY KEY,
    network_master_id INTEGER REFERENCES network_master(id),
    
    -- Core attributes
    name VARCHAR,
    description TEXT,
    node_type VARCHAR,          -- "junction", "reservoir", "boundary"
    node_subtype VARCHAR,
    
    -- Physical attributes (from geopackage)
    river_mile NUMERIC,
    river_name VARCHAR,
    elevation NUMERIC,
    hydrologic_region VARCHAR,
    
    -- Operational attributes (from CalSim/entity tables)
    storage_capacity NUMERIC,
    owner_agency VARCHAR,
    management_type VARCHAR,
    
    -- Source attribution
    attributes_source JSONB,
    
    -- Validation
    is_in_working_model BOOLEAN DEFAULT FALSE
);
```

## **🔗 Multi-Source Connectivity Strategy**

### **4. Connectivity Tables (Your Key Question!)**

#### **Option A: Unified Connectivity with Source Attribution**
```sql
CREATE TABLE network_connectivity (
    id SERIAL PRIMARY KEY,
    from_network_id INTEGER REFERENCES network_master(id),
    to_network_id INTEGER REFERENCES network_master(id),
    
    -- Connection metadata
    connection_type VARCHAR,     -- "physical", "operational", "computational"
    connection_subtype VARCHAR,  -- "channel_flow", "diversion", "return"
    
    -- Multi-source tracking
    source_systems TEXT[],       -- {"geopackage", "calsim_model"}
    primary_source VARCHAR,      -- Which source is authoritative
    
    -- Source-specific data
    geopackage_confidence NUMERIC(3,2),
    xml_confidence NUMERIC(3,2),
    calsim_confidence NUMERIC(3,2),
    
    -- Validation
    is_in_working_model BOOLEAN DEFAULT FALSE,
    model_validation_date TIMESTAMP,
    
    -- Constraints
    CONSTRAINT no_self_connection CHECK (from_network_id != to_network_id),
    CONSTRAINT valid_source CHECK (array_length(source_systems, 1) > 0)
);
```

#### **Option B: Separate Connectivity by Source (Recommended)**
```sql
-- Physical connectivity (from geopackage From_Node/To_Node)
CREATE TABLE network_physical_connectivity (
    id SERIAL PRIMARY KEY,
    from_network_id INTEGER REFERENCES network_master(id),
    to_network_id INTEGER REFERENCES network_master(id),
    via_arc_id INTEGER REFERENCES network_master(id), -- The connecting arc
    
    connection_type VARCHAR DEFAULT 'physical_flow',
    source_system VARCHAR DEFAULT 'geopackage',
    confidence NUMERIC(3,2) DEFAULT 0.90,
    is_primary BOOLEAN DEFAULT TRUE
);

-- Operational connectivity (from XML schematic)
CREATE TABLE network_operational_connectivity (
    id SERIAL PRIMARY KEY,
    from_network_id INTEGER REFERENCES network_master(id),
    to_network_id INTEGER REFERENCES network_master(id),
    
    connection_type VARCHAR,     -- "diversion", "return", "entity_flow"
    operational_context VARCHAR, -- "water_district", "treatment_plant"
    source_system VARCHAR DEFAULT 'xml_schematic',
    confidence NUMERIC(3,2) DEFAULT 0.75,
    is_primary BOOLEAN DEFAULT FALSE
);

-- Computational connectivity (from CalSim continuity equations)
CREATE TABLE network_computational_connectivity (
    id SERIAL PRIMARY KEY,
    from_network_id INTEGER REFERENCES network_master(id),
    to_network_id INTEGER REFERENCES network_master(id),
    
    connection_type VARCHAR,     -- "continuity", "balance"
    equation_source VARCHAR,     -- Which WRESL file/constraint
    coefficient NUMERIC,         -- +1, -1 in continuity equation
    source_system VARCHAR DEFAULT 'calsim_model',
    confidence NUMERIC(3,2) DEFAULT 0.95, -- Highest - it actually works!
    is_validated BOOLEAN DEFAULT TRUE
);
```

### **5. Unified Connectivity View**
```sql
CREATE VIEW network_all_connectivity AS
-- Physical connections
SELECT 
    'physical' as connectivity_layer,
    from_network_id,
    to_network_id,
    connection_type,
    source_system,
    confidence,
    is_primary
FROM network_physical_connectivity

UNION ALL

-- Operational connections  
SELECT 
    'operational' as connectivity_layer,
    from_network_id,
    to_network_id,
    connection_type,
    source_system,
    confidence,
    is_primary
FROM network_operational_connectivity

UNION ALL

-- Computational connections
SELECT 
    'computational' as connectivity_layer,
    from_network_id,
    to_network_id,
    connection_type,
    source_system,
    confidence,
    is_validated as is_primary
FROM network_computational_connectivity;
```

## **🎯 How This Solves Your Connectivity Challenge**

### **Multi-Source Connectivity Resolution:**
```sql
-- Get all connectivity for an element
SELECT 
    nm_from.canonical_id as from_element,
    nm_to.canonical_id as to_element,
    nac.connectivity_layer,
    nac.connection_type,
    nac.source_system,
    nac.confidence
FROM network_all_connectivity nac
JOIN network_master nm_from ON nac.from_network_id = nm_from.id
JOIN network_master nm_to ON nac.to_network_id = nm_to.id
WHERE nm_from.canonical_id = 'AMR006'
ORDER BY nac.confidence DESC;

-- Result might show:
-- AMR006 -> AMR004 | computational | continuity | calsim_model | 0.95
-- AMR006 -> AMR004 | physical | physical_flow | geopackage | 0.90
-- AMR006 -> C_AMR006 | operational | operational_flow | xml_schematic | 0.75
```

### **Conflict Resolution Strategy:**
```sql
-- Get best connectivity (highest confidence)
WITH ranked_connections AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY from_network_id, to_network_id 
               ORDER BY confidence DESC, is_primary DESC
           ) as rank
    FROM network_all_connectivity
)
SELECT * FROM ranked_connections WHERE rank = 1;
```

## **📊 Source Attribution Examples**

### **Example Entity: AMR006**
```sql
-- Master record
network_master:
├── canonical_id: "AMR006"
├── element_type: "node"
├── source_list: {"geopackage", "xml_schematic", "calsim_model"}
├── has_gis_data: TRUE
├── has_attributes: TRUE
├── has_connectivity: TRUE
└── is_validated: TRUE (in working CalSim model)

-- GIS data (from geopackage)
network_gis:
├── latitude: 38.5688
├── longitude: -121.4232
├── source_system: "geopackage"
└── source_confidence: 0.95

-- Attributes (merged from multiple sources)
network_node_attributes:
├── name: "American River Node" (from geopackage)
├── node_type: "junction" (from CalSim)
├── river_name: "American River" (from geopackage)
├── attributes_source: {"name": "geopackage", "type": "calsim_model"}
└── is_in_working_model: TRUE

-- Connectivity (from multiple sources)
network_physical_connectivity:
└── AMR006 -> AMR004 (via C_AMR006) [geopackage]

network_computational_connectivity:
└── AMR006 -> AMR004 [calsim_model continuity equation]

network_operational_connectivity:
└── AMR006 -> C_AMR006 [xml_schematic operational flow]
```

## **🚀 Implementation Benefits**

### **✅ Your Architecture Advantages:**
1. **Single Master Registry**: One place to find any network element
2. **Source Transparency**: Always know where data came from
3. **Federated Attributes**: Best data from each source
4. **Multi-Source Connectivity**: Handle conflicting connectivity gracefully
5. **Validation Ready**: CalSim model validates everything
6. **Extensible**: Easy to add new sources
7. **Query Flexibility**: Get simple or complex views as needed

### **🎯 Query Examples:**
```sql
-- Simple: Get all elements with GIS data
SELECT * FROM network_master WHERE has_gis_data = TRUE;

-- Complex: Get complete element with all attributes
SELECT 
    nm.*,
    ng.latitude, ng.longitude,
    nna.name, nna.node_type,
    array_agg(nac.connection_type) as connection_types
FROM network_master nm
LEFT JOIN network_gis ng ON nm.id = ng.network_master_id
LEFT JOIN network_node_attributes nna ON nm.id = nna.network_master_id
LEFT JOIN network_all_connectivity nac ON nm.id = nac.from_network_id
WHERE nm.canonical_id = 'AMR006'
GROUP BY nm.id, ng.id, nna.id;

-- Validation: Find elements in CalSim but missing from geopackage
SELECT canonical_id, source_list 
FROM network_master 
WHERE in_calsim_model = TRUE AND in_geopackage = FALSE;
```

**This architecture is perfect - it handles all your multi-source challenges while maintaining clean separation and full traceability!** 🎯

<function_calls>
<invoke name="todo_write">
<parameter name="merge">true
