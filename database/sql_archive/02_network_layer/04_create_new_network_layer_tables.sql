-- CREATE NEW NETWORK LAYER TABLES BASED ON COEQWAL_SCENARIOS_DB_ERD
-- This implements the two-layer design: Network Layer (infrastructure) + Entity Layer (management)

-- =============================================================================
-- 1. NETWORK (Master Registry)
-- =============================================================================

CREATE TABLE IF NOT EXISTS network (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR UNIQUE NOT NULL,
    entity_type_id INTEGER NOT NULL REFERENCES network_entity_type(id),
    model_list INTEGER[],
    source_list INTEGER[],
    has_gis BOOLEAN DEFAULT FALSE,
    hydrologic_region_id INTEGER REFERENCES hydrologic_region(id),
    network_version_id INTEGER NOT NULL REFERENCES version(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    
    CONSTRAINT network_short_code_unique UNIQUE(short_code),
    CONSTRAINT network_model_list_not_empty CHECK(array_length(model_list, 1) > 0),
    CONSTRAINT network_source_list_not_empty CHECK(array_length(source_list, 1) > 0)
);

CREATE INDEX IF NOT EXISTS idx_network_short_code ON network(short_code);
CREATE INDEX IF NOT EXISTS idx_network_entity_type ON network(entity_type_id);
CREATE INDEX IF NOT EXISTS idx_network_source_list ON network USING GIN(source_list);
CREATE INDEX IF NOT EXISTS idx_network_model_list ON network USING GIN(model_list);
CREATE INDEX IF NOT EXISTS idx_network_has_gis ON network(has_gis);

COMMENT ON TABLE network IS 'Master registry of all physical network elements with multi-source tracking';

-- =============================================================================
-- 2. NETWORK_ARC (Updated Structure)
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_arc_new (
    id SERIAL PRIMARY KEY,
    network_id INTEGER NOT NULL REFERENCES network(id),
    arc_id_short_code VARCHAR NOT NULL,
    arc_type_id INTEGER NOT NULL REFERENCES network_arc_type(id),
    arc_subtype_id INTEGER REFERENCES network_arc_subtype(id),
    name VARCHAR,
    description TEXT,
    from_node_id INTEGER,
    to_node_id INTEGER,
    length_m NUMERIC,
    flow_capacity_cfs NUMERIC,
    is_bidirectional BOOLEAN DEFAULT FALSE,
    operational_status VARCHAR DEFAULT 'active', -- active, inactive, seasonal
    network_version_id INTEGER NOT NULL REFERENCES version(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    
    CONSTRAINT network_arc_new_unique_network_arc UNIQUE(network_id, arc_id_short_code),
    CONSTRAINT network_arc_new_operational_status_check CHECK(operational_status IN ('active', 'inactive', 'seasonal', 'planned'))
);

CREATE INDEX IF NOT EXISTS idx_network_arc_new_network_id ON network_arc_new(network_id);
CREATE INDEX IF NOT EXISTS idx_network_arc_new_arc_id ON network_arc_new(arc_id_short_code);
CREATE INDEX IF NOT EXISTS idx_network_arc_new_type ON network_arc_new(arc_type_id);
CREATE INDEX IF NOT EXISTS idx_network_arc_new_from_node ON network_arc_new(from_node_id);
CREATE INDEX IF NOT EXISTS idx_network_arc_new_to_node ON network_arc_new(to_node_id);

COMMENT ON TABLE network_arc_new IS 'Network arcs with updated ERD structure - channels, diversions, inflows';

-- =============================================================================
-- 3. NETWORK_NODE (Updated Structure)  
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_node_new (
    id SERIAL PRIMARY KEY,
    network_id INTEGER NOT NULL REFERENCES network(id),
    node_type_id INTEGER NOT NULL REFERENCES network_node_type(id),
    node_subtype_id INTEGER REFERENCES network_node_subtype(id),
    name VARCHAR,
    description TEXT,
    river_mile NUMERIC,
    river_name VARCHAR,
    elevation_ft NUMERIC,
    storage_capacity_taf NUMERIC,
    operational_status VARCHAR DEFAULT 'active',
    network_version_id INTEGER NOT NULL REFERENCES version(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    
    CONSTRAINT network_node_new_unique_network_node UNIQUE(network_id),
    CONSTRAINT network_node_new_operational_status_check CHECK(operational_status IN ('active', 'inactive', 'seasonal', 'planned'))
);

CREATE INDEX IF NOT EXISTS idx_network_node_new_network_id ON network_node_new(network_id);
CREATE INDEX IF NOT EXISTS idx_network_node_new_type ON network_node_new(node_type_id);
CREATE INDEX IF NOT EXISTS idx_network_node_new_river_name ON network_node_new(river_name);

COMMENT ON TABLE network_node_new IS 'Network nodes with updated ERD structure - junctions, reservoirs, boundaries';

-- =============================================================================
-- 4. NETWORK_OPERATIONAL_CONNECTIVITY
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_operational_connectivity (
    id SERIAL PRIMARY KEY,
    from_network_id INTEGER NOT NULL REFERENCES network(id),
    to_network_id INTEGER NOT NULL REFERENCES network(id),
    via_arc_network_id INTEGER REFERENCES network(id),
    connectivity_type VARCHAR NOT NULL,
    flow_direction VARCHAR DEFAULT 'downstream', -- 'upstream', 'downstream', 'bidirectional'
    operational_priority INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    seasonal_start_month INTEGER,
    seasonal_end_month INTEGER,
    network_version_id INTEGER NOT NULL REFERENCES version(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    
    CONSTRAINT network_connectivity_unique UNIQUE(from_network_id, to_network_id, via_arc_network_id),
    CONSTRAINT network_connectivity_type_check CHECK(connectivity_type IN ('direct', 'operational', 'seasonal', 'planned')),
    CONSTRAINT network_connectivity_direction_check CHECK(flow_direction IN ('upstream', 'downstream', 'bidirectional')),
    CONSTRAINT network_connectivity_seasonal_months CHECK(
        (seasonal_start_month IS NULL AND seasonal_end_month IS NULL) OR
        (seasonal_start_month BETWEEN 1 AND 12 AND seasonal_end_month BETWEEN 1 AND 12)
    ),
    CONSTRAINT network_connectivity_no_self_reference CHECK(from_network_id != to_network_id)
);

CREATE INDEX IF NOT EXISTS idx_network_connectivity_from ON network_operational_connectivity(from_network_id);
CREATE INDEX IF NOT EXISTS idx_network_connectivity_to ON network_operational_connectivity(to_network_id);
CREATE INDEX IF NOT EXISTS idx_network_connectivity_via_arc ON network_operational_connectivity(via_arc_network_id);
CREATE INDEX IF NOT EXISTS idx_network_connectivity_type ON network_operational_connectivity(connectivity_type);
CREATE INDEX IF NOT EXISTS idx_network_connectivity_active ON network_operational_connectivity(is_active);

COMMENT ON TABLE network_operational_connectivity IS 'Operational connectivity between network elements with seasonal and directional support';

-- =============================================================================
-- 5. NETWORK_GIS (Updated Structure)
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_gis_new (
    id SERIAL PRIMARY KEY,
    network_id INTEGER NOT NULL REFERENCES network(id),
    geometry_type_id INTEGER NOT NULL REFERENCES geometry_type(id),
    geom GEOMETRY NOT NULL,
    srid INTEGER DEFAULT 4326,
    coordinate_precision VARCHAR DEFAULT 'high', -- 'high', 'medium', 'low'
    data_quality_score NUMERIC DEFAULT 1.0 CHECK(data_quality_score BETWEEN 0.0 AND 1.0),
    validation_status VARCHAR DEFAULT 'validated', -- 'validated', 'pending', 'failed'
    source_dataset VARCHAR,
    network_version_id INTEGER NOT NULL REFERENCES version(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    
    CONSTRAINT network_gis_new_unique_network UNIQUE(network_id, geometry_type_id),
    CONSTRAINT network_gis_new_precision_check CHECK(coordinate_precision IN ('high', 'medium', 'low')),
    CONSTRAINT network_gis_new_validation_check CHECK(validation_status IN ('validated', 'pending', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_network_gis_new_network_id ON network_gis_new(network_id);
CREATE INDEX IF NOT EXISTS idx_network_gis_new_geom ON network_gis_new USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_network_gis_new_geometry_type ON network_gis_new(geometry_type_id);

COMMENT ON TABLE network_gis_new IS 'Spatial data for network elements with quality tracking';

-- =============================================================================
-- 6. ADD FOREIGN KEY CONSTRAINTS (After both arc and node tables exist)
-- =============================================================================

ALTER TABLE network_arc_new 
ADD CONSTRAINT fk_network_arc_from_node 
FOREIGN KEY (from_node_id) REFERENCES network_node_new(id);

ALTER TABLE network_arc_new 
ADD CONSTRAINT fk_network_arc_to_node 
FOREIGN KEY (to_node_id) REFERENCES network_node_new(id);

-- =============================================================================
-- 7. CREATE HELPER VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW v_network_complete AS
SELECT 
    n.id,
    n.short_code,
    net.short_code as entity_type,
    net.label as entity_type_label,
    n.has_gis,
    hr.short_code as hydrologic_region,
    vf.short_code as version_family,
    v.version_number,
    n.created_at,
    d.display_name as created_by_name
FROM network n
JOIN network_entity_type net ON n.entity_type_id = net.id
LEFT JOIN hydrologic_region hr ON n.hydrologic_region_id = hr.id
JOIN version v ON n.network_version_id = v.id
JOIN version_family vf ON v.version_family_id = vf.id
JOIN developer d ON n.created_by = d.id
ORDER BY n.short_code;

COMMENT ON VIEW v_network_complete IS 'Complete view of network elements with type and version information';

-- =============================================================================
-- COMPLETION MESSAGE
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ NEW NETWORK LAYER TABLES CREATED SUCCESSFULLY';
    RAISE NOTICE '📊 Tables created:';
    RAISE NOTICE '   - network (master registry)';
    RAISE NOTICE '   - network_arc_new (updated structure)';
    RAISE NOTICE '   - network_node_new (updated structure)';  
    RAISE NOTICE '   - network_operational_connectivity';
    RAISE NOTICE '   - network_gis_new (updated structure)';
    RAISE NOTICE '   - v_network_complete (helper view)';
    RAISE NOTICE '';
    RAISE NOTICE '🔄 Next steps:';
    RAISE NOTICE '   1. Create variable and tier tables';
    RAISE NOTICE '   2. Migrate data from old network tables';
    RAISE NOTICE '   3. Test new structure with sample data';
END $$;
