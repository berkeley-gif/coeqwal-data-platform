-- REDESIGN NETWORK TYPE HIERARCHY - ELIMINATE CALSIM_ENTITY_TYPE OVERLAP
-- Create proper type/subtype hierarchy using network_arc_type and network_node_type

-- =============================================================================
-- 1. CREATE NETWORK_ARC_TYPE_CATEGORY (Parent Types)
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_arc_type_category (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    description TEXT,
    network_entity_type_id INTEGER NOT NULL REFERENCES network_entity_type(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

INSERT INTO network_arc_type_category (short_code, name, description, network_entity_type_id) VALUES
('CH', 'Channel', 'River channels, canals, and waterways', (SELECT id FROM network_entity_type WHERE short_code = 'arc')),
('D', 'Diversion', 'Water diversions and withdrawals', (SELECT id FROM network_entity_type WHERE short_code = 'arc')),
('IN', 'Inflow', 'Inflows and water sources', (SELECT id FROM network_entity_type WHERE short_code = 'arc')),
('RT', 'Return Flow', 'Return flows and drainage', (SELECT id FROM network_entity_type WHERE short_code = 'arc')),
('SP', 'Spillway', 'Spillways and overflows', (SELECT id FROM network_entity_type WHERE short_code = 'arc')),
('SR', 'Source', 'Water sources and origins', (SELECT id FROM network_entity_type WHERE short_code = 'arc')),
('CT', 'Control', 'Flow control structures', (SELECT id FROM network_entity_type WHERE short_code = 'arc'))
ON CONFLICT (short_code) DO NOTHING;

-- =============================================================================
-- 2. CREATE NETWORK_NODE_TYPE_CATEGORY (Parent Types)
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_node_type_category (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    description TEXT,
    network_entity_type_id INTEGER NOT NULL REFERENCES network_entity_type(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

INSERT INTO network_node_type_category (short_code, name, description, network_entity_type_id) VALUES
('CH', 'Channel Node', 'Channel junctions and stream nodes', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('STR', 'Storage', 'Reservoirs and storage facilities', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('PS', 'Pump Station', 'Pumping facilities', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('NP', 'Non-Project', 'Non-project demand units', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('PR', 'Project', 'Project demand units', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('OM', 'Off-Model', 'Off-model connections', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('RFS', 'Return Flow Station', 'Return flow discharge points', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('S', 'Source', 'Water source nodes', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('WTP', 'Water Treatment Plant', 'Water treatment facilities', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('WWTP', 'Wastewater Treatment Plant', 'Wastewater treatment facilities', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('JUNC', 'Junction', 'Network junctions', (SELECT id FROM network_entity_type WHERE short_code = 'node')),
('INFR', 'Infrastructure', 'Infrastructure nodes', (SELECT id FROM network_entity_type WHERE short_code = 'node'))
ON CONFLICT (short_code) DO NOTHING;

-- =============================================================================
-- 3. UPDATE NETWORK_ARC_TYPE - ADD CATEGORY REFERENCE
-- =============================================================================

ALTER TABLE network_arc_type 
ADD COLUMN IF NOT EXISTS arc_type_category_id INTEGER REFERENCES network_arc_type_category(id);

UPDATE network_arc_type 
SET arc_type_category_id = (
    SELECT natc.id 
    FROM network_arc_type_category natc 
    WHERE network_arc_type.short_code LIKE natc.short_code || '%'
    LIMIT 1
);

ALTER TABLE network_arc_type 
ADD COLUMN IF NOT EXISTS subtype_code VARCHAR;

UPDATE network_arc_type 
SET subtype_code = CASE 
    WHEN position('-' in short_code) > 0 
    THEN substring(short_code from position('-' in short_code) + 1)
    ELSE 'NA'
END;

-- =============================================================================
-- 4. UPDATE NETWORK_NODE_TYPE - ADD CATEGORY REFERENCE  
-- =============================================================================

ALTER TABLE network_node_type 
ADD COLUMN IF NOT EXISTS node_type_category_id INTEGER REFERENCES network_node_type_category(id);

UPDATE network_node_type 
SET node_type_category_id = (
    SELECT nntc.id 
    FROM network_node_type_category nntc 
    WHERE network_node_type.short_code LIKE nntc.short_code || '%'
    LIMIT 1
);

ALTER TABLE network_node_type 
ADD COLUMN IF NOT EXISTS subtype_code VARCHAR;

UPDATE network_node_type 
SET subtype_code = CASE 
    WHEN position('-' in short_code) > 0 
    THEN substring(short_code from position('-' in short_code) + 1)
    ELSE 'NA'
END;

-- =============================================================================
-- 5. CREATE SUBTYPE LOOKUP TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_arc_subtype_lookup (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

INSERT INTO network_arc_subtype_lookup (short_code, name, description) VALUES
('ST', 'Stream', 'Natural stream or river'),
('CL', 'Canal', 'Constructed canal or channel'),
('BP', 'Bypass', 'Bypass or alternate route'),
('HIS', 'Historical', 'Historical or legacy connection'),
('NA', 'Not Applicable', 'Generic or unspecified subtype'),
('NS', 'Non-Simulated', 'Non-simulated connection'),
('PRP', 'Pre-Project', 'Pre-project condition'),
('IM', 'Import', 'Imported water'),
('LI', 'Lateral Inflow', 'Lateral inflow connection')
ON CONFLICT (short_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS network_node_subtype_lookup (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

INSERT INTO network_node_subtype_lookup (short_code, name, description) VALUES
('BYP', 'Bypass', 'Bypass or flood control'),
('CNL', 'Canal', 'Canal junction or node'),
('GWO', 'Groundwater Outflow', 'Groundwater discharge point'),
('SIM', 'Simulated', 'Simulated in model'),
('NSM', 'Non-Simulated', 'Not simulated in model'),
('SG', 'Stream Gage', 'Stream flow measurement station'),
('STM', 'Stream', 'Natural stream node'),
('A', 'Agricultural', 'Agricultural demand'),
('U', 'Urban', 'Urban demand'),
('R', 'Return', 'Return flow point'),
('OMD', 'Off-Model Demand', 'External demand'),
('OMDD', 'Off-Model Direct Diversion', 'External diversion'),
('OMR', 'Off-Model Return', 'External return flow'),
('PRP', 'Pre-Project', 'Pre-project condition'),
('NA', 'Not Applicable', 'Generic or unspecified subtype')
ON CONFLICT (short_code) DO NOTHING;

-- =============================================================================
-- 6. LINK SUBTYPES TO EXISTING TYPES
-- =============================================================================

ALTER TABLE network_arc_type 
ADD COLUMN IF NOT EXISTS subtype_lookup_id INTEGER REFERENCES network_arc_subtype_lookup(id);

UPDATE network_arc_type 
SET subtype_lookup_id = (
    SELECT nasl.id 
    FROM network_arc_subtype_lookup nasl 
    WHERE network_arc_type.subtype_code = nasl.short_code
    LIMIT 1
);

ALTER TABLE network_node_type 
ADD COLUMN IF NOT EXISTS subtype_lookup_id INTEGER REFERENCES network_node_subtype_lookup(id);

UPDATE network_node_type 
SET subtype_lookup_id = (
    SELECT nnsl.id 
    FROM network_node_subtype_lookup nnsl 
    WHERE network_node_type.subtype_code = nnsl.short_code
    LIMIT 1
);

-- =============================================================================
-- 7. CREATE COMPREHENSIVE VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW v_network_arc_types_complete AS
SELECT 
    nat.id,
    nat.short_code as full_code,
    natc.short_code as type_code,
    natc.name as type_name,
    nat.subtype_code,
    nasl.name as subtype_name,
    nat.name as full_name,
    nat.description,
    nat.is_active
FROM network_arc_type nat
JOIN network_arc_type_category natc ON nat.arc_type_category_id = natc.id
LEFT JOIN network_arc_subtype_lookup nasl ON nat.subtype_lookup_id = nasl.id
ORDER BY natc.short_code, nat.subtype_code;

CREATE OR REPLACE VIEW v_network_node_types_complete AS
SELECT 
    nnt.id,
    nnt.short_code as full_code,
    nntc.short_code as type_code,
    nntc.name as type_name,
    nnt.subtype_code,
    nnsl.name as subtype_name,
    nnt.name as full_name,
    nnt.description,
    nnt.is_active
FROM network_node_type nnt
JOIN network_node_type_category nntc ON nnt.node_type_category_id = nntc.id
LEFT JOIN network_node_subtype_lookup nnsl ON nnt.subtype_lookup_id = nnsl.id
ORDER BY nntc.short_code, nnt.subtype_code;

-- =============================================================================
-- 8. UPDATE NETWORK TABLES TO USE NEW STRUCTURE
-- =============================================================================

ALTER TABLE network_arc_new 
DROP COLUMN IF EXISTS arc_subtype_id;

ALTER TABLE network_arc_new 
ADD COLUMN IF NOT EXISTS arc_type_category_id INTEGER REFERENCES network_arc_type_category(id),
ADD COLUMN IF NOT EXISTS arc_subtype_lookup_id INTEGER REFERENCES network_arc_subtype_lookup(id);

ALTER TABLE network_node_new 
DROP COLUMN IF EXISTS node_subtype_id;

ALTER TABLE network_node_new 
ADD COLUMN IF NOT EXISTS node_type_category_id INTEGER REFERENCES network_node_type_category(id),
ADD COLUMN IF NOT EXISTS node_subtype_lookup_id INTEGER REFERENCES network_node_subtype_lookup(id);

-- =============================================================================
-- 9. MIGRATION HELPER - MAP CALSIM ENTITY TYPES TO NEW STRUCTURE
-- =============================================================================

CREATE TABLE IF NOT EXISTS calsim_entity_type_migration_map (
    old_calsim_type VARCHAR,
    new_entity_type VARCHAR,
    new_type_category VARCHAR,
    new_subtype VARCHAR,
    notes TEXT
);

INSERT INTO calsim_entity_type_migration_map VALUES
('reservoir', 'node', 'STR', 'SIM', 'Reservoir -> Storage/Simulated'),
('channel', 'arc', 'CH', 'ST', 'Channel -> Channel/Stream'),
('inflow', 'arc', 'IN', 'LI', 'Inflow -> Inflow/Lateral Inflow'),
('demand_unit_agriculture', 'node', 'NP', 'A', 'Ag DU -> Non-Project/Agricultural'),
('demand_unit_urban', 'node', 'NP', 'U', 'Urban DU -> Non-Project/Urban'),
('demand_unit_refuge', 'node', 'NP', 'R', 'Refuge DU -> Non-Project/Return'),
('groundwater', 'node', 'CH', 'GWO', 'Groundwater -> Channel/Groundwater Outflow'),
('salinity_node', 'node', 'CH', 'STM', 'Salinity -> Channel/Stream'),
('delta_outflow', 'arc', 'CH', 'ST', 'Delta Outflow -> Channel/Stream'),
('delta_export', 'arc', 'D', 'CL', 'Delta Export -> Diversion/Canal'),
('infrastructure', 'node', 'INFR', 'NA', 'Infrastructure -> Infrastructure/NA'),
('junction', 'node', 'JUNC', 'NA', 'Junction -> Junction/NA'),
('flow_management', 'node', 'INFR', 'NA', 'Flow Management -> Infrastructure/NA');

-- =============================================================================
-- COMPLETION MESSAGE
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ NETWORK TYPE HIERARCHY REDESIGNED SUCCESSFULLY';
    RAISE NOTICE '📊 New structure:';
    RAISE NOTICE '   - network_entity_type (arc/node/null) - TOP LEVEL';
    RAISE NOTICE '   - network_arc_type_category + network_node_type_category - TYPE LEVEL';
    RAISE NOTICE '   - network_arc_subtype_lookup + network_node_subtype_lookup - SUBTYPE LEVEL';
    RAISE NOTICE '   - Updated network_arc_type + network_node_type - FULL HIERARCHY';
    RAISE NOTICE '';
    RAISE NOTICE '🔄 Next steps:';
    RAISE NOTICE '   1. Drop calsim_entity_type table (after migration)';
    RAISE NOTICE '   2. Update network_arc_new and network_node_new tables';
    RAISE NOTICE '   3. Test the new hierarchy with sample data';
    RAISE NOTICE '';
    RAISE NOTICE '📋 Check results:';
    RAISE NOTICE '   SELECT * FROM v_network_arc_types_complete;';
    RAISE NOTICE '   SELECT * FROM v_network_node_types_complete;';
END $$;
