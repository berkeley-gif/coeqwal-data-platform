-- IMPLEMENT CLEAN NETWORK ARCHITECTURE - ELIMINATE OVERLAPPING TABLES
-- This script removes the overlapping calsim_entity_type and old network type tables
-- and implements the clean 3-tier hierarchy documented in the ERD

-- =============================================================================
-- PHASE 1: BACKUP AND DROP OVERLAPPING TABLES
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '🗑️  PHASE 1: REMOVING OVERLAPPING TABLES';
    RAISE NOTICE '   - Backing up data from overlapping tables';
    RAISE NOTICE '   - Dropping calsim_entity_type (replaced by network type hierarchy)';
    RAISE NOTICE '   - Dropping old network_arc_type and network_node_type';
END $$;

CREATE TABLE IF NOT EXISTS _backup_calsim_entity_type AS 
SELECT * FROM calsim_entity_type;

CREATE TABLE IF NOT EXISTS _backup_network_arc_type AS 
SELECT * FROM network_arc_type;

CREATE TABLE IF NOT EXISTS _backup_network_node_type AS 
SELECT * FROM network_node_type;

DROP TABLE IF EXISTS calsim_entity_type CASCADE;
DROP TABLE IF EXISTS network_arc_type CASCADE;  
DROP TABLE IF EXISTS network_node_type CASCADE;

DROP TABLE IF EXISTS network_arc_subtype CASCADE;
DROP TABLE IF EXISTS network_node_subtype CASCADE;

-- =============================================================================
-- PHASE 2: CREATE CLEAN NETWORK TYPE HIERARCHY
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '🏗️  PHASE 2: CREATING CLEAN NETWORK TYPE HIERARCHY';
    RAISE NOTICE '   - Tier 1: network_entity_type (already exists)';
    RAISE NOTICE '   - Tier 2: network_arc_subtype + network_node_subtype';
    RAISE NOTICE '   - Tier 3: network_arc_type + network_node_type';
END $$;

-- =============================================================================
-- TIER 2: SUBTYPES (Detail Level)
-- =============================================================================

CREATE TABLE network_arc_type (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    description TEXT,
    model_source_id INTEGER REFERENCES model_source(id),
    source_id INTEGER REFERENCES source(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

CREATE TABLE network_node_type (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    description TEXT,
    model_source_id INTEGER REFERENCES model_source(id),
    source_id INTEGER REFERENCES source(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

CREATE TABLE network_arc_subtype (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    description TEXT,
    arc_type_id INTEGER NOT NULL REFERENCES network_arc_type(id),
    model_source_id INTEGER REFERENCES model_source(id),
    source_id INTEGER REFERENCES source(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

CREATE TABLE network_node_subtype (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    description TEXT,
    node_type_id INTEGER NOT NULL REFERENCES network_node_type(id),
    model_source_id INTEGER REFERENCES model_source(id),
    source_id INTEGER REFERENCES source(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

CREATE INDEX idx_network_arc_type_short_code ON network_arc_type(short_code);
CREATE INDEX idx_network_node_type_short_code ON network_node_type(short_code);
CREATE INDEX idx_network_arc_subtype_short_code ON network_arc_subtype(short_code);
CREATE INDEX idx_network_arc_subtype_type ON network_arc_subtype(arc_type_id);
CREATE INDEX idx_network_node_subtype_short_code ON network_node_subtype(short_code);
CREATE INDEX idx_network_node_subtype_type ON network_node_subtype(node_type_id);

-- =============================================================================
-- PHASE 3: LOAD SEED DATA FROM CSV FILES
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '🌱 PHASE 3: LOADING SEED DATA FROM CSV FILES';
    RAISE NOTICE '   - Loading arc types first (parents)';
    RAISE NOTICE '   - Loading node types first (parents)';
    RAISE NOTICE '   - Loading arc subtypes (children)';
    RAISE NOTICE '   - Loading node subtypes (children)';
END $$;

COPY network_arc_type (short_code, name, description, model_source_id, source_id)
FROM PROGRAM 'aws s3 cp s3://coeqwal-seeds-dev/01_network/network_arc_type.csv -'
WITH (FORMAT csv, HEADER true);

COPY network_node_type (short_code, name, description, model_source_id, source_id)
FROM PROGRAM 'aws s3 cp s3://coeqwal-seeds-dev/01_network/network_node_type.csv -'
WITH (FORMAT csv, HEADER true);

COPY network_arc_subtype (short_code, name, description, arc_type_id, model_source_id, source_id)
FROM PROGRAM 'aws s3 cp s3://coeqwal-seeds-dev/01_network/network_arc_subtype.csv -'
WITH (FORMAT csv, HEADER true);

COPY network_node_subtype (short_code, name, description, node_type_id, model_source_id, source_id)
FROM PROGRAM 'aws s3 cp s3://coeqwal-seeds-dev/01_network/network_node_subtype.csv -'
WITH (FORMAT csv, HEADER true);

-- =============================================================================
-- PHASE 4: CREATE HELPER VIEWS
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '👀 PHASE 4: CREATING HELPER VIEWS';
    RAISE NOTICE '   - v_network_arc_types_complete';
    RAISE NOTICE '   - v_network_node_types_complete';
    RAISE NOTICE '   - v_calsim_entity_migration_map';
END $$;

CREATE OR REPLACE VIEW v_network_arc_types_complete AS
SELECT 
    nat.id as type_id,
    nat.short_code as type_code,
    nat.name as type_name,
    nat.description as type_description,
    nas.id as subtype_id,
    nas.short_code as subtype_code,
    nas.name as subtype_name,
    nas.description as subtype_description,
    CONCAT(nat.short_code, '-', nas.short_code) as full_code,
    nat.is_active as type_active,
    nas.is_active as subtype_active,
    'arc' as entity_type
FROM network_arc_type nat
LEFT JOIN network_arc_subtype nas ON nas.arc_type_id = nat.id
ORDER BY nat.short_code, nas.short_code;

CREATE OR REPLACE VIEW v_network_node_types_complete AS
SELECT 
    nnt.id as type_id,
    nnt.short_code as type_code,
    nnt.name as type_name,
    nnt.description as type_description,
    nns.id as subtype_id,
    nns.short_code as subtype_code,
    nns.name as subtype_name,
    nns.description as subtype_description,
    CONCAT(nnt.short_code, '-', nns.short_code) as full_code,
    nnt.is_active as type_active,
    nns.is_active as subtype_active,
    'node' as entity_type
FROM network_node_type nnt
LEFT JOIN network_node_subtype nns ON nns.node_type_id = nnt.id
ORDER BY nnt.short_code, nns.short_code;

CREATE OR REPLACE VIEW v_calsim_entity_migration_map AS
SELECT 
    'reservoir' as old_calsim_type,
    'node' as new_entity_type,
    'STR' as new_type_category,
    'SIM' as new_subtype,
    'STR-SIM' as new_full_code,
    'Reservoir -> Storage/Simulated' as notes
UNION ALL
SELECT 'channel', 'arc', 'CH', 'ST', 'CH-ST', 'Channel -> Channel/Stream'
UNION ALL
SELECT 'inflow', 'arc', 'IN', 'LI', 'IN-LI', 'Inflow -> Inflow/Lateral Inflow'
UNION ALL
SELECT 'demand_unit_agriculture', 'node', 'NP', 'A', 'NP-A', 'Ag DU -> Non-Project/Agricultural'
UNION ALL
SELECT 'demand_unit_urban', 'node', 'NP', 'U', 'NP-U', 'Urban DU -> Non-Project/Urban'
UNION ALL
SELECT 'demand_unit_refuge', 'node', 'NP', 'R', 'NP-R', 'Refuge DU -> Non-Project/Return'
UNION ALL
SELECT 'groundwater', 'node', 'CH', 'GWO', 'CH-GWO', 'Groundwater -> Channel/Groundwater Outflow'
UNION ALL
SELECT 'salinity_node', 'node', 'CH', 'STM', 'CH-STM', 'Salinity -> Channel/Stream'
UNION ALL
SELECT 'delta_outflow', 'arc', 'CH', 'ST', 'CH-ST', 'Delta Outflow -> Channel/Stream'
UNION ALL
SELECT 'delta_export', 'arc', 'D', 'CL', 'D-CL', 'Delta Export -> Diversion/Canal'
UNION ALL
SELECT 'infrastructure', 'node', 'INFR', 'NA', 'INFR', 'Infrastructure -> Infrastructure/NA'
UNION ALL
SELECT 'junction', 'node', 'JUNC', 'NA', 'JUNC', 'Junction -> Junction/NA'
UNION ALL
SELECT 'flow_management', 'node', 'INFR', 'NA', 'INFR', 'Flow Management -> Infrastructure/NA';

-- =============================================================================
-- PHASE 5: VALIDATION AND SUMMARY
-- =============================================================================

DO $$
DECLARE
    arc_subtypes_count INTEGER;
    node_subtypes_count INTEGER;
    arc_types_count INTEGER;
    node_types_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO arc_subtypes_count FROM network_arc_subtype;
    SELECT COUNT(*) INTO node_subtypes_count FROM network_node_subtype;
    SELECT COUNT(*) INTO arc_types_count FROM network_arc_type;
    SELECT COUNT(*) INTO node_types_count FROM network_node_type;
    
    RAISE NOTICE '✅ CLEAN NETWORK ARCHITECTURE IMPLEMENTED SUCCESSFULLY';
    RAISE NOTICE '';
    RAISE NOTICE '📊 RECORDS LOADED:';
    RAISE NOTICE '   - Arc Subtypes: %', arc_subtypes_count;
    RAISE NOTICE '   - Node Subtypes: %', node_subtypes_count;
    RAISE NOTICE '   - Complete Arc Types: %', arc_types_count;
    RAISE NOTICE '   - Complete Node Types: %', node_types_count;
    RAISE NOTICE '';
    RAISE NOTICE '🎯 CLEAN 3-TIER STRUCTURE:';
    RAISE NOTICE '   Tier 1: network_entity_type (3 records: arc, node, null)';
    RAISE NOTICE '   Tier 2: subtypes (% arc + % node subtypes)', arc_subtypes_count, node_subtypes_count;
    RAISE NOTICE '   Tier 3: full_types (% arc + % node types)', arc_types_count, node_types_count;
    RAISE NOTICE '';
    RAISE NOTICE '👀 TEST THE NEW HIERARCHY:';
    RAISE NOTICE '   SELECT * FROM v_network_arc_types_complete LIMIT 5;';
    RAISE NOTICE '   SELECT * FROM v_network_node_types_complete LIMIT 5;';
    RAISE NOTICE '   SELECT * FROM v_calsim_entity_migration_map;';
    RAISE NOTICE '';
    RAISE NOTICE '🗑️  BACKUP TABLES CREATED:';
    RAISE NOTICE '   - _backup_calsim_entity_type';
    RAISE NOTICE '   - _backup_network_arc_type';
    RAISE NOTICE '   - _backup_network_node_type';
    RAISE NOTICE '   (Drop these after confirming migration success)';
END $$;
