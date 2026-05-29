-- UPSERT NETWORK TYPE TABLES FROM S3
-- Clean implementation of the 4 network type tables loading from S3 bucket

-- =============================================================================
-- CREATE NETWORK TYPE TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_arc_type (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    label VARCHAR NOT NULL,
    description TEXT,
    network_entity_type_id INTEGER NOT NULL REFERENCES network_entity_type(id),
    model_source_id INTEGER REFERENCES model_source(id),
    source_id INTEGER REFERENCES source(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

CREATE TABLE IF NOT EXISTS network_node_type (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    label VARCHAR NOT NULL,
    description TEXT,
    network_entity_type_id INTEGER NOT NULL REFERENCES network_entity_type(id),
    model_source_id INTEGER REFERENCES model_source(id),
    source_id INTEGER REFERENCES source(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator() REFERENCES developer(id)
);

CREATE TABLE IF NOT EXISTS network_arc_subtype (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    label VARCHAR NOT NULL,
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

CREATE TABLE IF NOT EXISTS network_node_subtype (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    label VARCHAR NOT NULL,
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

-- =============================================================================
-- CREATE INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_network_arc_type_short_code ON network_arc_type(short_code);
CREATE INDEX IF NOT EXISTS idx_network_node_type_short_code ON network_node_type(short_code);
CREATE INDEX IF NOT EXISTS idx_network_arc_subtype_short_code ON network_arc_subtype(short_code);
CREATE INDEX IF NOT EXISTS idx_network_arc_subtype_type ON network_arc_subtype(arc_type_id);
CREATE INDEX IF NOT EXISTS idx_network_node_subtype_short_code ON network_node_subtype(short_code);
CREATE INDEX IF NOT EXISTS idx_network_node_subtype_type ON network_node_subtype(node_type_id);

-- =============================================================================
-- LOAD DATA FROM S3 (Parents First, Then Children)
-- =============================================================================

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
-- CREATE HELPER VIEWS
-- =============================================================================

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

-- =============================================================================
-- VALIDATION AND SUMMARY
-- =============================================================================

DO $$
DECLARE
    arc_types_count INTEGER;
    node_types_count INTEGER;
    arc_subtypes_count INTEGER;
    node_subtypes_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO arc_types_count FROM network_arc_type;
    SELECT COUNT(*) INTO node_types_count FROM network_node_type;
    SELECT COUNT(*) INTO arc_subtypes_count FROM network_arc_subtype;
    SELECT COUNT(*) INTO node_subtypes_count FROM network_node_subtype;
    
    RAISE NOTICE '✅ NETWORK TYPE TABLES LOADED FROM S3';
    RAISE NOTICE '';
    RAISE NOTICE '📊 RECORDS LOADED:';
    RAISE NOTICE '   - Arc Types: %', arc_types_count;
    RAISE NOTICE '   - Node Types: %', node_types_count;
    RAISE NOTICE '   - Arc Subtypes: %', arc_subtypes_count;
    RAISE NOTICE '   - Node Subtypes: %', node_subtypes_count;
    RAISE NOTICE '';
    RAISE NOTICE '🎯 CLEAN HIERARCHY STRUCTURE:';
    RAISE NOTICE '   Tier 1: network_entity_type (3 records: arc, node, null)';
    RAISE NOTICE '   Tier 2: types (% arc + % node types)', arc_types_count, node_types_count;
    RAISE NOTICE '   Tier 3: subtypes (% arc + % node subtypes)', arc_subtypes_count, node_subtypes_count;
    RAISE NOTICE '';
    RAISE NOTICE '👀 TEST THE HIERARCHY:';
    RAISE NOTICE '   SELECT * FROM v_network_arc_types_complete LIMIT 5;';
    RAISE NOTICE '   SELECT * FROM v_network_node_types_complete LIMIT 5;';
END $$;
