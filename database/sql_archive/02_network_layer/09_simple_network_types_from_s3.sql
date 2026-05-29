-- SIMPLE NETWORK TYPE TABLES FROM S3
-- Clean implementation matching S3 file structure exactly

-- =============================================================================
-- CREATE TABLES WITH PROPER CONNECTIONS
-- =============================================================================

CREATE TABLE IF NOT EXISTS network_arc_type (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    label VARCHAR NOT NULL,
    description TEXT,
    network_entity_type_id INTEGER NOT NULL DEFAULT 1 REFERENCES network_entity_type(id),
    model_source_id INTEGER DEFAULT 1 REFERENCES model_source(id),
    source_id INTEGER DEFAULT 4 REFERENCES source(id),
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
    network_entity_type_id INTEGER NOT NULL DEFAULT 2 REFERENCES network_entity_type(id),
    model_source_id INTEGER DEFAULT 1 REFERENCES model_source(id),
    source_id INTEGER DEFAULT 4 REFERENCES source(id),
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
    arc_type_id INTEGER REFERENCES network_arc_type(id),
    model_source_id INTEGER DEFAULT 1 REFERENCES model_source(id),
    source_id INTEGER DEFAULT 4 REFERENCES source(id),
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
    node_type_id INTEGER REFERENCES network_node_type(id),
    model_source_id INTEGER DEFAULT 1 REFERENCES model_source(id),
    source_id INTEGER DEFAULT 4 REFERENCES source(id),
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
CREATE INDEX IF NOT EXISTS idx_network_arc_type_entity_type ON network_arc_type(network_entity_type_id);
CREATE INDEX IF NOT EXISTS idx_network_node_type_short_code ON network_node_type(short_code);
CREATE INDEX IF NOT EXISTS idx_network_node_type_entity_type ON network_node_type(network_entity_type_id);
CREATE INDEX IF NOT EXISTS idx_network_arc_subtype_short_code ON network_arc_subtype(short_code);
CREATE INDEX IF NOT EXISTS idx_network_arc_subtype_type ON network_arc_subtype(arc_type_id);
CREATE INDEX IF NOT EXISTS idx_network_node_subtype_short_code ON network_node_subtype(short_code);
CREATE INDEX IF NOT EXISTS idx_network_node_subtype_type ON network_node_subtype(node_type_id);

-- =============================================================================
-- LOAD DATA FROM S3 (Exact Column Match)
-- =============================================================================

COPY network_arc_type (short_code, label, description, network_entity_type_id, model_source_id, source_id, is_active)
FROM PROGRAM 'aws s3 cp s3://coeqwal-seeds-dev/01_network/network_arc_type.csv -'
WITH (FORMAT csv, HEADER true);

COPY network_node_type (short_code, label, description, network_entity_type_id, model_source_id, source_id, is_active)
FROM PROGRAM 'aws s3 cp s3://coeqwal-seeds-dev/01_network/network_node_type.csv -'
WITH (FORMAT csv, HEADER true);

COPY network_arc_subtype (short_code, label, description, arc_type_id, model_source_id, source_id, is_active)
FROM PROGRAM 'aws s3 cp s3://coeqwal-seeds-dev/01_network/network_arc_subtype.csv -'
WITH (FORMAT csv, HEADER true);

COPY network_node_subtype (short_code, label, description, node_type_id, model_source_id, source_id, is_active)
FROM PROGRAM 'aws s3 cp s3://coeqwal-seeds-dev/01_network/network_node_subtype.csv -'
WITH (FORMAT csv, HEADER true);

-- =============================================================================
-- CREATE HELPER VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW v_network_arc_types AS
SELECT 
    nat.id,
    nat.short_code,
    nat.label,
    nat.description,
    net.short_code as entity_type,
    nat.is_active
FROM network_arc_type nat
JOIN network_entity_type net ON nat.network_entity_type_id = net.id
ORDER BY nat.short_code;

CREATE OR REPLACE VIEW v_network_node_types AS
SELECT 
    nnt.id,
    nnt.short_code,
    nnt.label,
    nnt.description,
    net.short_code as entity_type,
    nnt.is_active
FROM network_node_type nnt
JOIN network_entity_type net ON nnt.network_entity_type_id = net.id
ORDER BY nnt.short_code;

CREATE OR REPLACE VIEW v_network_arc_subtypes AS
SELECT 
    nas.id,
    nas.short_code,
    nas.label,
    nas.description,
    nat.short_code as arc_type,
    nas.is_active
FROM network_arc_subtype nas
LEFT JOIN network_arc_type nat ON nas.arc_type_id = nat.id
ORDER BY nas.short_code;

CREATE OR REPLACE VIEW v_network_node_subtypes AS
SELECT 
    nns.id,
    nns.short_code,
    nns.label,
    nns.description,
    nnt.short_code as node_type,
    nns.is_active
FROM network_node_subtype nns
LEFT JOIN network_node_type nnt ON nns.node_type_id = nnt.id
ORDER BY nns.short_code;

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
    RAISE NOTICE '   - Arc Types: % (connected to network_entity_type.arc)', arc_types_count;
    RAISE NOTICE '   - Node Types: % (connected to network_entity_type.node)', node_types_count;
    RAISE NOTICE '   - Arc Subtypes: % (arc_type_id will be set later)', arc_subtypes_count;
    RAISE NOTICE '   - Node Subtypes: % (node_type_id will be set later)', node_subtypes_count;
    RAISE NOTICE '';
    RAISE NOTICE '👀 TEST THE TABLES:';
    RAISE NOTICE '   SELECT * FROM v_network_arc_types;';
    RAISE NOTICE '   SELECT * FROM v_network_node_types;';
    RAISE NOTICE '   SELECT * FROM v_network_arc_subtypes;';
    RAISE NOTICE '   SELECT * FROM v_network_node_subtypes;';
END $$;
