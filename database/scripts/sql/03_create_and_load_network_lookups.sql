-- CREATE AND LOAD NETWORK TYPE AND SUBTYPE LOOKUP TABLES
-- Creates tables and loads data from CSV seed files
-- Run from project root: \i database/scripts/sql/03_create_and_load_network_lookups.sql

\set ON_ERROR_STOP on

\echo '============================================================================'
\echo 'CREATING AND LOADING NETWORK TYPE AND SUBTYPE LOOKUP TABLES'
\echo '============================================================================'

-- Create network_arc_type table
\echo ''
\echo '1. CREATING NETWORK_ARC_TYPE TABLE:'
\echo '-----------------------------------'

CREATE TABLE IF NOT EXISTS network_arc_type (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_network_arc_type_short_code ON network_arc_type(short_code);
CREATE INDEX IF NOT EXISTS idx_network_arc_type_active ON network_arc_type(is_active, short_code);

-- Create network_arc_subtype table
\echo ''
\echo '2. CREATING NETWORK_ARC_SUBTYPE TABLE:'
\echo '-------------------------------------'

CREATE TABLE IF NOT EXISTS network_arc_subtype (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_network_arc_subtype_short_code ON network_arc_subtype(short_code);
CREATE INDEX IF NOT EXISTS idx_network_arc_subtype_active ON network_arc_subtype(is_active, short_code);

-- Create network_node_type table
\echo ''
\echo '3. CREATING NETWORK_NODE_TYPE TABLE:'
\echo '------------------------------------'

CREATE TABLE IF NOT EXISTS network_node_type (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_network_node_type_short_code ON network_node_type(short_code);
CREATE INDEX IF NOT EXISTS idx_network_node_type_active ON network_node_type(is_active, short_code);

-- Create network_node_subtype table
\echo ''
\echo '4. CREATING NETWORK_NODE_SUBTYPE TABLE:'
\echo '--------------------------------------'

CREATE TABLE IF NOT EXISTS network_node_subtype (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_network_node_subtype_short_code ON network_node_subtype(short_code);
CREATE INDEX IF NOT EXISTS idx_network_node_subtype_active ON network_node_subtype(is_active, short_code);

-- Load data from CSV files
\echo ''
\echo '5. LOADING DATA FROM CSV SEED FILES:'
\echo '-----------------------------------'

\echo 'Loading network_arc_type...'
\copy network_arc_type(short_code, label, description, is_active) FROM 'database/seed_tables/05_network_lookups/network_arc_type.csv' WITH CSV HEADER;

\echo 'Loading network_arc_subtype...'
\copy network_arc_subtype(short_code, label, description, is_active) FROM 'database/seed_tables/05_network_lookups/network_arc_subtype.csv' WITH CSV HEADER;

\echo 'Loading network_node_type...'
\copy network_node_type(short_code, label, description, is_active) FROM 'database/seed_tables/05_network_lookups/network_node_type.csv' WITH CSV HEADER;

\echo 'Loading network_node_subtype...'
\copy network_node_subtype(short_code, label, description, is_active) FROM 'database/seed_tables/05_network_lookups/network_node_subtype.csv' WITH CSV HEADER;

-- Add foreign key constraints
\echo ''
\echo '6. ADDING FOREIGN KEY CONSTRAINTS:'
\echo '---------------------------------'

ALTER TABLE network_arc_type ADD CONSTRAINT IF NOT EXISTS fk_network_arc_type_created_by
    FOREIGN KEY (created_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;
    
ALTER TABLE network_arc_type ADD CONSTRAINT IF NOT EXISTS fk_network_arc_type_updated_by
    FOREIGN KEY (updated_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE network_arc_subtype ADD CONSTRAINT IF NOT EXISTS fk_network_arc_subtype_created_by
    FOREIGN KEY (created_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;
    
ALTER TABLE network_arc_subtype ADD CONSTRAINT IF NOT EXISTS fk_network_arc_subtype_updated_by
    FOREIGN KEY (updated_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE network_node_type ADD CONSTRAINT IF NOT EXISTS fk_network_node_type_created_by
    FOREIGN KEY (created_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;
    
ALTER TABLE network_node_type ADD CONSTRAINT IF NOT EXISTS fk_network_node_type_updated_by
    FOREIGN KEY (updated_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE network_node_subtype ADD CONSTRAINT IF NOT EXISTS fk_network_node_subtype_created_by
    FOREIGN KEY (created_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;
    
ALTER TABLE network_node_subtype ADD CONSTRAINT IF NOT EXISTS fk_network_node_subtype_updated_by
    FOREIGN KEY (updated_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;

-- Verify results
\echo ''
\echo '7. VERIFICATION:'
\echo '---------------'

\echo 'Arc types loaded:'
SELECT COUNT(*) as count, 'network_arc_type' as table_name FROM network_arc_type;
SELECT short_code, label FROM network_arc_type ORDER BY id LIMIT 5;

\echo ''
\echo 'Arc subtypes loaded:'
SELECT COUNT(*) as count, 'network_arc_subtype' as table_name FROM network_arc_subtype;
SELECT short_code, label FROM network_arc_subtype ORDER BY id LIMIT 5;

\echo ''
\echo 'Node types loaded:'
SELECT COUNT(*) as count, 'network_node_type' as table_name FROM network_node_type;
SELECT short_code, label FROM network_node_type ORDER BY id LIMIT 5;

\echo ''
\echo 'Node subtypes loaded:'
SELECT COUNT(*) as count, 'network_node_subtype' as table_name FROM network_node_subtype;
SELECT short_code, label FROM network_node_subtype ORDER BY id LIMIT 5;

-- Summary
\echo ''
\echo 'Summary:'
SELECT 
    'network_arc_type' as table_name,
    COUNT(*) as records
FROM network_arc_type
UNION ALL
SELECT 
    'network_arc_subtype' as table_name,
    COUNT(*) as records
FROM network_arc_subtype
UNION ALL
SELECT 
    'network_node_type' as table_name,
    COUNT(*) as records
FROM network_node_type
UNION ALL
SELECT 
    'network_node_subtype' as table_name,
    COUNT(*) as records
FROM network_node_subtype;

\echo ''
\echo 'NETWORK TYPE AND SUBTYPE LOOKUP TABLES CREATED AND LOADED'
\echo '============================================================================'
