-- LOAD NETWORK_NODE TABLE FROM S3
-- =================================
-- Loads network_node.csv with node-specific physical attributes
-- Depends on: network table (for network_id FK via short_code lookup)

\echo ''
\echo '📍 LOADING NETWORK_NODE TABLE FROM S3'
\echo '======================================'

\echo ''
\echo '🏗️  Creating network_node table...'

CREATE TABLE IF NOT EXISTS network_node (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    network_id INTEGER NOT NULL,
    riv_mi NUMERIC,
    c2vsim_gw VARCHAR,
    c2vsim_sw VARCHAR,
    nrest_gage VARCHAR,
    strm_code VARCHAR,
    rm_ii VARCHAR,
    model_source_id INTEGER DEFAULT 1,
    source_id INTEGER DEFAULT 4,
    network_version_id INTEGER NOT NULL DEFAULT 12,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

\echo '✅ network_node table created'

\echo '🔗 Adding foreign key constraints...'

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_node_network_id_fkey' 
        AND table_name = 'network_node'
    ) THEN
        ALTER TABLE network_node 
        ADD CONSTRAINT network_node_network_id_fkey 
        FOREIGN KEY (network_id) 
        REFERENCES network(id) 
        ON DELETE CASCADE ON UPDATE CASCADE;
        
        ALTER TABLE network_node 
        ADD CONSTRAINT network_node_model_source_id_fkey 
        FOREIGN KEY (model_source_id) 
        REFERENCES model_source(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_node 
        ADD CONSTRAINT network_node_source_id_fkey 
        FOREIGN KEY (source_id) 
        REFERENCES source(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_node 
        ADD CONSTRAINT network_node_network_version_id_fkey 
        FOREIGN KEY (network_version_id) 
        REFERENCES version(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_node 
        ADD CONSTRAINT network_node_created_by_fkey 
        FOREIGN KEY (created_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_node 
        ADD CONSTRAINT network_node_updated_by_fkey 
        FOREIGN KEY (updated_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

\echo '✅ Foreign key constraints added'

\echo '📊 Creating indexes...'

CREATE UNIQUE INDEX IF NOT EXISTS network_node_short_code_key 
    ON network_node(short_code);

CREATE INDEX IF NOT EXISTS idx_network_node_network_id 
    ON network_node(network_id);

CREATE INDEX IF NOT EXISTS idx_network_node_strm_code 
    ON network_node(strm_code);

CREATE INDEX IF NOT EXISTS idx_network_node_version 
    ON network_node(network_version_id);

\echo '✅ Indexes created'

\echo ''
\echo '🧹 Clearing existing network_node data...'
TRUNCATE TABLE network_node CASCADE;
\echo '✅ Existing data cleared'

\echo ''
\echo '🔄 Creating staging table for network_id lookup...'

DROP TABLE IF EXISTS network_node_staging;

CREATE TEMP TABLE network_node_staging (
    short_code VARCHAR,
    network_id INTEGER,
    riv_mi NUMERIC,
    c2vsim_gw VARCHAR,
    c2vsim_sw VARCHAR,
    nrest_gage VARCHAR,
    strm_code VARCHAR,
    rm_ii VARCHAR,
    model_source_id INTEGER,
    source_id INTEGER,
    network_version_id INTEGER,
    is_active BOOLEAN
);

\echo '📥 Loading network_node.csv from S3 into staging...'

SELECT aws_s3.table_import_from_s3(
    'network_node_staging',
    'short_code, network_id, riv_mi, c2vsim_gw, c2vsim_sw, nrest_gage, strm_code, rm_ii, model_source_id, source_id, network_version_id, is_active',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '02_network/network_node.csv',
    'us-west-2'
);

\echo '✅ Data loaded into staging'

\echo ''
\echo '🔗 Looking up network_id from network table via short_code...'

UPDATE network_node_staging nns
SET network_id = n.id
FROM network n
WHERE nns.short_code = n.short_code;

\echo ''
\echo '🔍 Verifying network_id lookups...'

SELECT 
    COUNT(*) as total_records,
    COUNT(network_id) as successful_lookups,
    COUNT(*) - COUNT(network_id) as failed_lookups
FROM network_node_staging;

\echo ''
\echo '✅ Inserting into network_node table...'

INSERT INTO network_node (
    short_code, network_id, riv_mi, c2vsim_gw, c2vsim_sw, 
    nrest_gage, strm_code, rm_ii, model_source_id, source_id, 
    network_version_id, is_active
)
SELECT 
    short_code, network_id, riv_mi, c2vsim_gw, c2vsim_sw, 
    nrest_gage, strm_code, rm_ii, model_source_id, source_id, 
    network_version_id, is_active
FROM network_node_staging
WHERE network_id IS NOT NULL;

DROP TABLE network_node_staging;

\echo '✅ network_node data loaded'

\echo ''
\echo '🔍 VERIFYING NETWORK_NODE DATA'
\echo '=============================='

\echo ''
\echo '📊 Total node records:'
SELECT COUNT(*) as total_nodes FROM network_node;

\echo ''
\echo '📊 Nodes with attributes:'
SELECT 
    COUNT(*) as total,
    COUNT(riv_mi) as with_river_mile,
    COUNT(c2vsim_gw) as with_c2vsim_gw,
    COUNT(c2vsim_sw) as with_c2vsim_sw,
    COUNT(strm_code) as with_stream_code
FROM network_node;

\echo ''
\echo '📊 Top stream codes:'
SELECT 
    strm_code,
    COUNT(*) as node_count
FROM network_node
WHERE strm_code IS NOT NULL AND strm_code != ''
GROUP BY strm_code
ORDER BY node_count DESC
LIMIT 10;

\echo ''
\echo '📊 Sample records:'
SELECT 
    nn.short_code,
    nn.riv_mi,
    nn.strm_code,
    n.name,
    n.entity_type_id
FROM network_node nn
JOIN network n ON nn.network_id = n.id
LIMIT 5;

\echo ''
\echo '🎉 NETWORK_NODE TABLE SUCCESSFULLY LOADED!'
\echo '=========================================='
\echo 'Node-specific physical attributes now available!'

