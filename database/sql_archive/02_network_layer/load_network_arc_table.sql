-- LOAD NETWORK_ARC TABLE FROM S3
-- ================================
-- Loads network_arc.csv with arc-specific physical attributes
-- Depends on: network table (for network_id FK via short_code lookup)

\echo ''
\echo '🔀 LOADING NETWORK_ARC TABLE FROM S3'
\echo '====================================='

\echo ''
\echo '🏗️  Creating network_arc table...'

CREATE TABLE IF NOT EXISTS network_arc (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    network_id INTEGER NOT NULL,
    river VARCHAR,
    from_node VARCHAR,
    to_node VARCHAR,
    shape_length_m NUMERIC,
    model_source_id INTEGER DEFAULT 1,
    source_id INTEGER DEFAULT 4,
    network_version_id INTEGER NOT NULL DEFAULT 12,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

\echo '✅ network_arc table created'

\echo '🔗 Adding foreign key constraints...'

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_arc_network_id_fkey' 
        AND table_name = 'network_arc'
    ) THEN
        ALTER TABLE network_arc 
        ADD CONSTRAINT network_arc_network_id_fkey 
        FOREIGN KEY (network_id) 
        REFERENCES network(id) 
        ON DELETE CASCADE ON UPDATE CASCADE;
        
        ALTER TABLE network_arc 
        ADD CONSTRAINT network_arc_model_source_id_fkey 
        FOREIGN KEY (model_source_id) 
        REFERENCES model_source(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_arc 
        ADD CONSTRAINT network_arc_source_id_fkey 
        FOREIGN KEY (source_id) 
        REFERENCES source(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_arc 
        ADD CONSTRAINT network_arc_network_version_id_fkey 
        FOREIGN KEY (network_version_id) 
        REFERENCES version(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_arc 
        ADD CONSTRAINT network_arc_created_by_fkey 
        FOREIGN KEY (created_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_arc 
        ADD CONSTRAINT network_arc_updated_by_fkey 
        FOREIGN KEY (updated_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

\echo '✅ Foreign key constraints added'

\echo '📊 Creating indexes...'

CREATE UNIQUE INDEX IF NOT EXISTS network_arc_short_code_key 
    ON network_arc(short_code);

CREATE INDEX IF NOT EXISTS idx_network_arc_network_id 
    ON network_arc(network_id);

CREATE INDEX IF NOT EXISTS idx_network_arc_river 
    ON network_arc(river);

CREATE INDEX IF NOT EXISTS idx_network_arc_connectivity 
    ON network_arc(from_node, to_node);

CREATE INDEX IF NOT EXISTS idx_network_arc_version 
    ON network_arc(network_version_id);

\echo '✅ Indexes created'

\echo ''
\echo '🧹 Clearing existing network_arc data...'
TRUNCATE TABLE network_arc CASCADE;
\echo '✅ Existing data cleared'

\echo ''
\echo '🔄 Creating staging table for network_id lookup...'

DROP TABLE IF EXISTS network_arc_staging;

CREATE TEMP TABLE network_arc_staging (
    short_code VARCHAR,
    network_id INTEGER,
    river VARCHAR,
    from_node VARCHAR,
    to_node VARCHAR,
    shape_length_m NUMERIC,
    model_source_id INTEGER,
    source_id INTEGER,
    network_version_id INTEGER,
    is_active BOOLEAN
);

\echo '📥 Loading network_arc.csv from S3 into staging...'

SELECT aws_s3.table_import_from_s3(
    'network_arc_staging',
    'short_code, network_id, river, from_node, to_node, shape_length_m, model_source_id, source_id, network_version_id, is_active',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '02_network/network_arc.csv',
    'us-west-2'
);

\echo '✅ Data loaded into staging'

\echo ''
\echo '🔗 Looking up network_id from network table via short_code...'

UPDATE network_arc_staging nas
SET network_id = n.id
FROM network n
WHERE nas.short_code = n.short_code;

\echo ''
\echo '🔍 Verifying network_id lookups...'

SELECT 
    COUNT(*) as total_records,
    COUNT(network_id) as successful_lookups,
    COUNT(*) - COUNT(network_id) as failed_lookups
FROM network_arc_staging;

\echo ''
\echo '✅ Inserting into network_arc table...'

INSERT INTO network_arc (
    short_code, network_id, river, from_node, to_node, 
    shape_length_m, model_source_id, source_id, 
    network_version_id, is_active
)
SELECT 
    short_code, network_id, river, from_node, to_node, 
    shape_length_m, model_source_id, source_id, 
    network_version_id, is_active
FROM network_arc_staging
WHERE network_id IS NOT NULL;

DROP TABLE network_arc_staging;

\echo '✅ network_arc data loaded'

\echo ''
\echo '🔍 VERIFYING NETWORK_ARC DATA'
\echo '============================='

\echo ''
\echo '📊 Total arc records:'
SELECT COUNT(*) as total_arcs FROM network_arc;

\echo ''
\echo '📊 Arcs with attributes:'
SELECT 
    COUNT(*) as total,
    COUNT(river) as with_river,
    COUNT(from_node) as with_from_node,
    COUNT(to_node) as with_to_node,
    COUNT(shape_length_m) as with_length
FROM network_arc;

\echo ''
\echo '📊 Top river systems:'
SELECT 
    river,
    COUNT(*) as arc_count
FROM network_arc
WHERE river IS NOT NULL AND river != ''
GROUP BY river
ORDER BY arc_count DESC
LIMIT 10;

\echo ''
\echo '📊 Sample arc connectivity:'
SELECT 
    na.short_code,
    na.river,
    na.from_node,
    na.to_node,
    ROUND(na.shape_length_m::numeric, 2) as length_m,
    n.entity_type_id
FROM network_arc na
JOIN network n ON na.network_id = n.id
WHERE na.from_node IS NOT NULL AND na.to_node IS NOT NULL
LIMIT 5;

\echo ''
\echo '🎉 NETWORK_ARC TABLE SUCCESSFULLY LOADED!'
\echo '========================================='
\echo 'Arc connectivity and physical attributes now available!'

