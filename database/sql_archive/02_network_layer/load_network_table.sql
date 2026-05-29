-- LOAD NETWORK TABLE FROM S3
-- ============================
-- Loads network.csv into the network table with full enterprise features:
-- - Audit metadata using coeqwal_current_operator() function
-- - Versioning (network family, version_id=12)
-- - Foreign keys to lookup tables
-- - Performance indexes (including GIN for arrays)
-- - Data validation

\echo ''
\echo '🌐 LOADING NETWORK TABLE FROM S3'
\echo '================================='

\echo ''
\echo '📋 Checking if network table exists...'
SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name = 'network'
) as table_exists;

\echo ''
\echo '🏗️  Creating network table...'

CREATE TABLE IF NOT EXISTS network (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR,
    description TEXT,
    comment TEXT,
    entity_type_id INTEGER NOT NULL,
    type_id INTEGER,
    subtype_ids INTEGER[],
    model_list INTEGER[] NOT NULL,
    source_list INTEGER[] NOT NULL,
    has_gis BOOLEAN DEFAULT FALSE,
    hydrologic_region_id INTEGER,
    riv_sys VARCHAR,
    strm_code VARCHAR,
    network_version_id INTEGER NOT NULL DEFAULT 12,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    
    CONSTRAINT network_model_list_not_empty CHECK (array_length(model_list, 1) > 0),
    CONSTRAINT network_source_list_not_empty CHECK (array_length(source_list, 1) > 0)
);

\echo '✅ Network table created'

\echo ''
\echo '🔗 Adding foreign key constraints...'

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_entity_type_id_fkey' 
        AND table_name = 'network'
    ) THEN
        ALTER TABLE network 
        ADD CONSTRAINT network_entity_type_id_fkey 
        FOREIGN KEY (entity_type_id) 
        REFERENCES network_entity_type(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_type_id_fkey' 
        AND table_name = 'network'
    ) THEN
        ALTER TABLE network 
        ADD CONSTRAINT network_type_id_fkey 
        FOREIGN KEY (type_id) 
        REFERENCES network_type(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_hydrologic_region_id_fkey' 
        AND table_name = 'network'
    ) THEN
        ALTER TABLE network 
        ADD CONSTRAINT network_hydrologic_region_id_fkey 
        FOREIGN KEY (hydrologic_region_id) 
        REFERENCES hydrologic_region(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_network_version_id_fkey' 
        AND table_name = 'network'
    ) THEN
        ALTER TABLE network 
        ADD CONSTRAINT network_network_version_id_fkey 
        FOREIGN KEY (network_version_id) 
        REFERENCES version(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_created_by_fkey' 
        AND table_name = 'network'
    ) THEN
        ALTER TABLE network 
        ADD CONSTRAINT network_created_by_fkey 
        FOREIGN KEY (created_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network 
        ADD CONSTRAINT network_updated_by_fkey 
        FOREIGN KEY (updated_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

\echo '✅ Foreign key constraints added'

\echo ''
\echo '📊 Creating performance indexes...'

CREATE UNIQUE INDEX IF NOT EXISTS network_short_code_key 
    ON network(short_code);

CREATE INDEX IF NOT EXISTS idx_network_entity_type 
    ON network(entity_type_id);

CREATE INDEX IF NOT EXISTS idx_network_type 
    ON network(type_id);

CREATE INDEX IF NOT EXISTS idx_network_source_list 
    ON network USING GIN(source_list);

CREATE INDEX IF NOT EXISTS idx_network_model_list 
    ON network USING GIN(model_list);

CREATE INDEX IF NOT EXISTS idx_network_has_gis 
    ON network(has_gis);

CREATE INDEX IF NOT EXISTS idx_network_hydrologic_region 
    ON network(hydrologic_region_id);

CREATE INDEX IF NOT EXISTS idx_network_strm_code 
    ON network(strm_code);

CREATE INDEX IF NOT EXISTS idx_network_version 
    ON network(network_version_id);

\echo '✅ Indexes created'

\echo ''
\echo '📋 Verifying network version...'
SELECT v.id as network_version_id, vf.short_code as family, v.version_number 
FROM version v
JOIN version_family vf ON v.version_family_id = vf.id
WHERE vf.short_code = 'network';

\echo ''
\echo '🧹 Clearing existing network data...'
TRUNCATE TABLE network CASCADE;
\echo '✅ Existing data cleared'

\echo ''
\echo '📥 Loading network.csv from S3...'

SELECT aws_s3.table_import_from_s3(
    'network',
    'short_code, name, description, comment, entity_type_id, type_id, subtype_ids, model_list, source_list, has_gis, hydrologic_region_id, riv_sys, strm_code, network_version_id',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '02_network/network.csv',
    'us-west-2'
);

\echo '✅ network.csv loaded from S3'

\echo ''
\echo '🔍 VERIFYING NETWORK DATA'
\echo '========================='

\echo ''
\echo '📊 Total records loaded:'
SELECT COUNT(*) as total_records FROM network;

\echo ''
\echo '📊 Entity type breakdown:'
SELECT 
    CASE entity_type_id
        WHEN 1 THEN 'Arcs'
        WHEN 2 THEN 'Nodes'
        ELSE 'Other'
    END as entity_type,
    COUNT(*) as count
FROM network
GROUP BY entity_type_id
ORDER BY entity_type_id;

\echo ''
\echo '📊 Records by data source:'
SELECT 
    source_list,
    COUNT(*) as count
FROM network
GROUP BY source_list
ORDER BY count DESC
LIMIT 5;

\echo ''
\echo '📊 GIS data availability:'
SELECT 
    has_gis,
    COUNT(*) as count
FROM network
GROUP BY has_gis;

\echo ''
\echo '📊 Hydrologic region distribution:'
SELECT 
    hr.short_code as region,
    hr.label,
    COUNT(n.*) as network_elements
FROM network n
LEFT JOIN hydrologic_region hr ON n.hydrologic_region_id = hr.id
GROUP BY hr.short_code, hr.label
ORDER BY network_elements DESC;

\echo ''
\echo '📊 Network type distribution (top 10):'
SELECT 
    nt.short_code as type,
    nt.label,
    COUNT(n.*) as count
FROM network n
LEFT JOIN network_type nt ON n.type_id = nt.id
GROUP BY nt.short_code, nt.label
ORDER BY count DESC
LIMIT 10;

\echo ''
\echo '📊 Sample records:'
SELECT 
    short_code,
    name,
    entity_type_id,
    type_id,
    has_gis,
    hydrologic_region_id,
    strm_code
FROM network
LIMIT 10;

\echo ''
\echo '🎉 NETWORK TABLE SUCCESSFULLY LOADED!'
\echo '====================================='
\echo 'Summary:'
\echo '• All foreign keys validated'
\echo '• All indexes created for performance'
\echo '• Audit metadata populated'
\echo '• Versioning integrated'
\echo '• Ready for network_node, network_arc, and network_gis tables'

