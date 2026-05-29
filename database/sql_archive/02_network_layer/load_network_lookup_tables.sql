-- LOAD NETWORK LOOKUP TABLES FROM S3
-- ====================================
-- Loads network_type and network_subtype lookup tables
-- These must be loaded BEFORE the main network table
-- Uses unified approach (single type/subtype tables for both arcs and nodes)

\echo ''
\echo '🔍 LOADING NETWORK LOOKUP TABLES'
\echo '================================='

-- ============================================================================
-- 1. CREATE NETWORK_TYPE TABLE
-- ============================================================================

\echo ''
\echo '🏗️  Creating network_type table...'

CREATE TABLE IF NOT EXISTS network_type (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL,
    label VARCHAR NOT NULL,
    description TEXT,
    network_entity_type_id INTEGER NOT NULL,
    model_source_id INTEGER DEFAULT 1,
    source_id INTEGER DEFAULT 4,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    
    UNIQUE(short_code, network_entity_type_id)
);

\echo '✅ network_type table created'

\echo '🔗 Adding network_type foreign keys...'

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_type_entity_type_id_fkey' 
        AND table_name = 'network_type'
    ) THEN
        ALTER TABLE network_type 
        ADD CONSTRAINT network_type_entity_type_id_fkey 
        FOREIGN KEY (network_entity_type_id) 
        REFERENCES network_entity_type(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_type 
        ADD CONSTRAINT network_type_model_source_id_fkey 
        FOREIGN KEY (model_source_id) 
        REFERENCES model_source(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_type 
        ADD CONSTRAINT network_type_source_id_fkey 
        FOREIGN KEY (source_id) 
        REFERENCES source(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_type 
        ADD CONSTRAINT network_type_created_by_fkey 
        FOREIGN KEY (created_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_type 
        ADD CONSTRAINT network_type_updated_by_fkey 
        FOREIGN KEY (updated_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

\echo '📊 Creating network_type indexes...'

CREATE UNIQUE INDEX IF NOT EXISTS network_type_short_code_key 
    ON network_type(short_code);

CREATE INDEX IF NOT EXISTS idx_network_type_entity_type 
    ON network_type(network_entity_type_id);

CREATE INDEX IF NOT EXISTS idx_network_type_active 
    ON network_type(is_active);

\echo '✅ network_type indexes created'

-- ============================================================================
-- 2. CREATE NETWORK_SUBTYPE TABLE
-- ============================================================================

\echo ''
\echo '🏗️  Creating network_subtype table...'

CREATE TABLE IF NOT EXISTS network_subtype (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL,
    label VARCHAR NOT NULL,
    description TEXT,
    network_entity_type_id INTEGER NOT NULL,
    type_id INTEGER NOT NULL,
    model_source_id INTEGER DEFAULT 1,
    source_id INTEGER DEFAULT 4,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    
    UNIQUE(short_code, network_entity_type_id)
);

\echo '✅ network_subtype table created'

\echo '🔗 Adding network_subtype foreign keys...'

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'network_subtype_entity_type_id_fkey' 
        AND table_name = 'network_subtype'
    ) THEN
        ALTER TABLE network_subtype 
        ADD CONSTRAINT network_subtype_entity_type_id_fkey 
        FOREIGN KEY (network_entity_type_id) 
        REFERENCES network_entity_type(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_subtype 
        ADD CONSTRAINT network_subtype_type_id_fkey 
        FOREIGN KEY (type_id) 
        REFERENCES network_type(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_subtype 
        ADD CONSTRAINT network_subtype_model_source_id_fkey 
        FOREIGN KEY (model_source_id) 
        REFERENCES model_source(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_subtype 
        ADD CONSTRAINT network_subtype_source_id_fkey 
        FOREIGN KEY (source_id) 
        REFERENCES source(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_subtype 
        ADD CONSTRAINT network_subtype_created_by_fkey 
        FOREIGN KEY (created_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE network_subtype 
        ADD CONSTRAINT network_subtype_updated_by_fkey 
        FOREIGN KEY (updated_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

\echo '📊 Creating network_subtype indexes...'

CREATE INDEX IF NOT EXISTS idx_network_subtype_short_code 
    ON network_subtype(short_code);

CREATE INDEX IF NOT EXISTS idx_network_subtype_entity_type 
    ON network_subtype(network_entity_type_id);

CREATE INDEX IF NOT EXISTS idx_network_subtype_type 
    ON network_subtype(type_id);

CREATE INDEX IF NOT EXISTS idx_network_subtype_active 
    ON network_subtype(is_active);

\echo '✅ network_subtype indexes created'

-- ============================================================================
-- 3. LOAD DATA FROM S3
-- ============================================================================

\echo ''
\echo '🧹 Clearing existing lookup data...'
TRUNCATE TABLE network_subtype CASCADE;
TRUNCATE TABLE network_type CASCADE;
\echo '✅ Existing data cleared'

\echo ''
\echo '📥 Loading network_type from S3...'

SELECT aws_s3.table_import_from_s3(
    'network_type',
    'short_code, label, description, network_entity_type_id, model_source_id, source_id, is_active',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '01_lookup/network_type.csv',
    'us-west-2'
);

\echo '✅ network_type loaded'

\echo ''
\echo '📥 Loading network_subtype from S3...'

SELECT aws_s3.table_import_from_s3(
    'network_subtype',
    'short_code, label, description, network_entity_type_id, type_id, model_source_id, source_id, is_active',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '01_lookup/network_subtype.csv',
    'us-west-2'
);

\echo '✅ network_subtype loaded'

-- ============================================================================
-- 4. VERIFICATION
-- ============================================================================

\echo ''
\echo '🔍 VERIFYING LOOKUP TABLES'
\echo '=========================='

\echo ''
\echo '📊 network_type records:'
SELECT COUNT(*) as total_types FROM network_type;

\echo ''
\echo '📊 Arc types (entity_type_id=1):'
SELECT id, short_code, label 
FROM network_type 
WHERE network_entity_type_id = 1 
ORDER BY id;

\echo ''
\echo '📊 Node types (entity_type_id=2):'
SELECT id, short_code, label 
FROM network_type 
WHERE network_entity_type_id = 2 
ORDER BY id;

\echo ''
\echo '📊 network_subtype records:'
SELECT COUNT(*) as total_subtypes FROM network_subtype;

\echo ''
\echo '📊 Arc subtypes (entity_type_id=1):'
SELECT id, short_code, label, type_id 
FROM network_subtype 
WHERE network_entity_type_id = 1 
ORDER BY id;

\echo ''
\echo '📊 Node subtypes (entity_type_id=2):'
SELECT id, short_code, label, type_id 
FROM network_subtype 
WHERE network_entity_type_id = 2 
ORDER BY id;

\echo ''
\echo '🎉 NETWORK LOOKUP TABLES SUCCESSFULLY LOADED!'
\echo '=============================================='
\echo 'Ready to load main network table with proper FK references!'

