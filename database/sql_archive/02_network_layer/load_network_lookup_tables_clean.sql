-- LOAD NETWORK LOOKUP TABLES FROM S3 (CLEAN START)
-- ==================================================
-- Drops and recreates network_type and network_subtype tables
-- Resets sequences to start from 1
-- Ensures clean IDs: types 1-21, subtypes 1-27

\echo ''
\echo '🔍 LOADING NETWORK LOOKUP TABLES (CLEAN START)'
\echo '=============================================='

-- ============================================================================
-- 0. DROP EXISTING TABLES TO RESET SEQUENCES
-- ============================================================================

\echo ''
\echo '🗑️  Dropping existing lookup tables to reset sequences...'

DROP TABLE IF EXISTS network_subtype CASCADE;
DROP TABLE IF EXISTS network_type CASCADE;

\echo '✅ Old tables dropped, sequences will reset to 1'

-- ============================================================================
-- 1. CREATE NETWORK_TYPE TABLE
-- ============================================================================

\echo ''
\echo '🏗️  Creating network_type table...'

CREATE TABLE network_type (
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

\echo '✅ Foreign keys added'

\echo '📊 Creating network_type indexes...'

CREATE UNIQUE INDEX network_type_short_code_entity_key 
    ON network_type(short_code, network_entity_type_id);

CREATE INDEX idx_network_type_entity_type 
    ON network_type(network_entity_type_id);

CREATE INDEX idx_network_type_active 
    ON network_type(is_active);

\echo '✅ network_type indexes created'

-- ============================================================================
-- 2. CREATE NETWORK_SUBTYPE TABLE
-- ============================================================================

\echo ''
\echo '🏗️  Creating network_subtype table...'

CREATE TABLE network_subtype (
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

\echo '✅ Foreign keys added'

\echo '📊 Creating network_subtype indexes...'

CREATE INDEX idx_network_subtype_short_code 
    ON network_subtype(short_code);

CREATE INDEX idx_network_subtype_entity_type 
    ON network_subtype(network_entity_type_id);

CREATE INDEX idx_network_subtype_type 
    ON network_subtype(type_id);

CREATE INDEX idx_network_subtype_active 
    ON network_subtype(is_active);

\echo '✅ network_subtype indexes created'

-- ============================================================================
-- 3. LOAD DATA FROM S3
-- ============================================================================

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
\echo '📊 Arc types (entity_type_id=1, should be IDs 1-10):'
SELECT id, short_code, label 
FROM network_type 
WHERE network_entity_type_id = 1 
ORDER BY id;

\echo ''
\echo '📊 Node types (entity_type_id=2, should be IDs 11-21):'
SELECT id, short_code, label 
FROM network_type 
WHERE network_entity_type_id = 2 
ORDER BY id;

\echo ''
\echo '📊 network_subtype records:'
SELECT COUNT(*) as total_subtypes FROM network_subtype;

\echo ''
\echo '📊 Arc subtypes (entity_type_id=1, should be IDs 1-10):'
SELECT id, short_code, label, type_id 
FROM network_subtype 
WHERE network_entity_type_id = 1 
ORDER BY id;

\echo ''
\echo '📊 Node subtypes (entity_type_id=2, should be IDs 11-27):'
SELECT id, short_code, label, type_id 
FROM network_subtype 
WHERE network_entity_type_id = 2 
ORDER BY id;

\echo ''
\echo '✅ VERIFY IDs ARE CLEAN:'
\echo '   • Arc types should be IDs 1-10'
\echo '   • Node types should be IDs 11-21'
\echo '   • Arc subtypes should be IDs 1-10'
\echo '   • Node subtypes should be IDs 11-27'
\echo ''
\echo '🎉 NETWORK LOOKUP TABLES SUCCESSFULLY LOADED WITH CLEAN IDs!'
\echo '============================================================='
\echo 'Ready to load main network table with proper FK references!'

