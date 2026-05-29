-- VERIFY LOOKUP TABLE INTEGRITY AFTER UPDATES
-- Checks foreign keys, indexes, constraints, and data consistency

\echo ''
\echo '🔍 COMPREHENSIVE LOOKUP TABLE INTEGRITY VERIFICATION'
\echo '===================================================='
\echo ''

-- ============================================================================
-- 1. VERIFY TABLE CONTENTS
-- ============================================================================

\echo '📊 STEP 1: VERIFY TABLE CONTENTS'
\echo '================================'

\echo ''
\echo '🔍 HYDROLOGIC_REGION (should have 6 records):'
SELECT id, short_code, label, is_active, created_by, updated_by 
FROM hydrologic_region 
ORDER BY id;

\echo ''
\echo '🔍 SOURCE (should have 9+ records):'
SELECT id, source, description, is_active, created_by, updated_by 
FROM source 
ORDER BY id;

-- ============================================================================
-- 2. CHECK FOREIGN KEY CONSTRAINTS
-- ============================================================================

\echo ''
\echo '📊 STEP 2: FOREIGN KEY CONSTRAINT VERIFICATION'
\echo '=============================================='

\echo ''
\echo '🔍 All foreign key constraints in the database:'
SELECT 
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND (ccu.table_name = 'hydrologic_region' OR ccu.table_name = 'source')
ORDER BY tc.table_name, tc.constraint_name;

-- ============================================================================
-- 3. CHECK INDEXES
-- ============================================================================

\echo ''
\echo '📊 STEP 3: INDEX VERIFICATION'
\echo '============================='

\echo ''
\echo '🔍 Indexes on hydrologic_region:'
SELECT 
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'hydrologic_region'
ORDER BY indexname;

\echo ''
\echo '🔍 Indexes on source:'
SELECT 
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'source'
ORDER BY indexname;

-- ============================================================================
-- 4. TEST FOREIGN KEY LOOKUPS
-- ============================================================================

\echo ''
\echo '📊 STEP 4: FOREIGN KEY LOOKUP TESTS'
\echo '==================================='

\echo ''
\echo '🔍 Testing hydrologic_region FKs (sample from entity tables):'
SELECT 
    'du_agriculture_entity' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT hydrologic_region_id) as distinct_regions,
    MIN(hydrologic_region_id) as min_region_id,
    MAX(hydrologic_region_id) as max_region_id
FROM du_agriculture_entity
WHERE hydrologic_region_id IS NOT NULL

UNION ALL

SELECT 
    'du_urban_entity' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT hydrologic_region_id) as distinct_regions,
    MIN(hydrologic_region_id) as min_region_id,
    MAX(hydrologic_region_id) as max_region_id
FROM du_urban_entity
WHERE hydrologic_region_id IS NOT NULL

UNION ALL

SELECT 
    'reservoir_entity' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT hydrologic_region_id) as distinct_regions,
    MIN(hydrologic_region_id) as min_region_id,
    MAX(hydrologic_region_id) as max_region_id
FROM reservoir_entity
WHERE hydrologic_region_id IS NOT NULL;

\echo ''
\echo '🔍 Testing source FKs (sample from entity tables):'
SELECT 
    'network_gis' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT source_id) as distinct_sources,
    MIN(source_id) as min_source_id,
    MAX(source_id) as max_source_id
FROM network_gis
WHERE source_id IS NOT NULL;

-- ============================================================================
-- 5. VERIFY REFERENTIAL INTEGRITY
-- ============================================================================

\echo ''
\echo '📊 STEP 5: REFERENTIAL INTEGRITY CHECK'
\echo '======================================'

\echo ''
\echo '🔍 Checking for orphaned hydrologic_region references:'
WITH referenced_regions AS (
    SELECT DISTINCT hydrologic_region_id FROM du_agriculture_entity WHERE hydrologic_region_id IS NOT NULL
    UNION
    SELECT DISTINCT hydrologic_region_id FROM du_urban_entity WHERE hydrologic_region_id IS NOT NULL
    UNION
    SELECT DISTINCT hydrologic_region_id FROM du_refuge_entity WHERE hydrologic_region_id IS NOT NULL
    UNION
    SELECT DISTINCT hydrologic_region_id FROM reservoir_entity WHERE hydrologic_region_id IS NOT NULL
)
SELECT 
    rr.hydrologic_region_id,
    CASE 
        WHEN hr.id IS NULL THEN '❌ ORPHANED'
        ELSE '✅ VALID'
    END as status,
    hr.short_code,
    hr.label
FROM referenced_regions rr
LEFT JOIN hydrologic_region hr ON rr.hydrologic_region_id = hr.id
ORDER BY rr.hydrologic_region_id;

\echo ''
\echo '🔍 Checking for orphaned source references:'
WITH referenced_sources AS (
    SELECT DISTINCT source_id FROM network_gis WHERE source_id IS NOT NULL
    UNION
    SELECT DISTINCT model_source_id FROM network_gis WHERE model_source_id IS NOT NULL
)
SELECT 
    rs.source_id,
    CASE 
        WHEN s.id IS NULL THEN '❌ ORPHANED'
        ELSE '✅ VALID'
    END as status,
    s.source,
    s.description
FROM referenced_sources rs
LEFT JOIN source s ON rs.source_id = s.id
ORDER BY rs.source_id;

-- ============================================================================
-- 6. SEQUENCE STATUS
-- ============================================================================

\echo ''
\echo '📊 STEP 6: SEQUENCE STATUS'
\echo '=========================='

\echo ''
\echo '🔍 Current sequence values:'
SELECT 
    'hydrologic_region_id_seq' as sequence_name,
    last_value,
    is_called
FROM hydrologic_region_id_seq

UNION ALL

SELECT 
    'source_id_seq' as sequence_name,
    last_value,
    is_called
FROM source_id_seq;

-- ============================================================================
-- 7. FINAL SUMMARY
-- ============================================================================

\echo ''
\echo '📊 FINAL INTEGRITY SUMMARY'
\echo '=========================='

\echo ''
\echo '✅ VERIFICATION COMPLETE!'
\echo ''
\echo 'Key Points:'
\echo '• Non-sequential IDs are NORMAL in PostgreSQL'
\echo '• Foreign keys work with any valid ID values'
\echo '• Indexes work regardless of ID sequence'
\echo '• Referential integrity is maintained'
\echo ''
\echo 'If all checks above show ✅ VALID, your database is healthy!'
\echo ''
