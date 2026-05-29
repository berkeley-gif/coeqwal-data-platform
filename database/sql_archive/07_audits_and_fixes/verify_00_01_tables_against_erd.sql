-- COMPREHENSIVE VERIFICATION: 00_VERSIONING & 01_LOOKUP TABLES
-- Verifies database structure matches seed tables and ERD requirements

\echo ''
\echo '🔍 COMPREHENSIVE 00_VERSIONING & 01_LOOKUP VERIFICATION'
\echo '======================================================='
\echo ''
\echo 'This script verifies:'
\echo '• Table structures match seed CSV expectations'
\echo '• Foreign key relationships are correct'
\echo '• Indexes are properly configured'
\echo '• Data integrity is maintained'
\echo '• ERD alignment for network layer dependencies'
\echo ''

-- ============================================================================
-- 1. VERIFY 00_VERSIONING TABLES
-- ============================================================================

\echo '📊 SECTION 1: 00_VERSIONING TABLES'
\echo '=================================='

\echo ''
\echo '🔍 1.1 VERSION_FAMILY table structure:'
\d version_family

\echo ''
\echo '🔍 1.2 VERSION table structure:'
\d version

\echo ''
\echo '🔍 1.3 DEVELOPER table structure:'
\d developer

\echo ''
\echo '🔍 1.4 DOMAIN_FAMILY_MAP table structure:'
\d domain_family_map

\echo ''
\echo '📊 1.5 Versioning data verification:'
SELECT 
    'version_family' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count
FROM version_family

UNION ALL

SELECT 
    'version' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count
FROM version

UNION ALL

SELECT 
    'developer' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count
FROM developer

UNION ALL

SELECT 
    'domain_family_map' as table_name,
    COUNT(*) as record_count,
    NULL as active_count
FROM domain_family_map;

\echo ''
\echo '🔍 1.6 Versioning foreign key relationships:'
SELECT 
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name IN ('version_family', 'version', 'developer', 'domain_family_map')
ORDER BY tc.table_name, tc.constraint_name;

-- ============================================================================
-- 2. VERIFY 01_LOOKUP TABLES
-- ============================================================================

\echo ''
\echo '📊 SECTION 2: 01_LOOKUP TABLES'
\echo '=============================='

\echo ''
\echo '🔍 2.1 HYDROLOGIC_REGION table structure:'
\d hydrologic_region

\echo ''
\echo '🔍 2.2 SOURCE table structure:'
\d source

\echo ''
\echo '🔍 2.3 MODEL_SOURCE table structure:'
\d model_source

\echo ''
\echo '🔍 2.4 GEOMETRY_TYPE table structure:'
\d geometry_type

\echo ''
\echo '🔍 2.5 SPATIAL_SCALE table structure:'
\d spatial_scale

\echo ''
\echo '🔍 2.6 TEMPORAL_SCALE table structure:'
\d temporal_scale

\echo ''
\echo '🔍 2.7 STATISTIC_TYPE table structure:'
\d statistic_type

\echo ''
\echo '🔍 2.8 UNIT table structure:'
\d unit

\echo ''
\echo '📊 2.9 Lookup tables data verification:'
SELECT 
    'hydrologic_region' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM hydrologic_region

UNION ALL

SELECT 
    'source' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM source

UNION ALL

SELECT 
    'model_source' as table_name,
    COUNT(*) as record_count,
    NULL as active_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM model_source

UNION ALL

SELECT 
    'geometry_type' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM geometry_type

UNION ALL

SELECT 
    'spatial_scale' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM spatial_scale

UNION ALL

SELECT 
    'temporal_scale' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM temporal_scale

UNION ALL

SELECT 
    'statistic_type' as table_name,
    COUNT(*) as record_count,
    NULL as active_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM statistic_type

UNION ALL

SELECT 
    'unit' as table_name,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE is_active = true) as active_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM unit;

\echo ''
\echo '🔍 2.10 Lookup tables foreign key relationships:'
SELECT 
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name IN ('hydrologic_region', 'source', 'model_source', 'geometry_type', 
                          'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
ORDER BY tc.table_name, tc.constraint_name;

-- ============================================================================
-- 3. VERIFY NETWORK LAYER DEPENDENCIES
-- ============================================================================

\echo ''
\echo '📊 SECTION 3: NETWORK LAYER DEPENDENCY VERIFICATION'
\echo '=================================================='

\echo ''
\echo '🔍 3.1 Network tables that should reference 00/01 tables:'
SELECT 
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND ccu.table_name IN ('hydrologic_region', 'source', 'model_source', 'developer', 'version')
    AND tc.table_name LIKE 'network_%'
ORDER BY tc.table_name, ccu.table_name;

-- ============================================================================
-- 4. INDEX VERIFICATION
-- ============================================================================

\echo ''
\echo '📊 SECTION 4: INDEX VERIFICATION'
\echo '==============================='

\echo ''
\echo '🔍 4.1 Indexes on 00_versioning tables:'
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename IN ('version_family', 'version', 'developer', 'domain_family_map')
ORDER BY tablename, indexname;

\echo ''
\echo '🔍 4.2 Indexes on 01_lookup tables:'
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename IN ('hydrologic_region', 'source', 'model_source', 'geometry_type', 
                    'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
ORDER BY tablename, indexname;

-- ============================================================================
-- 5. SEED TABLE ALIGNMENT CHECK
-- ============================================================================

\echo ''
\echo '📊 SECTION 5: SEED TABLE ALIGNMENT'
\echo '=================================='

\echo ''
\echo '🔍 5.1 Expected vs Actual record counts (based on our previous audit):'
\echo ''
\echo 'Expected counts from seed tables:'
\echo '• version_family: 13 records'
\echo '• version: 13 records'  
\echo '• developer: 2 records'
\echo '• domain_family_map: 35 records'
\echo '• hydrologic_region: 6 records (after our update)'
\echo '• source: 9 records (after our update)'
\echo '• model_source: 1 record'
\echo '• geometry_type: 4 records'
\echo '• spatial_scale: 11 records'
\echo '• temporal_scale: 8 records'
\echo '• statistic_type: 6 records'
\echo '• unit: 5 records'

-- ============================================================================
-- 6. CRITICAL FUNCTION VERIFICATION
-- ============================================================================

\echo ''
\echo '📊 SECTION 6: CRITICAL FUNCTION VERIFICATION'
\echo '==========================================='

\echo ''
\echo '🔍 6.1 Verify coeqwal_current_operator() function:'
SELECT coeqwal_current_operator() as current_operator_id;

\echo ''
\echo '🔍 6.2 Verify get_active_version() function:'
SELECT get_active_version(1) as theme_version_id;

\echo ''
\echo '🔍 6.3 All custom functions in database:'
SELECT 
    routine_name,
    routine_type,
    data_type as return_type
FROM information_schema.routines 
WHERE routine_schema = 'public' 
    AND routine_name LIKE '%coeqwal%' OR routine_name LIKE '%get_%'
ORDER BY routine_name;

-- ============================================================================
-- 7. FINAL SUMMARY
-- ============================================================================

\echo ''
\echo '📊 FINAL VERIFICATION SUMMARY'
\echo '============================='
\echo ''
\echo '✅ VERIFICATION COMPLETE!'
\echo ''
\echo 'Review the output above for:'
\echo '• Table structures match expected schema'
\echo '• Record counts align with seed data'
\echo '• Foreign key relationships are correct'
\echo '• Indexes are properly configured'
\echo '• Network layer dependencies are satisfied'
\echo '• Critical functions are available'
\echo ''
\echo 'If all sections show expected results, your 00_versioning'
\echo 'and 01_lookup tables are ready for commit!'
\echo ''
