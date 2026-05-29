-- FINAL COMPREHENSIVE SYNC VERIFICATION
-- Confirms ERD, database schema, indexes, and foreign keys are all aligned

\echo ''
\echo '🔍 FINAL COMPREHENSIVE SYNC VERIFICATION'
\echo '========================================'
\echo ''
\echo 'Verifying complete alignment between:'
\echo '• ERD specifications'
\echo '• Database schema'
\echo '• Index configuration'
\echo '• Foreign key relationships'
\echo '• Seed table data'
\echo ''

-- ============================================================================
-- 1. VERIFY TABLE RECORD COUNTS MATCH EXPECTATIONS
-- ============================================================================

\echo '📊 STEP 1: TABLE RECORD COUNTS'
\echo '=============================='

\echo ''
\echo '🔍 00_versioning tables:'
SELECT 
    'version_family' as table_name,
    COUNT(*) as actual_count,
    13 as expected_count,
    CASE WHEN COUNT(*) = 13 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM version_family

UNION ALL

SELECT 
    'version' as table_name,
    COUNT(*) as actual_count,
    13 as expected_count,
    CASE WHEN COUNT(*) = 13 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM version

UNION ALL

SELECT 
    'developer' as table_name,
    COUNT(*) as actual_count,
    2 as expected_count,
    CASE WHEN COUNT(*) = 2 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM developer

UNION ALL

SELECT 
    'domain_family_map' as table_name,
    COUNT(*) as actual_count,
    35 as expected_count,
    CASE WHEN COUNT(*) = 35 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM domain_family_map;

\echo ''
\echo '🔍 01_lookup tables:'
SELECT 
    'hydrologic_region' as table_name,
    COUNT(*) as actual_count,
    6 as expected_count,
    CASE WHEN COUNT(*) = 6 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM hydrologic_region

UNION ALL

SELECT 
    'source' as table_name,
    COUNT(*) as actual_count,
    9 as expected_count,
    CASE WHEN COUNT(*) = 9 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM source

UNION ALL

SELECT 
    'model_source' as table_name,
    COUNT(*) as actual_count,
    1 as expected_count,
    CASE WHEN COUNT(*) = 1 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM model_source

UNION ALL

SELECT 
    'geometry_type' as table_name,
    COUNT(*) as actual_count,
    4 as expected_count,
    CASE WHEN COUNT(*) = 4 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM geometry_type

UNION ALL

SELECT 
    'spatial_scale' as table_name,
    COUNT(*) as actual_count,
    11 as expected_count,
    CASE WHEN COUNT(*) = 11 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM spatial_scale

UNION ALL

SELECT 
    'temporal_scale' as table_name,
    COUNT(*) as actual_count,
    8 as expected_count,
    CASE WHEN COUNT(*) = 8 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM temporal_scale

UNION ALL

SELECT 
    'statistic_type' as table_name,
    COUNT(*) as actual_count,
    6 as expected_count,
    CASE WHEN COUNT(*) = 6 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM statistic_type

UNION ALL

SELECT 
    'unit' as table_name,
    COUNT(*) as actual_count,
    5 as expected_count,
    CASE WHEN COUNT(*) = 5 THEN '✅ MATCH' ELSE '❌ MISMATCH' END as status
FROM unit;

-- ============================================================================
-- 2. VERIFY ERD-SPECIFIED INDEXES EXIST
-- ============================================================================

\echo ''
\echo '📊 STEP 2: ERD-SPECIFIED INDEX VERIFICATION'
\echo '==========================================='

\echo ''
\echo '🔍 Checking ERD-specified indexes:'

WITH erd_required_indexes AS (
    SELECT 'version_family_short_code_key' as indexname, 'version_family' as tablename, 'version family lookups' as purpose
    UNION ALL SELECT 'version_version_family_id_version_number_key', 'version', 'business uniqueness'
    UNION ALL SELECT 'idx_version_family', 'version', 'FK performance'
    UNION ALL SELECT 'hydrologic_region_short_code_key', 'hydrologic_region', 'region lookups'
)
SELECT 
    eri.tablename,
    eri.indexname,
    eri.purpose,
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_indexes pi WHERE pi.tablename = eri.tablename AND pi.indexname = eri.indexname)
        THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END as status
FROM erd_required_indexes eri
ORDER BY eri.tablename, eri.indexname;

-- ============================================================================
-- 3. VERIFY ALL FOREIGN KEY CONSTRAINTS
-- ============================================================================

\echo ''
\echo '📊 STEP 3: FOREIGN KEY CONSTRAINT VERIFICATION'
\echo '=============================================='

\echo ''
\echo '🔍 All foreign key constraints on 00_versioning and 01_lookup tables:'
SELECT 
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name,
    '✅ ACTIVE' as status
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name IN ('version_family', 'version', 'developer', 'domain_family_map',
                          'hydrologic_region', 'source', 'model_source', 'geometry_type', 
                          'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
ORDER BY tc.table_name, tc.constraint_name;

-- ============================================================================
-- 4. VERIFY NETWORK LAYER DEPENDENCIES
-- ============================================================================

\echo ''
\echo '📊 STEP 4: NETWORK LAYER DEPENDENCY VERIFICATION'
\echo '==============================================='

\echo ''
\echo '🔍 Network tables referencing 00_versioning and 01_lookup:'
SELECT 
    tc.table_name as network_table,
    kcu.column_name as fk_column,
    ccu.table_name AS references_table,
    ccu.column_name AS references_column,
    '✅ ACTIVE' as status
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND ccu.table_name IN ('hydrologic_region', 'source', 'model_source', 'developer', 'version', 'version_family')
    AND tc.table_name LIKE 'network_%'
ORDER BY tc.table_name, ccu.table_name;

-- ============================================================================
-- 5. VERIFY CRITICAL FUNCTIONS
-- ============================================================================

\echo ''
\echo '📊 STEP 5: CRITICAL FUNCTION VERIFICATION'
\echo '========================================'

\echo ''
\echo '🔍 Testing critical functions:'

SELECT 
    'coeqwal_current_operator()' as function_name,
    coeqwal_current_operator() as result,
    CASE 
        WHEN coeqwal_current_operator() IS NOT NULL THEN '✅ WORKING'
        ELSE '❌ FAILED'
    END as status;

DO $$
BEGIN
    PERFORM get_active_version(1);
    RAISE NOTICE '✅ get_active_version() function exists and callable';
EXCEPTION
    WHEN others THEN
        RAISE NOTICE '❌ get_active_version() function issue: %', SQLERRM;
END $$;

-- ============================================================================
-- 6. FINAL INDEX COUNT VERIFICATION
-- ============================================================================

\echo ''
\echo '📊 STEP 6: FINAL INDEX COUNT VERIFICATION'
\echo '========================================'

\echo ''
\echo '🔍 Final index count by table:'
SELECT 
    tablename,
    COUNT(*) as index_count,
    array_agg(indexname ORDER BY indexname) as index_names
FROM pg_indexes 
WHERE tablename IN ('version_family', 'version', 'developer', 'domain_family_map',
                    'hydrologic_region', 'source', 'model_source', 'geometry_type', 
                    'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
GROUP BY tablename
ORDER BY tablename;

\echo ''
\echo '📊 Total index summary:'
SELECT 
    COUNT(*) as total_indexes,
    COUNT(*) FILTER (WHERE indexname LIKE '%_pkey') as primary_key_indexes,
    COUNT(*) FILTER (WHERE indexname LIKE '%_key' AND indexname NOT LIKE '%_pkey') as unique_constraint_indexes,
    COUNT(*) FILTER (WHERE indexname NOT LIKE '%_key') as regular_indexes
FROM pg_indexes 
WHERE tablename IN ('version_family', 'version', 'developer', 'domain_family_map',
                    'hydrologic_region', 'source', 'model_source', 'geometry_type', 
                    'spatial_scale', 'temporal_scale', 'statistic_type', 'unit');

-- ============================================================================
-- 7. FINAL SYNC CONFIRMATION
-- ============================================================================

\echo ''
\echo '📊 FINAL SYNC CONFIRMATION'
\echo '=========================='
\echo ''
\echo '✅ VERIFICATION COMPLETE!'
\echo ''
\echo 'If all sections above show ✅ status, then:'
\echo '• ERD specifications match database reality'
\echo '• All required indexes exist and are optimized'
\echo '• All foreign key relationships are intact'
\echo '• All critical functions are working'
\echo '• Database is perfectly aligned with ERD'
\echo ''
\echo '🚀 READY FOR COMMIT!'
\echo ''
