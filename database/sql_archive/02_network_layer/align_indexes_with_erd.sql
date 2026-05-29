-- ALIGN DATABASE INDEXES WITH ERD SPECIFICATION
-- Compares current database indexes with ERD requirements

\echo ''
\echo '🔍 ALIGNING DATABASE INDEXES WITH ERD SPECIFICATION'
\echo '=================================================='
\echo ''
\echo 'ERD specifies these indexes for 00_versioning and 01_lookup tables:'
\echo '• version_family_short_code_key (short_code)'
\echo '• version_version_family_id_version_number_key (version_family_id, version_number)'
\echo '• idx_version_family (version_family_id)'
\echo '• hydrologic_region_short_code_key (short_code)'
\echo '• source_source_key (source)'
\echo '• Plus automatic primary key indexes (not documented)'
\echo ''

-- ============================================================================
-- 1. CHECK CURRENT INDEXES
-- ============================================================================

\echo '📊 STEP 1: CURRENT DATABASE INDEXES'
\echo '==================================='

\echo ''
\echo '🔍 Current indexes on 00_versioning tables:'
SELECT 
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename IN ('version_family', 'version', 'developer', 'domain_family_map')
ORDER BY tablename, indexname;

\echo ''
\echo '🔍 Current indexes on 01_lookup tables:'
SELECT 
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename IN ('hydrologic_region', 'source', 'model_source', 'geometry_type', 
                    'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
ORDER BY tablename, indexname;

-- ============================================================================
-- 2. IDENTIFY MISSING INDEXES
-- ============================================================================

\echo ''
\echo '📊 STEP 2: ERD vs DATABASE INDEX COMPARISON'
\echo '==========================================='

\echo ''
\echo '🔍 Checking if ERD-specified indexes exist:'

SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'version_family' AND indexname = 'version_family_short_code_key')
        THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END as status,
    'version_family_short_code_key' as required_index,
    'version_family' as table_name;

SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'version' AND indexname = 'version_version_family_id_version_number_key')
        THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END as status,
    'version_version_family_id_version_number_key' as required_index,
    'version' as table_name

UNION ALL

SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'version' AND indexname = 'idx_version_family')
        THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END as status,
    'idx_version_family' as required_index,
    'version' as table_name;

SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'hydrologic_region' AND indexname = 'hydrologic_region_short_code_key')
        THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END as status,
    'hydrologic_region_short_code_key' as required_index,
    'hydrologic_region' as table_name;

SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'source' AND indexname = 'source_source_key')
        THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END as status,
    'source_source_key' as required_index,
    'source' as table_name;

-- ============================================================================
-- 3. IDENTIFY EXTRA INDEXES
-- ============================================================================

\echo ''
\echo '📊 STEP 3: EXTRA INDEXES NOT IN ERD'
\echo '==================================='

\echo ''
\echo '🔍 Indexes that exist in database but not specified in ERD:'
\echo '(These are either primary keys, duplicates, or performance indexes we removed from ERD)'

WITH erd_indexes AS (
    SELECT 'version_family_short_code_key' as indexname, 'version_family' as tablename
    UNION ALL SELECT 'version_version_family_id_version_number_key', 'version'
    UNION ALL SELECT 'idx_version_family', 'version'
    UNION ALL SELECT 'hydrologic_region_short_code_key', 'hydrologic_region'
    UNION ALL SELECT 'source_source_key', 'source'
),
all_indexes AS (
    SELECT tablename, indexname
    FROM pg_indexes 
    WHERE tablename IN ('version_family', 'version', 'developer', 'domain_family_map',
                        'hydrologic_region', 'source', 'model_source', 'geometry_type', 
                        'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
)
SELECT 
    ai.tablename,
    ai.indexname,
    CASE 
        WHEN ai.indexname LIKE '%_pkey' THEN '📋 PRIMARY KEY (auto-created)'
        WHEN ei.indexname IS NOT NULL THEN '✅ IN ERD'
        ELSE '❓ EXTRA INDEX'
    END as status
FROM all_indexes ai
LEFT JOIN erd_indexes ei ON ai.tablename = ei.tablename AND ai.indexname = ei.indexname
ORDER BY ai.tablename, ai.indexname;

-- ============================================================================
-- 4. RECOMMENDATIONS
-- ============================================================================

\echo ''
\echo '📊 STEP 4: INDEX ALIGNMENT RECOMMENDATIONS'
\echo '=========================================='

\echo ''
\echo '💡 ANALYSIS SUMMARY:'
\echo ''
\echo 'The database likely has MORE indexes than ERD specifies because:'
\echo '• PostgreSQL auto-creates primary key indexes'
\echo '• Previous scripts created performance indexes'
\echo '• Some duplicate indexes exist'
\echo ''
\echo '🎯 RECOMMENDED ACTIONS:'
\echo ''
\echo 'IF missing indexes found:'
\echo '• Create missing indexes specified in ERD'
\echo ''
\echo 'IF extra indexes found:'
\echo '• Keep them (they work fine, just not documented)'
\echo '• OR drop duplicates/unnecessary ones for cleanliness'
\echo ''
\echo 'PRIMARY KEY indexes:'
\echo '• Always exist (PostgreSQL requirement)'
\echo '• Not documented in ERD (assumed)'
\echo ''

\echo ''
\echo '🔍 NEXT STEPS:'
\echo '1. Review the comparison above'
\echo '2. Identify any missing ERD-specified indexes'
\echo '3. Decide whether to clean up extra indexes'
\echo '4. Create index alignment script if needed'
\echo ''
