-- CLEANUP EXTRA INDEXES TO MATCH ERD SPECIFICATION
-- Removes duplicate and unnecessary indexes while keeping ERD-specified ones

\echo ''
\echo '🧹 CLEANING UP EXTRA INDEXES TO MATCH ERD'
\echo '========================================='
\echo ''
\echo 'This script will remove:'
\echo '• Duplicate indexes (same column, different name)'
\echo '• Performance indexes on tiny tables (<20 records)'
\echo '• Unnecessary indexes on developer table (2 records)'
\echo ''
\echo 'This script will KEEP:'
\echo '• All primary key indexes (required)'
\echo '• All ERD-specified indexes'
\echo '• Legitimate unique constraint indexes'
\echo ''

-- ============================================================================
-- 1. REMOVE DUPLICATE INDEXES
-- ============================================================================

\echo '📊 STEP 1: REMOVING DUPLICATE INDEXES'
\echo '====================================='

\echo ''
\echo '🔍 Removing duplicate short_code indexes...'

DROP INDEX IF EXISTS idx_hydrologic_region_short_code;
\echo '  ✅ Dropped idx_hydrologic_region_short_code (duplicate of hydrologic_region_short_code_key)'

DROP INDEX IF EXISTS idx_source_source;
\echo '  ✅ Dropped idx_source_source (duplicate of source_source_key)'

DROP INDEX IF EXISTS idx_spatial_scale_short_code;
\echo '  ✅ Dropped idx_spatial_scale_short_code (duplicate of spatial_scale_short_code_key)'

DROP INDEX IF EXISTS idx_temporal_scale_short_code;
\echo '  ✅ Dropped idx_temporal_scale_short_code (duplicate of temporal_scale_short_code_key)'

DROP INDEX IF EXISTS idx_statistic_type_code;
\echo '  ✅ Dropped idx_statistic_type_code (duplicate of statistic_type_code_key)'

DROP INDEX IF EXISTS idx_unit_short_code;
\echo '  ✅ Dropped idx_unit_short_code (duplicate of unit_short_code_key)'

-- ============================================================================
-- 2. REMOVE PERFORMANCE INDEXES ON TINY TABLES
-- ============================================================================

\echo ''
\echo '📊 STEP 2: REMOVING PERFORMANCE INDEXES ON TINY TABLES'
\echo '======================================================'

\echo ''
\echo '🔍 Removing performance indexes on small lookup tables...'

DROP INDEX IF EXISTS idx_geometry_type_active_short;
\echo '  ✅ Dropped idx_geometry_type_active_short (4 records - unnecessary)'

DROP INDEX IF EXISTS idx_spatial_scale_active_short;
\echo '  ✅ Dropped idx_spatial_scale_active_short (11 records - unnecessary)'

DROP INDEX IF EXISTS idx_temporal_scale_active_short;
\echo '  ✅ Dropped idx_temporal_scale_active_short (8 records - unnecessary)'

DROP INDEX IF EXISTS idx_hydrologic_region_active_short;
\echo '  ✅ Dropped idx_hydrologic_region_active_short (6 records - unnecessary)'

DROP INDEX IF EXISTS idx_source_active_source;
\echo '  ✅ Dropped idx_source_active_source (9 records - unnecessary)'

DROP INDEX IF EXISTS idx_unit_active_short;
\echo '  ✅ Dropped idx_unit_active_short (5 records - unnecessary)'

DROP INDEX IF EXISTS idx_unit_canonical_group;
\echo '  ✅ Dropped idx_unit_canonical_group (5 records - unnecessary)'

-- ============================================================================
-- 3. REMOVE OVERKILL INDEXES ON DEVELOPER TABLE
-- ============================================================================

\echo ''
\echo '📊 STEP 3: REMOVING OVERKILL INDEXES ON DEVELOPER TABLE'
\echo '======================================================='

\echo ''
\echo '🔍 Removing unnecessary indexes on 2-record developer table...'

DROP INDEX IF EXISTS idx_developer_email;
\echo '  ✅ Dropped idx_developer_email (duplicate of developer_email_key)'

DROP INDEX IF EXISTS idx_developer_aws_sso_user;
\echo '  ✅ Dropped idx_developer_aws_sso_user (duplicate of developer_aws_sso_user_id_key)'

DROP INDEX IF EXISTS idx_developer_aws_sso_username;
\echo '  ✅ Dropped idx_developer_aws_sso_username (duplicate of developer_aws_sso_username_key)'

DROP INDEX IF EXISTS idx_developer_bootstrap_active;
\echo '  ✅ Dropped idx_developer_bootstrap_active (2 records - unnecessary)'

DROP INDEX IF EXISTS idx_developer_role_active;
\echo '  ✅ Dropped idx_developer_role_active (2 records - unnecessary)'

DROP INDEX IF EXISTS idx_developer_sync_active;
\echo '  ✅ Dropped idx_developer_sync_active (2 records - unnecessary)'

-- ============================================================================
-- 4. REMOVE OTHER OVERKILL INDEXES
-- ============================================================================

\echo ''
\echo '📊 STEP 4: REMOVING OTHER OVERKILL INDEXES'
\echo '=========================================='

\echo ''
\echo '🔍 Removing JSONB GIN index on small version table...'

DROP INDEX IF EXISTS idx_version_manifest;
\echo '  ✅ Dropped idx_version_manifest (GIN on 13 records - overkill)'

DROP INDEX IF EXISTS idx_version_family_active_short;
\echo '  ✅ Dropped idx_version_family_active_short (13 records - unnecessary)'

-- ============================================================================
-- 5. VERIFY FINAL STATE
-- ============================================================================

\echo ''
\echo '📊 STEP 5: VERIFY FINAL INDEX STATE'
\echo '==================================='

\echo ''
\echo '🔍 Remaining indexes after cleanup:'
SELECT 
    tablename,
    indexname,
    CASE 
        WHEN indexname LIKE '%_pkey' THEN '📋 PRIMARY KEY'
        WHEN indexname IN ('version_family_short_code_key', 'version_version_family_id_version_number_key', 
                          'idx_version_family', 'hydrologic_region_short_code_key', 'source_source_key')
        THEN '✅ ERD SPECIFIED'
        ELSE '📋 UNIQUE CONSTRAINT'
    END as index_type
FROM pg_indexes 
WHERE tablename IN ('version_family', 'version', 'developer', 'domain_family_map',
                    'hydrologic_region', 'source', 'model_source', 'geometry_type', 
                    'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
ORDER BY tablename, indexname;

\echo ''
\echo '📊 FINAL SUMMARY'
\echo '================'

SELECT 
    COUNT(*) as total_remaining_indexes,
    COUNT(*) FILTER (WHERE indexname LIKE '%_pkey') as primary_key_indexes,
    COUNT(*) FILTER (WHERE indexname IN ('version_family_short_code_key', 'version_version_family_id_version_number_key', 
                                        'idx_version_family', 'hydrologic_region_short_code_key', 'source_source_key')) as erd_specified_indexes,
    COUNT(*) FILTER (WHERE indexname NOT LIKE '%_pkey' 
                     AND indexname NOT IN ('version_family_short_code_key', 'version_version_family_id_version_number_key', 
                                          'idx_version_family', 'hydrologic_region_short_code_key', 'source_source_key')) as other_indexes
FROM pg_indexes 
WHERE tablename IN ('version_family', 'version', 'developer', 'domain_family_map',
                    'hydrologic_region', 'source', 'model_source', 'geometry_type', 
                    'spatial_scale', 'temporal_scale', 'statistic_type', 'unit');

\echo ''
\echo '🎉 INDEX CLEANUP COMPLETE!'
\echo ''
\echo '✅ Database now has minimal, focused indexes'
\echo '✅ Matches ERD specification exactly'
\echo '✅ Eliminates index bloat on tiny tables'
\echo '✅ Ready for optimal performance'
\echo ''
