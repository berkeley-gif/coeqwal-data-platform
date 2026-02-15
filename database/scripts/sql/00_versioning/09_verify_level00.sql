-- =============================================================================
-- 09_verify_level00.sql
-- Comprehensive verification of Level 00 (Versioning Layer)
-- =============================================================================
-- Run in Cloud9: \i database/scripts/sql/00_versioning/09_verify_level00.sql
-- =============================================================================

\echo '============================================================================'
\echo 'LEVEL 00 VERSIONING LAYER - VERIFICATION'
\echo '============================================================================'

-- =============================================================================
-- 1. AUDIT TRIGGERS
-- =============================================================================
\echo ''
\echo '1. AUDIT TRIGGERS'
\echo '----------------'

SELECT 'Tables with audit triggers:' as check;
SELECT COUNT(DISTINCT event_object_table) as count
FROM information_schema.triggers 
WHERE trigger_schema = 'public' 
AND trigger_name LIKE 'audit_fields_%';

SELECT 'Tables WITHOUT audit triggers (expected: audit_log, spatial_ref_sys):' as check;
SELECT t.table_name
FROM information_schema.tables t
WHERE t.table_schema = 'public' 
AND t.table_type = 'BASE TABLE'
AND NOT EXISTS (
    SELECT 1 FROM information_schema.triggers tr 
    WHERE tr.event_object_table = t.table_name 
    AND tr.trigger_name LIKE 'audit_fields_%'
)
ORDER BY t.table_name;

-- =============================================================================
-- 2. VERSION FAMILIES & VERSIONS
-- =============================================================================
\echo ''
\echo '2. VERSION FAMILIES & VERSIONS'
\echo '------------------------------'

SELECT 'Version families with versions:' as check;
SELECT vf.id, vf.short_code, v.version_number, vf.is_active
FROM version_family vf
LEFT JOIN version v ON v.version_family_id = vf.id
ORDER BY vf.id;

SELECT 'Version families WITHOUT version record (should be 0):' as check;
SELECT COUNT(*) as count
FROM version_family vf
LEFT JOIN version v ON v.version_family_id = vf.id
WHERE v.id IS NULL;

-- =============================================================================
-- 3. DOMAIN FAMILY MAP
-- =============================================================================
\echo ''
\echo '3. DOMAIN FAMILY MAP'
\echo '--------------------'

SELECT 'Tables per version family:' as check;
SELECT vf.id, vf.short_code as family, COUNT(dfm.table_name) as table_count
FROM version_family vf
LEFT JOIN domain_family_map dfm ON dfm.version_family_id = vf.id
GROUP BY vf.id, vf.short_code
ORDER BY vf.id;

SELECT 'Unmapped tables (should be 0):' as check;
SELECT COUNT(*) as count
FROM information_schema.tables t
WHERE t.table_schema = 'public' 
AND t.table_type = 'BASE TABLE'
AND t.table_name NOT IN (SELECT table_name FROM domain_family_map);

SELECT 'Invalid version_family_id references (should be 0):' as check;
SELECT COUNT(*) as count
FROM domain_family_map dfm
LEFT JOIN version_family vf ON dfm.version_family_id = vf.id
WHERE vf.id IS NULL;

-- =============================================================================
-- 4. DEVELOPERS & OPERATOR FUNCTION
-- =============================================================================
\echo ''
\echo '4. DEVELOPERS & OPERATOR FUNCTION'
\echo '----------------------------------'

SELECT 'Active developers:' as check;
SELECT id, email, display_name, aws_sso_username, role
FROM developer 
WHERE is_active = true
ORDER BY id;

SELECT 'Current operator test:' as check;
SELECT coeqwal_current_operator() as operator_id;

-- =============================================================================
-- 5. AUDIT COLUMNS COVERAGE
-- =============================================================================
\echo ''
\echo '5. AUDIT COLUMNS COVERAGE'
\echo '-------------------------'

SELECT 'Tables missing audit columns (excluding audit_log, spatial_ref_sys):' as check;
SELECT t.table_name,
    SUM(CASE WHEN c.column_name = 'created_at' THEN 1 ELSE 0 END) as has_created_at,
    SUM(CASE WHEN c.column_name = 'created_by' THEN 1 ELSE 0 END) as has_created_by,
    SUM(CASE WHEN c.column_name = 'updated_at' THEN 1 ELSE 0 END) as has_updated_at,
    SUM(CASE WHEN c.column_name = 'updated_by' THEN 1 ELSE 0 END) as has_updated_by
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
    ON t.table_name = c.table_name 
    AND c.column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by')
WHERE t.table_schema = 'public' 
AND t.table_type = 'BASE TABLE'
AND t.table_name NOT IN ('spatial_ref_sys', 'audit_log')
GROUP BY t.table_name
HAVING COUNT(DISTINCT c.column_name) < 4
ORDER BY t.table_name;

-- =============================================================================
-- 6. SUMMARY
-- =============================================================================
\echo ''
\echo '============================================================================'
\echo 'SUMMARY'
\echo '============================================================================'

SELECT 
    (SELECT COUNT(*) FROM version_family) as version_families,
    (SELECT COUNT(*) FROM version) as versions,
    (SELECT COUNT(*) FROM domain_family_map) as domain_mappings,
    (SELECT COUNT(*) FROM developer WHERE is_active = true) as active_developers,
    (SELECT COUNT(DISTINCT event_object_table) FROM information_schema.triggers 
     WHERE trigger_schema = 'public' AND trigger_name LIKE 'audit_fields_%') as tables_with_triggers;

\echo ''
\echo '============================================================================'
\echo 'Expected values:'
\echo '  - version_families: 14'
\echo '  - versions: 14'
\echo '  - domain_mappings: 69'
\echo '  - active_developers: 2+'
\echo '  - tables_with_triggers: 67'
\echo '============================================================================'
\echo 'VERIFICATION COMPLETE'
\echo '============================================================================'
