-- =============================================================================
-- 09_verify_level02.sql
-- Comprehensive verification of Level 02 (Network Layer)
-- =============================================================================
-- Run in Cloud9: \i database/scripts/sql/02_network/09_verify_level02.sql
-- =============================================================================
--
-- Level 02 Tables (4 total):
--   network, network_arc, network_node, network_gis
--
-- Note: network_entity_type, network_type, network_subtype are now in Level 01
-- (lookup tables). They are verified in 09_verify_level01.sql.
--
-- Verification Checks:
--   1. Audit columns exist
--   2. Audit triggers applied
--   3. Version family mapping
--   4. FK relationships (to developer, to lookups)
--   5. Row counts
--   6. Schema accuracy
--   7. Data integrity
--   8. Naming conventions
--   9. Layer-specific: connectivity and entity type references
-- =============================================================================

\echo '============================================================================'
\echo 'LEVEL 02 NETWORK LAYER - VERIFICATION'
\echo '============================================================================'

-- =============================================================================
-- 1. AUDIT COLUMNS
-- =============================================================================
\echo ''
\echo '1. AUDIT COLUMNS'
\echo '----------------'

SELECT 'Tables with all 4 audit columns:' as check;
SELECT t.table_name,
    MAX(CASE WHEN c.column_name = 'created_at' THEN 'Y' ELSE 'N' END) as created_at,
    MAX(CASE WHEN c.column_name = 'created_by' THEN 'Y' ELSE 'N' END) as created_by,
    MAX(CASE WHEN c.column_name = 'updated_at' THEN 'Y' ELSE 'N' END) as updated_at,
    MAX(CASE WHEN c.column_name = 'updated_by' THEN 'Y' ELSE 'N' END) as updated_by
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
    ON t.table_name = c.table_name 
    AND c.column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by')
WHERE t.table_schema = 'public' 
AND t.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
GROUP BY t.table_name
ORDER BY t.table_name;

SELECT 'Tables MISSING audit columns (should be 0):' as check;
SELECT t.table_name, 4 - COUNT(DISTINCT c.column_name) as missing_count
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
    ON t.table_name = c.table_name 
    AND c.column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by')
WHERE t.table_schema = 'public' 
AND t.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
GROUP BY t.table_name
HAVING COUNT(DISTINCT c.column_name) < 4;

-- =============================================================================
-- 2. AUDIT TRIGGERS
-- =============================================================================
\echo ''
\echo '2. AUDIT TRIGGERS'
\echo '-----------------'

SELECT 'Tables with audit triggers:' as check;
SELECT COUNT(DISTINCT event_object_table) as count
FROM information_schema.triggers 
WHERE trigger_schema = 'public' 
AND trigger_name LIKE 'audit_fields_%'
AND event_object_table IN ('network', 'network_arc', 'network_node', 'network_gis');

SELECT 'Tables WITHOUT audit triggers (should be 0):' as check;
SELECT t.table_name
FROM information_schema.tables t
WHERE t.table_schema = 'public' 
AND t.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
AND NOT EXISTS (
    SELECT 1 FROM information_schema.triggers tr 
    WHERE tr.event_object_table = t.table_name 
    AND tr.trigger_name LIKE 'audit_fields_%'
);

-- =============================================================================
-- 3. VERSION FAMILY MAPPING
-- =============================================================================
\echo ''
\echo '3. VERSION FAMILY MAPPING'
\echo '-------------------------'

SELECT 'Tables mapped to version families:' as check;
SELECT 
    dfm.table_name,
    vf.short_code as version_family,
    vf.id as family_id,
    dfm.database_level,
    dfm.note
FROM domain_family_map dfm
JOIN version_family vf ON dfm.version_family_id = vf.id
WHERE dfm.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
ORDER BY dfm.table_name;

SELECT 'Tables NOT in domain_family_map (should be 0):' as check;
SELECT t.table_name
FROM information_schema.tables t
WHERE t.table_schema = 'public' 
AND t.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
AND t.table_name NOT IN (SELECT table_name FROM domain_family_map);

\echo ''
\echo 'Expected version family mappings:'
\echo '  network_gis -> geospatial (9)'
\echo '  All others  -> network (12)'

-- =============================================================================
-- 4. FK RELATIONSHIPS
-- =============================================================================
\echo ''
\echo '4. FK RELATIONSHIPS'
\echo '-------------------'

SELECT 'All FK constraints for network tables:' as check;
SELECT 
    tc.table_name,
    kcu.column_name,
    ccu.table_name as references_table
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu 
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
AND tc.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
ORDER BY tc.table_name, kcu.column_name;

SELECT 'FK to lookup tables (Level 01):' as check;
SELECT 
    tc.table_name,
    kcu.column_name,
    ccu.table_name as lookup_table
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu 
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
AND tc.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
AND ccu.table_name IN ('hydrologic_region', 'source', 'model_source',
                       'network_entity_type', 'network_type', 'network_subtype')
ORDER BY tc.table_name, kcu.column_name;

-- =============================================================================
-- 5. ROW COUNTS
-- =============================================================================
\echo ''
\echo '5. ROW COUNTS'
\echo '-------------'

SELECT 'network' as table_name, COUNT(*) as rows FROM network
UNION ALL SELECT 'network_arc', COUNT(*) FROM network_arc
UNION ALL SELECT 'network_node', COUNT(*) FROM network_node
UNION ALL SELECT 'network_gis', COUNT(*) FROM network_gis
ORDER BY table_name;

-- =============================================================================
-- 6. SCHEMA ACCURACY
-- =============================================================================
\echo ''
\echo '6. SCHEMA ACCURACY'
\echo '------------------'

SELECT 'Key columns per table:' as check;
SELECT t.table_name, 
    string_agg(c.column_name, ', ' ORDER BY c.ordinal_position) as columns
FROM information_schema.tables t
JOIN information_schema.columns c ON t.table_name = c.table_name
WHERE t.table_schema = 'public'
AND t.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
AND c.column_name NOT IN ('created_at', 'created_by', 'updated_at', 'updated_by')
GROUP BY t.table_name
ORDER BY t.table_name;

-- =============================================================================
-- 7. DATA INTEGRITY
-- =============================================================================
\echo ''
\echo '7. DATA INTEGRITY'
\echo '-----------------'

SELECT 'Referential integrity - invalid created_by (should be 0):' as check;
SELECT 'network' as table_name, COUNT(*) as invalid
FROM network WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'network_arc', COUNT(*) FROM network_arc WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'network_node', COUNT(*) FROM network_node WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'network_gis', COUNT(*) FROM network_gis WHERE created_by NOT IN (SELECT id FROM developer);

-- =============================================================================
-- 8. NAMING CONVENTIONS
-- =============================================================================
\echo ''
\echo '8. NAMING CONVENTIONS'
\echo '---------------------'

SELECT 'Plural table names (should be 0):' as check;
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
AND table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
AND (table_name LIKE '%s' AND table_name NOT LIKE '%ss' AND table_name NOT LIKE '%ics' AND table_name NOT LIKE '%_gis');

-- =============================================================================
-- 9. LAYER-SPECIFIC: NETWORK RELATIONSHIPS
-- =============================================================================
\echo ''
\echo '9. NETWORK-SPECIFIC CHECKS'
\echo '--------------------------'

SELECT 'Network entries by type:' as check;
SELECT nt.short_code as type, COUNT(n.id) as networks
FROM network_type nt
LEFT JOIN network n ON n.type_id = nt.id
GROUP BY nt.id, nt.short_code
ORDER BY COUNT(n.id) DESC;

SELECT 'Networks with GIS data:' as check;
SELECT COUNT(*) as total_network,
       SUM(CASE WHEN has_gis THEN 1 ELSE 0 END) as with_gis,
       SUM(CASE WHEN NOT has_gis THEN 1 ELSE 0 END) as without_gis
FROM network;

-- =============================================================================
-- 10. SUMMARY
-- =============================================================================
\echo ''
\echo '============================================================================'
\echo '10. SUMMARY'
\echo '============================================================================'

SELECT 
    4 as total_tables,
    (SELECT COUNT(*) FROM (
        SELECT t.table_name
        FROM information_schema.tables t
        JOIN information_schema.columns c ON t.table_name = c.table_name
        WHERE t.table_schema = 'public' 
        AND t.table_name IN ('network', 'network_arc', 'network_node', 'network_gis')
        AND c.column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by')
        GROUP BY t.table_name
        HAVING COUNT(DISTINCT c.column_name) = 4
    ) sub) as tables_with_audit_cols,
    (SELECT COUNT(DISTINCT event_object_table) 
     FROM information_schema.triggers 
     WHERE trigger_schema = 'public' AND trigger_name LIKE 'audit_fields_%'
     AND event_object_table IN ('network', 'network_arc', 'network_node', 'network_gis')) as tables_with_triggers,
    (SELECT COUNT(*) FROM domain_family_map 
     WHERE table_name IN ('network', 'network_arc', 'network_node', 'network_gis')) as tables_in_domain_map;

\echo ''
\echo 'Expected values:'
\echo '  - total_tables: 4'
\echo '  - tables_with_audit_cols: 4'
\echo '  - tables_with_triggers: 4'
\echo '  - tables_in_domain_map: 4'
\echo '============================================================================'
\echo 'VERIFICATION COMPLETE'
\echo '============================================================================'
