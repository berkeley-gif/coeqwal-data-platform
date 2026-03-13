-- =============================================================================
-- 09_verify_level01.sql
-- Comprehensive verification of Level 01 (Lookup/Reference Tables)
-- =============================================================================
-- Run in Cloud9: \i database/scripts/sql/01_lookup/09_verify_level01.sql
-- =============================================================================
--
-- Level 01 Tables (13 total):
--   hydrologic_region, source, model_source, unit, spatial_scale,
--   temporal_scale, statistic_category, statistic_type, geometry_type,
--   network_entity_type, network_type, network_subtype, watershed
--
-- Verification Checks:
--   1. Audit columns exist
--   2. Audit triggers applied
--   3. Version family mapping
--   4. FK relationships (lookups referencing other lookups)
--   5. Row counts
--   6. Schema accuracy
--   7. Data integrity
--   8. Naming conventions
-- =============================================================================

\echo '============================================================================'
\echo 'LEVEL 01 LOOKUP TABLES - VERIFICATION'
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
AND t.table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                     'spatial_scale', 'temporal_scale', 'statistic_category',
                     'statistic_type', 'geometry_type', 'network_entity_type',
                     'network_type', 'network_subtype', 'watershed')
GROUP BY t.table_name
ORDER BY t.table_name;

SELECT 'Tables MISSING audit columns (should be 0):' as check;
SELECT t.table_name, 4 - COUNT(DISTINCT c.column_name) as missing_count
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
    ON t.table_name = c.table_name 
    AND c.column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by')
WHERE t.table_schema = 'public' 
AND t.table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                     'spatial_scale', 'temporal_scale', 'statistic_category',
                     'statistic_type', 'geometry_type', 'network_entity_type',
                     'network_type', 'network_subtype', 'watershed')
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
AND event_object_table IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                           'spatial_scale', 'temporal_scale', 'statistic_category',
                           'statistic_type', 'geometry_type', 'network_entity_type',
                           'network_type', 'network_subtype', 'watershed');

SELECT 'Tables WITHOUT audit triggers (should be 0):' as check;
SELECT t.table_name
FROM information_schema.tables t
WHERE t.table_schema = 'public' 
AND t.table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                     'spatial_scale', 'temporal_scale', 'statistic_category',
                     'statistic_type', 'geometry_type', 'network_entity_type',
                     'network_type', 'network_subtype', 'watershed')
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
WHERE dfm.table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                         'spatial_scale', 'temporal_scale', 'statistic_category',
                         'statistic_type', 'geometry_type', 'network_entity_type',
                         'network_type', 'network_subtype', 'watershed')
ORDER BY dfm.table_name;

SELECT 'Tables NOT in domain_family_map (should be 0):' as check;
SELECT t.table_name
FROM information_schema.tables t
WHERE t.table_schema = 'public' 
AND t.table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                     'spatial_scale', 'temporal_scale', 'statistic_category',
                     'statistic_type', 'geometry_type', 'network_entity_type',
                     'network_type', 'network_subtype', 'watershed')
AND t.table_name NOT IN (SELECT table_name FROM domain_family_map);

\echo ''
\echo 'Expected version family mappings for Level 01:'
\echo '  geometry_type          -> geospatial (9)'
\echo '  watershed              -> geospatial (9)'
\echo '  network_entity_type    -> network (12)'
\echo '  network_type           -> network (12)'
\echo '  network_subtype        -> network (12)'
\echo '  All others             -> metadata (11)'

-- =============================================================================
-- 4. FK RELATIONSHIPS (created_by, updated_by -> developer)
-- =============================================================================
\echo ''
\echo '4. FK RELATIONSHIPS'
\echo '-------------------'

SELECT 'FK constraints to developer table:' as check;
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
AND tc.table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                      'spatial_scale', 'temporal_scale', 'statistic_category',
                      'statistic_type', 'geometry_type', 'network_entity_type',
                      'network_type', 'network_subtype', 'watershed')
AND ccu.table_name = 'developer'
ORDER BY tc.table_name, kcu.column_name;

-- =============================================================================
-- 5. ROW COUNTS
-- =============================================================================
\echo ''
\echo '5. ROW COUNTS'
\echo '-------------'

SELECT 'hydrologic_region' as table_name, COUNT(*) as rows, 
       SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active FROM hydrologic_region
UNION ALL SELECT 'source', COUNT(*), SUM(CASE WHEN is_active THEN 1 ELSE 0 END) FROM source
UNION ALL SELECT 'model_source', COUNT(*), COUNT(*) FROM model_source
UNION ALL SELECT 'unit', COUNT(*), SUM(CASE WHEN is_active THEN 1 ELSE 0 END) FROM unit
UNION ALL SELECT 'spatial_scale', COUNT(*), COUNT(*) FROM spatial_scale
UNION ALL SELECT 'temporal_scale', COUNT(*), COUNT(*) FROM temporal_scale
UNION ALL SELECT 'statistic_category', COUNT(*), COUNT(*) FROM statistic_category
UNION ALL SELECT 'statistic_type', COUNT(*), COUNT(*) FROM statistic_type
UNION ALL SELECT 'geometry_type', COUNT(*), COUNT(*) FROM geometry_type
UNION ALL SELECT 'network_entity_type', COUNT(*), SUM(CASE WHEN is_active THEN 1 ELSE 0 END) FROM network_entity_type
UNION ALL SELECT 'network_type', COUNT(*), SUM(CASE WHEN is_active THEN 1 ELSE 0 END) FROM network_type
UNION ALL SELECT 'network_subtype', COUNT(*), SUM(CASE WHEN is_active THEN 1 ELSE 0 END) FROM network_subtype
UNION ALL SELECT 'watershed', COUNT(*), SUM(CASE WHEN is_active THEN 1 ELSE 0 END) FROM watershed
ORDER BY table_name;

-- =============================================================================
-- 6. SCHEMA ACCURACY
-- =============================================================================
\echo ''
\echo '6. SCHEMA ACCURACY'
\echo '------------------'

SELECT 'Tables with standard short_code/label columns:' as check;
SELECT t.table_name,
    MAX(CASE WHEN c.column_name = 'short_code' THEN 'Y' 
             WHEN c.column_name = 'code' THEN 'code'
             WHEN c.column_name = 'source' THEN 'source'
             ELSE 'N' END) as has_short_code,
    MAX(CASE WHEN c.column_name = 'label' THEN 'Y' 
             WHEN c.column_name = 'name' THEN 'name'
             WHEN c.column_name = 'full_name' THEN 'full_name'
             WHEN c.column_name = 'description' THEN 'desc'
             ELSE 'N' END) as has_label
FROM information_schema.tables t
LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
WHERE t.table_schema = 'public' 
AND t.table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                     'spatial_scale', 'temporal_scale', 'statistic_category',
                     'statistic_type', 'geometry_type', 'network_entity_type',
                     'network_type', 'network_subtype', 'watershed')
GROUP BY t.table_name
ORDER BY t.table_name;

-- =============================================================================
-- 7. DATA INTEGRITY
-- =============================================================================
\echo ''
\echo '7. DATA INTEGRITY'
\echo '-----------------'

SELECT 'Referential integrity - invalid created_by (should be 0):' as check;
SELECT 'hydrologic_region' as table_name, COUNT(*) as invalid
FROM hydrologic_region WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'source', COUNT(*) FROM source WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'statistic_category', COUNT(*) FROM statistic_category WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'statistic_type', COUNT(*) FROM statistic_type WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'network_entity_type', COUNT(*) FROM network_entity_type WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'network_type', COUNT(*) FROM network_type WHERE created_by NOT IN (SELECT id FROM developer)
UNION ALL SELECT 'network_subtype', COUNT(*) FROM network_subtype WHERE created_by NOT IN (SELECT id FROM developer);

SELECT 'Duplicate short_codes (should be 0):' as check;
SELECT 'hydrologic_region' as table_name, short_code, COUNT(*) as duplicates
FROM hydrologic_region GROUP BY short_code HAVING COUNT(*) > 1
UNION ALL SELECT 'spatial_scale', short_code, COUNT(*) FROM spatial_scale GROUP BY short_code HAVING COUNT(*) > 1
UNION ALL SELECT 'temporal_scale', short_code, COUNT(*) FROM temporal_scale GROUP BY short_code HAVING COUNT(*) > 1
UNION ALL SELECT 'statistic_category', short_code, COUNT(*) FROM statistic_category GROUP BY short_code HAVING COUNT(*) > 1
UNION ALL SELECT 'statistic_type', short_code, COUNT(*) FROM statistic_type GROUP BY short_code HAVING COUNT(*) > 1
UNION ALL SELECT 'geometry_type', short_code, COUNT(*) FROM geometry_type GROUP BY short_code HAVING COUNT(*) > 1
UNION ALL SELECT 'network_entity_type', short_code, COUNT(*) FROM network_entity_type GROUP BY short_code HAVING COUNT(*) > 1
UNION ALL SELECT 'network_type', short_code, COUNT(*) FROM network_type GROUP BY short_code HAVING COUNT(*) > 1;

SELECT 'Source IDs sequential (no gaps):' as check;
SELECT MAX(id) as max_id, COUNT(*) as row_count,
       CASE WHEN MAX(id) = COUNT(*) THEN 'PASS' ELSE 'GAP' END as status
FROM source;

-- =============================================================================
-- 8. NAMING CONVENTIONS
-- =============================================================================
\echo ''
\echo '8. NAMING CONVENTIONS'
\echo '---------------------'

SELECT 'Plural table names (should be 0):' as check;
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
AND table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                   'spatial_scale', 'temporal_scale', 'statistic_type', 
                   'geometry_type', 'network_entity_type', 'network_type',
                   'network_subtype', 'watershed')
AND (table_name LIKE '%s' AND table_name NOT LIKE '%ss' AND table_name NOT LIKE '%ics');

-- =============================================================================
-- 9. LAYER-SPECIFIC: NETWORK LOOKUP HIERARCHY
-- =============================================================================
\echo ''
\echo '9. NETWORK LOOKUP HIERARCHY'
\echo '---------------------------'

SELECT 'Entity types:' as check;
SELECT id, short_code, label FROM network_entity_type ORDER BY id;

SELECT 'Network type hierarchy (type → entity_type):' as check;
SELECT nt.short_code as type, net.short_code as entity_type, COUNT(ns.id) as subtypes
FROM network_type nt
JOIN network_entity_type net ON nt.network_entity_type_id = net.id
LEFT JOIN network_subtype ns ON ns.type_id = nt.id
GROUP BY nt.id, nt.short_code, net.short_code
ORDER BY net.short_code, nt.short_code;

-- =============================================================================
-- 10. SUMMARY
-- =============================================================================
\echo ''
\echo '============================================================================'
\echo '10. SUMMARY'
\echo '============================================================================'

SELECT 
    13 as total_tables,
    (SELECT COUNT(*) FROM (
        SELECT t.table_name
        FROM information_schema.tables t
        JOIN information_schema.columns c ON t.table_name = c.table_name
        WHERE t.table_schema = 'public' 
AND t.table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                      'spatial_scale', 'temporal_scale', 'statistic_category',
                      'statistic_type', 'geometry_type', 'network_entity_type',
                      'network_type', 'network_subtype', 'watershed')
        AND c.column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by')
        GROUP BY t.table_name
        HAVING COUNT(DISTINCT c.column_name) = 4
    ) sub) as tables_with_audit_cols,
    (SELECT COUNT(DISTINCT event_object_table) 
     FROM information_schema.triggers 
     WHERE trigger_schema = 'public' AND trigger_name LIKE 'audit_fields_%'
     AND event_object_table IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                                'spatial_scale', 'temporal_scale', 'statistic_category',
                                'statistic_type', 'geometry_type', 'network_entity_type',
                                'network_type', 'network_subtype', 'watershed')) as tables_with_triggers,
    (SELECT COUNT(*) FROM domain_family_map 
     WHERE table_name IN ('hydrologic_region', 'source', 'model_source', 'unit', 
                          'spatial_scale', 'temporal_scale', 'statistic_category',
                          'statistic_type', 'geometry_type', 'network_entity_type',
                          'network_type', 'network_subtype', 'watershed')) as tables_in_domain_map;

\echo ''
\echo 'Expected values:'
\echo '  - total_tables: 13'
\echo '  - tables_with_audit_cols: 13'
\echo '  - tables_with_triggers: 13'
\echo '  - tables_in_domain_map: 13'
\echo '============================================================================'
\echo 'VERIFICATION COMPLETE'
\echo '============================================================================'
