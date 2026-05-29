-- FIX DOMAIN_FAMILY_MAP INVALID TABLE REFERENCES
-- Remove mappings for tables that no longer exist

\echo ''
\echo 'FIXING DOMAIN_FAMILY_MAP INVALID REFERENCES'
\echo '==========================================='
\echo ''

\echo '1. CHECKING FOR INVALID TABLE REFERENCES'
\echo '========================================'

\echo ''
\echo 'Current domain_family_map entries:'
SELECT 
    table_name,
    version_family_id,
    note,
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_tables WHERE tablename = dfm.table_name AND schemaname = 'public')
        THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END as table_status
FROM domain_family_map dfm
ORDER BY table_name;

\echo ''
\echo '2. REMOVING INVALID REFERENCES'
\echo '============================='

\echo ''
\echo 'Removing mappings for deleted tables...'

DELETE FROM domain_family_map 
WHERE table_name IN ('channel_entity', 'inflow_entity', 'reservoir_entity');

\echo '  ✅ Removed deleted entity table references'

DELETE FROM domain_family_map 
WHERE table_name IN ('network_arc', 'network_node');

\echo '  ✅ Removed non-existent network table references'

DELETE FROM domain_family_map dfm
WHERE NOT EXISTS (
    SELECT 1 FROM pg_tables pt 
    WHERE pt.tablename = dfm.table_name 
    AND pt.schemaname = 'public'
);

\echo '  ✅ Removed any other invalid table references'

\echo ''
\echo '3. VERIFICATION'
\echo '=============='

\echo ''
\echo 'Remaining valid domain_family_map entries:'
SELECT 
    COUNT(*) as total_mappings,
    COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM pg_tables WHERE tablename = dfm.table_name AND schemaname = 'public')) as valid_mappings
FROM domain_family_map dfm;

\echo ''
\echo 'All remaining mappings (should all be valid):'
SELECT 
    table_name,
    vf.short_code as version_family,
    note
FROM domain_family_map dfm
JOIN version_family vf ON dfm.version_family_id = vf.id
ORDER BY vf.short_code, table_name;

\echo ''
\echo '✅ DOMAIN_FAMILY_MAP CLEANUP COMPLETE!'
\echo ''
\echo 'All invalid table references removed'
\echo 'Versioning system integrity restored'
\echo ''
