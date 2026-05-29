-- REMOVE FINAL DUPLICATE INDEX
-- Clean up the last remaining duplicate on hydrologic_region

\echo ''
\echo '🧹 REMOVING FINAL DUPLICATE INDEX'
\echo '================================='
\echo ''

\echo '🔍 hydrologic_region currently has 2 short_code indexes:'
\echo '• hydrologic_region_short_code_key (✅ ERD specified - KEEP)'
\echo '• idx_hydrologic_region_short_code (❓ duplicate - REMOVE)'
\echo ''

DROP INDEX IF EXISTS idx_hydrologic_region_short_code;
\echo '  ✅ Dropped idx_hydrologic_region_short_code (duplicate)'

\echo ''
\echo '🔍 Final hydrologic_region indexes:'
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'hydrologic_region' 
ORDER BY indexname;

\echo ''
\echo '✅ FINAL DUPLICATE CLEANUP COMPLETE!'
