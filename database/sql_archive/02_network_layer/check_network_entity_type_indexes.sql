-- CHECK NETWORK_ENTITY_TYPE INDEXES
-- Verify what indexes actually exist vs. what ERD documents

\echo ''
\echo '🔍 CHECKING NETWORK_ENTITY_TYPE INDEXES'
\echo '======================================'
\echo ''

\echo '🔍 Current indexes on network_entity_type:'
SELECT 
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'network_entity_type'
ORDER BY indexname;

\echo ''
\echo '🔍 Table structure:'
\d network_entity_type

\echo ''
\echo '📊 ANALYSIS:'
\echo 'ERD currently shows:'
\echo '• network_entity_type_pkey (id)'
\echo '• network_entity_type_short_code_key (short_code)'
\echo ''
\echo 'Database should have:'
\echo '• Primary key index (automatic)'
\echo '• Unique constraint index on short_code (automatic)'
\echo '• No additional performance indexes needed (4 records)'
\echo ''
