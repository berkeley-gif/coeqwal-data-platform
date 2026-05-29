-- LOAD NETWORK TYPE TABLES USING PSQL \copy COMMANDS
-- Simple psql script to load the 4 network type tables from S3

\echo '📥 DOWNLOADING AND LOADING NETWORK TYPE TABLES FROM S3'
\echo '======================================================'
\echo ''

\echo 'Loading network_arc_type...'
\! aws s3 cp s3://coeqwal-seeds-dev/01_network/network_arc_type.csv /tmp/network_arc_type.csv
\copy network_arc_type (short_code, label, description, network_entity_type_id, model_source_id, source_id, is_active) FROM '/tmp/network_arc_type.csv' WITH CSV HEADER

\echo 'Loading network_node_type...'
\! aws s3 cp s3://coeqwal-seeds-dev/01_network/network_node_type.csv /tmp/network_node_type.csv
\copy network_node_type (short_code, label, description, network_entity_type_id, model_source_id, source_id, is_active) FROM '/tmp/network_node_type.csv' WITH CSV HEADER

\echo 'Loading network_arc_subtype...'
\! aws s3 cp s3://coeqwal-seeds-dev/01_network/network_arc_subtype.csv /tmp/network_arc_subtype.csv
\copy network_arc_subtype (short_code, label, description, arc_type_id, model_source_id, source_id, is_active) FROM '/tmp/network_arc_subtype.csv' WITH CSV HEADER

\echo 'Loading network_node_subtype...'
\! aws s3 cp s3://coeqwal-seeds-dev/01_network/network_node_subtype.csv /tmp/network_node_subtype.csv
\copy network_node_subtype (short_code, label, description, node_type_id, model_source_id, source_id, is_active) FROM '/tmp/network_node_subtype.csv' WITH CSV HEADER

\echo ''
\echo '📊 VERIFICATION - Record Counts:'
SELECT 'network_arc_type' as table_name, COUNT(*) as records FROM network_arc_type
UNION ALL
SELECT 'network_node_type', COUNT(*) FROM network_node_type  
UNION ALL
SELECT 'network_arc_subtype', COUNT(*) FROM network_arc_subtype
UNION ALL
SELECT 'network_node_subtype', COUNT(*) FROM network_node_subtype
ORDER BY table_name;

\echo ''
\echo '👀 SAMPLE DATA:'
\echo 'Arc types:'
SELECT short_code, label, network_entity_type_id FROM network_arc_type ORDER BY short_code LIMIT 5;

\echo 'Node types:'
SELECT short_code, label, network_entity_type_id FROM network_node_type ORDER BY short_code LIMIT 5;

\! rm -f /tmp/network_arc_type.csv /tmp/network_node_type.csv /tmp/network_arc_subtype.csv /tmp/network_node_subtype.csv

\echo ''
\echo '✅ NETWORK TYPE TABLES LOADED SUCCESSFULLY FROM S3'
