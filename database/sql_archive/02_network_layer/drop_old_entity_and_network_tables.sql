-- DROP OLD ENTITY AND NETWORK TABLES FOR CLEAN REDESIGN
-- Removes old entity tables and network type tables to start fresh

\echo ''
\echo 'DROPPING OLD ENTITY AND NETWORK TABLES FOR CLEAN REDESIGN'
\echo '========================================================='
\echo ''
\echo 'This script will remove:'
\echo '• Old entity tables (using network_topology_id/network_node_id)'
\echo '• Old source link tables (replaced by source_ids arrays)'
\echo '• Old separate network type tables (replaced by unified approach)'
\echo '• Legacy network_integration_status table'
\echo ''
\echo 'This script will KEEP:'
\echo '• network_entity_type (foundation table)'
\echo '• All 00_versioning tables'
\echo '• All 01_lookup tables'
\echo '• network_gis (will be migrated later)'
\echo ''

-- ============================================================================
-- 1. DROP OLD ENTITY TABLES
-- ============================================================================

\echo '1. DROPPING OLD ENTITY TABLES'
\echo '============================='

\echo ''
\echo 'Dropping entity tables with old network references...'

DROP TABLE IF EXISTS channel_entity CASCADE;
\echo '  ✅ Dropped channel_entity (used network_topology_id)'

DROP TABLE IF EXISTS inflow_entity CASCADE;
\echo '  ✅ Dropped inflow_entity (used network_topology_id)'

DROP TABLE IF EXISTS reservoir_entity CASCADE;
\echo '  ✅ Dropped reservoir_entity (used network_node_id)'

DROP TABLE IF EXISTS du_agriculture_entity CASCADE;
\echo '  ✅ Dropped du_agriculture_entity (used old structure)'

DROP TABLE IF EXISTS du_urban_entity CASCADE;
\echo '  ✅ Dropped du_urban_entity (used old structure)'

DROP TABLE IF EXISTS du_refuge_entity CASCADE;
\echo '  ✅ Dropped du_refuge_entity (used old structure)'

-- ============================================================================
-- 2. DROP OLD SOURCE LINK TABLES
-- ============================================================================

\echo ''
\echo '2. DROPPING OLD SOURCE LINK TABLES'
\echo '=================================='

\echo ''
\echo 'Dropping source link tables (replaced by source_ids arrays)...'

DROP TABLE IF EXISTS channel_entity_source_link CASCADE;
\echo '  ✅ Dropped channel_entity_source_link (replaced by source_ids array)'

DROP TABLE IF EXISTS inflow_entity_source_link CASCADE;
\echo '  ✅ Dropped inflow_entity_source_link (replaced by source_ids array)'

-- ============================================================================
-- 3. DROP OLD NETWORK TYPE TABLES
-- ============================================================================

\echo ''
\echo '3. DROPPING OLD NETWORK TYPE TABLES'
\echo '=================================='

\echo ''
\echo 'Dropping separate network type tables (replaced by unified approach)...'

DROP TABLE IF EXISTS network_arc_type CASCADE;
\echo '  ✅ Dropped network_arc_type (replaced by unified network_type)'

DROP TABLE IF EXISTS network_node_type CASCADE;
\echo '  ✅ Dropped network_node_type (replaced by unified network_type)'

DROP TABLE IF EXISTS network_arc_subtype CASCADE;
\echo '  ✅ Dropped network_arc_subtype (replaced by unified network_subtype)'

DROP TABLE IF EXISTS network_node_subtype CASCADE;
\echo '  ✅ Dropped network_node_subtype (replaced by unified network_subtype)'

-- ============================================================================
-- 4. DROP LEGACY TABLES
-- ============================================================================

\echo ''
\echo '4. DROPPING LEGACY TABLES'
\echo '========================='

\echo ''
\echo 'Dropping legacy tables from old schema...'

DROP TABLE IF EXISTS network_integration_status CASCADE;
\echo '  ✅ Dropped network_integration_status (legacy table)'

-- ============================================================================
-- 5. VERIFY REMAINING TABLES
-- ============================================================================

\echo ''
\echo '5. VERIFICATION - REMAINING TABLES'
\echo '=================================='

\echo ''
\echo 'Tables that should remain:'
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'public'
    AND tablename IN ('network_entity_type', 'version_family', 'version', 'developer', 
                      'domain_family_map', 'hydrologic_region', 'source', 'model_source',
                      'geometry_type', 'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
ORDER BY tablename;

\echo ''
\echo 'Verify these tables were removed:'
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'channel_entity')
        THEN 'channel_entity still exists'
        ELSE 'channel_entity removed ✅'
    END as channel_entity_status,
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'network_arc_type')
        THEN 'network_arc_type still exists'
        ELSE 'network_arc_type removed ✅'
    END as network_arc_type_status,
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'reservoir_entity')
        THEN 'reservoir_entity still exists'
        ELSE 'reservoir_entity removed ✅'
    END as reservoir_entity_status;

\echo ''
\echo 'DATABASE CLEANUP COMPLETE!'
\echo ''
\echo 'Ready for clean implementation of:'
\echo '• 02_network layer (when geopackage is ready)'
\echo '• 03_entity_system layer (can start immediately)'
\echo '• 04_variable_system layer (can start immediately)'
\echo ''
