-- LOAD RESERVOIR ENTITY DATA FROM S3
-- Loads: reservoir_entity, reservoir_group, reservoir_group_member
-- Source: s3://coeqwal-seeds-dev/04_calsim_data/
--
-- Prerequisites:
--   1. Run 01_create_reservoir_entity_tables.sql first
--   2. Ensure aws_s3 extension is enabled
--   3. Upload CSVs to S3 bucket
--
-- Run with: psql -f 02_load_reservoir_entity_from_s3.sql

\echo ''
\echo '========================================='
\echo 'LOADING RESERVOIR ENTITY DATA FROM S3'
\echo '========================================='

-- ============================================
-- CLEAR EXISTING DATA
-- ============================================
\echo ''
\echo 'Clearing existing data for clean load...'

-- Delete in order respecting FK constraints
TRUNCATE TABLE reservoir_group_member CASCADE;
TRUNCATE TABLE reservoir_group CASCADE;
TRUNCATE TABLE reservoir_entity CASCADE;

\echo '✅ Existing data cleared'

-- ============================================
-- 1. LOAD RESERVOIR_ENTITY
-- ============================================
\echo ''
\echo 'Loading reservoir_entity from S3...'

SELECT aws_s3.table_import_from_s3(
    'reservoir_entity',
    'id, network_node_id, short_code, name, description, associated_river, entity_type_id, schematic_type_id, hydrologic_region_id, capacity_taf, dead_pool_taf, surface_area_acres, operational_purpose, has_gis_data, entity_version_id, source_ids',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/reservoir_entity.csv',
    'us-west-2'
);

\echo '✅ reservoir_entity loaded'

-- ============================================
-- 2. LOAD RESERVOIR_GROUP
-- ============================================
\echo ''
\echo 'Loading reservoir_group from S3...'

SELECT aws_s3.table_import_from_s3(
    'reservoir_group',
    'id, short_code, label, description, display_order, is_active, created_at, created_by, updated_at, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/reservoir_group.csv',
    'us-west-2'
);

\echo '✅ reservoir_group loaded'

-- ============================================
-- 3. LOAD RESERVOIR_GROUP_MEMBER
-- ============================================
\echo ''
\echo 'Loading reservoir_group_member from S3...'

SELECT aws_s3.table_import_from_s3(
    'reservoir_group_member',
    'id, reservoir_group_id, reservoir_entity_id, display_order, is_active, created_at, created_by, updated_at, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/reservoir_group_member.csv',
    'us-west-2'
);

\echo '✅ reservoir_group_member loaded'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Record counts:'
SELECT 'reservoir_entity' as table_name, COUNT(*) as records FROM reservoir_entity
UNION ALL
SELECT 'reservoir_group', COUNT(*) FROM reservoir_group
UNION ALL
SELECT 'reservoir_group_member', COUNT(*) FROM reservoir_group_member;

\echo ''
\echo 'Major reservoirs (via reservoir_group):'
SELECT re.id, re.short_code, re.name, re.capacity_taf, re.hydrologic_region_id
FROM reservoir_entity re
JOIN reservoir_group_member rgm ON re.id = rgm.reservoir_entity_id
JOIN reservoir_group rg ON rgm.reservoir_group_id = rg.id
WHERE rg.short_code = 'major'
ORDER BY re.capacity_taf DESC;

\echo ''
\echo 'Reservoir groups:'
SELECT id, short_code, label,
       (SELECT COUNT(*) FROM reservoir_group_member WHERE reservoir_group_id = rg.id) as member_count
FROM reservoir_group rg
ORDER BY display_order;

\echo ''
\echo 'Major group members:'
SELECT rg.short_code as group_name, re.short_code as reservoir, re.capacity_taf
FROM reservoir_group_member rgm
JOIN reservoir_group rg ON rgm.reservoir_group_id = rg.id
JOIN reservoir_entity re ON rgm.reservoir_entity_id = re.id
WHERE rg.short_code = 'major'
ORDER BY rgm.display_order;

\echo ''
\echo '✅ All reservoir entity data loaded successfully'
