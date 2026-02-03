-- LOAD DU_URBAN_GROUP DATA FROM S3
-- Loads: du_urban_group, du_urban_group_member
-- Source: s3://coeqwal-seeds-dev/04_calsim_data/
--
-- Prerequisites:
--   1. Run 01_create_du_urban_entity.sql first
--   2. Run 01b_load_du_urban_entity_from_s3.sql first
--   3. Run 02b_create_du_urban_group_tables.sql first
--   4. Ensure aws_s3 extension is enabled
--   5. Upload CSVs to S3 bucket
--
-- Run with: psql -f 02c_load_du_urban_group_from_s3.sql

\echo ''
\echo '========================================='
\echo 'LOADING DU_URBAN_GROUP DATA FROM S3'
\echo '========================================='

-- ============================================
-- CLEAR EXISTING DATA
-- ============================================
\echo ''
\echo 'Clearing existing data for clean load...'

-- Delete in order respecting FK constraints
TRUNCATE TABLE du_urban_group_member CASCADE;
TRUNCATE TABLE du_urban_group CASCADE;

\echo '✅ Existing data cleared'

-- ============================================
-- 1. LOAD DU_URBAN_GROUP
-- ============================================
\echo ''
\echo 'Loading du_urban_group from S3...'

SELECT aws_s3.table_import_from_s3(
    'du_urban_group',
    'id, short_code, label, description, display_order, is_active, created_at, created_by, updated_at, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/du_urban_group.csv',
    'us-west-2'
);

\echo '✅ du_urban_group loaded'

-- ============================================
-- 2. LOAD DU_URBAN_GROUP_MEMBER
-- ============================================
\echo ''
\echo 'Loading du_urban_group_member from S3...'

SELECT aws_s3.table_import_from_s3(
    'du_urban_group_member',
    'id, du_urban_group_id, du_id, display_order, is_active, created_at, created_by, updated_at, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/du_urban_group_member.csv',
    'us-west-2'
);

\echo '✅ du_urban_group_member loaded'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Record counts:'
SELECT 'du_urban_group' as table_name, COUNT(*) as records FROM du_urban_group
UNION ALL
SELECT 'du_urban_group_member', COUNT(*) FROM du_urban_group_member;

\echo ''
\echo 'Groups with member counts:'
SELECT g.id, g.short_code, g.label,
       (SELECT COUNT(*) FROM du_urban_group_member WHERE du_urban_group_id = g.id) as member_count
FROM du_urban_group g
ORDER BY g.display_order;

\echo ''
\echo 'Tier group members (sample):'
SELECT gm.du_id, du.community_agency
FROM du_urban_group_member gm
JOIN du_urban_group g ON gm.du_urban_group_id = g.id
LEFT JOIN du_urban_entity du ON gm.du_id = du.du_id
WHERE g.short_code = 'tier'
ORDER BY gm.display_order
LIMIT 15;

\echo ''
\echo '✅ All DU_URBAN_GROUP data loaded successfully'
