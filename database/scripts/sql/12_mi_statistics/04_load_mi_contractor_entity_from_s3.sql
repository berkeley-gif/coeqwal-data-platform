-- LOAD MI_CONTRACTOR ENTITY DATA FROM S3
-- Loads: mi_contractor_group, mi_contractor, mi_contractor_group_member, mi_contractor_delivery_arc
-- Source: s3://coeqwal-seeds-dev/04_calsim_data/
--
-- Prerequisites:
--   1. Run 03_create_mi_contractor_entity_tables.sql first
--   2. Ensure aws_s3 extension is enabled
--   3. Upload CSVs to S3 bucket
--
-- Run with: psql -f 04_load_mi_contractor_entity_from_s3.sql

\echo ''
\echo '========================================='
\echo 'LOADING MI_CONTRACTOR ENTITY DATA FROM S3'
\echo '========================================='

-- ============================================
-- CLEAR EXISTING DATA
-- ============================================
\echo ''
\echo 'Clearing existing data for clean load...'

-- Delete in order respecting FK constraints
TRUNCATE TABLE mi_contractor_delivery_arc CASCADE;
TRUNCATE TABLE mi_contractor_group_member CASCADE;
TRUNCATE TABLE mi_contractor CASCADE;
TRUNCATE TABLE mi_contractor_group CASCADE;

\echo '✅ Existing data cleared'

-- ============================================
-- 1. LOAD MI_CONTRACTOR_GROUP
-- ============================================
\echo ''
\echo 'Loading mi_contractor_group from S3...'

SELECT aws_s3.table_import_from_s3(
    'mi_contractor_group',
    'id, short_code, label, description, display_order, is_active, created_at, created_by, updated_at, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/mi_contractor_group.csv',
    'us-west-2'
);

\echo '✅ mi_contractor_group loaded'

-- ============================================
-- 2. LOAD MI_CONTRACTOR
-- ============================================
\echo ''
\echo 'Loading mi_contractor from S3...'

SELECT aws_s3.table_import_from_s3(
    'mi_contractor',
    'id, short_code, contractor_name, project, region, contractor_type, contract_amount_taf, source_contractor_id, source_file, is_active, created_at, created_by, updated_at, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/mi_contractor.csv',
    'us-west-2'
);

\echo '✅ mi_contractor loaded'

-- ============================================
-- 3. LOAD MI_CONTRACTOR_GROUP_MEMBER
-- ============================================
\echo ''
\echo 'Loading mi_contractor_group_member from S3...'

SELECT aws_s3.table_import_from_s3(
    'mi_contractor_group_member',
    'id, mi_contractor_group_id, mi_contractor_id, display_order, is_active, created_at, created_by, updated_at, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/mi_contractor_group_member.csv',
    'us-west-2'
);

\echo '✅ mi_contractor_group_member loaded'

-- ============================================
-- 4. LOAD MI_CONTRACTOR_DELIVERY_ARC
-- ============================================
\echo ''
\echo 'Loading mi_contractor_delivery_arc from S3...'

SELECT aws_s3.table_import_from_s3(
    'mi_contractor_delivery_arc',
    'id, mi_contractor_id, delivery_arc, arc_type, is_active, created_at, created_by, updated_at, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '04_calsim_data/mi_contractor_delivery_arc.csv',
    'us-west-2'
);

\echo '✅ mi_contractor_delivery_arc loaded'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Record counts:'
SELECT 'mi_contractor_group' as table_name, COUNT(*) as records FROM mi_contractor_group
UNION ALL
SELECT 'mi_contractor', COUNT(*) FROM mi_contractor
UNION ALL
SELECT 'mi_contractor_group_member', COUNT(*) FROM mi_contractor_group_member
UNION ALL
SELECT 'mi_contractor_delivery_arc', COUNT(*) FROM mi_contractor_delivery_arc;

\echo ''
\echo 'Contractors by project and type:'
SELECT project, contractor_type, COUNT(*) as count
FROM mi_contractor
GROUP BY project, contractor_type
ORDER BY project, contractor_type;

\echo ''
\echo 'Contractor groups:'
SELECT g.id, g.short_code, g.label,
       (SELECT COUNT(*) FROM mi_contractor_group_member WHERE mi_contractor_group_id = g.id) as member_count
FROM mi_contractor_group g
ORDER BY g.display_order;

\echo ''
\echo 'SWP M&I contractors (sample):'
SELECT c.id, c.short_code, c.contractor_name, c.contract_amount_taf
FROM mi_contractor c
JOIN mi_contractor_group_member gm ON c.id = gm.mi_contractor_id
JOIN mi_contractor_group g ON gm.mi_contractor_group_id = g.id
WHERE g.short_code = 'swp_mi'
ORDER BY c.contract_amount_taf DESC
LIMIT 10;

\echo ''
\echo 'Delivery arcs by contractor (sample):'
SELECT c.short_code, c.contractor_name, COUNT(da.id) as arc_count,
       STRING_AGG(da.delivery_arc, ', ' ORDER BY da.delivery_arc) as arcs
FROM mi_contractor c
LEFT JOIN mi_contractor_delivery_arc da ON c.id = da.mi_contractor_id
GROUP BY c.short_code, c.contractor_name
ORDER BY c.short_code
LIMIT 10;

\echo ''
\echo '✅ All MI_CONTRACTOR entity data loaded successfully'
