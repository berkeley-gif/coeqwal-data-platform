-- LOAD DU_URBAN_ENTITY DATA FROM S3
-- Loads urban demand unit data from seed CSV
-- Source: s3://coeqwal-seeds-dev/04_calsim_data/du_urban_entity.csv
--
-- Prerequisites:
--   1. Run 01_create_du_urban_entity.sql first
--   2. Ensure aws_s3 extension is enabled
--   3. Upload du_urban_entity.csv to S3 bucket
--
-- Run with: psql -f 01b_load_du_urban_entity_from_s3.sql

\echo ''
\echo '========================================='
\echo 'LOADING DU_URBAN_ENTITY DATA FROM S3'
\echo '========================================='

-- ============================================
-- CLEAR EXISTING DATA
-- ============================================
\echo ''
\echo 'Clearing existing data for clean load...'

TRUNCATE TABLE du_urban_entity CASCADE;

\echo '✅ Existing data cleared'

-- ============================================
-- LOAD FROM S3
-- ============================================
\echo ''
\echo 'Loading du_urban_entity from S3...'

-- Note: CSV columns map to table columns as follows:
-- CSV: DU_ID, WBA_ID, hydrologic_region, Dups, Class, CS3_Type, total_acres, polygon_count,
--      community_agency, gw, sw, point_of_diversion, source, model_source, has_gis_data, primary_contractor_short_code
-- Table: du_id, wba_id, hydrologic_region, dups, du_class, cs3_type, total_acres, polygon_count,
--        community_agency, gw, sw, point_of_diversion, source, model_source, has_gis_data, primary_contractor_short_code

SELECT aws_s3.table_import_from_s3(
    'du_urban_entity',
    'du_id, wba_id, hydrologic_region, dups, du_class, cs3_type, total_acres, polygon_count, community_agency, gw, sw, point_of_diversion, source, model_source, has_gis_data, primary_contractor_short_code',
    '(format csv, header true, force_null (dups, wba_id, cs3_type, total_acres, polygon_count, primary_contractor_short_code))',
    'coeqwal-seeds-dev',
    '04_calsim_data/du_urban_entity.csv',
    'us-west-2'
);

\echo '✅ du_urban_entity loaded from S3'

-- ============================================
-- SET AUDIT FIELDS
-- ============================================
\echo ''
\echo 'Setting audit fields...'

UPDATE du_urban_entity SET
    is_active = TRUE,
    created_at = NOW(),
    created_by = 1,
    updated_at = NOW(),
    updated_by = 1
WHERE created_by IS NULL;

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Record count:'
SELECT COUNT(*) as total_records FROM du_urban_entity;

\echo ''
\echo 'Records by hydrologic region:'
SELECT hydrologic_region, COUNT(*) as count
FROM du_urban_entity
GROUP BY hydrologic_region
ORDER BY hydrologic_region;

\echo ''
\echo 'Records by CS3 type:'
SELECT cs3_type, COUNT(*) as count
FROM du_urban_entity
GROUP BY cs3_type
ORDER BY cs3_type;

\echo ''
\echo 'Sample records:'
SELECT du_id, wba_id, hydrologic_region, cs3_type, community_agency
FROM du_urban_entity
ORDER BY du_id
LIMIT 10;

\echo ''
\echo '✅ du_urban_entity data loaded successfully'
