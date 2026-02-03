-- LOAD DU_AGRICULTURE_ENTITY DATA FROM S3
-- Loads agricultural demand unit data from seed CSV
-- Source: s3://coeqwal-seeds-dev/04_calsim_data/du_agriculture_entity.csv
--
-- Prerequisites:
--   1. Run 01_create_ag_entity_tables.sql first
--   2. Ensure aws_s3 extension is enabled
--   3. Upload du_agriculture_entity.csv to S3 bucket
--
-- Run with: psql -f 01b_load_du_agriculture_entity_from_s3.sql

\echo ''
\echo '==========================================='
\echo 'LOADING DU_AGRICULTURE_ENTITY DATA FROM S3'
\echo '==========================================='

-- ============================================
-- CLEAR EXISTING DATA
-- ============================================
\echo ''
\echo 'Clearing existing data for clean load...'

TRUNCATE TABLE du_agriculture_entity CASCADE;

\echo 'Existing data cleared'

-- ============================================
-- LOAD FROM S3
-- ============================================
\echo ''
\echo 'Loading du_agriculture_entity from S3...'

-- CSV columns (25 total):
-- DU_ID, WBA_ID, hydrologic_region, Dups, Class, CS3_Type, total_acres, polygon_count,
-- source, model_source, agency, provider, gw, sw, point_of_diversion, diversion_arc,
-- river_reach, river_mile_start, river_mile_end, bank, area_acres, annual_diversion_taf,
-- demand_unit, table_id, has_gis_data

SELECT aws_s3.table_import_from_s3(
    'du_agriculture_entity',
    'du_id, wba_id, hydrologic_region, dups, du_class, cs3_type, total_acres, polygon_count, source, model_source, agency, provider, gw, sw, point_of_diversion, diversion_arc, river_reach, river_mile_start, river_mile_end, bank, area_acres, annual_diversion_taf, demand_unit, table_id, has_gis_data',
    '(format csv, header true, force_null (wba_id, dups, du_class, cs3_type, total_acres, polygon_count, source, model_source, agency, provider, point_of_diversion, diversion_arc, river_reach, river_mile_start, river_mile_end, bank, area_acres, annual_diversion_taf, demand_unit, table_id))',
    'coeqwal-seeds-dev',
    '04_calsim_data/du_agriculture_entity.csv',
    'us-west-2'
);

\echo 'du_agriculture_entity loaded from S3'

-- ============================================
-- SET AUDIT FIELDS
-- ============================================
\echo ''
\echo 'Setting audit fields...'

UPDATE du_agriculture_entity SET
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
SELECT COUNT(*) as total_records FROM du_agriculture_entity;

\echo ''
\echo 'Records by hydrologic region:'
SELECT hydrologic_region, COUNT(*) as count
FROM du_agriculture_entity
GROUP BY hydrologic_region
ORDER BY hydrologic_region;

\echo ''
\echo 'Records by CS3 type:'
SELECT cs3_type, COUNT(*) as count
FROM du_agriculture_entity
GROUP BY cs3_type
ORDER BY cs3_type;

\echo ''
\echo 'Records by provider:'
SELECT provider, COUNT(*) as count
FROM du_agriculture_entity
GROUP BY provider
ORDER BY provider;

\echo ''
\echo 'Sample records:'
SELECT du_id, wba_id, hydrologic_region, cs3_type, agency, provider, diversion_arc
FROM du_agriculture_entity
ORDER BY du_id
LIMIT 10;

\echo ''
\echo 'Records with river reach data:'
SELECT du_id, river_reach, river_mile_start, river_mile_end, bank
FROM du_agriculture_entity
WHERE river_reach IS NOT NULL AND river_reach != ''
ORDER BY du_id
LIMIT 5;

\echo ''
\echo 'du_agriculture_entity data loaded successfully'
