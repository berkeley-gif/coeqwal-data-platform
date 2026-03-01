-- =============================================================================
-- inspect_layer01.sql
-- Prints all Layer 01 lookup table contents to the console, including
-- all columns (data + audit fields).
-- =============================================================================
-- Run with: psql $DATABASE_URL -f database/scripts/sql/01_lookup/inspect_layer01.sql
-- =============================================================================

\echo '============================================================================'
\echo 'LAYER 01 LOOKUP TABLES — FULL CONTENTS (all columns)'
\echo '============================================================================'

\echo ''
\echo '1. hydrologic_region'
\echo '--------------------'
SELECT id, short_code, label, is_active,
       created_at, created_by, updated_at, updated_by
FROM hydrologic_region ORDER BY id;

\echo ''
\echo '2. source'
\echo '---------'
SELECT id, source, description, is_active,
       created_at, created_by, updated_at, updated_by
FROM source ORDER BY id;

\echo ''
\echo '3. model_source'
\echo '---------------'
SELECT id, short_code, name, version_family_id, description,
       created_at, created_by, updated_at, updated_by
FROM model_source ORDER BY id;

\echo ''
\echo '4. unit'
\echo '-------'
SELECT id, short_code, full_name, canonical_group, is_active,
       created_at, created_by, updated_at, updated_by
FROM unit ORDER BY id;

\echo ''
\echo '5. spatial_scale'
\echo '----------------'
SELECT id, short_code, label, is_active,
       created_at, created_by, updated_at, updated_by
FROM spatial_scale ORDER BY id;

\echo ''
\echo '6. temporal_scale'
\echo '-----------------'
SELECT id, short_code, label, is_active,
       created_at, created_by, updated_at, updated_by
FROM temporal_scale ORDER BY id;

\echo ''
\echo '7. statistic_type'
\echo '-----------------'
SELECT id, short_code, label, is_percentile,
       created_at, created_by, updated_at, updated_by
FROM statistic_type ORDER BY is_percentile, id;

\echo ''
\echo '8. geometry_type'
\echo '----------------'
SELECT id, short_code, label, is_active,
       created_at, created_by, updated_at, updated_by
FROM geometry_type ORDER BY id;

\echo ''
\echo '9. variable_type'
\echo '-----------------'
SELECT id, short_code, label, is_active,
       created_at, created_by, updated_at, updated_by
FROM variable_type ORDER BY id;

\echo ''
\echo '10. calsim_model_variable_type'
\echo '------------------------------'
SELECT id, short_code, label, is_active,
       created_at, created_by, updated_at, updated_by
FROM calsim_model_variable_type ORDER BY id;

\echo ''
\echo '11. derived_variable_type'
\echo '-------------------------'
SELECT id, short_code, label, is_active,
       created_at, created_by, updated_at, updated_by
FROM derived_variable_type ORDER BY id;

\echo ''
\echo '12. network_type'
\echo '----------------'
SELECT id, short_code, label, description, network_entity_type_id, is_active,
       created_at, created_by, updated_at, updated_by
FROM network_type ORDER BY id;

\echo ''
\echo '13. network_subtype'
\echo '-------------------'
SELECT id, short_code, label, type_id, is_active,
       created_at, created_by, updated_at, updated_by
FROM network_subtype ORDER BY type_id, id;

\echo ''
\echo '14. wba (Water Budget Areas) — first 10 rows'
\echo '--------------------------------------------'
SELECT id, wba_id, wba_name, hydrologic_region, hydrologic_region_id,
       created_at, created_by, updated_at, updated_by
FROM wba ORDER BY id LIMIT 10;
SELECT COUNT(*) AS total_wba_rows FROM wba;

\echo ''
\echo '15. watershed'
\echo '-------------'
SELECT id, short_code, name, hydrologic_region_short_code, is_active,
       created_at, created_by, updated_at, updated_by
FROM watershed ORDER BY id;

\echo ''
\echo '============================================================================'
\echo 'ROW COUNT SUMMARY'
\echo '============================================================================'
SELECT table_name, row_count FROM (
    SELECT 'hydrologic_region'          AS table_name, COUNT(*) AS row_count FROM hydrologic_region
    UNION ALL SELECT 'source',                         COUNT(*) FROM source
    UNION ALL SELECT 'model_source',                   COUNT(*) FROM model_source
    UNION ALL SELECT 'unit',                           COUNT(*) FROM unit
    UNION ALL SELECT 'spatial_scale',                  COUNT(*) FROM spatial_scale
    UNION ALL SELECT 'temporal_scale',                 COUNT(*) FROM temporal_scale
    UNION ALL SELECT 'statistic_type',                 COUNT(*) FROM statistic_type
    UNION ALL SELECT 'geometry_type',                  COUNT(*) FROM geometry_type
    UNION ALL SELECT 'variable_type',                  COUNT(*) FROM variable_type
    UNION ALL SELECT 'calsim_model_variable_type',     COUNT(*) FROM calsim_model_variable_type
    UNION ALL SELECT 'derived_variable_type',          COUNT(*) FROM derived_variable_type
    UNION ALL SELECT 'network_type',                   COUNT(*) FROM network_type
    UNION ALL SELECT 'network_subtype',                COUNT(*) FROM network_subtype
    UNION ALL SELECT 'wba',                            COUNT(*) FROM wba
    UNION ALL SELECT 'watershed',                      COUNT(*) FROM watershed
) t
ORDER BY table_name;

\echo ''
\echo '============================================================================'
\echo 'AUDIT FIELD PROVENANCE CHECK'
\echo '============================================================================'
SELECT table_name,
       min_created_by,
       max_updated_by,
       CASE WHEN min_created_by = 2 AND max_updated_by = 2 THEN 'OK'
            ELSE 'CHECK' END AS provenance_status
FROM (
    SELECT 'hydrologic_region'          AS table_name, MIN(created_by) AS min_created_by, MAX(updated_by) AS max_updated_by FROM hydrologic_region
    UNION ALL SELECT 'source',                         MIN(created_by), MAX(updated_by) FROM source
    UNION ALL SELECT 'model_source',                   MIN(created_by), MAX(updated_by) FROM model_source
    UNION ALL SELECT 'unit',                           MIN(created_by), MAX(updated_by) FROM unit
    UNION ALL SELECT 'spatial_scale',                  MIN(created_by), MAX(updated_by) FROM spatial_scale
    UNION ALL SELECT 'temporal_scale',                 MIN(created_by), MAX(updated_by) FROM temporal_scale
    UNION ALL SELECT 'statistic_type',                 MIN(created_by), MAX(updated_by) FROM statistic_type
    UNION ALL SELECT 'geometry_type',                  MIN(created_by), MAX(updated_by) FROM geometry_type
    UNION ALL SELECT 'variable_type',                  MIN(created_by), MAX(updated_by) FROM variable_type
    UNION ALL SELECT 'calsim_model_variable_type',     MIN(created_by), MAX(updated_by) FROM calsim_model_variable_type
    UNION ALL SELECT 'derived_variable_type',          MIN(created_by), MAX(updated_by) FROM derived_variable_type
    UNION ALL SELECT 'network_type',                   MIN(created_by), MAX(updated_by) FROM network_type
    UNION ALL SELECT 'network_subtype',                MIN(created_by), MAX(updated_by) FROM network_subtype
    UNION ALL SELECT 'wba',                            MIN(created_by), MAX(updated_by) FROM wba
    UNION ALL SELECT 'watershed',                      MIN(created_by), MAX(updated_by) FROM watershed
) t
ORDER BY table_name;
