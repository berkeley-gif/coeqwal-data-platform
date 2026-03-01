-- =============================================================================
-- inspect_layer01.sql
-- Prints all Layer 01 lookup table contents to the console.
-- =============================================================================
-- Run with: psql $DATABASE_URL -f database/scripts/sql/01_lookup/inspect_layer01.sql
-- =============================================================================

\echo '============================================================================'
\echo 'LAYER 01 LOOKUP TABLES — FULL CONTENTS'
\echo '============================================================================'

\echo ''
\echo '1. hydrologic_region'
\echo '--------------------'
SELECT id, short_code, label, is_active FROM hydrologic_region ORDER BY id;

\echo ''
\echo '2. source'
\echo '---------'
SELECT id, source, is_active FROM source ORDER BY id;

\echo ''
\echo '3. model_source'
\echo '---------------'
SELECT id, short_code, name, version_family_id, description FROM model_source ORDER BY id;

\echo ''
\echo '4. unit'
\echo '-------'
SELECT id, short_code, full_name, canonical_group, is_active FROM unit ORDER BY id;

\echo ''
\echo '5. spatial_scale'
\echo '----------------'
SELECT id, short_code, label, is_active FROM spatial_scale ORDER BY id;

\echo ''
\echo '6. temporal_scale'
\echo '-----------------'
SELECT id, short_code, label, is_active FROM temporal_scale ORDER BY id;

\echo ''
\echo '7. statistic_type'
\echo '-----------------'
SELECT id, short_code, label, is_percentile, is_active FROM statistic_type
    WHERE is_active IS DISTINCT FROM false ORDER BY is_percentile, id;

\echo ''
\echo '8. geometry_type'
\echo '----------------'
SELECT id, short_code, label, is_active FROM geometry_type ORDER BY id;

\echo ''
\echo '9. calsim_variable_type'
\echo '-----------------------'
SELECT id, short_code, label, is_active FROM calsim_variable_type ORDER BY id;

\echo ''
\echo '10. variable_type'
\echo '-----------------'
SELECT id, short_code, label, is_active FROM variable_type ORDER BY id;

\echo ''
\echo '11. calsim_model_variable_type'
\echo '------------------------------'
SELECT id, short_code, label, is_active FROM calsim_model_variable_type ORDER BY id;

\echo ''
\echo '12. derived_variable_type'
\echo '-------------------------'
SELECT id, short_code, label, is_active FROM derived_variable_type ORDER BY id;

\echo ''
\echo '13. network_type'
\echo '----------------'
SELECT id, short_code, label, entity_key, is_active FROM network_type ORDER BY id;

\echo ''
\echo '14. network_subtype'
\echo '-------------------'
SELECT id, short_code, label, type_id, is_active FROM network_subtype ORDER BY type_id, id;

\echo ''
\echo '15. wba (Water Budget Areas)'
\echo '----------------------------'
SELECT id, wba_id, wba_name, hydrologic_region, is_active
    FROM wba ORDER BY id
    LIMIT 10;
SELECT COUNT(*) AS total_wba_rows FROM wba;

\echo ''
\echo '16. watershed  [PLANNED — table exists, not yet fully linked to network]'
\echo '-------------------------------------------------------------------------'
SELECT id, short_code, name, hydrologic_region_short_code, is_active FROM watershed ORDER BY id;

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
    UNION ALL SELECT 'calsim_variable_type',           COUNT(*) FROM calsim_variable_type
    UNION ALL SELECT 'variable_type',                  COUNT(*) FROM variable_type
    UNION ALL SELECT 'calsim_model_variable_type',     COUNT(*) FROM calsim_model_variable_type
    UNION ALL SELECT 'derived_variable_type',          COUNT(*) FROM derived_variable_type
    UNION ALL SELECT 'network_type',                   COUNT(*) FROM network_type
    UNION ALL SELECT 'network_subtype',                COUNT(*) FROM network_subtype
    UNION ALL SELECT 'wba',                            COUNT(*) FROM wba
    UNION ALL SELECT 'watershed',                      COUNT(*) FROM watershed
) t
ORDER BY table_name;
