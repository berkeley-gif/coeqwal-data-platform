-- =============================================================================
-- 04_layer01_provenance_and_domain_map.sql
-- Corrects provenance on all Layer 01 lookup tables (created_by/updated_by to 2)
-- and registers missing Layer 01 tables in domain_family_map.
-- =============================================================================

\echo '============================================================================'
\echo 'MIGRATION 04: LAYER 01 PROVENANCE + DOMAIN FAMILY MAP'
\echo '============================================================================'


-- =============================================================================
-- PART 1: Correct created_by / updated_by on all Layer 01 tables
-- =============================================================================
\echo ''
\echo 'PART 1: Setting created_by=2 / updated_by=2 on all Layer 01 rows...'

UPDATE hydrologic_region        SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE source                   SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE model_source             SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE unit                     SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE spatial_scale            SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE temporal_scale           SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE statistic_type           SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE geometry_type            SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE calsim_variable_type     SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE variable_type            SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE network_type             SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE network_subtype          SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE wba                      SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE watershed                SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE calsim_model_variable_type SET created_by = 2, updated_by = 2 WHERE created_by = 1;
UPDATE derived_variable_type    SET created_by = 2, updated_by = 2 WHERE created_by = 1;

\echo '  Verifying provenance...'
SELECT table_name,
       created_by,
       (SELECT display_name FROM developer WHERE id = created_by) AS owner
FROM (
    SELECT 'hydrologic_region'          AS table_name, MIN(created_by) AS created_by FROM hydrologic_region
    UNION ALL SELECT 'source',                         MIN(created_by) FROM source
    UNION ALL SELECT 'model_source',                   MIN(created_by) FROM model_source
    UNION ALL SELECT 'unit',                           MIN(created_by) FROM unit
    UNION ALL SELECT 'spatial_scale',                  MIN(created_by) FROM spatial_scale
    UNION ALL SELECT 'temporal_scale',                 MIN(created_by) FROM temporal_scale
    UNION ALL SELECT 'statistic_type',                 MIN(created_by) FROM statistic_type
    UNION ALL SELECT 'geometry_type',                  MIN(created_by) FROM geometry_type
    UNION ALL SELECT 'calsim_variable_type',           MIN(created_by) FROM calsim_variable_type
    UNION ALL SELECT 'variable_type',                  MIN(created_by) FROM variable_type
    UNION ALL SELECT 'network_type',                   MIN(created_by) FROM network_type
    UNION ALL SELECT 'network_subtype',                MIN(created_by) FROM network_subtype
    UNION ALL SELECT 'wba',                            MIN(created_by) FROM wba
    UNION ALL SELECT 'watershed',                      MIN(created_by) FROM watershed
    UNION ALL SELECT 'calsim_model_variable_type',     MIN(created_by) FROM calsim_model_variable_type
    UNION ALL SELECT 'derived_variable_type',          MIN(created_by) FROM derived_variable_type
) t
ORDER BY table_name;


-- =============================================================================
-- PART 2: Add missing Layer 01 entries to domain_family_map
-- =============================================================================
\echo ''
\echo 'PART 2: Registering missing Layer 01 tables in domain_family_map...'

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, created_by, updated_by)
SELECT
    'public'        AS schema_name,
    e.table_name,
    vf.id           AS version_family_id,
    e.note,
    2               AS created_by,
    2               AS updated_by
FROM (VALUES
    ('watershed',                 'geospatial', 'Watershed geographic lookup'),
    ('wba',                       'geospatial', 'Water Budget Area polygons'),
    ('network_type',              'network',    'Network element type classification'),
    ('network_subtype',           'network',    'Network element subtype classification'),
    ('model_source',              'metadata',   'Simulation model source registry'),
    ('spatial_scale',             'metadata',   'Geographic scale classification'),
    ('temporal_scale',            'metadata',   'Time scale classification'),
    ('calsim_variable_type',      'variable',   'CalSim variable type (output/state/decision)'),
    ('variable_type',             'variable',   'Water use variable type classification'),
    ('calsim_model_variable_type','variable',   'CalSim model variable behavior classification'),
    ('derived_variable_type',     'variable',   'Derived variable category classification')
) AS e(table_name, family_short_code, note)
JOIN version_family vf ON vf.short_code = e.family_short_code
ON CONFLICT (schema_name, table_name) DO NOTHING;

\echo '  Verifying domain_family_map for all Layer 01 tables...'
SELECT
    dfm.table_name,
    vf.short_code   AS version_family,
    dfm.note
FROM domain_family_map dfm
JOIN version_family vf ON vf.id = dfm.version_family_id
WHERE dfm.table_name IN (
    'hydrologic_region','source','model_source','unit',
    'spatial_scale','temporal_scale','statistic_type','geometry_type',
    'calsim_variable_type','variable_type','network_type','network_subtype',
    'wba','watershed','calsim_model_variable_type','derived_variable_type'
)
ORDER BY vf.short_code, dfm.table_name;


-- =============================================================================
-- SUMMARY
-- =============================================================================
\echo ''
\echo 'Total domain_family_map entries:'
SELECT COUNT(*) FROM domain_family_map;

\echo ''
\echo '============================================================================'
\echo 'MIGRATION 04 COMPLETE'
\echo '============================================================================'
