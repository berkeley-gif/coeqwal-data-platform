-- REBUILD DOMAIN_FAMILY_MAP FOR EXISTING TABLES
-- Map current tables to appropriate version families

\echo ''
\echo 'REBUILDING DOMAIN_FAMILY_MAP FOR EXISTING TABLES'
\echo '==============================================='
\echo ''
\echo 'The cleanup revealed that ALL 35 domain_family_map entries'
\echo 'were for non-existent tables. We need to rebuild mappings'
\echo 'for the tables that actually exist.'
\echo ''

\echo 'ADDING MAPPINGS FOR EXISTING TABLES'
\echo '=================================='


INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note) VALUES
('public', 'hydrologic_region', 11, 'Geographic region lookup versioning'),
('public', 'source', 11, 'Data source lookup versioning'),
('public', 'model_source', 11, 'Model source lookup versioning'),
('public', 'geometry_type', 11, 'Geometry type lookup versioning'),
('public', 'spatial_scale', 11, 'Spatial scale lookup versioning'),
('public', 'temporal_scale', 11, 'Temporal scale lookup versioning'),
('public', 'statistic_type', 11, 'Statistic type lookup versioning'),
('public', 'unit', 11, 'Unit lookup versioning');

\echo '  ✅ Added 01_lookup table mappings (metadata family)'

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note) VALUES
('public', 'network_entity_type', 12, 'Network entity type foundation versioning');

\echo '  ✅ Added foundation table mappings (network family)'

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note) VALUES
('public', 'network_gis', 9, 'Network GIS data versioning');

\echo '  ✅ Added legacy table mappings (geospatial family)'

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note) VALUES
('public', 'variable_type', 6, 'Variable type system versioning');

\echo '  ✅ Added system table mappings (variable family)'

\echo ''
\echo 'VERIFICATION'
\echo '==========='

\echo ''
\echo 'Rebuilt domain_family_map entries:'
SELECT 
    dfm.table_name,
    vf.short_code as version_family,
    dfm.note,
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_tables WHERE tablename = dfm.table_name AND schemaname = 'public')
        THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END as table_status
FROM domain_family_map dfm
JOIN version_family vf ON dfm.version_family_id = vf.id
ORDER BY vf.short_code, dfm.table_name;

\echo ''
\echo 'Summary:'
SELECT 
    COUNT(*) as total_mappings,
    COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM pg_tables WHERE tablename = dfm.table_name AND schemaname = 'public')) as valid_mappings
FROM domain_family_map dfm;

\echo ''
\echo '✅ DOMAIN_FAMILY_MAP REBUILT!'
\echo ''
\echo 'All existing tables now properly mapped to version families'
\echo 'Versioning system integrity restored'
\echo 'Ready for new table development'
\echo ''
