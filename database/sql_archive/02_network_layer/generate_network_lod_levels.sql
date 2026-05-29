-- GENERATE PRE-SIMPLIFIED LOD LEVELS FOR NETWORK_GIS
-- =====================================================
-- Creates regional and basin-scale simplified geometries from high-precision data
-- For optimal map performance and bandwidth savings at different zoom levels

\echo ''
\echo 'GENERATING LOD LEVELS FOR NETWORK_GIS'
\echo '==========================================='

\echo ''
\echo '1. Updating existing records to precision_level = "precise"'

UPDATE network_gis 
SET precision_level = 'precise'
WHERE precision_level = 'high' OR precision_level IS NULL;

\echo '✅ Updated existing records to "precise"'

\echo ''
\echo 'Current state:'
SELECT 
    precision_level,
    COUNT(*) as feature_count,
    ROUND(AVG(ST_NPoints(geom))) as avg_vertices
FROM network_gis
GROUP BY precision_level;

\echo ''
\echo '2. Generating REGIONAL LOD (zoom 5.5-7, ~1km simplification)'

INSERT INTO network_gis (
    short_code,
    network_id,
    precision_level,
    geom_wkt,
    srid,
    geom,
    estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
)
SELECT 
    short_code,
    network_id,
    'regional' as precision_level,
    ST_AsText(ST_SimplifyPreserveTopology(geom, 0.01)) as geom_wkt,
    srid,
    ST_SimplifyPreserveTopology(geom, 0.01) as geom,
    1000.0 as estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
FROM network_gis
WHERE precision_level = 'precise';

\echo '✅ Generated regional LOD'

\echo ''
\echo '3. Generating BASIN LOD (zoom 7-9, ~100m simplification)...'

INSERT INTO network_gis (
    short_code,
    network_id,
    precision_level,
    geom_wkt,
    srid,
    geom,
    estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
)
SELECT 
    short_code,
    network_id,
    'basin' as precision_level,
    ST_AsText(ST_SimplifyPreserveTopology(geom, 0.001)) as geom_wkt,
    srid,
    ST_SimplifyPreserveTopology(geom, 0.001) as geom,
    100.0 as estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
FROM network_gis
WHERE precision_level = 'precise';

\echo '✅ Generated basin LOD'

\echo ''
\echo 'Final LOD distribution:'

SELECT 
    precision_level,
    COUNT(*) as feature_count,
    ROUND(AVG(ST_NPoints(geom))) as avg_vertices,
    ROUND(AVG(estimated_accuracy_meters)) as avg_accuracy_m,
    pg_size_pretty(SUM(pg_column_size(geom))) as total_size
FROM network_gis
GROUP BY precision_level
ORDER BY 
    CASE precision_level
        WHEN 'regional' THEN 1
        WHEN 'basin' THEN 2
        WHEN 'precise' THEN 3
    END;

\echo ''
\echo '✅ LOD GENERATION COMPLETE'
\echo ''
\echo 'Expected result:'
\echo '   - regional: 4,154 features (~50% fewer vertices, ~70% bandwidth savings)'
\echo '   - basin:    4,154 features (~30% fewer vertices, ~40% bandwidth savings)'
\echo '   - precise:  4,154 features (original detail)'
\echo '   - TOTAL:    12,462 records in network_gis'

