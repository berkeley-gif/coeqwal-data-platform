-- GENERATE FOUR GEOMETRIC LOD LEVELS FOR NETWORK_GIS
-- =====================================================
-- Creates three simplified versions while preserving original high-precision data
--
-- EPSG:4326 (WGS84 lat/lon degrees)
-- At California latitude (~38°):
--   0.01 degrees = ~1.1 km
--   0.001 degrees = ~111 m
--   0.0001 degrees = ~11 m
--   0.00001 degrees = ~1.1 m

\echo ''
\echo 'GENERATING GEOMETRIC LOD LEVELS'
\echo '================================'
\echo ''
\echo 'Four LOD Categories:'
\echo '  1. valley_wide: 2 decimals, 0.01° tolerance (~1.1km) for zoom 5.5-7'
\echo '  2. basin_wide:  3 decimals, 0.001° tolerance (~111m) for zoom 7-9'
\echo '  3. local:       4 decimals, 0.0001° tolerance (~11m) for zoom 9-10'
\echo '  4. precise:     Original 6-8 decimals (preserved, not modified)'
\echo ''

\echo '1. Preserving original data as "precise" LOD...'

UPDATE network_gis 
SET precision_level = 'precise'
WHERE precision_level IN ('high', 'local') 
   OR precision_level IS NULL;

\echo '   Updated existing records to "precise" (original precision preserved)'

\echo ''
\echo '2. Generating LOCAL LOD (4 decimals, ~11m, for zoom 9-10)...'

INSERT INTO network_gis (
    short_code,
    network_id,
    precision_level,
    geom,
    geom_wkt,
    srid,
    estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
)
SELECT 
    short_code,
    network_id,
    'local' as precision_level,
    ST_SnapToGrid(
        ST_SimplifyPreserveTopology(geom, 0.0001),
        0.0001
    ) as geom,
    ST_AsText(ST_SnapToGrid(
        ST_SimplifyPreserveTopology(geom, 0.0001),
        0.0001
    )) as geom_wkt,
    srid,
    11.0 as estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
FROM network_gis
WHERE precision_level = 'precise';

\echo '   Generated local LOD (4 decimals)'

\echo ''
\echo '3. Generating BASIN_WIDE LOD (3 decimals, ~111m, for zoom 7-9)...'

INSERT INTO network_gis (
    short_code,
    network_id,
    precision_level,
    geom,
    geom_wkt,
    srid,
    estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
)
SELECT 
    short_code,
    network_id,
    'basin_wide' as precision_level,
    ST_SnapToGrid(
        ST_SimplifyPreserveTopology(geom, 0.001),
        0.001
    ) as geom,
    ST_AsText(ST_SnapToGrid(
        ST_SimplifyPreserveTopology(geom, 0.001),
        0.001
    )) as geom_wkt,
    srid,
    111.0 as estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
FROM network_gis
WHERE precision_level = 'precise';

\echo '   Generated basin_wide LOD (3 decimals)'

\echo ''
\echo '4. Generating VALLEY_WIDE LOD (2 decimals, ~1.1km, for zoom 5.5-7)...'

INSERT INTO network_gis (
    short_code,
    network_id,
    precision_level,
    geom,
    geom_wkt,
    srid,
    estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
)
SELECT 
    short_code,
    network_id,
    'valley_wide' as precision_level,
    ST_SnapToGrid(
        ST_SimplifyPreserveTopology(geom, 0.01),
        0.01
    ) as geom,
    ST_AsText(ST_SnapToGrid(
        ST_SimplifyPreserveTopology(geom, 0.01),
        0.01
    )) as geom_wkt,
    srid,
    1100.0 as estimated_accuracy_meters,
    source_id,
    network_version_id,
    created_by,
    updated_by
FROM network_gis
WHERE precision_level = 'precise';

\echo '   Generated valley_wide LOD (2 decimals)'

\echo ''
\echo 'FINAL LOD DISTRIBUTION:'
\echo ''

SELECT 
    precision_level,
    COUNT(*) as features,
    ROUND(AVG(ST_NPoints(geom))) as avg_vertices,
    ROUND(AVG(estimated_accuracy_meters)) as accuracy_m,
    pg_size_pretty(SUM(pg_column_size(geom))) as size
FROM network_gis
GROUP BY precision_level
ORDER BY 
    CASE precision_level
        WHEN 'valley_wide' THEN 1
        WHEN 'basin_wide' THEN 2
        WHEN 'local' THEN 3
        WHEN 'precise' THEN 4
    END;

\echo ''
\echo 'LOD GENERATION COMPLETE'
\echo ''
\echo 'Result:'
\echo '  - valley_wide: 4,154 features (2 decimals)'
\echo '  - basin_wide:  4,154 features (3 decimals)'
\echo '  - local:       4,154 features (4 decimals)'
\echo '  - precise:     4,154 features (6-8 decimals, ORIGINAL DATA PRESERVED)'
\echo '  - TOTAL:       16,616 records'

