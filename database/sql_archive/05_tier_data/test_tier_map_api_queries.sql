-- ============================================================================
-- TEST TIER MAP API QUERIES
-- Verify that all tier map API queries return correct data
-- ============================================================================

\echo ''
\echo '========================================================================'
\echo 'TIER MAP API QUERY TESTS'
\echo '========================================================================'
\echo ''

-- ============================================================================
-- TEST 1: Main Tier Map Query (ENV_FLOWS)
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 1: Environmental Flows Tier Map Data (s0011)'
\echo '------------------------------------------------------------------------'

WITH tier_locations AS (
    SELECT 
        tlr.scenario_short_code,
        tlr.tier_short_code,
        tlr.location_type,
        tlr.location_id,
        tlr.location_name,
        tlr.tier_level,
        tlr.tier_value,
        tlr.display_order,
        td.name as tier_name,
        td.tier_type
    FROM tier_location_result tlr
    JOIN tier_definition td ON tlr.tier_short_code = td.short_code
    WHERE tlr.scenario_short_code = 's0011'
    AND tlr.tier_short_code = 'ENV_FLOWS'
    AND tlr.is_active = TRUE
)
SELECT 
    tl.location_id,
    tl.location_name,
    tl.tier_level,
    tl.tier_value,
    tl.location_type,
    CASE 
        WHEN tl.location_type = 'node' THEN
            (SELECT ST_AsGeoJSON(geom)::jsonb 
             FROM network_gis 
             WHERE short_code = tl.location_id 
             AND geometry_type_id = 1)
        ELSE NULL
    END as geometry_check
FROM tier_locations tl
ORDER BY tl.display_order, tl.location_name
LIMIT 5;

\echo ''
\echo 'Expected: 5 rows with location_id, name, tier_level (1-4), and geometry'
\echo ''

-- ============================================================================
-- TEST 2: Reservoir Storage with Polygon Geometries
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 2: Reservoir Storage Tier Map Data (s0011) - POLYGONS'
\echo '------------------------------------------------------------------------'

WITH tier_locations AS (
    SELECT 
        tlr.location_id,
        tlr.location_name,
        tlr.tier_level,
        tlr.tier_value
    FROM tier_location_result tlr
    WHERE tlr.scenario_short_code = 's0011'
    AND tlr.tier_short_code = 'RES_STOR'
    AND tlr.is_active = TRUE
)
SELECT 
    tl.location_id,
    tl.location_name,
    tl.tier_level,
    CASE 
        WHEN tl.location_id IN ('SLUIS_CVP', 'SLUIS_SWP') THEN
            (SELECT ST_GeometryType(geom) FROM reservoirs WHERE short_code = 'SLUIS')
        ELSE
            (SELECT ST_GeometryType(geom) FROM reservoirs WHERE short_code = tl.location_id)
    END as geometry_type,
    CASE 
        WHEN tl.location_id IN ('SLUIS_CVP', 'SLUIS_SWP') THEN
            (SELECT ST_Area(geom::geography) / 1000000 FROM reservoirs WHERE short_code = 'SLUIS')
        ELSE
            (SELECT ST_Area(geom::geography) / 1000000 FROM reservoirs WHERE short_code = tl.location_id)
    END as area_km2
FROM tier_locations tl
ORDER BY tl.location_name;

\echo ''
\echo 'Expected: 8 rows with MULTIPOLYGON geometries'
\echo 'Note: SLUIS_CVP and SLUIS_SWP should have identical geometry'
\echo ''

-- ============================================================================
-- TEST 3: Groundwater Storage with WBA Polygons
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 3: Groundwater Storage Tier Map Data (s0011) - WBA POLYGONS'
\echo '------------------------------------------------------------------------'

WITH tier_locations AS (
    SELECT 
        tlr.location_id,
        tlr.location_name,
        tlr.tier_level,
        COUNT(*) OVER () as total_count
    FROM tier_location_result tlr
    WHERE tlr.scenario_short_code = 's0011'
    AND tlr.tier_short_code = 'GW_STOR'
    AND tlr.is_active = TRUE
)
SELECT 
    tl.total_count,
    COUNT(DISTINCT tl.tier_level) as tier_levels_used,
    COUNT(DISTINCT w.short_code) as wba_geometries_found
FROM tier_locations tl
LEFT JOIN wba w ON tl.location_id = w.short_code;

\echo ''
\echo 'Expected: 42 locations, 4 tier levels, 42 geometries found'
\echo ''

-- ============================================================================
-- TEST 4: San Luis Reservoir Special Case
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 4: San Luis Reservoir Twin Handling'
\echo '------------------------------------------------------------------------'

WITH sluis_data AS (
    SELECT 
        tlr.scenario_short_code,
        tlr.location_id,
        tlr.tier_level,
        ST_AsText(ST_Centroid(r.geom)) as centroid
    FROM tier_location_result tlr
    LEFT JOIN reservoirs r ON r.short_code = 'SLUIS'
    WHERE tlr.tier_short_code = 'RES_STOR'
    AND tlr.location_id IN ('SLUIS_CVP', 'SLUIS_SWP')
    AND tlr.is_active = TRUE
)
SELECT * FROM sluis_data
ORDER BY scenario_short_code, location_id;

\echo ''
\echo 'Expected: 6 rows (3 scenarios × 2 SLUIS entries), all with same centroid'
\echo ''

-- ============================================================================
-- TEST 5: Available Scenarios Query
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 5: Available Scenarios'
\echo '------------------------------------------------------------------------'

SELECT DISTINCT 
    scenario_short_code,
    COUNT(DISTINCT tier_short_code) as tier_count,
    COUNT(*) as location_count
FROM tier_location_result
WHERE is_active = TRUE
GROUP BY scenario_short_code
ORDER BY scenario_short_code;

\echo ''
\echo 'Expected: 3 scenarios, each with 7 tiers and 73 locations'
\echo ''

-- ============================================================================
-- TEST 6: Available Tiers Query
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 6: Available Tiers for Scenario s0011'
\echo '------------------------------------------------------------------------'

SELECT 
    td.short_code,
    td.name,
    td.tier_type,
    td.tier_count,
    COUNT(tlr.id) as location_count
FROM tier_definition td
JOIN tier_location_result tlr ON td.short_code = tlr.tier_short_code
WHERE tlr.scenario_short_code = 's0011'
AND tlr.is_active = TRUE
AND td.is_active = TRUE
GROUP BY td.short_code, td.name, td.tier_type, td.tier_count
ORDER BY td.tier_type DESC, td.short_code;

\echo ''
\echo 'Expected: 7 tiers with location counts'
\echo ''

-- ============================================================================
-- TEST 7: Tier Summary Query
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 7: Tier Summary for Scenario s0011'
\echo '------------------------------------------------------------------------'

SELECT 
    td.short_code,
    td.name,
    td.tier_type,
    td.tier_count,
    COUNT(tlr.id) as location_count,
    COUNT(DISTINCT tlr.tier_level) as tier_levels_used,
    MIN(tlr.tier_level) as min_tier,
    MAX(tlr.tier_level) as max_tier
FROM tier_definition td
JOIN tier_location_result tlr ON td.short_code = tlr.tier_short_code
WHERE tlr.scenario_short_code = 's0011'
AND tlr.is_active = TRUE
AND td.is_active = TRUE
GROUP BY td.short_code, td.name, td.tier_type, td.tier_count
ORDER BY td.tier_type DESC, td.short_code;

\echo ''
\echo 'Expected: 7 tiers with statistics (counts, min/max tier levels)'
\echo ''

-- ============================================================================
-- TEST 8: Geometry Type Distribution
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 8: Geometry Type Distribution by Tier'
\echo '------------------------------------------------------------------------'

WITH tier_geo AS (
    SELECT 
        tlr.tier_short_code,
        tlr.location_type,
        COUNT(*) as count,
        CASE 
            WHEN tlr.location_type = 'node' THEN 'Point'
            WHEN tlr.location_type = 'reservoir' THEN 'Polygon'
            WHEN tlr.location_type = 'wba' THEN 'Polygon'
            WHEN tlr.location_type = 'compliance_station' THEN 'Point'
            ELSE 'Unknown'
        END as expected_geometry_type
    FROM tier_location_result tlr
    WHERE tlr.scenario_short_code = 's0011'
    AND tlr.is_active = TRUE
    GROUP BY tlr.tier_short_code, tlr.location_type
)
SELECT * FROM tier_geo
ORDER BY tier_short_code, location_type;

\echo ''
\echo 'Expected: Breakdown showing Points (nodes, compliance stations) and Polygons (reservoirs, WBAs)'
\echo ''

-- ============================================================================
-- TEST 9: Data Integrity Check
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 9: Data Integrity - Missing Geometries'
\echo '------------------------------------------------------------------------'

WITH tier_geo_check AS (
    SELECT 
        tlr.scenario_short_code,
        tlr.tier_short_code,
        tlr.location_id,
        tlr.location_type,
        CASE 
            WHEN tlr.location_type = 'node' THEN
                EXISTS(SELECT 1 FROM network_gis WHERE short_code = tlr.location_id)
            WHEN tlr.location_type = 'reservoir' AND tlr.location_id IN ('SLUIS_CVP', 'SLUIS_SWP') THEN
                EXISTS(SELECT 1 FROM reservoirs WHERE short_code = 'SLUIS')
            WHEN tlr.location_type = 'reservoir' THEN
                EXISTS(SELECT 1 FROM reservoirs WHERE short_code = tlr.location_id)
            WHEN tlr.location_type = 'wba' THEN
                EXISTS(SELECT 1 FROM wba WHERE short_code = tlr.location_id)
            WHEN tlr.location_type = 'compliance_station' THEN
                EXISTS(SELECT 1 FROM compliance_stations WHERE short_code = tlr.location_id)
            ELSE FALSE
        END as has_geometry
    FROM tier_location_result tlr
    WHERE tlr.is_active = TRUE
)
SELECT 
    COUNT(*) as total_locations,
    COUNT(*) FILTER (WHERE has_geometry) as with_geometry,
    COUNT(*) FILTER (WHERE NOT has_geometry) as missing_geometry
FROM tier_geo_check;

\echo ''
\echo 'Expected: All locations should have geometry (missing_geometry = 0)'
\echo ''

-- ============================================================================
-- TEST 10: Performance Check - Full Query
-- ============================================================================
\echo '------------------------------------------------------------------------'
\echo 'TEST 10: Performance Check - Full Tier Map Query'
\echo '------------------------------------------------------------------------'

\timing on

WITH tier_locations AS (
    SELECT 
        tlr.scenario_short_code,
        tlr.tier_short_code,
        tlr.location_type,
        tlr.location_id,
        tlr.location_name,
        tlr.tier_level,
        tlr.tier_value,
        tlr.display_order,
        td.name as tier_name,
        td.tier_type
    FROM tier_location_result tlr
    JOIN tier_definition td ON tlr.tier_short_code = td.short_code
    WHERE tlr.scenario_short_code = 's0011'
    AND tlr.tier_short_code = 'GW_STOR'
    AND tlr.is_active = TRUE
)
SELECT 
    COUNT(*) as feature_count,
    COUNT(DISTINCT tl.tier_level) as tier_levels,
    AVG(LENGTH(ST_AsGeoJSON(w.geom)::text)) as avg_geojson_size_bytes
FROM tier_locations tl
LEFT JOIN wba w ON tl.location_id = w.short_code;

\timing off

\echo ''
\echo 'Expected: Should complete in < 100ms'
\echo ''

-- ============================================================================
-- SUMMARY
-- ============================================================================
\echo ''
\echo '========================================================================'
\echo 'TEST SUMMARY'
\echo '========================================================================'
\echo 'If all tests pass:'
\echo '  ✓ All tier location data has corresponding geometries'
\echo '  ✓ San Luis reservoir twin handling works correctly'
\echo '  ✓ All geometry types (Point, Polygon) are supported'
\echo '  ✓ API queries perform well (< 100ms)'
\echo '  ✓ Data integrity is maintained across all tables'
\echo ''
\echo 'API is ready for frontend integration!'
\echo '========================================================================'
\echo ''

