-- FIND TIER EVALUATION LOCATIONS
-- ================================
-- Queries to help identify which network nodes are used for tier evaluations

\echo ''
\echo '🔍 FINDING TIER EVALUATION LOCATIONS'
\echo '====================================='

-- 1. RESERVOIR STORAGE LOCATIONS
\echo ''
\echo '🏔️  RESERVOIR STORAGE (RES_STOR) - 7 reservoirs:'
\echo '------------------------------------------------'
SELECT 
    n.short_code,
    n.name,
    nt.label as type,
    ng.geom IS NOT NULL as has_geometry
FROM network n
LEFT JOIN network_type nt ON n.type_id = nt.id
LEFT JOIN network_gis ng ON n.id = ng.network_id
WHERE n.type_id = 17  -- Storage type
ORDER BY n.short_code;

-- 2. PUMP STATIONS (for FW_EXP)
\echo ''
\echo '⚡ PUMP STATIONS (FW_EXP) - Looking for Banks & Jones:'
\echo '----------------------------------------------------'
SELECT 
    n.short_code,
    n.name,
    n.description,
    ng.geom IS NOT NULL as has_geometry
FROM network n
LEFT JOIN network_gis ng ON n.id = ng.network_id
WHERE n.type_id = 15  -- Pump Station type
   OR n.name ILIKE '%banks%'
   OR n.name ILIKE '%jones%'
   OR n.name ILIKE '%tracy%'
   OR n.short_code ILIKE '%banks%'
   OR n.short_code ILIKE '%jones%'
ORDER BY n.short_code;

-- 3. STREAM GAUGES (potential ENV_FLOWS locations)
\echo ''
\echo '📊 STREAM GAUGES (ENV_FLOWS candidates) - Top 20 by river:'
\echo '----------------------------------------------------------'
SELECT 
    n.short_code,
    n.name,
    nn.riv_mi,
    nn.strm_code,
    n.riv_sys,
    ng.geom IS NOT NULL as has_geometry
FROM network n
LEFT JOIN network_node nn ON n.id = nn.network_id
LEFT JOIN network_gis ng ON n.id = ng.network_id
WHERE n.type_id = 18  -- Stream type
  AND nn.riv_mi IS NOT NULL
  AND nn.strm_code IN ('SAC', 'SJR', 'FTR', 'AMR', 'MCD', 'TUO', 'STS')  -- Major rivers
ORDER BY nn.strm_code, nn.riv_mi
LIMIT 20;

-- 4. DELTA REGION NODES
\echo ''
\echo '🌊 DELTA REGION NODES (DELTA_ECO, FW_DELTA_USES candidates):'
\echo '------------------------------------------------------------'
SELECT 
    n.short_code,
    n.name,
    nn.strm_code,
    hr.label as region,
    ng.geom IS NOT NULL as has_geometry
FROM network n
LEFT JOIN network_node nn ON n.id = nn.network_id
LEFT JOIN hydrologic_region hr ON n.hydrologic_region_id = hr.id
LEFT JOIN network_gis ng ON n.id = ng.network_id
WHERE hr.short_code = 'DELTA'
  AND n.entity_type_id = 2  -- Nodes only
ORDER BY n.short_code
LIMIT 20;

-- 5. DEMAND UNITS (potential WBA locations for GW_STOR)
\echo ''
\echo '💧 DEMAND UNITS BY WBA PREFIX (GW_STOR - need WBA boundaries):'
\echo '--------------------------------------------------------------'
SELECT 
    LEFT(n.short_code, 3) as wba_prefix,
    COUNT(*) as demand_units,
    COUNT(ng.id) as with_geometry
FROM network n
LEFT JOIN network_gis ng ON n.id = ng.network_id
WHERE n.type_id = 21  -- Demand unit type
  AND n.short_code ~ '^\d{2}[A-Z]?_'  -- WBA pattern
GROUP BY LEFT(n.short_code, 3)
ORDER BY wba_prefix;

-- 6. SUMMARY OF AVAILABLE SPATIAL DATA
\echo ''
\echo '📊 SUMMARY - Tier Location Data Availability:'
\echo '=============================================

'
SELECT 
    'Reservoirs (RES_STOR)' as tier,
    COUNT(*) as count,
    COUNT(ng.id) as with_gis
FROM network n
LEFT JOIN network_gis ng ON n.id = ng.network_id
WHERE n.type_id = 17

UNION ALL

SELECT 
    'Pump Stations (FW_EXP)' as tier,
    COUNT(*) as count,
    COUNT(ng.id) as with_gis
FROM network n
LEFT JOIN network_gis ng ON n.id = ng.network_id
WHERE n.type_id = 15

UNION ALL

SELECT 
    'Stream Gauges (ENV_FLOWS)' as tier,
    COUNT(*) as count,
    COUNT(ng.id) as with_gis
FROM network n
LEFT JOIN network_gis ng ON n.id = ng.network_id
WHERE n.type_id = 18
  AND EXISTS (SELECT 1 FROM network_node nn WHERE nn.network_id = n.id AND nn.riv_mi IS NOT NULL);

\echo ''
\echo '🎯 NEXT STEPS:'
\echo '1. Review reservoir list - identify which 7 are used for RES_STOR'
\echo '2. Find Banks and Jones pump stations in pump list'
\echo '3. Get ENV_FLOWS evaluation point list from CalSim documentation'
\echo '4. For GW_STOR: Need WBA boundary shapefile or centroid coordinates'
\echo '5. Create tier_location_result.csv with location IDs and tier assignments'



