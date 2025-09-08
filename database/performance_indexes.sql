-- ============================================================================
-- CRITICAL PERFORMANCE INDEXES FOR NETWORK API
-- ============================================================================
-- These indexes will reduce 17+ second queries to sub-second responses

-- 1. SPATIAL INDEXES (MOST CRITICAL)
-- PostGIS spatial index for geometry operations
CREATE INDEX IF NOT EXISTS idx_network_gis_geom_spatial 
ON network_gis USING GIST (geom);

-- Spatial index with geometry type for faster filtering
CREATE INDEX IF NOT EXISTS idx_network_gis_geom_type_spatial 
ON network_gis USING GIST (geom) WHERE geometry_type IN ('point', 'linestring');

-- 2. JOIN OPTIMIZATION INDEXES
-- Critical for network_topology <-> network_gis joins
CREATE INDEX IF NOT EXISTS idx_network_topology_short_code 
ON network_topology (short_code);

CREATE INDEX IF NOT EXISTS idx_network_gis_short_code 
ON network_gis (short_code);

-- Composite index for common filtering patterns
CREATE INDEX IF NOT EXISTS idx_network_topology_type_status 
ON network_topology (schematic_type, connectivity_status);

-- 3. ELEMENT TYPE FILTERING INDEXES
-- For fast element type filtering
CREATE INDEX IF NOT EXISTS idx_network_topology_element_type 
ON network_topology (type) WHERE type IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_network_topology_element_subtype 
ON network_topology (type, subtype) WHERE type IS NOT NULL;

-- 4. RIVER SYSTEM INDEXES
-- For river-based queries and sorting
CREATE INDEX IF NOT EXISTS idx_network_topology_river_mile 
ON network_topology (river_name, river_mile) WHERE river_name IS NOT NULL;

-- 5. CONNECTIVITY INDEXES
-- For network traversal queries
CREATE INDEX IF NOT EXISTS idx_network_topology_from_node 
ON network_topology (from_node) WHERE from_node IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_network_topology_to_node 
ON network_topology (to_node) WHERE to_node IS NOT NULL;

-- Composite connectivity index
CREATE INDEX IF NOT EXISTS idx_network_topology_connectivity 
ON network_topology (from_node, to_node, schematic_type) 
WHERE from_node IS NOT NULL AND to_node IS NOT NULL;

-- 6. GEOMETRY TYPE OPTIMIZATION
-- For faster geometry type filtering in network_gis
CREATE INDEX IF NOT EXISTS idx_network_gis_geometry_type 
ON network_gis (geometry_type);

-- 7. COMPOSITE SPATIAL + TYPE INDEX
-- Ultimate performance index for bbox + type queries
CREATE INDEX IF NOT EXISTS idx_network_gis_spatial_composite 
ON network_gis USING GIST (geom, geometry_type);

-- ============================================================================
-- ANALYZE TABLES FOR QUERY PLANNER
-- ============================================================================

ANALYZE network_topology;
ANALYZE network_gis;

-- ============================================================================
-- PERFORMANCE VALIDATION QUERIES
-- ============================================================================

-- Test spatial query performance (should be <1 second)
EXPLAIN ANALYZE
SELECT COUNT(*) 
FROM network_topology nt
INNER JOIN network_gis ng ON nt.short_code = ng.short_code
WHERE nt.schematic_type = 'node'
AND nt.connectivity_status = 'connected'
AND ng.geom && ST_MakeEnvelope(-122.3, 40.3, -122.1, 40.5, 4326);

-- Test traversal query performance
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM network_topology 
WHERE from_node = 'SAC273' OR to_node = 'SAC273';

-- Show index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE schemaname = 'public' 
AND (tablename = 'network_topology' OR tablename = 'network_gis')
ORDER BY idx_tup_read DESC;

-- ============================================================================
-- EXPECTED PERFORMANCE IMPROVEMENTS
-- ============================================================================
-- Before indexes: 17+ seconds
-- After indexes:  <1 second (95%+ improvement)
-- Spatial queries: Sub-second with proper GIST indexes
-- Network traversal: <500ms with connectivity indexes
