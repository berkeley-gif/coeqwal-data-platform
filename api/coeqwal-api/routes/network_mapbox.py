"""
Network API routes optimized for mapbox app
For network traversal on Mapbox maps optimizing use of the network_topology table
"""
import json
import asyncpg
from fastapi import HTTPException, Query
from typing import Optional
import hashlib
import time

# Simple in-memory cache for spatial queries (5 minute TTL)
_spatial_cache = {}
_cache_ttl = 300  # 5 minutes


async def get_network_geojson(
    db_pool: asyncpg.Pool,
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    include_arcs: bool = Query(True, description="Include arc geometries"),
    include_nodes: bool = Query(True, description="Include node geometries"),
    limit: int = Query(5000, description="Maximum features to return")
):
    """
    Get network features as GeoJSON for Mapbox display within bounding box
    """
    try:
        start_time = time.time()
        
        # Parse bounding box
        bbox_coords = [float(x) for x in bbox.split(',')]
        if len(bbox_coords) != 4:
            raise ValueError("Bounding box must have 4 coordinates")
        
        min_lng, min_lat, max_lng, max_lat = bbox_coords
        
        # Create cache key
        cache_key = hashlib.md5(
            f"{bbox}_{include_arcs}_{include_nodes}_{limit}".encode()
        ).hexdigest()
        
        # Check cache
        current_time = time.time()
        if cache_key in _spatial_cache:
            cached_data, cache_time = _spatial_cache[cache_key]
            if current_time - cache_time < _cache_ttl:
                print(f"⚡ Cache hit for bbox query ({current_time - start_time:.3f}s)")
                return cached_data
        
        # Build query conditions
        type_conditions = []
        if include_nodes:
            type_conditions.append("'node'")
        if include_arcs:
            type_conditions.append("'arc'")
        
        if not type_conditions:
            return {"type": "FeatureCollection", "features": []}
        
        type_filter = f"nt.schematic_type IN ({','.join(type_conditions)})"
        
        # ULTRA-FAST query using spatial index and minimal data
        # Include from_node/to_node for arc properties
        query = f"""
        SELECT 
            nt.id,
            nt.short_code,
            nt.schematic_type,
            nt.from_node,
            nt.to_node,
            nt.connectivity_status,
            nt.type,
            nt.subtype,
            COALESCE(nt.river_name, '') as river_name,
            COALESCE(nt.arc_name, '') as arc_name,
            nt.river_mile,
            nt.shape_length,
            ng.geometry_type,
            ST_AsGeoJSON(ng.geom) as geometry
        FROM network_topology nt
        INNER JOIN network_gis ng ON nt.short_code = ng.short_code
        WHERE {type_filter}
        AND nt.connectivity_status = 'connected'
        AND ng.geom && ST_MakeEnvelope($1, $2, $3, $4, 4326)
        ORDER BY nt.id
        LIMIT $5;
        """
        
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                query, 
                min_lng, min_lat, max_lng, max_lat, limit
            )
        
        # Convert to GeoJSON with error handling
        features = []
        for row in rows:
            try:
                geometry = json.loads(row['geometry']) if row['geometry'] else None
                
                # Create feature properties with safe access
                properties = {
                    'id': row['id'],
                    'short_code': row['short_code'],
                    'type': row['schematic_type'],
                    'connectivity_status': row['connectivity_status'],
                    'element_type': row['type'] if row['type'] else '',
                    'subtype': row['subtype'] if row['subtype'] else ''
                }
            except Exception as e:
                print(f"Error processing row {row.get('short_code', 'unknown')}: {e}")
                continue
            
            # Add type-specific properties with safe access
            try:
                if row['schematic_type'] == 'node':
                    properties.update({
                        'river_name': row['river_name'] if row['river_name'] else '',
                        'river_mile': float(row['river_mile']) if row['river_mile'] else None,
                        'display_name': row['river_name'] if row['river_name'] else row['short_code']
                    })
                elif row['schematic_type'] == 'arc':
                    properties.update({
                        'arc_name': row['arc_name'] if row['arc_name'] else '',
                        'shape_length': float(row['shape_length']) if row['shape_length'] else None,
                        'from_node': row['from_node'] if row['from_node'] else '',
                        'to_node': row['to_node'] if row['to_node'] else '',
                        'display_name': row['arc_name'] if row['arc_name'] else f"{row['from_node'] or ''} → {row['to_node'] or ''}"
                    })
                
                feature = {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": properties
                }
                features.append(feature)
                
            except Exception as e:
                print(f"Error processing properties for {row.get('short_code', 'unknown')}: {e}")
                continue
        
        result = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "total_features": len(features),
                "bbox": bbox_coords,
                "includes_arcs": include_arcs,
                "includes_nodes": include_nodes,
                "query_time_ms": round((time.time() - start_time) * 1000, 2)
            }
        }
        
        # Cache the result
        _spatial_cache[cache_key] = (result, current_time)
        
        # Clean old cache entries (simple cleanup)
        if len(_spatial_cache) > 100:
            old_keys = [k for k, (_, t) in _spatial_cache.items() if current_time - t > _cache_ttl]
            for k in old_keys[:50]:  # Remove oldest 50 entries
                _spatial_cache.pop(k, None)
        
        print(f"🚀 Spatial query completed in {time.time() - start_time:.3f}s")
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


async def get_network_nodes_fast(
    db_pool: asyncpg.Pool,
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    limit: int = Query(1000, description="Maximum nodes to return")
):
    """
    ULTRA-FAST nodes-only endpoint for initial map loading
    Optimized for speed with minimal data transfer
    """
    try:
        start_time = time.time()
        
        # Parse bounding box
        bbox_coords = [float(x) for x in bbox.split(',')]
        if len(bbox_coords) != 4:
            raise ValueError("Bounding box must have 4 coordinates")
        
        min_lng, min_lat, max_lng, max_lat = bbox_coords
        
        # Create cache key
        cache_key = f"nodes_fast_{hashlib.md5(f'{bbox}_{limit}'.encode()).hexdigest()}"
        
        # Check cache
        current_time = time.time()
        if cache_key in _spatial_cache:
            cached_data, cache_time = _spatial_cache[cache_key]
            if current_time - cache_time < _cache_ttl:
                print(f"⚡ Fast nodes cache hit ({current_time - start_time:.3f}s)")
                return cached_data
        
        # ULTRA-FAST query - nodes only, minimal fields
        query = """
        SELECT 
            nt.id,
            nt.short_code,
            nt.type,
            nt.subtype,
            nt.river_name,
            nt.river_mile,
            ST_X(ng.geom) as lng,
            ST_Y(ng.geom) as lat
        FROM network_topology nt
        INNER JOIN network_gis ng ON nt.short_code = ng.short_code
        WHERE nt.schematic_type = 'node'
        AND nt.connectivity_status = 'connected'
        AND ng.geometry_type = 'point'
        AND ng.geom && ST_MakeEnvelope($1, $2, $3, $4, 4326)
        ORDER BY nt.id
        LIMIT $5;
        """
        
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query, min_lng, min_lat, max_lng, max_lat, limit)
        
        # Convert to simplified GeoJSON (much faster than ST_AsGeoJSON)
        features = []
        for row in rows:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row['lng']), float(row['lat'])]
                },
                "properties": {
                    'id': row['id'],
                    'short_code': row['short_code'],
                    'type': 'node',
                    'element_type': row['type'] or 'CH',
                    'subtype': row['subtype'],
                    'river_name': row['river_name'],
                    'river_mile': float(row['river_mile']) if row['river_mile'] else None,
                    'display_name': row['river_name'] or row['short_code'],
                    'connectivity_status': 'connected'
                }
            }
            features.append(feature)
        
        result = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "total_features": len(features),
                "bbox": bbox_coords,
                "query_time_ms": round((time.time() - start_time) * 1000, 2),
                "api_type": "fast_nodes_only"
            }
        }
        
        # Cache the result
        _spatial_cache[cache_key] = (result, current_time)
        
        print(f"🚀 Fast nodes query completed in {time.time() - start_time:.3f}s")
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


async def traverse_network_geojson(
    db_pool: asyncpg.Pool,
    short_code: str,
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(10, description="Maximum traversal depth"),
    include_arcs: bool = Query(True, description="Include connecting arcs in result")
):
    """
    Traverse network from a short_code and return GeoJSON for Mapbox visualization
    """
    try:
        # Build enhanced traversal query with multiple connection strategies
        if direction == "upstream":
            traversal_query = """
            WITH RECURSIVE network_traversal AS (
                -- Base: Start with clicked element
                SELECT 
                    nt.id, nt.short_code, nt.schematic_type,
                    nt.from_node, nt.to_node, nt.connectivity_status,
                    nt.river_name, nt.arc_name, nt.type, nt.river_mile,
                    0 as depth,
                    ARRAY[nt.short_code] as path
                FROM network_topology nt
                WHERE nt.short_code = $1
                
                UNION ALL
                
                -- Recursive: Follow upstream connections with enhanced logic
                SELECT 
                    nt.id, nt.short_code, nt.schematic_type,
                    nt.from_node, nt.to_node, nt.connectivity_status,
                    nt.river_name, nt.arc_name, nt.type, nt.river_mile,
                    prev.depth + 1,
                    prev.path || nt.short_code
                FROM network_traversal prev
                JOIN network_topology nt ON (
                    -- Direct connectivity
                    nt.to_node = prev.short_code OR 
                    nt.short_code = prev.from_node OR
                    -- Enhanced: Same river system connectivity
                    (nt.river_name = prev.river_name AND nt.river_name IS NOT NULL 
                     AND nt.river_mile IS NOT NULL AND prev.river_mile IS NOT NULL
                     AND ABS(nt.river_mile - prev.river_mile) <= 10) OR
                    -- Enhanced: Element type patterns (D_ to node, I_ to node, etc.)
                    (prev.schematic_type = 'node' AND nt.schematic_type = 'arc' 
                     AND (nt.short_code LIKE 'D_' || prev.short_code || '%' OR
                          nt.short_code LIKE 'I_' || prev.short_code || '%' OR
                          nt.short_code LIKE 'R_' || prev.short_code || '%'))
                )
                WHERE prev.depth < $2
                AND NOT (nt.short_code = ANY(prev.path))
                AND nt.connectivity_status = 'connected'
            )"""
        elif direction == "downstream":
            traversal_query = """
            WITH RECURSIVE network_traversal AS (
                -- Base: Start with clicked element
                SELECT 
                    nt.id, nt.short_code, nt.schematic_type,
                    nt.from_node, nt.to_node, nt.connectivity_status,
                    nt.river_name, nt.arc_name, nt.type, nt.river_mile,
                    0 as depth,
                    ARRAY[nt.short_code] as path
                FROM network_topology nt
                WHERE nt.short_code = $1
                
                UNION ALL
                
                -- Recursive: Follow downstream connections with enhanced logic
                SELECT 
                    nt.id, nt.short_code, nt.schematic_type,
                    nt.from_node, nt.to_node, nt.connectivity_status,
                    nt.river_name, nt.arc_name, nt.type, nt.river_mile,
                    prev.depth + 1,
                    prev.path || nt.short_code
                FROM network_traversal prev
                JOIN network_topology nt ON (
                    -- Direct connectivity
                    nt.from_node = prev.short_code OR 
                    nt.short_code = prev.to_node OR
                    -- Enhanced: Same river system connectivity (downstream = lower mile)
                    (nt.river_name = prev.river_name AND nt.river_name IS NOT NULL 
                     AND nt.river_mile IS NOT NULL AND prev.river_mile IS NOT NULL
                     AND nt.river_mile <= prev.river_mile AND ABS(nt.river_mile - prev.river_mile) <= 10) OR
                    -- Enhanced: Element type patterns
                    (prev.schematic_type = 'node' AND nt.schematic_type = 'arc' 
                     AND (nt.short_code LIKE 'C_' || prev.short_code OR
                          nt.short_code LIKE 'D_' || prev.short_code || '%' OR
                          nt.from_node = prev.short_code))
                )
                WHERE prev.depth < $2
                AND NOT (nt.short_code = ANY(prev.path))
                AND nt.connectivity_status = 'connected'
            )"""
        else:  # both
            traversal_query = """
            WITH RECURSIVE network_traversal AS (
                -- Base: Start with clicked element
                SELECT 
                    nt.id, nt.short_code, nt.schematic_type,
                    nt.from_node, nt.to_node, nt.connectivity_status,
                    nt.river_name, nt.arc_name, nt.type, nt.river_mile,
                    0 as depth,
                    ARRAY[nt.short_code] as path
                FROM network_topology nt
                WHERE nt.short_code = $1
                
                UNION ALL
                
                -- Recursive: Follow all connections with enhanced logic
                SELECT 
                    nt.id, nt.short_code, nt.schematic_type,
                    nt.from_node, nt.to_node, nt.connectivity_status,
                    nt.river_name, nt.arc_name, nt.type, nt.river_mile,
                    prev.depth + 1,
                    prev.path || nt.short_code
                FROM network_traversal prev
                JOIN network_topology nt ON (
                    -- Direct connectivity
                    nt.from_node = prev.short_code OR 
                    nt.to_node = prev.short_code OR
                    nt.short_code = prev.from_node OR
                    nt.short_code = prev.to_node OR
                    -- Enhanced: Same river system connectivity
                    (nt.river_name = prev.river_name AND nt.river_name IS NOT NULL 
                     AND nt.river_mile IS NOT NULL AND prev.river_mile IS NOT NULL
                     AND ABS(nt.river_mile - prev.river_mile) <= 15) OR
                    -- Enhanced: CalSim naming patterns
                    (prev.schematic_type = 'node' AND nt.schematic_type = 'arc' 
                     AND (nt.short_code LIKE 'C_' || prev.short_code OR
                          nt.short_code LIKE 'D_' || prev.short_code || '%' OR
                          nt.short_code LIKE 'I_' || prev.short_code || '%' OR
                          nt.short_code LIKE 'R_' || prev.short_code || '%')) OR
                    -- Enhanced: Reverse patterns (arc to node)
                    (prev.schematic_type = 'arc' AND nt.schematic_type = 'node'
                     AND (prev.short_code LIKE 'C_' || nt.short_code OR
                          prev.short_code LIKE 'D_' || nt.short_code || '%' OR
                          prev.short_code LIKE 'I_' || nt.short_code || '%' OR
                          prev.short_code LIKE 'R_' || nt.short_code || '%'))
                )
                WHERE prev.depth < $2
                AND NOT (nt.short_code = ANY(prev.path))
                AND nt.connectivity_status = 'connected'
            )"""
        
        # Complete the query with GeoJSON output
        final_query = traversal_query + """
        SELECT 
            trav.id,
            trav.short_code,
            trav.schematic_type,
            trav.from_node,
            trav.to_node,
            trav.connectivity_status,
            trav.river_name,
            trav.arc_name,
            trav.type,
            trav.depth,
            ST_AsGeoJSON(ng.geom) as geometry,
            ng.geometry_type
        FROM network_traversal trav
        LEFT JOIN network_gis ng ON trav.short_code = ng.short_code
        ORDER BY trav.depth, trav.schematic_type, trav.short_code;
        """
        
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(final_query, short_code, max_depth)
        
        # Convert to GeoJSON
        features = []
        for row in rows:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            
            properties = {
                'id': row['id'],
                'short_code': row['short_code'],
                'type': row['schematic_type'],
                'depth': row['depth'],
                'connectivity_status': row['connectivity_status'],
                'element_type': row['type']
            }
            
            # Add type-specific properties
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'display_name': row['river_name'] or row['short_code']
                })
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'],
                    'to_node': row['to_node'],
                    'display_name': row['arc_name'] or f"{row['from_node']} → {row['to_node']}"
                })
            
            if geometry:  # Only include features with geometry
                feature = {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": properties
                }
                features.append(feature)
        
        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "start_element": short_code,
                "direction": direction,
                "max_depth": max_depth,
                "total_features": len(features),
                "includes_arcs": include_arcs
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


async def get_network_element_details(
    db_pool: asyncpg.Pool,
    short_code: str
):
    """
    Get detailed information about a specific network element
    """
    try:
        query = """
        SELECT 
            nt.*,
            ST_AsGeoJSON(ng.geom) as geometry,
            ng.geometry_type
        FROM network_topology nt
        LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
        WHERE nt.short_code = $1;
        """
        
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(query, short_code)
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Network element {short_code} not found")
        
        # Build response
        element_data = dict(row)
        
        # Parse geometry if available
        if element_data['geometry']:
            element_data['geometry'] = json.loads(element_data['geometry'])
        
        return element_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
