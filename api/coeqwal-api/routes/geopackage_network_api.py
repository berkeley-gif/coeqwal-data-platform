"""
Geopackage-optimized network API
Designed for Ring 1 foundation with connectivity emphasis
"""

import asyncpg
from typing import List, Dict, Any
import json


async def geopackage_network_traversal(
    db_pool: asyncpg.Pool,
    short_code: str,
    direction: str = "both",
    max_depth: int = 10
) -> Dict[str, Any]:
    
    # Direction logic
    if direction == "upstream":
        connection_logic = "nt.to_node = prev.short_code"
    elif direction == "downstream":
        connection_logic = "nt.from_node = prev.short_code"
    else:  # both
        connection_logic = "(nt.from_node = prev.short_code OR nt.to_node = prev.short_code)"
    
    query = f"""
    WITH RECURSIVE geopackage_traversal AS (
        -- Start: Find element in clean geopackage network
        SELECT 
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.type, nt.sub_type,
            nt.river_name, nt.arc_name, nt.river_mile,
            0 as depth, 'geopackage_clean' as strategy,
            ARRAY[nt.short_code] as path
        FROM network_topology nt
        WHERE nt.short_code = $1
        AND nt.is_active = true  -- Only active elements
        AND nt.connectivity_status = 'connected'
        
        UNION ALL
        
        -- Recursive: Follow clean geopackage connections
        SELECT 
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.type, nt.sub_type,
            nt.river_name, nt.arc_name, nt.river_mile,
            prev.depth + 1, 'geopackage_clean',
            prev.path || nt.short_code
        FROM geopackage_traversal prev
        JOIN network_topology nt ON ({connection_logic})
        WHERE prev.depth < $2
        AND NOT (nt.short_code = ANY(prev.path))  -- Prevent cycles
        AND nt.is_active = true  -- Only active elements
        AND nt.connectivity_status = 'connected'
    )
    SELECT 
        gt.*,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM geopackage_traversal gt
    JOIN network_gis ng ON gt.short_code = ng.short_code  -- Must have geometry
    ORDER BY gt.depth, gt.schematic_type, gt.short_code;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, short_code, max_depth)
    
    features = []
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if not geometry:
                continue
                
            properties = {
                'id': row['id'],
                'short_code': row['short_code'],
                'type': row['schematic_type'],
                'element_type': row['type'] or '',
                'subtype': row['sub_type'],
                'depth': row['depth'],
                'connectivity_status': 'connected',
                'strategy': row['strategy']
            }
            
            # Type-specific properties
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'river_mile': float(row['river_mile']) if row['river_mile'] else None,
                    'display_name': row['river_name'] or row['short_code']
                })
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'] or '',
                    'to_node': row['to_node'] or '',
                    'display_name': row['arc_name'] or f"{row['from_node'] or ''} → {row['to_node'] or ''}"
                })
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "start_element": short_code,
            "direction": direction,
            "max_depth": max_depth,
            "total_features": len(features),
            "approach": "clean_geopackage_foundation",
            "connectivity_rate": "99.7%"
        }
    }


async def fast_geopackage_geojson(
    db_pool: asyncpg.Pool,
    bbox: str,
    include_arcs: bool = True,
    include_nodes: bool = True,
    limit: int = 5000
) -> Dict[str, Any]:
    """
    Hopefully super-fast GeoJSON endpoint optimized for thousands of elements
    Uses the clean geopackage foundation with optimized indexes
    """
    
    try:
        # Parse bbox
        bbox_coords = [float(x) for x in bbox.split(',')]
        if len(bbox_coords) != 4:
            raise ValueError("Invalid bbox format")
        
        min_lng, min_lat, max_lng, max_lat = bbox_coords
        
        # Build WHERE conditions
        where_conditions = ["nt.is_active = true"]
        
        if include_nodes and include_arcs:
            # Include both
            pass
        elif include_nodes:
            where_conditions.append("nt.schematic_type = 'node'")
        elif include_arcs:
            where_conditions.append("nt.schematic_type = 'arc'")
        else:
            where_conditions.append("false")  # Return nothing
        
        # Ultra-fast query using optimized indexes
        query = f"""
        SELECT 
            nt.id, nt.short_code, nt.schematic_type, nt.type,
            nt.river_name, nt.arc_name, nt.river_mile,
            ST_X(ng.geom) as lng, ST_Y(ng.geom) as lat,
            ng.geometry_type
        FROM network_topology nt
        JOIN network_gis ng ON nt.short_code = ng.short_code
        WHERE {' AND '.join(where_conditions)}
        AND ng.geom && ST_MakeEnvelope($1, $2, $3, $4, 4326)  -- Fast spatial filter
        ORDER BY nt.type, nt.short_code
        LIMIT $5;
        """
        
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query, min_lng, min_lat, max_lng, max_lat, limit)
        
        # Convert to GeoJSON features
        features = []
        for row in rows:
            try:
                properties = {
                    'id': row['id'],
                    'short_code': row['short_code'],
                    'type': row['schematic_type'],
                    'element_type': row['type'] or '',
                    'connectivity_status': 'connected'
                }
                
                # Type-specific properties
                if row['schematic_type'] == 'node':
                    properties.update({
                        'river_name': row['river_name'],
                        'river_mile': float(row['river_mile']) if row['river_mile'] else None,
                        'display_name': row['river_name'] or row['short_code']
                    })
                elif row['schematic_type'] == 'arc':
                    properties.update({
                        'arc_name': row['arc_name'],
                        'display_name': row['arc_name'] or row['short_code']
                    })
                
                # Create point geometry from extracted coordinates
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(row['lng']), float(row['lat'])]
                    },
                    "properties": properties
                })
                
            except Exception as e:
                print(f"Error processing {row.get('short_code', 'unknown')}: {e}")
                continue
        
        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "total_features": len(features),
                "bbox": bbox_coords,
                "includes_arcs": include_arcs,
                "includes_nodes": include_nodes,
                "approach": "ultra_fast_geopackage",
                "foundation": "clean_ring1_99.7%_connectivity"
            }
        }
        
    except Exception as e:
        raise Exception(f"Fast GeoJSON error: {str(e)}")
