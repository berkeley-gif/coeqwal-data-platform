"""
SIMPLE network traversal for map visualization
Clean, straightforward approach using existing from_node/to_node connectivity
"""

import asyncpg
from typing import List, Dict, Any
import json


async def simple_network_traversal(
    db_pool: asyncpg.Pool,
    short_code: str,
    direction: str = "both",
    max_depth: int = 5
) -> Dict[str, Any]:
    """
    Simple, clean network traversal for map visualization
    
    Uses existing from_node/to_node connectivity (82% connected)
    No complex algorithms - just follow the connections
    """
    
    # Simple recursive query - no fancy algorithms
    if direction == "upstream":
        connection_logic = "nt.to_node = prev.short_code"
    elif direction == "downstream":  
        connection_logic = "nt.from_node = prev.short_code"
    else:  # both
        connection_logic = "(nt.from_node = prev.short_code OR nt.to_node = prev.short_code)"
    
    query = f"""
    WITH RECURSIVE simple_traversal AS (
        -- Start node
        SELECT 
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.type, nt.subtype,
            nt.river_name, nt.arc_name, nt.river_mile,
            0 as depth,
            ARRAY[nt.short_code] as path
        FROM network_topology nt
        WHERE nt.short_code = $1
        AND nt.connectivity_status = 'connected'
        
        UNION ALL
        
        -- Follow connections
        SELECT 
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.type, nt.subtype,
            nt.river_name, nt.arc_name, nt.river_mile,
            prev.depth + 1,
            prev.path || nt.short_code
        FROM simple_traversal prev
        JOIN network_topology nt ON ({connection_logic})
        WHERE prev.depth < $2
        AND NOT (nt.short_code = ANY(prev.path))  -- Prevent cycles
        AND nt.connectivity_status = 'connected'
    )
    SELECT 
        st.*,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM simple_traversal st
    LEFT JOIN network_gis ng ON st.short_code = ng.short_code
    WHERE ng.geom IS NOT NULL  -- Only elements with map geometry
    ORDER BY st.depth, st.schematic_type, st.short_code;
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
                'subtype': row['subtype'],
                'depth': row['depth'],
                'connectivity_status': 'connected'
            }
            
            # Add type-specific display info
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
            "approach": "simple_direct_connectivity"
        }
    }
