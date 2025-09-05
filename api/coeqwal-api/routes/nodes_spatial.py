"""
Spatial filtering endpoints for network nodes
For COEQWAL FastAPI api
"""

from fastapi import Query, HTTPException
from typing import Optional
import asyncpg

async def get_nodes_spatial(
    db_pool: asyncpg.Pool,
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    zoom: int = Query(10, description="Map zoom level"),
    limit: int = Query(1000, description="Maximum nodes to return")
):
    """
    Get nodes within bounding box with zoom-based priority filtering
    
    Zoom <= 7: Priority types only (STR, PR, PS, S)
    Zoom > 7: All node types
    """
    try:
        # Parse bounding box
        min_lng, min_lat, max_lng, max_lat = map(float, bbox.split(','))
        
        # Zoom-based filtering
        if zoom <= 7:
            # Low zoom: Priority types only
            type_filter = """
            AND nt.short_code IN ('STR', 'STR-SIM', 'PR-A', 'PR-U', 'PR-R', 'PS', 'PS-SG', 'S-A', 'S-U')
            """
        else:
            # High zoom: All types
            type_filter = ""
        
        query = f"""
        SELECT 
            n.id,
            n.short_code,
            n.name,
            n.description,
            nt.short_code as node_type,
            nt.name as node_type_name,
            hr.short_code as hydrologic_region,
            ST_AsGeoJSON(n.geom)::jsonb as geometry,
            n.elevation,
            n.capacity_taf,
            -- Count connected arcs for interactivity
            (SELECT COUNT(*) FROM network_arc WHERE from_node_id = n.id OR to_node_id = n.id) as connected_arcs
        FROM network_node n
        LEFT JOIN network_node_type nt ON nt.id = n.node_type_id
        LEFT JOIN hydrologic_region hr ON hr.id = n.hydrologic_region_id
        WHERE ST_Intersects(n.geom, ST_MakeEnvelope($1, $2, $3, $4, 4326))
        {type_filter}
        AND n.geom IS NOT NULL
        ORDER BY 
            -- Priority order: STR -> PR -> PS -> S -> Others
            CASE 
                WHEN nt.short_code LIKE 'STR%' THEN 1
                WHEN nt.short_code LIKE 'PR-%' THEN 2  
                WHEN nt.short_code LIKE 'PS%' THEN 3
                WHEN nt.short_code LIKE 'S-%' THEN 4
                ELSE 5
            END,
            COALESCE(n.capacity_taf, 0) DESC,
            n.name
        LIMIT $5;
        """
        
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query, min_lng, min_lat, max_lng, max_lat, limit)
            
            nodes = []
            for row in rows:
                nodes.append({
                    "id": row["id"],
                    "short_code": row["short_code"],
                    "name": row["name"],
                    "description": row["description"],
                    "node_type": row["node_type"],
                    "node_type_name": row["node_type_name"],
                    "hydrologic_region": row["hydrologic_region"],
                    "geometry": row["geometry"],
                    "elevation": float(row["elevation"]) if row["elevation"] else None,
                    "capacity_taf": float(row["capacity_taf"]) if row["capacity_taf"] else None,
                    "connected_arcs": row["connected_arcs"],
                    "is_interactive": row["connected_arcs"] > 0
                })
                
            return {
                "nodes": nodes,
                "total": len(nodes),
                "bbox": bbox,
                "zoom": zoom,
                "priority_filter": zoom <= 7,
                "truncated": len(nodes) >= limit
            }
            
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid bbox format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


async def get_node_network(
    db_pool: asyncpg.Pool,
    node_id: int,
    direction: str = "both",
    max_depth: int = 2,
    include_arcs: bool = True
):
    """
    Get upstream/downstream network from a clicked node
    """
    try:
        # Build direction conditions
        if direction == "upstream":
            arc_condition = "a.to_node_id = $1"
            next_node_field = "a.from_node_id"
        elif direction == "downstream":
            arc_condition = "a.from_node_id = $1" 
            next_node_field = "a.to_node_id"
        else:  # both
            arc_condition = "(a.from_node_id = $1 OR a.to_node_id = $1)"
            next_node_field = "CASE WHEN a.from_node_id = $1 THEN a.to_node_id ELSE a.from_node_id END"
        
        # Get connected nodes via traversal
        traversal_query = f"""
        WITH RECURSIVE network_traversal AS (
            -- Base: Start with clicked node
            SELECT 
                n.id, n.short_code, n.name,
                ST_AsGeoJSON(n.geom)::jsonb as geometry,
                nt.short_code as node_type,
                0 as depth,
                ARRAY[n.id] as path
            FROM network_node n
            LEFT JOIN network_node_type nt ON nt.id = n.node_type_id
            WHERE n.id = $1
            
            UNION ALL
            
            -- Recursive: Follow connected arcs
            SELECT 
                n.id, n.short_code, n.name,
                ST_AsGeoJSON(n.geom)::jsonb as geometry,
                nt.short_code as node_type,
                nt_prev.depth + 1,
                nt_prev.path || n.id
            FROM network_traversal nt_prev
            JOIN network_arc a ON {arc_condition.replace('$1', 'nt_prev.id')}
            JOIN network_node n ON n.id = {next_node_field}
            LEFT JOIN network_node_type nt ON nt.id = n.node_type_id
            WHERE nt_prev.depth < $2
            AND NOT (n.id = ANY(nt_prev.path))  -- Prevent cycles
        )
        SELECT DISTINCT * FROM network_traversal
        ORDER BY depth, node_type, name;
        """
        
        async with db_pool.acquire() as conn:
            # Get traversal nodes
            nodes = await conn.fetch(traversal_query, node_id, max_depth)
            
            result = {
                "source_node_id": node_id,
                "direction": direction,
                "max_depth": max_depth,
                "nodes": [dict(row) for row in nodes],
                "arcs": []
            }
            
            if include_arcs and len(nodes) > 1:
                # Get connecting arcs between traversal nodes
                node_ids = [row["id"] for row in nodes]
                arc_query = """
                SELECT 
                    a.id, a.short_code, a.name,
                    a.from_node_id, a.to_node_id,
                    fn.short_code as from_node_code,
                    tn.short_code as to_node_code,
                    ST_AsGeoJSON(a.geom)::jsonb as geometry,
                    at.short_code as arc_type
                FROM network_arc a
                LEFT JOIN network_node fn ON fn.id = a.from_node_id
                LEFT JOIN network_node tn ON tn.id = a.to_node_id
                LEFT JOIN network_arc_type at ON at.id = a.arc_type_id
                WHERE (a.from_node_id = ANY($1) AND a.to_node_id = ANY($1))
                AND a.geom IS NOT NULL
                ORDER BY a.name;
                """
                
                arcs = await conn.fetch(arc_query, node_ids)
                result["arcs"] = [dict(row) for row in arcs]
            
            return result
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
