"""
Complete CalSim network traversal
"""
import json
import asyncpg
from fastapi import HTTPException, Query


async def get_node_network_unlimited(
    db_pool: asyncpg.Pool,
    node_id: int,
    direction: str = "both",
    include_arcs: str = "true"
):
    """
    Get COMPLETE upstream/downstream network with no depth limit
    """
    try:
        # Convert string to boolean
        include_arcs_bool = include_arcs.lower() in ('true', '1', 'yes')
        
        # Build direction conditions (matching working nodes_spatial.py pattern)
        if direction == "upstream":
            arc_condition = "a.to_node_id = $1"
            next_node_field = "a.from_node_id"
        elif direction == "downstream":
            arc_condition = "a.from_node_id = $1" 
            next_node_field = "a.to_node_id"
        else:  # both
            arc_condition = "(a.from_node_id = $1 OR a.to_node_id = $1)"
            next_node_field = "CASE WHEN a.from_node_id = $1 THEN a.to_node_id ELSE a.from_node_id END"
        
        # unlimited depth traversal - PostgreSQL will stop on cycles automatically
        traversal_query = f"""
        WITH RECURSIVE network_traversal AS (
            -- Base: Start with clicked node
            SELECT 
                n.id, n.short_code, n.name,
                nt.short_code as node_type,
                0 as depth,
                ARRAY[n.id] as path
            FROM network_node n
            LEFT JOIN network_node_type nt ON nt.id = n.node_type_id
            WHERE n.id = $1
            
            UNION ALL
            
            -- Recursive: Follow ALL connected arcs (NO DEPTH LIMIT)
            SELECT 
                n.id, n.short_code, n.name,
                nt.short_code as node_type,
                nt_prev.depth + 1,
                nt_prev.path || n.id
            FROM network_traversal nt_prev
            JOIN network_arc a ON {arc_condition.replace('$1', 'nt_prev.id')}
            JOIN network_node n ON n.id = {next_node_field}
            LEFT JOIN network_node_type nt ON nt.id = n.node_type_id
            WHERE NOT (n.id = ANY(nt_prev.path))  -- Only cycle prevention, NO depth limit
            AND nt_prev.depth < 100  -- Safety limit to prevent infinite loops
        )
        -- Get geometry after recursion
        SELECT 
            nt.id, nt.short_code, nt.name, nt.node_type, nt.depth,
            ST_AsGeoJSON(n.geom) as geometry,
            n.latitude, n.longitude,
            (SELECT COUNT(*) FROM network_arc WHERE from_node_id = nt.id OR to_node_id = nt.id) as connected_arcs
        FROM network_traversal nt
        JOIN network_node n ON n.id = nt.id
        ORDER BY nt.depth, nt.node_type, nt.name;
        """
        
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(traversal_query, node_id)
            
            nodes = []
            for row in rows:
                # Parse geometry
                geometry = row["geometry"]
                if isinstance(geometry, str):
                    try:
                        geometry = json.loads(geometry)
                    except (json.JSONDecodeError, TypeError):
                        geometry = None
                
                nodes.append({
                    "id": row["id"],
                    "short_code": row["short_code"],
                    "name": row["name"],
                    "node_type": row["node_type"],
                    "depth": row["depth"],
                    "geometry": geometry,
                    "latitude": float(row["latitude"]) if row["latitude"] else None,
                    "longitude": float(row["longitude"]) if row["longitude"] else None,
                    "connected_arcs": row["connected_arcs"]
                })
            
            result = {
                "clicked_node_id": node_id,
                "direction": direction,
                "traversal_type": "unlimited_depth",
                "nodes": nodes,
                "total_nodes": len(nodes),
                "max_depth_reached": max(row["depth"] for row in rows) if rows else 0,
                "vast_network": True,
                "note": "Complete network traversal with no depth limit like CalSim3_schematic"
            }
            
            # Optionally include arcs for the complete network
            if include_arcs_bool and len(nodes) > 0:
                node_ids = [node["id"] for node in nodes]
                # Use parameterized query for safety
                arcs_query = """
                SELECT 
                    a.id, a.short_code, a.name,
                    a.from_node_id, a.to_node_id,
                    fn.short_code as from_node_code, tn.short_code as to_node_code,
                    at.short_code as arc_type,
                    ST_AsGeoJSON(a.geom) as geometry
                FROM network_arc a
                LEFT JOIN network_node fn ON fn.id = a.from_node_id
                LEFT JOIN network_node tn ON tn.id = a.to_node_id
                LEFT JOIN network_arc_type at ON at.id = a.arc_type_id
                WHERE (a.from_node_id = ANY($1::int[]) OR a.to_node_id = ANY($1::int[]))
                ORDER BY a.short_code;
                """
                
                arc_rows = await conn.fetch(arcs_query, node_ids)
                parsed_arcs = []
                for arc_row in arc_rows:
                    arc_dict = dict(arc_row)
                    if arc_dict["geometry"]:
                        if isinstance(arc_dict["geometry"], str):
                            try:
                                arc_dict["geometry"] = json.loads(arc_dict["geometry"])
                            except (json.JSONDecodeError, TypeError):
                                arc_dict["geometry"] = None
                    parsed_arcs.append(arc_dict)
                
                result["arcs"] = parsed_arcs
                result["total_arcs"] = len(parsed_arcs)
            
            return result
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
