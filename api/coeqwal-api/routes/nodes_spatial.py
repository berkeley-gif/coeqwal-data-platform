"""
Spatial filtering endpoints for network nodes
For COEQWAL FastAPI api
"""

from fastapi import Query, HTTPException
import asyncpg
import json


async def get_nodes_spatial(
    db_pool: asyncpg.Pool,
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    zoom: int = Query(10, description="Map zoom level"),
    limit: int = Query(
        5000, description="Maximum nodes to return (enhanced for 7K+ network)"
    ),
):
    """
    Get nodes within bounding box with zoom-based priority filtering

    Zoom <= 7: Priority types only (STR, PR, PS, S)
    Zoom > 7: All node types
    """
    try:
        # Parse bounding box
        min_lng, min_lat, max_lng, max_lat = map(float, bbox.split(","))

        # Zoom-based filtering (DISABLED for enhanced network testing)
        # For enhanced network testing, show all node types regardless of zoom
        type_filter = ""  # No filtering - show all nodes for testing

        # Original zoom filtering (commented out for testing):
        # if zoom <= 7:
        #     type_filter = "AND nt.short_code IN ('STR', 'STR-SIM', 'PR-A', 'PR-U', 'PR-R', 'PS', 'PS-SG', 'S-A', 'S-U')"
        # else:
        #     type_filter = ""

        query = f"""
        SELECT 
            n.id,
            n.short_code,
            n.name,
            n.description,
            nt.short_code as node_type,
            nt.name as node_type_name,
            hr.short_code as hydrologic_region,
            ST_AsGeoJSON(n.geom)::json as geometry,
            n.latitude,
            n.longitude,
            n.riv_mi,
            n.riv_name,
            
            -- Precise reservoir identification
            CASE 
                WHEN re.id IS NOT NULL THEN true
                WHEN nt.short_code IN ('STR', 'STR-SIM', 'STR-NSM') THEN true
                ELSE false 
            END as is_reservoir,
            
            -- Reservoir attributes (only for actual reservoirs)
            re.capacity_taf,
            re.operational_purpose,
            re.associated_river,
            
            -- Map category for frontend styling
            CASE 
                WHEN re.id IS NOT NULL OR nt.short_code IN ('STR', 'STR-SIM', 'STR-NSM') THEN 'reservoir'
                WHEN nt.short_code LIKE 'PS%' THEN 'pump_station'
                WHEN nt.short_code LIKE 'WTP%' THEN 'water_treatment'
                WHEN nt.short_code = 'WWTP' THEN 'wastewater_treatment'
                WHEN nt.short_code LIKE 'PR-%' THEN 'project'
                WHEN nt.short_code LIKE 'S-%' THEN 'source'
                ELSE 'other'
            END as map_category,
            
            -- Count connected arcs for interactivity
            (SELECT COUNT(*) FROM network_arc WHERE from_node_id = n.id OR to_node_id = n.id) as connected_arcs
            
        FROM network_node n
        LEFT JOIN network_node_type nt ON nt.id = n.node_type_id
        LEFT JOIN hydrologic_region hr ON hr.id = n.hydrologic_region_id
        LEFT JOIN reservoir_entity re ON re.network_node_id = n.id
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
            COALESCE(re.capacity_taf, 0) DESC,
            n.name
        LIMIT $5;
        """

        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query, min_lng, min_lat, max_lng, max_lat, limit)

            nodes = []
            for row in rows:
                # Parse geometry JSON string to object
                geometry = row["geometry"]
                if isinstance(geometry, str):
                    try:
                        geometry = json.loads(geometry)
                    except (json.JSONDecodeError, TypeError):
                        geometry = None

                nodes.append(
                    {
                        "id": row["id"],
                        "short_code": row["short_code"],
                        "name": row["name"],
                        "description": row["description"],
                        "node_type": row["node_type"],
                        "node_type_name": row["node_type_name"],
                        "hydrologic_region": row["hydrologic_region"],
                        "geometry": geometry,  # Parsed JSON object
                        "latitude": float(row["latitude"]) if row["latitude"] else None,
                        "longitude": float(row["longitude"])
                        if row["longitude"]
                        else None,
                        "riv_mi": float(row["riv_mi"]) if row["riv_mi"] else None,
                        "riv_name": row["riv_name"],
                        # Reservoir identification and attributes
                        "is_reservoir": row["is_reservoir"],
                        "capacity_taf": float(row["capacity_taf"])
                        if row["capacity_taf"]
                        else None,
                        "operational_purpose": row["operational_purpose"],
                        "associated_river": row["associated_river"],
                        # Map styling
                        "map_category": row["map_category"],
                        # Interactivity
                        "connected_arcs": row["connected_arcs"],
                        "is_interactive": row["connected_arcs"] > 0,
                    }
                )

            return {
                "nodes": nodes,
                "total": len(nodes),
                "bbox": bbox,
                "zoom": zoom,
                "priority_filter": zoom <= 7,
                "truncated": len(nodes) >= limit,
            }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid bbox format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


async def get_node_network(
    db_pool: asyncpg.Pool,
    node_id: int,
    direction: str = "both",
    max_depth: int = 50,  # basically unlimited network traversal depth
    include_arcs: str = "true",
):
    """
    Get upstream/downstream network from a clicked node
    """
    try:
        # Convert string to boolean
        include_arcs_bool = include_arcs.lower() in ("true", "1", "yes")
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
                nt.short_code as node_type,
                nt_prev.depth + 1,
                nt_prev.path || n.id
            FROM network_traversal nt_prev
            JOIN network_arc a ON {arc_condition.replace("$1", "nt_prev.id")}
            JOIN network_node n ON n.id = {next_node_field}
            LEFT JOIN network_node_type nt ON nt.id = n.node_type_id
            WHERE nt_prev.depth < $2
            AND NOT (n.id = ANY(nt_prev.path))  -- Prevent cycles
        )
        -- Get geometry after recursion to avoid JSON comparison issues
        SELECT 
            nt.id, nt.short_code, nt.name, nt.node_type, nt.depth,
            ST_AsGeoJSON(n.geom) as geometry
        FROM network_traversal nt
        JOIN network_node n ON n.id = nt.id
        ORDER BY nt.depth, nt.node_type, nt.name;
        """

        async with db_pool.acquire() as conn:
            # Get traversal nodes
            nodes = await conn.fetch(traversal_query, node_id, max_depth)

            # Parse geometry for nodes
            parsed_nodes = []
            for row in nodes:
                node_dict = dict(row)
                if isinstance(node_dict["geometry"], str):
                    try:
                        node_dict["geometry"] = json.loads(node_dict["geometry"])
                    except (json.JSONDecodeError, TypeError):
                        node_dict["geometry"] = None
                parsed_nodes.append(node_dict)

            result = {
                "source_node_id": node_id,
                "direction": direction,
                "max_depth": max_depth,
                "nodes": parsed_nodes,
                "arcs": [],
            }

            if include_arcs_bool and len(nodes) > 1:
                # Get connecting arcs between traversal nodes
                node_ids = [row["id"] for row in nodes]
                arc_query = """
                SELECT 
                    a.id, a.short_code, a.name,
                    a.from_node_id, a.to_node_id,
                    fn.short_code as from_node_code,
                    tn.short_code as to_node_code,
                    ST_AsGeoJSON(a.geom)::json as geometry,
                    at.short_code as arc_type
                FROM network_arc a
                LEFT JOIN network_node fn ON fn.id = a.from_node_id
                LEFT JOIN network_node tn ON tn.id = a.to_node_id
                LEFT JOIN network_arc_type at ON at.id = a.arc_type_id
                WHERE (a.from_node_id = ANY($1) OR a.to_node_id = ANY($1))
                ORDER BY a.name;
                """

                arcs = await conn.fetch(arc_query, node_ids)

                # Parse geometry for arcs
                parsed_arcs = []
                for row in arcs:
                    arc_dict = dict(row)
                    if isinstance(arc_dict["geometry"], str):
                        try:
                            arc_dict["geometry"] = json.loads(arc_dict["geometry"])
                        except (json.JSONDecodeError, TypeError):
                            arc_dict["geometry"] = None
                    parsed_arcs.append(arc_dict)

                result["arcs"] = parsed_arcs

            return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


async def get_all_nodes_unfiltered(
    db_pool: asyncpg.Pool,
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    limit: int = Query(10000, description="Maximum nodes to return"),
    source_filter: str = Query(
        "all", description="'geopackage', 'network_schematic', or 'all'"
    ),
):
    """
    Get ALL nodes within bounding box with NO type filtering - for enhanced network testing
    """
    try:
        # Parse bounding box
        min_lng, min_lat, max_lng, max_lat = map(float, bbox.split(","))

        # Optional source filtering
        source_condition = ""
        if source_filter == "geopackage":
            source_condition = "AND src.source = 'geopackage'"
        elif source_filter == "network_schematic":
            source_condition = "AND src.source = 'network_schematic'"

        query = f"""
        SELECT 
            n.id,
            n.short_code,
            n.name,
            n.description,
            nt.short_code as node_type,
            nt.name as node_type_name,
            hr.short_code as hydrologic_region,
            ST_AsGeoJSON(n.geom)::json as geometry,
            n.latitude,
            n.longitude,
            n.riv_mi,
            n.riv_name,
            n.json_id,
            
            -- Precise reservoir identification
            CASE 
                WHEN re.id IS NOT NULL THEN true
                WHEN nt.short_code IN ('STR', 'STR-SIM', 'STR-NSM') THEN true
                ELSE false 
            END as is_reservoir,
            
            -- Reservoir attributes
            re.capacity_taf,
            re.operational_purpose,
            re.associated_river,
            
            -- Map category
            CASE 
                WHEN re.id IS NOT NULL OR nt.short_code IN ('STR', 'STR-SIM', 'STR-NSM') THEN 'reservoir'
                WHEN nt.short_code LIKE 'PS%' THEN 'pump_station'
                WHEN nt.short_code LIKE 'WTP%' THEN 'water_treatment'
                WHEN nt.short_code = 'WWTP' THEN 'wastewater_treatment'
                WHEN nt.short_code LIKE 'PR-%' THEN 'project'
                WHEN nt.short_code LIKE 'S-%' THEN 'source'
                WHEN nt.short_code LIKE 'CH-%' THEN 'channel'
                ELSE 'other'
            END as map_category,
            
            -- Source information
            src.source as data_source,
            
            -- Count connected arcs
            (SELECT COUNT(*) FROM network_arc WHERE from_node_id = n.id OR to_node_id = n.id) as connected_arcs
            
        FROM network_node n
        LEFT JOIN network_node_type nt ON nt.id = n.node_type_id
        LEFT JOIN hydrologic_region hr ON hr.id = n.hydrologic_region_id
        LEFT JOIN reservoir_entity re ON re.network_node_id = n.id
        LEFT JOIN source src ON src.id = n.source_id
        WHERE ST_Intersects(n.geom, ST_MakeEnvelope($1, $2, $3, $4, 4326))
        {source_condition}
        AND n.geom IS NOT NULL
        ORDER BY 
            -- Priority: highly connected nodes first
            (SELECT COUNT(*) FROM network_arc WHERE from_node_id = n.id OR to_node_id = n.id) DESC,
            CASE WHEN nt.short_code LIKE 'STR%' THEN 1 ELSE 2 END,
            n.name
        LIMIT $5;
        """

        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query, min_lng, min_lat, max_lng, max_lat, limit)

            nodes = []
            for row in rows:
                # Parse geometry
                geometry = row["geometry"]
                if isinstance(geometry, str):
                    try:
                        geometry = json.loads(geometry)
                    except (json.JSONDecodeError, TypeError):
                        geometry = None

                nodes.append(
                    {
                        "id": row["id"],
                        "short_code": row["short_code"],
                        "name": row["name"],
                        "description": row["description"],
                        "node_type": row["node_type"],
                        "node_type_name": row["node_type_name"],
                        "hydrologic_region": row["hydrologic_region"],
                        "geometry": geometry,
                        "latitude": float(row["latitude"]) if row["latitude"] else None,
                        "longitude": float(row["longitude"])
                        if row["longitude"]
                        else None,
                        "riv_mi": float(row["riv_mi"]) if row["riv_mi"] else None,
                        "riv_name": row["riv_name"],
                        "json_id": row["json_id"],
                        "is_reservoir": row["is_reservoir"],
                        "capacity_taf": float(row["capacity_taf"])
                        if row["capacity_taf"]
                        else None,
                        "operational_purpose": row["operational_purpose"],
                        "associated_river": row["associated_river"],
                        "map_category": row["map_category"],
                        "data_source": row["data_source"],
                        "connected_arcs": row["connected_arcs"],
                        "is_interactive": row["connected_arcs"] > 0,
                    }
                )

            return {
                "nodes": nodes,
                "total": len(nodes),
                "bbox": bbox,
                "limit": limit,
                "source_filter": source_filter,
                "type_filtering": "disabled",
                "enhanced_network": True,
                "note": "All node types included for enhanced network testing",
            }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid bbox format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
