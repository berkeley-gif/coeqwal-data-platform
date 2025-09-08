"""
Network API routes optimized for mapbox app
Clean version with only working endpoints
"""

from fastapi import APIRouter, Query, Path, HTTPException
import json
from .network_mapbox import (
    get_network_geojson,
    get_network_nodes_fast,
    traverse_network_geojson, 
    get_network_element_details
)
from .simple_network_traversal import simple_network_traversal
from .connectivity_diagnostics import diagnose_connectivity_problems, suggest_connectivity_improvements
from .systematic_network_traversal import systematic_network_traversal

router = APIRouter(prefix="/api/network", tags=["network-mapbox"])

# Global variable to hold db_pool reference, set by main.py
db_pool = None

def set_db_pool(pool):
    """Set the database pool - called from main.py after pool creation"""
    global db_pool
    db_pool = pool


@router.get("/geojson")
async def api_get_network_geojson(
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    include_arcs: bool = Query(True, description="Include arcs in response"),
    include_nodes: bool = Query(True, description="Include nodes in response"),
    limit: int = Query(10000, description="Maximum number of features to return")
):
    """
    Get network elements as GeoJSON within a bounding box
    
    Optimized for Mapbox with spatial indexing and performance enhancements
    Example: /api/network/geojson?bbox=-122.5,37.5,-122.0,38.0&include_nodes=true&include_arcs=true&limit=1000
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_network_geojson(db_pool, bbox, include_arcs, include_nodes, limit)


@router.get("/nodes/fast")
async def api_get_network_nodes_fast(
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    limit: int = Query(5000, description="Maximum number of nodes to return")
):
    """
    Get network nodes optimized for fast initial map loading
    
    Ultra-fast endpoint using ST_X/ST_Y for coordinates, minimal properties
    Example: /api/network/nodes/fast?bbox=-122.5,37.5,-122.0,38.0&limit=1000
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_network_nodes_fast(db_pool, bbox, limit)


@router.get("/traverse/{short_code}")
async def api_traverse_network_geojson(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(3, description="Maximum traversal depth")
):
    """
    Network traversal returning GeoJSON features
    
    Example: /api/network/traverse/SAC273?direction=both&max_depth=3
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await traverse_network_geojson(db_pool, short_code, direction, max_depth)


@router.get("/element/{short_code}")
async def api_get_network_element_details(
    short_code: str = Path(..., description="Short code of network element")
):
    """
    Get detailed information about a specific network element
    
    Example: /api/network/element/SAC273
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_network_element_details(db_pool, short_code)


@router.get("/traverse/{short_code}/simple")
async def api_simple_network_traversal(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(5, description="Maximum traversal depth")
):
    """
    SIMPLE network traversal for map visualization
    
    Clean, straightforward approach using existing from_node/to_node connectivity
    No complex algorithms - just follow the direct connections
    Example: /api/network/traverse/SAC273/simple?direction=both&max_depth=5
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await simple_network_traversal(db_pool, short_code, direction, max_depth)


@router.get("/connectivity/diagnose")
async def api_diagnose_connectivity():
    """
    Diagnose connectivity problems in the network data
    
    Shows actual vs expected connectivity, identifies gaps
    Example: /api/network/connectivity/diagnose
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await diagnose_connectivity_problems(db_pool)


@router.get("/connectivity/suggestions")
async def api_connectivity_suggestions():
    """
    Get suggestions for improving network connectivity
    
    Identifies patterns that could create better connections
    Example: /api/network/connectivity/suggestions
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await suggest_connectivity_improvements(db_pool)


@router.get("/traverse/{short_code}/systematic")
async def api_systematic_network_traversal(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'")
):
    """
    SYSTEMATIC three-pass network traversal
    
    Pass 1: Geopackage-only (most reliable)
    Pass 2: XML with geometry (fill gaps)  
    Pass 3: XML without geometry (logical connections)
    No depth limits - gets complete network
    Example: /api/network/traverse/SAC273/systematic?direction=both
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await systematic_network_traversal(db_pool, short_code, direction)


@router.get("/elements/search")
async def api_search_network_elements(
    element_type: str = Query(None, description="Filter by element type: STR, CH, PS, WTP, etc."),
    sort_by: str = Query("short_code", description="Sort field: capacity_taf, surface_area_acres, short_code, river_name"),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    limit: int = Query(50, description="Maximum number of results"),
    has_geometry: bool = Query(True, description="Only include elements with map geometry"),
    has_capacity: bool = Query(False, description="Only include elements with capacity data")
):
    """
    GENERIC network element search with flexible filtering and sorting
    
    Examples:
    - Top 9 biggest reservoirs: /api/network/elements/search?element_type=STR&sort_by=capacity_taf&sort_order=desc&limit=9&has_capacity=true
    - All pump stations: /api/network/elements/search?element_type=PS&limit=20
    - Largest channels by length: /api/network/elements/search?element_type=CH&sort_by=shape_length&sort_order=desc&limit=10
    - Sacramento River nodes: /api/network/elements/search?river_name=Sacramento%20River&limit=20
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    
    # Build dynamic WHERE conditions
    where_conditions = ["nt.connectivity_status = 'connected'"]
    params = []
    param_count = 0
    
    if element_type:
        param_count += 1
        where_conditions.append(f"nt.type = ${param_count}")
        params.append(element_type)
    
    if has_geometry:
        where_conditions.append("ng.geom IS NOT NULL")
    
    if has_capacity and element_type == "STR":
        where_conditions.append("re.capacity_taf IS NOT NULL")
    
    # Build dynamic ORDER BY
    valid_sort_fields = {
        "short_code": "nt.short_code",
        "capacity_taf": "re.capacity_taf", 
        "surface_area_acres": "re.surface_area_acres",
        "river_name": "nt.river_name",
        "shape_length": "nt.shape_length",
        "river_mile": "nt.river_mile"
    }
    
    sort_field = valid_sort_fields.get(sort_by, "nt.short_code")
    sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"
    
    # Build query with optional joins based on element type
    if element_type == "STR":
        # Include reservoir data
        query = f"""
        SELECT 
            nt.id, nt.short_code, nt.type, nt.subtype, nt.river_name, nt.river_mile,
            re.name as entity_name,
            re.capacity_taf,
            re.dead_pool_taf,
            re.surface_area_acres,
            re.operational_purpose,
            re.associated_river,
            ST_AsGeoJSON(ng.geom) as geometry,
            ng.geometry_type
        FROM network_topology nt
        LEFT JOIN reservoir_entity re ON nt.short_code = re.short_code
        LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
        WHERE {' AND '.join(where_conditions)}
        ORDER BY {sort_field} {sort_direction} NULLS LAST
        LIMIT {limit};
        """
    else:
        # Generic query for other element types
        query = f"""
        SELECT 
            nt.id, nt.short_code, nt.type, nt.subtype, nt.river_name, nt.river_mile,
            nt.arc_name, nt.shape_length,
            ST_AsGeoJSON(ng.geom) as geometry,
            ng.geometry_type
        FROM network_topology nt
        LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
        WHERE {' AND '.join(where_conditions)}
        ORDER BY {sort_field} {sort_direction} NULLS LAST
        LIMIT {limit};
        """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    
    features = []
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if not geometry and has_geometry:
                continue
                
            properties = {
                "id": row['id'],
                "short_code": row['short_code'],
                "type": "node" if row['type'] in ['STR', 'PS', 'WTP', 'WWTP'] else "arc",
                "element_type": row['type'],
                "subtype": row.get('subtype'),
                "river_name": row.get('river_name'),
                "river_mile": float(row['river_mile']) if row.get('river_mile') else None,
                "display_name": row.get('entity_name') or row.get('arc_name') or row['short_code'],
                "rank": len(features) + 1
            }
            
            # Add type-specific properties
            if element_type == "STR" and 'capacity_taf' in row:
                properties.update({
                    "capacity_taf": float(row['capacity_taf']) if row['capacity_taf'] else None,
                    "dead_pool_taf": float(row['dead_pool_taf']) if row['dead_pool_taf'] else None,
                    "surface_area_acres": float(row['surface_area_acres']) if row['surface_area_acres'] else None,
                    "operational_purpose": row.get('operational_purpose'),
                    "associated_river": row.get('associated_river')
                })
            elif 'shape_length' in row:
                properties.update({
                    "arc_name": row.get('arc_name'),
                    "shape_length": float(row['shape_length']) if row.get('shape_length') else None
                })
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing element {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "total_features": len(features),
            "element_type": element_type,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "has_geometry": has_geometry,
            "has_capacity": has_capacity
        }
    }
