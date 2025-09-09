"""
Clean network endpoints for the geopackage Ring 1 foundation
Optimized for fast downloads and connectivity
"""

from fastapi import APIRouter, Query, Path, HTTPException
import json
from .geopackage_network_api import (
    geopackage_network_traversal,
    fast_geopackage_geojson
)
from .water_trail_api import (
    get_water_trail_from_reservoir,
    get_major_reservoir_trails
)

router = APIRouter(prefix="/api/network", tags=["clean-geopackage-network"])

# Global variable to hold db_pool reference, set by main.py
db_pool = None

def set_db_pool(pool):
    """Set the database pool - called from main.py after pool creation"""
    global db_pool
    db_pool = pool


@router.get("/geojson/fast")
async def api_fast_geopackage_geojson(
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    include_arcs: bool = Query(True, description="Include arcs in response"),
    include_nodes: bool = Query(True, description="Include nodes in response"),
    limit: int = Query(5000, description="Maximum number of features to return")
):
    """
    Hopefully-fast GeoJSON endpoint for thousands of elements
    
    Optimized for the clean geopackage Ring 1 foundation
    Uses ST_X/ST_Y for hopefully-fast coordinate extraction
    Example: /api/network/geojson/fast?bbox=-122.5,37.5,-122.0,38.0&limit=2000
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await fast_geopackage_geojson(db_pool, bbox, include_arcs, include_nodes, limit)


@router.get("/traverse/{short_code}/geopackage")
async def api_geopackage_network_traversal(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(10, description="Maximum traversal depth")
):
    """
    Clean geopackage network traversal
    Example: /api/network/traverse/FOLSM/geopackage?direction=both&max_depth=10
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await geopackage_network_traversal(db_pool, short_code, direction, max_depth)


@router.get("/reservoirs/top9")
async def api_get_top_9_reservoirs():
    """
    Custom api for RT meeting: Get the top 9 biggest reservoirs by capacity
    
    Uses the clean geopackage foundation - all 68 reservoirs have geometry
    Example: /api/network/reservoirs/top9
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    
    query = """
    SELECT 
        nt.id, nt.short_code, nt.type, nt.river_name,
        re.name as reservoir_name,
        re.capacity_taf,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    JOIN reservoir_entity re ON nt.short_code = re.short_code
    JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.type = 'STR'  -- Reservoirs
    AND nt.is_active = true
    AND re.capacity_taf IS NOT NULL
    ORDER BY re.capacity_taf DESC NULLS LAST
    LIMIT 9;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    features = []
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if geometry:
                features.append({
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "id": row['id'],
                        "short_code": row['short_code'],
                        "type": "node",
                        "element_type": "STR",
                        "reservoir_name": row['reservoir_name'],
                        "capacity_taf": float(row['capacity_taf']) if row['capacity_taf'] else None,
                        "river_name": row['river_name'],
                        "display_name": row['reservoir_name'] or row['short_code'],
                        "rank": len(features) + 1
                    }
                })
        except Exception as e:
            print(f"Error processing reservoir {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "total_reservoirs": len(features),
            "ranking_criteria": "capacity_taf_descending",
            "foundation": "clean_geopackage_ring1"
        }
    }


@router.get("/elements/search")
async def api_search_network_elements(
    element_type: str = Query(None, description="Filter by element type: STR, CH, PS, WTP, etc."),
    sort_by: str = Query("short_code", description="Sort field: capacity_taf, river_mile, short_code"),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    limit: int = Query(50, description="Maximum number of results"),
    has_geometry: bool = Query(True, description="Only include elements with map geometry"),
    active_only: bool = Query(True, description="Only include active elements")
):
    """
    Generic network element search optimized for geopackage foundation
    
    Example: /api/network/elements/search?element_type=STR&sort_by=capacity_taf&limit=9
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    
    # Build WHERE conditions
    where_conditions = []
    params = []
    param_count = 0
    
    if active_only:
        where_conditions.append("nt.is_active = true")
    
    if element_type:
        param_count += 1
        where_conditions.append(f"nt.type = ${param_count}")
        params.append(element_type)
    
    if has_geometry:
        where_conditions.append("ng.geom IS NOT NULL")
    
    # Build ORDER BY
    valid_sort_fields = {
        "short_code": "nt.short_code",
        "capacity_taf": "re.capacity_taf",
        "river_mile": "nt.river_mile"
    }
    
    sort_field = valid_sort_fields.get(sort_by, "nt.short_code")
    sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"
    
    # Query with optional reservoir join for capacity data
    query = f"""
    SELECT 
        nt.id, nt.short_code, nt.type, nt.sub_type, 
        nt.river_name, nt.river_mile,
        re.name as entity_name,
        re.capacity_taf,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN reservoir_entity re ON nt.short_code = re.short_code AND nt.type = 'STR'
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE {' AND '.join(where_conditions) if where_conditions else 'true'}
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
                "subtype": row['sub_type'],
                "river_name": row['river_name'],
                "river_mile": float(row['river_mile']) if row['river_mile'] else None,
                "display_name": row['entity_name'] or row['river_name'] or row['short_code'],
                "rank": len(features) + 1
            }
            
            # Add reservoir-specific data
            if row['type'] == 'STR' and row['capacity_taf']:
                properties["capacity_taf"] = float(row['capacity_taf'])
            
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
            "foundation": "clean_geopackage_ring1"
        }
    }


@router.get("/trail/{reservoir_short_code}")
async def api_get_water_trail(
    reservoir_short_code: str = Path(..., description="Short code of reservoir to start trail from"),
    trail_type: str = Query("infrastructure", description="Trail type: infrastructure, river_system, treatment_chain"),
    max_depth: int = Query(6, description="Maximum trail depth")
):
    """
    WATER TRAIL - Connect the dots approach for legible network visualization
    
    Instead of showing all 2,930 nodes (visual blob), shows curated water trail
    Trail types:
    - infrastructure: STR, PS, WTP, WWTP + major river junctions (recommended)
    - river_system: Follow river miles and named waterways
    - treatment_chain: Focus on water treatment pathway
    
    Example: /api/network/trail/FOLSM?trail_type=infrastructure&max_depth=6
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_water_trail_from_reservoir(db_pool, reservoir_short_code, trail_type, max_depth)


@router.get("/trails/overview")
async def api_get_major_reservoir_trails(
    trail_type: str = Query("infrastructure", description="Trail type: infrastructure, river_system, treatment_chain")
):
    """
    CALIFORNIA WATER TRAILS OVERVIEW
    
    Shows trails for all major reservoirs - gives overview of California water system
    Much more legible than showing all nodes - focuses on key infrastructure pathways
    
    Example: /api/network/trails/overview?trail_type=infrastructure
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_major_reservoir_trails(db_pool, trail_type)
