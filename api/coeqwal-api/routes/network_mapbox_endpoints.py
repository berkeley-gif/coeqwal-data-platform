"""
Network API routes optimized for mapbox app
Clean version with only working endpoints
"""

from fastapi import APIRouter, Query, Path, HTTPException
from .network_mapbox import (
    get_network_geojson,
    get_network_nodes_fast,
    traverse_network_geojson, 
    get_network_element_details
)
from .simple_network_traversal import simple_network_traversal
from .connectivity_diagnostics import diagnose_connectivity_problems, suggest_connectivity_improvements
from .intelligent_network_traversal import intelligent_network_traversal
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


@router.get("/traverse/{short_code}/intelligent")
async def api_intelligent_network_traversal(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(8, description="Maximum traversal depth")
):
    """
    INTELLIGENT network traversal using multiple strategies
    
    Combines direct connections + naming patterns + river sequences + proximity
    Much better connectivity than simple approach
    Example: /api/network/traverse/SAC273/intelligent?direction=both&max_depth=8
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await intelligent_network_traversal(db_pool, short_code, direction, max_depth)


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
