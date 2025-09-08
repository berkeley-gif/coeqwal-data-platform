"""
Network API routes optimized for mapbox app
For network traversal on Mapbox maps optimizing use of the network_topology table
"""

from fastapi import APIRouter, Query, Path, HTTPException
from .network_mapbox import (
    get_network_geojson,
    get_network_nodes_fast,
    traverse_network_geojson, 
    get_network_element_details
)
from .enhanced_network_traversal import (
    enhanced_network_traversal,
    get_enhanced_connectivity_stats,
    get_network_gaps_analysis
)

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
    include_arcs: bool = Query(True, description="Include arc geometries"),
    include_nodes: bool = Query(True, description="Include node geometries"), 
    limit: int = Query(5000, description="Maximum features to return")
):
    """
    Get network features as GeoJSON for Mapbox display
    
    Example: /api/network/geojson?bbox=-122.5,37.5,-122.0,38.0&include_arcs=true&include_nodes=true
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_network_geojson(db_pool, bbox, include_arcs, include_nodes, limit)


@router.get("/nodes/fast")
async def api_get_network_nodes_fast(
    bbox: str = Query(..., description="Bounding box as 'minLng,minLat,maxLng,maxLat'"),
    limit: int = Query(1000, description="Maximum nodes to return")
):
    """
    ULTRA-FAST nodes-only endpoint for initial map loading
    
    Optimized for speed: nodes only, minimal fields, spatial indexing, caching
    Example: /api/network/nodes/fast?bbox=-122.5,37.5,-122.0,38.0&limit=1000
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_network_nodes_fast(db_pool, bbox, limit)


@router.get("/traverse/{short_code}")
async def api_traverse_network_geojson(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(10, description="Maximum traversal depth"),
    include_arcs: bool = Query(True, description="Include connecting arcs in result")
):
    """
    Traverse network from a short_code and return GeoJSON for Mapbox visualization
    
    Example: /api/network/traverse/SAC273?direction=downstream&max_depth=5
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await traverse_network_geojson(db_pool, short_code, direction, max_depth, include_arcs)


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


@router.get("/traverse/{short_code}/enhanced")
async def api_enhanced_network_traversal(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(8, description="Maximum traversal depth"),
    include_arcs: bool = Query(True, description="Include connecting arcs in result")
):
    """
    Enhanced network traversal using multiple connectivity strategies
    
    Uses direct connections + spatial proximity + river mile sequences + naming patterns
    Example: /api/network/traverse/SAC273/enhanced?direction=both&max_depth=8
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await enhanced_network_traversal(db_pool, short_code, direction, max_depth, include_arcs)


@router.get("/connectivity/stats")
async def api_get_connectivity_stats():
    """
    Get comprehensive network connectivity statistics
    
    Example: /api/network/connectivity/stats
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_enhanced_connectivity_stats(db_pool)


@router.get("/connectivity/gaps")
async def api_get_network_gaps():
    """
    Analyze network connectivity gaps and suggest improvements
    
    Example: /api/network/connectivity/gaps
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_network_gaps_analysis(db_pool)
