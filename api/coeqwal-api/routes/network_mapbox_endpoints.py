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
from .advanced_connectivity_strategies import (
    advanced_network_traversal,
    get_connectivity_diagnostics
)
from .logical_connectivity_strategies import (
    logical_network_traversal,
    get_logical_connectivity_stats
)
from .graph_based_connectivity import (
    graph_based_network_traversal,
    xml_first_with_physical_priority
)
from .simple_network_traversal import simple_network_traversal

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


@router.get("/traverse/{short_code}/advanced")
async def api_advanced_network_traversal(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(10, description="Maximum traversal depth"),
    include_arcs: bool = Query(True, description="Include connecting arcs in result")
):
    """
    ADVANCED network traversal using 6 comprehensive connectivity strategies
    
    Strategies: direct connections, CalSim patterns, river miles, stream codes, infrastructure, spatial
    Example: /api/network/traverse/SAC273/advanced?direction=both&max_depth=10
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await advanced_network_traversal(db_pool, short_code, direction, max_depth, include_arcs)


@router.get("/connectivity/diagnostics/{short_code}")
async def api_get_connectivity_diagnostics(
    short_code: str = Path(..., description="Short code of network element to diagnose")
):
    """
    Get comprehensive connectivity diagnostics for a specific element
    
    Shows direct, pattern, river, and spatial connection counts
    Example: /api/network/connectivity/diagnostics/SAC273
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_connectivity_diagnostics(db_pool, short_code)


@router.get("/traverse/{short_code}/logical")
async def api_logical_network_traversal(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(10, description="Maximum traversal depth"),
    include_arcs: bool = Query(True, description="Include connecting arcs in result")
):
    """
    LOGICAL network traversal focusing on meaningful water system connections
    
    Avoids spatial proximity, focuses on: direct connections, CalSim patterns, 
    river flow logic, infrastructure relationships, water balance areas
    Example: /api/network/traverse/SAC273/logical?direction=both&max_depth=10
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await logical_network_traversal(db_pool, short_code, direction, max_depth, include_arcs)


@router.get("/connectivity/logical/stats")
async def api_get_logical_connectivity_stats():
    """
    Get logical connectivity statistics (non-spatial analysis)
    
    Example: /api/network/connectivity/logical/stats
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await get_logical_connectivity_stats(db_pool)


@router.get("/traverse/{short_code}/graph")
async def api_graph_based_network_traversal(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(10, description="Maximum traversal depth"),
    include_arcs: bool = Query(True, description="Include connecting arcs in result")
):
    """
    MATHEMATICAL graph-based network traversal using graph theory
    
    Clean systematic approach: builds adjacency graph, uses BFS traversal
    Example: /api/network/traverse/SAC273/graph?direction=both&max_depth=10
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await graph_based_network_traversal(db_pool, short_code, direction, max_depth, include_arcs)


@router.get("/traverse/{short_code}/xml-comprehensive")
async def api_xml_first_with_physical_priority(
    short_code: str = Path(..., description="Short code of network element to start traversal from"),
    direction: str = Query("both", description="Direction: 'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(8, description="Maximum traversal depth")
):
    """
    XML-COMPREHENSIVE connectivity strategy (CORRECTED APPROACH)
    
    XML provides comprehensive connectivity (~6,000 elements), geopackage provides physical attributes
    This addresses the core issue: geopackage has incomplete connectivity, XML fills the gaps
    Example: /api/network/traverse/SAC273/xml-comprehensive?direction=both&max_depth=8
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return await xml_first_with_physical_priority(db_pool, short_code, direction, max_depth)


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
