"""
COEQWAL API
FastAPI backend for the Collaboratory for Equity in Water Allocation (COEQWAL) project.

This API provides:
- Tier data for scenario outcome visualization (charts and maps)
- CalSim3 network node/arc data with spatial queries
- Scenario file downloads from S3

Documentation: https://api.coeqwal.org/docs
"""

from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import asyncpg
import json
import os
import logging
import time
from datetime import datetime
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Import our endpoints
from routes.nodes_spatial import get_nodes_spatial, get_node_network, get_all_nodes_unfiltered
from routes.network_traversal import get_node_network_unlimited
from routes.tier_endpoints import router as tier_router, set_db_pool as set_tier_db_pool
from routes.tier_map_endpoints import router as tier_map_router, set_db_pool as set_tier_map_db_pool
from routes.scenario_endpoints import router as scenario_router, set_db_pool as set_scenario_db_pool
from routes.download_endpoints import router as download_router
from routes.reservoir_statistics_endpoints import (
    router as reservoir_stats_router,
    set_db_pool as set_reservoir_stats_db_pool
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rate limiter - 200 requests per minute per IP (generous for normal use)
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["200/minute", "20/second"]
)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@host:port/db")

# Database connection pool
db_pool = None

# =============================================================================
# API METADATA & TAGS
# =============================================================================

API_TITLE = "COEQWAL API"
API_VERSION = "2.1.0"
API_DESCRIPTION = """
Data API for the Collaboratory for Equity in Water Allocation (COEQWAL) project.

Enables exploration of how different water management scenarios affect communities, 
agriculture, and ecosystems across California.
"""

TAGS_METADATA = [
    {
        "name": "scenarios",
        "description": "**Scenario definitions and metadata.** Lists scenarios, themes, and key assumptions.",
    },
    {
        "name": "tiers",
        "description": "**Tier definitions and scenario tier data.** Used for charts showing outcome distributions.",
    },
    {
        "name": "tier-map",
        "description": "**Tier map visualization data.** Returns GeoJSON for mapping tier outcomes by location.",
    },
    {
        "name": "network",
        "description": "**CalSim3 water network data.** Nodes (reservoirs, demand units) and arcs (rivers, canals) with spatial queries.",
    },
    {
        "name": "downloads",
        "description": "**Scenario file downloads.** Lists available files and generates presigned S3 URLs.",
    },
    {
        "name": "statistics",
        "description": "**Reservoir statistics.** Monthly percentile data for reservoir storage band charts.",
    },
    {
        "name": "system",
        "description": "**System endpoints.** Health checks and API info.",
    },
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage database connection pool lifecycle"""
    global db_pool
    
    # Startup
    logger.info("Creating database connection pool...")
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=5,     # Always keep 5 connections warm
        max_size=50,    # Scale up to 50 for workshops
        max_queries=50000,
        max_inactive_connection_lifetime=300,
        command_timeout=30
    )
    logger.info(f"Database pool created with {db_pool._queue.qsize()} connections")
    
    # Set the database pool for tier router
    set_tier_db_pool(db_pool)
    
    # Set the database pool for tier map router
    set_tier_map_db_pool(db_pool)
    
    # Set the database pool for scenario router
    set_scenario_db_pool(db_pool)

    # Set the database pool for reservoir statistics router
    set_reservoir_stats_db_pool(db_pool)

    yield
    
    # Shutdown
    logger.info("Closing database connection pool...")
    await db_pool.close()

app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION,
    lifespan=lifespan,
    openapi_tags=TAGS_METADATA,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Rate limiting setup
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Include scenario endpoints
app.include_router(scenario_router)

# Include tier endpoints
app.include_router(tier_router)

# Include tier map endpoints
app.include_router(tier_map_router)

# Include download endpoints (replaces problematic Lambda service)
app.include_router(download_router)

# Include reservoir statistics endpoints
app.include_router(reservoir_stats_router)

# Middleware for performance
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:*",
        "https://coeqwal.org",
        "https://www.coeqwal.org",
        "https://dev.coeqwal.org",
        "https://staging.coeqwal.org",
        "https://scenario-list-main.vercel.app",
        "https://yuya737.github.io",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    # Allow localhost and any Vercel preview deployments
    allow_origin_regex=r"https?://localhost:\d+|https://.*\.vercel\.app",
)

# Dependency for database connections
async def get_db():
    """Get database connection from pool"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    
    async with db_pool.acquire() as connection:
        yield connection

# Performance monitoring middleware
@app.middleware("http")
async def add_performance_headers(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    # Log slow queries
    if process_time > 1.0:
        logger.warning(f"Slow query: {request.url.path} took {process_time:.2f}s")
    
    return response

# Pydantic models
# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class NetworkNode(BaseModel):
    """A CalSim3 network node with spatial data"""
    id: int = Field(..., description="Internal database ID")
    short_code: str = Field(..., description="Unique node identifier (e.g., 'SHSTA' for Shasta)")
    calsim_id: Optional[str] = Field(None, description="CalSim model identifier")
    name: Optional[str] = Field(None, description="Human-readable name")
    description: Optional[str] = Field(None, description="Node description")
    node_type: str = Field(..., description="Node type (Reservoir, Demand, Junction, etc.)")
    latitude: Optional[float] = Field(None, description="Latitude in WGS84")
    longitude: Optional[float] = Field(None, description="Longitude in WGS84")
    hydrologic_region: Optional[str] = Field(None, description="Hydrologic region code (SAC, SJR, etc.)")
    geojson: Dict[str, Any] = Field(..., description="GeoJSON geometry object")
    attributes: Dict[str, Any] = Field(..., description="Additional node attributes")

class NetworkArc(BaseModel):
    """A CalSim3 network arc (river, canal, or pipeline connection)"""
    id: int = Field(..., description="Internal database ID")
    short_code: str = Field(..., description="Unique arc identifier")
    calsim_id: Optional[str] = Field(None, description="CalSim model identifier")
    name: Optional[str] = Field(None, description="Human-readable name")
    description: Optional[str] = Field(None, description="Arc description")
    arc_type: str = Field(..., description="Arc type (River, Canal, Pipeline, etc.)")
    from_node: Optional[str] = Field(None, description="Source node short_code")
    to_node: Optional[str] = Field(None, description="Destination node short_code")
    shape_length: Optional[float] = Field(None, description="Arc length in meters")
    hydrologic_region: Optional[str] = Field(None, description="Hydrologic region code")
    geojson: Dict[str, Any] = Field(..., description="GeoJSON geometry object (LineString)")
    attributes: Dict[str, Any] = Field(..., description="Additional arc attributes")

class ConnectedElement(BaseModel):
    """A network element connected to the origin node"""
    id: int = Field(..., description="Element database ID")
    short_code: str = Field(..., description="Element identifier")
    name: Optional[str] = Field(None, description="Element name")
    element_type: str = Field(..., description="'node' or 'arc'")
    distance: Optional[int] = Field(None, description="Hops from origin node")
    direction: Optional[str] = Field(None, description="'upstream' or 'downstream'")

class NetworkAnalysis(BaseModel):
    """Network traversal analysis results"""
    origin_id: int = Field(..., description="Starting node ID")
    origin_type: str = Field(..., description="'node' or 'arc'")
    upstream_nodes: List[ConnectedElement] = Field(..., description="Nodes upstream of origin")
    downstream_nodes: List[ConnectedElement] = Field(..., description="Nodes downstream of origin")
    connected_arcs: List[ConnectedElement] = Field(..., description="Arcs connected to origin")


# =============================================================================
# SYSTEM ENDPOINTS
# =============================================================================

@app.get("/", tags=["system"], summary="Service Info")
@limiter.limit("100/minute")
async def root(request: Request):
    """
    Service information and documentation links.
    """
    return {
        "service": API_TITLE,
        "version": API_VERSION,
        "description": "Data API for California water management scenario analysis",
        "project": "Collaboratory for Equity in Water Allocation (COEQWAL)",
        "links": {
            "endpoints": "/api",
            "documentation": "/docs",
            "health": "/api/health"
        }
    }

@app.get("/api", tags=["system"], summary="API Reference")
@limiter.limit("100/minute")
async def api_root(request: Request):
    """
    Complete endpoint reference for developers.
    """
    return {
        "api": API_TITLE,
        "version": API_VERSION,
        "base_url": "https://api.coeqwal.org",
        "documentation": "https://api.coeqwal.org/docs",
        "endpoints": {
            "scenarios": {
                "description": "Water management scenario definitions",
                "list": "GET /api/scenarios",
                "detail": "GET /api/scenarios/{scenario_id}",
                "compare": "GET /api/scenarios/{id}/compare/{other_id}"
            },
            "tiers": {
                "description": "Outcome tier data for charts",
                "definitions": "GET /api/tiers/list",
                "scenario_data": "GET /api/tiers/scenarios/{scenario_id}/tiers"
            },
            "tier_map": {
                "description": "GeoJSON tier data for map visualization",
                "geojson": "GET /api/tier-map/{scenario}/{tier}",
                "scenarios": "GET /api/tier-map/scenarios",
                "summary": "GET /api/tier-map/summary/{scenario}"
            },
            "network": {
                "description": "CalSim3 water infrastructure network",
                "nodes": "GET /api/nodes",
                "arcs": "GET /api/arcs",
                "spatial_query": "GET /api/nodes/spatial?bbox={minLng,minLat,maxLng,maxLat}",
                "search": "GET /api/search?q={query}"
            },
            "downloads": {
                "description": "Model output file downloads",
                "list": "GET /api/scenario",
                "download": "GET /api/download?scenario={id}&type={zip|output|sv}"
            },
            "statistics": {
                "description": "Reservoir storage percentile statistics",
                "reservoir_percentiles": "GET /api/statistics/scenarios/{scenario_id}/reservoirs/{reservoir_id}/percentiles",
                "all_reservoirs": "GET /api/statistics/scenarios/{scenario_id}/reservoir-percentiles",
                "list_reservoirs": "GET /api/statistics/reservoirs",
                "list_scenarios": "GET /api/statistics/scenarios"
            }
        },
        "data_summary": {
            "scenarios": 8,
            "tier_indicators": 9,
            "network_nodes": 1400,
            "network_arcs": 1063
        }
    }

# =============================================================================
# NETWORK ENDPOINTS
# =============================================================================

@app.get("/api/nodes", 
         response_model=List[NetworkNode],
         tags=["network"],
         summary="Get network nodes",
         description="Get CalSim3 network nodes with coordinates and attributes. Use for downloading all nodes or filtering by region.")
async def get_all_nodes(
    limit: int = Query(1000, le=10000, description="Maximum nodes to return (max 10,000)"),
    region: Optional[str] = Query(None, description="Filter by hydrologic region: SAC, SJR, TUL, SF, SC, CC, NC"),
    db: asyncpg.Connection = Depends(get_db)
):
    """
    Get CalSim3 network nodes with spatial data and attributes.
    
    **Use cases:**
    - Download all nodes for offline analysis
    - Filter by hydrologic region for focused study
    - Get node coordinates for map visualization
    
    **Example:** `GET /api/nodes?region=SAC&limit=500`
    """
    try:
        # Build query with optional filters
        where_clause = "WHERE n.geom IS NOT NULL"
        params = [limit]
        param_count = 1
        
        if region:
            param_count += 1
            where_clause += f" AND hr.short_code = ${param_count}"
            params.append(region)
        
        query = f"""
        SELECT 
            n.id,
            n.short_code,
            n.calsim_id,
            n.name,
            n.description,
            nt.name as node_type,
            n.latitude,
            n.longitude,
            hr.short_code as hydrologic_region,
            ST_AsGeoJSON(n.geom) as geojson,
            jsonb_build_object(
                'riv_mi', n.riv_mi,
                'riv_name', n.riv_name,
                'comment', n.comment,
                'type', n.type,
                'sub_type', n.sub_type,
                'strm_code', n.strm_code,
                'created_at', n.created_at,
                'updated_at', n.updated_at
            ) as attributes
        FROM network_node n
        LEFT JOIN network_node_type nt ON n.node_type_id = nt.id
        LEFT JOIN hydrologic_region hr ON n.hydrologic_region_id = hr.id
        {where_clause}
        ORDER BY n.id
        LIMIT $1
        """
        
        rows = await db.fetch(query, *params)
        
        nodes = []
        for row in rows:
            geojson = json.loads(row['geojson']) if row['geojson'] else {}
            # Fix: attributes comes as JSON string, need to parse it
            attributes = row['attributes']
            if isinstance(attributes, str):
                attributes = json.loads(attributes)
            elif attributes is None:
                attributes = {}
            
            nodes.append(NetworkNode(
                id=row['id'],
                short_code=row['short_code'],
                calsim_id=row['calsim_id'],
                name=row['name'],
                description=row['description'],
                node_type=row['node_type'] or 'Unknown',
                latitude=row['latitude'],
                longitude=row['longitude'],
                hydrologic_region=row['hydrologic_region'],
                geojson=geojson,
                attributes=attributes
            ))
        
        return nodes
        
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query failed")

@app.get("/api/arcs", 
         response_model=List[NetworkArc],
         tags=["network"],
         summary="Get network arcs", 
         description="Get CalSim3 network arcs (rivers, canals, pipelines) with geometry and attributes.")
async def get_all_arcs(
    limit: int = Query(1000, le=10000, description="Maximum arcs to return (max 10,000)"),
    region: Optional[str] = Query(None, description="Filter by hydrologic region: SAC, SJR, TUL, SF, SC, CC, NC"),
    db: asyncpg.Connection = Depends(get_db)
):
    """
    Get CalSim3 network arcs with spatial data and attributes.
    
    **Arc types include:**
    - Rivers and streams
    - Canals and aqueducts
    - Pipelines
    
    **Example:** `GET /api/arcs?region=SAC&limit=500`
    """
    try:
        # Build query with optional filters
        where_clause = "WHERE a.geom IS NOT NULL"
        params = [limit]
        param_count = 1
        
        if region:
            param_count += 1
            where_clause += f" AND hr.short_code = ${param_count}"
            params.append(region)
        
        query = f"""
        SELECT 
            a.id,
            a.short_code,
            a.calsim_id,
            a.name,
            a.description,
            at.name as arc_type,
            a.from_node,
            a.to_node,
            a.shape_length,
            hr.short_code as hydrologic_region,
            ST_AsGeoJSON(a.geom) as geojson,
            jsonb_build_object(
                'arc_id', a.arc_id,
                'type', a.type,
                'sub_type', a.sub_type,
                'node_suffix_comment', a.node_suffix_comment,
                'is_reversible', a.is_reversible,
                'flow_capacity', a.flow_capacity,
                'created_at', a.created_at,
                'updated_at', a.updated_at
            ) as attributes
        FROM network_arc a
        LEFT JOIN network_arc_type at ON a.arc_type_id = at.id
        LEFT JOIN hydrologic_region hr ON a.hydrologic_region_id = hr.id
        {where_clause}
        ORDER BY a.id
        LIMIT $1
        """
        
        rows = await db.fetch(query, *params)
        
        arcs = []
        for row in rows:
            geojson = json.loads(row['geojson']) if row['geojson'] else {}
            # Fix: attributes comes as JSON string, need to parse it
            attributes = row['attributes']
            if isinstance(attributes, str):
                attributes = json.loads(attributes)
            elif attributes is None:
                attributes = {}
                
            arcs.append(NetworkArc(
                id=row['id'],
                short_code=row['short_code'],
                calsim_id=row['calsim_id'],
                name=row['name'],
                description=row['description'],
                arc_type=row['arc_type'] or 'Unknown',
                from_node=row['from_node'],
                to_node=row['to_node'],
                shape_length=row['shape_length'],
                hydrologic_region=row['hydrologic_region'],
                geojson=geojson,
                attributes=attributes
            ))
        
        return arcs
        
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query failed")

@app.get("/api/nodes/{node_id}/analysis", 
         response_model=NetworkAnalysis,
         tags=["network"],
         summary="Analyze node connections")
async def get_node_analysis(
    node_id: int,
    max_depth: int = Query(3, ge=1, le=10, description="Maximum traversal depth (1-10 hops)"),
    db: asyncpg.Connection = Depends(get_db)
):
    """
    Get network connectivity analysis for a specific node.
    
    Returns upstream nodes, downstream nodes, and connected arcs
    up to the specified depth.
    
    **Example:** `GET /api/nodes/42/analysis?max_depth=5`
    """
    try:
        # Get upstream nodes
        upstream_query = """
        SELECT u.node_id, n.short_code, n.name, u.distance, 'upstream' as direction
        FROM get_upstream_nodes($1, $2) u
        JOIN network_node n ON u.node_id = n.id
        ORDER BY u.distance, n.short_code
        """
        upstream_rows = await db.fetch(upstream_query, node_id, max_depth)
        
        # Get downstream nodes  
        downstream_query = """
        SELECT d.node_id, n.short_code, n.name, d.distance, 'downstream' as direction
        FROM get_downstream_nodes($1, $2) d
        JOIN network_node n ON d.node_id = n.id
        ORDER BY d.distance, n.short_code
        """
        downstream_rows = await db.fetch(downstream_query, node_id, max_depth)
        
        # Get connected arcs
        arcs_query = """
        SELECT c.arc_id, a.short_code, a.name, c.direction, 0 as distance
        FROM get_connected_arcs($1) c
        JOIN network_arc a ON c.arc_id = a.id
        ORDER BY c.direction, a.short_code
        """
        arc_rows = await db.fetch(arcs_query, node_id)
        
        # Build response
        upstream_nodes = [
            ConnectedElement(
                id=row['node_id'],
                short_code=row['short_code'],
                name=row['name'],
                element_type='node',
                distance=row['distance'],
                direction=row['direction']
            ) for row in upstream_rows
        ]
        
        downstream_nodes = [
            ConnectedElement(
                id=row['node_id'],
                short_code=row['short_code'],
                name=row['name'],
                element_type='node',
                distance=row['distance'],
                direction=row['direction']
            ) for row in downstream_rows
        ]
        
        connected_arcs = [
            ConnectedElement(
                id=row['arc_id'],
                short_code=row['short_code'],
                name=row['name'],
                element_type='arc',
                distance=row['distance'],
                direction=row['direction']
            ) for row in arc_rows
        ]
        
        return NetworkAnalysis(
            origin_id=node_id,
            origin_type='node',
            upstream_nodes=upstream_nodes,
            downstream_nodes=downstream_nodes,
            connected_arcs=connected_arcs
        )
        
    except Exception as e:
        logger.error(f"Network analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Network analysis failed")

@app.get("/api/arcs/{arc_id}/analysis", 
         response_model=NetworkAnalysis,
         tags=["network"],
         summary="Analyze arc connections")
async def get_arc_analysis(
    arc_id: int,
    db: asyncpg.Connection = Depends(get_db)
):
    """
    Get network connectivity for a specific arc.
    
    Returns the from_node and to_node endpoints of the arc.
    """
    try:
        # Get arc endpoints and their connections
        query = """
        SELECT 
            a.from_node_id,
            a.to_node_id,
            fn.short_code as from_node_code,
            fn.name as from_node_name,
            tn.short_code as to_node_code,
            tn.name as to_node_name
        FROM network_arc a
        LEFT JOIN network_node fn ON a.from_node_id = fn.id
        LEFT JOIN network_node tn ON a.to_node_id = tn.id
        WHERE a.id = $1
        """
        
        arc_row = await db.fetchrow(query, arc_id)
        if not arc_row:
            raise HTTPException(status_code=404, detail="Arc not found")
        
        connected_nodes = []
        
        # Add from_node if exists
        if arc_row['from_node_id']:
            connected_nodes.append(ConnectedElement(
                id=arc_row['from_node_id'],
                short_code=arc_row['from_node_code'],
                name=arc_row['from_node_name'],
                element_type='node',
                distance=0,
                direction='from'
            ))
        
        # Add to_node if exists
        if arc_row['to_node_id']:
            connected_nodes.append(ConnectedElement(
                id=arc_row['to_node_id'],
                short_code=arc_row['to_node_code'],
                name=arc_row['to_node_name'],
                element_type='node',
                distance=0,
                direction='to'
            ))
        
        return NetworkAnalysis(
            origin_id=arc_id,
            origin_type='arc',
            upstream_nodes=[],  # Could extend to get upstream from from_node
            downstream_nodes=[], # Could extend to get downstream from to_node
            connected_arcs=connected_nodes  # Using this field for connected nodes
        )
        
    except Exception as e:
        logger.error(f"Arc analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Arc analysis failed")

@app.get("/api/search", tags=["network"], summary="Search network elements")
async def search_network(
    q: str = Query(..., min_length=2, description="Search query (min 2 characters)"),
    limit: int = Query(20, le=100, description="Maximum results (max 100)"),
    db: asyncpg.Connection = Depends(get_db)
):
    """
    Search network nodes and arcs by name, short_code, or calsim_id.
    
    **Example:** `GET /api/search?q=shasta&limit=10`
    
    Returns matching nodes and arcs with their IDs and names.
    """
    try:
        search_pattern = f"%{q.lower()}%"
        
        # Search nodes
        node_query = """
        SELECT 'node' as type, id, short_code, name, description
        FROM network_node
        WHERE LOWER(short_code) LIKE $1 
           OR LOWER(name) LIKE $1 
           OR LOWER(calsim_id) LIKE $1
        ORDER BY short_code
        LIMIT $2
        """
        
        # Search arcs
        arc_query = """
        SELECT 'arc' as type, id, short_code, name, description
        FROM network_arc
        WHERE LOWER(short_code) LIKE $1 
           OR LOWER(name) LIKE $1 
           OR LOWER(calsim_id) LIKE $1
        ORDER BY short_code
        LIMIT $2
        """
        
        node_results = await db.fetch(node_query, search_pattern, limit // 2)
        arc_results = await db.fetch(arc_query, search_pattern, limit // 2)
        
        results = []
        for row in node_results:
            results.append({
                "type": row['type'],
                "id": row['id'],
                "short_code": row['short_code'],
                "name": row['name'],
                "description": row['description']
            })
        
        for row in arc_results:
            results.append({
                "type": row['type'],
                "id": row['id'],
                "short_code": row['short_code'],
                "name": row['name'],
                "description": row['description']
            })
        
        return {"results": results, "total": len(results)}
        
    except Exception as e:
        logger.error(f"Search failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Search failed")

# =============================================================================
# SPATIAL QUERY ENDPOINTS
# =============================================================================

@app.get("/api/nodes/spatial", tags=["network"], summary="Spatial node query")
async def api_get_nodes_spatial(
    bbox: str = Query(..., description="Bounding box: 'minLng,minLat,maxLng,maxLat'", 
                      example="-122.5,37.0,-121.0,38.5"),
    zoom: int = Query(10, ge=1, le=20, description="Map zoom level (1-20)"),
    limit: int = Query(1000, le=10000, description="Maximum nodes (max 10,000)")
):
    """
    Get nodes within a bounding box with zoom-based filtering.
    
    Higher zoom levels return more detailed results.
    Lower zoom levels prioritize major infrastructure.
    
    **Example:** `GET /api/nodes/spatial?bbox=-122.5,37.0,-121.0,38.5&zoom=10`
    """
    return await get_nodes_spatial(db_pool, bbox, zoom, limit)

@app.get("/api/nodes/{node_id}/network", tags=["network"], summary="Node network traversal")
async def api_get_node_network(
    node_id: int,
    direction: str = Query("both", description="'upstream', 'downstream', or 'both'"),
    max_depth: int = Query(50, ge=1, le=100, description="Max traversal depth (1-100)"),
    include_arcs: str = Query("true", description="Include arc geometries (true/false)")
):
    """
    Traverse the water network from a starting node.
    
    Returns connected nodes and optionally arc geometries for visualization.
    
    **Example:** `GET /api/nodes/42/network?direction=downstream&max_depth=20`
    """
    return await get_node_network(db_pool, node_id, direction, max_depth, include_arcs)

@app.get("/api/nodes/unfiltered", tags=["network"], summary="All nodes in bbox (unfiltered)")
async def api_get_all_nodes_unfiltered(
    bbox: str = Query(..., description="Bounding box: 'minLng,minLat,maxLng,maxLat'"),
    limit: int = Query(10000, le=50000, description="Maximum nodes (max 50,000)"),
    source_filter: str = Query("all", description="'geopackage', 'network_schematic', or 'all'")
):
    """
    Get ALL nodes within bounding box without zoom filtering.
    
    Use for complete network data export or detailed analysis.
    Warning: May return large datasets.
    """
    return await get_all_nodes_unfiltered(db_pool, bbox, limit, source_filter)

@app.get("/api/nodes/{node_id}/network/unlimited", tags=["network"], summary="Full network traversal")
async def api_get_node_network_unlimited(
    node_id: int,
    direction: str = Query("both", description="'upstream', 'downstream', or 'both'"),
    include_arcs: str = Query("true", description="Include arc geometries (true/false)")
):
    """
    Get complete upstream/downstream network with no depth limit.
    
    Warning: May return very large networks for major nodes.
    """
    return await get_node_network_unlimited(db_pool, node_id, direction, include_arcs)

# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/api/health", tags=["system"], summary="Health check")
@limiter.limit("60/minute")
async def health_check(request: Request):
    """
    Check API and database health.
    
    Returns status, timestamp, and database connection state.
    Used by monitoring systems and load balancers.
    """
    try:
        async with db_pool.acquire() as db:
            await db.fetchval("SELECT 1")
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "disconnected",
            "error": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
