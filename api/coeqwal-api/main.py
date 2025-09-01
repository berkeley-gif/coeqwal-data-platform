"""
COEQWAL API
FastAPI backend
With connection pooling and workshop optimization
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import asyncpg
import json
import os
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@host:port/db")

# Database connection pool
db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage database connection pool lifecycle"""
    global db_pool
    
    # Startup
    logger.info("Creating database connection pool...")
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=5,      # Always keep 5 connections warm
        max_size=20,     # Scale up to 20 for workshops
        max_queries=50000,
        max_inactive_connection_lifetime=300,
        command_timeout=30
    )
    logger.info(f"Database pool created with {db_pool._queue.qsize()} connections")
    
    yield
    
    # Shutdown
    logger.info("Closing database connection pool...")
    await db_pool.close()

app = FastAPI(
    title="COEQWAL Network API",
    description="COEQWAL Production API",
    version="2.0.0",
    lifespan=lifespan
)

# Middleware for good performance
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://localhost:3003",
        "http://localhost:3004",
        "https://dev.coeqwal.org",
        "https://staging.coeqwal.org",
        "https://coeqwal.org",
        "https://*.coeqwal.org"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
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
class NetworkNode(BaseModel):
    id: int
    short_code: str
    calsim_id: Optional[str]
    name: Optional[str]
    description: Optional[str]
    node_type: str
    latitude: Optional[float]
    longitude: Optional[float]
    hydrologic_region: Optional[str]
    geojson: Dict[str, Any]
    attributes: Dict[str, Any]

class NetworkArc(BaseModel):
    id: int
    short_code: str
    calsim_id: Optional[str]
    name: Optional[str]
    description: Optional[str]
    arc_type: str
    from_node: Optional[str]
    to_node: Optional[str]
    shape_length: Optional[float]
    hydrologic_region: Optional[str]
    geojson: Dict[str, Any]
    attributes: Dict[str, Any]

class ConnectedElement(BaseModel):
    id: int
    short_code: str
    name: Optional[str]
    element_type: str  # 'node' or 'arc'
    distance: Optional[int]  # hops from origin
    direction: Optional[str]  # 'upstream' or 'downstream'

class NetworkAnalysis(BaseModel):
    origin_id: int
    origin_type: str
    upstream_nodes: List[ConnectedElement]
    downstream_nodes: List[ConnectedElement]
    connected_arcs: List[ConnectedElement]

# Database connection
async def get_db_connection():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/")
async def root():
    return {
        "message": "COEQWAL API",
        "version": "1.0.0",
        "endpoints": {
            "nodes": "/api/nodes",
            "arcs": "/api/arcs", 
            "node_analysis": "/api/nodes/{node_id}/analysis",
            "arc_analysis": "/api/arcs/{arc_id}/analysis",
            "search": "/api/search"
        }
    }

@app.get("/api/nodes", response_model=List[NetworkNode])
async def get_all_nodes(
    limit: int = Query(1000, le=10000, description="Maximum number of nodes to return"),
    region: Optional[str] = Query(None, description="Filter by hydrologic region"),
    db: asyncpg.Connection = Depends(get_db)
):
    """Get all network nodes with their spatial data for Mapbox visualization"""
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
        
        rows = await conn.fetch(query, *params)
        
        nodes = []
        for row in rows:
            geojson = json.loads(row['geojson']) if row['geojson'] else {}
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
                attributes=row['attributes'] or {}
            ))
        
        return nodes
        
    finally:
        await conn.close()

@app.get("/api/arcs", response_model=List[NetworkArc])
async def get_all_arcs(
    limit: int = Query(1000, le=10000, description="Maximum number of arcs to return"),
    region: Optional[str] = Query(None, description="Filter by hydrologic region"),
    db: asyncpg.Connection = Depends(get_db)
):
    """Get all network arcs with their spatial data for Mapbox visualization"""
    
    conn = await get_db_connection()
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
        
        rows = await conn.fetch(query, *params)
        
        arcs = []
        for row in rows:
            geojson = json.loads(row['geojson']) if row['geojson'] else {}
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
                attributes=row['attributes'] or {}
            ))
        
        return arcs
        
    finally:
        await conn.close()

@app.get("/api/nodes/{node_id}/analysis", response_model=NetworkAnalysis)
async def get_node_analysis(
    node_id: int,
    max_depth: int = Query(3, description="Maximum depth for network traversal")
):
    """Get network analysis for a specific node (upstream/downstream connections)"""
    
    conn = await get_db_connection()
    try:
        # Get upstream nodes
        upstream_query = """
        SELECT node_id, short_code, name, distance, 'upstream' as direction
        FROM get_upstream_nodes($1, $2) u
        JOIN network_node n ON u.node_id = n.id
        ORDER BY distance, short_code
        """
        upstream_rows = await conn.fetch(upstream_query, node_id, max_depth)
        
        # Get downstream nodes  
        downstream_query = """
        SELECT node_id, short_code, name, distance, 'downstream' as direction
        FROM get_downstream_nodes($1, $2) d
        JOIN network_node n ON d.node_id = n.id
        ORDER BY distance, short_code
        """
        downstream_rows = await conn.fetch(downstream_query, node_id, max_depth)
        
        # Get connected arcs
        arcs_query = """
        SELECT arc_id, short_code, name, direction, 0 as distance
        FROM get_connected_arcs($1) c
        JOIN network_arc a ON c.arc_id = a.id
        ORDER BY direction, short_code
        """
        arc_rows = await conn.fetch(arcs_query, node_id)
        
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
        
    finally:
        await conn.close()

@app.get("/api/arcs/{arc_id}/analysis", response_model=NetworkAnalysis)
async def get_arc_analysis(arc_id: int):
    """Get network analysis for a specific arc (connected nodes)"""
    
    conn = await get_db_connection()
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
        
        arc_row = await conn.fetchrow(query, arc_id)
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
            upstream_nodes=[],  # Extend to get upstream from from_node
            downstream_nodes=[], # Extend to get downstream from to_node
            connected_arcs=connected_nodes  # Field for connected nodes
        )
        
    finally:
        await conn.close()

@app.get("/api/search")
async def search_network(
    q: str = Query(..., description="Search query for nodes/arcs by name or code"),
    limit: int = Query(20, description="Maximum results to return")
):
    """Search network elements by name or code"""
    
    conn = await get_db_connection()
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
        
        node_results = await conn.fetch(node_query, search_pattern, limit // 2)
        arc_results = await conn.fetch(arc_query, search_pattern, limit // 2)
        
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
        
    finally:
        await conn.close()

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = await get_db_connection()
        await conn.fetchval("SELECT 1")
        await conn.close()
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

# === EXPANDABLE ENDPOINTS FOR FUTURE DATA ===

@app.get("/api/scenarios")
async def get_scenarios(
    limit: int = Query(100, le=1000),
    theme_id: Optional[int] = None,
    db: asyncpg.Connection = Depends(get_db)
):
    """Get scenario data - expandable for future scenario analysis"""
    # Placeholder for when you add scenario tables
    return {"message": "Scenarios endpoint ready for expansion", "limit": limit}

@app.get("/api/variables")
async def get_variables(
    limit: int = Query(1000, le=50000),
    entity_type: Optional[str] = None,
    variable_type: Optional[str] = None,
    db: asyncpg.Connection = Depends(get_db)
):
    """Get variable data - ready for large datasets"""
    # Placeholder for when you add Ring 2/3 data
    return {"message": "Variables endpoint ready for GB-scale data", "limit": limit}

@app.get("/api/timeseries/{variable_id}")
async def get_timeseries(
    variable_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(10000, le=100000),
    db: asyncpg.Connection = Depends(get_db)
):
    """Get time series data - optimized for large datasets"""
    # Placeholder for time series data
    return {
        "variable_id": variable_id,
        "message": "Time series endpoint ready for large data volumes",
        "limit": limit
    }

@app.get("/api/bulk/export")
async def bulk_export(
    data_type: str = Query(..., description="Type of data to export"),
    format: str = Query("geojson", description="Export format: geojson, csv, parquet"),
    db: asyncpg.Connection = Depends(get_db)
):
    """Bulk data export for large datasets"""
    # Future: Stream large datasets efficiently
    return {
        "message": "Bulk export endpoint ready for GB-scale data",
        "data_type": data_type,
        "format": format
    }

# === PERFORMANCE ENDPOINTS ===

@app.get("/api/performance/stats")
async def get_performance_stats(db: asyncpg.Connection = Depends(get_db)):
    """Get API performance statistics"""
    try:
        # Database performance metrics
        db_stats = await db.fetchrow("""
            SELECT 
                COUNT(*) as total_nodes,
                COUNT(CASE WHEN geom IS NOT NULL THEN 1 END) as nodes_with_geometry,
                (SELECT COUNT(*) FROM network_arc) as total_arcs,
                (SELECT COUNT(*) FROM network_arc WHERE geom IS NOT NULL) as arcs_with_geometry
            FROM network_node
        """)
        
        return {
            "database_stats": dict(db_stats),
            "api_version": "2.0.0",
            "max_nodes_per_request": 10000,
            "max_arcs_per_request": 10000,
            "connection_pool": {
                "size": db_pool.get_size() if db_pool else 0,
                "available": db_pool.get_size() - db_pool.get_busy_count() if db_pool else 0
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
