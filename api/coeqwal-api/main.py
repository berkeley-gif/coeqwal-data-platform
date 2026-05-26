"""
coeqwal-api/main.py - COEQWAL API
FastAPI backend for the Collaboratory for Equity in Water Allocation (COEQWAL) project.

This API provides:
- Tier data for scenario outcome visualization (charts and maps)
- CalSim3 network node and arc lists for offline analysis
- Per-scenario water system statistics (reservoir, CWS, AG, env-flow, refuge, delta)

Geometry policy: per [database/README.md](../../database/README.md#api-conventions-geometry),
no scenario or location geometry flows through this API. Polygon and point geometry for
map rendering is served via Mapbox vector tiles. The `/api/nodes` and `/api/arcs` list
endpoints are the only routes that still return GeoJSON, and exist for bulk offline use.

Scenario model run file downloads are served by a separate AWS Lambda gateway, not this API.

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
from routes.tier_endpoints import router as tier_router, set_db_pool as set_tier_db_pool
from routes.scenario_endpoints import (
    router as scenario_router,
    set_db_pool as set_scenario_db_pool,
)
from routes.reservoir_statistics_endpoints import (
    router as reservoir_stats_router,
    set_db_pool as set_reservoir_stats_db_pool,
)
from routes.mi_contractor_endpoints import (
    router as mi_contractor_router,
    set_db_pool as set_mi_contractor_db_pool,
)
from routes.demand_unit_endpoints import (
    router as demand_unit_router,
    set_db_pool as set_demand_unit_db_pool,
)
from routes.ag_endpoints import (
    router as ag_router,
    set_db_pool as set_ag_db_pool,
)
from routes.cws_aggregate_endpoints import (
    router as cws_aggregate_router,
    set_db_pool as set_cws_aggregate_db_pool,
)
from routes.batch_statistics_endpoints import (
    router as batch_stats_router,
    set_db_pool as set_batch_stats_db_pool,
)
from routes.refuge_endpoints import (
    router as refuge_router,
    set_db_pool as set_refuge_db_pool,
)
from routes.env_flow_endpoints import (
    router as env_flow_router,
    set_db_pool as set_env_flow_db_pool,
)
from routes.delta_endpoints import (
    router as delta_router,
    set_db_pool as set_delta_db_pool,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rate limiter - 200 requests per minute per IP (generous for normal use)
limiter = Limiter(
    key_func=get_remote_address, default_limits=["200/minute", "20/second"]
)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@host:port/db")

# Database connection pool
db_pool = None

# =============================================================================
# API METADATA & TAGS
# =============================================================================

API_TITLE = "COEQWAL API"
API_VERSION = "2.1.1"
API_DESCRIPTION = """
Data API for the Collaboratory for Equity in Water Allocation (COEQWAL) project. Enables exploration of how different water management scenarios affect communities, agriculture, and ecosystems across California.

The official `coeqwal-website` consumes these endpoints via the `@repo/data` package, not raw `fetch()`. See the [API README](https://github.com/berkeley-gif/coeqwal-data-platform/blob/main/api/coeqwal-api/README.md#how-coeqwal-website-consumes-this-api) for the integration pattern.
"""

TAGS_METADATA = [
    {
        "name": "scenarios",
        "description": "**Scenario definitions and metadata.** Lists scenarios, themes, and key assumptions.",
    },
    {
        "name": "tiers",
        "description": "**Tier definitions, scenario tier scores, and per-location tier assignments.** No geometry is served. The map joins `location_id` to Mapbox vector tile features.",
    },
    {
        "name": "network",
        "description": "**CalSim3 water network data.** Bulk node (point) and arc (line) lists.",
    },
    {
        "name": "statistics",
        "description": "**Statistics from scenario model run data.**",
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
        min_size=5,  # Always keep 5 connections warm
        max_size=50,  # Scale up to 50 for workshops
        max_queries=50000,
        max_inactive_connection_lifetime=300,
        command_timeout=30,
    )
    logger.info(f"Database pool created with {db_pool._queue.qsize()} connections")

    # Set the database pool for tier router
    set_tier_db_pool(db_pool)

    # Set the database pool for scenario router
    set_scenario_db_pool(db_pool)

    # Set the database pool for reservoir statistics router
    set_reservoir_stats_db_pool(db_pool)

    set_mi_contractor_db_pool(db_pool)
    set_demand_unit_db_pool(db_pool)
    set_ag_db_pool(db_pool)
    set_cws_aggregate_db_pool(db_pool)
    set_batch_stats_db_pool(db_pool)
    set_refuge_db_pool(db_pool)
    set_env_flow_db_pool(db_pool)
    set_delta_db_pool(db_pool)

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

# Include reservoir statistics endpoints
app.include_router(reservoir_stats_router)

# M&I contractor statistics router
app.include_router(mi_contractor_router)

# Demand unit statistics router
app.include_router(demand_unit_router)

# Agricultural statistics router
app.include_router(ag_router)

# CWS aggregate statistics router (SWP/CVP/MWD project-level rollups)
app.include_router(cws_aggregate_router)

# Batch statistics router (for Data Explorer performance)
app.include_router(batch_stats_router)

# Wildlife refuge statistics router
app.include_router(refuge_router)

# Environmental river flows statistics router
app.include_router(env_flow_router)

# Delta statistics router (X2, salinity, outflow)
app.include_router(delta_router)

# Middleware for performance
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://coeqwal.org",
        "https://www.coeqwal.org",
        "https://dev.coeqwal.org",
        "https://staging.coeqwal.org",
        "https://yuya737.github.io",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
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
    short_code: str = Field(
        ..., description="Unique node identifier (e.g., 'SHSTA' for Shasta)"
    )
    calsim_id: Optional[str] = Field(None, description="CalSim model identifier")
    name: Optional[str] = Field(None, description="Human-readable name")
    description: Optional[str] = Field(None, description="Node description")
    node_type: str = Field(
        ..., description="Node type (Reservoir, Demand, Junction, etc.)"
    )
    latitude: Optional[float] = Field(None, description="Latitude in WGS84")
    longitude: Optional[float] = Field(None, description="Longitude in WGS84")
    hydrologic_region: Optional[str] = Field(
        None, description="Hydrologic region code (SAC, SJR, etc.)"
    )
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
    geojson: Dict[str, Any] = Field(
        ..., description="GeoJSON geometry object (LineString)"
    )
    attributes: Dict[str, Any] = Field(..., description="Additional arc attributes")


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
            "health": "/api/health",
        },
    }


@app.get("/api", tags=["system"], summary="API Reference")
@limiter.limit("100/minute")
async def api_root(
    request: Request,
    db: asyncpg.Connection = Depends(get_db),
):
    """
    Complete endpoint reference for developers.

    `data_summary` counts are computed live from the database on each
    request (cached 5 minutes at the CDN via Cache-Control). They reflect
    only `is_active = TRUE` rows for scenarios and tier indicators, to
    match the listing endpoints.
    """
    # Live counts so the summary never drifts from reality. Four small
    # COUNT(*) queries against tables we already index; in practice this
    # adds ~5ms to a request that's cached for 5 minutes anyway.
    scenarios_count = await db.fetchval(
        "SELECT COUNT(*) FROM scenario WHERE is_active = TRUE"
    )
    sibling_groups_count = await db.fetchval(
        "SELECT COUNT(DISTINCT hydroclimate_sibling) FROM scenario WHERE is_active = TRUE"
    )
    tier_indicators_count = await db.fetchval(
        "SELECT COUNT(*) FROM tier_definition WHERE is_active = TRUE"
    )
    network_nodes_count = await db.fetchval("SELECT COUNT(*) FROM network_node")
    network_arcs_count = await db.fetchval("SELECT COUNT(*) FROM network_arc")

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
            },
            "tiers": {
                "description": "Outcome tier data for charts, dashboards, and the map. The `locations_batch` endpoint returns per-location tier assignments without geometry. Polygon and point geometry comes from Mapbox vector tiles.",
                "list": "GET /api/tiers/list",
                "definitions": "GET /api/tiers/definitions",
                "scenario_data": "GET /api/tiers/scenarios/{scenario_id}/tiers",
                "batch": "GET /api/tiers/batch?scenarios={id1},{id2},...",
                "locations_batch": "GET /api/tiers/scenarios/{scenario_id}/locations?codes={code1},{code2},...",
            },
            "network": {
                "description": "CalSim3 water infrastructure network. Bulk node and arc lists for offline analysis, downloads, and ETL re-exports.",
                "nodes": "GET /api/nodes",
                "arcs": "GET /api/arcs",
            },
            "statistics": {
                "description": "Per-scenario statistics for band charts and dashboards",
                "reservoir_percentiles": "GET /api/statistics/scenarios/{scenario_id}/reservoirs/{reservoir_id}/percentiles",
                "all_reservoirs": "GET /api/statistics/scenarios/{scenario_id}/reservoir-percentiles",
                "spill_monthly": "GET /api/statistics/scenarios/{scenario_id}/spill-monthly",
                "batch": "GET /api/statistics/batch?scenarios={s1},{s2}&types=storage,cws,ag,env_flow",
                "ag_aggregates_list": "GET /api/statistics/ag-aggregates",
                "ag_aggregates_monthly": "GET /api/statistics/scenarios/{scenario_id}/ag-aggregates/monthly",
                "ag_aggregates_period": "GET /api/statistics/scenarios/{scenario_id}/ag-aggregates/period-summary",
                "cws_aggregates_list": "GET /api/statistics/cws-aggregates",
                "cws_aggregates_monthly": "GET /api/statistics/scenarios/{scenario_id}/cws-aggregates/monthly",
                "cws_aggregates_period": "GET /api/statistics/scenarios/{scenario_id}/cws-aggregates/period-summary",
                "mi_contractors_list": "GET /api/statistics/mi-contractors",
                "mi_contractors_monthly": "GET /api/statistics/scenarios/{scenario_id}/mi-contractors/monthly",
                "mi_contractors_period": "GET /api/statistics/scenarios/{scenario_id}/mi-contractors/period-summary",
            },
        },
        "data_summary": {
            "scenarios": scenarios_count,
            "sibling_groups": sibling_groups_count,
            "tier_indicators": tier_indicators_count,
            "network_nodes": network_nodes_count,
            "network_arcs": network_arcs_count,
        },
    }


# =============================================================================
# NETWORK ENDPOINTS
# =============================================================================


@app.get(
    "/api/nodes",
    response_model=List[NetworkNode],
    tags=["network"],
    summary="Get network nodes",
    description="Get CalSim3 network nodes with coordinates and attributes. Use for downloading all nodes or filtering by region.",
)
async def get_all_nodes(
    limit: int = Query(
        1000, le=10000, description="Maximum nodes to return (max 10,000)"
    ),
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region: SAC, SJR, TUL, SF, SC, CC, NC"
    ),
    db: asyncpg.Connection = Depends(get_db),
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
            geojson = json.loads(row["geojson"]) if row["geojson"] else {}
            # Fix: attributes comes as JSON string, need to parse it
            attributes = row["attributes"]
            if isinstance(attributes, str):
                attributes = json.loads(attributes)
            elif attributes is None:
                attributes = {}

            nodes.append(
                NetworkNode(
                    id=row["id"],
                    short_code=row["short_code"],
                    calsim_id=row["calsim_id"],
                    name=row["name"],
                    description=row["description"],
                    node_type=row["node_type"] or "Unknown",
                    latitude=row["latitude"],
                    longitude=row["longitude"],
                    hydrologic_region=row["hydrologic_region"],
                    geojson=geojson,
                    attributes=attributes,
                )
            )

        return nodes

    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query failed")


@app.get(
    "/api/arcs",
    response_model=List[NetworkArc],
    tags=["network"],
    summary="Get network arcs",
    description="Get CalSim3 network arcs (rivers, canals, pipelines) with geometry and attributes.",
)
async def get_all_arcs(
    limit: int = Query(
        1000, le=10000, description="Maximum arcs to return (max 10,000)"
    ),
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region: SAC, SJR, TUL, SF, SC, CC, NC"
    ),
    db: asyncpg.Connection = Depends(get_db),
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
            geojson = json.loads(row["geojson"]) if row["geojson"] else {}
            # Fix: attributes comes as JSON string, need to parse it
            attributes = row["attributes"]
            if isinstance(attributes, str):
                attributes = json.loads(attributes)
            elif attributes is None:
                attributes = {}

            arcs.append(
                NetworkArc(
                    id=row["id"],
                    short_code=row["short_code"],
                    calsim_id=row["calsim_id"],
                    name=row["name"],
                    description=row["description"],
                    arc_type=row["arc_type"] or "Unknown",
                    from_node=row["from_node"],
                    to_node=row["to_node"],
                    shape_length=row["shape_length"],
                    hydrologic_region=row["hydrologic_region"],
                    geojson=geojson,
                    attributes=attributes,
                )
            )

        return arcs

    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query failed")


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
            "database": "connected",
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "disconnected",
            "error": str(e),
        }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
