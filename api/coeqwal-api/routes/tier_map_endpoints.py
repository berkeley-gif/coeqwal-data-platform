"""
Map/tier visualization API endpoints
Provides tier data with geospatial geometries
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
import asyncpg
from pydantic import BaseModel
import json

router = APIRouter(prefix="/api/tier-map", tags=["tier-map"])

# Pydantic models for response validation
class TierMapFeature(BaseModel):
    """GeoJSON Feature for a single tier location"""
    type: str = "Feature"
    geometry: Dict[str, Any]  # GeoJSON geometry
    properties: Dict[str, Any]  # Tier level, location info, etc.

class TierMapResponse(BaseModel):
    """GeoJSON FeatureCollection for tier visualization"""
    type: str = "FeatureCollection"
    features: List[TierMapFeature]
    metadata: Dict[str, Any]

# Database connection dependency (set by main.py)
db_pool = None

def set_db_pool(pool):
    global db_pool
    db_pool = pool

async def get_db():
    if db_pool is None:
        raise HTTPException(status_code=500, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection

@router.get("/{scenario_short_code}/{tier_short_code}")
async def get_tier_map_data(
    scenario_short_code: str,
    tier_short_code: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> TierMapResponse:
    """
    Get tier visualization data for a specific scenario and tier combination.
    
    Returns GeoJSON FeatureCollection with:
    - Points (nodes, compliance stations) or polygons (reservoirs, WBAs)
    - Tier level (1-4) for coloring on frontend
    - Location metadata (name, type, etc.)
    
    Special handling: SLUIS_CVP and SLUIS_SWP both return the same SLUIS polygon
    """
    try:
        # Main query to get tier location data with geometries
        query = """
        WITH tier_locations AS (
            SELECT 
                tlr.scenario_short_code,
                tlr.tier_short_code,
                tlr.location_type,
                tlr.location_id,
                tlr.location_name,
                tlr.tier_level,
                tlr.tier_value,
                tlr.display_order,
                td.name as tier_name,
                td.tier_type
            FROM tier_location_result tlr
            JOIN tier_definition td ON tlr.tier_short_code = td.short_code
            WHERE tlr.scenario_short_code = $1
            AND tlr.tier_short_code = $2
        )
        SELECT 
            tl.location_type,
            tl.location_id,
            tl.location_name,
            tl.tier_level,
            tl.tier_value,
            tl.display_order,
            tl.tier_name,
            tl.tier_type,
            CASE 
                -- Handle San Luis special case: both SLUIS_CVP and SLUIS_SWP point to SLUIS polygon
                WHEN tl.location_type = 'reservoir' AND tl.location_id IN ('SLUIS_CVP', 'SLUIS_SWP') THEN
                    (SELECT ST_AsGeoJSON(geom)::jsonb 
                     FROM reservoirs 
                     WHERE calsim_short_code = 'SLUIS')
                -- Regular reservoir lookup
                WHEN tl.location_type = 'reservoir' THEN
                    (SELECT ST_AsGeoJSON(geom)::jsonb 
                     FROM reservoirs 
                     WHERE calsim_short_code = tl.location_id)
                -- Region lookup (DELTA, SAC) - uses WBA table
                WHEN tl.location_type = 'region' THEN
                    (SELECT ST_AsGeoJSON(geom)::jsonb 
                     FROM wba 
                     WHERE wba_id = tl.location_id)
                -- WBA (aquifer) lookup
                WHEN tl.location_type = 'wba' THEN
                    (SELECT ST_AsGeoJSON(geom)::jsonb 
                     FROM wba 
                     WHERE wba_id = tl.location_id)
                -- Compliance station lookup
                WHEN tl.location_type = 'compliance_station' THEN
                    (SELECT ST_AsGeoJSON(geom)::jsonb 
                     FROM compliance_stations 
                     WHERE station_code = tl.location_id)
                -- Network node lookup
                WHEN tl.location_type = 'network_node' THEN
                    (SELECT ST_AsGeoJSON(geom)::jsonb 
                     FROM network_gis 
                     WHERE short_code = tl.location_id)
                ELSE NULL
            END as geometry,
            CASE 
                WHEN tl.location_type = 'reservoir' THEN 'Reservoir'
                WHEN tl.location_type = 'wba' THEN 'Aquifer'
                WHEN tl.location_type = 'region' THEN 'Region'
                WHEN tl.location_type = 'compliance_station' THEN 'Compliance Station'
                WHEN tl.location_type = 'network_node' THEN 'Environmental Flow'
                ELSE tl.location_type
            END as location_type_display
        FROM tier_locations tl
        ORDER BY tl.display_order, tl.location_name
        """
        
        rows = await connection.fetch(query, scenario_short_code, tier_short_code)
        
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tier data found for scenario '{scenario_short_code}' and tier '{tier_short_code}'"
            )
        
        # Build GeoJSON featureCollection
        features = []
        tier_name = None
        tier_type = None
        
        for row in rows:
            if not row['geometry']:
                # Skip if no geometry found (shouldn't happen with proper data)
                continue
            
            # Store tier metadata for response
            if not tier_name:
                tier_name = row['tier_name']
                tier_type = row['tier_type']
            
            # Parse geometry (comes from PostGIS as JSON)
            geometry = row['geometry']
            if isinstance(geometry, str):
                geometry = json.loads(geometry)
            
            # Build feature properties
            properties = {
                "location_id": row['location_id'],
                "location_name": row['location_name'],
                "location_type": row['location_type'],
                "location_type_display": row['location_type_display'],
                "tier_level": row['tier_level'],
                "tier_value": row['tier_value'],
                "display_order": row['display_order'],
                # Add color hints for frontend (optional, frontend can also compute)
                "tier_color_class": f"tier-{row['tier_level']}"
            }
            
            features.append(TierMapFeature(
                type="Feature",
                geometry=geometry,
                properties=properties
            ))
        
        # Build metadata
        metadata = {
            "scenario": scenario_short_code,
            "tier_code": tier_short_code,
            "tier_name": tier_name,
            "tier_type": tier_type,
            "feature_count": len(features),
            "location_types": list(set(row['location_type'] for row in rows))
        }
        
        return TierMapResponse(
            type="FeatureCollection",
            features=features,
            metadata=metadata
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/scenarios")
async def get_available_scenarios(
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get list of available scenarios for tier map visualization
    """
    try:
        query = """
        SELECT DISTINCT 
            scenario_short_code,
            COUNT(DISTINCT tier_short_code) as tier_count,
            COUNT(*) as location_count
        FROM tier_location_result
        GROUP BY scenario_short_code
        ORDER BY scenario_short_code
        """
        
        rows = await connection.fetch(query)
        
        scenarios = [
            {
                "scenario_code": row['scenario_short_code'],
                "tier_count": row['tier_count'],
                "location_count": row['location_count']
            }
            for row in rows
        ]
        
        return {
            "scenarios": scenarios,
            "total": len(scenarios)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/tiers")
async def get_available_tiers(
    scenario_short_code: Optional[str] = Query(None, description="Filter by scenario"),
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get list of available tiers for tier map visualization
    Optionally filter by scenario
    """
    try:
        if scenario_short_code:
            query = """
            SELECT DISTINCT 
                td.short_code,
                td.name,
                td.description,
                td.tier_type,
                td.tier_count,
                COUNT(tlr.id) as location_count
            FROM tier_definition td
            JOIN tier_location_result tlr ON td.short_code = tlr.tier_short_code
            WHERE tlr.scenario_short_code = $1
            AND td.is_active = TRUE
            GROUP BY td.short_code, td.name, td.description, td.tier_type, td.tier_count
            ORDER BY td.tier_type DESC, td.short_code
            """
            rows = await connection.fetch(query, scenario_short_code)
        else:
            query = """
            SELECT 
                short_code,
                name,
                description,
                tier_type,
                tier_count
            FROM tier_definition
            WHERE is_active = TRUE
            ORDER BY tier_type DESC, short_code
            """
            rows = await connection.fetch(query)
        
        tiers = []
        for row in rows:
            tier_data = {
                "tier_code": row['short_code'],
                "tier_name": row['name'],
                "description": row['description'] or '',
                "tier_type": row['tier_type'],
                "tier_count": row['tier_count']
            }
            if scenario_short_code:
                tier_data['location_count'] = row['location_count']
            tiers.append(tier_data)
        
        return {
            "tiers": tiers,
            "total": len(tiers)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/summary/{scenario_short_code}")
async def get_scenario_tier_summary(
    scenario_short_code: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get summary of all tiers for a scenario (for UI tier selector)
    """
    try:
        query = """
        SELECT 
            td.short_code,
            td.name,
            td.description,
            td.tier_type,
            td.tier_count,
            COUNT(tlr.id) as location_count,
            COUNT(DISTINCT tlr.tier_level) as tier_levels_used
        FROM tier_definition td
        JOIN tier_location_result tlr ON td.short_code = tlr.tier_short_code
        WHERE tlr.scenario_short_code = $1
        AND td.is_active = TRUE
        GROUP BY td.short_code, td.name, td.description, td.tier_type, td.tier_count
        ORDER BY td.tier_type DESC, td.short_code
        """
        
        rows = await connection.fetch(query, scenario_short_code)
        
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tier data found for scenario '{scenario_short_code}'"
            )
        
        tiers = [
            {
                "tier_code": row['short_code'],
                "tier_name": row['name'],
                "description": row['description'] or '',
                "tier_type": row['tier_type'],
                "tier_count": row['tier_count'],
                "location_count": row['location_count'],
                "tier_levels_used": row['tier_levels_used']
            }
            for row in rows
        ]
        
        return {
            "scenario": scenario_short_code,
            "tiers": tiers,
            "total_tiers": len(tiers)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
