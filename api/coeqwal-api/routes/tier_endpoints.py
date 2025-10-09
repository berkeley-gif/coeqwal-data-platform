"""
Tier API endpoints for COEQWAL interpretive framework
Provides tier definitions and scenario tier data
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional, Any
import asyncpg
from pydantic import BaseModel

router = APIRouter(prefix="/api/tiers", tags=["tiers"])

# Pydantic models for response validation
class TierDefinition(BaseModel):
    short_code: str
    name: str
    description: str
    tier_type: str
    tier_count: int
    is_active: bool

class TierData(BaseModel):
    tier: str
    value: Optional[int]
    normalized: Optional[float]

class MultiValueTierResult(BaseModel):
    scenario: str
    tier_code: str
    tier_type: str
    data: List[TierData]
    total_value: int

class SingleValueTierResult(BaseModel):
    scenario: str
    tier_code: str
    tier_type: str
    single_tier_level: int


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

@router.get("/definitions")
async def get_tier_definitions(
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, str]:
    """
    Get tier definitions for tooltip content
    Returns: {short_code: description} mapping
    """
    try:
        query = """
        SELECT short_code, name, description 
        FROM tier_definition 
        WHERE is_active = TRUE 
        ORDER BY short_code
        """
        
        rows = await connection.fetch(query)
        
        # Return as {short_code: description} for frontend compatibility
        return {
            row['short_code']: row['description'] or row['name']
            for row in rows
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/list")
async def get_all_tier_definitions(
    connection: asyncpg.Connection = Depends(get_db)
) -> List[TierDefinition]:
    """
    Get complete tier definitions list
    """
    try:
        query = """
        SELECT short_code, name, description, tier_type, tier_count, is_active
        FROM tier_definition 
        WHERE is_active = TRUE 
        ORDER BY tier_type DESC, short_code
        """
        
        rows = await connection.fetch(query)
        
        return [
            TierDefinition(
                short_code=row['short_code'],
                name=row['name'],
                description=row['description'] or '',
                tier_type=row['tier_type'],
                tier_count=row['tier_count'],
                is_active=row['is_active']
            )
            for row in rows
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/scenarios/{scenario_id}/tiers/{tier_short_code}")
async def get_scenario_tier_data(
    scenario_id: str,
    tier_short_code: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get specific tier data for a scenario
    Returns either multi-value or single-value tier data
    """
    try:
        query = """
        SELECT 
            tr.scenario_short_code,
            tr.tier_short_code,
            td.name,
            td.tier_type,
            tr.tier_1_value,
            tr.tier_2_value,
            tr.tier_3_value,
            tr.tier_4_value,
            tr.norm_tier_1,
            tr.norm_tier_2,
            tr.norm_tier_3,
            tr.norm_tier_4,
            tr.total_value,
            tr.single_tier_level
        FROM tier_result tr
        JOIN tier_definition td ON tr.tier_short_code = td.short_code
        WHERE tr.scenario_short_code = $1 
        AND tr.tier_short_code = $2
        AND tr.is_active = TRUE
        """
        
        row = await connection.fetchrow(query, scenario_id, tier_short_code)
        
        if not row:
            raise HTTPException(
                status_code=404, 
                detail=f"Tier data not found for scenario {scenario_id}, tier {tier_short_code}"
            )
        
        # Return different format based on tier_type
        if row['tier_type'] == 'multi_value':
            return {
                "scenario": row['scenario_short_code'],
                "tier_code": row['tier_short_code'],
                "name": row['name'],
                "tier_type": "multi_value",
                "data": [
                    {
                        "tier": "tier1",
                        "value": row['tier_1_value'],
                        "normalized": float(row['norm_tier_1']) if row['norm_tier_1'] else 0.0
                    },
                    {
                        "tier": "tier2", 
                        "value": row['tier_2_value'],
                        "normalized": float(row['norm_tier_2']) if row['norm_tier_2'] else 0.0
                    },
                    {
                        "tier": "tier3",
                        "value": row['tier_3_value'], 
                        "normalized": float(row['norm_tier_3']) if row['norm_tier_3'] else 0.0
                    },
                    {
                        "tier": "tier4",
                        "value": row['tier_4_value'],
                        "normalized": float(row['norm_tier_4']) if row['norm_tier_4'] else 0.0
                    }
                ],
                "total_value": row['total_value']
            }
        else:
            # Single value tier
            return {
                "scenario": row['scenario_short_code'],
                "tier_code": row['tier_short_code'], 
                "name": row['name'],
                "tier_type": "single_value",
                "single_tier_level": row['single_tier_level']
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/scenarios/{scenario_id}/tiers")
async def get_all_scenario_tiers(
    scenario_id: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get all tier data for a scenario
    """
    try:
        query = """
        SELECT 
            tr.tier_short_code,
            td.name,
            td.tier_type,
            tr.tier_1_value,
            tr.tier_2_value,
            tr.tier_3_value,
            tr.tier_4_value,
            tr.norm_tier_1,
            tr.norm_tier_2,
            tr.norm_tier_3,
            tr.norm_tier_4,
            tr.total_value,
            tr.single_tier_level
        FROM tier_result tr
        JOIN tier_definition td ON tr.tier_short_code = td.short_code
        WHERE tr.scenario_short_code = $1
        AND tr.is_active = TRUE
        ORDER BY td.tier_type DESC, tr.tier_short_code
        """
        
        rows = await connection.fetch(query, scenario_id)
        
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tier data found for scenario {scenario_id}"
            )
        
        tiers = {}
        
        for row in rows:
            tier_code = row['tier_short_code']
            
            if row['tier_type'] == 'multi_value':
                tiers[tier_code] = {
                    "name": row['name'],
                    "type": "multi_value",
                    "data": [
                        {
                            "tier": "tier1",
                            "value": row['tier_1_value'],
                            "normalized": float(row['norm_tier_1']) if row['norm_tier_1'] else 0.0
                        },
                        {
                            "tier": "tier2",
                            "value": row['tier_2_value'],
                            "normalized": float(row['norm_tier_2']) if row['norm_tier_2'] else 0.0
                        },
                        {
                            "tier": "tier3", 
                            "value": row['tier_3_value'],
                            "normalized": float(row['norm_tier_3']) if row['norm_tier_3'] else 0.0
                        },
                        {
                            "tier": "tier4",
                            "value": row['tier_4_value'],
                            "normalized": float(row['norm_tier_4']) if row['norm_tier_4'] else 0.0
                        }
                    ],
                    "total": row['total_value']
                }
            else:
                tiers[tier_code] = {
                    "name": row['name'],
                    "type": "single_value", 
                    "level": row['single_tier_level']
                }
        
        return {
            "scenario": scenario_id,
            "tiers": tiers
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
