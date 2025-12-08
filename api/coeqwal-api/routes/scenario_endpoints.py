"""
Scenario API endpoints for COEQWAL.

Provides scenario metadata and definitions.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Any
import asyncpg

router = APIRouter(prefix="/api/scenarios", tags=["scenarios"])

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


@router.get("", summary="List all scenarios")
async def get_all_scenarios(
    connection: asyncpg.Connection = Depends(get_db)
) -> List[Dict[str, Any]]:
    """
    Get list of all scenarios with metadata.
    
    Returns scenario definitions including:
    - `short_code`: Unique identifier (e.g., 's0020')
    - `title`: Display title
    - `description`: Full description
    - `status`: Current status (e.g., 'FINAL')
    - `theme`: Associated theme short_code
    - `hydroclimate`: Hydroclimate setting
    - `land_use`: Land use setting
    
    **Use case:** Build scenario selection UI, show scenario cards.
    """
    try:
        query = """
        SELECT 
            s.short_code,
            s.title,
            s.description,
            s.status,
            s.hydroclimate_id,
            s.is_active,
            h.short_code as hydroclimate,
            h.title as hydroclimate_title
        FROM scenario s
        LEFT JOIN hydroclimate h ON s.hydroclimate_id = h.id
        WHERE s.is_active = 1
        ORDER BY s.short_code
        """
        
        rows = await connection.fetch(query)
        
        return [
            {
                "short_code": row['short_code'],
                "title": row['title'] or row['short_code'],
                "description": row['description'],
                "status": row['status'],
                "hydroclimate": row['hydroclimate'],
                "hydroclimate_title": row['hydroclimate_title'],
                "is_active": bool(row['is_active'])
            }
            for row in rows
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/{scenario_id}", summary="Get scenario details")
async def get_scenario(
    scenario_id: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get detailed metadata for a specific scenario.
    
    **Example:** `GET /api/scenarios/s0020`
    
    Returns full scenario metadata including themes and key assumptions.
    """
    try:
        # Get scenario base info
        scenario_query = """
        SELECT 
            s.id,
            s.short_code,
            s.title,
            s.description,
            s.status,
            s.hydroclimate_id,
            s.is_active,
            h.short_code as hydroclimate,
            h.title as hydroclimate_title
        FROM scenario s
        LEFT JOIN hydroclimate h ON s.hydroclimate_id = h.id
        WHERE s.short_code = $1
        """
        
        scenario = await connection.fetchrow(scenario_query, scenario_id)
        
        if not scenario:
            raise HTTPException(
                status_code=404,
                detail=f"Scenario {scenario_id} not found"
            )
        
        # Get associated themes
        theme_query = """
        SELECT t.short_code, t.title, t.short_title
        FROM theme t
        JOIN theme_scenario_link tsl ON t.id = tsl.theme_id
        JOIN scenario s ON s.id = tsl.scenario_id
        WHERE s.short_code = $1
        """
        themes = await connection.fetch(theme_query, scenario_id)
        
        # Get key assumptions
        assumption_query = """
        SELECT ad.short_code, ad.title, ad.description
        FROM assumption_definition ad
        JOIN scenario_key_assumption_link skal ON ad.id = skal.assumption_id
        JOIN scenario s ON s.id = skal.scenario_id
        WHERE s.short_code = $1
        """
        assumptions = await connection.fetch(assumption_query, scenario_id)
        
        # Get key operations
        operation_query = """
        SELECT od.short_code, od.title, od.description
        FROM operation_definition od
        JOIN scenario_key_operation_link skol ON od.id = skol.operation_id
        JOIN scenario s ON s.id = skol.scenario_id
        WHERE s.short_code = $1
        """
        operations = await connection.fetch(operation_query, scenario_id)
        
        return {
            "short_code": scenario['short_code'],
            "title": scenario['title'] or scenario['short_code'],
            "description": scenario['description'],
            "status": scenario['status'],
            "hydroclimate": scenario['hydroclimate'],
            "hydroclimate_title": scenario['hydroclimate_title'],
            "is_active": bool(scenario['is_active']),
            "themes": [
                {
                    "short_code": t['short_code'],
                    "title": t['title'],
                    "short_title": t['short_title']
                }
                for t in themes
            ],
            "key_assumptions": [
                {
                    "short_code": a['short_code'],
                    "title": a['title'],
                    "description": a['description']
                }
                for a in assumptions
            ],
            "key_operations": [
                {
                    "short_code": o['short_code'],
                    "title": o['title'],
                    "description": o['description']
                }
                for o in operations
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/{scenario_id}/compare/{other_scenario_id}", summary="Compare two scenarios")
async def compare_scenarios(
    scenario_id: str,
    other_scenario_id: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Compare tier scores between two scenarios.
    
    **Example:** `GET /api/scenarios/s0020/compare/s0029`
    
    Returns side-by-side tier scores for quick comparison.
    """
    try:
        query = """
        SELECT 
            tr.scenario_short_code,
            tr.tier_short_code,
            td.name as tier_name,
            td.tier_type,
            tr.norm_tier_1,
            tr.norm_tier_2,
            tr.norm_tier_3,
            tr.norm_tier_4,
            tr.single_tier_level
        FROM tier_result tr
        JOIN tier_definition td ON tr.tier_short_code = td.short_code
        WHERE tr.scenario_short_code IN ($1, $2)
        AND tr.is_active = TRUE
        ORDER BY tr.tier_short_code, tr.scenario_short_code
        """
        
        rows = await connection.fetch(query, scenario_id, other_scenario_id)
        
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tier data found for scenarios {scenario_id} or {other_scenario_id}"
            )
        
        # Group by tier
        comparison = {}
        for row in rows:
            tier_code = row['tier_short_code']
            scenario = row['scenario_short_code']
            
            if tier_code not in comparison:
                comparison[tier_code] = {
                    "name": row['tier_name'],
                    "type": row['tier_type'],
                    scenario_id: None,
                    other_scenario_id: None
                }
            
            # Calculate weighted score
            if row['tier_type'] == 'multi_value':
                n1 = float(row['norm_tier_1'] or 0)
                n2 = float(row['norm_tier_2'] or 0)
                n3 = float(row['norm_tier_3'] or 0)
                n4 = float(row['norm_tier_4'] or 0)
                total = n1 + n2 + n3 + n4
                if total > 0:
                    weighted = (1*n1 + 2*n2 + 3*n3 + 4*n4) / total
                    normalized = (4 - weighted) / 3
                else:
                    weighted = None
                    normalized = None
            else:
                level = row['single_tier_level'] or 0
                weighted = float(level)
                normalized = (4 - weighted) / 3 if level else None
            
            comparison[tier_code][scenario] = {
                "weighted_score": round(weighted, 3) if weighted else None,
                "normalized_score": round(normalized, 3) if normalized else None
            }
        
        return {
            "scenarios": [scenario_id, other_scenario_id],
            "comparison": comparison
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

