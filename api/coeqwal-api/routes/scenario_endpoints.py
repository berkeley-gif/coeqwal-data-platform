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
    connection: asyncpg.Connection = Depends(get_db),
) -> List[Dict[str, Any]]:
    """
    Get list of all scenarios with metadata.

    Returns scenario definitions including:
    - `scenario_id`: Friendly identifier (e.g., 's0020')
    - `short_code`: Technical code (e.g., 's0020_DCRadjBL_2020LU_wTUCP')
    - `name`: Display name
    - `description`: Full description
    - `is_active`: Whether scenario is active

    **Use case:** Build scenario selection UI, show scenario cards.
    """
    try:
        # Simple query - no joins
        query = """
        SELECT 
            scenario_id,
            short_code,
            name,
            short_title,
            description,
            simple_description,
            hydroclimate_id,
            is_active
        FROM scenario
        WHERE is_active = 1 OR is_active::text = 'true' OR is_active::text = 't'
        ORDER BY scenario_id
        """

        rows = await connection.fetch(query)

        # If still empty, try without the filter
        if not rows:
            query_all = """
            SELECT 
                scenario_id,
                short_code,
                name,
                short_title,
                description,
                simple_description,
                hydroclimate_id,
                is_active
            FROM scenario
            ORDER BY scenario_id
            """
            rows = await connection.fetch(query_all)

        return [
            {
                "scenario_id": row["scenario_id"],
                "short_code": row["short_code"],
                "name": row["name"] or row["short_title"] or row["short_code"],
                "short_title": row["short_title"],
                "description": row["description"] or row["simple_description"],
                "is_active": bool(row["is_active"])
                if row["is_active"] is not None
                else True,
            }
            for row in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/{scenario_id}", summary="Get scenario details")
async def get_scenario(
    scenario_id: str, connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get detailed metadata for a specific scenario.

    **Example:** `GET /api/scenarios/s0020`

    Returns full scenario metadata including themes and key assumptions.
    """
    try:
        # Get scenario base info - simple query first
        # Try matching by scenario_id first (e.g., 's0020'), then by short_code
        scenario_query = """
        SELECT 
            id,
            scenario_id,
            short_code,
            name,
            description,
            simple_description,
            short_title,
            hydroclimate_id,
            is_active
        FROM scenario
        WHERE scenario_id = $1 OR short_code = $1
        """

        scenario = await connection.fetchrow(scenario_query, scenario_id)

        if not scenario:
            raise HTTPException(
                status_code=404, detail=f"Scenario {scenario_id} not found"
            )

        # Get hydroclimate info separately (may not exist)
        hydroclimate = None
        hydroclimate_name = None
        if scenario["hydroclimate_id"]:
            hc = await connection.fetchrow(
                "SELECT short_code, name FROM hydroclimate WHERE id = $1",
                scenario["hydroclimate_id"],
            )
            if hc:
                hydroclimate = hc["short_code"]
                hydroclimate_name = hc["name"]

        # Get associated themes (may be empty)
        theme_query = """
        SELECT t.short_code, t.name, t.short_title
        FROM theme t
        JOIN theme_scenario_link tsl ON t.id = tsl.theme_id
        WHERE tsl.scenario_id = $1
        """
        themes = await connection.fetch(theme_query, scenario["id"])

        # Get key assumptions (may be empty)
        assumption_query = """
        SELECT ad.short_code, ad.name, ad.description
        FROM assumption_definition ad
        JOIN scenario_key_assumption_link skal ON ad.id = skal.assumption_id
        WHERE skal.scenario_id = $1
        """
        assumptions = await connection.fetch(assumption_query, scenario["id"])

        # Get key operations (may be empty)
        operation_query = """
        SELECT od.short_code, od.name, od.description
        FROM operation_definition od
        JOIN scenario_key_operation_link skol ON od.id = skol.operation_id
        WHERE skol.scenario_id = $1
        """
        operations = await connection.fetch(operation_query, scenario["id"])

        return {
            "scenario_id": scenario["scenario_id"],
            "short_code": scenario["short_code"],
            "name": scenario["name"]
            or scenario["short_title"]
            or scenario["short_code"],
            "short_title": scenario["short_title"],
            "description": scenario["description"] or scenario["simple_description"],
            "hydroclimate": hydroclimate,
            "hydroclimate_name": hydroclimate_name,
            "is_active": bool(scenario["is_active"]),
            "themes": [
                {
                    "short_code": t["short_code"],
                    "name": t["name"],
                    "short_title": t["short_title"],
                }
                for t in themes
            ],
            "key_assumptions": [
                {
                    "short_code": a["short_code"],
                    "name": a["name"],
                    "description": a["description"],
                }
                for a in assumptions
            ],
            "key_operations": [
                {
                    "short_code": o["short_code"],
                    "name": o["name"],
                    "description": o["description"],
                }
                for o in operations
            ],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/{scenario_id}/compare/{other_scenario_id}", summary="Compare two scenarios"
)
async def compare_scenarios(
    scenario_id: str,
    other_scenario_id: str,
    connection: asyncpg.Connection = Depends(get_db),
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
                detail=f"No tier data found for scenarios {scenario_id} or {other_scenario_id}",
            )

        # Group by tier
        comparison = {}
        for row in rows:
            tier_code = row["tier_short_code"]
            scenario = row["scenario_short_code"]

            if tier_code not in comparison:
                comparison[tier_code] = {
                    "name": row["tier_name"],
                    "type": row["tier_type"],
                    scenario_id: None,
                    other_scenario_id: None,
                }

            # Calculate weighted score
            if row["tier_type"] == "multi_value":
                n1 = float(row["norm_tier_1"] or 0)
                n2 = float(row["norm_tier_2"] or 0)
                n3 = float(row["norm_tier_3"] or 0)
                n4 = float(row["norm_tier_4"] or 0)
                total = n1 + n2 + n3 + n4
                if total > 0:
                    weighted = (1 * n1 + 2 * n2 + 3 * n3 + 4 * n4) / total
                    normalized = (4 - weighted) / 3
                else:
                    weighted = None
                    normalized = None
            else:
                level = row["single_tier_level"] or 0
                weighted = float(level)
                normalized = (4 - weighted) / 3 if level else None

            comparison[tier_code][scenario] = {
                "weighted_score": round(weighted, 3) if weighted else None,
                "normalized_score": round(normalized, 3) if normalized else None,
            }

        return {"scenarios": [scenario_id, other_scenario_id], "comparison": comparison}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
