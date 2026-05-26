"""
Scenario API endpoints for COEQWAL.

Provides scenario metadata and definitions.
"""

from fastapi import APIRouter, HTTPException, Depends, Response
from typing import Dict, List, Any
import asyncpg

from routes._common.null_handling import safe_float, safe_int

router = APIRouter(prefix="/api/scenarios", tags=["scenarios"])

# Cache-Control header for catalog endpoints whose contents only change between
# ETL runs. 5 minutes gives CDNs and browsers a safe reuse window without
# masking new data for long after a deploy.
STATIC_CATALOG_CACHE_CONTROL = "public, max-age=300"

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
    response: Response,
    connection: asyncpg.Connection = Depends(get_db),
) -> List[Dict[str, Any]]:
    """
    Get list of all scenarios with metadata.

    Returns scenario definitions including:
    - `short_code`: Friendly identifier (e.g., 's0020')
    - `run_name`: Technical run name (e.g., 's0020_DCRadjBL_2020LU_wTUCP')
    - `name`: Display name
    - `short_description`: Brief description
    - `hydroclimate_id`: Numeric hydroclimate id (internal)
    - `hydroclimate_short_code`: Hydroclimate short code, e.g. `historical`, `cc50`,
      `cc95`. Frontends should prefer this over the numeric id when resolving
      sibling groups to concrete scenario runs for a given hydroclimate.
    - `is_active`: Whether scenario is active

    **Use case:** Build scenario selection UI, show scenario cards, resolve
    sibling-group IDs to actual scenario codes for the active hydroclimate
    without needing a hardcoded string-to-id map on the client.
    """
    try:
        # Join hydroclimate to expose short_code so clients can resolve sibling
        # groups to the active hydroclimate without a hardcoded numeric map.
        query = """
        SELECT
            s.short_code,
            s.run_name,
            s.hydroclimate_id,
            h.short_code AS hydroclimate_short_code,
            s.hydroclimate_sibling,
            s.is_active,
            sg.name,
            sg.short_description,
            sg.long_description,
            sg.baseline_group
        FROM scenario s
        LEFT JOIN scenario_hydroclimate_sibling sg
            ON s.hydroclimate_sibling = sg.short_code
        LEFT JOIN hydroclimate h
            ON s.hydroclimate_id = h.id
        WHERE s.is_active = TRUE
        ORDER BY s.short_code
        """

        rows = await connection.fetch(query)

        if not rows:
            query_all = """
            SELECT
                s.short_code,
                s.run_name,
                s.hydroclimate_id,
                h.short_code AS hydroclimate_short_code,
                s.hydroclimate_sibling,
                s.is_active,
                sg.name,
                sg.short_description,
                sg.long_description,
                sg.baseline_group
            FROM scenario s
            LEFT JOIN scenario_hydroclimate_sibling sg
                ON s.hydroclimate_sibling = sg.short_code
            LEFT JOIN hydroclimate h
                ON s.hydroclimate_id = h.id
            ORDER BY s.short_code
            """
            rows = await connection.fetch(query_all)

        response.headers["Cache-Control"] = STATIC_CATALOG_CACHE_CONTROL
        return [
            {
                "short_code": row["short_code"],
                "run_name": row["run_name"],
                "name": row["name"] or row["short_code"],
                "short_description": row["short_description"],
                "hydroclimate_id": row["hydroclimate_id"],
                "hydroclimate_short_code": row["hydroclimate_short_code"],
                "baseline_scenario": row["baseline_group"],
                "sibling_group": row["hydroclimate_sibling"],
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
        scenario_query = """
        SELECT
            s.id,
            s.short_code,
            s.run_name,
            s.hydroclimate_id,
            s.hydroclimate_sibling,
            s.is_active,
            sg.name,
            sg.short_description,
            sg.long_description,
            sg.baseline_group
        FROM scenario s
        LEFT JOIN scenario_hydroclimate_sibling sg
            ON s.hydroclimate_sibling = sg.short_code
        WHERE s.short_code = $1 OR s.run_name = $1
        """

        scenario = await connection.fetchrow(scenario_query, scenario_id)

        if not scenario:
            raise HTTPException(
                status_code=404, detail=f"Scenario {scenario_id} not found"
            )

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

        theme_query = """
        SELECT t.short_code, t.name, t.short_title
        FROM theme t
        JOIN theme_scenario_link tsl ON t.id = tsl.theme_id
        WHERE tsl.scenario_id = $1
        """
        themes = await connection.fetch(theme_query, scenario["id"])

        assumption_query = """
        SELECT ad.short_code, ad.name, ad.description
        FROM assumption_definition ad
        JOIN scenario_key_assumption_link skal ON ad.id = skal.assumption_id
        WHERE skal.scenario_id = $1
        """
        assumptions = await connection.fetch(assumption_query, scenario["id"])

        operation_query = """
        SELECT od.short_code, od.name, od.description
        FROM operation_definition od
        JOIN scenario_key_operation_link skol ON od.id = skol.operation_id
        WHERE skol.scenario_id = $1
        """
        operations = await connection.fetch(operation_query, scenario["id"])

        return {
            "short_code": scenario["short_code"],
            "run_name": scenario["run_name"],
            "name": scenario["name"] or scenario["short_code"],
            "short_description": scenario["short_description"],
            "long_description": scenario["long_description"],
            "hydroclimate": hydroclimate,
            "hydroclimate_name": hydroclimate_name,
            "baseline_scenario": scenario["baseline_group"],
            "sibling_group": scenario["hydroclimate_sibling"],
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
                n1_raw = safe_float(row["norm_tier_1"])
                n2_raw = safe_float(row["norm_tier_2"])
                n3_raw = safe_float(row["norm_tier_3"])
                n4_raw = safe_float(row["norm_tier_4"])

                if n1_raw is None and n2_raw is None and n3_raw is None and n4_raw is None:
                    # No tier data at all for this row.
                    weighted = None
                    normalized = None
                else:
                    n1 = n1_raw if n1_raw is not None else 0.0
                    n2 = n2_raw if n2_raw is not None else 0.0
                    n3 = n3_raw if n3_raw is not None else 0.0
                    n4 = n4_raw if n4_raw is not None else 0.0
                    total = n1 + n2 + n3 + n4
                    if total > 0:
                        weighted = (1 * n1 + 2 * n2 + 3 * n3 + 4 * n4) / total
                        normalized = (4 - weighted) / 3
                    else:
                        weighted = None
                        normalized = None
            else:
                level = safe_int(row["single_tier_level"])
                if level is None:
                    weighted = None
                    normalized = None
                else:
                    weighted = float(level)
                    normalized = (4 - weighted) / 3

            # Preserve a real zero by rounding only when there is a numeric value.
            comparison[tier_code][scenario] = {
                "weighted_score": round(weighted, 3) if weighted is not None else None,
                "normalized_score": round(normalized, 3) if normalized is not None else None,
            }

        return {"scenarios": [scenario_id, other_scenario_id], "comparison": comparison}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
