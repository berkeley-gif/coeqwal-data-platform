"""
Scenario API endpoints for COEQWAL.

Provides scenario metadata and definitions.
"""

from fastapi import APIRouter, HTTPException, Depends, Response
from typing import Dict, List, Any
import asyncpg

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
    Get the active scenario list with the minimal field set the frontend uses.

    Each row carries six fields:
    - `short_code`: Friendly identifier (e.g. `s0020`)
    - `name`: Display name from sibling-group prose
    - `short_description`: Brief description, 1-2 sentences
    - `hydroclimate_id`: Numeric hydroclimate id (internal)
    - `sibling_group`: Sibling-group short code (same strategy under
      different hydroclimates share this value)
    - `is_active`: Whether the scenario is active

    Fields the API no longer returns (intentionally): `run_name`,
    `long_description`, `hydroclimate_short_code`, `hydroclimate_name`,
    `baseline_scenario`, plus the per-scenario `themes`, `key_assumptions`,
    and `key_operations` arrays. The frontend currently sources themes from
    a local `scenarioMetadata` map keyed by `sibling_group` and operation
    icons from a hardcoded `opsIcons.tsx`. The database README roadmap
    tracks the cutover that would move that content into the DB and bring
    these payload fields back.

    **Use case:** Single fetch for the scenario explorer to render cards.
    `GET /api/scenarios/{short_code}` returns the same shape for a single
    scenario.
    """
    try:
        # Active-only first, then all rows as a fallback so the UI never
        # sees an empty list during seed gaps
        base_query = """
        SELECT
            s.short_code,
            s.hydroclimate_id,
            s.hydroclimate_sibling,
            s.is_active,
            sg.name,
            sg.short_description
        FROM scenario s
        LEFT JOIN scenario_hydroclimate_sibling sg
            ON s.hydroclimate_sibling = sg.short_code
        {where}
        ORDER BY s.short_code
        """

        rows = await connection.fetch(base_query.format(where="WHERE s.is_active = TRUE"))
        if not rows:
            rows = await connection.fetch(base_query.format(where=""))

        response.headers["Cache-Control"] = STATIC_CATALOG_CACHE_CONTROL
        return [
            {
                "short_code": row["short_code"],
                "name": row["name"] or row["short_code"],
                "short_description": row["short_description"],
                "hydroclimate_id": row["hydroclimate_id"],
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
    scenario_id: str,
    response: Response,
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get metadata for a specific scenario, in the same six-field shape as
    each entry returned by `GET /api/scenarios`.

    **Example:** `GET /api/scenarios/s0020`

    `scenario_id` is the friendly `short_code` (e.g. `s0020`). The full
    `run_name` is not accepted here. Use `GET /api/scenarios` to map
    `run_name` to `short_code` if needed.

    Returns: `short_code`, `name`, `short_description`, `hydroclimate_id`,
    `sibling_group`, `is_active`.
    """
    try:
        query = """
        SELECT
            s.short_code,
            s.hydroclimate_id,
            s.hydroclimate_sibling,
            s.is_active,
            sg.name,
            sg.short_description
        FROM scenario s
        LEFT JOIN scenario_hydroclimate_sibling sg
            ON s.hydroclimate_sibling = sg.short_code
        WHERE s.short_code = $1
        """

        row = await connection.fetchrow(query, scenario_id)

        if not row:
            raise HTTPException(
                status_code=404, detail=f"Scenario {scenario_id} not found"
            )

        response.headers["Cache-Control"] = STATIC_CATALOG_CACHE_CONTROL
        return {
            "short_code": row["short_code"],
            "name": row["name"] or row["short_code"],
            "short_description": row["short_description"],
            "hydroclimate_id": row["hydroclimate_id"],
            "sibling_group": row["hydroclimate_sibling"],
            "is_active": bool(row["is_active"])
            if row["is_active"] is not None
            else True,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

