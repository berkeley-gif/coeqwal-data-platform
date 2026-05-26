"""
Scenario API endpoints for COEQWAL.

Provides scenario metadata and definitions.

Caching: shared in-process TTL helper (default 5 min, env-driven via
API_CACHE_TTL_SECONDS). All responses also set a matching Cache-Control
max-age so CDNs and browsers can reuse them.
"""

import logging
from typing import Any, Dict, List

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Response

from routes._common import api_cache_max_age, make_ttl_cache

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scenarios", tags=["scenarios"])

# Static catalog. Scenario rows change between ETL runs only
_static_cache = make_ttl_cache("scenarios_static", maxsize=200)

db_pool = None


def set_db_pool(pool):
    global db_pool
    db_pool = pool


async def get_db():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection


def _set_catalog_cache(response: Response) -> None:
    """Set the static-catalog Cache-Control header on the response."""
    response.headers["Cache-Control"] = f"public, max-age={api_cache_max_age()}"


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

    **Use case:** Single fetch for the scenario explorer to render cards.
    `GET /api/scenarios/{short_code}` returns the same shape for a single
    scenario.
    """
    cache_key = "scenarios:list"
    if cache_key in _static_cache:
        _set_catalog_cache(response)
        return _static_cache[cache_key]

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
    except Exception as e:
        log.error(f"scenarios list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    result = [
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
    _static_cache[cache_key] = result
    _set_catalog_cache(response)
    return result


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
    cache_key = f"scenarios:get:{scenario_id}"
    if cache_key in _static_cache:
        _set_catalog_cache(response)
        return _static_cache[cache_key]

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
    except Exception as e:
        log.error(f"scenario detail query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not row:
        raise HTTPException(
            status_code=404, detail=f"Scenario '{scenario_id}' not found"
        )

    result = {
        "short_code": row["short_code"],
        "name": row["name"] or row["short_code"],
        "short_description": row["short_description"],
        "hydroclimate_id": row["hydroclimate_id"],
        "sibling_group": row["hydroclimate_sibling"],
        "is_active": bool(row["is_active"])
        if row["is_active"] is not None
        else True,
    }
    _static_cache[cache_key] = result
    _set_catalog_cache(response)
    return result
