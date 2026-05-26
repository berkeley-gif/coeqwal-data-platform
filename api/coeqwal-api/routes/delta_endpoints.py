"""
Delta Statistics API Endpoints.

Serves monthly percentile distributions and period summaries for Delta variables:
  - X2 position (2 ppt isohaline, KM)
  - Salinity at compliance points (Emmaton, Jersey Point, Rock Slough, Collinsville.UMHOS/CM)
  - Salinity at pumping plants (Banks, Tracy/Jones.UMHOS/CM)
  - Net Delta Outflow (NDO.TAF)

Endpoints:
  GET /api/statistics/scenarios/{id}/delta/monthly
     .Monthly percentile bands for all 8 Delta variables

Performance: 30-minute in-process TTL cache + 15-minute browser Cache-Control.
Water months: 1=October ... 12=September
"""

import logging
from typing import Any, Dict, Optional

from cachetools import TTLCache
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from routes._common.null_handling import safe_float, safe_int

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

_db_pool = None

_stats_cache: TTLCache = TTLCache(maxsize=2000, ttl=1800)
_CACHE_MAX_AGE_STATS = 900


def set_db_pool(pool) -> None:
    global _db_pool
    _db_pool = pool


def _json_response(data: Dict[str, Any], max_age: int) -> JSONResponse:
    return JSONResponse(
        content=data,
        headers={"Cache-Control": f"public, max-age={max_age}"},
    )


async def _fetch_delta_monthly(
    pool, scenario_id: str, category: Optional[str]
) -> Dict[str, Any]:
    """
    Fetch monthly percentile distributions from delta_monthly.
    One row per (variable_code × water_month).
    """
    cache_key = f"delta_monthly:{scenario_id}:{category or ''}"
    if cache_key in _stats_cache:
        return _stats_cache[cache_key]

    async with pool.acquire() as conn:
        query = """
            SELECT
                variable_code,
                water_month,
                avg,
                cv,
                unit,
                avg_cfs,
                q0, q10, q30, q50, q70, q90, q100,
                exc_p5, exc_p10, exc_p25, exc_p50, exc_p75, exc_p90, exc_p95,
                sample_count
            FROM delta_monthly
            WHERE scenario_short_code = $1
        """
        params: list = [scenario_id]

        if category:
            variable_codes = {
                "x2": ["x2"],
                "salinity_compliance": ["em_ec", "jp_ec", "rs_ec", "co_ec"],
                "salinity_pumps": ["banks_ec", "tracy_ec"],
                "outflow": ["ndo"],
            }.get(category, [])
            if variable_codes:
                placeholders = ", ".join(
                    f"${i + 2}" for i in range(len(variable_codes))
                )
                query += f" AND variable_code IN ({placeholders})"
                params.extend(variable_codes)

        query += " ORDER BY variable_code, water_month"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"delta monthly query failed for {scenario_id}: {e}")
            raise

    data = [
        {
            "variable_code": row["variable_code"],
            "water_month": row["water_month"],
            "avg": safe_float(row["avg"]),
            "cv": safe_float(row["cv"]),
            "unit": row["unit"],
            "avg_cfs": safe_float(row["avg_cfs"]),
            "q0": safe_float(row["q0"]),
            "q10": safe_float(row["q10"]),
            "q30": safe_float(row["q30"]),
            "q50": safe_float(row["q50"]),
            "q70": safe_float(row["q70"]),
            "q90": safe_float(row["q90"]),
            "q100": safe_float(row["q100"]),
            "exc_p5": safe_float(row["exc_p5"]),
            "exc_p10": safe_float(row["exc_p10"]),
            "exc_p25": safe_float(row["exc_p25"]),
            "exc_p50": safe_float(row["exc_p50"]),
            "exc_p75": safe_float(row["exc_p75"]),
            "exc_p90": safe_float(row["exc_p90"]),
            "exc_p95": safe_float(row["exc_p95"]),
            "sample_count": safe_int(row["sample_count"]),
        }
        for row in rows
    ]

    result = {"scenario_id": scenario_id, "data": data, "count": len(data)}
    _stats_cache[cache_key] = result
    return result


@router.get(
    "/scenarios/{scenario_id}/delta/monthly",
    summary="Monthly Delta statistics (X2, salinity, outflow)",
    description=(
        "Returns monthly percentile distributions for Delta variables: "
        "X2 position (KM), salinity at compliance points (UMHOS/CM), "
        "salinity at pumping plants (UMHOS/CM), and Net Delta Outflow (TAF). "
        "One row per (variable_code × water_month). "
        "Use the category filter to request a subset."
    ),
)
async def get_delta_monthly(
    scenario_id: str,
    category: Optional[str] = Query(
        None,
        description=(
            "Filter by category: 'x2', 'salinity_compliance', "
            "'salinity_pumps', or 'outflow'. Omit for all."
        ),
    ),
):
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        result = await _fetch_delta_monthly(_db_pool, scenario_id, category)
    except Exception:
        raise HTTPException(status_code=500, detail="Database query failed")

    if not result["data"]:
        raise HTTPException(
            status_code=404,
            detail=f"No delta monthly statistics found for scenario '{scenario_id}'",
        )

    return _json_response(result, _CACHE_MAX_AGE_STATS)
