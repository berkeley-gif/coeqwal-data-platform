"""
Wildlife Refuge Demand Unit Statistics API Endpoints.

CalSim variable semantics:
  AWO_{DU_ID} = Applied Water Output = DEMAND (from SV input, TAF)
  DN_{DU_ID}  = Net Surface Water Delivery (from deliveries CSV, TAF)
  Shortage    = max(demand - delivery, 0). Derived, no native CalSim variable

Provides statistics for 18 wildlife refuge demand units:
  GET /api/statistics/refuge-demand-units                              List
  GET /api/statistics/scenarios/{id}/refuge-demand-units/monthly       Merged
  GET /api/statistics/scenarios/{id}/refuge-demand-units/period-summary

Water months: 1=October ... 12=September
Values: TAF (thousand acre-feet), percentages, or correlation coefficients

Caching: shared in-process TTL helper (default 5 min, env-driven via
API_CACHE_TTL_SECONDS). All responses set a matching Cache-Control max-age.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from routes._common import (
    api_cache_max_age,
    make_ttl_cache,
    safe_float,
    safe_int,
    safe_str,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

_db_pool = None

# In-process response caches
# 18 DUs * 19 scenarios * 2 routes = ~700 entries
_stats_cache = make_ttl_cache("refuge_stats", maxsize=2000)
_static_cache = make_ttl_cache("refuge_static", maxsize=20)


def set_db_pool(pool):
    """Set the database connection pool."""
    global _db_pool
    _db_pool = pool


def _json_response(data: Dict[str, Any], max_age: int) -> JSONResponse:
    """Wrap a dict in a JSONResponse with Cache-Control headers."""
    return JSONResponse(
        content=data,
        headers={"Cache-Control": f"public, max-age={max_age}"},
    )


def _label_from(refuge_or_wildlife_area: Any, du_id: Any) -> str:
    """Pick a non-empty display label, falling back to du_id when needed."""
    label = safe_str(refuge_or_wildlife_area)
    if label:
        return label
    return safe_str(du_id) or ""


# =============================================================================
# LIST REFUGE DEMAND UNITS
# =============================================================================


@router.get(
    "/refuge-demand-units",
    summary="List wildlife refuge demand units",
    description=(
        "Returns the 18 wildlife refuge demand units with hydrologic region, "
        "CS3 type, refuge name, managing agency, and provider."
    ),
)
async def list_refuge_demand_units(
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region (SAC, SJR, TULARE)"
    ),
    cs3_type: Optional[str] = Query(
        None, description="Filter by CS3 type (PR=Priority Refuge, NR=Non-priority Refuge)"
    ),
):
    """List wildlife refuge demand units with optional filters."""
    cache_key = f"list:{region or ''}:{cs3_type or ''}"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            query = """
                SELECT
                    du_id,
                    wba_id,
                    hydrologic_region,
                    cs3_type,
                    refuge_or_wildlife_area,
                    managed_by,
                    provider,
                    sw
                FROM du_refuge_entity
                WHERE is_active = TRUE
            """
            params: List[Any] = []
            if region:
                params.append(region.upper())
                query += f" AND UPPER(hydrologic_region) = ${len(params)}"
            if cs3_type:
                params.append(cs3_type.upper())
                query += f" AND UPPER(cs3_type) = ${len(params)}"
            query += " ORDER BY hydrologic_region, du_id"
            rows = await conn.fetch(query, *params)
    except Exception as e:
        log.error(f"refuge-demand-units list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    demand_units = [
        {
            "du_id": safe_str(row["du_id"]),
            # `label` is the uniform entity-display field. The original
            # `refuge_or_wildlife_area` is kept alongside for backward compat
            "label": _label_from(row["refuge_or_wildlife_area"], row["du_id"]),
            "wba_id": safe_str(row["wba_id"]),
            "hydrologic_region": safe_str(row["hydrologic_region"]),
            "cs3_type": safe_str(row["cs3_type"]),
            "refuge_or_wildlife_area": safe_str(row["refuge_or_wildlife_area"]),
            "managed_by": safe_str(row["managed_by"]),
            "provider": safe_str(row["provider"]),
            "sw": row["sw"],
        }
        for row in rows
    ]

    result = {"demand_units": demand_units, "count": len(demand_units)}
    _static_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


# =============================================================================
# MONTHLY DELIVERY + SHORTAGE  (merged, keyed by DU)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/refuge-demand-units/monthly",
    summary="Monthly delivery and shortage for refuge demand units",
    description=(
        "Returns per-DU monthly percentile bands for SW delivery and "
        "derived shortage in one payload. Each DU entry carries both "
        "`monthly_delivery` and `monthly_shortage`, keyed by water_month "
        "(1=October ... 12=September). Shortage = max(demand - delivery, 0)."
    ),
)
async def get_refuge_monthly(
    scenario_id: str,
    du_id: Optional[str] = Query(None, description="Filter to a single demand unit"),
    water_month: Optional[int] = Query(
        None, ge=1, le=12, description="Filter to a specific water month (1=Oct, 12=Sep)"
    ),
):
    """Get monthly delivery + shortage statistics for refuge demand units."""
    cache_key = f"monthly:{scenario_id}:{du_id or ''}:{water_month or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            delivery_query = """
                SELECT
                    m.du_id,
                    e.refuge_or_wildlife_area,
                    m.water_month,
                    m.delivery_avg_taf,
                    m.delivery_cv,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.sample_count
                FROM refuge_du_delivery_monthly m
                LEFT JOIN du_refuge_entity e ON m.du_id = e.du_id
                WHERE m.scenario_short_code = $1
                  AND m.is_active = TRUE
            """
            shortage_query = """
                SELECT
                    m.du_id,
                    e.refuge_or_wildlife_area,
                    m.water_month,
                    m.shortage_avg_taf,
                    m.shortage_cv,
                    m.shortage_pct_avg,
                    m.shortage_pct_cv,
                    m.shortage_frequency_pct,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.sample_count
                FROM refuge_du_shortage_monthly m
                LEFT JOIN du_refuge_entity e ON m.du_id = e.du_id
                WHERE m.scenario_short_code = $1
                  AND m.is_active = TRUE
            """
            params: List[Any] = [scenario_id]
            if du_id:
                params.append(du_id)
                delivery_query += f" AND m.du_id = ${len(params)}"
                shortage_query += f" AND m.du_id = ${len(params)}"
            if water_month is not None:
                params.append(water_month)
                delivery_query += f" AND m.water_month = ${len(params)}"
                shortage_query += f" AND m.water_month = ${len(params)}"
            delivery_query += " ORDER BY m.du_id, m.water_month"
            shortage_query += " ORDER BY m.du_id, m.water_month"

            delivery_rows = await conn.fetch(delivery_query, *params)
            shortage_rows = await conn.fetch(shortage_query, *params)
    except Exception as e:
        log.error(f"refuge monthly query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    demand_units: Dict[str, Dict[str, Any]] = {}

    def _ensure(row: Any) -> Dict[str, Any]:
        du = row["du_id"]
        if du not in demand_units:
            demand_units[du] = {
                "label": _label_from(row["refuge_or_wildlife_area"], du),
                "refuge_or_wildlife_area": safe_str(row["refuge_or_wildlife_area"]),
                "monthly_delivery": {},
                "monthly_shortage": {},
            }
        return demand_units[du]

    for row in delivery_rows:
        entry = _ensure(row)
        entry["monthly_delivery"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["delivery_avg_taf"]),
            "cv": safe_float(row["delivery_cv"]),
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

    for row in shortage_rows:
        entry = _ensure(row)
        entry["monthly_shortage"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["shortage_avg_taf"]),
            "cv": safe_float(row["shortage_cv"]),
            "shortage_pct_avg": safe_float(row["shortage_pct_avg"]),
            "shortage_pct_cv": safe_float(row["shortage_pct_cv"]),
            "shortage_frequency_pct": safe_float(row["shortage_frequency_pct"]),
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

    if not demand_units:
        raise HTTPException(
            status_code=404,
            detail=f"No refuge monthly statistics found for scenario '{scenario_id}'",
        )

    result = {
        "scenario_id": scenario_id,
        "demand_units": demand_units,
        "count": len(demand_units),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


# =============================================================================
# PERIOD SUMMARY  (keyed by DU)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/refuge-demand-units/period-summary",
    summary="Period-of-record summary for refuge demand units",
    description=(
        "Returns annual delivery / shortage averages, annual exceedance bands, "
        "and `reliability_pct_95` per refuge DU. `reliability_pct_95` is the "
        "95th percentile of annual shortage %: in 95 of 100 simulated years "
    ),
)
async def get_refuge_period_summary(
    scenario_id: str,
    du_id: Optional[str] = Query(None, description="Filter to a single demand unit"),
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region (SAC, SJR, TULARE)"
    ),
):
    """Get period-of-record summary for refuge demand units."""
    cache_key = f"period:{scenario_id}:{du_id or ''}:{region or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            query = """
                SELECT
                    ps.du_id,
                    e.refuge_or_wildlife_area,
                    ps.simulation_start_year,
                    ps.simulation_end_year,
                    ps.total_years,
                    ps.annual_delivery_avg_taf,
                    ps.annual_delivery_cv,
                    ps.delivery_exc_p5,
                    ps.delivery_exc_p10,
                    ps.delivery_exc_p25,
                    ps.delivery_exc_p50,
                    ps.delivery_exc_p75,
                    ps.delivery_exc_p90,
                    ps.delivery_exc_p95,
                    ps.annual_shortage_avg_taf,
                    ps.annual_shortage_cv,
                    ps.annual_shortage_pct_avg,
                    ps.annual_shortage_pct_cv,
                    ps.reliability_pct_95
                FROM refuge_du_period_summary ps
                LEFT JOIN du_refuge_entity e ON ps.du_id = e.du_id
                WHERE ps.scenario_short_code = $1
                  AND ps.is_active = TRUE
            """
            params: List[Any] = [scenario_id]
            if du_id:
                params.append(du_id)
                query += f" AND ps.du_id = ${len(params)}"
            if region:
                params.append(region.upper())
                query += f" AND UPPER(e.hydrologic_region) = ${len(params)}"
            query += " ORDER BY ps.du_id"
            rows = await conn.fetch(query, *params)
    except Exception as e:
        log.error(f"refuge period-summary query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No refuge period summary found for scenario '{scenario_id}'",
        )

    demand_units: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        du = row["du_id"]
        demand_units[du] = {
            "label": _label_from(row["refuge_or_wildlife_area"], du),
            "refuge_or_wildlife_area": safe_str(row["refuge_or_wildlife_area"]),
            "simulation_start_year": safe_int(row["simulation_start_year"]),
            "simulation_end_year": safe_int(row["simulation_end_year"]),
            "total_years": safe_int(row["total_years"]),
            "annual_delivery_avg_taf": safe_float(row["annual_delivery_avg_taf"]),
            "annual_delivery_cv": safe_float(row["annual_delivery_cv"]),
            "delivery_exc_p5": safe_float(row["delivery_exc_p5"]),
            "delivery_exc_p10": safe_float(row["delivery_exc_p10"]),
            "delivery_exc_p25": safe_float(row["delivery_exc_p25"]),
            "delivery_exc_p50": safe_float(row["delivery_exc_p50"]),
            "delivery_exc_p75": safe_float(row["delivery_exc_p75"]),
            "delivery_exc_p90": safe_float(row["delivery_exc_p90"]),
            "delivery_exc_p95": safe_float(row["delivery_exc_p95"]),
            "annual_shortage_avg_taf": safe_float(row["annual_shortage_avg_taf"]),
            "annual_shortage_cv": safe_float(row["annual_shortage_cv"]),
            "annual_shortage_pct_avg": safe_float(row["annual_shortage_pct_avg"]),
            "annual_shortage_pct_cv": safe_float(row["annual_shortage_pct_cv"]),
            "reliability_pct_95": safe_float(row["reliability_pct_95"]),
        }

    result = {
        "scenario_id": scenario_id,
        "demand_units": demand_units,
        "count": len(demand_units),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())
