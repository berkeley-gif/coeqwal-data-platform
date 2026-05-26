"""
cws_aggregate_endpoints.py

System-level CWS / M&I project rollups (6 aggregates):
- swp_total / swp_nod / swp_sod  (State Water Project, all + N/S split)
- cvp_nod / cvp_sod              (Central Valley Project, N/S split)
- mwd                            (Metropolitan Water District)

Each aggregate is a CalSim variable that already sums many demand units
(e.g. `DEL_SWP_PMI`, `DEL_CVP_PMI_N`, `DEL_SWP_MWD`). Use these endpoints
when you want the project-level rollup directly, instead of summing
per-DU `du_delivery_monthly` rows.

Routes:
  GET /cws-aggregates                                   List
  GET /scenarios/{id}/cws-aggregates/monthly            Merged: delivery + shortage
  GET /scenarios/{id}/cws-aggregates/period-summary     Period summary

The merged monthly + period helpers in this module are also called by the
batch endpoint (`/api/statistics/batch`) so the SQL lives in one place.

Caching: shared in-process TTL helper (default 5 min, env-driven via
API_CACHE_TTL_SECONDS). All responses set a matching Cache-Control max-age.
"""

import logging
from typing import Any, Dict, Optional

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

# 6 aggregates * 19 scenarios * 2 routes -> ~230 entries
_stats_cache = make_ttl_cache("cws_aggregate_stats", maxsize=1000)
_static_cache = make_ttl_cache("cws_aggregate_static", maxsize=10)


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


# =============================================================================
# SHARED HELPERS (also used by the batch endpoint)
# =============================================================================


async def _fetch_cws_aggregate_monthly_rows(conn, scenario_id: str):
    """Run the monthly query and return raw asyncpg rows."""
    query = """
        SELECT
            e.short_code,
            e.label,
            e.project,
            e.region,
            m.water_month,
            m.delivery_avg_taf,
            m.delivery_cv,
            m.delivery_q0, m.delivery_q10, m.delivery_q30, m.delivery_q50,
            m.delivery_q70, m.delivery_q90, m.delivery_q100,
            m.delivery_exc_p5, m.delivery_exc_p10, m.delivery_exc_p25,
            m.delivery_exc_p50, m.delivery_exc_p75, m.delivery_exc_p90,
            m.delivery_exc_p95,
            m.shortage_avg_taf,
            m.shortage_cv,
            m.shortage_frequency_pct,
            m.shortage_q0, m.shortage_q10, m.shortage_q30, m.shortage_q50,
            m.shortage_q70, m.shortage_q90, m.shortage_q100,
            m.demand_avg_taf,
            m.percent_of_demand_avg,
            m.sample_count
        FROM cws_aggregate_monthly m
        JOIN cws_aggregate_entity e ON m.cws_aggregate_id = e.id
        WHERE m.scenario_short_code = $1 AND e.is_active = TRUE
        ORDER BY e.display_order, m.water_month
    """
    return await conn.fetch(query, scenario_id)


def _shape_cws_aggregate_monthly(rows) -> Dict[str, Dict[str, Any]]:
    """Group asyncpg monthly rows by short_code into the response shape."""
    aggregates: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        short_code = row["short_code"]
        if short_code not in aggregates:
            aggregates[short_code] = {
                "label": safe_str(row["label"]),
                "project": safe_str(row["project"]),
                "region": safe_str(row["region"]),
                "monthly_delivery": {},
                "monthly_shortage": {},
            }
        wm = str(row["water_month"])
        aggregates[short_code]["monthly_delivery"][wm] = {
            "avg_taf": safe_float(row["delivery_avg_taf"]),
            "cv": safe_float(row["delivery_cv"]),
            "q0": safe_float(row["delivery_q0"]),
            "q10": safe_float(row["delivery_q10"]),
            "q30": safe_float(row["delivery_q30"]),
            "q50": safe_float(row["delivery_q50"]),
            "q70": safe_float(row["delivery_q70"]),
            "q90": safe_float(row["delivery_q90"]),
            "q100": safe_float(row["delivery_q100"]),
            "exc_p5": safe_float(row["delivery_exc_p5"]),
            "exc_p10": safe_float(row["delivery_exc_p10"]),
            "exc_p25": safe_float(row["delivery_exc_p25"]),
            "exc_p50": safe_float(row["delivery_exc_p50"]),
            "exc_p75": safe_float(row["delivery_exc_p75"]),
            "exc_p90": safe_float(row["delivery_exc_p90"]),
            "exc_p95": safe_float(row["delivery_exc_p95"]),
            "demand_avg_taf": safe_float(row["demand_avg_taf"]),
            "percent_of_demand": safe_float(row["percent_of_demand_avg"]),
            "sample_count": safe_int(row["sample_count"]),
        }
        aggregates[short_code]["monthly_shortage"][wm] = {
            "avg_taf": safe_float(row["shortage_avg_taf"]),
            "cv": safe_float(row["shortage_cv"]),
            "frequency_pct": safe_float(row["shortage_frequency_pct"]),
            "q0": safe_float(row["shortage_q0"]),
            "q10": safe_float(row["shortage_q10"]),
            "q30": safe_float(row["shortage_q30"]),
            "q50": safe_float(row["shortage_q50"]),
            "q70": safe_float(row["shortage_q70"]),
            "q90": safe_float(row["shortage_q90"]),
            "q100": safe_float(row["shortage_q100"]),
        }
    return aggregates


async def _fetch_cws_aggregate_period_rows(conn, scenario_id: str):
    """Run the period-summary query and return raw asyncpg rows."""
    query = """
        SELECT
            e.short_code,
            e.label,
            e.project,
            e.region,
            p.simulation_start_year,
            p.simulation_end_year,
            p.total_years,
            p.annual_delivery_avg_taf,
            p.annual_delivery_cv,
            p.annual_delivery_min_taf,
            p.annual_delivery_max_taf,
            p.delivery_exc_p5, p.delivery_exc_p10, p.delivery_exc_p25,
            p.delivery_exc_p50, p.delivery_exc_p75, p.delivery_exc_p90,
            p.delivery_exc_p95,
            p.annual_shortage_avg_taf,
            p.shortage_years_count,
            p.shortage_frequency_pct,
            p.shortage_exc_p5, p.shortage_exc_p10, p.shortage_exc_p25,
            p.shortage_exc_p50, p.shortage_exc_p75, p.shortage_exc_p90,
            p.shortage_exc_p95,
            p.annual_demand_avg_taf,
            p.reliability_pct,
            p.avg_pct_allocation_met,
            p.avg_pct_demand_met
        FROM cws_aggregate_period_summary p
        JOIN cws_aggregate_entity e ON p.cws_aggregate_id = e.id
        WHERE p.scenario_short_code = $1 AND e.is_active = TRUE
        ORDER BY e.display_order
    """
    return await conn.fetch(query, scenario_id)


def _shape_cws_aggregate_period(rows) -> Dict[str, Dict[str, Any]]:
    """Group asyncpg period-summary rows by short_code."""
    aggregates: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        aggregates[row["short_code"]] = {
            "label": safe_str(row["label"]),
            "project": safe_str(row["project"]),
            "region": safe_str(row["region"]),
            "simulation_start_year": safe_int(row["simulation_start_year"]),
            "simulation_end_year": safe_int(row["simulation_end_year"]),
            "total_years": safe_int(row["total_years"]),
            "annual_delivery_avg_taf": safe_float(row["annual_delivery_avg_taf"]),
            "annual_delivery_cv": safe_float(row["annual_delivery_cv"]),
            "annual_delivery_min_taf": safe_float(row["annual_delivery_min_taf"]),
            "annual_delivery_max_taf": safe_float(row["annual_delivery_max_taf"]),
            "delivery_exceedance": {
                "p5": safe_float(row["delivery_exc_p5"]),
                "p10": safe_float(row["delivery_exc_p10"]),
                "p25": safe_float(row["delivery_exc_p25"]),
                "p50": safe_float(row["delivery_exc_p50"]),
                "p75": safe_float(row["delivery_exc_p75"]),
                "p90": safe_float(row["delivery_exc_p90"]),
                "p95": safe_float(row["delivery_exc_p95"]),
            },
            "annual_shortage_avg_taf": safe_float(row["annual_shortage_avg_taf"]),
            "shortage_years_count": safe_int(row["shortage_years_count"]),
            "shortage_frequency_pct": safe_float(row["shortage_frequency_pct"]),
            "shortage_exceedance": {
                "p5": safe_float(row["shortage_exc_p5"]),
                "p10": safe_float(row["shortage_exc_p10"]),
                "p25": safe_float(row["shortage_exc_p25"]),
                "p50": safe_float(row["shortage_exc_p50"]),
                "p75": safe_float(row["shortage_exc_p75"]),
                "p90": safe_float(row["shortage_exc_p90"]),
                "p95": safe_float(row["shortage_exc_p95"]),
            },
            "annual_demand_avg_taf": safe_float(row["annual_demand_avg_taf"]),
            "reliability_pct": safe_float(row["reliability_pct"]),
            "avg_pct_allocation_met": safe_float(row["avg_pct_allocation_met"]),
            "avg_pct_demand_met": safe_float(row["avg_pct_demand_met"]),
        }
    return aggregates


async def fetch_cws_aggregates_monthly_payload(
    pool, scenario_id: str
) -> Dict[str, Any]:
    """Public helper for the batch endpoint. Returns the standalone shape."""
    async with pool.acquire() as conn:
        rows = await _fetch_cws_aggregate_monthly_rows(conn, scenario_id)
    aggregates = _shape_cws_aggregate_monthly(rows)
    return {
        "scenario_id": scenario_id,
        "aggregates": aggregates,
        "count": len(aggregates),
    }


async def fetch_cws_aggregates_period_payload(
    pool, scenario_id: str
) -> Dict[str, Any]:
    """Public helper for the batch endpoint. Returns the standalone shape."""
    async with pool.acquire() as conn:
        rows = await _fetch_cws_aggregate_period_rows(conn, scenario_id)
    aggregates = _shape_cws_aggregate_period(rows)
    return {
        "scenario_id": scenario_id,
        "aggregates": aggregates,
        "count": len(aggregates),
    }


# =============================================================================
# HTTP ROUTES
# =============================================================================


@router.get(
    "/cws-aggregates",
    summary="List CWS / M&I aggregates",
    description=(
        "Returns the 6 active CWS aggregate entities (SWP total + N/S, "
        "CVP N/S, MWD) with their CalSim delivery and shortage variable names."
    ),
)
async def list_cws_aggregates():
    """List CWS aggregate entities."""
    cache_key = "cws_aggregates:list"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            query = """
                SELECT
                    short_code,
                    label,
                    description,
                    project,
                    region,
                    delivery_variable,
                    shortage_variable,
                    display_order
                FROM cws_aggregate_entity
                WHERE is_active = TRUE
                ORDER BY display_order
            """
            rows = await conn.fetch(query)
    except Exception as e:
        log.error(f"cws-aggregates list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    aggregates = [
        {
            "short_code": safe_str(row["short_code"]),
            "label": safe_str(row["label"]),
            "description": safe_str(row["description"]),
            "project": safe_str(row["project"]),
            "region": safe_str(row["region"]),
            "delivery_variable": safe_str(row["delivery_variable"]),
            "shortage_variable": safe_str(row["shortage_variable"]),
            "display_order": safe_int(row["display_order"]),
        }
        for row in rows
    ]

    result = {"aggregates": aggregates, "count": len(aggregates)}
    _static_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


@router.get(
    "/scenarios/{scenario_id}/cws-aggregates/monthly",
    summary="Monthly delivery and shortage for CWS aggregates",
    description=(
        "Returns per-aggregate monthly percentile bands for delivery and "
        "shortage in one payload, keyed by short_code. Each aggregate carries "
        "`monthly_delivery` and `monthly_shortage` maps keyed by water_month "
        "(0 = annual rollup, 1=October ... 12=September)."
    ),
)
async def get_cws_aggregate_monthly(
    scenario_id: str,
    aggregate: Optional[str] = Query(
        None,
        description="Comma-separated short codes (e.g., 'swp_total,mwd'). Defaults to all.",
    ),
):
    """Get monthly CWS aggregate delivery + shortage data."""
    cache_key = f"monthly:{scenario_id}:{aggregate or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            rows = await _fetch_cws_aggregate_monthly_rows(conn, scenario_id)
    except Exception as e:
        log.error(f"cws-aggregates monthly query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No CWS aggregate monthly data found for scenario '{scenario_id}'",
        )

    aggregates = _shape_cws_aggregate_monthly(rows)

    if aggregate:
        wanted = {a.strip() for a in aggregate.split(",") if a.strip()}
        aggregates = {k: v for k, v in aggregates.items() if k in wanted}
        if not aggregates:
            raise HTTPException(
                status_code=404,
                detail=f"No matching aggregates for filter '{aggregate}'",
            )

    result = {
        "scenario_id": scenario_id,
        "aggregates": aggregates,
        "count": len(aggregates),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


@router.get(
    "/scenarios/{scenario_id}/cws-aggregates/period-summary",
    summary="Period-of-record summary for CWS aggregates",
    description=(
        "Returns annual averages, exceedance bands, and reliability metrics "
        "per CWS aggregate for the full simulation period."
    ),
)
async def get_cws_aggregate_period_summary(
    scenario_id: str,
    aggregate: Optional[str] = Query(
        None, description="Comma-separated short codes. Defaults to all."
    ),
):
    """Get period-of-record summary for CWS aggregates."""
    cache_key = f"period:{scenario_id}:{aggregate or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            rows = await _fetch_cws_aggregate_period_rows(conn, scenario_id)
    except Exception as e:
        log.error(f"cws-aggregates period-summary query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No CWS aggregate period summary found for scenario '{scenario_id}'",
        )

    aggregates = _shape_cws_aggregate_period(rows)

    if aggregate:
        wanted = {a.strip() for a in aggregate.split(",") if a.strip()}
        aggregates = {k: v for k, v in aggregates.items() if k in wanted}
        if not aggregates:
            raise HTTPException(
                status_code=404,
                detail=f"No matching aggregates for filter '{aggregate}'",
            )

    result = {
        "scenario_id": scenario_id,
        "aggregates": aggregates,
        "count": len(aggregates),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())
