"""
M&I Contractor API Endpoints.

Provides metadata and statistics for SWP/CVP M&I water contractors:
- Contractor directory                  (mi_contractor table, 30 active rows)
- Monthly delivery + shortage           (mi_delivery_monthly + mi_shortage_monthly)
- Period-of-record summary              (mi_contractor_period_summary)

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

# Database pool - set by main.py at startup
_db_pool = None

# In-process response caches
# Per-scenario stats: 30 contractors * 19 scenarios * 2 routes = ~1,140 entries
_stats_cache = make_ttl_cache("mi_contractor_stats", maxsize=2000)
# Static directory list: a handful of filter permutations
_static_cache = make_ttl_cache("mi_contractor_static", maxsize=10)


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
# CONTRACTOR DIRECTORY
# =============================================================================


@router.get(
    "/mi-contractors",
    summary="List M&I contractors",
    description=(
        "Returns active M&I contractors from the `mi_contractor` table "
        "with their project, region, type, and full contract amount in TAF."
    ),
)
async def list_mi_contractors():
    """List all active M&I contractors."""
    cache_key = "list:all"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    short_code,
                    contractor_name,
                    project,
                    region,
                    contractor_type,
                    contract_amount_taf
                FROM mi_contractor
                WHERE is_active = TRUE
                ORDER BY short_code
                """
            )
    except Exception as e:
        log.error(f"mi-contractors list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    contractors = [
        {
            "short_code": safe_str(row["short_code"]),
            # `name` kept for backward compat with any existing consumer.
            # `label` is the uniform entity-display field used across all
            # statistics endpoints.
            "name": safe_str(row["contractor_name"]),
            "label": safe_str(row["contractor_name"]),
            "project": safe_str(row["project"]),
            "region": safe_str(row["region"]),
            "contractor_type": safe_str(row["contractor_type"]),
            "contract_amount_taf": safe_float(row["contract_amount_taf"]),
        }
        for row in rows
    ]

    result = {"contractors": contractors, "count": len(contractors)}
    _static_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


# =============================================================================
# MONTHLY DELIVERY + SHORTAGE
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/mi-contractors/monthly",
    summary="Monthly delivery and shortage for M&I contractors",
    description=(
        "Returns per-contractor monthly percentile bands for delivery and "
        "shortage in one payload. Each contractor entry carries both "
        "`monthly_delivery` and `monthly_shortage`, keyed by water_month "
        "(1=October ... 12=September)."
    ),
)
async def get_mi_monthly(
    scenario_id: str,
    contractor: Optional[str] = Query(
        None, description="Comma-separated contractor short_codes to filter"
    ),
):
    """Get monthly delivery + shortage statistics for M&I contractors."""
    cache_key = f"monthly:{scenario_id}:{contractor or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    codes = [c.strip() for c in contractor.split(",")] if contractor else None

    try:
        async with _db_pool.acquire() as conn:
            delivery_query = """
                SELECT
                    m.mi_contractor_code,
                    c.contractor_name,
                    m.water_month,
                    m.delivery_avg_taf,
                    m.delivery_cv,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.demand_avg_taf, m.percent_of_demand_avg,
                    m.sample_count
                FROM mi_delivery_monthly m
                LEFT JOIN mi_contractor c ON m.mi_contractor_code = c.short_code
                WHERE m.scenario_short_code = $1
            """
            shortage_query = """
                SELECT
                    m.mi_contractor_code,
                    c.contractor_name,
                    m.water_month,
                    m.shortage_avg_taf,
                    m.shortage_cv,
                    m.shortage_frequency_pct,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.sample_count
                FROM mi_shortage_monthly m
                LEFT JOIN mi_contractor c ON m.mi_contractor_code = c.short_code
                WHERE m.scenario_short_code = $1
            """
            params: list = [scenario_id]
            if codes:
                delivery_query += " AND m.mi_contractor_code = ANY($2)"
                shortage_query += " AND m.mi_contractor_code = ANY($2)"
                params.append(codes)
            delivery_query += " ORDER BY m.mi_contractor_code, m.water_month"
            shortage_query += " ORDER BY m.mi_contractor_code, m.water_month"

            delivery_rows = await conn.fetch(delivery_query, *params)
            shortage_rows = await conn.fetch(shortage_query, *params)
    except Exception as e:
        log.error(f"mi-contractors monthly query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    contractors: Dict[str, Dict[str, Any]] = {}

    def _ensure(code: str, label: Any) -> Dict[str, Any]:
        if code not in contractors:
            contractors[code] = {
                "label": safe_str(label),
                "monthly_delivery": {},
                "monthly_shortage": {},
            }
        return contractors[code]

    for row in delivery_rows:
        code = row["mi_contractor_code"]
        entry = _ensure(code, row["contractor_name"])
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
            "demand_avg_taf": safe_float(row["demand_avg_taf"]),
            "percent_of_demand": safe_float(row["percent_of_demand_avg"]),
            "sample_count": safe_int(row["sample_count"]),
        }

    for row in shortage_rows:
        code = row["mi_contractor_code"]
        entry = _ensure(code, row["contractor_name"])
        entry["monthly_shortage"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["shortage_avg_taf"]),
            "cv": safe_float(row["shortage_cv"]),
            "frequency_pct": safe_float(row["shortage_frequency_pct"]),
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

    if not contractors:
        raise HTTPException(
            status_code=404,
            detail=f"No monthly statistics found for scenario '{scenario_id}'",
        )

    result = {
        "scenario_id": scenario_id,
        "contractors": contractors,
        "count": len(contractors),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


# =============================================================================
# PERIOD SUMMARY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/mi-contractors/period-summary",
    summary="Period-of-record summary for M&I contractors",
    description=(
        "Returns annual averages, reliability, demand, and delivery/shortage "
        "exceedance values per M&I contractor for one scenario."
    ),
)
async def get_mi_period_summary(
    scenario_id: str,
    contractor: Optional[str] = Query(
        None, description="Comma-separated contractor short_codes to filter"
    ),
):
    """Get period-of-record summary for M&I contractors."""
    cache_key = f"period:{scenario_id}:{contractor or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    codes = [c.strip() for c in contractor.split(",")] if contractor else None

    try:
        async with _db_pool.acquire() as conn:
            query = """
                SELECT
                    p.mi_contractor_code,
                    c.contractor_name,
                    p.simulation_start_year,
                    p.simulation_end_year,
                    p.total_years,
                    p.annual_delivery_avg_taf,
                    p.annual_delivery_cv,
                    p.delivery_exc_p5, p.delivery_exc_p10, p.delivery_exc_p25,
                    p.delivery_exc_p50, p.delivery_exc_p75, p.delivery_exc_p90, p.delivery_exc_p95,
                    p.annual_shortage_avg_taf,
                    p.shortage_years_count,
                    p.shortage_frequency_pct,
                    p.shortage_exc_p5, p.shortage_exc_p10, p.shortage_exc_p25,
                    p.shortage_exc_p50, p.shortage_exc_p75, p.shortage_exc_p90, p.shortage_exc_p95,
                    p.reliability_pct,
                    p.annual_demand_avg_taf,
                    p.avg_pct_demand_met
                FROM mi_contractor_period_summary p
                LEFT JOIN mi_contractor c ON p.mi_contractor_code = c.short_code
                WHERE p.scenario_short_code = $1
            """
            params: list = [scenario_id]
            if codes:
                query += " AND p.mi_contractor_code = ANY($2)"
                params.append(codes)
            query += " ORDER BY p.mi_contractor_code"

            rows = await conn.fetch(query, *params)
    except Exception as e:
        log.error(f"mi-contractors period-summary query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No period summary found for scenario '{scenario_id}'",
        )

    contractors: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = row["mi_contractor_code"]
        contractors[code] = {
            "label": safe_str(row["contractor_name"]),
            "simulation_start_year": safe_int(row["simulation_start_year"]),
            "simulation_end_year": safe_int(row["simulation_end_year"]),
            "total_years": safe_int(row["total_years"]),
            "annual_delivery_avg_taf": safe_float(row["annual_delivery_avg_taf"]),
            "annual_delivery_cv": safe_float(row["annual_delivery_cv"]),
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
            "reliability_pct": safe_float(row["reliability_pct"]),
            "annual_demand_avg_taf": safe_float(row["annual_demand_avg_taf"]),
            "avg_pct_demand_met": safe_float(row["avg_pct_demand_met"]),
        }

    result = {
        "scenario_id": scenario_id,
        "contractors": contractors,
        "count": len(contractors),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())
