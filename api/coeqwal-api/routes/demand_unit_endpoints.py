"""
Urban Demand Unit Statistics API Endpoints.

Provides statistics for urban demand units (tier matrix DUs):
- Grouped demand unit list for dropdown population
- Monthly delivery + shortage (merged in one payload per DU)
- Period-of-record summary

71 canonical CWS demand units organized by extraction category:
- var_wba: WBA-style units with DL_* delivery (40 units)
- var_gw_only: Groundwater-only units (3 units)
- var_swp_contractor: SWP contractor PMI deliveries (11 units)
- var_named_locality: Named localities with D_* arcs (15 units)
- var_missing: No CalSim variables found (2 units)

Note: SBA036 and SCVWD are aliases for the same Santa Clara Valley WD.

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

# Database pool - set by main.py at startup
_db_pool = None

# In-process response caches
# Per-scenario stats: ~71 DUs * 19 scenarios * 2 routes = ~2,700 entries
_stats_cache = make_ttl_cache("demand_unit_stats", maxsize=4000)
_static_cache = make_ttl_cache("demand_unit_static", maxsize=20)


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


def _label_from(community_agency: Any, du_id: Any) -> str:
    """Pick a non-empty display label, falling back to du_id when needed."""
    label = safe_str(community_agency)
    if label:
        return label
    return safe_str(du_id) or ""


# =============================================================================
# DEMAND UNIT DIRECTORY
# =============================================================================


@router.get(
    "/demand-units",
    summary="List urban demand units",
    description=(
        "Returns the tier-matrix urban demand units with their classification, "
        "groundwater/surface-water flags, and CalSim variable names. "
        "Use the `group` query parameter to filter by extraction category."
    ),
)
async def list_demand_units(
    group: Optional[str] = Query(
        None, description="Filter by group short_code (e.g., var_wba, var_swp_contractor)"
    ),
):
    """List urban demand units with optional group filter."""
    cache_key = f"list:{group or ''}"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            if group:
                query = """
                    SELECT
                        e.du_id,
                        e.hydrologic_region,
                        e.community_agency,
                        e.du_class,
                        e.cs3_type,
                        e.gw,
                        e.sw,
                        v.variable_type,
                        v.delivery_variable,
                        v.shortage_variable
                    FROM du_urban_group_member gm
                    JOIN du_urban_group g ON gm.du_urban_group_id = g.id
                    JOIN du_urban_entity e ON gm.du_id = e.du_id
                    LEFT JOIN du_urban_variable v ON gm.du_id = v.du_id
                    WHERE g.short_code = $1
                      AND g.is_active = TRUE
                      AND gm.is_active = TRUE
                    ORDER BY gm.display_order
                """
                rows = await conn.fetch(query, group)
            else:
                query = """
                    SELECT
                        e.du_id,
                        e.hydrologic_region,
                        e.community_agency,
                        e.du_class,
                        e.cs3_type,
                        e.gw,
                        e.sw,
                        v.variable_type,
                        v.delivery_variable,
                        v.shortage_variable
                    FROM du_urban_group_member gm
                    JOIN du_urban_group g ON gm.du_urban_group_id = g.id
                    JOIN du_urban_entity e ON gm.du_id = e.du_id
                    LEFT JOIN du_urban_variable v ON gm.du_id = v.du_id
                    WHERE g.short_code = 'tier'
                      AND g.is_active = TRUE
                    ORDER BY gm.display_order
                """
                rows = await conn.fetch(query)
    except Exception as e:
        log.error(f"demand-units list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    demand_units = [
        {
            "du_id": safe_str(row["du_id"]),
            # `label` is the uniform entity-display field. `community_agency` is
            # preserved alongside for callers that already read it
            "label": _label_from(row["community_agency"], row["du_id"]),
            "hydrologic_region": safe_str(row["hydrologic_region"]),
            "community_agency": safe_str(row["community_agency"]),
            "du_class": safe_str(row["du_class"]),
            "cs3_type": safe_str(row["cs3_type"]),
            "gw": row["gw"],
            "sw": row["sw"],
            "variable_type": safe_str(row["variable_type"]),
            "delivery_variable": safe_str(row["delivery_variable"]),
            "shortage_variable": safe_str(row["shortage_variable"]),
        }
        for row in rows
    ]

    result = {"demand_units": demand_units, "count": len(demand_units)}
    _static_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


# =============================================================================
# MONTHLY DELIVERY + SHORTAGE  (merged)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/demand-units/monthly",
    summary="Monthly delivery and shortage for urban demand units",
    description=(
        "Returns per-DU monthly percentile bands for delivery and shortage "
        "in one payload. Each DU entry carries both `monthly_delivery` and "
        "`monthly_shortage`, keyed by water_month (1=October ... 12=September)."
    ),
)
async def get_du_monthly(
    scenario_id: str,
    du_id: Optional[str] = Query(
        None, description="Comma-separated DU IDs to filter"
    ),
    group: Optional[str] = Query(
        None, description="Filter by group short_code (e.g., var_wba)"
    ),
):
    """Get monthly delivery + shortage statistics for urban demand units."""
    cache_key = f"monthly:{scenario_id}:{du_id or ''}:{group or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            delivery_rows = await _fetch_du_monthly_rows(
                conn,
                table="du_delivery_monthly",
                scenario_id=scenario_id,
                du_id=du_id,
                group=group,
                metric_columns=[
                    "delivery_avg_taf",
                    "delivery_cv",
                    "demand_avg_taf",
                    "percent_of_demand_avg",
                ],
            )
            shortage_rows = await _fetch_du_monthly_rows(
                conn,
                table="du_shortage_monthly",
                scenario_id=scenario_id,
                du_id=du_id,
                group=group,
                metric_columns=[
                    "shortage_avg_taf",
                    "shortage_cv",
                    "shortage_frequency_pct",
                ],
            )
    except Exception as e:
        log.error(f"demand-units monthly query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    demand_units: Dict[str, Dict[str, Any]] = {}

    def _ensure(du: str, row: Any) -> Dict[str, Any]:
        if du not in demand_units:
            demand_units[du] = {
                "label": _label_from(row["community_agency"], du),
                "community_agency": safe_str(row["community_agency"]),
                "hydrologic_region": safe_str(row["hydrologic_region"]),
                "monthly_delivery": {},
                "monthly_shortage": {},
            }
        return demand_units[du]

    for row in delivery_rows:
        du = row["du_id"]
        entry = _ensure(du, row)
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
        du = row["du_id"]
        entry = _ensure(du, row)
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

    if not demand_units:
        raise HTTPException(
            status_code=404,
            detail=f"No monthly statistics found for scenario '{scenario_id}'",
        )

    result = {
        "scenario_id": scenario_id,
        "demand_units": demand_units,
        "count": len(demand_units),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


async def _fetch_du_monthly_rows(
    conn,
    *,
    table: str,
    scenario_id: str,
    du_id: Optional[str],
    group: Optional[str],
    metric_columns: List[str],
) -> List[Any]:
    """Fetch monthly rows for the delivery or shortage table.

    Composes one of two query shapes: the group-filtered variant joins through
    the urban group membership table so callers can scope by extraction
    category (`var_wba`, `var_swp_contractor`, ...). The default variant
    optionally narrows by `du_id`.
    """
    common_columns = ", ".join(
        [
            "m.du_id",
            "e.community_agency",
            "e.hydrologic_region",
            "m.water_month",
            *[f"m.{c}" for c in metric_columns],
            "m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100",
            "m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95",
            "m.sample_count",
        ]
    )

    if group:
        query = f"""
            SELECT {common_columns}
            FROM {table} m
            JOIN du_urban_group_member gm ON m.du_id = gm.du_id
            JOIN du_urban_group g ON gm.du_urban_group_id = g.id
            LEFT JOIN du_urban_entity e ON m.du_id = e.du_id
            WHERE m.scenario_short_code = $1
              AND g.short_code = $2
              AND g.is_active = TRUE
            ORDER BY gm.display_order, m.water_month
        """
        return await conn.fetch(query, scenario_id, group)

    query = f"""
        SELECT {common_columns}
        FROM {table} m
        LEFT JOIN du_urban_entity e ON m.du_id = e.du_id
        WHERE m.scenario_short_code = $1
    """
    params: List[Any] = [scenario_id]
    if du_id:
        ids = [d.strip() for d in du_id.split(",")]
        query += f" AND m.du_id = ANY(${len(params) + 1})"
        params.append(ids)
    query += " ORDER BY m.du_id, m.water_month"
    return await conn.fetch(query, *params)


# =============================================================================
# PERIOD SUMMARY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/demand-units/period-summary",
    summary="Period-of-record summary for urban demand units",
    description=(
        "Returns annual averages, reliability, demand, and delivery/shortage "
        "exceedance values per urban demand unit for one scenario."
    ),
)
async def get_du_period_summary(
    scenario_id: str,
    du_id: Optional[str] = Query(
        None, description="Comma-separated DU IDs to filter"
    ),
    group: Optional[str] = Query(
        None, description="Filter by group short_code (e.g., var_wba)"
    ),
):
    """Get period-of-record summary for urban demand units."""
    cache_key = f"period:{scenario_id}:{du_id or ''}:{group or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    period_columns = """
        p.du_id,
        e.community_agency,
        e.hydrologic_region,
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
        p.avg_pct_demand_met,
        p.annual_demand_avg_taf
    """

    try:
        async with _db_pool.acquire() as conn:
            if group:
                query = f"""
                    SELECT {period_columns}
                    FROM du_period_summary p
                    JOIN du_urban_group_member gm ON p.du_id = gm.du_id
                    JOIN du_urban_group g ON gm.du_urban_group_id = g.id
                    LEFT JOIN du_urban_entity e ON p.du_id = e.du_id
                    WHERE p.scenario_short_code = $1
                      AND g.short_code = $2
                      AND g.is_active = TRUE
                    ORDER BY gm.display_order
                """
                rows = await conn.fetch(query, scenario_id, group)
            else:
                query = f"""
                    SELECT {period_columns}
                    FROM du_period_summary p
                    LEFT JOIN du_urban_entity e ON p.du_id = e.du_id
                    WHERE p.scenario_short_code = $1
                """
                params: List[Any] = [scenario_id]
                if du_id:
                    ids = [d.strip() for d in du_id.split(",")]
                    query += f" AND p.du_id = ANY(${len(params) + 1})"
                    params.append(ids)
                query += " ORDER BY p.du_id"
                rows = await conn.fetch(query, *params)
    except Exception as e:
        log.error(f"demand-units period-summary query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No period summary found for scenario '{scenario_id}'",
        )

    demand_units: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        du = row["du_id"]
        demand_units[du] = {
            "label": _label_from(row["community_agency"], du),
            "community_agency": safe_str(row["community_agency"]),
            "hydrologic_region": safe_str(row["hydrologic_region"]),
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
            "avg_pct_demand_met": safe_float(row["avg_pct_demand_met"]),
            "annual_demand_avg_taf": safe_float(row["annual_demand_avg_taf"]),
        }

    result = {
        "scenario_id": scenario_id,
        "demand_units": demand_units,
        "count": len(demand_units),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())
