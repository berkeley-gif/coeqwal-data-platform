"""
Agricultural Demand Unit Statistics API Endpoints.

IMPORTANT - CalSim Variable Semantics (from COEQWAL modeler documentation):
- AW_{DU_ID} = Applied Water = DEMAND (from SV input file)
- DN_{DU_ID} = Net Delivery = SURFACE WATER DELIVERY (from DV output file)
- GP_{DU_ID} = Groundwater Pumping (explicit for some DUs)
- Groundwater Pumping = AW - DN (calculated for most DUs)
- GW_SHORT_{DU_ID} = Groundwater RESTRICTION Shortage (COEQWAL-specific)

In CalSim, agricultural demand is assumed to be fully met:
  Demand (AW) = Surface Water Delivery (DN) + Groundwater Pumping (GP)

Routes:
  GET /ag-demand-units                                List
  GET /scenarios/{id}/ag-demand-units/monthly         Merged: demand + sw-delivery
                                                       + gw-pumping + shortage
  GET /scenarios/{id}/ag-demand-units/period-summary  Period summary
  GET /ag-aggregates                                  Aggregate list
  GET /scenarios/{id}/ag-aggregates/monthly           Aggregate monthly delivery
  GET /scenarios/{id}/ag-aggregates/period-summary    Aggregate period summary

Water months: 1=October, 2=November, ..., 12=September
Values: TAF (thousand acre-feet)

Note: Sacramento region DUs do NOT have GW restriction shortage data.

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
# Per-scenario stats: ~144 DUs * 19 scenarios * 2 routes = ~5,500 entries
_stats_cache = make_ttl_cache("ag_du_stats", maxsize=8000)
_static_cache = make_ttl_cache("ag_du_static", maxsize=50)


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


def _label_from(agency: Any, du_id: Any) -> str:
    """Pick a non-empty display label, falling back to du_id when needed."""
    label = safe_str(agency)
    if label:
        return label
    return safe_str(du_id) or ""


# =============================================================================
# LIST AG DEMAND UNITS
# =============================================================================


@router.get(
    "/ag-demand-units",
    summary="List agricultural demand units",
    description=(
        "Returns active agricultural demand units with hydrologic region, "
        "CS3 type, provider, GW/SW flags, acreage, and GIS-data availability."
    ),
)
async def list_ag_demand_units(
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region (SAC, SJR, TULARE)"
    ),
    cs3_type: Optional[str] = Query(
        None, description="Filter by CS3 type (PA, SA, XA, PR, NR, or blank for NA)"
    ),
    provider: Optional[str] = Query(
        None, description="Filter by water provider (CVP, SWP, Reclamation)"
    ),
):
    """List agricultural demand units with optional filters."""
    cache_key = f"list:{region or ''}:{cs3_type or ''}:{provider or ''}"
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
                    agency,
                    provider,
                    gw,
                    sw,
                    total_acres,
                    has_gis_data
                FROM du_agriculture_entity
                WHERE is_active = TRUE
            """
            params: List[Any] = []

            if region:
                query += f" AND hydrologic_region = ${len(params) + 1}"
                params.append(region.upper())

            if cs3_type is not None:
                if cs3_type == "":
                    query += " AND (cs3_type IS NULL OR cs3_type = '')"
                else:
                    query += f" AND cs3_type = ${len(params) + 1}"
                    params.append(cs3_type.upper())

            if provider:
                query += f" AND provider ILIKE ${len(params) + 1}"
                params.append(f"%{provider}%")

            query += " ORDER BY du_id"

            rows = await conn.fetch(query, *params) if params else await conn.fetch(query)
    except Exception as e:
        log.error(f"ag-demand-units list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    demand_units = [
        {
            "du_id": safe_str(row["du_id"]),
            # `label` is the uniform entity-display field. `agency` is preserved
            # alongside for callers that already read it
            "label": _label_from(row["agency"], row["du_id"]),
            "wba_id": safe_str(row["wba_id"]),
            "hydrologic_region": safe_str(row["hydrologic_region"]),
            "cs3_type": safe_str(row["cs3_type"]),
            "agency": safe_str(row["agency"]),
            "provider": safe_str(row["provider"]),
            "gw": row["gw"],
            "sw": row["sw"],
            "total_acres": safe_float(row["total_acres"]),
            "has_gis_data": row["has_gis_data"],
        }
        for row in rows
    ]

    result = {"demand_units": demand_units, "count": len(demand_units)}
    _static_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


# =============================================================================
# MONTHLY: DEMAND + SW DELIVERY + GW PUMPING + SHORTAGE  (merged)
# =============================================================================


# Common SELECT clause shared by all four monthly queries. Each query inlines
# its metric-specific columns and reuses these entity/percentile columns
_COMMON_ENTITY_COLS = """
    m.du_id,
    e.agency,
    e.hydrologic_region,
    e.cs3_type,
    e.provider,
    m.water_month
"""

_COMMON_PERCENTILE_COLS = """
    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
    m.sample_count
"""


def _ag_du_filter_clause(
    params: List[Any],
    du_id: Optional[str],
    region: Optional[str],
    cs3_type: Optional[str],
) -> str:
    """Build the WHERE-suffix shared by every monthly query.

    Appends bound parameters in-place on `params` and returns the SQL fragment
    to append after `WHERE scenario_short_code = $1`.
    """
    clause = ""
    if du_id:
        ids = [d.strip() for d in du_id.split(",")]
        clause += f" AND m.du_id = ANY(${len(params) + 1})"
        params.append(ids)
    if region:
        clause += f" AND e.hydrologic_region = ${len(params) + 1}"
        params.append(region.upper())
    if cs3_type is not None:
        if cs3_type == "":
            clause += " AND (e.cs3_type IS NULL OR e.cs3_type = '')"
        else:
            clause += f" AND e.cs3_type = ${len(params) + 1}"
            params.append(cs3_type.upper())
    return clause


@router.get(
    "/scenarios/{scenario_id}/ag-demand-units/monthly",
    summary="Monthly demand, SW delivery, GW pumping, and shortage for AG DUs",
    description=(
        "Returns four monthly metrics per AG demand unit in one payload:\n"
        "- `monthly_demand` (Applied Water, AW_*)\n"
        "- `monthly_sw_delivery` (Net Delivery, DN_*)\n"
        "- `monthly_gw_pumping` (GP_* or calculated as AW - DN; `is_calculated` flag)\n"
        "- `monthly_shortage` (GW restriction, GW_SHORT_*; SJR/TULARE only)\n\n"
        "Each map is keyed by water_month (1=October ... 12=September)."
    ),
)
async def get_ag_du_monthly(
    scenario_id: str,
    du_id: Optional[str] = Query(
        None, description="Comma-separated DU IDs to filter (e.g., '64_PA1,72_XA1')"
    ),
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region (SAC, SJR, TULARE)"
    ),
    cs3_type: Optional[str] = Query(
        None, description="Filter by CS3 type (PA, SA, XA, etc.)"
    ),
):
    """Get monthly demand + sw-delivery + gw-pumping + shortage stats for AG DUs."""
    cache_key = f"monthly:{scenario_id}:{du_id or ''}:{region or ''}:{cs3_type or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            demand_rows = await _fetch_ag_du_metric(
                conn,
                table="ag_du_demand_monthly",
                metric_cols=["m.demand_avg_taf", "m.demand_cv"],
                scenario_id=scenario_id,
                du_id=du_id,
                region=region,
                cs3_type=cs3_type,
            )
            sw_rows = await _fetch_ag_du_metric(
                conn,
                table="ag_du_sw_delivery_monthly",
                metric_cols=["m.sw_delivery_avg_taf", "m.sw_delivery_cv"],
                scenario_id=scenario_id,
                du_id=du_id,
                region=region,
                cs3_type=cs3_type,
            )
            gw_rows = await _fetch_ag_du_metric(
                conn,
                table="ag_du_gw_pumping_monthly",
                metric_cols=[
                    "m.gw_pumping_avg_taf",
                    "m.gw_pumping_cv",
                    "m.is_calculated",
                ],
                scenario_id=scenario_id,
                du_id=du_id,
                region=region,
                cs3_type=cs3_type,
            )
            shortage_rows = await _fetch_ag_du_metric(
                conn,
                table="ag_du_shortage_monthly",
                metric_cols=[
                    "m.shortage_avg_taf",
                    "m.shortage_cv",
                    "m.shortage_frequency_pct",
                    "m.shortage_pct_of_demand_avg",
                ],
                scenario_id=scenario_id,
                du_id=du_id,
                region=region,
                cs3_type=cs3_type,
            )
    except Exception as e:
        log.error(f"ag-demand-units monthly query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    demand_units: Dict[str, Dict[str, Any]] = {}

    def _ensure(row: Any) -> Dict[str, Any]:
        du = row["du_id"]
        if du not in demand_units:
            demand_units[du] = {
                "label": _label_from(row["agency"], du),
                "agency": safe_str(row["agency"]),
                "hydrologic_region": safe_str(row["hydrologic_region"]),
                "cs3_type": safe_str(row["cs3_type"]),
                "provider": safe_str(row["provider"]),
                "monthly_demand": {},
                "monthly_sw_delivery": {},
                "monthly_gw_pumping": {},
                "monthly_shortage": {},
            }
        return demand_units[du]

    def _percentiles(row: Any) -> Dict[str, Any]:
        return {
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

    for row in demand_rows:
        entry = _ensure(row)
        entry["monthly_demand"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["demand_avg_taf"]),
            "cv": safe_float(row["demand_cv"]),
            **_percentiles(row),
        }

    for row in sw_rows:
        entry = _ensure(row)
        entry["monthly_sw_delivery"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["sw_delivery_avg_taf"]),
            "cv": safe_float(row["sw_delivery_cv"]),
            **_percentiles(row),
        }

    for row in gw_rows:
        entry = _ensure(row)
        entry["monthly_gw_pumping"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["gw_pumping_avg_taf"]),
            "cv": safe_float(row["gw_pumping_cv"]),
            "is_calculated": row["is_calculated"],
            **_percentiles(row),
        }

    for row in shortage_rows:
        entry = _ensure(row)
        entry["monthly_shortage"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["shortage_avg_taf"]),
            "cv": safe_float(row["shortage_cv"]),
            "shortage_frequency_pct": safe_float(row["shortage_frequency_pct"]),
            "shortage_pct_of_demand_avg": safe_float(row["shortage_pct_of_demand_avg"]),
            **_percentiles(row),
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


async def _fetch_ag_du_metric(
    conn,
    *,
    table: str,
    metric_cols: List[str],
    scenario_id: str,
    du_id: Optional[str],
    region: Optional[str],
    cs3_type: Optional[str],
) -> List[Any]:
    """Fetch rows from one of the four AG DU monthly tables."""
    metric_clause = ",\n            ".join(metric_cols)
    params: List[Any] = [scenario_id]
    where_suffix = _ag_du_filter_clause(params, du_id, region, cs3_type)
    query = f"""
        SELECT
            {_COMMON_ENTITY_COLS},
            {metric_clause},
            {_COMMON_PERCENTILE_COLS}
        FROM {table} m
        LEFT JOIN du_agriculture_entity e ON m.du_id = e.du_id
        WHERE m.scenario_short_code = $1
        {where_suffix}
        ORDER BY m.du_id, m.water_month
    """
    return await conn.fetch(query, *params)


# =============================================================================
# DU PERIOD SUMMARY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-demand-units/period-summary",
    summary="Period-of-record summary for AG demand units",
    description=(
        "Returns annual averages for demand, SW delivery, GW pumping, and "
        "GW restriction shortage, plus reliability and exceedance bands."
    ),
)
async def get_ag_du_period_summary(
    scenario_id: str,
    du_id: Optional[str] = Query(
        None, description="Comma-separated DU IDs to filter"
    ),
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region (SAC, SJR, TULARE)"
    ),
    cs3_type: Optional[str] = Query(
        None, description="Filter by CS3 type (PA, SA, XA, etc.)"
    ),
):
    """Get period-of-record summary for agricultural demand units."""
    cache_key = f"period:{scenario_id}:{du_id or ''}:{region or ''}:{cs3_type or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            query = """
                SELECT
                    p.du_id,
                    e.agency,
                    e.hydrologic_region,
                    e.cs3_type,
                    e.provider,
                    p.simulation_start_year,
                    p.simulation_end_year,
                    p.total_years,
                    p.annual_demand_avg_taf,
                    p.annual_demand_cv,
                    p.demand_exc_p5, p.demand_exc_p10, p.demand_exc_p25,
                    p.demand_exc_p50, p.demand_exc_p75, p.demand_exc_p90, p.demand_exc_p95,
                    p.annual_sw_delivery_avg_taf,
                    p.annual_sw_delivery_cv,
                    p.annual_gw_pumping_avg_taf,
                    p.annual_gw_pumping_cv,
                    p.gw_pumping_pct_of_demand,
                    p.annual_shortage_avg_taf,
                    p.shortage_years_count,
                    p.shortage_frequency_pct,
                    p.annual_shortage_pct_of_demand,
                    p.reliability_pct,
                    p.avg_pct_demand_met
                FROM ag_du_period_summary p
                LEFT JOIN du_agriculture_entity e ON p.du_id = e.du_id
                WHERE p.scenario_short_code = $1
            """
            params: List[Any] = [scenario_id]

            if du_id:
                ids = [d.strip() for d in du_id.split(",")]
                query += f" AND p.du_id = ANY(${len(params) + 1})"
                params.append(ids)

            if region:
                query += f" AND e.hydrologic_region = ${len(params) + 1}"
                params.append(region.upper())

            if cs3_type is not None:
                if cs3_type == "":
                    query += " AND (e.cs3_type IS NULL OR e.cs3_type = '')"
                else:
                    query += f" AND e.cs3_type = ${len(params) + 1}"
                    params.append(cs3_type.upper())

            query += " ORDER BY p.du_id"
            rows = await conn.fetch(query, *params)
    except Exception as e:
        log.error(f"ag-demand-units period-summary query failed for {scenario_id}: {e}")
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
            "label": _label_from(row["agency"], du),
            "agency": safe_str(row["agency"]),
            "hydrologic_region": safe_str(row["hydrologic_region"]),
            "cs3_type": safe_str(row["cs3_type"]),
            "provider": safe_str(row["provider"]),
            "simulation_start_year": safe_int(row["simulation_start_year"]),
            "simulation_end_year": safe_int(row["simulation_end_year"]),
            "total_years": safe_int(row["total_years"]),
            "annual_demand_avg_taf": safe_float(row["annual_demand_avg_taf"]),
            "annual_demand_cv": safe_float(row["annual_demand_cv"]),
            "demand_exceedance": {
                "p5": safe_float(row["demand_exc_p5"]),
                "p10": safe_float(row["demand_exc_p10"]),
                "p25": safe_float(row["demand_exc_p25"]),
                "p50": safe_float(row["demand_exc_p50"]),
                "p75": safe_float(row["demand_exc_p75"]),
                "p90": safe_float(row["demand_exc_p90"]),
                "p95": safe_float(row["demand_exc_p95"]),
            },
            "annual_sw_delivery_avg_taf": safe_float(row["annual_sw_delivery_avg_taf"]),
            "annual_sw_delivery_cv": safe_float(row["annual_sw_delivery_cv"]),
            "annual_gw_pumping_avg_taf": safe_float(row["annual_gw_pumping_avg_taf"]),
            "annual_gw_pumping_cv": safe_float(row["annual_gw_pumping_cv"]),
            "gw_pumping_pct_of_demand": safe_float(row["gw_pumping_pct_of_demand"]),
            "annual_shortage_avg_taf": safe_float(row["annual_shortage_avg_taf"]),
            "shortage_years_count": safe_int(row["shortage_years_count"]),
            "shortage_frequency_pct": safe_float(row["shortage_frequency_pct"]),
            "annual_shortage_pct_of_demand": safe_float(row["annual_shortage_pct_of_demand"]),
            "reliability_pct": safe_float(row["reliability_pct"]),
            "avg_pct_demand_met": safe_float(row["avg_pct_demand_met"]),
        }

    result = {
        "scenario_id": scenario_id,
        "demand_units": demand_units,
        "count": len(demand_units),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


# =============================================================================
# AG AGGREGATES (SWP/CVP project totals + N/S splits)
# =============================================================================
#
# Touched in Slice F: aggregate symmetry. The handlers below get extracted into
# canonical fetchers so the batch handler can delegate, and gain `count` on
# the list envelope. Untouched for now beyond the routes that Slice D directly
# replaced

@router.get(
    "/ag-aggregates",
    summary="List agricultural aggregates",
    description="Returns active agricultural aggregate entities (SWP/CVP project aggregates).",
)
async def list_ag_aggregates():
    """List all agricultural aggregate entities."""
    cache_key = "ag_aggregates:list"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            query = """
                SELECT short_code, label, project, region, delivery_variable, description
                FROM ag_aggregate_entity
                WHERE is_active = TRUE
                ORDER BY display_order
            """
            rows = await conn.fetch(query)
    except Exception as e:
        log.error(f"ag-aggregates list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    aggregates = [
        {
            "short_code": safe_str(row["short_code"]),
            "label": safe_str(row["label"]),
            "project": safe_str(row["project"]),
            "region": safe_str(row["region"]),
            "delivery_variable": safe_str(row["delivery_variable"]),
            "description": safe_str(row["description"]),
        }
        for row in rows
    ]

    result = {"aggregates": aggregates, "count": len(aggregates)}
    _static_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


@router.get(
    "/scenarios/{scenario_id}/ag-aggregates/monthly",
    summary="Monthly delivery statistics for AG aggregates",
    description=(
        "Returns monthly percentile bands per AG aggregate "
        "(swp_pag, swp_pag_n, swp_pag_s, cvp_pag_n, cvp_pag_s)."
    ),
)
async def get_ag_aggregate_monthly(
    scenario_id: str,
    aggregate: Optional[str] = Query(
        None,
        description="Comma-separated aggregate codes. Defaults to all.",
    ),
):
    """Get monthly delivery statistics for agricultural aggregates."""
    cache_key = f"ag_aggregates:monthly:{scenario_id}:{aggregate or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            query = """
                SELECT
                    m.aggregate_code,
                    e.label,
                    e.project,
                    e.region,
                    m.water_month,
                    m.delivery_avg_taf,
                    m.delivery_cv,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.sample_count
                FROM ag_aggregate_monthly m
                LEFT JOIN ag_aggregate_entity e ON m.aggregate_code = e.short_code
                WHERE m.scenario_short_code = $1
            """
            params: List[Any] = [scenario_id]

            if aggregate:
                codes = [a.strip() for a in aggregate.split(",")]
                query += f" AND m.aggregate_code = ANY(${len(params) + 1})"
                params.append(codes)

            query += " ORDER BY e.display_order, m.water_month"
            rows = await conn.fetch(query, *params)
    except Exception as e:
        log.error(f"ag-aggregates monthly query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No aggregate data found for scenario '{scenario_id}'",
        )

    aggregates: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = row["aggregate_code"]
        if code not in aggregates:
            aggregates[code] = {
                "label": safe_str(row["label"]),
                "project": safe_str(row["project"]),
                "region": safe_str(row["region"]),
                "monthly_delivery": {},
            }

        aggregates[code]["monthly_delivery"][str(row["water_month"])] = {
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

    result = {
        "scenario_id": scenario_id,
        "aggregates": aggregates,
        "count": len(aggregates),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


@router.get(
    "/scenarios/{scenario_id}/ag-aggregates/period-summary",
    summary="Period-of-record summary for AG aggregates",
    description=(
        "Returns annual delivery averages and exceedance bands for SWP/CVP "
        "project-level agricultural aggregates."
    ),
)
async def get_ag_aggregate_period_summary(
    scenario_id: str,
    aggregate: Optional[str] = Query(
        None, description="Comma-separated aggregate codes. Defaults to all."
    ),
):
    """Get period-of-record summary for agricultural aggregates."""
    cache_key = f"ag_aggregates:period:{scenario_id}:{aggregate or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        async with _db_pool.acquire() as conn:
            query = """
                SELECT
                    p.aggregate_code,
                    e.label,
                    e.project,
                    e.region,
                    p.simulation_start_year,
                    p.simulation_end_year,
                    p.total_years,
                    p.annual_delivery_avg_taf,
                    p.annual_delivery_cv,
                    p.delivery_exc_p5, p.delivery_exc_p10, p.delivery_exc_p25,
                    p.delivery_exc_p50, p.delivery_exc_p75, p.delivery_exc_p90, p.delivery_exc_p95
                FROM ag_aggregate_period_summary p
                LEFT JOIN ag_aggregate_entity e ON p.aggregate_code = e.short_code
                WHERE p.scenario_short_code = $1
            """
            params: List[Any] = [scenario_id]

            if aggregate:
                codes = [a.strip() for a in aggregate.split(",")]
                query += f" AND p.aggregate_code = ANY(${len(params) + 1})"
                params.append(codes)

            query += " ORDER BY e.display_order"
            rows = await conn.fetch(query, *params)
    except Exception as e:
        log.error(f"ag-aggregates period-summary query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No aggregate period summary found for scenario '{scenario_id}'",
        )

    aggregates: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = row["aggregate_code"]
        aggregates[code] = {
            "label": safe_str(row["label"]),
            "project": safe_str(row["project"]),
            "region": safe_str(row["region"]),
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
        }

    result = {
        "scenario_id": scenario_id,
        "aggregates": aggregates,
        "count": len(aggregates),
    }
    _stats_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())
