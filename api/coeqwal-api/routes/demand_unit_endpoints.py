"""
Urban Demand Unit Statistics API Endpoints.

Provides statistics for urban demand units (tier matrix DUs) including:
- Grouped demand unit list for dropdown population
- Monthly delivery statistics
- Period-of-record summary

71 canonical CWS demand units organized by extraction category:
- var_wba: WBA-style units with DL_* delivery (40 units)
- var_gw_only: Groundwater-only units (3 units)
- var_swp_contractor: SWP contractor PMI deliveries (11 units)
- var_named_locality: Named localities with D_* arcs (15 units)
- var_missing: No CalSim variables found (2 units)

Note: SBA036 and SCVWD are aliases for the same Santa Clara Valley WD.
"""

import logging
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, HTTPException, Query

from routes._common.null_handling import safe_float, safe_int

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

# Database pool - set by main.py at startup
_db_pool = None


def set_db_pool(pool):
    """Set the database connection pool."""
    global _db_pool
    _db_pool = pool


@router.get(
    "/demand-units",
    summary="List urban demand units",
    description="Returns available urban demand unit entities.",
)
async def list_demand_units(
    group: Optional[str] = Query(
        None, description="Filter by group short_code (e.g., var_wba, var_swp_contractor)"
    ),
):
    """
    List all urban demand units with optional group filter.

    **Examples:**
    - `GET /api/statistics/demand-units` - All units
    - `GET /api/statistics/demand-units?group=var_wba` - WBA units only
    - `GET /api/statistics/demand-units?group=var_swp_contractor` - SWP contractors only
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        if group:
            # Filter by group membership
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
            # Return all tier matrix units (71 canonical)
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

    return {
        "demand_units": [
            {
                "du_id": row["du_id"],
                "hydrologic_region": row["hydrologic_region"],
                "community_agency": row["community_agency"],
                "du_class": row["du_class"],
                "cs3_type": row["cs3_type"],
                "gw": row["gw"],
                "sw": row["sw"],
                "variable_type": row["variable_type"],
                "delivery_variable": row["delivery_variable"],
                "shortage_variable": row["shortage_variable"],
            }
            for row in rows
        ],
        "count": len(rows),
    }


# =============================================================================
# MONTHLY DELIVERY STATISTICS (bulk)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/demand-units/delivery-monthly",
    summary="Get monthly delivery statistics for demand units",
)
async def get_du_delivery_monthly(
    scenario_id: str,
    du_id: Optional[str] = Query(
        None, description="Comma-separated DU IDs to filter"
    ),
    group: Optional[str] = Query(
        None, description="Filter by group short_code (e.g., var_wba)"
    ),
):
    """
    Get monthly delivery statistics for urban demand units.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/demand-units/delivery-monthly` - All units
    - `GET /api/statistics/scenarios/s0020/demand-units/delivery-monthly?du_id=ACWA,SCVWD`
    - `GET /api/statistics/scenarios/s0020/demand-units/delivery-monthly?group=var_swp_contractor`
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        if group:
            # Filter by group membership
            query = """
                SELECT
                    m.du_id,
                    e.community_agency,
                    e.hydrologic_region,
                    m.water_month,
                    m.delivery_avg_taf,
                    m.delivery_cv,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.demand_avg_taf, m.percent_of_demand_avg,
                    m.sample_count
                FROM du_delivery_monthly m
                JOIN du_urban_group_member gm ON m.du_id = gm.du_id
                JOIN du_urban_group g ON gm.du_urban_group_id = g.id
                LEFT JOIN du_urban_entity e ON m.du_id = e.du_id
                WHERE m.scenario_short_code = $1
                  AND g.short_code = $2
                  AND g.is_active = TRUE
                ORDER BY gm.display_order, m.water_month
            """
            rows = await conn.fetch(query, scenario_id, group)
        else:
            query = """
                SELECT
                    m.du_id,
                    e.community_agency,
                    e.hydrologic_region,
                    m.water_month,
                    m.delivery_avg_taf,
                    m.delivery_cv,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.demand_avg_taf, m.percent_of_demand_avg,
                    m.sample_count
                FROM du_delivery_monthly m
                LEFT JOIN du_urban_entity e ON m.du_id = e.du_id
                WHERE m.scenario_short_code = $1
            """
            params: List[Any] = [scenario_id]

            if du_id:
                ids = [d.strip() for d in du_id.split(",")]
                query += f" AND m.du_id = ANY(${len(params) + 1})"
                params.append(ids)

            query += " ORDER BY m.du_id, m.water_month"
            rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No delivery data found for scenario {scenario_id}",
        )

    # Group by DU
    demand_units: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        du = row["du_id"]
        if du not in demand_units:
            demand_units[du] = {
                "community_agency": row["community_agency"],
                "hydrologic_region": row["hydrologic_region"],
                "monthly_delivery": {},
            }

        demand_units[du]["monthly_delivery"][str(row["water_month"])] = {
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

    return {"scenario_id": scenario_id, "demand_units": demand_units}


# =============================================================================
# SHORTAGE MONTHLY (bulk)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/demand-units/shortage-monthly",
    summary="Get monthly shortage statistics for demand units",
)
async def get_du_shortage_monthly(
    scenario_id: str,
    du_id: Optional[str] = Query(
        None, description="Comma-separated DU IDs to filter"
    ),
    group: Optional[str] = Query(
        None, description="Filter by group short_code (e.g., var_wba)"
    ),
):
    """
    Get monthly shortage statistics for urban demand units.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/demand-units/shortage-monthly` - All units
    - `GET /api/statistics/scenarios/s0020/demand-units/shortage-monthly?du_id=ACWA,SCVWD`
    - `GET /api/statistics/scenarios/s0020/demand-units/shortage-monthly?group=var_swp_contractor`
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        if group:
            # Filter by group membership
            query = """
                SELECT
                    m.du_id,
                    e.community_agency,
                    e.hydrologic_region,
                    m.water_month,
                    m.shortage_avg_taf,
                    m.shortage_cv,
                    m.shortage_frequency_pct,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.demand_avg_taf, m.percent_of_demand_avg,
                    m.sample_count
                FROM du_shortage_monthly m
                JOIN du_urban_group_member gm ON m.du_id = gm.du_id
                JOIN du_urban_group g ON gm.du_urban_group_id = g.id
                LEFT JOIN du_urban_entity e ON m.du_id = e.du_id
                WHERE m.scenario_short_code = $1
                  AND g.short_code = $2
                  AND g.is_active = TRUE
                ORDER BY gm.display_order, m.water_month
            """
            rows = await conn.fetch(query, scenario_id, group)
        else:
            query = """
                SELECT
                    m.du_id,
                    e.community_agency,
                    e.hydrologic_region,
                    m.water_month,
                    m.shortage_avg_taf,
                    m.shortage_cv,
                    m.shortage_frequency_pct,
                    m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                    m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                    m.demand_avg_taf, m.percent_of_demand_avg,
                    m.sample_count
                FROM du_shortage_monthly m
                LEFT JOIN du_urban_entity e ON m.du_id = e.du_id
                WHERE m.scenario_short_code = $1
            """
            params: List[Any] = [scenario_id]

            if du_id:
                ids = [d.strip() for d in du_id.split(",")]
                query += f" AND m.du_id = ANY(${len(params) + 1})"
                params.append(ids)

            query += " ORDER BY m.du_id, m.water_month"
            rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No shortage data found for scenario {scenario_id}",
        )

    # Group by DU
    demand_units: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        du = row["du_id"]
        if du not in demand_units:
            demand_units[du] = {
                "community_agency": row["community_agency"],
                "hydrologic_region": row["hydrologic_region"],
                "monthly_shortage": {},
            }

        demand_units[du]["monthly_shortage"][str(row["water_month"])] = {
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
            "demand_avg_taf": safe_float(row["demand_avg_taf"]),
            "percent_of_demand": safe_float(row["percent_of_demand_avg"]),
            "sample_count": safe_int(row["sample_count"]),
        }

    return {"scenario_id": scenario_id, "demand_units": demand_units}


# =============================================================================
# PERIOD SUMMARY (bulk)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/demand-units/period-summary",
    summary="Get period summary for demand units",
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
    """
    Get period-of-record summary for urban demand units.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/demand-units/period-summary`
    - `GET /api/statistics/scenarios/s0020/demand-units/period-summary?du_id=ACWA`
    - `GET /api/statistics/scenarios/s0020/demand-units/period-summary?group=var_swp_contractor`
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        if group:
            # Filter by group membership
            query = """
                SELECT
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
            query = """
                SELECT
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

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No period summary found for scenario {scenario_id}",
        )

    demand_units: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        du = row["du_id"]
        demand_units[du] = {
            "community_agency": row["community_agency"],
            "hydrologic_region": row["hydrologic_region"],
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

    return {"scenario_id": scenario_id, "demand_units": demand_units}
