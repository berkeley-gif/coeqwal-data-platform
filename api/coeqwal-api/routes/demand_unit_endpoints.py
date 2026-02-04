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

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

# Database pool - set by main.py at startup
_db_pool = None


def set_db_pool(pool):
    """Set the database connection pool."""
    global _db_pool
    _db_pool = pool


def safe_float(val) -> Optional[float]:
    """Safely convert to float, returning None for NULL."""
    if val is None:
        return None
    return float(val)


def safe_int(val) -> Optional[int]:
    """Safely convert to int, returning None for NULL."""
    if val is None:
        return None
    return int(val)


# =============================================================================
# LIST DEMAND UNITS BY GROUP (for dropdown)
# =============================================================================


@router.get(
    "/demand-units/groups",
    summary="List demand units organized by group",
    description="Returns demand units organized by extraction category group for dropdown population.",
)
async def list_demand_units_by_group() -> Dict[str, Any]:
    """
    Get demand units organized by variable extraction category groups.

    **Use case:** Populate dropdown selector in UI, organized by group.

    **Response:**
    ```json
    {
      "groups": [
        {
          "short_code": "var_wba",
          "label": "WBA Delivery Units",
          "description": "...",
          "units": [
            {
              "du_id": "02_PU",
              "display_name": "Zone 02 Project Urban",
              "community_agency": "City of Redding, etc.",
              "project": "CVP",
              "hydrologic_region": "SAC",
              "variable_type": "delivery"
            },
            ...
          ]
        },
        ...
      ]
    }
    ```

    **Notes:**
    - SBA036 and SCVWD are aliases for Santa Clara Valley WD (same CalSim variable)
    - var_gw_only units have groundwater pumping, not surface delivery
    - var_missing units (JLIND, UPANG) have no CalSim data
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        # Get variable category groups (var_*) with their members
        query = """
            SELECT
                g.short_code as group_code,
                g.label as group_label,
                g.description as group_description,
                g.display_order as group_order,
                gm.du_id,
                gm.display_order as du_order,
                e.community_agency,
                e.hydrologic_region,
                e.cs3_type,
                v.variable_type,
                v.delivery_variable,
                v.notes as variable_notes,
                CASE 
                    WHEN g.short_code = 'var_swp_contractor' THEN 'SWP'
                    WHEN g.short_code = 'var_gw_only' THEN 'GW'
                    WHEN e.hydrologic_region IN ('SAC', 'SJR') THEN 'CVP'
                    ELSE NULL
                END as project
            FROM du_urban_group g
            JOIN du_urban_group_member gm ON g.id = gm.du_urban_group_id
            LEFT JOIN du_urban_entity e ON gm.du_id = e.du_id
            LEFT JOIN du_urban_variable v ON gm.du_id = v.du_id
            WHERE g.short_code LIKE 'var_%'
              AND g.is_active = TRUE
              AND gm.is_active = TRUE
            ORDER BY g.display_order, gm.display_order
        """
        rows = await conn.fetch(query)

    if not rows:
        raise HTTPException(status_code=404, detail="No demand unit groups found")

    # Organize by group
    groups_dict: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        group_code = row["group_code"]

        if group_code not in groups_dict:
            groups_dict[group_code] = {
                "short_code": group_code,
                "label": row["group_label"],
                "description": row["group_description"],
                "units": [],
            }

        # Build display name
        du_id = row["du_id"]
        community = row["community_agency"]
        if community:
            display_name = f"{du_id} - {community[:50]}"
        else:
            display_name = du_id

        groups_dict[group_code]["units"].append({
            "du_id": du_id,
            "display_name": display_name,
            "community_agency": community,
            "project": row["project"],
            "hydrologic_region": row["hydrologic_region"],
            "cs3_type": row["cs3_type"],
            "variable_type": row["variable_type"],
            "delivery_variable": row["delivery_variable"],
        })

    # Convert to list, maintaining order
    groups = list(groups_dict.values())

    return {"groups": groups, "total_units": sum(len(g["units"]) for g in groups)}


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
# SINGLE DEMAND UNIT STATISTICS (for dropdown selection)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/demand-units/{du_id}/statistics",
    summary="Get all statistics for a single demand unit",
)
async def get_single_du_statistics(
    scenario_id: str,
    du_id: str,
) -> Dict[str, Any]:
    """
    Get complete statistics for a single demand unit.

    **Use case:** When user selects a demand unit from dropdown.

    **Example:** `GET /api/statistics/scenarios/s0020/demand-units/ACWA/statistics`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "du_id": "ACWA",
      "community_agency": "Alameda County Water Agency",
      "project": "SWP",
      "hydrologic_region": "SJR",
      "variable_type": "delivery",
      "period_summary": {
        "simulation_start_year": 1922,
        "simulation_end_year": 2021,
        "annual_delivery_avg_taf": 45.5,
        "reliability_pct": 92.0,
        ...
      },
      "monthly_delivery": {
        "1": {"avg_taf": 3.5, "cv": 0.45, ...},
        ...
      },
      "monthly_shortage": {
        "1": {"avg_taf": 0.5, "cv": 1.2, ...},
        ...
      }
    }
    ```
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        # Get entity info with variable mapping
        entity_query = """
            SELECT
                e.du_id,
                e.community_agency,
                e.hydrologic_region,
                e.cs3_type,
                v.variable_type,
                v.delivery_variable,
                v.shortage_variable,
                CASE 
                    WHEN v.variable_type = 'gw_pumping' THEN 'GW'
                    ELSE NULL
                END as project
            FROM du_urban_entity e
            LEFT JOIN du_urban_variable v ON e.du_id = v.du_id
            WHERE e.du_id = $1
        """
        entity = await conn.fetchrow(entity_query, du_id)

        if not entity:
            raise HTTPException(status_code=404, detail=f"Demand unit {du_id} not found")

        # Get period summary
        period_query = """
            SELECT
                simulation_start_year, simulation_end_year, total_years,
                annual_delivery_avg_taf, annual_delivery_cv,
                delivery_exc_p5, delivery_exc_p10, delivery_exc_p25,
                delivery_exc_p50, delivery_exc_p75, delivery_exc_p90, delivery_exc_p95,
                annual_shortage_avg_taf, shortage_years_count, shortage_frequency_pct,
                shortage_exc_p5, shortage_exc_p10, shortage_exc_p25,
                shortage_exc_p50, shortage_exc_p75, shortage_exc_p90, shortage_exc_p95,
                reliability_pct, avg_pct_demand_met, annual_demand_avg_taf
            FROM du_period_summary
            WHERE scenario_short_code = $1 AND du_id = $2
        """
        period_row = await conn.fetchrow(period_query, scenario_id, du_id)

        # Get monthly delivery stats
        delivery_query = """
            SELECT
                water_month, delivery_avg_taf, delivery_cv,
                q0, q10, q30, q50, q70, q90, q100,
                exc_p5, exc_p10, exc_p25, exc_p50, exc_p75, exc_p90, exc_p95,
                sample_count
            FROM du_delivery_monthly
            WHERE scenario_short_code = $1 AND du_id = $2
            ORDER BY water_month
        """
        delivery_rows = await conn.fetch(delivery_query, scenario_id, du_id)

        # Get monthly shortage stats (table may not exist)
        shortage_rows = []
        try:
            shortage_query = """
                SELECT
                    water_month, shortage_avg_taf, shortage_cv, shortage_frequency_pct,
                    q0, q10, q30, q50, q70, q90, q100,
                    exc_p5, exc_p10, exc_p25, exc_p50, exc_p75, exc_p90, exc_p95,
                    sample_count
                FROM du_shortage_monthly
                WHERE scenario_short_code = $1 AND du_id = $2
                ORDER BY water_month
            """
            shortage_rows = await conn.fetch(shortage_query, scenario_id, du_id)
        except Exception:
            # Table may not exist yet
            pass

    # Build response
    result: Dict[str, Any] = {
        "scenario_id": scenario_id,
        "du_id": du_id,
        "community_agency": entity["community_agency"],
        "project": entity["project"],
        "hydrologic_region": entity["hydrologic_region"],
        "cs3_type": entity["cs3_type"],
        "variable_type": entity["variable_type"],
        "delivery_variable": entity["delivery_variable"],
        "shortage_variable": entity["shortage_variable"],
    }

    # Period summary
    if period_row:
        result["period_summary"] = {
            "simulation_start_year": safe_int(period_row["simulation_start_year"]),
            "simulation_end_year": safe_int(period_row["simulation_end_year"]),
            "total_years": safe_int(period_row["total_years"]),
            "annual_delivery_avg_taf": safe_float(period_row["annual_delivery_avg_taf"]),
            "annual_delivery_cv": safe_float(period_row["annual_delivery_cv"]),
            "delivery_exceedance": {
                "p5": safe_float(period_row["delivery_exc_p5"]),
                "p10": safe_float(period_row["delivery_exc_p10"]),
                "p25": safe_float(period_row["delivery_exc_p25"]),
                "p50": safe_float(period_row["delivery_exc_p50"]),
                "p75": safe_float(period_row["delivery_exc_p75"]),
                "p90": safe_float(period_row["delivery_exc_p90"]),
                "p95": safe_float(period_row["delivery_exc_p95"]),
            },
            "annual_shortage_avg_taf": safe_float(period_row["annual_shortage_avg_taf"]),
            "shortage_years_count": safe_int(period_row["shortage_years_count"]),
            "shortage_frequency_pct": safe_float(period_row["shortage_frequency_pct"]),
            "shortage_exceedance": {
                "p5": safe_float(period_row["shortage_exc_p5"]),
                "p10": safe_float(period_row["shortage_exc_p10"]),
                "p25": safe_float(period_row["shortage_exc_p25"]),
                "p50": safe_float(period_row["shortage_exc_p50"]),
                "p75": safe_float(period_row["shortage_exc_p75"]),
                "p90": safe_float(period_row["shortage_exc_p90"]),
                "p95": safe_float(period_row["shortage_exc_p95"]),
            },
            "reliability_pct": safe_float(period_row["reliability_pct"]),
            "avg_pct_demand_met": safe_float(period_row["avg_pct_demand_met"]),
            "annual_demand_avg_taf": safe_float(period_row["annual_demand_avg_taf"]),
        }
    else:
        result["period_summary"] = None

    # Monthly delivery
    monthly_delivery: Dict[str, Dict[str, Any]] = {}
    for row in delivery_rows:
        wm = str(row["water_month"])
        monthly_delivery[wm] = {
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
    result["monthly_delivery"] = monthly_delivery if monthly_delivery else None

    # Monthly shortage
    monthly_shortage: Dict[str, Dict[str, Any]] = {}
    for row in shortage_rows:
        wm = str(row["water_month"])
        monthly_shortage[wm] = {
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
    result["monthly_shortage"] = monthly_shortage if monthly_shortage else None

    return result


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
