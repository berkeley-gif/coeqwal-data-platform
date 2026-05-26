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

Provides statistics for agricultural demand units including:
- Monthly DEMAND statistics (from AW_* variables - applied water requirement)
- Monthly SURFACE WATER DELIVERY statistics (from DN_* variables)
- Monthly GROUNDWATER PUMPING statistics (from GP_* or calculated as AW - DN)
- Monthly GW RESTRICTION shortage statistics (from GW_SHORT_* - SJR/Tulare only)
- Period-of-record summary
- Aggregate statistics (SWP PAG, CVP PAG, etc.)

Water months: 1=October, 2=November, ..., 12=September
Values: TAF (thousand acre-feet)

Note: Sacramento region DUs do NOT have GW shortage data.
"""

import logging
from typing import Optional

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


# =============================================================================
# LIST AG DEMAND UNITS
# =============================================================================


@router.get(
    "/ag-demand-units",
    summary="List agricultural demand units",
    description="Returns available agricultural demand unit entities with filtering options.",
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
    """
    List all agricultural demand units with optional filters.

    **Use case:** Populate demand unit selector dropdowns in the UI.

    **Filters:**
    - `region`: SAC (Sacramento), SJR (San Joaquin), TULARE
    - `cs3_type`: PA (Project AG), SA (Settlement AG), XA (Exchange AG), PR (Project Refuge), NR (Non-project Refuge), blank for NA (Non-project)
    - `provider`: CVP, SWP, Reclamation

    **Response:**
    ```json
    {
      "demand_units": [
        {
          "du_id": "64_PA1",
          "wba_id": "64",
          "hydrologic_region": "SJR",
          "cs3_type": "PA",
          "agency": "Westlands WD",
          "provider": "CVP",
          "gw": true,
          "sw": true
        },
        ...
      ]
    }
    ```
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

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
        params = []

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

    return {
        "demand_units": [
            {
                "du_id": row["du_id"],
                "wba_id": row["wba_id"],
                "hydrologic_region": row["hydrologic_region"],
                "cs3_type": row["cs3_type"],
                "agency": row["agency"],
                "provider": row["provider"],
                "gw": row["gw"],
                "sw": row["sw"],
                "total_acres": safe_float(row["total_acres"]),
                "has_gis_data": row["has_gis_data"],
            }
            for row in rows
        ],
        "count": len(rows),
    }


# =============================================================================
# DU DEMAND MONTHLY (from AW_* - Applied Water)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-demand-units/demand-monthly",
    summary="Get monthly DEMAND statistics for AG demand units",
)
async def get_ag_du_demand_monthly(
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
    """
    Get monthly DEMAND statistics for agricultural demand units.

    **IMPORTANT:** This is DEMAND (applied water requirement), NOT delivery.
    Source: AW_* variables from CalSim SV input file.
    
    For actual surface water delivery, use `/sw-delivery-monthly`.
    For groundwater pumping, use `/gw-pumping-monthly`.

    **Use case:** Render percentile band charts showing demand distribution
    across water years for each month.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/demand-monthly` (all DUs)
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/demand-monthly?du_id=64_PA1` (single)
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/demand-monthly?region=SJR` (by region)

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "demand_units": {
        "64_PA1": {
          "agency": "Westlands WD",
          "hydrologic_region": "SJR",
          "cs3_type": "PA",
          "monthly_demand": {
            "1": {"avg_taf": 125.5, "cv": 0.35, "q0": 45.2, ...},
            ...
          }
        }
      }
    }
    ```

    **Water months:** 1=October, 2=November, ..., 12=September
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                m.du_id,
                e.agency,
                e.hydrologic_region,
                e.cs3_type,
                e.provider,
                m.water_month,
                m.demand_avg_taf,
                m.demand_cv,
                m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                m.sample_count
            FROM ag_du_demand_monthly m
            LEFT JOIN du_agriculture_entity e ON m.du_id = e.du_id
            WHERE m.scenario_short_code = $1
        """
        params = [scenario_id]

        if du_id:
            ids = [d.strip() for d in du_id.split(",")]
            query += f" AND m.du_id = ANY(${len(params) + 1})"
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

        query += " ORDER BY m.du_id, m.water_month"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No demand data found for scenario {scenario_id}",
        )

    # Group by DU
    demand_units = {}
    for row in rows:
        du = row["du_id"]
        if du not in demand_units:
            demand_units[du] = {
                "agency": row["agency"],
                "hydrologic_region": row["hydrologic_region"],
                "cs3_type": row["cs3_type"],
                "provider": row["provider"],
                "monthly_demand": {},
            }

        demand_units[du]["monthly_demand"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["demand_avg_taf"]),
            "cv": safe_float(row["demand_cv"]),
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
# DU SURFACE WATER DELIVERY MONTHLY (from DN_* - Net Delivery)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-demand-units/sw-delivery-monthly",
    summary="Get monthly SURFACE WATER DELIVERY statistics for AG demand units",
)
async def get_ag_du_sw_delivery_monthly(
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
    """
    Get monthly SURFACE WATER DELIVERY statistics for agricultural demand units.

    **IMPORTANT:** This is actual surface water delivery, NOT demand.
    Source: DN_* (Net Delivery) variables from CalSim DV output file.
    
    For groundwater-only DUs, there is no surface water delivery data.
    For total demand, use `/demand-monthly`.
    For groundwater pumping, use `/gw-pumping-monthly`.

    **Use case:** Render percentile band charts showing SW delivery distribution
    across water years for each month.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/sw-delivery-monthly` (all DUs)
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/sw-delivery-monthly?du_id=64_PA1`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "demand_units": {
        "64_PA1": {
          "agency": "Westlands WD",
          "hydrologic_region": "SJR",
          "cs3_type": "PA",
          "monthly_sw_delivery": {
            "1": {"avg_taf": 100.5, "cv": 0.40, "q0": 35.2, ...},
            ...
          }
        }
      }
    }
    ```

    **Water months:** 1=October, 2=November, ..., 12=September
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                m.du_id,
                e.agency,
                e.hydrologic_region,
                e.cs3_type,
                e.provider,
                m.water_month,
                m.sw_delivery_avg_taf,
                m.sw_delivery_cv,
                m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                m.sample_count
            FROM ag_du_sw_delivery_monthly m
            LEFT JOIN du_agriculture_entity e ON m.du_id = e.du_id
            WHERE m.scenario_short_code = $1
        """
        params = [scenario_id]

        if du_id:
            ids = [d.strip() for d in du_id.split(",")]
            query += f" AND m.du_id = ANY(${len(params) + 1})"
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

        query += " ORDER BY m.du_id, m.water_month"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No surface water delivery data found for scenario {scenario_id}. Some DUs may be groundwater-only.",
        )

    # Group by DU
    demand_units = {}
    for row in rows:
        du = row["du_id"]
        if du not in demand_units:
            demand_units[du] = {
                "agency": row["agency"],
                "hydrologic_region": row["hydrologic_region"],
                "cs3_type": row["cs3_type"],
                "provider": row["provider"],
                "monthly_sw_delivery": {},
            }

        demand_units[du]["monthly_sw_delivery"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["sw_delivery_avg_taf"]),
            "cv": safe_float(row["sw_delivery_cv"]),
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
# DU GROUNDWATER PUMPING MONTHLY (from GP_* or calculated as AW - DN)
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-demand-units/gw-pumping-monthly",
    summary="Get monthly GROUNDWATER PUMPING statistics for AG demand units",
)
async def get_ag_du_gw_pumping_monthly(
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
    """
    Get monthly GROUNDWATER PUMPING statistics for agricultural demand units.

    **IMPORTANT:** In CalSim, agricultural demand is assumed to be fully met.
    GW pumping = Demand (AW) - Surface Water Delivery (DN)
    
    Source: GP_* variables where available, or calculated as AW - DN.
    The `is_calculated` field indicates whether the value was calculated or from explicit data.

    For demand (applied water requirement), use `/demand-monthly`.
    For surface water delivery, use `/sw-delivery-monthly`.

    **Use case:** Render percentile band charts showing GW pumping distribution
    across water years for each month.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/gw-pumping-monthly` (all DUs)
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/gw-pumping-monthly?region=SJR`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "demand_units": {
        "64_PA1": {
          "agency": "Westlands WD",
          "hydrologic_region": "SJR",
          "cs3_type": "PA",
          "monthly_gw_pumping": {
            "1": {"avg_taf": 25.0, "cv": 0.55, "is_calculated": true, ...},
            ...
          }
        }
      }
    }
    ```

    **Water months:** 1=October, 2=November, ..., 12=September
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                m.du_id,
                e.agency,
                e.hydrologic_region,
                e.cs3_type,
                e.provider,
                m.water_month,
                m.gw_pumping_avg_taf,
                m.gw_pumping_cv,
                m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                m.is_calculated,
                m.sample_count
            FROM ag_du_gw_pumping_monthly m
            LEFT JOIN du_agriculture_entity e ON m.du_id = e.du_id
            WHERE m.scenario_short_code = $1
        """
        params = [scenario_id]

        if du_id:
            ids = [d.strip() for d in du_id.split(",")]
            query += f" AND m.du_id = ANY(${len(params) + 1})"
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

        query += " ORDER BY m.du_id, m.water_month"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No groundwater pumping data found for scenario {scenario_id}",
        )

    # Group by DU
    demand_units = {}
    for row in rows:
        du = row["du_id"]
        if du not in demand_units:
            demand_units[du] = {
                "agency": row["agency"],
                "hydrologic_region": row["hydrologic_region"],
                "cs3_type": row["cs3_type"],
                "provider": row["provider"],
                "monthly_gw_pumping": {},
            }

        demand_units[du]["monthly_gw_pumping"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["gw_pumping_avg_taf"]),
            "cv": safe_float(row["gw_pumping_cv"]),
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
            "is_calculated": row["is_calculated"],
            "sample_count": safe_int(row["sample_count"]),
        }

    return {"scenario_id": scenario_id, "demand_units": demand_units}


# =============================================================================
# DU PERIOD SUMMARY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-demand-units/period-summary",
    summary="Get period summary for AG demand units",
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
    """
    Get period-of-record summary for agricultural demand units.

    **Use case:** Dashboard display of key reliability metrics and
    exceedance statistics for the full simulation period.

    **IMPORTANT:** Now correctly distinguishes:
    - `annual_demand_avg_taf` - Total demand (from AW_*)
    - `annual_sw_delivery_avg_taf` - Surface water delivery (from DN_*)
    - `annual_gw_pumping_avg_taf` - Groundwater pumping (calculated as AW - DN)
    - `gw_pumping_pct_of_demand` - GW pumping as % of total demand

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "demand_units": {
        "64_PA1": {
          "agency": "Westlands WD",
          "hydrologic_region": "SJR",
          "simulation_start_year": 1922,
          "simulation_end_year": 2021,
          "total_years": 100,
          "annual_demand_avg_taf": 496.0,
          "annual_demand_cv": 0.28,
          "demand_exceedance": {...},
          "annual_sw_delivery_avg_taf": 400.0,
          "annual_sw_delivery_cv": 0.32,
          "annual_gw_pumping_avg_taf": 96.0,
          "annual_gw_pumping_cv": 0.45,
          "gw_pumping_pct_of_demand": 19.4,
          "annual_shortage_avg_taf": 45.5,
          "shortage_years_count": 35,
          "shortage_frequency_pct": 35.0,
          "annual_shortage_pct_of_demand": 9.2,
          "reliability_pct": 90.8,
          "avg_pct_demand_met": 90.8
        }
      }
    }
    ```

    **Note:** Sacramento region DUs will have NULL shortage values.
    GW shortage is for groundwater RESTRICTION shortage only (GW_SHORT_*).
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

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
        params = [scenario_id]

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

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No period summary found for scenario {scenario_id}",
        )

    demand_units = {}
    for row in rows:
        du = row["du_id"]
        demand_units[du] = {
            "agency": row["agency"],
            "hydrologic_region": row["hydrologic_region"],
            "cs3_type": row["cs3_type"],
            "provider": row["provider"],
            "simulation_start_year": safe_int(row["simulation_start_year"]),
            "simulation_end_year": safe_int(row["simulation_end_year"]),
            "total_years": safe_int(row["total_years"]),
            # Demand (from AW_*)
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
            # Surface Water Delivery (from DN_*)
            "annual_sw_delivery_avg_taf": safe_float(row["annual_sw_delivery_avg_taf"]),
            "annual_sw_delivery_cv": safe_float(row["annual_sw_delivery_cv"]),
            # Groundwater Pumping (calculated as AW - DN)
            "annual_gw_pumping_avg_taf": safe_float(row["annual_gw_pumping_avg_taf"]),
            "annual_gw_pumping_cv": safe_float(row["annual_gw_pumping_cv"]),
            "gw_pumping_pct_of_demand": safe_float(row["gw_pumping_pct_of_demand"]),
            # Shortage (from GW_SHORT_*)
            "annual_shortage_avg_taf": safe_float(row["annual_shortage_avg_taf"]),
            "shortage_years_count": safe_int(row["shortage_years_count"]),
            "shortage_frequency_pct": safe_float(row["shortage_frequency_pct"]),
            "annual_shortage_pct_of_demand": safe_float(row["annual_shortage_pct_of_demand"]),
            "reliability_pct": safe_float(row["reliability_pct"]),
            "avg_pct_demand_met": safe_float(row["avg_pct_demand_met"]),
        }

    return {"scenario_id": scenario_id, "demand_units": demand_units}
