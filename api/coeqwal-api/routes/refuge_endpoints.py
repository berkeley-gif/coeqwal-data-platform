"""
Wildlife Refuge Demand Unit Statistics API Endpoints.

CalSim variable semantics:
  AWO_{DU_ID} = Applied Water Output = DEMAND (from SV input, TAF)
  DN_{DU_ID}  = Net Surface Water Delivery (from deliveries CSV, TAF)
  Shortage    = max(demand - delivery, 0) .derived, no native CalSim variable

Provides statistics for 18 wildlife refuge demand units:
  - GET /api/statistics/refuge-demand-units          .list DUs + metadata
  - GET /api/statistics/scenarios/{id}/refuge-demand-units/delivery-monthly
  - GET /api/statistics/scenarios/{id}/refuge-demand-units/shortage-monthly
  - GET /api/statistics/scenarios/{id}/refuge-demand-units/period-summary

Water months: 1=October ... 12=September
Values: TAF (thousand acre-feet), percentages, or correlation coefficients
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from routes._common.null_handling import safe_float, safe_int

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

_db_pool = None


def set_db_pool(pool):
    """Set the database connection pool."""
    global _db_pool
    _db_pool = pool


# =============================================================================
# LIST REFUGE DEMAND UNITS
# =============================================================================


@router.get(
    "/refuge-demand-units",
    summary="List wildlife refuge demand units",
    description="Returns all 18 wildlife refuge demand unit entities with optional filtering.",
)
async def list_refuge_demand_units(
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region (SAC, SJR, TULARE)"
    ),
    cs3_type: Optional[str] = Query(
        None, description="Filter by CS3 type (PR = Priority Refuge, NR = Non-priority Refuge)"
    ),
):
    """
    List wildlife refuge demand units.

    **Regions:** SAC (Sacramento), SJR (San Joaquin), TULARE
    **CS3 types:** PR (Priority Refuge.CVP contract), NR (Non-priority Refuge.water rights)

    Response:
    ```json
    {
      "demand_units": [
        {
          "du_id": "08N_PR1",
          "wba_id": "08N",
          "hydrologic_region": "SAC",
          "cs3_type": "PR",
          "refuge_or_wildlife_area": "Sacramento NWR",
          "managed_by": "USFWS",
          "provider": "Reclamation / Glenn-Colusa Canal",
          "sw": true
        },
        ...
      ],
      "total": 18
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
                refuge_or_wildlife_area,
                managed_by,
                provider,
                sw
            FROM du_refuge_entity
            WHERE is_active = TRUE
        """
        params = []

        if region:
            params.append(region.upper())
            query += f" AND UPPER(hydrologic_region) = ${len(params)}"
        if cs3_type:
            params.append(cs3_type.upper())
            query += f" AND UPPER(cs3_type) = ${len(params)}"

        query += " ORDER BY hydrologic_region, du_id"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"refuge-demand-units query failed: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

    demand_units = [
        {
            "du_id": row["du_id"],
            "wba_id": row["wba_id"],
            "hydrologic_region": row["hydrologic_region"],
            "cs3_type": row["cs3_type"],
            "refuge_or_wildlife_area": row["refuge_or_wildlife_area"],
            "managed_by": row["managed_by"],
            "provider": row["provider"],
            "sw": row["sw"],
        }
        for row in rows
    ]

    return {"demand_units": demand_units, "total": len(demand_units)}


# =============================================================================
# MONTHLY DELIVERY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/refuge-demand-units/delivery-monthly",
    summary="Monthly SW delivery statistics for refuge demand units",
    description=(
        "Returns monthly surface water delivery percentile bands for all refuge "
        "demand units in a given scenario. Values in TAF."
    ),
)
async def get_refuge_delivery_monthly(
    scenario_id: str,
    du_id: Optional[str] = Query(None, description="Filter to a single demand unit"),
    water_month: Optional[int] = Query(
        None, ge=1, le=12, description="Filter to a specific water month (1=Oct, 12=Sep)"
    ),
):
    """
    Monthly surface water delivery statistics.

    Each row covers one (du_id × water_month) combination.
    Percentile bands span all simulated water years for that month.

    Response includes: delivery_avg_taf, delivery_cv, q0–q100, exc_p5–exc_p95, sample_count.
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                du_id,
                water_month,
                delivery_avg_taf,
                delivery_cv,
                q0, q10, q30, q50, q70, q90, q100,
                exc_p5, exc_p10, exc_p25, exc_p50, exc_p75, exc_p90, exc_p95,
                sample_count
            FROM refuge_du_delivery_monthly
            WHERE scenario_short_code = $1
              AND is_active = TRUE
        """
        params: list = [scenario_id]

        if du_id:
            params.append(du_id)
            query += f" AND du_id = ${len(params)}"
        if water_month is not None:
            params.append(water_month)
            query += f" AND water_month = ${len(params)}"

        query += " ORDER BY du_id, water_month"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"refuge delivery-monthly query failed for {scenario_id}: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No refuge delivery statistics found for scenario '{scenario_id}'"
        )

    data = [
        {
            "du_id": row["du_id"],
            "water_month": row["water_month"],
            "delivery_avg_taf": safe_float(row["delivery_avg_taf"]),
            "delivery_cv": safe_float(row["delivery_cv"]),
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

    return {
        "scenario_id": scenario_id,
        "data": data,
        "count": len(data),
    }


# =============================================================================
# MONTHLY SHORTAGE
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/refuge-demand-units/shortage-monthly",
    summary="Monthly shortage statistics for refuge demand units",
    description=(
        "Returns monthly delivery shortage statistics (TAF and %) for all refuge "
        "demand units. Shortage = max(demand - delivery, 0). Values in TAF and %."
    ),
)
async def get_refuge_shortage_monthly(
    scenario_id: str,
    du_id: Optional[str] = Query(None, description="Filter to a single demand unit"),
    water_month: Optional[int] = Query(
        None, ge=1, le=12, description="Filter to a specific water month (1=Oct, 12=Sep)"
    ),
):
    """
    Monthly delivery shortage statistics (TAF and %).

    Shortage is derived: shortage_taf = max(AWO_{DU_ID} - DN_{DU_ID}, 0).
    No native CalSim shortage variable exists for refuge demand units.

    Response includes: shortage_avg_taf, shortage_cv, shortage_pct_avg, shortage_pct_cv,
    shortage_frequency_pct, q0–q100 (TAF), exc_p5–exc_p95 (TAF), sample_count.
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                du_id,
                water_month,
                shortage_avg_taf,
                shortage_cv,
                shortage_pct_avg,
                shortage_pct_cv,
                shortage_frequency_pct,
                q0, q10, q30, q50, q70, q90, q100,
                exc_p5, exc_p10, exc_p25, exc_p50, exc_p75, exc_p90, exc_p95,
                sample_count
            FROM refuge_du_shortage_monthly
            WHERE scenario_short_code = $1
              AND is_active = TRUE
        """
        params: list = [scenario_id]

        if du_id:
            params.append(du_id)
            query += f" AND du_id = ${len(params)}"
        if water_month is not None:
            params.append(water_month)
            query += f" AND water_month = ${len(params)}"

        query += " ORDER BY du_id, water_month"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"refuge shortage-monthly query failed for {scenario_id}: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No refuge shortage statistics found for scenario '{scenario_id}'"
        )

    data = [
        {
            "du_id": row["du_id"],
            "water_month": row["water_month"],
            "shortage_avg_taf": safe_float(row["shortage_avg_taf"]),
            "shortage_cv": safe_float(row["shortage_cv"]),
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
        for row in rows
    ]

    return {
        "scenario_id": scenario_id,
        "data": data,
        "count": len(data),
    }


# =============================================================================
# PERIOD SUMMARY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/refuge-demand-units/period-summary",
    summary="Period-of-record summary for refuge demand units",
    description=(
        "Returns period-of-record annual delivery/shortage statistics and reliability "
        "for all refuge demand units in a scenario."
    ),
)
async def get_refuge_period_summary(
    scenario_id: str,
    du_id: Optional[str] = Query(None, description="Filter to a single demand unit"),
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region (SAC, SJR, TULARE)"
    ),
):
    """
    Period-of-record summary statistics.

    reliability_pct_95: 95th percentile of annual shortage %.
    Interpretation: in 95 of 100 simulated years, annual shortage ≤ this value.
    A value of 0 means perfectly reliable in 95% of years.

    Response includes: annual delivery/shortage averages and CVs, reliability_pct_95,
    annual delivery exceedance curve (exc_p5–exc_p95), simulation period metadata.
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                ps.du_id,
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
        """

        if region:
            query += """
                JOIN du_refuge_entity e ON ps.du_id = e.du_id
            """

        query += " WHERE ps.scenario_short_code = $1 AND ps.is_active = TRUE"
        params: list = [scenario_id]

        if du_id:
            params.append(du_id)
            query += f" AND ps.du_id = ${len(params)}"
        if region:
            params.append(region.upper())
            query += f" AND UPPER(e.hydrologic_region) = ${len(params)}"

        query += " ORDER BY ps.du_id"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"refuge period-summary query failed for {scenario_id}: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No refuge period summary found for scenario '{scenario_id}'"
        )

    data = [
        {
            "du_id": row["du_id"],
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
        for row in rows
    ]

    return {
        "scenario_id": scenario_id,
        "data": data,
        "count": len(data),
    }
