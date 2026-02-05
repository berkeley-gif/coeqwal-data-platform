"""
Agricultural Demand Unit Statistics API Endpoints.

Provides statistics for agricultural demand units including:
- Monthly delivery statistics (from AW_* variables)
- Monthly shortage statistics (from GW_SHORT_* - SJR/Tulare only)
- Period-of-record summary
- Aggregate statistics (SWP PAG, CVP PAG, etc.)

Water months: 1=October, 2=November, ..., 12=September
Values: TAF (thousand acre-feet)

Note: Sacramento region DUs do NOT have shortage data.
"""

import logging
from typing import Optional

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
    """Safely convert value to float, returning None for NULL."""
    if val is None:
        return None
    return float(val)


def safe_int(val) -> Optional[int]:
    """Safely convert value to int, returning None for NULL."""
    if val is None:
        return None
    return int(val)


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
# LIST AG AGGREGATES
# =============================================================================


@router.get(
    "/ag-aggregates",
    summary="List agricultural aggregates",
    description="Returns available agricultural aggregate entities (SWP/CVP project aggregates).",
)
async def list_ag_aggregates():
    """
    List all agricultural aggregate entities.

    **Response:**
    ```json
    {
      "aggregates": [
        {"short_code": "swp_pag", "label": "SWP Project AG", "project": "SWP", "region": "TOTAL"},
        {"short_code": "swp_pag_n", "label": "SWP Project AG North", "project": "SWP", "region": "NOD"},
        ...
      ]
    }
    ```
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT short_code, label, project, region, delivery_variable, description
            FROM ag_aggregate_entity
            WHERE is_active = TRUE
            ORDER BY display_order
        """
        rows = await conn.fetch(query)

    return {
        "aggregates": [
            {
                "short_code": row["short_code"],
                "label": row["label"],
                "project": row["project"],
                "region": row["region"],
                "delivery_variable": row["delivery_variable"],
                "description": row["description"],
            }
            for row in rows
        ]
    }


# =============================================================================
# DU DELIVERY MONTHLY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-demand-units/delivery-monthly",
    summary="Get monthly delivery statistics for AG demand units",
)
async def get_ag_du_delivery_monthly(
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
    Get monthly delivery statistics for agricultural demand units.

    **Use case:** Render percentile band charts showing delivery distribution
    across water years for each month.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/delivery-monthly` (all DUs)
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/delivery-monthly?du_id=64_PA1` (single)
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/delivery-monthly?region=SJR` (by region)

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "demand_units": {
        "64_PA1": {
          "agency": "Westlands WD",
          "hydrologic_region": "SJR",
          "cs3_type": "PA",
          "monthly_delivery": {
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
                m.delivery_avg_taf,
                m.delivery_cv,
                m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                m.sample_count
            FROM ag_du_delivery_monthly m
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
            detail=f"No delivery data found for scenario {scenario_id}",
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
# DU SHORTAGE MONTHLY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-demand-units/shortage-monthly",
    summary="Get monthly shortage statistics for AG demand units",
)
async def get_ag_du_shortage_monthly(
    scenario_id: str,
    du_id: Optional[str] = Query(
        None, description="Comma-separated DU IDs to filter"
    ),
    region: Optional[str] = Query(
        None, description="Filter by hydrologic region (SJR, TULARE only - Sacramento has no shortage data)"
    ),
):
    """
    Get monthly groundwater shortage statistics for agricultural demand units.

    **Important:** Only SJR and Tulare region DUs have shortage data.
    Sacramento region DUs do NOT have shortage data in CalSim output.

    **Use case:** Display shortage metrics including shortage as % of demand.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/shortage-monthly?region=SJR`
    - `GET /api/statistics/scenarios/s0020/ag-demand-units/shortage-monthly?du_id=64_PA1,72_XA1`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "demand_units": {
        "64_PA1": {
          "agency": "Westlands WD",
          "hydrologic_region": "SJR",
          "monthly_shortage": {
            "1": {
              "avg_taf": 12.5,
              "cv": 1.2,
              "frequency_pct": 35.5,
              "pct_of_demand_avg": 8.5,
              ...
            },
            ...
          }
        }
      }
    }
    ```
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
                m.water_month,
                m.shortage_avg_taf,
                m.shortage_cv,
                m.shortage_frequency_pct,
                m.shortage_pct_of_demand_avg,
                m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                m.sample_count
            FROM ag_du_shortage_monthly m
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

        query += " ORDER BY m.du_id, m.water_month"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No shortage data found for scenario {scenario_id}. Note: Sacramento region DUs do not have shortage data.",
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
                "monthly_shortage": {},
            }

        demand_units[du]["monthly_shortage"][str(row["water_month"])] = {
            "avg_taf": safe_float(row["shortage_avg_taf"]),
            "cv": safe_float(row["shortage_cv"]),
            "frequency_pct": safe_float(row["shortage_frequency_pct"]),
            "pct_of_demand_avg": safe_float(row["shortage_pct_of_demand_avg"]),
            "q0": safe_float(row["q0"]),
            "q10": safe_float(row["q10"]),
            "q30": safe_float(row["q30"]),
            "q50": safe_float(row["q50"]),
            "q70": safe_float(row["q70"]),
            "q90": safe_float(row["q90"]),
            "q100": safe_float(row["q100"]),
            # Exceedance percentiles
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
          "annual_delivery_avg_taf": 450.5,
          "annual_delivery_cv": 0.28,
          "delivery_exceedance": {...},
          "annual_shortage_avg_taf": 45.5,
          "shortage_years_count": 35,
          "shortage_frequency_pct": 35.0,
          "annual_shortage_pct_of_demand": 9.2,
          "reliability_pct": 90.8,
          "avg_pct_demand_met": 90.8,
          "annual_demand_avg_taf": 496.0
        }
      }
    }
    ```

    **Note:** Sacramento region DUs will have NULL shortage values.
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
                p.annual_delivery_avg_taf,
                p.annual_delivery_cv,
                p.delivery_exc_p5, p.delivery_exc_p10, p.delivery_exc_p25,
                p.delivery_exc_p50, p.delivery_exc_p75, p.delivery_exc_p90, p.delivery_exc_p95,
                p.annual_shortage_avg_taf,
                p.shortage_years_count,
                p.shortage_frequency_pct,
                p.annual_shortage_pct_of_demand,
                p.reliability_pct,
                p.avg_pct_demand_met,
                p.annual_demand_avg_taf
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
            "annual_shortage_pct_of_demand": safe_float(row["annual_shortage_pct_of_demand"]),
            "reliability_pct": safe_float(row["reliability_pct"]),
            "avg_pct_demand_met": safe_float(row["avg_pct_demand_met"]),
            "annual_demand_avg_taf": safe_float(row["annual_demand_avg_taf"]),
        }

    return {"scenario_id": scenario_id, "demand_units": demand_units}


# =============================================================================
# AGGREGATE MONTHLY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-aggregates/monthly",
    summary="Get monthly statistics for AG aggregates",
)
async def get_ag_aggregate_monthly(
    scenario_id: str,
    aggregate: Optional[str] = Query(
        None, description="Comma-separated aggregate codes (e.g., 'swp_pag,cvp_pag_n'). Defaults to all."
    ),
):
    """
    Get monthly delivery statistics for agricultural aggregates.

    **Use case:** Display SWP/CVP project-level agricultural delivery statistics.

    **Available aggregates:**
    - `swp_pag` - SWP Project AG Total
    - `swp_pag_n` - SWP Project AG North of Delta
    - `swp_pag_s` - SWP Project AG South of Delta
    - `cvp_pag_n` - CVP Project AG North of Delta
    - `cvp_pag_s` - CVP Project AG South of Delta

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/ag-aggregates/monthly` (all)
    - `GET /api/statistics/scenarios/s0020/ag-aggregates/monthly?aggregate=swp_pag`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "aggregates": {
        "swp_pag": {
          "label": "SWP Project AG",
          "monthly_delivery": {
            "1": {"avg_taf": 125.5, "cv": 0.35, ...},
            ...
          }
        }
      }
    }
    ```
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

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
        params = [scenario_id]

        if aggregate:
            codes = [a.strip() for a in aggregate.split(",")]
            query += f" AND m.aggregate_code = ANY(${len(params) + 1})"
            params.append(codes)

        query += " ORDER BY e.display_order, m.water_month"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No aggregate data found for scenario {scenario_id}",
        )

    aggregates = {}
    for row in rows:
        code = row["aggregate_code"]
        if code not in aggregates:
            aggregates[code] = {
                "label": row["label"],
                "project": row["project"],
                "region": row["region"],
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

    return {"scenario_id": scenario_id, "aggregates": aggregates}


# =============================================================================
# AGGREGATE PERIOD SUMMARY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/ag-aggregates/period-summary",
    summary="Get period summary for AG aggregates",
)
async def get_ag_aggregate_period_summary(
    scenario_id: str,
    aggregate: Optional[str] = Query(
        None, description="Comma-separated aggregate codes. Defaults to all."
    ),
):
    """
    Get period-of-record summary for agricultural aggregates.

    **Use case:** Dashboard display of annual delivery statistics
    for SWP/CVP project-level agricultural deliveries.

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "aggregates": {
        "swp_pag": {
          "label": "SWP Project AG",
          "simulation_start_year": 1922,
          "simulation_end_year": 2021,
          "total_years": 100,
          "annual_delivery_avg_taf": 2506.5,
          "annual_delivery_cv": 0.28,
          "delivery_exceedance": {...}
        }
      }
    }
    ```
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

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
        params = [scenario_id]

        if aggregate:
            codes = [a.strip() for a in aggregate.split(",")]
            query += f" AND p.aggregate_code = ANY(${len(params) + 1})"
            params.append(codes)

        query += " ORDER BY e.display_order"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No aggregate period summary found for scenario {scenario_id}",
        )

    aggregates = {}
    for row in rows:
        code = row["aggregate_code"]
        aggregates[code] = {
            "label": row["label"],
            "project": row["project"],
            "region": row["region"],
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

    return {"scenario_id": scenario_id, "aggregates": aggregates}
