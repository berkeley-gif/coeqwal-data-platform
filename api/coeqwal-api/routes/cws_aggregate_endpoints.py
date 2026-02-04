"""
CWS Aggregate Statistics API endpoints for COEQWAL.

Provides monthly and period summary data for Community Water System (CWS) aggregates:
- SWP Total M&I: Total State Water Project Municipal & Industrial deliveries
- CVP North: Central Valley Project M&I deliveries - North of Delta
- CVP South: Central Valley Project M&I deliveries - South of Delta
- MWD: Metropolitan Water District aggregate deliveries

Water months: Oct=1, Nov=2, ..., Sep=12
Values: TAF (thousand acre-feet)
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, Any, List, Optional
import asyncpg

router = APIRouter(prefix="/api/statistics", tags=["statistics"])


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

db_pool = None


def set_db_pool(pool):
    global db_pool
    db_pool = pool


async def get_db():
    if db_pool is None:
        raise HTTPException(status_code=500, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


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


async def parse_aggregates(
    aggregate: Optional[str],
    connection: asyncpg.Connection,
) -> List[str]:
    """
    Parse aggregate filter parameter.

    Args:
        aggregate: Comma-separated short_codes (e.g., 'swp_total,cvp_nod') or None for all
        connection: Database connection

    Returns:
        List of aggregate short_codes to query
    """
    if aggregate:
        return [a.strip() for a in aggregate.split(",")]

    # Default: return all aggregates
    query = "SELECT short_code FROM cws_aggregate_entity ORDER BY display_order"
    rows = await connection.fetch(query)
    return [row["short_code"] for row in rows]


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/cws-aggregates", summary="List CWS aggregates")
async def list_cws_aggregates(
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get list of available CWS aggregate entities.

    **Use case:** Populate aggregate selector dropdowns in the UI.

    **Response:**
    ```json
    {
      "aggregates": [
        {"short_code": "swp_total", "label": "SWP Total M&I", "project": "SWP"},
        {"short_code": "cvp_nod", "label": "CVP North", "project": "CVP"},
        {"short_code": "cvp_sod", "label": "CVP South", "project": "CVP"},
        {"short_code": "mwd", "label": "Metropolitan Water District", "project": "MWD"}
      ]
    }
    ```
    """
    try:
        query = """
        SELECT short_code, label, description, project, region,
               delivery_variable, shortage_variable, display_order
        FROM cws_aggregate_entity
        WHERE is_active = TRUE
        ORDER BY display_order
        """
        rows = await connection.fetch(query)

        if not rows:
            raise HTTPException(
                status_code=404, detail="No CWS aggregates found"
            )

        aggregates = [
            {
                "short_code": row["short_code"],
                "label": row["label"],
                "description": row["description"],
                "project": row["project"],
                "region": row["region"],
            }
            for row in rows
        ]

        return {"aggregates": aggregates}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/scenarios/{scenario_id}/cws-aggregates/monthly",
    summary="Get CWS aggregate monthly statistics",
)
async def get_cws_aggregate_monthly(
    scenario_id: str,
    aggregate: Optional[str] = Query(
        None,
        description="Comma-separated aggregate short_codes (e.g., 'swp_total,cvp_nod'). Defaults to all.",
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get monthly delivery and shortage statistics for CWS aggregates.

    **Use case:** Render percentile band charts showing delivery/shortage
    distribution across water years for each month.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/cws-aggregates/monthly` (all aggregates)
    - `GET /api/statistics/scenarios/s0020/cws-aggregates/monthly?aggregate=swp_total` (single)
    - `GET /api/statistics/scenarios/s0020/cws-aggregates/monthly?aggregate=cvp_nod,cvp_sod` (multiple)

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "aggregates": {
        "swp_total": {
          "label": "SWP Total M&I",
          "monthly_delivery": {
            "1": {"avg_taf": 125.5, "cv": 0.35, "q0": 45.2, "q10": 78.3, ...},
            ...
          },
          "monthly_shortage": {
            "1": {"avg_taf": 12.5, "cv": 1.2, "frequency_pct": 35.5, ...},
            ...
          }
        }
      }
    }
    ```

    **Water months:** 1=October, 2=November, ..., 12=September
    """
    aggregate_list = await parse_aggregates(aggregate, connection)

    try:
        query = """
        SELECT
            e.short_code, e.label,
            m.water_month,
            m.delivery_avg_taf, m.delivery_cv,
            m.delivery_q0, m.delivery_q10, m.delivery_q30, m.delivery_q50,
            m.delivery_q70, m.delivery_q90, m.delivery_q100,
            m.shortage_avg_taf, m.shortage_cv, m.shortage_frequency_pct,
            m.shortage_q0, m.shortage_q10, m.shortage_q30, m.shortage_q50,
            m.shortage_q70, m.shortage_q90, m.shortage_q100,
            m.demand_avg_taf, m.percent_of_demand_avg,
            m.sample_count
        FROM cws_aggregate_monthly m
        JOIN cws_aggregate_entity e ON m.cws_aggregate_id = e.id
        WHERE m.scenario_short_code = $1 AND e.short_code = ANY($2)
        ORDER BY e.display_order, m.water_month
        """
        rows = await connection.fetch(query, scenario_id, aggregate_list)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No monthly data found for scenario {scenario_id}",
            )

        aggregates = {}
        for row in rows:
            short_code = row["short_code"]

            if short_code not in aggregates:
                aggregates[short_code] = {
                    "label": row["label"],
                    "monthly_delivery": {},
                    "monthly_shortage": {},
                }

            wm = row["water_month"]

            # Delivery statistics
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
                "demand_avg_taf": safe_float(row["demand_avg_taf"]),
                "percent_of_demand": safe_float(row["percent_of_demand_avg"]),
                "sample_count": safe_int(row["sample_count"]),
            }

            # Shortage statistics
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

        return {"scenario_id": scenario_id, "aggregates": aggregates}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/scenarios/{scenario_id}/cws-aggregates/period-summary",
    summary="Get CWS aggregate period summary",
)
async def get_cws_aggregate_period_summary(
    scenario_id: str,
    aggregate: Optional[str] = Query(
        None,
        description="Comma-separated aggregate short_codes. Defaults to all.",
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get period-of-record summary statistics for CWS aggregates.

    **Use case:** Dashboard display of key reliability metrics and
    exceedance statistics for the full simulation period.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/cws-aggregates/period-summary`
    - `GET /api/statistics/scenarios/s0020/cws-aggregates/period-summary?aggregate=swp_total`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "aggregates": {
        "swp_total": {
          "label": "SWP Total M&I",
          "simulation_start_year": 1922,
          "simulation_end_year": 2021,
          "total_years": 100,
          "annual_delivery_avg_taf": 1506.5,
          "annual_delivery_cv": 0.28,
          "delivery_exceedance": {
            "p5": 2050.5, "p10": 1950.2, "p25": 1750.1,
            "p50": 1506.5, "p75": 1250.3, "p90": 1050.1, "p95": 920.5
          },
          "annual_shortage_avg_taf": 150.5,
          "shortage_years_count": 45,
          "shortage_frequency_pct": 45.0,
          "shortage_exceedance": { ... },
          "reliability_pct": 90.0,
          "avg_pct_allocation_met": 91.5
        }
      }
    }
    ```
    """
    aggregate_list = await parse_aggregates(aggregate, connection)

    try:
        query = """
        SELECT
            e.short_code, e.label,
            p.simulation_start_year, p.simulation_end_year, p.total_years,
            p.annual_delivery_avg_taf, p.annual_delivery_cv,
            p.delivery_exc_p5, p.delivery_exc_p10, p.delivery_exc_p25,
            p.delivery_exc_p50, p.delivery_exc_p75, p.delivery_exc_p90, p.delivery_exc_p95,
            p.annual_shortage_avg_taf, p.shortage_years_count, p.shortage_frequency_pct,
            p.shortage_exc_p5, p.shortage_exc_p10, p.shortage_exc_p25,
            p.shortage_exc_p50, p.shortage_exc_p75, p.shortage_exc_p90, p.shortage_exc_p95,
            p.reliability_pct, p.avg_pct_allocation_met,
            p.annual_demand_avg_taf, p.avg_pct_demand_met
        FROM cws_aggregate_period_summary p
        JOIN cws_aggregate_entity e ON p.cws_aggregate_id = e.id
        WHERE p.scenario_short_code = $1 AND e.short_code = ANY($2)
        ORDER BY e.display_order
        """
        rows = await connection.fetch(query, scenario_id, aggregate_list)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No period summary found for scenario {scenario_id}",
            )

        aggregates = {}
        for row in rows:
            short_code = row["short_code"]

            aggregates[short_code] = {
                "label": row["label"],
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
                "avg_pct_allocation_met": safe_float(row["avg_pct_allocation_met"]),
                "annual_demand_avg_taf": safe_float(row["annual_demand_avg_taf"]),
                "avg_pct_demand_met": safe_float(row["avg_pct_demand_met"]),
            }

        return {"scenario_id": scenario_id, "aggregates": aggregates}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/scenarios/{scenario_id}/cws-aggregates/{aggregate_id}/monthly",
    summary="Get single CWS aggregate monthly statistics",
)
async def get_single_cws_aggregate_monthly(
    scenario_id: str,
    aggregate_id: str,
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get monthly statistics for a single CWS aggregate.

    **Example:** `GET /api/statistics/scenarios/s0020/cws-aggregates/swp_total/monthly`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "aggregate_id": "swp_total",
      "label": "SWP Total M&I",
      "monthly_delivery": {
        "1": {"avg_taf": 125.5, "cv": 0.35, ...},
        ...
      },
      "monthly_shortage": {
        "1": {"avg_taf": 12.5, ...},
        ...
      }
    }
    ```
    """
    try:
        # Get entity info
        entity_query = """
        SELECT id, label FROM cws_aggregate_entity
        WHERE short_code = $1 AND is_active = TRUE
        """
        entity = await connection.fetchrow(entity_query, aggregate_id)
        if not entity:
            raise HTTPException(
                status_code=404, detail=f"Aggregate {aggregate_id} not found"
            )

        query = """
        SELECT
            water_month,
            delivery_avg_taf, delivery_cv,
            delivery_q0, delivery_q10, delivery_q30, delivery_q50,
            delivery_q70, delivery_q90, delivery_q100,
            shortage_avg_taf, shortage_cv, shortage_frequency_pct,
            shortage_q0, shortage_q10, shortage_q30, shortage_q50,
            shortage_q70, shortage_q90, shortage_q100,
            demand_avg_taf, percent_of_demand_avg,
            sample_count
        FROM cws_aggregate_monthly
        WHERE scenario_short_code = $1 AND cws_aggregate_id = $2
        ORDER BY water_month
        """
        rows = await connection.fetch(query, scenario_id, entity["id"])

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No monthly data found for {aggregate_id} in scenario {scenario_id}",
            )

        monthly_delivery = {}
        monthly_shortage = {}

        for row in rows:
            wm = row["water_month"]

            monthly_delivery[wm] = {
                "avg_taf": safe_float(row["delivery_avg_taf"]),
                "cv": safe_float(row["delivery_cv"]),
                "q0": safe_float(row["delivery_q0"]),
                "q10": safe_float(row["delivery_q10"]),
                "q30": safe_float(row["delivery_q30"]),
                "q50": safe_float(row["delivery_q50"]),
                "q70": safe_float(row["delivery_q70"]),
                "q90": safe_float(row["delivery_q90"]),
                "q100": safe_float(row["delivery_q100"]),
                "demand_avg_taf": safe_float(row["demand_avg_taf"]),
                "percent_of_demand": safe_float(row["percent_of_demand_avg"]),
                "sample_count": safe_int(row["sample_count"]),
            }

            monthly_shortage[wm] = {
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

        return {
            "scenario_id": scenario_id,
            "aggregate_id": aggregate_id,
            "label": entity["label"],
            "monthly_delivery": monthly_delivery,
            "monthly_shortage": monthly_shortage,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
