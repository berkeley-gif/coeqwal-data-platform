"""
Batch Statistics API endpoint for COEQWAL.

Provides a single endpoint to fetch multiple statistics types for multiple scenarios
in a single request, dramatically reducing load times in the Data Explorer.

Instead of making ~24 individual API calls (N scenarios × M types × P endpoints),
clients can make 1 batched request.

Example:
    GET /api/statistics/batch?scenarios=s0020,s0021,s0022&types=storage,cws,ag
"""

import asyncio
from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, List, Optional

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
# QUERY FUNCTIONS
# =============================================================================


async def fetch_storage_monthly(conn, scenario_id: str) -> Dict[str, Any]:
    """Fetch monthly storage percentile data for major reservoirs."""
    query = """
    SELECT
        re.short_code as reservoir_id,
        re.name as reservoir_name,
        re.capacity_taf,
        re.dead_pool_taf,
        sm.water_month,
        sm.pct_q0, sm.pct_q10, sm.pct_q30, sm.pct_q50,
        sm.pct_q70, sm.pct_q90, sm.pct_q100, sm.pct_mean,
        sm.taf_q0, sm.taf_q10, sm.taf_q30, sm.taf_q50,
        sm.taf_q70, sm.taf_q90, sm.taf_q100, sm.taf_mean
    FROM reservoir_storage_monthly sm
    JOIN reservoir_entity re ON sm.reservoir_entity_id = re.id
    JOIN reservoir_group_member rgm ON rgm.reservoir_entity_id = re.id
    JOIN reservoir_group rg ON rg.id = rgm.reservoir_group_id
    WHERE sm.scenario_short_code = $1 AND rg.short_code = 'major'
    ORDER BY re.short_code, sm.water_month
    """
    rows = await conn.fetch(query, scenario_id)

    reservoirs = {}
    for row in rows:
        rid = row["reservoir_id"]
        if rid not in reservoirs:
            reservoirs[rid] = {
                "name": row["reservoir_name"],
                "capacity_taf": safe_float(row["capacity_taf"]),
                "dead_pool_taf": safe_float(row["dead_pool_taf"]),
                "monthly_percent": {},
                "monthly_taf": {},
            }

        wm = str(row["water_month"])
        reservoirs[rid]["monthly_percent"][wm] = {
            "q0": safe_float(row["pct_q0"]),
            "q10": safe_float(row["pct_q10"]),
            "q30": safe_float(row["pct_q30"]),
            "q50": safe_float(row["pct_q50"]),
            "q70": safe_float(row["pct_q70"]),
            "q90": safe_float(row["pct_q90"]),
            "q100": safe_float(row["pct_q100"]),
            "mean": safe_float(row["pct_mean"]),
        }
        reservoirs[rid]["monthly_taf"][wm] = {
            "q0": safe_float(row["taf_q0"]),
            "q10": safe_float(row["taf_q10"]),
            "q30": safe_float(row["taf_q30"]),
            "q50": safe_float(row["taf_q50"]),
            "q70": safe_float(row["taf_q70"]),
            "q90": safe_float(row["taf_q90"]),
            "q100": safe_float(row["taf_q100"]),
            "mean": safe_float(row["taf_mean"]),
        }

    return {"scenario_id": scenario_id, "reservoirs": reservoirs}


async def fetch_cws_aggregates_monthly(conn, scenario_id: str) -> Dict[str, Any]:
    """Fetch monthly CWS aggregate delivery and shortage data."""
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
        m.demand_avg_taf, m.percent_of_demand_avg
    FROM cws_aggregate_monthly m
    JOIN cws_aggregate_entity e ON m.cws_aggregate_id = e.id
    WHERE m.scenario_short_code = $1 AND e.is_active = TRUE
    ORDER BY e.display_order, m.water_month
    """
    rows = await conn.fetch(query, scenario_id)

    aggregates = {}
    for row in rows:
        short_code = row["short_code"]
        if short_code not in aggregates:
            aggregates[short_code] = {
                "label": row["label"],
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
            "demand_avg_taf": safe_float(row["demand_avg_taf"]),
            "percent_of_demand": safe_float(row["percent_of_demand_avg"]),
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

    return {"scenario_id": scenario_id, "aggregates": aggregates}


async def fetch_cws_aggregates_period(conn, scenario_id: str) -> Dict[str, Any]:
    """Fetch period summary for CWS aggregates."""
    query = """
    SELECT
        e.short_code, e.label,
        p.annual_delivery_avg_taf,
        p.reliability_pct,
        p.shortage_frequency_pct
    FROM cws_aggregate_period p
    JOIN cws_aggregate_entity e ON p.cws_aggregate_id = e.id
    WHERE p.scenario_short_code = $1 AND e.is_active = TRUE
    ORDER BY e.display_order
    """
    rows = await conn.fetch(query, scenario_id)

    aggregates = {}
    for row in rows:
        aggregates[row["short_code"]] = {
            "label": row["label"],
            "annual_delivery_avg_taf": safe_float(row["annual_delivery_avg_taf"]),
            "reliability_pct": safe_float(row["reliability_pct"]),
            "shortage_frequency_pct": safe_float(row["shortage_frequency_pct"]),
        }

    return {"scenario_id": scenario_id, "aggregates": aggregates}


async def fetch_ag_aggregates_monthly(conn, scenario_id: str) -> Dict[str, Any]:
    """Fetch monthly AG aggregate delivery data."""
    query = """
    SELECT
        e.short_code, e.label,
        m.water_month,
        m.delivery_avg_taf, m.delivery_cv,
        m.delivery_q0, m.delivery_q10, m.delivery_q30, m.delivery_q50,
        m.delivery_q70, m.delivery_q90, m.delivery_q100
    FROM ag_aggregate_monthly m
    JOIN ag_aggregate_entity e ON m.ag_aggregate_id = e.id
    WHERE m.scenario_short_code = $1 AND e.is_active = TRUE
    ORDER BY e.display_order, m.water_month
    """
    rows = await conn.fetch(query, scenario_id)

    aggregates = {}
    for row in rows:
        short_code = row["short_code"]
        if short_code not in aggregates:
            aggregates[short_code] = {
                "label": row["label"],
                "monthly_delivery": {},
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
        }

    return {"scenario_id": scenario_id, "aggregates": aggregates}


async def fetch_ag_aggregates_period(conn, scenario_id: str) -> Dict[str, Any]:
    """Fetch period summary for AG aggregates."""
    query = """
    SELECT
        e.short_code, e.label,
        p.annual_delivery_avg_taf
    FROM ag_aggregate_period p
    JOIN ag_aggregate_entity e ON p.ag_aggregate_id = e.id
    WHERE p.scenario_short_code = $1 AND e.is_active = TRUE
    ORDER BY e.display_order
    """
    rows = await conn.fetch(query, scenario_id)

    aggregates = {}
    for row in rows:
        aggregates[row["short_code"]] = {
            "label": row["label"],
            "annual_delivery_avg_taf": safe_float(row["annual_delivery_avg_taf"]),
        }

    return {"scenario_id": scenario_id, "aggregates": aggregates}


# =============================================================================
# BATCH ENDPOINT
# =============================================================================


@router.get(
    "/batch",
    summary="Batch fetch statistics for multiple scenarios",
    description="Fetch storage, CWS, and AG statistics for multiple scenarios in a single request.",
)
async def get_batch_statistics(
    scenarios: str = Query(
        ...,
        description="Comma-separated scenario IDs (e.g., 's0020,s0021,s0022')",
    ),
    types: str = Query(
        "storage,cws,ag",
        description="Comma-separated data types to fetch: storage, cws, ag",
    ),
) -> Dict[str, Any]:
    """
    Batch fetch statistics for multiple scenarios and data types.

    This endpoint reduces frontend load time by fetching all required data
    in a single request instead of multiple individual requests.

    **Parameters:**
    - `scenarios`: Comma-separated scenario IDs (required)
    - `types`: Comma-separated data types (default: storage,cws,ag)

    **Response:**
    ```json
    {
      "scenarios": ["s0020", "s0021"],
      "storage": {
        "s0020": { "reservoirs": {...} },
        "s0021": { "reservoirs": {...} }
      },
      "cws": {
        "s0020": { "monthly": {...}, "period": {...} },
        "s0021": { "monthly": {...}, "period": {...} }
      },
      "ag": {
        "s0020": { "monthly": {...}, "period": {...} },
        "s0021": { "monthly": {...}, "period": {...} }
      }
    }
    ```
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    # Parse parameters
    scenario_list = [s.strip() for s in scenarios.split(",") if s.strip()]
    type_list = [t.strip().lower() for t in types.split(",") if t.strip()]

    if not scenario_list:
        raise HTTPException(status_code=400, detail="No scenarios provided")

    valid_types = {"storage", "cws", "ag"}
    invalid_types = set(type_list) - valid_types
    if invalid_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid types: {invalid_types}. Valid types: {valid_types}",
        )

    # Build list of async tasks
    tasks = []
    task_keys = []  # Track (type, scenario, subtype) for each task

    async with _db_pool.acquire() as conn:
        for scenario_id in scenario_list:
            if "storage" in type_list:
                tasks.append(fetch_storage_monthly(conn, scenario_id))
                task_keys.append(("storage", scenario_id, "data"))

            if "cws" in type_list:
                tasks.append(fetch_cws_aggregates_monthly(conn, scenario_id))
                task_keys.append(("cws", scenario_id, "monthly"))
                tasks.append(fetch_cws_aggregates_period(conn, scenario_id))
                task_keys.append(("cws", scenario_id, "period"))

            if "ag" in type_list:
                tasks.append(fetch_ag_aggregates_monthly(conn, scenario_id))
                task_keys.append(("ag", scenario_id, "monthly"))
                tasks.append(fetch_ag_aggregates_period(conn, scenario_id))
                task_keys.append(("ag", scenario_id, "period"))

        # Run all tasks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Organize results
    response: Dict[str, Any] = {"scenarios": scenario_list}

    if "storage" in type_list:
        response["storage"] = {}
    if "cws" in type_list:
        response["cws"] = {}
    if "ag" in type_list:
        response["ag"] = {}

    for i, result in enumerate(results):
        data_type, scenario_id, subtype = task_keys[i]

        if isinstance(result, Exception):
            # Log error but continue with other results
            continue

        if data_type == "storage":
            response["storage"][scenario_id] = result
        elif data_type == "cws":
            if scenario_id not in response["cws"]:
                response["cws"][scenario_id] = {}
            response["cws"][scenario_id][subtype] = result
        elif data_type == "ag":
            if scenario_id not in response["ag"]:
                response["ag"][scenario_id] = {}
            response["ag"][scenario_id][subtype] = result

    return response
