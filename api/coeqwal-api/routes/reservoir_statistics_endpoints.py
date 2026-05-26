"""
Reservoir Statistics API endpoints for COEQWAL.

Provides monthly percentile data for reservoir storage, enabling
percentile band charts in the frontend.

Terminology:
- Reservoir entities: SHSTA, TRNTY, OROVL, etc. (short_code in reservoir_entity table)
- Statistics tables link to entities via reservoir_entity_id FK

Major Reservoirs (8 total, fetched from reservoir_group 'major'):
- SHSTA (Shasta), TRNTY (Trinity), OROVL (Oroville), FOLSM (Folsom)
- MELON (New Melones), MLRTN (Millerton), SLUIS_CVP (San Luis CVP), SLUIS_SWP (San Luis SWP)

API accepts entity short_codes only (SHSTA), not CalSim variable codes (S_SHSTA).

All 92 reservoirs available via statistics endpoints.

Water months: Oct=1, Nov=2, ..., Sep=12
Values: Percent of reservoir capacity (0-100+)
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, Any, List, Optional
import asyncpg

from routes._common.null_handling import safe_float, safe_int

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

# =============================================================================
# CONSTANTS
# =============================================================================

# Fallback entity short_codes if database query fails
MAJOR_RESERVOIRS_FALLBACK = [
    "SHSTA",
    "TRNTY",
    "OROVL",
    "FOLSM",
    "MELON",
    "MLRTN",
    "SLUIS_CVP",
    "SLUIS_SWP",
]


async def get_major_reservoirs(connection: asyncpg.Connection) -> List[str]:
    """
    Fetch major reservoir short_codes from reservoir_group membership.

    Returns entity short_codes (e.g., SHSTA, not S_SHSTA).
    The 'major' group is defined in reservoir_group_member table.
    """
    try:
        query = """
        SELECT re.short_code
        FROM reservoir_group_member rgm
        JOIN reservoir_entity re ON re.id = rgm.reservoir_entity_id
        JOIN reservoir_group rg ON rg.id = rgm.reservoir_group_id
        WHERE rg.short_code = 'major'
        ORDER BY re.short_code
        """
        rows = await connection.fetch(query)
        if rows:
            return [row["short_code"] for row in rows]
    except Exception:
        pass
    return MAJOR_RESERVOIRS_FALLBACK


# Valid reservoir group codes
VALID_RESERVOIR_GROUPS = ["major", "cvp", "swp"]


async def get_reservoirs_by_group(
    connection: asyncpg.Connection, group_code: str
) -> List[str]:
    """
    Fetch reservoir short_codes for a given group (major, cvp, swp).

    Args:
        connection: Database connection
        group_code: Group short_code ('major', 'cvp', or 'swp')

    Returns:
        List of reservoir entity short_codes (e.g., ['SHSTA', 'OROVL', ...])

    Raises:
        HTTPException: If group_code is invalid or group not found
    """
    if group_code not in VALID_RESERVOIR_GROUPS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid group '{group_code}'. Valid groups: {', '.join(VALID_RESERVOIR_GROUPS)}",
        )

    query = """
    SELECT re.short_code
    FROM reservoir_group_member rgm
    JOIN reservoir_entity re ON re.id = rgm.reservoir_entity_id
    JOIN reservoir_group rg ON rg.id = rgm.reservoir_group_id
    WHERE rg.short_code = $1
    ORDER BY re.short_code
    """
    rows = await connection.fetch(query, group_code)

    if not rows:
        raise HTTPException(
            status_code=404, detail=f"No reservoirs found for group '{group_code}'"
        )

    return [row["short_code"] for row in rows]


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
# ENDPOINTS
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/reservoir-percentiles",
    summary="Get reservoir percentiles for scenario",
)
async def get_all_reservoir_percentiles(
    scenario_id: str,
    reservoirs: Optional[str] = Query(
        None,
        description="Comma-separated reservoir short_codes (e.g., 'SHSTA,OROVL'). Defaults to 8 major reservoirs.",
    ),
    group: Optional[str] = Query(
        None,
        description="Reservoir group filter: 'major', 'cvp', or 'swp'. Cannot be used with 'reservoirs' parameter.",
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get percentile data for reservoirs (% of capacity) in a single request.

    **Use case:** Load reservoir data at once for comparison views
    or dashboard initialization.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/reservoir-percentiles` (defaults to major reservoirs)
    - `GET /api/statistics/scenarios/s0020/reservoir-percentiles?group=major` (8 major reservoirs)
    - `GET /api/statistics/scenarios/s0020/reservoir-percentiles?group=cvp` (CVP reservoirs)
    - `GET /api/statistics/scenarios/s0020/reservoir-percentiles?group=swp` (SWP reservoirs)
    - `GET /api/statistics/scenarios/s0020/reservoir-percentiles?reservoirs=SHSTA,OROVL` (custom list)

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "group": "major",
      "reservoirs": {
        "SHSTA": {
          "name": "Shasta",
          "capacity_taf": 4552.0,
          "dead_pool_taf": 115.0,
          "monthly_percentiles": {
            "1": {"q0": 32.1, "q10": 45.2, "q30": 58.7, "q50": 70.1, "q70": 81.2, "q90": 91.3, "q100": 98.5, "mean": 68.4},
            ...
          }
        },
        "OROVL": { ... },
        ...
      }
    }
    ```
    """
    reservoir_list = await parse_reservoirs(reservoirs, group, connection)

    try:
        query = """
        SELECT
            re.short_code, rmp.water_month,
            rmp.q0, rmp.q10, rmp.q30, rmp.q50, rmp.q70, rmp.q90, rmp.q100,
            rmp.mean_value,
            re.name, re.capacity_taf, re.dead_pool_taf
        FROM reservoir_monthly_percentile rmp
        JOIN reservoir_entity re ON rmp.reservoir_entity_id = re.id
        WHERE rmp.scenario_short_code = $1 AND re.short_code = ANY($2)
        ORDER BY re.short_code, rmp.water_month
        """
        rows = await connection.fetch(query, scenario_id, reservoir_list)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No percentile data found for scenario {scenario_id}",
            )

        reservoirs = {}
        for row in rows:
            short_code = row["short_code"]

            if short_code not in reservoirs:
                reservoirs[short_code] = {
                    "name": row["name"] or short_code,
                    "capacity_taf": safe_float(row["capacity_taf"]),
                    "dead_pool_taf": safe_float(row["dead_pool_taf"]),
                    "monthly_percentiles": {},
                }

            reservoirs[short_code]["monthly_percentiles"][row["water_month"]] = {
                "q0": safe_float(row["q0"]),
                "q10": safe_float(row["q10"]),
                "q30": safe_float(row["q30"]),
                "q50": safe_float(row["q50"]),
                "q70": safe_float(row["q70"]),
                "q90": safe_float(row["q90"]),
                "q100": safe_float(row["q100"]),
                "mean": safe_float(row["mean_value"]),
            }

        response = {"scenario_id": scenario_id, "reservoirs": reservoirs}
        if group:
            response["group"] = group
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/reservoir-groups", summary="List reservoir groups")
async def list_reservoir_groups(
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get list of reservoir groups with their member reservoirs.

    **Use case:** Populate group selector dropdowns in the UI.

    **Response:**
    ```json
    {
      "groups": [
        {
          "group_id": "major",
          "name": "Major Reservoirs",
          "reservoirs": ["FOLSM", "MELON", "MLRTN", "OROVL", "SHSTA", "SLUIS_CVP", "SLUIS_SWP", "TRNTY"]
        },
        {
          "group_id": "cvp",
          "name": "CVP Reservoirs",
          "reservoirs": ["FOLSM", "MELON", "MLRTN", "SHSTA", "SLUIS_CVP", "TRNTY"]
        },
        {
          "group_id": "swp",
          "name": "SWP Reservoirs",
          "reservoirs": ["OROVL", "SLUIS_SWP"]
        }
      ]
    }
    ```
    """
    try:
        query = """
        SELECT
            rg.short_code, rg.name,
            array_agg(re.short_code ORDER BY re.short_code) as reservoirs
        FROM reservoir_group rg
        JOIN reservoir_group_member rgm ON rg.id = rgm.reservoir_group_id
        JOIN reservoir_entity re ON re.id = rgm.reservoir_entity_id
        WHERE rg.short_code IN ('major', 'cvp', 'swp')
        GROUP BY rg.short_code, rg.name
        ORDER BY rg.short_code
        """
        rows = await connection.fetch(query)

        return {
            "groups": [
                {
                    "group_id": row["short_code"],
                    "name": row["name"] or row["short_code"],
                    "reservoirs": list(row["reservoirs"]),
                }
                for row in rows
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# =============================================================================
# NEW STATISTICS ENDPOINTS (All 92 Reservoirs)
# =============================================================================


async def parse_reservoirs(
    reservoirs: Optional[str], group: Optional[str], connection: asyncpg.Connection
) -> List[str]:
    """
    Parse reservoir filter from either comma-separated codes or group name.

    Args:
        reservoirs: Comma-separated reservoir short_codes (e.g., 'SHSTA,OROVL')
        group: Reservoir group code ('major', 'cvp', or 'swp')
        connection: Database connection

    Returns:
        List of entity short_codes (e.g., ['SHSTA', 'OROVL'])

    Raises:
        HTTPException: If both reservoirs and group are provided, or invalid input
    """
    # Mutual exclusivity check
    if reservoirs and group:
        raise HTTPException(
            status_code=400,
            detail="Cannot specify both 'reservoirs' and 'group' parameters. Use one or the other.",
        )

    # Group filter
    if group:
        return await get_reservoirs_by_group(connection, group)

    # Explicit reservoir list
    if reservoirs:
        codes = [r.strip() for r in reservoirs.split(",") if r.strip()]

        # Validate: reject S_* or C_* prefixed codes
        for code in codes:
            if code.startswith("S_") or code.startswith("C_"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Use entity short_code (e.g., SHSTA), not variable code ({code})",
                )
        return codes

    # Default to major reservoirs
    return await get_major_reservoirs(connection)


@router.get(
    "/scenarios/{scenario_id}/spill-monthly", summary="Get monthly spill statistics"
)
async def get_spill_monthly(
    scenario_id: str,
    reservoirs: Optional[str] = Query(
        None,
        description="Comma-separated reservoir short_codes. Defaults to 8 major reservoirs.",
    ),
    group: Optional[str] = Query(
        None,
        description="Reservoir group filter: 'major', 'cvp', or 'swp'. Cannot be used with 'reservoirs' parameter.",
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get monthly spill (flood release) statistics for reservoirs.

    **Use case:** Analyze spill frequency and magnitude by month.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/spill-monthly` (defaults to major reservoirs)
    - `GET /api/statistics/scenarios/s0020/spill-monthly?group=cvp` (CVP reservoirs)
    - `GET /api/statistics/scenarios/s0020/spill-monthly?reservoirs=SHSTA,OROVL`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "group": "cvp",
      "reservoirs": {
        "SHSTA": {
          "name": "Shasta",
          "monthly": {
            "1": {"spill_frequency_pct": 12.5, "spill_avg_cfs": 5000, "storage_at_spill_avg_pct": 95.2, ...},
            ...
          }
        }
      }
    }
    ```
    """
    reservoir_list = await parse_reservoirs(reservoirs, group, connection)

    try:
        query = """
        SELECT
            re.short_code, re.name, rsm.water_month,
            rsm.spill_months_count, rsm.total_months, rsm.spill_frequency_pct,
            rsm.spill_avg_cfs, rsm.spill_max_cfs,
            rsm.spill_q50, rsm.spill_q90, rsm.spill_q100,
            rsm.storage_at_spill_avg_pct
        FROM reservoir_spill_monthly rsm
        JOIN reservoir_entity re ON rsm.reservoir_entity_id = re.id
        WHERE rsm.scenario_short_code = $1 AND re.short_code = ANY($2)
        ORDER BY re.short_code, rsm.water_month
        """
        rows = await connection.fetch(query, scenario_id, reservoir_list)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No spill data found for scenario {scenario_id}",
            )

        result = {}
        for row in rows:
            short_code = row["short_code"]
            if short_code not in result:
                result[short_code] = {"name": row["name"] or short_code, "monthly": {}}

            result[short_code]["monthly"][row["water_month"]] = {
                "spill_months_count": safe_int(row["spill_months_count"]),
                "total_months": safe_int(row["total_months"]),
                "spill_frequency_pct": safe_float(row["spill_frequency_pct"]),
                "spill_avg_cfs": safe_float(row["spill_avg_cfs"]),
                "spill_max_cfs": safe_float(row["spill_max_cfs"]),
                "spill_q50": safe_float(row["spill_q50"]),
                "spill_q90": safe_float(row["spill_q90"]),
                "spill_q100": safe_float(row["spill_q100"]),
                "storage_at_spill_avg_pct": safe_float(row["storage_at_spill_avg_pct"]),
            }

        response = {"scenario_id": scenario_id, "reservoirs": result}
        if group:
            response["group"] = group
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/reservoirs/all", summary="List all reservoirs with statistics data")
async def list_all_reservoirs(
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get list of all reservoirs with statistics data available.

    **Use case:** Populate reservoir selector for custom reservoir selection.

    **Response:**
    ```json
    {
      "major": ["SHSTA", "TRNTY", ...],
      "all": [
        {"reservoir_id": "ALMNR", "name": "Almanor", "capacity_taf": 1143.0},
        ...
      ],
      "total": 90
    }
    ```
    """
    try:
        query = """
        SELECT DISTINCT
            re.short_code, re.name,
            rps.capacity_taf
        FROM reservoir_period_summary rps
        JOIN reservoir_entity re ON rps.reservoir_entity_id = re.id
        ORDER BY re.short_code
        """
        rows = await connection.fetch(query)

        all_reservoirs = [
            {
                "reservoir_id": row["short_code"],
                "name": row["name"] or row["short_code"],
                "capacity_taf": safe_float(row["capacity_taf"]),
            }
            for row in rows
        ]

        major_reservoirs = await get_major_reservoirs(connection)
        return {
            "major": major_reservoirs,
            "all": all_reservoirs,
            "total": len(all_reservoirs),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
