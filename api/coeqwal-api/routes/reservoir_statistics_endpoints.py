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
from pydantic import BaseModel, Field

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


async def get_reservoir_metadata(
    connection: asyncpg.Connection, short_codes: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch reservoir metadata (name, capacity, dead_pool) from database.

    Returns dict keyed by short_code with reservoir attributes.
    """
    try:
        query = """
        SELECT short_code, name, capacity_taf, dead_pool_taf
        FROM reservoir_entity
        WHERE short_code = ANY($1)
        """
        rows = await connection.fetch(query, short_codes)
        return {
            row["short_code"]: {
                "name": row["name"] or row["short_code"],
                "capacity_taf": float(row["capacity_taf"])
                if row["capacity_taf"]
                else 0.0,
                "dead_pool_taf": float(row["dead_pool_taf"])
                if row["dead_pool_taf"]
                else 0.0,
            }
            for row in rows
        }
    except Exception:
        return {}


WATER_MONTH_NAMES = {
    1: "October",
    2: "November",
    3: "December",
    4: "January",
    5: "February",
    6: "March",
    7: "April",
    8: "May",
    9: "June",
    10: "July",
    11: "August",
    12: "September",
}


async def get_all_reservoir_metadata(
    connection: asyncpg.Connection,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch metadata for all reservoirs from database.

    Returns dict keyed by short_code with reservoir attributes.
    """
    try:
        query = """
        SELECT short_code, name, capacity_taf, dead_pool_taf
        FROM reservoir_entity
        """
        rows = await connection.fetch(query)
        return {
            row["short_code"]: {
                "name": row["name"] or row["short_code"],
                "capacity_taf": float(row["capacity_taf"])
                if row["capacity_taf"]
                else 0.0,
                "dead_pool_taf": float(row["dead_pool_taf"])
                if row["dead_pool_taf"]
                else 0.0,
            }
            for row in rows
        }
    except Exception:
        return {}


# =============================================================================
# PYDANTIC MODELS
# =============================================================================


class MonthlyPercentiles(BaseModel):
    """Percentile data for a single water month"""

    q0: float = Field(..., description="0th percentile - minimum (% of capacity)")
    q10: float = Field(..., description="10th percentile")
    q30: float = Field(..., description="30th percentile")
    q50: float = Field(..., description="50th percentile (median)")
    q70: float = Field(..., description="70th percentile")
    q90: float = Field(..., description="90th percentile")
    q100: float = Field(..., description="100th percentile - maximum")
    mean: float = Field(..., description="Mean value")


class ReservoirPercentileResponse(BaseModel):
    """Response for single reservoir percentile data"""

    reservoir_id: str = Field(..., description="Reservoir short_code (e.g., 'SHSTA')")
    reservoir_name: str = Field(..., description="Human-readable name")
    scenario_id: str = Field(..., description="Scenario identifier")
    unit: str = Field("percent_capacity", description="Data unit")
    max_capacity_taf: float = Field(..., description="Reservoir capacity in TAF")
    monthly_percentiles: Dict[int, MonthlyPercentiles] = Field(
        ..., description="Percentiles by water month (1=Oct, 12=Sep)"
    )


class ReservoirSummary(BaseModel):
    """Summary info for a reservoir"""

    reservoir_id: str
    reservoir_name: str
    max_capacity_taf: float


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
    "/scenarios/{scenario_id}/reservoirs/{reservoir_id}/percentiles",
    summary="Get reservoir monthly percentiles",
)
async def get_reservoir_percentiles(
    scenario_id: str,
    reservoir_id: str,
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get monthly percentile data for a reservoir (for band charts).

    **Use case:** Render percentile band charts showing storage distribution
    across water years for each month.

    **Example:** `GET /api/statistics/scenarios/s0020/reservoirs/SHSTA/percentiles`

    **Response:**
    ```json
    {
      "reservoir_id": "SHSTA",
      "reservoir_name": "Shasta",
      "scenario_id": "s0020",
      "unit": "percent_capacity",
      "capacity_taf": 4552.0,
      "dead_pool_taf": 115.0,
      "monthly_percentiles": {
        "1": {"q0": 32.1, "q10": 45.2, "q30": 58.7, "q50": 70.1, "q70": 81.2, "q90": 91.3, "q100": 98.5, "mean": 68.4},
        "2": {"q0": 35.2, "q10": 48.1, ...},
        ...
        "12": {"q0": 30.5, ...}
      }
    }
    ```

    **Water months:** 1=October, 2=November, ..., 12=September

    **Band chart rendering:**
    - Outer band: q10 to q90 (lightest color)
    - Inner bands: q30-q70 (darker)
    - Center line: q50 (median)
    - Full range: q0 (min) to q100 (max) available for tooltips
    """
    # Reject S_* prefixed codes
    if reservoir_id.startswith("S_") or reservoir_id.startswith("C_"):
        raise HTTPException(
            status_code=400,
            detail=f"Use entity short_code (e.g., SHSTA), not variable code ({reservoir_id})",
        )

    try:
        # Get reservoir metadata
        metadata = await get_reservoir_metadata(connection, [reservoir_id])
        if not metadata:
            raise HTTPException(
                status_code=404, detail=f"Reservoir {reservoir_id} not found"
            )

        query = """
        SELECT
            rmp.water_month, rmp.q0, rmp.q10, rmp.q30, rmp.q50, rmp.q70, rmp.q90, rmp.q100,
            rmp.mean_value
        FROM reservoir_monthly_percentile rmp
        JOIN reservoir_entity re ON rmp.reservoir_entity_id = re.id
        WHERE rmp.scenario_short_code = $1 AND re.short_code = $2
        ORDER BY rmp.water_month
        """
        rows = await connection.fetch(query, scenario_id, reservoir_id)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No percentile data found for reservoir {reservoir_id} in scenario {scenario_id}",
            )

        monthly = {}
        for row in rows:
            monthly[row["water_month"]] = {
                "q0": float(row["q0"]) if row["q0"] else 0.0,
                "q10": float(row["q10"]) if row["q10"] else 0.0,
                "q30": float(row["q30"]) if row["q30"] else 0.0,
                "q50": float(row["q50"]) if row["q50"] else 0.0,
                "q70": float(row["q70"]) if row["q70"] else 0.0,
                "q90": float(row["q90"]) if row["q90"] else 0.0,
                "q100": float(row["q100"]) if row["q100"] else 0.0,
                "mean": float(row["mean_value"]) if row["mean_value"] else 0.0,
            }

        attrs = metadata[reservoir_id]
        return {
            "reservoir_id": reservoir_id,
            "reservoir_name": attrs["name"],
            "scenario_id": scenario_id,
            "unit": "percent_capacity",
            "capacity_taf": attrs["capacity_taf"],
            "dead_pool_taf": attrs["dead_pool_taf"],
            "monthly_percentiles": monthly,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


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
                    "capacity_taf": float(row["capacity_taf"])
                    if row["capacity_taf"]
                    else 0.0,
                    "dead_pool_taf": float(row["dead_pool_taf"])
                    if row["dead_pool_taf"]
                    else 0.0,
                    "monthly_percentiles": {},
                }

            reservoirs[short_code]["monthly_percentiles"][row["water_month"]] = {
                "q0": float(row["q0"]) if row["q0"] else 0.0,
                "q10": float(row["q10"]) if row["q10"] else 0.0,
                "q30": float(row["q30"]) if row["q30"] else 0.0,
                "q50": float(row["q50"]) if row["q50"] else 0.0,
                "q70": float(row["q70"]) if row["q70"] else 0.0,
                "q90": float(row["q90"]) if row["q90"] else 0.0,
                "q100": float(row["q100"]) if row["q100"] else 0.0,
                "mean": float(row["mean_value"]) if row["mean_value"] else 0.0,
            }

        response = {"scenario_id": scenario_id, "reservoirs": reservoirs}
        if group:
            response["group"] = group
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/reservoirs", summary="List available reservoirs")
async def list_reservoirs(
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get list of major reservoirs with percentile data available.

    **Use case:** Populate reservoir selector dropdowns in the UI.

    **Response:**
    ```json
    {
      "reservoirs": [
        {"reservoir_id": "SHSTA", "reservoir_name": "Shasta"},
        {"reservoir_id": "TRNTY", "reservoir_name": "Trinity"},
        ...
      ]
    }
    ```
    """
    try:
        # Get major reservoirs from database
        query = """
        SELECT re.short_code, re.name
        FROM reservoir_group_member rgm
        JOIN reservoir_entity re ON re.id = rgm.reservoir_entity_id
        JOIN reservoir_group rg ON rg.id = rgm.reservoir_group_id
        WHERE rg.short_code = 'major'
        ORDER BY re.name
        """
        rows = await connection.fetch(query)

        return {
            "reservoirs": [
                {
                    "reservoir_id": row["short_code"],
                    "reservoir_name": row["name"] or row["short_code"],
                }
                for row in rows
            ]
        }
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


@router.get("/scenarios", summary="List scenarios with percentile data")
async def list_scenarios_with_percentiles(
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Discover which scenarios have reservoir percentile data.

    **Use case:** Show available scenarios in the UI before making
    detailed data requests.

    **Response:**
    ```json
    {
      "scenarios": [
        {
          "scenario_id": "s0020",
          "reservoirs": ["FOLSM", "MELON", "MLRTN", ...],
          "reservoir_count": 8
        },
        ...
      ],
      "total": 8
    }
    ```
    """
    try:
        query = """
        SELECT
            rmp.scenario_short_code,
            array_agg(DISTINCT re.short_code ORDER BY re.short_code) as reservoirs,
            COUNT(DISTINCT re.short_code) as reservoir_count
        FROM reservoir_monthly_percentile rmp
        JOIN reservoir_entity re ON rmp.reservoir_entity_id = re.id
        GROUP BY rmp.scenario_short_code
        ORDER BY rmp.scenario_short_code
        """
        rows = await connection.fetch(query)

        scenarios = [
            {
                "scenario_id": row["scenario_short_code"],
                "reservoirs": list(row["reservoirs"]),
                "reservoir_count": row["reservoir_count"],
            }
            for row in rows
        ]

        return {"scenarios": scenarios, "total": len(scenarios)}

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
    "/scenarios/{scenario_id}/storage-monthly", summary="Get monthly storage statistics"
)
async def get_storage_monthly(
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
    Get monthly storage statistics for reservoirs in both percent-of-capacity and TAF.

    **Use case:** Render storage percentile band charts for each water month.
    Use `monthly_percent` for normalized comparison across reservoirs, or
    `monthly_taf` for absolute volume visualization.

    **Water Month Keys:**
    The response uses numeric keys 1-12 representing water year months:
    - 1 = October (start of water year)
    - 2 = November
    - 3 = December
    - 4 = January
    - 5 = February
    - 6 = March
    - 7 = April
    - 8 = May
    - 9 = June
    - 10 = July
    - 11 = August
    - 12 = September (end of water year)

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/storage-monthly` (defaults to major reservoirs)
    - `GET /api/statistics/scenarios/s0020/storage-monthly?group=cvp` (CVP reservoirs)
    - `GET /api/statistics/scenarios/s0020/storage-monthly?reservoirs=SHSTA,OROVL,FOLSM`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "reservoirs": {
        "SHSTA": {
          "name": "Shasta",
          "capacity_taf": 4552.0,
          "monthly_percent": {
            "1": {"q0": 32.1, "q10": 45.2, "q30": 58.7, "q50": 70.1, "q70": 81.2, "q90": 91.3, "q100": 98.5, "mean": 68.4},
            "2": {...},
            ...
            "12": {...}
          },
          "monthly_taf": {
            "1": {"q0": 1461, "q10": 2057, "q30": 2672, "q50": 3190, "q70": 3696, "q90": 4156, "q100": 4484, "mean": 3113},
            "2": {...},
            ...
            "12": {...}
          }
        }
      }
    }
    ```

    **Percentile bands for charts:**
    - Outer band: q10 to q90 (lightest)
    - Middle band: q30 to q70
    - Center line: q50 (median)
    - Full range: q0 (min) to q100 (max) for tooltips
    """
    reservoir_list = await parse_reservoirs(reservoirs, group, connection)

    try:
        query = """
        SELECT
            re.short_code, re.name, rsm.water_month,
            rsm.storage_avg_taf, rsm.storage_cv, rsm.storage_pct_capacity,
            rsm.q0, rsm.q10, rsm.q30, rsm.q50, rsm.q70, rsm.q90, rsm.q100,
            rsm.q0_taf, rsm.q10_taf, rsm.q30_taf, rsm.q50_taf, rsm.q70_taf, rsm.q90_taf, rsm.q100_taf,
            rsm.capacity_taf, rsm.sample_count
        FROM reservoir_storage_monthly rsm
        JOIN reservoir_entity re ON rsm.reservoir_entity_id = re.id
        WHERE rsm.scenario_short_code = $1 AND re.short_code = ANY($2)
        ORDER BY re.short_code, rsm.water_month
        """
        rows = await connection.fetch(query, scenario_id, reservoir_list)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No storage data found for scenario {scenario_id}",
            )

        result = {}
        for row in rows:
            short_code = row["short_code"]
            if short_code not in result:
                result[short_code] = {
                    "name": row["name"] or short_code,
                    "capacity_taf": float(row["capacity_taf"])
                    if row["capacity_taf"]
                    else 0.0,
                    "monthly_percent": {},
                    "monthly_taf": {},
                }

            wm = row["water_month"]

            # Percent of capacity
            result[short_code]["monthly_percent"][wm] = {
                "q0": float(row["q0"]) if row["q0"] else 0.0,
                "q10": float(row["q10"]) if row["q10"] else 0.0,
                "q30": float(row["q30"]) if row["q30"] else 0.0,
                "q50": float(row["q50"]) if row["q50"] else 0.0,
                "q70": float(row["q70"]) if row["q70"] else 0.0,
                "q90": float(row["q90"]) if row["q90"] else 0.0,
                "q100": float(row["q100"]) if row["q100"] else 0.0,
                "mean": float(row["storage_pct_capacity"])
                if row["storage_pct_capacity"]
                else 0.0,
            }

            # Volume in TAF
            result[short_code]["monthly_taf"][wm] = {
                "q0": float(row["q0_taf"]) if row["q0_taf"] else 0.0,
                "q10": float(row["q10_taf"]) if row["q10_taf"] else 0.0,
                "q30": float(row["q30_taf"]) if row["q30_taf"] else 0.0,
                "q50": float(row["q50_taf"]) if row["q50_taf"] else 0.0,
                "q70": float(row["q70_taf"]) if row["q70_taf"] else 0.0,
                "q90": float(row["q90_taf"]) if row["q90_taf"] else 0.0,
                "q100": float(row["q100_taf"]) if row["q100_taf"] else 0.0,
                "mean": float(row["storage_avg_taf"])
                if row["storage_avg_taf"]
                else 0.0,
            }

        response = {"scenario_id": scenario_id, "reservoirs": result}
        if group:
            response["group"] = group
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


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
                "spill_months_count": row["spill_months_count"] or 0,
                "total_months": row["total_months"] or 0,
                "spill_frequency_pct": float(row["spill_frequency_pct"])
                if row["spill_frequency_pct"]
                else 0.0,
                "spill_avg_cfs": float(row["spill_avg_cfs"])
                if row["spill_avg_cfs"]
                else 0.0,
                "spill_max_cfs": float(row["spill_max_cfs"])
                if row["spill_max_cfs"]
                else 0.0,
                "spill_q50": float(row["spill_q50"]) if row["spill_q50"] else 0.0,
                "spill_q90": float(row["spill_q90"]) if row["spill_q90"] else 0.0,
                "spill_q100": float(row["spill_q100"]) if row["spill_q100"] else 0.0,
                "storage_at_spill_avg_pct": float(row["storage_at_spill_avg_pct"])
                if row["storage_at_spill_avg_pct"]
                else None,
            }

        response = {"scenario_id": scenario_id, "reservoirs": result}
        if group:
            response["group"] = group
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/scenarios/{scenario_id}/period-summary",
    summary="Get period-of-record summary statistics",
)
async def get_period_summary(
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
    Get period-of-record summary statistics for reservoirs.

    **Use case:** Storage exceedance curves, spill risk assessment, threshold markers.

    **Examples:**
    - `GET /api/statistics/scenarios/s0020/period-summary` (defaults to major reservoirs)
    - `GET /api/statistics/scenarios/s0020/period-summary?group=cvp` (CVP reservoirs)
    - `GET /api/statistics/scenarios/s0020/period-summary?reservoirs=SHSTA,OROVL`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "group": "cvp",
      "reservoirs": {
        "SHSTA": {
          "name": "Shasta",
          "capacity_taf": 4552.0,
          "simulation_years": {"start": 1922, "end": 2021, "total": 100},
          "storage_exceedance": {"p5": 35.2, "p10": 42.1, "p25": 55.3, "p50": 68.7, "p75": 81.2, "p90": 91.5, "p95": 96.2},
          "thresholds": {"dead_pool_taf": 115.0, "dead_pool_pct": 2.5, "spill_threshold_pct": 95.2},
          "spill": {"frequency_pct": 45.0, "years_count": 45, "mean_cfs": 5000, "peak_cfs": 25000, ...}
        }
      }
    }
    ```
    """
    reservoir_list = await parse_reservoirs(reservoirs, group, connection)

    try:
        query = """
        SELECT
            re.short_code, re.name,
            rps.simulation_start_year, rps.simulation_end_year, rps.total_years,
            rps.storage_exc_p5, rps.storage_exc_p10, rps.storage_exc_p25, rps.storage_exc_p50,
            rps.storage_exc_p75, rps.storage_exc_p90, rps.storage_exc_p95,
            rps.dead_pool_taf, rps.dead_pool_pct, rps.spill_threshold_pct,
            rps.spill_years_count, rps.spill_frequency_pct,
            rps.spill_mean_cfs, rps.spill_peak_cfs,
            rps.annual_spill_avg_taf, rps.annual_spill_cv, rps.annual_spill_max_taf,
            rps.annual_max_spill_q50, rps.annual_max_spill_q90, rps.annual_max_spill_q100,
            rps.capacity_taf
        FROM reservoir_period_summary rps
        JOIN reservoir_entity re ON rps.reservoir_entity_id = re.id
        WHERE rps.scenario_short_code = $1 AND re.short_code = ANY($2)
        ORDER BY re.short_code
        """
        rows = await connection.fetch(query, scenario_id, reservoir_list)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No period summary data found for scenario {scenario_id}",
            )

        result = {}
        for row in rows:
            short_code = row["short_code"]
            result[short_code] = {
                "name": row["name"] or short_code,
                "capacity_taf": float(row["capacity_taf"])
                if row["capacity_taf"]
                else 0.0,
                "simulation_years": {
                    "start": row["simulation_start_year"],
                    "end": row["simulation_end_year"],
                    "total": row["total_years"],
                },
                "storage_exceedance": {
                    "p5": float(row["storage_exc_p5"])
                    if row["storage_exc_p5"]
                    else 0.0,
                    "p10": float(row["storage_exc_p10"])
                    if row["storage_exc_p10"]
                    else 0.0,
                    "p25": float(row["storage_exc_p25"])
                    if row["storage_exc_p25"]
                    else 0.0,
                    "p50": float(row["storage_exc_p50"])
                    if row["storage_exc_p50"]
                    else 0.0,
                    "p75": float(row["storage_exc_p75"])
                    if row["storage_exc_p75"]
                    else 0.0,
                    "p90": float(row["storage_exc_p90"])
                    if row["storage_exc_p90"]
                    else 0.0,
                    "p95": float(row["storage_exc_p95"])
                    if row["storage_exc_p95"]
                    else 0.0,
                },
                "thresholds": {
                    "dead_pool_taf": float(row["dead_pool_taf"])
                    if row["dead_pool_taf"]
                    else 0.0,
                    "dead_pool_pct": float(row["dead_pool_pct"])
                    if row["dead_pool_pct"]
                    else 0.0,
                    "spill_threshold_pct": float(row["spill_threshold_pct"])
                    if row["spill_threshold_pct"]
                    else None,
                },
                "spill": {
                    "years_count": row["spill_years_count"] or 0,
                    "frequency_pct": float(row["spill_frequency_pct"])
                    if row["spill_frequency_pct"]
                    else 0.0,
                    "mean_cfs": float(row["spill_mean_cfs"])
                    if row["spill_mean_cfs"]
                    else 0.0,
                    "peak_cfs": float(row["spill_peak_cfs"])
                    if row["spill_peak_cfs"]
                    else 0.0,
                    "annual_avg_taf": float(row["annual_spill_avg_taf"])
                    if row["annual_spill_avg_taf"]
                    else 0.0,
                    "annual_cv": float(row["annual_spill_cv"])
                    if row["annual_spill_cv"]
                    else 0.0,
                    "annual_max_taf": float(row["annual_spill_max_taf"])
                    if row["annual_spill_max_taf"]
                    else 0.0,
                    "annual_max_q50": float(row["annual_max_spill_q50"])
                    if row["annual_max_spill_q50"]
                    else 0.0,
                    "annual_max_q90": float(row["annual_max_spill_q90"])
                    if row["annual_max_spill_q90"]
                    else 0.0,
                    "annual_max_q100": float(row["annual_max_spill_q100"])
                    if row["annual_max_spill_q100"]
                    else 0.0,
                },
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
                "capacity_taf": float(row["capacity_taf"])
                if row["capacity_taf"]
                else 0.0,
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
