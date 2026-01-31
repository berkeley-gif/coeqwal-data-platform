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
    'SHSTA', 'TRNTY', 'OROVL', 'FOLSM',
    'MELON', 'MLRTN', 'SLUIS_CVP', 'SLUIS_SWP'
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
            return [row['short_code'] for row in rows]
    except Exception:
        pass
    return MAJOR_RESERVOIRS_FALLBACK


async def get_reservoir_metadata(
    connection: asyncpg.Connection,
    short_codes: List[str]
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
            row['short_code']: {
                'name': row['name'] or row['short_code'],
                'capacity_taf': float(row['capacity_taf']) if row['capacity_taf'] else 0.0,
                'dead_pool_taf': float(row['dead_pool_taf']) if row['dead_pool_taf'] else 0.0,
            }
            for row in rows
        }
    except Exception:
        return {}

WATER_MONTH_NAMES = {
    1: 'October', 2: 'November', 3: 'December',
    4: 'January', 5: 'February', 6: 'March',
    7: 'April', 8: 'May', 9: 'June',
    10: 'July', 11: 'August', 12: 'September',
}


async def get_all_reservoir_metadata(connection: asyncpg.Connection) -> Dict[str, Dict[str, Any]]:
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
            row['short_code']: {
                'name': row['name'] or row['short_code'],
                'capacity_taf': float(row['capacity_taf']) if row['capacity_taf'] else 0.0,
                'dead_pool_taf': float(row['dead_pool_taf']) if row['dead_pool_taf'] else 0.0,
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
    summary="Get reservoir monthly percentiles"
)
async def get_reservoir_percentiles(
    scenario_id: str,
    reservoir_id: str,
    connection: asyncpg.Connection = Depends(get_db)
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
    if reservoir_id.startswith('S_') or reservoir_id.startswith('C_'):
        raise HTTPException(
            status_code=400,
            detail=f"Use entity short_code (e.g., SHSTA), not variable code ({reservoir_id})"
        )

    try:
        # Get reservoir metadata
        metadata = await get_reservoir_metadata(connection, [reservoir_id])
        if not metadata:
            raise HTTPException(
                status_code=404,
                detail=f"Reservoir {reservoir_id} not found"
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
                detail=f"No percentile data found for reservoir {reservoir_id} in scenario {scenario_id}"
            )

        monthly = {}
        for row in rows:
            monthly[row['water_month']] = {
                'q0': float(row['q0']) if row['q0'] else 0.0,
                'q10': float(row['q10']) if row['q10'] else 0.0,
                'q30': float(row['q30']) if row['q30'] else 0.0,
                'q50': float(row['q50']) if row['q50'] else 0.0,
                'q70': float(row['q70']) if row['q70'] else 0.0,
                'q90': float(row['q90']) if row['q90'] else 0.0,
                'q100': float(row['q100']) if row['q100'] else 0.0,
                'mean': float(row['mean_value']) if row['mean_value'] else 0.0,
            }

        attrs = metadata[reservoir_id]
        return {
            'reservoir_id': reservoir_id,
            'reservoir_name': attrs['name'],
            'scenario_id': scenario_id,
            'unit': 'percent_capacity',
            'capacity_taf': attrs['capacity_taf'],
            'dead_pool_taf': attrs['dead_pool_taf'],
            'monthly_percentiles': monthly
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/scenarios/{scenario_id}/reservoir-percentiles",
    summary="Get all reservoir percentiles for scenario"
)
async def get_all_reservoir_percentiles(
    scenario_id: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get percentile data for all major reservoirs in a single request.

    **Use case:** Load all reservoir data at once for comparison views
    or dashboard initialization.

    **Example:** `GET /api/statistics/scenarios/s0020/reservoir-percentiles`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
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
    try:
        query = """
        SELECT
            re.short_code, rmp.water_month,
            rmp.q0, rmp.q10, rmp.q30, rmp.q50, rmp.q70, rmp.q90, rmp.q100,
            rmp.mean_value,
            re.name, re.capacity_taf, re.dead_pool_taf
        FROM reservoir_monthly_percentile rmp
        JOIN reservoir_entity re ON rmp.reservoir_entity_id = re.id
        WHERE rmp.scenario_short_code = $1
        ORDER BY re.short_code, rmp.water_month
        """
        rows = await connection.fetch(query, scenario_id)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No percentile data found for scenario {scenario_id}"
            )

        reservoirs = {}
        for row in rows:
            short_code = row['short_code']

            if short_code not in reservoirs:
                reservoirs[short_code] = {
                    'name': row['name'] or short_code,
                    'capacity_taf': float(row['capacity_taf']) if row['capacity_taf'] else 0.0,
                    'dead_pool_taf': float(row['dead_pool_taf']) if row['dead_pool_taf'] else 0.0,
                    'monthly_percentiles': {}
                }

            reservoirs[short_code]['monthly_percentiles'][row['water_month']] = {
                'q0': float(row['q0']) if row['q0'] else 0.0,
                'q10': float(row['q10']) if row['q10'] else 0.0,
                'q30': float(row['q30']) if row['q30'] else 0.0,
                'q50': float(row['q50']) if row['q50'] else 0.0,
                'q70': float(row['q70']) if row['q70'] else 0.0,
                'q90': float(row['q90']) if row['q90'] else 0.0,
                'q100': float(row['q100']) if row['q100'] else 0.0,
                'mean': float(row['mean_value']) if row['mean_value'] else 0.0,
            }

        return {
            'scenario_id': scenario_id,
            'reservoirs': reservoirs
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/reservoirs", summary="List available reservoirs")
async def list_reservoirs(
    connection: asyncpg.Connection = Depends(get_db)
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
            'reservoirs': [
                {'reservoir_id': row['short_code'], 'reservoir_name': row['name'] or row['short_code']}
                for row in rows
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/scenarios", summary="List scenarios with percentile data")
async def list_scenarios_with_percentiles(
    connection: asyncpg.Connection = Depends(get_db)
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
                'scenario_id': row['scenario_short_code'],
                'reservoirs': list(row['reservoirs']),
                'reservoir_count': row['reservoir_count']
            }
            for row in rows
        ]

        return {
            'scenarios': scenarios,
            'total': len(scenarios)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# =============================================================================
# NEW STATISTICS ENDPOINTS (All 92 Reservoirs)
# =============================================================================

async def parse_reservoirs(
    reservoirs: Optional[str],
    connection: asyncpg.Connection
) -> List[str]:
    """
    Parse comma-separated reservoir short_codes. No S_* prefix accepted.

    Returns list of entity short_codes (e.g., ['SHSTA', 'OROVL']).
    """
    if not reservoirs:
        return await get_major_reservoirs(connection)

    codes = [r.strip() for r in reservoirs.split(',') if r.strip()]

    # Validate: reject S_* or C_* prefixed codes
    for code in codes:
        if code.startswith('S_') or code.startswith('C_'):
            raise HTTPException(
                status_code=400,
                detail=f"Use entity short_code (e.g., SHSTA), not variable code ({code})"
            )
    return codes


@router.get(
    "/scenarios/{scenario_id}/storage-monthly",
    summary="Get monthly storage statistics"
)
async def get_storage_monthly(
    scenario_id: str,
    reservoirs: Optional[str] = Query(
        None,
        description="Comma-separated reservoir short_codes (e.g., 'SHSTA,OROVL'). Defaults to 8 major reservoirs."
    ),
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get monthly storage statistics for reservoirs.

    **Use case:** Render storage percentile band charts for each water month.

    **Example:** `GET /api/statistics/scenarios/s0020/storage-monthly`
    **With custom reservoirs:** `GET /api/statistics/scenarios/s0020/storage-monthly?reservoirs=SHSTA,OROVL,FOLSM`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "reservoirs": {
        "SHSTA": {
          "name": "Shasta",
          "capacity_taf": 4552.0,
          "monthly": {
            "1": {"storage_avg_taf": 2850.5, "storage_pct_capacity": 62.6, "q0": 32.1, "q10": 45.2, ...},
            ...
          }
        }
      }
    }
    ```
    """
    reservoir_list = await parse_reservoirs(reservoirs, connection)

    try:
        query = """
        SELECT
            re.short_code, re.name, rsm.water_month,
            rsm.storage_avg_taf, rsm.storage_cv, rsm.storage_pct_capacity,
            rsm.q0, rsm.q10, rsm.q30, rsm.q50, rsm.q70, rsm.q90, rsm.q100,
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
                detail=f"No storage data found for scenario {scenario_id}"
            )

        result = {}
        for row in rows:
            short_code = row['short_code']
            if short_code not in result:
                result[short_code] = {
                    'name': row['name'] or short_code,
                    'capacity_taf': float(row['capacity_taf']) if row['capacity_taf'] else 0.0,
                    'monthly': {}
                }

            result[short_code]['monthly'][row['water_month']] = {
                'storage_avg_taf': float(row['storage_avg_taf']) if row['storage_avg_taf'] else 0.0,
                'storage_cv': float(row['storage_cv']) if row['storage_cv'] else 0.0,
                'storage_pct_capacity': float(row['storage_pct_capacity']) if row['storage_pct_capacity'] else 0.0,
                'q0': float(row['q0']) if row['q0'] else 0.0,
                'q10': float(row['q10']) if row['q10'] else 0.0,
                'q30': float(row['q30']) if row['q30'] else 0.0,
                'q50': float(row['q50']) if row['q50'] else 0.0,
                'q70': float(row['q70']) if row['q70'] else 0.0,
                'q90': float(row['q90']) if row['q90'] else 0.0,
                'q100': float(row['q100']) if row['q100'] else 0.0,
                'sample_count': row['sample_count'] or 0,
            }

        return {
            'scenario_id': scenario_id,
            'reservoirs': result
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/scenarios/{scenario_id}/spill-monthly",
    summary="Get monthly spill statistics"
)
async def get_spill_monthly(
    scenario_id: str,
    reservoirs: Optional[str] = Query(
        None,
        description="Comma-separated reservoir short_codes. Defaults to 8 major reservoirs."
    ),
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get monthly spill (flood release) statistics for reservoirs.

    **Use case:** Analyze spill frequency and magnitude by month.

    **Example:** `GET /api/statistics/scenarios/s0020/spill-monthly`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
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
    reservoir_list = await parse_reservoirs(reservoirs, connection)

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
                detail=f"No spill data found for scenario {scenario_id}"
            )

        result = {}
        for row in rows:
            short_code = row['short_code']
            if short_code not in result:
                result[short_code] = {
                    'name': row['name'] or short_code,
                    'monthly': {}
                }

            result[short_code]['monthly'][row['water_month']] = {
                'spill_months_count': row['spill_months_count'] or 0,
                'total_months': row['total_months'] or 0,
                'spill_frequency_pct': float(row['spill_frequency_pct']) if row['spill_frequency_pct'] else 0.0,
                'spill_avg_cfs': float(row['spill_avg_cfs']) if row['spill_avg_cfs'] else 0.0,
                'spill_max_cfs': float(row['spill_max_cfs']) if row['spill_max_cfs'] else 0.0,
                'spill_q50': float(row['spill_q50']) if row['spill_q50'] else 0.0,
                'spill_q90': float(row['spill_q90']) if row['spill_q90'] else 0.0,
                'spill_q100': float(row['spill_q100']) if row['spill_q100'] else 0.0,
                'storage_at_spill_avg_pct': float(row['storage_at_spill_avg_pct']) if row['storage_at_spill_avg_pct'] else None,
            }

        return {
            'scenario_id': scenario_id,
            'reservoirs': result
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/scenarios/{scenario_id}/period-summary",
    summary="Get period-of-record summary statistics"
)
async def get_period_summary(
    scenario_id: str,
    reservoirs: Optional[str] = Query(
        None,
        description="Comma-separated reservoir short_codes. Defaults to 8 major reservoirs."
    ),
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get period-of-record summary statistics for reservoirs.

    **Use case:** Storage exceedance curves, spill risk assessment, threshold markers.

    **Example:** `GET /api/statistics/scenarios/s0020/period-summary`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
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
    reservoir_list = await parse_reservoirs(reservoirs, connection)

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
                detail=f"No period summary data found for scenario {scenario_id}"
            )

        result = {}
        for row in rows:
            short_code = row['short_code']
            result[short_code] = {
                'name': row['name'] or short_code,
                'capacity_taf': float(row['capacity_taf']) if row['capacity_taf'] else 0.0,
                'simulation_years': {
                    'start': row['simulation_start_year'],
                    'end': row['simulation_end_year'],
                    'total': row['total_years'],
                },
                'storage_exceedance': {
                    'p5': float(row['storage_exc_p5']) if row['storage_exc_p5'] else 0.0,
                    'p10': float(row['storage_exc_p10']) if row['storage_exc_p10'] else 0.0,
                    'p25': float(row['storage_exc_p25']) if row['storage_exc_p25'] else 0.0,
                    'p50': float(row['storage_exc_p50']) if row['storage_exc_p50'] else 0.0,
                    'p75': float(row['storage_exc_p75']) if row['storage_exc_p75'] else 0.0,
                    'p90': float(row['storage_exc_p90']) if row['storage_exc_p90'] else 0.0,
                    'p95': float(row['storage_exc_p95']) if row['storage_exc_p95'] else 0.0,
                },
                'thresholds': {
                    'dead_pool_taf': float(row['dead_pool_taf']) if row['dead_pool_taf'] else 0.0,
                    'dead_pool_pct': float(row['dead_pool_pct']) if row['dead_pool_pct'] else 0.0,
                    'spill_threshold_pct': float(row['spill_threshold_pct']) if row['spill_threshold_pct'] else None,
                },
                'spill': {
                    'years_count': row['spill_years_count'] or 0,
                    'frequency_pct': float(row['spill_frequency_pct']) if row['spill_frequency_pct'] else 0.0,
                    'mean_cfs': float(row['spill_mean_cfs']) if row['spill_mean_cfs'] else 0.0,
                    'peak_cfs': float(row['spill_peak_cfs']) if row['spill_peak_cfs'] else 0.0,
                    'annual_avg_taf': float(row['annual_spill_avg_taf']) if row['annual_spill_avg_taf'] else 0.0,
                    'annual_cv': float(row['annual_spill_cv']) if row['annual_spill_cv'] else 0.0,
                    'annual_max_taf': float(row['annual_spill_max_taf']) if row['annual_spill_max_taf'] else 0.0,
                    'annual_max_q50': float(row['annual_max_spill_q50']) if row['annual_max_spill_q50'] else 0.0,
                    'annual_max_q90': float(row['annual_max_spill_q90']) if row['annual_max_spill_q90'] else 0.0,
                    'annual_max_q100': float(row['annual_max_spill_q100']) if row['annual_max_spill_q100'] else 0.0,
                },
            }

        return {
            'scenario_id': scenario_id,
            'reservoirs': result
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/reservoirs/all",
    summary="List all reservoirs with statistics data"
)
async def list_all_reservoirs(
    connection: asyncpg.Connection = Depends(get_db)
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
                'reservoir_id': row['short_code'],
                'name': row['name'] or row['short_code'],
                'capacity_taf': float(row['capacity_taf']) if row['capacity_taf'] else 0.0,
            }
            for row in rows
        ]

        major_reservoirs = await get_major_reservoirs(connection)
        return {
            'major': major_reservoirs,
            'all': all_reservoirs,
            'total': len(all_reservoirs)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
