"""
Reservoir Statistics API endpoints for COEQWAL.

Provides monthly percentile data for reservoir storage, enabling
percentile band charts in the frontend.

Reservoirs:
- S_SHSTA (Shasta), S_TRNTY (Trinity), S_OROVL (Oroville)
- S_FOLSM (Folsom), S_MELON (New Melones), S_MLRTN (Millerton)
- S_SLUIS_CVP (San Luis CVP), S_SLUIS_SWP (San Luis SWP)

Water months: Oct=1, Nov=2, ..., Sep=12
Values: Percent of reservoir capacity (0-100+)
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
import asyncpg
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

# =============================================================================
# CONSTANTS
# =============================================================================

RESERVOIR_NAMES = {
    'S_SHSTA': 'Shasta',
    'S_TRNTY': 'Trinity',
    'S_OROVL': 'Oroville',
    'S_FOLSM': 'Folsom',
    'S_MELON': 'New Melones',
    'S_MLRTN': 'Millerton',
    'S_SLUIS_CVP': 'San Luis (CVP)',
    'S_SLUIS_SWP': 'San Luis (SWP)',
}

WATER_MONTH_NAMES = {
    1: 'October', 2: 'November', 3: 'December',
    4: 'January', 5: 'February', 6: 'March',
    7: 'April', 8: 'May', 9: 'June',
    10: 'July', 11: 'August', 12: 'September',
}


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class MonthlyPercentiles(BaseModel):
    """Percentile data for a single water month"""
    q10: float = Field(..., description="10th percentile (% of capacity)")
    q20: float = Field(..., description="20th percentile")
    q30: float = Field(..., description="30th percentile")
    q40: float = Field(..., description="40th percentile")
    q50: float = Field(..., description="50th percentile (median)")
    q60: float = Field(..., description="60th percentile")
    q70: float = Field(..., description="70th percentile")
    q80: float = Field(..., description="80th percentile")
    q90: float = Field(..., description="90th percentile")
    min: float = Field(..., description="Minimum value")
    max: float = Field(..., description="Maximum value")
    mean: float = Field(..., description="Mean value")


class ReservoirPercentileResponse(BaseModel):
    """Response for single reservoir percentile data"""
    reservoir_id: str = Field(..., description="Reservoir code (e.g., 'S_SHSTA')")
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

    **Example:** `GET /api/statistics/scenarios/s0020/reservoirs/S_SHSTA/percentiles`

    **Response:**
    ```json
    {
      "reservoir_id": "S_SHSTA",
      "reservoir_name": "Shasta",
      "scenario_id": "s0020",
      "unit": "percent_capacity",
      "max_capacity_taf": 4552.0,
      "monthly_percentiles": {
        "1": {"q10": 45.2, "q20": 52.1, ..., "mean": 65.3},
        "2": {"q10": 48.1, ...},
        ...
        "12": {"q10": 42.8, ...}
      }
    }
    ```

    **Water months:** 1=October, 2=November, ..., 12=September

    **Band chart rendering:**
    - Outer band: q10 to q90 (lightest color)
    - Inner bands: q20-q80, q30-q70, q40-q60 (progressively darker)
    - Center line: q50 (median)
    """
    if reservoir_id not in RESERVOIR_NAMES:
        valid_ids = ', '.join(sorted(RESERVOIR_NAMES.keys()))
        raise HTTPException(
            status_code=400,
            detail=f"Invalid reservoir_id: {reservoir_id}. Valid options: {valid_ids}"
        )

    try:
        query = """
        SELECT
            water_month, q10, q20, q30, q40, q50, q60, q70, q80, q90,
            min_value, max_value, mean_value, max_capacity_taf
        FROM reservoir_monthly_percentile
        WHERE scenario_short_code = $1 AND reservoir_code = $2
        ORDER BY water_month
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
                'q10': float(row['q10']) if row['q10'] else 0.0,
                'q20': float(row['q20']) if row['q20'] else 0.0,
                'q30': float(row['q30']) if row['q30'] else 0.0,
                'q40': float(row['q40']) if row['q40'] else 0.0,
                'q50': float(row['q50']) if row['q50'] else 0.0,
                'q60': float(row['q60']) if row['q60'] else 0.0,
                'q70': float(row['q70']) if row['q70'] else 0.0,
                'q80': float(row['q80']) if row['q80'] else 0.0,
                'q90': float(row['q90']) if row['q90'] else 0.0,
                'min': float(row['min_value']) if row['min_value'] else 0.0,
                'max': float(row['max_value']) if row['max_value'] else 0.0,
                'mean': float(row['mean_value']) if row['mean_value'] else 0.0,
            }

        return {
            'reservoir_id': reservoir_id,
            'reservoir_name': RESERVOIR_NAMES[reservoir_id],
            'scenario_id': scenario_id,
            'unit': 'percent_capacity',
            'max_capacity_taf': float(rows[0]['max_capacity_taf']),
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
    Get percentile data for all 8 reservoirs in a single request.

    **Use case:** Load all reservoir data at once for comparison views
    or dashboard initialization.

    **Example:** `GET /api/statistics/scenarios/s0020/reservoir-percentiles`

    **Response:**
    ```json
    {
      "scenario_id": "s0020",
      "reservoirs": {
        "S_SHSTA": {
          "name": "Shasta",
          "max_capacity_taf": 4552.0,
          "monthly_percentiles": {
            "1": {"q10": 45.2, "q20": 52.1, ..., "mean": 65.3},
            ...
          }
        },
        "S_OROVL": { ... },
        ...
      }
    }
    ```
    """
    try:
        query = """
        SELECT
            reservoir_code, water_month,
            q10, q20, q30, q40, q50, q60, q70, q80, q90,
            min_value, max_value, mean_value, max_capacity_taf
        FROM reservoir_monthly_percentile
        WHERE scenario_short_code = $1
        ORDER BY reservoir_code, water_month
        """
        rows = await connection.fetch(query, scenario_id)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No percentile data found for scenario {scenario_id}"
            )

        reservoirs = {}
        for row in rows:
            res_code = row['reservoir_code']

            if res_code not in reservoirs:
                reservoirs[res_code] = {
                    'name': RESERVOIR_NAMES.get(res_code, res_code),
                    'max_capacity_taf': float(row['max_capacity_taf']),
                    'monthly_percentiles': {}
                }

            reservoirs[res_code]['monthly_percentiles'][row['water_month']] = {
                'q10': float(row['q10']) if row['q10'] else 0.0,
                'q20': float(row['q20']) if row['q20'] else 0.0,
                'q30': float(row['q30']) if row['q30'] else 0.0,
                'q40': float(row['q40']) if row['q40'] else 0.0,
                'q50': float(row['q50']) if row['q50'] else 0.0,
                'q60': float(row['q60']) if row['q60'] else 0.0,
                'q70': float(row['q70']) if row['q70'] else 0.0,
                'q80': float(row['q80']) if row['q80'] else 0.0,
                'q90': float(row['q90']) if row['q90'] else 0.0,
                'min': float(row['min_value']) if row['min_value'] else 0.0,
                'max': float(row['max_value']) if row['max_value'] else 0.0,
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
async def list_reservoirs() -> Dict[str, Any]:
    """
    Get list of reservoirs with percentile data available.

    **Use case:** Populate reservoir selector dropdowns in the UI.

    **Response:**
    ```json
    {
      "reservoirs": [
        {"reservoir_id": "S_SHSTA", "reservoir_name": "Shasta"},
        {"reservoir_id": "S_TRNTY", "reservoir_name": "Trinity"},
        ...
      ]
    }
    ```
    """
    return {
        'reservoirs': [
            {'reservoir_id': k, 'reservoir_name': v}
            for k, v in sorted(RESERVOIR_NAMES.items(), key=lambda x: x[1])
        ]
    }


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
          "reservoirs": ["S_FOLSM", "S_MELON", "S_MLRTN", ...],
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
            scenario_short_code,
            array_agg(DISTINCT reservoir_code ORDER BY reservoir_code) as reservoirs,
            COUNT(DISTINCT reservoir_code) as reservoir_count
        FROM reservoir_monthly_percentile
        GROUP BY scenario_short_code
        ORDER BY scenario_short_code
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
