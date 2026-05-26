"""
reservoir_statistics_endpoints.py

Provides monthly percentile data for reservoir storage, monthly spill
statistics, and the canonical reservoir / reservoir-group directories.

Terminology:
- Reservoir entities: SHSTA, TRNTY, OROVL, etc. (`short_code` in
  `reservoir_entity` table)
- Statistics tables link to entities via `reservoir_entity_id` FK

Major reservoirs (8 total, fetched from `reservoir_group` 'major'):
- SHSTA (Shasta), TRNTY (Trinity), OROVL (Oroville), FOLSM (Folsom)
- MELON (New Melones), MLRTN (Millerton), SLUIS_CVP, SLUIS_SWP (San Luis)

The API accepts entity short_codes only (SHSTA), not CalSim variable codes
(`S_SHSTA`).

Water months: Oct=1, Nov=2, ..., Sep=12
Values: percent of reservoir capacity (0-100+) or absolute TAF.

Caching: shared in-process TTL helper (default 5 min, env-driven via
API_CACHE_TTL_SECONDS). All responses set a matching Cache-Control max-age.
"""

import logging
from typing import Any, Dict, List, Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from routes._common import (
    api_cache_max_age,
    make_ttl_cache,
    safe_float,
    safe_int,
    safe_str,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

# =============================================================================
# CONSTANTS
# =============================================================================

# Fallback entity short_codes if the database lookup of the major group fails
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

# Valid reservoir group codes
VALID_RESERVOIR_GROUPS = ["major", "cvp", "swp"]


# =============================================================================
# DATABASE CONNECTION + CACHE
# =============================================================================

db_pool = None

# Per-scenario stats: 92 reservoirs * 19 scenarios * 2 routes -> ~3,500 entries
_stats_cache = make_ttl_cache("reservoir_stats", maxsize=5000)
_static_cache = make_ttl_cache("reservoir_static", maxsize=20)


def set_db_pool(pool):
    global db_pool
    db_pool = pool


async def get_db():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection


def _json_response(data: Dict[str, Any], max_age: int) -> JSONResponse:
    """Wrap a dict in a JSONResponse with Cache-Control headers."""
    return JSONResponse(
        content=data,
        headers={"Cache-Control": f"public, max-age={max_age}"},
    )


# =============================================================================
# RESERVOIR GROUP HELPERS
# =============================================================================


async def get_major_reservoirs(connection: asyncpg.Connection) -> List[str]:
    """Fetch major reservoir short_codes from `reservoir_group` membership.

    Falls back to a hardcoded list of the 8 known majors if the lookup fails.
    Returns entity short_codes (e.g., SHSTA), not CalSim variable codes
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
    except Exception as e:
        log.warning(f"major-reservoirs query failed; using fallback: {e}")
    return MAJOR_RESERVOIRS_FALLBACK


async def get_reservoirs_by_group(
    connection: asyncpg.Connection, group_code: str
) -> List[str]:
    """Fetch reservoir short_codes for a given group ('major', 'cvp', 'swp')."""
    if group_code not in VALID_RESERVOIR_GROUPS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid group '{group_code}'. "
                f"Valid groups: {', '.join(VALID_RESERVOIR_GROUPS)}"
            ),
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
            status_code=404,
            detail=f"No reservoirs found for group '{group_code}'",
        )

    return [row["short_code"] for row in rows]


async def parse_reservoirs(
    reservoirs: Optional[str], group: Optional[str], connection: asyncpg.Connection
) -> List[str]:
    """Resolve a reservoir filter from either an explicit list or a group code.

    The two parameters are mutually exclusive. When neither is provided,
    defaults to the major reservoir group
    """
    if reservoirs and group:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot specify both 'reservoirs' and 'group' parameters. "
                "Use one or the other."
            ),
        )

    if group:
        return await get_reservoirs_by_group(connection, group)

    if reservoirs:
        codes = [r.strip() for r in reservoirs.split(",") if r.strip()]
        for code in codes:
            if code.startswith("S_") or code.startswith("C_"):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Use entity short_code (e.g., SHSTA), "
                        f"not variable code ({code})"
                    ),
                )
        return codes

    return await get_major_reservoirs(connection)


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/reservoir-percentiles",
    summary="Monthly storage percentiles for reservoirs",
    description=(
        "Per-reservoir monthly storage percentile bands (% of capacity). "
        "Defaults to the 8 major reservoirs when neither `reservoirs` nor "
        "`group` is supplied."
    ),
)
async def get_all_reservoir_percentiles(
    scenario_id: str,
    reservoirs: Optional[str] = Query(
        None,
        description=(
            "Comma-separated reservoir short_codes (e.g., 'SHSTA,OROVL'). "
            "Defaults to 8 major reservoirs."
        ),
    ),
    group: Optional[str] = Query(
        None,
        description=(
            "Reservoir group filter: 'major', 'cvp', or 'swp'. "
            "Cannot be combined with `reservoirs`."
        ),
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> JSONResponse:
    """Get monthly storage percentile bands for reservoirs."""
    cache_key = f"percentiles:{scenario_id}:{reservoirs or ''}:{group or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

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
    except Exception as e:
        log.error(f"reservoir-percentiles query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No percentile data found for scenario '{scenario_id}'",
        )

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        short_code = row["short_code"]
        if short_code not in out:
            label = safe_str(row["name"]) or short_code
            out[short_code] = {
                "label": label,
                "name": label,
                "capacity_taf": safe_float(row["capacity_taf"]),
                "dead_pool_taf": safe_float(row["dead_pool_taf"]),
                "monthly_percentiles": {},
            }

        out[short_code]["monthly_percentiles"][row["water_month"]] = {
            "q0": safe_float(row["q0"]),
            "q10": safe_float(row["q10"]),
            "q30": safe_float(row["q30"]),
            "q50": safe_float(row["q50"]),
            "q70": safe_float(row["q70"]),
            "q90": safe_float(row["q90"]),
            "q100": safe_float(row["q100"]),
            "mean": safe_float(row["mean_value"]),
        }

    response: Dict[str, Any] = {
        "scenario_id": scenario_id,
        "reservoirs": out,
        "count": len(out),
    }
    if group:
        response["group"] = group

    _stats_cache[cache_key] = response
    return _json_response(response, api_cache_max_age())


@router.get(
    "/reservoir-groups",
    summary="List reservoir groups and their members",
    description="Returns the 'major', 'cvp', and 'swp' reservoir groups with their member entity short_codes.",
)
async def list_reservoir_groups(
    connection: asyncpg.Connection = Depends(get_db),
) -> JSONResponse:
    """List reservoir groups with their member reservoirs."""
    cache_key = "reservoir_groups"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], api_cache_max_age())

    try:
        query = """
        SELECT
            rg.short_code, rg.label,
            array_agg(re.short_code ORDER BY re.short_code) as reservoirs
        FROM reservoir_group rg
        JOIN reservoir_group_member rgm ON rg.id = rgm.reservoir_group_id
        JOIN reservoir_entity re ON re.id = rgm.reservoir_entity_id
        WHERE rg.short_code IN ('major', 'cvp', 'swp')
        GROUP BY rg.short_code, rg.label
        ORDER BY rg.short_code
        """
        rows = await connection.fetch(query)
    except Exception as e:
        log.error(f"reservoir-groups query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    groups = [
        {
            "group_id": safe_str(row["short_code"]),
            "label": safe_str(row["label"]) or safe_str(row["short_code"]),
            "name": safe_str(row["label"]) or safe_str(row["short_code"]),
            "reservoirs": list(row["reservoirs"]),
        }
        for row in rows
    ]
    result = {"groups": groups, "count": len(groups)}
    _static_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())


@router.get(
    "/scenarios/{scenario_id}/spill-monthly",
    summary="Monthly spill statistics for reservoirs",
    description=(
        "Per-reservoir monthly spill frequency, average / max CFS, and storage "
        "at spill. Defaults to the 8 major reservoirs."
    ),
)
async def get_spill_monthly(
    scenario_id: str,
    reservoirs: Optional[str] = Query(
        None,
        description="Comma-separated reservoir short_codes. Defaults to 8 major reservoirs.",
    ),
    group: Optional[str] = Query(
        None,
        description=(
            "Reservoir group filter: 'major', 'cvp', or 'swp'. "
            "Cannot be combined with `reservoirs`."
        ),
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> JSONResponse:
    """Get monthly spill (flood release) statistics for reservoirs."""
    cache_key = f"spill:{scenario_id}:{reservoirs or ''}:{group or ''}"
    if cache_key in _stats_cache:
        return _json_response(_stats_cache[cache_key], api_cache_max_age())

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
    except Exception as e:
        log.error(f"spill-monthly query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No spill data found for scenario '{scenario_id}'",
        )

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        short_code = row["short_code"]
        if short_code not in out:
            label = safe_str(row["name"]) or short_code
            out[short_code] = {"label": label, "name": label, "monthly": {}}
        out[short_code]["monthly"][row["water_month"]] = {
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

    response: Dict[str, Any] = {
        "scenario_id": scenario_id,
        "reservoirs": out,
        "count": len(out),
    }
    if group:
        response["group"] = group

    _stats_cache[cache_key] = response
    return _json_response(response, api_cache_max_age())


@router.get(
    "/reservoirs",
    summary="List reservoirs with statistics data",
    description=(
        "Returns every reservoir entity that has at least one period-summary "
        "row (i.e. is available to the statistics endpoints), plus the major "
        "reservoir group for convenience."
    ),
)
async def list_reservoirs(
    connection: asyncpg.Connection = Depends(get_db),
) -> JSONResponse:
    """List reservoirs with statistics data."""
    cache_key = "reservoirs:list"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], api_cache_max_age())

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
        major_reservoirs = await get_major_reservoirs(connection)
    except Exception as e:
        log.error(f"reservoirs list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    all_reservoirs = [
        {
            "reservoir_id": safe_str(row["short_code"]),
            "label": safe_str(row["name"]) or safe_str(row["short_code"]),
            "name": safe_str(row["name"]) or safe_str(row["short_code"]),
            "capacity_taf": safe_float(row["capacity_taf"]),
        }
        for row in rows
    ]

    result = {
        "major": major_reservoirs,
        "all": all_reservoirs,
        "count": len(all_reservoirs),
    }
    _static_cache[cache_key] = result
    return _json_response(result, api_cache_max_age())
