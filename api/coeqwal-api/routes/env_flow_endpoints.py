"""
Environmental River Flows Statistics API Endpoints.

Three metrics for 59 CalSim channel reaches:
  Metric 1.River flows as % of natural unimpaired flow (monthly and seasonal)
  Metric 2.River flows as % of functional flow targets vs EFLOWS (seasonal, ~17 reaches)
  Metric 3.Flow alteration index: Pearson r between simulated and unimpaired flow

Endpoints:
  GET /api/statistics/channels
     .list all 59 channel reaches with watershed attributes
  GET /api/statistics/env-flow-seasons
     .list the 5 CEFF seasonal definitions (static lookup)
  GET /api/statistics/scenarios/{id}/channels/monthly
     .Metric 1 monthly: % unimpaired distribution for all channels
  GET /api/statistics/scenarios/{id}/channels/seasonal
     .Metric 1+2 seasonal: flow volumes, % unimpaired, % functional flow by CEFF season
  GET /api/statistics/scenarios/{id}/channels/period-summary
     .Metric 3: Pearson r flow alteration index + full-period aggregate stats

Performance strategy:
  - Responses are cached in-process with a 30-minute TTL (cachetools.TTLCache).
    The first request for a given (scenario, channel) key populates the cache; all
    subsequent requests in the same 30-minute window are served from memory with
    no DB round-trip. This makes a "room full of people" scenario cheap: DB load
    is proportional to unique keys × 1 per TTL window, not 1 per person.
  - Cache-Control: public, max-age=900 headers allow browsers and any CDN layer to
    cache responses for an additional 15 minutes beyond the in-process TTL.
  - Each fetch helper acquires its own pool connection so asyncio.gather tasks
    in the batch endpoint run in true parallel, not sequentially on one connection.

Water months: 1=October ... 12=September
CFS: cubic feet per second (raw flow storage)
"""

import logging
from typing import Any, Dict, Optional

from cachetools import TTLCache
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from routes._common.null_handling import safe_float, safe_int

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

_db_pool = None

# ---------------------------------------------------------------------------
# In-process response cache
# ---------------------------------------------------------------------------
# Key → serialized response dict.
# TTL: 1800 s (30 min). Env-flow ETL runs run are infrequent; data is stable.
# maxsize: sized to hold all scenarios × channels × endpoints with headroom.
#   19 scenarios × 59 channels × 3 stat endpoints = ~3,363 entries
#   + channel list, seasons = a handful
_stats_cache: TTLCache = TTLCache(maxsize=6000, ttl=1800)
# Static lookups (channel list, seasons).much longer TTL; effectively permanent.
_static_cache: TTLCache = TTLCache(maxsize=10, ttl=86400)  # 24 hours

# HTTP max-age values (seconds) sent on successful GET responses.
_CACHE_MAX_AGE_STATIC = 86400   # 24 h for channels / seasons.static data
_CACHE_MAX_AGE_STATS = 900      # 15 min for statistics.safe refresh interval


def set_db_pool(pool) -> None:
    global _db_pool
    _db_pool = pool


def _json_response(data: Dict[str, Any], max_age: int) -> JSONResponse:
    """Wrap a dict in a JSONResponse with Cache-Control headers."""
    return JSONResponse(
        content=data,
        headers={"Cache-Control": f"public, max-age={max_age}"},
    )


# =============================================================================
# LIST CHANNELS  (static.cached 24 h)
# =============================================================================


@router.get(
    "/channels",
    summary="List env-flow channel reaches",
    description=(
        "Returns all 59 CalSim channel reaches attributed for environmental flow analysis, "
        "with watershed and capability attributes. Results are stable between ETL runs."
    ),
)
async def list_channels(
    channel_class: Optional[str] = Query(
        None,
        description="Filter by class: 'stream', 'canal', or 'reservoir_release'",
    ),
    watershed: Optional[str] = Query(
        None, description="Filter by watershed short_code (e.g. 'SAC_LOWER')"
    ),
    has_mif: Optional[bool] = Query(
        None, description="Filter to reaches with/without a MIF companion variable"
    ),
    has_eflows: Optional[bool] = Query(
        None, description="Filter to reaches with/without functional flow targets"
    ),
):
    """
    List channel reaches used in environmental flow analysis.

    Returns attributes from the `env_flow_channel_full` view:
    network_arc_id, label, channel_class, channel_class_label, watershed_short_code,
    watershed_name, hydrologic_region, unimp_sv_variable, has_mif, has_eflows.
    """
    cache_key = f"channels:{channel_class}:{watershed}:{has_mif}:{has_eflows}"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], _CACHE_MAX_AGE_STATIC)

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                network_arc_id,
                label,
                channel_class,
                channel_class_label,
                watershed_short_code,
                watershed_name,
                hydrologic_region,
                unimp_sv_variable,
                has_mif,
                has_eflows
            FROM env_flow_channel_full
            WHERE is_active = TRUE
        """
        params: list = []

        if channel_class:
            params.append(channel_class.lower())
            query += f" AND channel_class = ${len(params)}"
        if watershed:
            params.append(watershed.upper())
            query += f" AND watershed_short_code = ${len(params)}"
        if has_mif is not None:
            params.append(has_mif)
            query += f" AND has_mif = ${len(params)}"
        if has_eflows is not None:
            params.append(has_eflows)
            query += f" AND has_eflows = ${len(params)}"

        query += " ORDER BY channel_class NULLS LAST, watershed_short_code, network_arc_id"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"channels list query failed: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

    channels = [
        {
            "network_arc_id": row["network_arc_id"],
            "label": row["label"],
            "channel_class": row["channel_class"],
            "channel_class_label": row["channel_class_label"],
            "watershed_short_code": row["watershed_short_code"],
            "watershed_name": row["watershed_name"],
            "hydrologic_region": row["hydrologic_region"],
            "unimp_sv_variable": row["unimp_sv_variable"],
            "has_mif": row["has_mif"],
            "has_eflows": row["has_eflows"],
        }
        for row in rows
    ]

    result = {"channels": channels, "total": len(channels)}
    _static_cache[cache_key] = result
    return _json_response(result, _CACHE_MAX_AGE_STATIC)


# =============================================================================
# LIST CEFF SEASONS  (static.cached 24 h)
# =============================================================================


@router.get(
    "/env-flow-seasons",
    summary="List CEFF seasonal definitions",
    description=(
        "Returns the 5 California Environmental Flows Framework (CEFF) seasons "
        "used to group functional flow statistics. Static lookup.rarely changes."
    ),
)
async def list_seasons():
    """
    CEFF 5-season calendar used for Metric 2 (% functional flows).

    season_id, short_code, name, description, calendar_months, wy_months, sort_order.
    Water months: 1=October … 12=September.
    """
    cache_key = "env_flow_seasons"
    if cache_key in _static_cache:
        return _json_response(_static_cache[cache_key], _CACHE_MAX_AGE_STATIC)

    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT id, short_code, name, description, calendar_months, wy_months, sort_order
                FROM env_flow_season
                WHERE is_active = TRUE
                ORDER BY sort_order
                """
            )
        except Exception as e:
            log.error(f"env_flow_seasons query failed: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

    seasons = [
        {
            "season_id": row["id"],
            "short_code": row["short_code"],
            "name": row["name"],
            "description": row["description"],
            "calendar_months": row["calendar_months"],
            "wy_months": list(row["wy_months"]),
            "sort_order": row["sort_order"],
        }
        for row in rows
    ]

    result = {"seasons": seasons, "total": len(seasons)}
    _static_cache[cache_key] = result
    return _json_response(result, _CACHE_MAX_AGE_STATIC)


# =============================================================================
# MONTHLY STATISTICS  (Metric 1)
# =============================================================================


async def _fetch_channels_monthly(pool, scenario_id: str, channel_id: Optional[str]) -> Dict[str, Any]:
    """
    Fetch monthly flow-volume and % unimpaired stats from env_flow_channel_monthly.
    Each element covers one (network_arc_id × water_month) combination.
    Acquires its own connection.safe to call concurrently via asyncio.gather.

    Columns added by migration 28: flow_avg_taf, flow_q*_cfs, flow_q*_taf,
    flow_exc_p*_cfs, flow_exc_p*_taf.
    """
    cache_key = f"monthly:{scenario_id}:{channel_id or ''}"
    if cache_key in _stats_cache:
        return _stats_cache[cache_key]

    async with pool.acquire() as conn:
        query = """
            SELECT
                network_arc_id,
                water_month,

                -- Raw flow (CFS mean + CV)
                flow_avg_cfs,
                flow_cv,
                -- Raw flow mean TAF/month (migration 28)
                flow_avg_taf,
                -- Raw flow percentile bands.CFS (migration 28)
                flow_q0_cfs,   flow_q10_cfs,  flow_q30_cfs,  flow_q50_cfs,
                flow_q70_cfs,  flow_q90_cfs,  flow_q100_cfs,
                flow_exc_p5_cfs,  flow_exc_p10_cfs, flow_exc_p25_cfs,
                flow_exc_p50_cfs, flow_exc_p75_cfs, flow_exc_p90_cfs, flow_exc_p95_cfs,
                -- Raw flow percentile bands.TAF/month (migration 28)
                flow_q0_taf,   flow_q10_taf,  flow_q30_taf,  flow_q50_taf,
                flow_q70_taf,  flow_q90_taf,  flow_q100_taf,
                flow_exc_p5_taf,  flow_exc_p10_taf, flow_exc_p25_taf,
                flow_exc_p50_taf, flow_exc_p75_taf, flow_exc_p90_taf, flow_exc_p95_taf,

                -- Unimpaired reference + % unimpaired
                unimp_avg_cfs,
                pct_unimpaired_avg,
                pct_unimpaired_cv,
                q0, q10, q30, q50, q70, q90, q100,
                exc_p5, exc_p10, exc_p25, exc_p50, exc_p75, exc_p90, exc_p95,

                sample_count
            FROM env_flow_channel_monthly
            WHERE scenario_short_code = $1
              AND is_active = TRUE
        """
        params: list = [scenario_id]

        if channel_id:
            params.append(channel_id)
            query += f" AND network_arc_id = ${len(params)}"

        query += " ORDER BY network_arc_id, water_month"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"channels monthly query failed for {scenario_id}: {e}")
            raise

    data = [
        {
            "network_arc_id": row["network_arc_id"],
            "water_month": row["water_month"],
            # Flow volume
            "flow_avg_cfs":      safe_float(row["flow_avg_cfs"]),
            "flow_cv":           safe_float(row["flow_cv"]),
            "flow_avg_taf":      safe_float(row["flow_avg_taf"]),
            # CFS percentile bands
            "flow_q0_cfs":       safe_float(row["flow_q0_cfs"]),
            "flow_q10_cfs":      safe_float(row["flow_q10_cfs"]),
            "flow_q30_cfs":      safe_float(row["flow_q30_cfs"]),
            "flow_q50_cfs":      safe_float(row["flow_q50_cfs"]),
            "flow_q70_cfs":      safe_float(row["flow_q70_cfs"]),
            "flow_q90_cfs":      safe_float(row["flow_q90_cfs"]),
            "flow_q100_cfs":     safe_float(row["flow_q100_cfs"]),
            "flow_exc_p5_cfs":   safe_float(row["flow_exc_p5_cfs"]),
            "flow_exc_p10_cfs":  safe_float(row["flow_exc_p10_cfs"]),
            "flow_exc_p25_cfs":  safe_float(row["flow_exc_p25_cfs"]),
            "flow_exc_p50_cfs":  safe_float(row["flow_exc_p50_cfs"]),
            "flow_exc_p75_cfs":  safe_float(row["flow_exc_p75_cfs"]),
            "flow_exc_p90_cfs":  safe_float(row["flow_exc_p90_cfs"]),
            "flow_exc_p95_cfs":  safe_float(row["flow_exc_p95_cfs"]),
            # TAF percentile bands
            "flow_q0_taf":       safe_float(row["flow_q0_taf"]),
            "flow_q10_taf":      safe_float(row["flow_q10_taf"]),
            "flow_q30_taf":      safe_float(row["flow_q30_taf"]),
            "flow_q50_taf":      safe_float(row["flow_q50_taf"]),
            "flow_q70_taf":      safe_float(row["flow_q70_taf"]),
            "flow_q90_taf":      safe_float(row["flow_q90_taf"]),
            "flow_q100_taf":     safe_float(row["flow_q100_taf"]),
            "flow_exc_p5_taf":   safe_float(row["flow_exc_p5_taf"]),
            "flow_exc_p10_taf":  safe_float(row["flow_exc_p10_taf"]),
            "flow_exc_p25_taf":  safe_float(row["flow_exc_p25_taf"]),
            "flow_exc_p50_taf":  safe_float(row["flow_exc_p50_taf"]),
            "flow_exc_p75_taf":  safe_float(row["flow_exc_p75_taf"]),
            "flow_exc_p90_taf":  safe_float(row["flow_exc_p90_taf"]),
            "flow_exc_p95_taf":  safe_float(row["flow_exc_p95_taf"]),
            # % unimpaired
            "unimp_avg_cfs":     safe_float(row["unimp_avg_cfs"]),
            "pct_unimpaired_avg": safe_float(row["pct_unimpaired_avg"]),
            "pct_unimpaired_cv": safe_float(row["pct_unimpaired_cv"]),
            "q0":   safe_float(row["q0"]),
            "q10":  safe_float(row["q10"]),
            "q30":  safe_float(row["q30"]),
            "q50":  safe_float(row["q50"]),
            "q70":  safe_float(row["q70"]),
            "q90":  safe_float(row["q90"]),
            "q100": safe_float(row["q100"]),
            "exc_p5":  safe_float(row["exc_p5"]),
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

    result = {"scenario_id": scenario_id, "data": data, "count": len(data)}
    _stats_cache[cache_key] = result
    return result


@router.get(
    "/scenarios/{scenario_id}/channels/monthly",
    summary="Monthly % unimpaired flow statistics (Metric 1)",
    description=(
        "Returns monthly distribution of river flow as a percentage of natural unimpaired flow "
        "for all 59 channel reaches (or a single reach) in a scenario. "
        "One row per (network_arc_id × water_month). "
        "pct_unimpaired = C_{reach} / UNIMP_{watershed} × 100. "
        "NULL where no unimpaired reference exists (Mokelumne, some canals). "
        "Values in CFS (flow) and % (ratios)."
    ),
)
async def get_channels_monthly(
    scenario_id: str,
    channel_id: Optional[str] = Query(
        None,
        description="Filter to a single channel (e.g. 'C_SAC049'). Omit to return all.",
    ),
):
    """
    Metric 1.Monthly % of natural unimpaired flow.

    Response: flat array of {network_arc_id, water_month, flow_avg_cfs, unimp_avg_cfs,
    pct_unimpaired_avg, pct_unimpaired_cv, q0–q100, exc_p5–exc_p95, sample_count}.
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        result = await _fetch_channels_monthly(_db_pool, scenario_id, channel_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Database query failed")

    if not result["data"]:
        raise HTTPException(
            status_code=404,
            detail=f"No env-flow monthly statistics found for scenario '{scenario_id}'",
        )

    return _json_response(result, _CACHE_MAX_AGE_STATS)


# =============================================================================
# SEASONAL STATISTICS  (Metric 1 seasonal + Metric 2)
# =============================================================================


async def _fetch_channels_seasonal(pool, scenario_id: str, channel_id: Optional[str]) -> Dict[str, Any]:
    """
    Fetch seasonal flow and % unimpaired / % functional flow stats.
    Acquires its own connection.safe to call concurrently via asyncio.gather.
    """
    cache_key = f"seasonal:{scenario_id}:{channel_id or ''}"
    if cache_key in _stats_cache:
        return _stats_cache[cache_key]

    async with pool.acquire() as conn:
        query = """
            SELECT
                s.network_arc_id,
                s.season_id,
                fs.short_code     AS season_short_code,
                fs.name           AS season_name,
                fs.sort_order     AS season_sort_order,

                -- Raw flow volume (CFS).all 59 channels
                s.flow_avg_cfs,
                s.flow_cv,
                s.flow_q0,  s.flow_q10, s.flow_q30, s.flow_q50,
                s.flow_q70, s.flow_q90, s.flow_q100,
                s.flow_exc_p5,  s.flow_exc_p10, s.flow_exc_p25,
                s.flow_exc_p50, s.flow_exc_p75, s.flow_exc_p90, s.flow_exc_p95,

                -- Natural flow reference + % unimpaired (Metric 1 seasonal).57 channels
                s.unimp_avg_cfs,
                s.pct_unimpaired_avg,
                s.pct_unimpaired_cv,
                s.unimp_q0,  s.unimp_q10, s.unimp_q30, s.unimp_q50,
                s.unimp_q70, s.unimp_q90, s.unimp_q100,
                s.unimp_exc_p5,  s.unimp_exc_p10, s.unimp_exc_p25,
                s.unimp_exc_p50, s.unimp_exc_p75, s.unimp_exc_p90, s.unimp_exc_p95,

                -- % Functional flows (Metric 2).~17 EFLOWS channels (NULL otherwise)
                s.pct_ff_avg,
                s.pct_ff_cv,
                s.deviation_avg,
                s.target_met_pct,
                s.q0,  s.q10,  s.q30,  s.q50,  s.q70,  s.q90,  s.q100,
                s.exc_p5,  s.exc_p10, s.exc_p25,
                s.exc_p50, s.exc_p75, s.exc_p90, s.exc_p95,

                s.sample_count
            FROM env_flow_channel_seasonal s
            JOIN env_flow_season fs ON fs.id = s.season_id
            WHERE s.scenario_short_code = $1
              AND s.is_active = TRUE
        """
        params: list = [scenario_id]

        if channel_id:
            params.append(channel_id)
            query += f" AND s.network_arc_id = ${len(params)}"

        query += " ORDER BY s.network_arc_id, fs.sort_order"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"channels seasonal query failed for {scenario_id}: {e}")
            raise

    data = [
        {
            "network_arc_id": row["network_arc_id"],
            "season_id": row["season_id"],
            "season_short_code": row["season_short_code"],
            "season_name": row["season_name"],
            "season_sort_order": row["season_sort_order"],
            # Raw flow
            "flow_avg_cfs": safe_float(row["flow_avg_cfs"]),
            "flow_cv": safe_float(row["flow_cv"]),
            "flow_q0": safe_float(row["flow_q0"]),
            "flow_q10": safe_float(row["flow_q10"]),
            "flow_q30": safe_float(row["flow_q30"]),
            "flow_q50": safe_float(row["flow_q50"]),
            "flow_q70": safe_float(row["flow_q70"]),
            "flow_q90": safe_float(row["flow_q90"]),
            "flow_q100": safe_float(row["flow_q100"]),
            "flow_exc_p5": safe_float(row["flow_exc_p5"]),
            "flow_exc_p10": safe_float(row["flow_exc_p10"]),
            "flow_exc_p25": safe_float(row["flow_exc_p25"]),
            "flow_exc_p50": safe_float(row["flow_exc_p50"]),
            "flow_exc_p75": safe_float(row["flow_exc_p75"]),
            "flow_exc_p90": safe_float(row["flow_exc_p90"]),
            "flow_exc_p95": safe_float(row["flow_exc_p95"]),
            # % unimpaired (Metric 1 seasonal)
            "unimp_avg_cfs": safe_float(row["unimp_avg_cfs"]),
            "pct_unimpaired_avg": safe_float(row["pct_unimpaired_avg"]),
            "pct_unimpaired_cv": safe_float(row["pct_unimpaired_cv"]),
            "unimp_q0": safe_float(row["unimp_q0"]),
            "unimp_q10": safe_float(row["unimp_q10"]),
            "unimp_q30": safe_float(row["unimp_q30"]),
            "unimp_q50": safe_float(row["unimp_q50"]),
            "unimp_q70": safe_float(row["unimp_q70"]),
            "unimp_q90": safe_float(row["unimp_q90"]),
            "unimp_q100": safe_float(row["unimp_q100"]),
            "unimp_exc_p5": safe_float(row["unimp_exc_p5"]),
            "unimp_exc_p10": safe_float(row["unimp_exc_p10"]),
            "unimp_exc_p25": safe_float(row["unimp_exc_p25"]),
            "unimp_exc_p50": safe_float(row["unimp_exc_p50"]),
            "unimp_exc_p75": safe_float(row["unimp_exc_p75"]),
            "unimp_exc_p90": safe_float(row["unimp_exc_p90"]),
            "unimp_exc_p95": safe_float(row["unimp_exc_p95"]),
            # % functional flows (Metric 2.NULL if no EFLOWS target)
            "pct_ff_avg": safe_float(row["pct_ff_avg"]),
            "pct_ff_cv": safe_float(row["pct_ff_cv"]),
            "deviation_avg": safe_float(row["deviation_avg"]),
            "target_met_pct": safe_float(row["target_met_pct"]),
            "ff_q0": safe_float(row["q0"]),
            "ff_q10": safe_float(row["q10"]),
            "ff_q30": safe_float(row["q30"]),
            "ff_q50": safe_float(row["q50"]),
            "ff_q70": safe_float(row["q70"]),
            "ff_q90": safe_float(row["q90"]),
            "ff_q100": safe_float(row["q100"]),
            "ff_exc_p5": safe_float(row["exc_p5"]),
            "ff_exc_p10": safe_float(row["exc_p10"]),
            "ff_exc_p25": safe_float(row["exc_p25"]),
            "ff_exc_p50": safe_float(row["exc_p50"]),
            "ff_exc_p75": safe_float(row["exc_p75"]),
            "ff_exc_p90": safe_float(row["exc_p90"]),
            "ff_exc_p95": safe_float(row["exc_p95"]),
            "sample_count": safe_int(row["sample_count"]),
        }
        for row in rows
    ]

    result = {"scenario_id": scenario_id, "data": data, "count": len(data)}
    _stats_cache[cache_key] = result
    return result


@router.get(
    "/scenarios/{scenario_id}/channels/seasonal",
    summary="Seasonal flow and % unimpaired / % functional flow statistics (Metrics 1+2)",
    description=(
        "Returns per-CEFF-season distributions of raw flow volume (CFS), % unimpaired flow, "
        "and % functional flow target for all channel reaches in a scenario. "
        "One row per (network_arc_id × season). "
        "flow_* columns: all 59 channels. "
        "unimp_*/pct_unimpaired_* columns: 57 channels (NULL for Mokelumne and canals without UNIMP). "
        "pct_ff_*/ff_* columns: ~17 channels with EFLOWS targets (NULL otherwise)."
    ),
)
async def get_channels_seasonal(
    scenario_id: str,
    channel_id: Optional[str] = Query(
        None,
        description="Filter to a single channel (e.g. 'C_FTR003'). Omit to return all.",
    ),
):
    """
    Metrics 1+2.Seasonal flow volume, % unimpaired, and % functional flow.

    season_short_code maps to the CEFF calendar: wet_peak, wet_base,
    spring_recession, dry, fall_pulse. See /api/statistics/env-flow-seasons
    for full season definitions.
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        result = await _fetch_channels_seasonal(_db_pool, scenario_id, channel_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Database query failed")

    if not result["data"]:
        raise HTTPException(
            status_code=404,
            detail=f"No env-flow seasonal statistics found for scenario '{scenario_id}'",
        )

    return _json_response(result, _CACHE_MAX_AGE_STATS)


# =============================================================================
# PERIOD SUMMARY  (Metric 3 + full-period averages)
# =============================================================================


async def _fetch_channels_period_summary(pool, scenario_id: str, channel_id: Optional[str]) -> Dict[str, Any]:
    """
    Fetch period-of-record statistics from env_flow_channel_period_summary.
    Acquires its own connection.safe to call concurrently via asyncio.gather.
    """
    cache_key = f"period:{scenario_id}:{channel_id or ''}"
    if cache_key in _stats_cache:
        return _stats_cache[cache_key]

    async with pool.acquire() as conn:
        query = """
            SELECT
                ps.network_arc_id,
                ps.simulation_start_year,
                ps.simulation_end_year,
                ps.total_months,

                -- Metric 3.flow alteration index
                ps.pearson_r,
                ps.p_value,

                -- Full-period aggregate stats
                ps.avg_pct_unimpaired,
                ps.annual_cv_pct_unimpaired,
                ps.avg_pct_ff,
                ps.annual_cv_pct_ff,
                ps.mif_met_pct,

                -- Capability flags (denormed from channel_entity for convenience)
                ce.has_mif,
                ce.has_eflows,
                ce.unimp_sv_variable
            FROM env_flow_channel_period_summary ps
            JOIN channel_entity ce ON ce.network_arc_id = ps.network_arc_id
            WHERE ps.scenario_short_code = $1
              AND ps.is_active = TRUE
        """
        params: list = [scenario_id]

        if channel_id:
            params.append(channel_id)
            query += f" AND ps.network_arc_id = ${len(params)}"

        query += " ORDER BY ps.network_arc_id"

        try:
            rows = await conn.fetch(query, *params)
        except Exception as e:
            log.error(f"channels period-summary query failed for {scenario_id}: {e}")
            raise

    data = [
        {
            "network_arc_id": row["network_arc_id"],
            "simulation_start_year": safe_int(row["simulation_start_year"]),
            "simulation_end_year": safe_int(row["simulation_end_year"]),
            "total_months": safe_int(row["total_months"]),
            # Metric 3
            "pearson_r": safe_float(row["pearson_r"]),
            "p_value": safe_float(row["p_value"]),
            # Full-period aggregates
            "avg_pct_unimpaired": safe_float(row["avg_pct_unimpaired"]),
            "annual_cv_pct_unimpaired": safe_float(row["annual_cv_pct_unimpaired"]),
            "avg_pct_ff": safe_float(row["avg_pct_ff"]),
            "annual_cv_pct_ff": safe_float(row["annual_cv_pct_ff"]),
            "mif_met_pct": safe_float(row["mif_met_pct"]),
            # Capability flags
            "has_mif": row["has_mif"],
            "has_eflows": row["has_eflows"],
            "unimp_sv_variable": row["unimp_sv_variable"],
        }
        for row in rows
    ]

    result = {"scenario_id": scenario_id, "data": data, "count": len(data)}
    _stats_cache[cache_key] = result
    return result


@router.get(
    "/scenarios/{scenario_id}/channels/period-summary",
    summary="Period-of-record flow alteration index and aggregate stats (Metric 3)",
    description=(
        "Returns the period-of-record flow alteration index (Pearson r between simulated and "
        "unimpaired monthly flow) and full-period averages of Metrics 1 and 2 for all channels. "
        "One row per network_arc_id. "
        "pearson_r ≈ +1: natural seasonal timing preserved. r ≈ 0: heavily altered. "
        "NULL where no unimpaired reference exists (Mokelumne, some canals)."
    ),
)
async def get_channels_period_summary(
    scenario_id: str,
    channel_id: Optional[str] = Query(
        None,
        description="Filter to a single channel. Omit to return all 59.",
    ),
):
    """
    Metric 3.Flow alteration index (Pearson r) + full-period aggregates.

    pearson_r: Pearson correlation between monthly C_{reach} and UNIMP_{watershed}
    across the full 1,200-month period of record (WY 1922–2021).
    p_value: two-tailed p-value for the correlation (sample size = total_months).
    mif_met_pct: fraction of months where simulated flow >= binding MIF threshold
    (NULL for reaches where has_mif = false).
    """
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        result = await _fetch_channels_period_summary(_db_pool, scenario_id, channel_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Database query failed")

    if not result["data"]:
        raise HTTPException(
            status_code=404,
            detail=f"No env-flow period summary found for scenario '{scenario_id}'",
        )

    return _json_response(result, _CACHE_MAX_AGE_STATS)
