"""data_in_depth_endpoints.py - API over the generic data_in_depth_* tables.

Hybrid compute: SQL filters/joins/fetches the raw rows; Python computes every
derived value live (see routes/data_in_depth_stats.py) so results stay correct
under WYT filtering. Retrieval is scenario-based but supports MULTIPLE scenarios
per request; the compute is still per single scenario (no cross-scenario pooling).

Endpoint:
  GET /api/data-in-depth/reservoir-storage
    ?scenarios=s0011,s0020        (required, CSV)
    &subjects=SHSTA,NOD_Reservoirs   (optional; default all reservoir + NOD_Reservoirs/SOD_Reservoirs)
    &periods=april,sept           (optional; default april,sept)
    &units=volume,pct_capacity    (optional; default both -> TAF, PCT_CAP)
    &include=values,exceedance,box,statistics   (optional; default all — combinable)
    &wyt=1,2                      (optional; default all -> no join, no recompute needed)
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from routes._common import api_cache_max_age, make_ttl_cache
from routes.data_in_depth_stats import INCLUDE_ALL, compute_reservoir_storage, compute_series

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/data-in-depth", tags=["data-in-depth"])

# -- request vocab ----------------------------------------------------------
UNIT_ALIASES = {"volume": "TAF", "taf": "TAF", "pct_capacity": "PCT_CAP", "pct_cap": "PCT_CAP"}
VALID_UNITS = {"TAF", "PCT_CAP"}
DEFAULT_PERIODS = ["april", "sept"]
VALID_PERIODS = {"april", "sept", "annual"}
VALID_INCLUDE = set(INCLUDE_ALL)
VALID_WYT = {1, 2, 3, 4, 5}

# -- db + cache -------------------------------------------------------------
db_pool = None
_cache = make_ttl_cache("data_in_depth", maxsize=5000)


def set_db_pool(pool):
    global db_pool
    db_pool = pool


async def get_db():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection


def _json_response(data: Dict[str, Any], max_age: int) -> JSONResponse:
    return JSONResponse(content=data, headers={"Cache-Control": f"public, max-age={max_age}"})


def _csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [p.strip() for p in value.split(",") if p.strip()]


@router.get("/reservoir-storage", summary="April/September reservoir storage (raw + live-computed stats)")
async def reservoir_storage(
    scenarios: str = Query(..., description="CSV of scenario short_codes (one or many)"),
    subjects: Optional[str] = Query(None, description="CSV of subject short_codes (SHSTA, NOD_Reservoirs, ...)"),
    periods: Optional[str] = Query(None, description="CSV; default april,sept"),
    units: Optional[str] = Query(None, description="CSV: volume|pct_capacity; default both"),
    include: Optional[str] = Query(None, description="CSV: values,exceedance,box,statistics; default all"),
    wyt: Optional[str] = Query(None, description="CSV of water-year-types 1-5; default all"),
    db=Depends(get_db),
):
    # --- parse + validate params ------------------------------------------
    scen_list = _csv(scenarios)
    if not scen_list:
        raise HTTPException(400, "scenarios is required")

    subj_list = _csv(subjects) or None
    period_list = _csv(periods) or DEFAULT_PERIODS
    if any(p not in VALID_PERIODS for p in period_list):
        raise HTTPException(400, f"invalid period; allowed: {sorted(VALID_PERIODS)}")

    unit_tokens = _csv(units) or ["volume", "pct_capacity"]
    try:
        unit_list = [UNIT_ALIASES.get(u.lower(), u.upper()) for u in unit_tokens]
    except AttributeError:
        raise HTTPException(400, "invalid units")
    if any(u not in VALID_UNITS for u in unit_list):
        raise HTTPException(400, "invalid unit; allowed: volume|pct_capacity (TAF|PCT_CAP)")

    include_list = _csv(include) or list(INCLUDE_ALL)
    if any(i not in VALID_INCLUDE for i in include_list):
        raise HTTPException(400, f"invalid include; allowed: {sorted(VALID_INCLUDE)}")

    wyt_list: Optional[List[int]] = None
    if wyt:
        try:
            wyt_list = [int(w) for w in _csv(wyt)]
        except ValueError:
            raise HTTPException(400, "wyt must be integers 1-5")
        if any(w not in VALID_WYT for w in wyt_list):
            raise HTTPException(400, "wyt values must be 1-5")

    # --- cache ------------------------------------------------------------
    cache_key = "|".join([
        ",".join(sorted(scen_list)),
        ",".join(sorted(subj_list)) if subj_list else "*",
        ",".join(sorted(period_list)),
        ",".join(sorted(unit_list)),
        ",".join(sorted(include_list)),
        ",".join(str(w) for w in sorted(wyt_list)) if wyt_list else "*",
    ])
    max_age = api_cache_max_age()
    cached = _cache.get(cache_key)
    if cached is not None:
        return _json_response(cached, max_age)

    # --- SQL: filter/join/fetch raw rows ----------------------------------
    # source_variable LIKE 'S\_%' scopes to reservoir STORAGE (S_<code>, S_NOD_Reservoirs, S_SOD_Reservoirs).
    params: List[Any] = [scen_list, period_list, unit_list]
    where = [
        "v.scenario_short_code = ANY($1)",
        "v.period = ANY($2)",
        "u.short_code = ANY($3)",
        r"v.source_variable LIKE 'S\_%'",
        "v.is_active = TRUE",
    ]
    join_wyt = ""
    if subj_list:
        params.append(subj_list)
        where.append(f"s.short_code = ANY(${len(params)})")
    if wyt_list:
        params.append(wyt_list)
        join_wyt = (
            "JOIN scenario_water_year_type w "
            "ON w.scenario_short_code = v.scenario_short_code AND w.water_year = v.water_year"
        )
        where.append(f"w.wyt = ANY(${len(params)})")

    sql = f"""
        SELECT v.scenario_short_code,
               s.short_code   AS subject_code,
               s.subject_kind AS subject_kind,
               s.label        AS subject_label,
               v.period,
               u.short_code   AS unit,
               v.water_year,
               v.value
        FROM data_in_depth_value v
        JOIN data_in_depth_subject s ON s.id = v.data_in_depth_subject_id
        JOIN unit u ON u.id = v.unit_id
        {join_wyt}
        WHERE {" AND ".join(where)}
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:  # noqa: BLE001
        log.error("reservoir-storage query failed: %s", e)
        raise HTTPException(500, "query failed")

    result = compute_reservoir_storage(rows, include=include_list, wyt_filter=wyt_list)
    _cache[cache_key] = result
    return _json_response(result, max_age)


# ===========================================================================
# RIVER FLOWS  (separate, self-contained endpoint)
# ===========================================================================
# River flow is annual (water-year sum of TAF), single unit (TAF), no
# percent-of-capacity. Scoped by source_variable LIKE 'C\_%'.
# NOTE: the S_/C_ source-variable prefix is a stopgap discriminator; it will
# not scale cleanly as more measures/subjects are added — revisit with an
# explicit measure/domain tag once the remaining subjects land.
RIVER_UNIT_ALIASES = {"volume": "TAF", "taf": "TAF"}
RIVER_VALID_UNITS = {"TAF"}
RIVER_DEFAULT_PERIODS = ["annual"]
RIVER_VALID_PERIODS = {"annual"}


@router.get("/river-flows", summary="Annual water-year river flow (raw TAF + live-computed stats)")
async def river_flows(
    scenarios: str = Query(..., description="CSV of scenario short_codes (one or many)"),
    subjects: Optional[str] = Query(None, description="CSV of river-node short_codes (SAC000, YUB002, ...)"),
    periods: Optional[str] = Query(None, description="CSV; default annual"),
    units: Optional[str] = Query(None, description="CSV: volume (TAF) only; default TAF"),
    include: Optional[str] = Query(None, description="CSV: values,exceedance,box,statistics; default all"),
    wyt: Optional[str] = Query(None, description="CSV of water-year-types 1-5; default all"),
    db=Depends(get_db),
):
    # --- parse + validate params ------------------------------------------
    scen_list = _csv(scenarios)
    if not scen_list:
        raise HTTPException(400, "scenarios is required")

    subj_list = _csv(subjects) or None
    period_list = _csv(periods) or RIVER_DEFAULT_PERIODS
    if any(p not in RIVER_VALID_PERIODS for p in period_list):
        raise HTTPException(400, f"invalid period; allowed: {sorted(RIVER_VALID_PERIODS)}")

    unit_tokens = _csv(units) or ["volume"]
    unit_list = [RIVER_UNIT_ALIASES.get(u.lower(), u.upper()) for u in unit_tokens]
    if any(u not in RIVER_VALID_UNITS for u in unit_list):
        raise HTTPException(400, "invalid unit; river flow supports volume (TAF) only")

    include_list = _csv(include) or list(INCLUDE_ALL)
    if any(i not in VALID_INCLUDE for i in include_list):
        raise HTTPException(400, f"invalid include; allowed: {sorted(VALID_INCLUDE)}")

    wyt_list: Optional[List[int]] = None
    if wyt:
        try:
            wyt_list = [int(w) for w in _csv(wyt)]
        except ValueError:
            raise HTTPException(400, "wyt must be integers 1-5")
        if any(w not in VALID_WYT for w in wyt_list):
            raise HTTPException(400, "wyt values must be 1-5")

    # --- cache ------------------------------------------------------------
    cache_key = "river|" + "|".join([
        ",".join(sorted(scen_list)),
        ",".join(sorted(subj_list)) if subj_list else "*",
        ",".join(sorted(period_list)),
        ",".join(sorted(unit_list)),
        ",".join(sorted(include_list)),
        ",".join(str(w) for w in sorted(wyt_list)) if wyt_list else "*",
    ])
    max_age = api_cache_max_age()
    cached = _cache.get(cache_key)
    if cached is not None:
        return _json_response(cached, max_age)

    # --- SQL: filter/join/fetch raw rows ----------------------------------
    # source_variable LIKE 'C\_%' scopes to river/channel flow (C_<code>).
    params: List[Any] = [scen_list, period_list, unit_list]
    where = [
        "v.scenario_short_code = ANY($1)",
        "v.period = ANY($2)",
        "u.short_code = ANY($3)",
        r"v.source_variable LIKE 'C\_%'",
        "v.is_active = TRUE",
    ]
    join_wyt = ""
    if subj_list:
        params.append(subj_list)
        where.append(f"s.short_code = ANY(${len(params)})")
    if wyt_list:
        params.append(wyt_list)
        join_wyt = (
            "JOIN scenario_water_year_type w "
            "ON w.scenario_short_code = v.scenario_short_code AND w.water_year = v.water_year"
        )
        where.append(f"w.wyt = ANY(${len(params)})")

    sql = f"""
        SELECT v.scenario_short_code,
               s.short_code   AS subject_code,
               s.subject_kind AS subject_kind,
               s.label        AS subject_label,
               v.period,
               u.short_code   AS unit,
               v.water_year,
               v.value
        FROM data_in_depth_value v
        JOIN data_in_depth_subject s ON s.id = v.data_in_depth_subject_id
        JOIN unit u ON u.id = v.unit_id
        {join_wyt}
        WHERE {" AND ".join(where)}
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:  # noqa: BLE001
        log.error("river-flows query failed: %s", e)
        raise HTTPException(500, "query failed")

    result = compute_series(rows, include=include_list, wyt_filter=wyt_list, subject_key="rivers")
    _cache[cache_key] = result
    return _json_response(result, max_age)


# ===========================================================================
# DELTA SALINITY  (separate, self-contained endpoint)
# ===========================================================================
# X2 is a metric subject sampled April & September, unit km. Its CalSim var has
# no S_/C_ prefix, so this endpoint scopes by an EXPLICIT source-variable list
# (the robust approach the prefix trick stands in for elsewhere). Add delta
# salinity vars here as more land.
DELTA_SOURCE_VARS = ["X2_PRV_KM"]
DELTA_VALID_UNITS = {"km"}
DELTA_DEFAULT_PERIODS = ["april", "sept"]
DELTA_VALID_PERIODS = {"april", "sept"}


@router.get("/delta-salinity", summary="April/September Delta X2 position (raw km + live-computed stats)")
async def delta_salinity(
    scenarios: str = Query(..., description="CSV of scenario short_codes (one or many)"),
    subjects: Optional[str] = Query(None, description="CSV of subject short_codes (default: X2)"),
    periods: Optional[str] = Query(None, description="CSV; default april,sept"),
    units: Optional[str] = Query(None, description="CSV: km only; default km"),
    include: Optional[str] = Query(None, description="CSV: values,exceedance,box,statistics; default all"),
    wyt: Optional[str] = Query(None, description="CSV of water-year-types 1-5; default all"),
    db=Depends(get_db),
):
    # --- parse + validate params ------------------------------------------
    scen_list = _csv(scenarios)
    if not scen_list:
        raise HTTPException(400, "scenarios is required")

    subj_list = _csv(subjects) or None
    period_list = _csv(periods) or DELTA_DEFAULT_PERIODS
    if any(p not in DELTA_VALID_PERIODS for p in period_list):
        raise HTTPException(400, f"invalid period; allowed: {sorted(DELTA_VALID_PERIODS)}")

    unit_list = [u.lower() for u in (_csv(units) or ["km"])]
    if any(u not in DELTA_VALID_UNITS for u in unit_list):
        raise HTTPException(400, "invalid unit; delta salinity supports km only")

    include_list = _csv(include) or list(INCLUDE_ALL)
    if any(i not in VALID_INCLUDE for i in include_list):
        raise HTTPException(400, f"invalid include; allowed: {sorted(VALID_INCLUDE)}")

    wyt_list: Optional[List[int]] = None
    if wyt:
        try:
            wyt_list = [int(w) for w in _csv(wyt)]
        except ValueError:
            raise HTTPException(400, "wyt must be integers 1-5")
        if any(w not in VALID_WYT for w in wyt_list):
            raise HTTPException(400, "wyt values must be 1-5")

    # --- cache ------------------------------------------------------------
    cache_key = "delta|" + "|".join([
        ",".join(sorted(scen_list)),
        ",".join(sorted(subj_list)) if subj_list else "*",
        ",".join(sorted(period_list)),
        ",".join(sorted(unit_list)),
        ",".join(sorted(include_list)),
        ",".join(str(w) for w in sorted(wyt_list)) if wyt_list else "*",
    ])
    max_age = api_cache_max_age()
    cached = _cache.get(cache_key)
    if cached is not None:
        return _json_response(cached, max_age)

    # --- SQL: filter/join/fetch raw rows ----------------------------------
    # Scope by explicit delta source-variable list (no prefix trick).
    params: List[Any] = [scen_list, period_list, unit_list, DELTA_SOURCE_VARS]
    where = [
        "v.scenario_short_code = ANY($1)",
        "v.period = ANY($2)",
        "u.short_code = ANY($3)",
        "v.source_variable = ANY($4)",
        "v.is_active = TRUE",
    ]
    join_wyt = ""
    if subj_list:
        params.append(subj_list)
        where.append(f"s.short_code = ANY(${len(params)})")
    if wyt_list:
        params.append(wyt_list)
        join_wyt = (
            "JOIN scenario_water_year_type w "
            "ON w.scenario_short_code = v.scenario_short_code AND w.water_year = v.water_year"
        )
        where.append(f"w.wyt = ANY(${len(params)})")

    sql = f"""
        SELECT v.scenario_short_code,
               s.short_code   AS subject_code,
               s.subject_kind AS subject_kind,
               s.label        AS subject_label,
               v.period,
               u.short_code   AS unit,
               v.water_year,
               v.value
        FROM data_in_depth_value v
        JOIN data_in_depth_subject s ON s.id = v.data_in_depth_subject_id
        JOIN unit u ON u.id = v.unit_id
        {join_wyt}
        WHERE {" AND ".join(where)}
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:  # noqa: BLE001
        log.error("delta-salinity query failed: %s", e)
        raise HTTPException(500, "query failed")

    result = compute_series(rows, include=include_list, wyt_filter=wyt_list, subject_key="subjects")
    _cache[cache_key] = result
    return _json_response(result, max_age)
