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


# ===========================================================================
# COMMUNITY WATER SYSTEMS  (separate, self-contained endpoint)
# ===========================================================================
# CWS is annual, FIVE measures across two sources with DIFFERENT units:
# delivery (TAF) and percent-demand-met (PCT_DEMAND_MET) from the original
# demand_met_by_year source; welfare_loss (USD), shortage_total (TAF), and
# shortage_pct (PCT_SHORTAGE) from the newer welfare-outcomes source (see
# etl/data_in_depth/cws_extract_decisions.md). Groups by MEASURE (not unit).
# Scoped by the explicit CWS source-variable list. Subjects are the CWS demand
# units + NOD_CWS/SOD_CWS aggregates - all 5 measures are available at every
# subject (added 2026-08-06 for the 3 welfare measures, once a NOD/SOD mapping
# existed for their DUs - see cws_extract_decisions.md). Entity pct_demand_met/
# shortage_pct are read directly from / derived from the source; aggregate
# pct_demand_met is demand-weighted and capped at 100 (open_issues.md #5);
# aggregate shortage_pct is demand-weighted
# (sum(shortage_total)/sum(supply_total+shortage_total)*100) and can never
# exceed 100 (both terms non-negative), so no capping is needed there.
CWS_MEASURE_TO_SRC = {
    "delivery": "CWS_DELIVERY",
    "pct_demand_met": "CWS_PCT_DEMAND_MET",
    "welfare_loss": "CWS_WELFARE_LOSS",
    "shortage_total": "CWS_SHORTAGE_TOTAL",
    "shortage_pct": "CWS_SHORTAGE_PCT",
}
CWS_VALID_MEASURES = set(CWS_MEASURE_TO_SRC)
CWS_DEFAULT_PERIODS = ["annual"]
CWS_VALID_PERIODS = {"annual"}
# welfare_loss's extreme skew (open_issues.md #9) makes an exceedance curve
# over an arbitrary wyt-filtered population misleading for now. Suppressed
# (not an error - the key is just silently absent) until a future version
# computes welfare_loss percentiles over ALL water years always, ignoring wyt,
# rather than recomputing per filtered population like every other series.
CWS_NO_EXCEEDANCE = {"welfare_loss"}


@router.get("/cws", summary="Annual CWS delivery, percent-demand-met & welfare outcomes (raw + live-computed stats)")
async def cws(
    scenarios: str = Query(..., description="CSV of scenario short_codes (one or many)"),
    subjects: Optional[str] = Query(None, description="CSV of CWS subject short_codes (02_PU, MWD, NOD_CWS, ...)"),
    periods: Optional[str] = Query(None, description="CSV; default annual"),
    measures: Optional[str] = Query(None, description="CSV: delivery,pct_demand_met,welfare_loss,shortage_total,shortage_pct; default all"),
    include: Optional[str] = Query(None, description="CSV: values,exceedance,box,statistics; default all"),
    wyt: Optional[str] = Query(None, description="CSV of water-year-types 1-5; default all"),
    db=Depends(get_db),
):
    # --- parse + validate params ------------------------------------------
    scen_list = _csv(scenarios)
    if not scen_list:
        raise HTTPException(400, "scenarios is required")

    subj_list = _csv(subjects) or None
    period_list = _csv(periods) or CWS_DEFAULT_PERIODS
    if any(p not in CWS_VALID_PERIODS for p in period_list):
        raise HTTPException(400, f"invalid period; allowed: {sorted(CWS_VALID_PERIODS)}")

    measure_list = [m.lower() for m in (_csv(measures) or list(CWS_VALID_MEASURES))]
    if any(m not in CWS_VALID_MEASURES for m in measure_list):
        raise HTTPException(400, f"invalid measure; allowed: {sorted(CWS_VALID_MEASURES)}")
    src_list = [CWS_MEASURE_TO_SRC[m] for m in measure_list]

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
    cache_key = "cws|" + "|".join([
        ",".join(sorted(scen_list)),
        ",".join(sorted(subj_list)) if subj_list else "*",
        ",".join(sorted(period_list)),
        ",".join(sorted(measure_list)),
        ",".join(sorted(include_list)),
        ",".join(str(w) for w in sorted(wyt_list)) if wyt_list else "*",
    ])
    max_age = api_cache_max_age()
    cached = _cache.get(cache_key)
    if cached is not None:
        return _json_response(cached, max_age)

    # --- SQL: filter/join/fetch raw rows ----------------------------------
    # Scope by explicit CWS source-variable list; map to friendly measure names.
    # No unit filter: each source_variable has exactly one unit (TAF for
    # delivery/shortage_total, PCT_DEMAND_MET for pct_demand_met, USD for
    # welfare_loss, PCT_SHORTAGE for shortage_pct), so source_variable alone
    # disambiguates.
    params: List[Any] = [scen_list, period_list, src_list]
    where = [
        "v.scenario_short_code = ANY($1)",
        "v.period = ANY($2)",
        "v.source_variable = ANY($3)",
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
               CASE v.source_variable
                    WHEN 'CWS_DELIVERY' THEN 'delivery'
                    WHEN 'CWS_PCT_DEMAND_MET' THEN 'pct_demand_met'
                    WHEN 'CWS_WELFARE_LOSS' THEN 'welfare_loss'
                    WHEN 'CWS_SHORTAGE_TOTAL' THEN 'shortage_total'
                    WHEN 'CWS_SHORTAGE_PCT' THEN 'shortage_pct'
                    ELSE v.source_variable END AS measure,
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
        log.error("cws query failed: %s", e)
        raise HTTPException(500, "query failed")

    result = compute_series(rows, include=include_list, wyt_filter=wyt_list,
                            subject_key="subjects", series_field="measure",
                            no_exceedance=CWS_NO_EXCEEDANCE)
    _cache[cache_key] = result
    return _json_response(result, max_age)


# ===========================================================================
# GROUNDWATER STORAGE  (separate, self-contained endpoint)
# ===========================================================================
# Groundwater is annual, TWO measures with DIFFERENT units and DIFFERENT
# physical meaning: volume (GW_STOR, TAF, extensive) and level (GW_LEVEL, ft,
# intensive - a water-table elevation, not a quantity). Groups by MEASURE
# (like CWS/ag). Subjects are the groundwater WBAs (location_type='wba') +
# NOD_GroundwaterStorage/SOD_GroundwaterStorage aggregates. Aggregates only
# have the volume measure - summing water-table elevations across WBAs is
# physically meaningless, so no level aggregate is ever computed (permanent,
# not a pending decision - see etl/data_in_depth/open_issues.md #8); requesting
# measures=level for the aggregates returns an empty series for that subject.
GW_MEASURE_TO_SRC = {"volume": "GW_STOR", "level": "GW_LEVEL"}
GW_VALID_MEASURES = set(GW_MEASURE_TO_SRC)
GW_DEFAULT_PERIODS = ["annual"]
GW_VALID_PERIODS = {"annual"}


@router.get("/groundwater-storage", summary="Annual groundwater storage volume & level (raw + live-computed stats)")
async def groundwater_storage(
    scenarios: str = Query(..., description="CSV of scenario short_codes (one or many)"),
    subjects: Optional[str] = Query(None, description="CSV of WBA subject short_codes (WBA2, NOD_GroundwaterStorage, ...)"),
    periods: Optional[str] = Query(None, description="CSV; default annual"),
    measures: Optional[str] = Query(None, description="CSV: volume,level; default both"),
    include: Optional[str] = Query(None, description="CSV: values,exceedance,box,statistics; default all"),
    wyt: Optional[str] = Query(None, description="CSV of water-year-types 1-5; default all"),
    db=Depends(get_db),
):
    # --- parse + validate params ------------------------------------------
    scen_list = _csv(scenarios)
    if not scen_list:
        raise HTTPException(400, "scenarios is required")

    subj_list = _csv(subjects) or None
    period_list = _csv(periods) or GW_DEFAULT_PERIODS
    if any(p not in GW_VALID_PERIODS for p in period_list):
        raise HTTPException(400, f"invalid period; allowed: {sorted(GW_VALID_PERIODS)}")

    measure_list = [m.lower() for m in (_csv(measures) or list(GW_VALID_MEASURES))]
    if any(m not in GW_VALID_MEASURES for m in measure_list):
        raise HTTPException(400, f"invalid measure; allowed: {sorted(GW_VALID_MEASURES)}")
    src_list = [GW_MEASURE_TO_SRC[m] for m in measure_list]

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
    cache_key = "gw|" + "|".join([
        ",".join(sorted(scen_list)),
        ",".join(sorted(subj_list)) if subj_list else "*",
        ",".join(sorted(period_list)),
        ",".join(sorted(measure_list)),
        ",".join(sorted(include_list)),
        ",".join(str(w) for w in sorted(wyt_list)) if wyt_list else "*",
    ])
    max_age = api_cache_max_age()
    cached = _cache.get(cache_key)
    if cached is not None:
        return _json_response(cached, max_age)

    # --- SQL: filter/join/fetch raw rows ----------------------------------
    # Scope by explicit groundwater source-variable list; map to friendly
    # measure names. No unit filter: each source_variable has exactly one
    # unit (TAF for volume, ft for level), so source_variable disambiguates.
    params: List[Any] = [scen_list, period_list, src_list]
    where = [
        "v.scenario_short_code = ANY($1)",
        "v.period = ANY($2)",
        "v.source_variable = ANY($3)",
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
               CASE v.source_variable
                    WHEN 'GW_STOR' THEN 'volume'
                    WHEN 'GW_LEVEL' THEN 'level'
                    ELSE v.source_variable END AS measure,
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
        log.error("groundwater-storage query failed: %s", e)
        raise HTTPException(500, "query failed")

    result = compute_series(rows, include=include_list, wyt_filter=wyt_list,
                            subject_key="subjects", series_field="measure")
    _cache[cache_key] = result
    return _json_response(result, max_age)


# ===========================================================================
# AGRICULTURE  (separate, self-contained endpoint)
# ===========================================================================
# Ag is annual, FOUR measures: net_diversion/gw_pumping/shortage (all TAF,
# taken directly from the source - shortage is a source column here, not
# derived) and revenue (USD, added 2026-07-29 - not a volume, but the generic
# value table doesn't require one). Groups by MEASURE (like CWS) for
# consistency even though three of the four share a unit. Scoped by the
# explicit ag source-variable list. Subjects are the ag demand units
# (location_type='ag_demand_unit') + NOD_Agriculture/SOD_Agriculture
# aggregates (all four measures summed across members - no capping/weighting
# ambiguity since these are all extensive quantities, not percentages).
AG_MEASURE_TO_SRC = {
    "net_diversion": "AG_NET_DIVERSION",
    "gw_pumping": "AG_GW_PUMPING",
    "shortage": "AG_SHORTAGE",
    "revenue": "AG_REVENUE",
}
AG_VALID_MEASURES = set(AG_MEASURE_TO_SRC)
AG_DEFAULT_PERIODS = ["annual"]
AG_VALID_PERIODS = {"annual"}


@router.get("/ag", summary="Annual ag net diversion, GW pumping, shortage & revenue (raw + live-computed stats)")
async def ag(
    scenarios: str = Query(..., description="CSV of scenario short_codes (one or many)"),
    subjects: Optional[str] = Query(None, description="CSV of ag subject short_codes (02_NA, NOD_Agriculture, ...)"),
    periods: Optional[str] = Query(None, description="CSV; default annual"),
    measures: Optional[str] = Query(None, description="CSV: net_diversion,gw_pumping,shortage,revenue; default all"),
    include: Optional[str] = Query(None, description="CSV: values,exceedance,box,statistics; default all"),
    wyt: Optional[str] = Query(None, description="CSV of water-year-types 1-5; default all"),
    db=Depends(get_db),
):
    # --- parse + validate params ------------------------------------------
    scen_list = _csv(scenarios)
    if not scen_list:
        raise HTTPException(400, "scenarios is required")

    subj_list = _csv(subjects) or None
    period_list = _csv(periods) or AG_DEFAULT_PERIODS
    if any(p not in AG_VALID_PERIODS for p in period_list):
        raise HTTPException(400, f"invalid period; allowed: {sorted(AG_VALID_PERIODS)}")

    measure_list = [m.lower() for m in (_csv(measures) or list(AG_VALID_MEASURES))]
    if any(m not in AG_VALID_MEASURES for m in measure_list):
        raise HTTPException(400, f"invalid measure; allowed: {sorted(AG_VALID_MEASURES)}")
    src_list = [AG_MEASURE_TO_SRC[m] for m in measure_list]

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
    cache_key = "ag|" + "|".join([
        ",".join(sorted(scen_list)),
        ",".join(sorted(subj_list)) if subj_list else "*",
        ",".join(sorted(period_list)),
        ",".join(sorted(measure_list)),
        ",".join(sorted(include_list)),
        ",".join(str(w) for w in sorted(wyt_list)) if wyt_list else "*",
    ])
    max_age = api_cache_max_age()
    cached = _cache.get(cache_key)
    if cached is not None:
        return _json_response(cached, max_age)

    # --- SQL: filter/join/fetch raw rows ----------------------------------
    # Scope by explicit ag source-variable list; map to friendly measure names.
    params: List[Any] = [scen_list, period_list, src_list]
    where = [
        "v.scenario_short_code = ANY($1)",
        "v.period = ANY($2)",
        "v.source_variable = ANY($3)",
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
               CASE v.source_variable
                    WHEN 'AG_NET_DIVERSION' THEN 'net_diversion'
                    WHEN 'AG_GW_PUMPING' THEN 'gw_pumping'
                    WHEN 'AG_SHORTAGE' THEN 'shortage'
                    WHEN 'AG_REVENUE' THEN 'revenue'
                    ELSE v.source_variable END AS measure,
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
        log.error("ag query failed: %s", e)
        raise HTTPException(500, "query failed")

    result = compute_series(rows, include=include_list, wyt_filter=wyt_list,
                            subject_key="subjects", series_field="measure")
    _cache[cache_key] = result
    return _json_response(result, max_age)


# ===========================================================================
# SALMON (WRLCM)  (separate, self-contained endpoint)
# ===========================================================================
# Salmon is annual, ONE subject (WRLCM_ADULT_FEMALES, a metric - no location),
# ONE source_variable (METRIC_AVG_ROLL), ONE unit (NOF_3YR_AVG). So this groups
# by UNIT (like system-deliveries/river-flows) - there is NO measure param.
#
# No aggregates (single subject, nothing to sum). water_year here is year_cal
# (calendar year, NOT the usual Oct-Sep water year - see extract_salmon.py).

SALMON_SOURCE_VARS = ["METRIC_AVG_ROLL"]
SALMON_UNIT_ALIASES = {"nof_3yr_avg": "NOF_3YR_AVG", "adult_females": "NOF_3YR_AVG"}
SALMON_VALID_UNITS = {"NOF_3YR_AVG"}
SALMON_DEFAULT_PERIODS = ["annual"]
SALMON_VALID_PERIODS = {"annual"}


@router.get("/salmon", summary="Annual WRLCM adult-females 3-yr rolling average (raw + live-computed stats)")
async def salmon(
    scenarios: str = Query(..., description="CSV of scenario short_codes (one or many)"),
    subjects: Optional[str] = Query(None, description="CSV of subject short_codes (default: WRLCM_ADULT_FEMALES)"),
    periods: Optional[str] = Query(None, description="CSV; default annual"),
    units: Optional[str] = Query(None, description="CSV: nof_3yr_avg only; default NOF_3YR_AVG"),
    include: Optional[str] = Query(None, description="CSV: values,exceedance,box,statistics; default all"),
    wyt: Optional[str] = Query(None, description="CSV of water-year-types 1-5; default all"),
    db=Depends(get_db),
):
    # --- parse + validate params ------------------------------------------
    scen_list = _csv(scenarios)
    if not scen_list:
        raise HTTPException(400, "scenarios is required")

    subj_list = _csv(subjects) or None
    period_list = _csv(periods) or SALMON_DEFAULT_PERIODS
    if any(p not in SALMON_VALID_PERIODS for p in period_list):
        raise HTTPException(400, f"invalid period; allowed: {sorted(SALMON_VALID_PERIODS)}")

    unit_tokens = _csv(units) or ["nof_3yr_avg"]
    unit_list = [SALMON_UNIT_ALIASES.get(u.lower(), u.upper()) for u in unit_tokens]
    if any(u not in SALMON_VALID_UNITS for u in unit_list):
        raise HTTPException(400, "invalid unit; salmon supports nof_3yr_avg (NOF_3YR_AVG) only")

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
    cache_key = "salmon|" + "|".join([
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
    # Scope by the explicit salmon source-variable list (single variable).
    params: List[Any] = [scen_list, period_list, unit_list, SALMON_SOURCE_VARS]
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
        log.error("salmon query failed: %s", e)
        raise HTTPException(500, "query failed")

    result = compute_series(rows, include=include_list, wyt_filter=wyt_list, subject_key="subjects")
    _cache[cache_key] = result
    return _json_response(result, max_age)


# ===========================================================================
# SYSTEM DELIVERIES  (separate, self-contained endpoint)
# ===========================================================================
# System deliveries is annual, ONE unit (TAF), 25 INDEPENDENT metric subjects -
# CVP/SWP delivery totals broken out by NOD/SOD/Total x AG/M&I/Refuges, Delta
# export totals, and Southern San Joaquin Valley export paths. UNLIKE every
# other data_in_depth domain, each subject IS its own leaf variable (short_code
# == source_variable, 1:1) - there's no measure-splitting (no CWS/ag/groundwater-
# style shared subject) and no aggregation (NOD/SOD/Total triplets are each their
# own pre-aggregated raw CalSim variable, not summed here - see
# seed_data_in_depth_system_deliveries_subjects.sql / extract_system_deliveries.py).
# So this groups by UNIT (like reservoir/river/delta) rather than measure, even
# though there's only one unit - shape matches river-flows most closely (annual,
# TAF-only, no percent-of-capacity, no aggregates), but scoped by an EXPLICIT
# source-variable list (like delta-salinity) since the 25 variables don't share
# a clean prefix (DEL_/C_/D_/SWP_ all appear).
SYSTEM_DELIVERIES_SOURCE_VARS = [
    "DEL_CVP_TOT_N_WAMER", "DEL_CVP_TOT_S_WLOSS", "DEL_CVP_TOTAL",
    "DEL_CVP_PAG_NOD", "DEL_CVP_PAG_SOD", "DEL_CVP_PAG_TOTAL",
    "DEL_CVP_PMI_TOTAL", "DEL_CVP_PMI_N_WAMER", "DEL_CVP_PMI_S",
    "DEL_CVP_PRF_TOTAL", "C_CVP_TOTAL_EXPORTS",
    "DEL_SWP_TOT_N", "DEL_SWP_TOT_S", "DEL_SWP_TOTAL",
    "DEL_SWP_PAG_NOD", "DEL_SWP_PAG_S", "DEL_SWP_PAG_TOTAL",
    "DEL_SWP_PMI", "DEL_SWP_PMI_N", "DEL_SWP_PMI_S",
    "D_MLRTN_FRK000", "D_CAA238_CVPCV", "SWP_TA_KERNAG",
    "C_CAA003_SWP", "C_CVPSWP_TOTAL_EXPORTS",
]
SYS_DEL_UNIT_ALIASES = {"volume": "TAF", "taf": "TAF"}
SYS_DEL_VALID_UNITS = {"TAF"}
SYS_DEL_DEFAULT_PERIODS = ["annual"]
SYS_DEL_VALID_PERIODS = {"annual"}


@router.get("/system-deliveries", summary="Annual CVP/SWP delivery & Delta export totals (raw + live-computed stats)")
async def system_deliveries(
    scenarios: str = Query(..., description="CSV of scenario short_codes (one or many)"),
    subjects: Optional[str] = Query(None, description="CSV of subject short_codes (DEL_CVP_TOTAL, C_CAA003_SWP, ...); default all 25"),
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
    period_list = _csv(periods) or SYS_DEL_DEFAULT_PERIODS
    if any(p not in SYS_DEL_VALID_PERIODS for p in period_list):
        raise HTTPException(400, f"invalid period; allowed: {sorted(SYS_DEL_VALID_PERIODS)}")

    unit_tokens = _csv(units) or ["volume"]
    unit_list = [SYS_DEL_UNIT_ALIASES.get(u.lower(), u.upper()) for u in unit_tokens]
    if any(u not in SYS_DEL_VALID_UNITS for u in unit_list):
        raise HTTPException(400, "invalid unit; system deliveries supports volume (TAF) only")

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
    cache_key = "sysdel|" + "|".join([
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
    # Scope by the explicit system-deliveries source-variable list (no prefix
    # trick - DEL_/C_/D_/SWP_ all appear across these 25 variables).
    params: List[Any] = [scen_list, period_list, unit_list, SYSTEM_DELIVERIES_SOURCE_VARS]
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
        log.error("system-deliveries query failed: %s", e)
        raise HTTPException(500, "query failed")

    result = compute_series(rows, include=include_list, wyt_filter=wyt_list, subject_key="subjects")
    _cache[cache_key] = result
    return _json_response(result, max_age)
