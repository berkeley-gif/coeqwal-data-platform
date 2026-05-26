"""
Tier API endpoints for COEQWAL interpretive framework.

Provides tier definitions and scenario tier data for outcome visualization.

Two tier types:
- multi_value: Distribution across locations (e.g., 70 tier-1, 30 tier-2, etc.)
- single_value: Single overall tier level (1-4)
"""

import logging
from typing import Any, Dict, List, Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from routes._common import (
    api_cache_max_age,
    make_ttl_cache,
    safe_float,
    safe_int,
    safe_str,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tiers", tags=["tiers"])

# Static catalog: tier definitions change between ETL runs only.
# Per-scenario tier batches are cached more loosely below
_static_cache = make_ttl_cache("tiers_static", maxsize=50)
_batch_cache = make_ttl_cache("tiers_batch", maxsize=500)


def _catalog_cache_header() -> str:
    """Return the Cache-Control value matching the in-process TTL."""
    return f"public, max-age={api_cache_max_age()}"

# =============================================================================
# PYDANTIC MODELS
# =============================================================================


class TierDefinition(BaseModel):
    """Definition of a tier indicator"""

    short_code: str = Field(
        ..., description="Unique identifier (e.g., 'AG_REV', 'CWS_DEL')"
    )
    name: str = Field(..., description="Display name (e.g., 'Agricultural revenue')")
    description: Optional[str] = Field(None, description="Detailed description for tooltips. Null when no description was seeded.")
    tier_type: str = Field(..., description="'multi_value' or 'single_value'")
    tier_count: int = Field(..., description="Number of tier levels (usually 4)")
    is_active: bool = Field(..., description="Whether this tier is currently active")


# NOTE: `TierData`, `MultiValueTierResult`, and `SingleValueTierResult` Pydantic
# models previously lived here but were never referenced as response_model on
# any handler (handlers return bare Dict[str, Any] shapes with richer fields
# than these models described). They were removed to prevent schema drift
# between the models and the actual responses. If we want typed responses here
# in the future, generate fresh models from the real handler return shapes.


# Database connection dependency (set by main.py)
db_pool = None


def set_db_pool(pool):
    global db_pool
    db_pool = pool


async def get_db():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection


def calculate_tier_scores(
    norm_1: Optional[float],
    norm_2: Optional[float],
    norm_3: Optional[float],
    norm_4: Optional[float],
) -> dict:
    """
    Calculate the two scores used by the Scenario Explorer for multi-value
    tier rows.

    Inputs may be None when the ETL row is missing or partial. If ALL four
    inputs are None, both outputs are None (no data). Otherwise None values
    are treated as 0 in the arithmetic so partial rows still yield scores.

    Returns:
    - weighted_score: 1.0 (best) to 4.0 (worst), drives sort comparators
    - normalized_score: 0.0 to 1.0 (higher = better), Y-axis for the
      parallel plot
    """
    if norm_1 is None and norm_2 is None and norm_3 is None and norm_4 is None:
        return {"weighted_score": None, "normalized_score": None}

    n1 = norm_1 if norm_1 is not None else 0.0
    n2 = norm_2 if norm_2 is not None else 0.0
    n3 = norm_3 if norm_3 is not None else 0.0
    n4 = norm_4 if norm_4 is not None else 0.0

    total_pct = n1 + n2 + n3 + n4

    # All-zero distribution: tier math is undefined
    if total_pct == 0:
        return {"weighted_score": None, "normalized_score": None}

    weighted_sum = (1 * n1) + (2 * n2) + (3 * n3) + (4 * n4)
    weighted_score = round(weighted_sum / total_pct, 3)

    # Map weighted_score 1.0 to normalized 1.0 (best), 4.0 to 0.0 (worst)
    normalized_score = round((4.0 - weighted_score) / 3.0, 3)

    return {
        "weighted_score": weighted_score,
        "normalized_score": normalized_score,
    }


@router.get("/definitions", summary="Get tier descriptions")
async def get_tier_definitions(
    response: Response,
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, str]:
    """
    Get tier definitions as a simple short_code → description mapping.

    **Use case:** Populate tooltips and help text in the UI.

    **Response format:**
    ```json
    {
      "AG_REV": "Impact on agricultural production and revenue",
      "CWS_DEL": "Reliability of deliveries to community water systems",
      ...
    }
    ```
    """
    cache_key = "tiers:definitions"
    if cache_key in _static_cache:
        response.headers["Cache-Control"] = _catalog_cache_header()
        return _static_cache[cache_key]

    try:
        query = """
        SELECT short_code, name, description 
        FROM tier_definition 
        WHERE is_active = TRUE 
        ORDER BY short_code
        """
        rows = await connection.fetch(query)
    except Exception as e:
        log.error(f"tier-definitions query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    # Fall back to `name` when no description was seeded, so tooltips still
    # have human-readable text. This is a domain-specific UX choice, not a
    # NULL-coercion problem (the alternative would force every consumer to
    # render an awkward placeholder).
    result = {row["short_code"]: row["description"] or row["name"] for row in rows}
    _static_cache[cache_key] = result
    response.headers["Cache-Control"] = _catalog_cache_header()
    return result


@router.get(
    "/list", summary="List all tier indicators", response_model=List[TierDefinition]
)
async def get_all_tier_definitions(
    response: Response,
    connection: asyncpg.Connection = Depends(get_db),
) -> List[TierDefinition]:
    """
    Get complete list of tier indicator definitions.

    **Use case:** Build tier selection UI, understand available indicators.

    Returns full metadata for each tier including:
    - `short_code`: Unique identifier for API calls
    - `name`: Human-readable display name
    - `tier_type`: 'multi_value' (distribution) or 'single_value' (overall score)
    - `tier_count`: Number of locations (for multi_value) or 1 (for single_value)
    """
    cache_key = "tiers:list"
    if cache_key in _static_cache:
        response.headers["Cache-Control"] = _catalog_cache_header()
        return _static_cache[cache_key]

    try:
        query = """
        SELECT short_code, name, description, tier_type, tier_count, is_active
        FROM tier_definition 
        WHERE is_active = TRUE 
        ORDER BY tier_type DESC, short_code
        """
        rows = await connection.fetch(query)
    except Exception as e:
        log.error(f"tier-list query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    result = [
        TierDefinition(
            short_code=row["short_code"],
            name=row["name"],
            description=safe_str(row["description"]),
            tier_type=row["tier_type"],
            tier_count=row["tier_count"],
            is_active=row["is_active"],
        )
        for row in rows
    ]
    _static_cache[cache_key] = result
    response.headers["Cache-Control"] = _catalog_cache_header()
    return result


@router.get("/scenarios/{scenario_id}/tiers", summary="Get all tiers for scenario")
async def get_all_scenario_tiers(
    scenario_id: str, connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get all tier indicator data for a scenario in a single request.

    **Use case:** Load all data for scenario comparison charts.

    **Example:** `GET /api/tiers/scenarios/s0020/tiers`

    Returns a dictionary of all tier indicators keyed by short_code:
    ```json
    {
      "scenario": "s0020",
      "tiers": {
        "AG_REV": { "name": "...", "type": "multi_value", "weighted_score": 1.78, "data": [...], "total": 132 },
        "CWS_DEL": { "name": "...", "type": "multi_value", "weighted_score": 1.12, "data": [...], "total": 91 },
        "DELTA_ECO": { "name": "...", "type": "single_value", "weighted_score": 3.0, "level": 3 },
        ...
      }
    }
    ```

    `weighted_score` is 1.0-4.0 for all tiers (lower is better). Use for sorting/comparison.
    """
    try:
        query = """
        SELECT 
            tr.tier_short_code,
            td.name,
            td.tier_type,
            tr.tier_1_value,
            tr.tier_2_value,
            tr.tier_3_value,
            tr.tier_4_value,
            tr.norm_tier_1,
            tr.norm_tier_2,
            tr.norm_tier_3,
            tr.norm_tier_4,
            tr.total_value,
            tr.single_tier_level
        FROM tier_result tr
        JOIN tier_definition td ON tr.tier_short_code = td.short_code
        WHERE tr.scenario_short_code = $1
        AND tr.is_active = TRUE
        ORDER BY td.tier_type DESC, tr.tier_short_code
        """
        rows = await connection.fetch(query, scenario_id)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"per-scenario tiers query failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    try:
        if not rows:
            raise HTTPException(
                status_code=404, detail=f"No tier data found for scenario {scenario_id}"
            )

        tiers = {}

        for row in rows:
            tier_code = row["tier_short_code"]

            if row["tier_type"] == "multi_value":
                norm_1 = safe_float(row["norm_tier_1"])
                norm_2 = safe_float(row["norm_tier_2"])
                norm_3 = safe_float(row["norm_tier_3"])
                norm_4 = safe_float(row["norm_tier_4"])

                scores = calculate_tier_scores(norm_1, norm_2, norm_3, norm_4)

                tiers[tier_code] = {
                    "name": row["name"],
                    "type": "multi_value",
                    "weighted_score": scores["weighted_score"],
                    "normalized_score": scores["normalized_score"],
                    # Fixed-length 4-element array, index i corresponds to
                    # tier level i+1. Clients derive the tier label from the
                    # index so we don't ship "tier1".."tier4" on every row
                    "data": [
                        {"value": safe_int(row["tier_1_value"]), "normalized": norm_1},
                        {"value": safe_int(row["tier_2_value"]), "normalized": norm_2},
                        {"value": safe_int(row["tier_3_value"]), "normalized": norm_3},
                        {"value": safe_int(row["tier_4_value"]), "normalized": norm_4},
                    ],
                    "total": safe_int(row["total_value"]),
                }
            else:
                level = safe_int(row["single_tier_level"])
                if level is None:
                    weighted = None
                    normalized = None
                else:
                    weighted = float(level)
                    normalized = round((4.0 - weighted) / 3.0, 3)
                tiers[tier_code] = {
                    "name": row["name"],
                    "type": "single_value",
                    "weighted_score": weighted,
                    "normalized_score": normalized,
                    "level": level,
                }

        return {"scenario": scenario_id, "tiers": tiers}
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"per-scenario tiers shaping failed for {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")


@router.get("/batch", summary="Get tiers for multiple scenarios in one request")
async def get_batch_scenario_tiers(
    scenarios: str = Query(
        ...,
        description="Comma-separated scenario IDs (e.g., 's0020,s0021,s0029')",
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Fetch all tier data for multiple scenarios in a single request.

    Replaces N individual calls to `/tiers/scenarios/{id}/tiers` with one
    batched query, dramatically reducing load times when switching
    hydroclimates or loading the Scenario Explorer.

    **Example:** `GET /api/tiers/batch?scenarios=s0020,s0021,s0029`

    **Response format:**
    ```json
    {
      "scenarios": {
        "s0020": { "tiers": { "AG_REV": {...}, "CWS_DEL": {...}, ... } },
        "s0021": { "tiers": { "AG_REV": {...}, "CWS_DEL": {...}, ... } }
      },
      "count": 2
    }
    ```
    """
    scenario_ids = [s.strip() for s in scenarios.split(",") if s.strip()]

    if not scenario_ids:
        raise HTTPException(status_code=400, detail="No scenario IDs provided")

    cache_key = "tiers:batch:" + ",".join(sorted(scenario_ids))
    if cache_key in _batch_cache:
        return _batch_cache[cache_key]

    try:
        query = """
        SELECT
            tr.scenario_short_code,
            tr.tier_short_code,
            td.name,
            td.tier_type,
            tr.tier_1_value,
            tr.tier_2_value,
            tr.tier_3_value,
            tr.tier_4_value,
            tr.norm_tier_1,
            tr.norm_tier_2,
            tr.norm_tier_3,
            tr.norm_tier_4,
            tr.total_value,
            tr.single_tier_level
        FROM tier_result tr
        JOIN tier_definition td ON tr.tier_short_code = td.short_code
        WHERE tr.scenario_short_code = ANY($1)
        AND tr.is_active = TRUE
        ORDER BY tr.scenario_short_code, td.tier_type DESC, tr.tier_short_code
        """

        rows = await connection.fetch(query, scenario_ids)

        result: Dict[str, Dict[str, Any]] = {}

        for row in rows:
            scenario_id = row["scenario_short_code"]
            tier_code = row["tier_short_code"]

            if scenario_id not in result:
                result[scenario_id] = {}

            if row["tier_type"] == "multi_value":
                norm_1 = safe_float(row["norm_tier_1"])
                norm_2 = safe_float(row["norm_tier_2"])
                norm_3 = safe_float(row["norm_tier_3"])
                norm_4 = safe_float(row["norm_tier_4"])

                scores = calculate_tier_scores(norm_1, norm_2, norm_3, norm_4)

                result[scenario_id][tier_code] = {
                    "name": row["name"],
                    "type": "multi_value",
                    "weighted_score": scores["weighted_score"],
                    "normalized_score": scores["normalized_score"],
                    # Fixed-length 4-element array, index i corresponds to
                    # tier level i+1 (see per-scenario handler for context)
                    "data": [
                        {"value": safe_int(row["tier_1_value"]), "normalized": norm_1},
                        {"value": safe_int(row["tier_2_value"]), "normalized": norm_2},
                        {"value": safe_int(row["tier_3_value"]), "normalized": norm_3},
                        {"value": safe_int(row["tier_4_value"]), "normalized": norm_4},
                    ],
                    "total": safe_int(row["total_value"]),
                }
            else:
                level = safe_int(row["single_tier_level"])
                if level is None:
                    weighted = None
                    normalized = None
                else:
                    weighted = float(level)
                    normalized = round((4.0 - weighted) / 3.0, 3)
                result[scenario_id][tier_code] = {
                    "name": row["name"],
                    "type": "single_value",
                    "weighted_score": weighted,
                    "normalized_score": normalized,
                    "level": level,
                }

        out = {
            "scenarios": {
                sid: {"scenario": sid, "tiers": tiers}
                for sid, tiers in result.items()
            },
            "count": len(result),
        }
        _batch_cache[cache_key] = out
        return out
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"batch tiers query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")


# =============================================================================
# PER-LOCATION TIER ASSIGNMENTS
# =============================================================================
#
# Returns the per-location tier scores that map visualizations and tables
# consume. No geometry. Polygon and point geometry comes from Mapbox vector
# tiles on the client side, joined to these payloads by `location_id` (or
# `du_id` / `wba_id` / `station_code` / network short_code, depending on the
# entity). Location types in the payload: reservoir, wba, region,
# compliance_station, network_node, demand_unit.
#
# See the geometry policy in `coeqwal-backend/database/README.md`
# ("API conventions, geometry") and the corresponding tile workflow in
# `coeqwal-website/packages/map/README.md` ("Adding new tile data via Mapbox
# Tiling Service").


@router.get(
    "/scenarios/{scenario_short_code}/locations",
    summary="Get per-location tier assignments for one or more outcomes (no geometry)",
)
async def get_scenario_tier_locations(
    scenario_short_code: str,
    codes: str = Query(
        ...,
        description=(
            "Comma-separated list of tier short codes, e.g. "
            "`CWS_DEL,AG_REV,ENV_FLOWS`. Must be non-empty."
        ),
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Per-location tier assignments for one or more outcomes in a single
    request. One SQL query replaces N parallel calls when a panel needs
    several outcomes at once (e.g. an equity heatmap showing all nine
    outcomes for one scenario).

    **Use case:** Batched per-location data for multi-outcome panels and the
    map (which joins these `location_id`s to Mapbox tile features).

    **Example:** `GET /api/tiers/scenarios/s0020/locations?codes=CWS_DEL,AG_REV,ENV_FLOWS`

    **Response:**
    ```json
    {
      "scenario": "s0020",
      "results": {
        "CWS_DEL":  { "scenario": "s0020", "tier_code": "CWS_DEL",  "tier_name": ..., "tier_type": ..., "locations": [...], "metadata": {...} },
        "AG_REV":   { "scenario": "s0020", "tier_code": "AG_REV",   ... },
        "ENV_FLOWS": { ... }
      },
      "missing": []
    }
    ```

    `missing` lists codes the client requested that have no active rows for
    this scenario (a normal case, for example `WRC_SALMON_AB` on `s0065`).

    Filters `tier_result.is_active = TRUE`, so retired scenarios and retired
    tier versions are never surfaced.
    """
    # Parse and lightly validate the codes list. Deduplicate while
    # preserving request order, upper-case for tolerance, reject empty /
    # malformed lists.
    seen = set()
    requested: List[str] = []
    for raw in codes.split(","):
        code = raw.strip().upper()
        if not code:
            continue
        if not code.replace("_", "").isalnum():
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tier short code: '{raw}'",
            )
        if code in seen:
            continue
        seen.add(code)
        requested.append(code)

    if not requested:
        raise HTTPException(
            status_code=400,
            detail="Query parameter 'codes' must list at least one tier short code.",
        )

    try:
        # Existence / active check so unknown and retired scenarios 404
        # explicitly instead of returning a misleading 200 with every
        # requested code dumped into `missing`. Cheap indexed lookup. Zero
        # cost on valid scenarios. The message is distinct between the two
        # cases so operators can tell "typo" from "scenario was retired".
        scenario_row = await connection.fetchrow(
            "SELECT is_active FROM scenario WHERE short_code = $1",
            scenario_short_code,
        )
        if scenario_row is None:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown scenario '{scenario_short_code}'.",
            )
        if not scenario_row["is_active"]:
            raise HTTPException(
                status_code=404,
                detail=f"Scenario '{scenario_short_code}' is not active.",
            )

        query = """
        SELECT
            tlr.tier_short_code,
            tlr.location_type,
            tlr.location_id,
            tlr.location_name,
            tlr.tier_level,
            tlr.tier_value,
            td.name AS tier_name,
            td.tier_type
        FROM tier_location_result tlr
        JOIN tier_definition td ON tlr.tier_short_code = td.short_code
        JOIN tier_result tr
          ON tr.scenario_short_code = tlr.scenario_short_code
         AND tr.tier_short_code = tlr.tier_short_code
         AND tr.tier_version_id = tlr.tier_version_id
         AND tr.is_active = TRUE
        WHERE tlr.scenario_short_code = $1
          AND tlr.tier_short_code = ANY($2::text[])
        ORDER BY tlr.tier_short_code, tlr.display_order, tlr.location_name
        """

        rows = await connection.fetch(query, scenario_short_code, requested)

        # Bucket rows by tier_short_code. Keys are only created for codes
        # that actually returned rows; codes with no active data land in
        # `missing`.
        buckets: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            code = row["tier_short_code"]
            bucket = buckets.get(code)
            if bucket is None:
                bucket = {
                    "scenario": scenario_short_code,
                    "tier_code": code,
                    "tier_name": row["tier_name"],
                    "tier_type": row["tier_type"],
                    "locations": [],
                    "_location_types": set(),
                    "_tier_counts": {1: 0, 2: 0, 3: 0, 4: 0},
                }
                buckets[code] = bucket

            bucket["locations"].append(
                {
                    "location_id": row["location_id"],
                    "location_name": row["location_name"],
                    "location_type": row["location_type"],
                    "tier_level": row["tier_level"],
                    "tier_value": row["tier_value"],
                }
            )
            bucket["_location_types"].add(row["location_type"])
            if row["tier_level"] in bucket["_tier_counts"]:
                bucket["_tier_counts"][row["tier_level"]] += 1

        results: Dict[str, Any] = {}
        for code, bucket in buckets.items():
            results[code] = {
                "scenario": bucket["scenario"],
                "tier_code": bucket["tier_code"],
                "tier_name": bucket["tier_name"],
                "tier_type": bucket["tier_type"],
                "locations": bucket["locations"],
                "metadata": {
                    "total_locations": len(bucket["locations"]),
                    "location_types": sorted(bucket["_location_types"]),
                    "tier_counts": bucket["_tier_counts"],
                },
            }

        missing = [code for code in requested if code not in results]

        return {
            "scenario": scenario_short_code,
            "results": results,
            "missing": missing,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(
            f"scenario-tier-locations query failed for {scenario_short_code}: {e}"
        )
        raise HTTPException(status_code=500, detail="Database query failed")
