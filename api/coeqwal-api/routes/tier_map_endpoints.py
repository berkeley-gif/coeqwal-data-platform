"""
tier_map_endpoints.py - Tier map visualization API endpoints

Provides tier data with geospatial geometries for map-based visualization.

Location Types:
- reservoir: Major reservoirs (Shasta, Oroville, etc.)
- wba: Water budget areas / aquifers (groundwater)
- region: Regions like Delta
- compliance_station: Flow compliance points
- network_node: CalSim network nodes (environmental flows)
- demand_unit: Urban and agricultural demand units (CWS, AG)

GeoJSON Format:
- Returns standard GeoJSON FeatureCollection
- Each feature includes tier_level (1-4) in properties
- Frontend can color by tier_level
"""

from fastapi import APIRouter, HTTPException, Depends, Query, Response
from typing import Dict, List, Optional, Any
import asyncpg
from pydantic import BaseModel, Field
import json

# Cache-Control header for catalog endpoints whose contents only change between ETL
# runs (tier/scenario/hydroclimate lists and their joins). 5 minutes gives CDNs and
# browsers a safe reuse window without masking new data for long after a deploy.
STATIC_CATALOG_CACHE_CONTROL = "public, max-age=300"

router = APIRouter(prefix="/api/tier-map", tags=["tier-map"])

# =============================================================================
# PYDANTIC MODELS
# =============================================================================


class TierMapFeature(BaseModel):
    """GeoJSON Feature for a single tier location"""

    type: str = Field("Feature", description="GeoJSON type")
    geometry: Dict[str, Any] = Field(
        ..., description="GeoJSON geometry (Point, Polygon, etc.)"
    )
    properties: Dict[str, Any] = Field(
        ..., description="Location metadata including tier_level"
    )


class TierMapResponse(BaseModel):
    """GeoJSON FeatureCollection for tier visualization"""

    type: str = Field("FeatureCollection", description="GeoJSON type")
    features: List[TierMapFeature] = Field(
        ..., description="Array of location features"
    )
    metadata: Dict[str, Any] = Field(..., description="Scenario, tier, and count info")


# Database connection dependency (set by main.py)
db_pool = None


def set_db_pool(pool):
    global db_pool
    db_pool = pool


async def get_db():
    if db_pool is None:
        raise HTTPException(status_code=500, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection


@router.get("/scenarios", summary="List available scenarios")
async def get_available_scenarios(
    response: Response,
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get list of scenarios that have tier map data available.

    **Use case:** Build scenario selector for map visualization.

    Returns count of tiers and locations available for each scenario.

    Filters to active scenarios only (`tier_result.is_active = TRUE`). Retired
    scenario rows still present in `tier_location_result` are excluded via the
    join, so API consumers never have to filter the active set themselves.
    """
    try:
        # INNER JOIN tier_result so retired scenarios (e.g. s0029) are not
        # surfaced even though their tier_location_result rows remain in the
        # database.tier_location_result has no is_active column, so
        # tier_result.is_active is the authoritative flag across the tier
        # surface.
        query = """
        SELECT
            tlr.scenario_short_code,
            COUNT(DISTINCT tlr.tier_short_code) as tier_count,
            COUNT(*) as location_count
        FROM tier_location_result tlr
        JOIN tier_result tr
          ON tr.scenario_short_code = tlr.scenario_short_code
         AND tr.tier_short_code = tlr.tier_short_code
         AND tr.tier_version_id = tlr.tier_version_id
         AND tr.is_active = TRUE
        GROUP BY tlr.scenario_short_code
        ORDER BY tlr.scenario_short_code
        """

        rows = await connection.fetch(query)

        scenarios = [
            {
                "scenario_code": row["scenario_short_code"],
                "tier_count": row["tier_count"],
                "location_count": row["location_count"],
            }
            for row in rows
        ]

        response.headers["Cache-Control"] = STATIC_CATALOG_CACHE_CONTROL
        return {"scenarios": scenarios, "total": len(scenarios)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/tiers", summary="List available tier indicators")
async def get_available_tiers(
    response: Response,
    scenario_short_code: Optional[str] = Query(
        None, description="Filter by scenario (e.g., 's0020')"
    ),
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get list of tier indicators available for map visualization.

    **Use case:** Build tier selector for map visualization.

    Optionally filter by scenario to see only tiers with data for that scenario.

    **Example:** `GET /api/tier-map/tiers?scenario_short_code=s0020`
    """
    try:
        if scenario_short_code:
            # Join tier_result so deactivated scenarios (e.g. retired s0029)
            # are not surfaced.tier_location_result has no is_active column,
            # so we rely on tier_result.is_active as the authoritative flag.
            query = """
            SELECT DISTINCT 
                td.short_code,
                td.name,
                td.description,
                td.tier_type,
                td.tier_count,
                COUNT(tlr.id) as location_count
            FROM tier_definition td
            JOIN tier_location_result tlr ON td.short_code = tlr.tier_short_code
            JOIN tier_result tr
              ON tr.scenario_short_code = tlr.scenario_short_code
             AND tr.tier_short_code = tlr.tier_short_code
             AND tr.tier_version_id = tlr.tier_version_id
             AND tr.is_active = TRUE
            WHERE tlr.scenario_short_code = $1
            AND td.is_active = TRUE
            GROUP BY td.short_code, td.name, td.description, td.tier_type, td.tier_count
            ORDER BY td.tier_type DESC, td.short_code
            """
            rows = await connection.fetch(query, scenario_short_code)
        else:
            query = """
            SELECT 
                short_code,
                name,
                description,
                tier_type,
                tier_count
            FROM tier_definition
            WHERE is_active = TRUE
            ORDER BY tier_type DESC, short_code
            """
            rows = await connection.fetch(query)

        tiers = []
        for row in rows:
            tier_data = {
                "tier_code": row["short_code"],
                "tier_name": row["name"],
                "description": row["description"] or "",
                "tier_type": row["tier_type"],
                "tier_count": row["tier_count"],
            }
            if scenario_short_code:
                tier_data["location_count"] = row["location_count"]
            tiers.append(tier_data)

        response.headers["Cache-Control"] = STATIC_CATALOG_CACHE_CONTROL
        return {"tiers": tiers, "total": len(tiers)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/summary/{scenario_short_code}", summary="Get scenario tier summary")
async def get_scenario_tier_summary(
    scenario_short_code: str, connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get summary of all tier indicators for a specific scenario.

    **Use case:** Display tier selector with location counts.

    Returns each tier's metadata plus count of locations with tier data.

    **Example:** `GET /api/tier-map/summary/s0020`
    """
    try:
        # Join tier_result so the summary returns 404 for retired scenarios
        # (e.g. s0029) whose tier_location_result rows still exist but are
        # flagged inactive at the tier_result level.
        query = """
        SELECT 
            td.short_code,
            td.name,
            td.description,
            td.tier_type,
            td.tier_count,
            COUNT(tlr.id) as location_count,
            COUNT(DISTINCT tlr.tier_level) as tier_levels_used
        FROM tier_definition td
        JOIN tier_location_result tlr ON td.short_code = tlr.tier_short_code
        JOIN tier_result tr
          ON tr.scenario_short_code = tlr.scenario_short_code
         AND tr.tier_short_code = tlr.tier_short_code
         AND tr.tier_version_id = tlr.tier_version_id
         AND tr.is_active = TRUE
        WHERE tlr.scenario_short_code = $1
        AND td.is_active = TRUE
        GROUP BY td.short_code, td.name, td.description, td.tier_type, td.tier_count
        ORDER BY td.tier_type DESC, td.short_code
        """

        rows = await connection.fetch(query, scenario_short_code)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tier data found for scenario '{scenario_short_code}'",
            )

        tiers = [
            {
                "tier_code": row["short_code"],
                "tier_name": row["name"],
                "description": row["description"] or "",
                "tier_type": row["tier_type"],
                "tier_count": row["tier_count"],
                "location_count": row["location_count"],
                "tier_levels_used": row["tier_levels_used"],
            }
            for row in rows
        ]

        return {
            "scenario": scenario_short_code,
            "tiers": tiers,
            "total_tiers": len(tiers),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# =============================================================================
# LOCATIONS ENDPOINTS (Return data even without geometry)
#
# Route ordering: the batch variant `/{scenario}/locations` is declared BEFORE
# the single-tier variant `/{scenario}/{tier}/locations` so FastAPI matches
# the literal "locations" segment instead of binding "locations" to the
# {tier_short_code} parameter of the older route.
# =============================================================================


@router.get(
    "/{scenario_short_code}/locations",
    summary="Get tier locations for multiple outcomes (batch, no geometry)",
)
async def get_tier_locations_batch(
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
    Get per-location tier assignments for multiple outcomes in a single request.

    Additive counterpart to `/{scenario_short_code}/{tier_short_code}/locations`
    (the single-tier route still works unchanged). One SQL query replaces N
    parallel calls when a panel needs several outcomes at once (e.g. an equity
    heatmap showing all nine outcomes for one scenario).

    **Use case:** Batched per-location data for multi-outcome panels.

    **Example:** `GET /api/tier-map/s0020/locations?codes=CWS_DEL,AG_REV,ENV_FLOWS`

    **Response:**
    ```json
    {
      "scenario": "s0020",
      "results": {
        "CWS_DEL": { "scenario": "s0020", "tier_code": "CWS_DEL", "tier_name": ..., "tier_type": ..., "locations": [...], "metadata": {...} },
        "AG_REV":  { "scenario": "s0020", "tier_code": "AG_REV",  ... },
        "ENV_FLOWS": { ... }
      },
      "missing": []
    }
    ```

    Each per-code entry matches the shape of the single-tier `/locations`
    endpoint, so callers can reuse the same parsing code. `missing` lists codes
    the client requested that have no active rows for this scenario (this is a
    normal case, for example `WRC_SALMON_AB` on `s0065`).

    Filters `tier_result.is_active = TRUE`, so retired scenarios and retired
    tier versions are never surfaced.
    """
    # Parse and lightly validate the codes list. Deduplicate while preserving
    # request order, upper-case for tolerance, reject empty / malformed lists.
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
        # requested code dumped into `missing`. Cheap indexed lookup; zero
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

        # Single query: same join shape as the single-tier endpoint but with
        # tier_short_code = ANY($2). tier_result.is_active = TRUE is the
        # authoritative active-set filter across the tier surface.
        query = """
        SELECT
            tlr.tier_short_code,
            tlr.location_type,
            tlr.location_id,
            tlr.location_name,
            tlr.tier_level,
            tlr.tier_value,
            tlr.display_order,
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

        # Bucket rows by tier_short_code. Keys are only created for codes that
        # actually returned rows; codes with no active data land in `missing`.
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
                    "display_order": row["display_order"],
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
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/{scenario_short_code}/{tier_short_code}/locations",
    summary="Get tier locations (no geometry)",
)
async def get_tier_locations(
    scenario_short_code: str,
    tier_short_code: str,
    connection: asyncpg.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get tier location data WITHOUT geometries.

    **Use case:** For CWS_DEL and AG_REV where the frontend matches
    location_id to existing Mapbox layer features.

    **Example:** `GET /api/tier-map/s0020/CWS_DEL/locations`

    **Response:**
    ```json
    {
      "scenario": "s0020",
      "tier_code": "CWS_DEL",
      "tier_name": "Community water system deliveries",
      "tier_type": "multi_value",
      "locations": [
        {"location_id": "26S_PU4", "tier_level": 2, ...},
        {"location_id": "73_NU", "tier_level": 1, ...}
      ],
      "metadata": {
        "total_locations": 91,
        "tier_counts": {"1": 87, "2": 1, "3": 0, "4": 3}
      }
    }
    ```

    **Tier Indicators using this endpoint:**
    - CWS_DEL (91 urban demand units)
    - AG_REV (132 agricultural demand units)
    """
    try:
        # Join tier_result so retired scenarios (e.g. s0029) return 404
        # instead of leaking their still-present tier_location_result rows.
        query = """
        SELECT 
            tlr.location_type,
            tlr.location_id,
            tlr.location_name,
            tlr.tier_level,
            tlr.tier_value,
            tlr.display_order,
            td.name as tier_name,
            td.tier_type
        FROM tier_location_result tlr
        JOIN tier_definition td ON tlr.tier_short_code = td.short_code
        JOIN tier_result tr
          ON tr.scenario_short_code = tlr.scenario_short_code
         AND tr.tier_short_code = tlr.tier_short_code
         AND tr.tier_version_id = tlr.tier_version_id
         AND tr.is_active = TRUE
        WHERE tlr.scenario_short_code = $1
        AND tlr.tier_short_code = $2
        ORDER BY tlr.display_order, tlr.location_name
        """

        rows = await connection.fetch(query, scenario_short_code, tier_short_code)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tier data found for scenario '{scenario_short_code}' and tier '{tier_short_code}'",
            )

        # Build locations array
        locations = []
        tier_name = None
        tier_type = None

        for row in rows:
            if not tier_name:
                tier_name = row["tier_name"]
                tier_type = row["tier_type"]

            locations.append(
                {
                    "location_id": row["location_id"],
                    "location_name": row["location_name"],
                    "location_type": row["location_type"],
                    "tier_level": row["tier_level"],
                    "tier_value": row["tier_value"],
                    "display_order": row["display_order"],
                }
            )

        # Count by tier level
        tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
        for loc in locations:
            if loc["tier_level"] in tier_counts:
                tier_counts[loc["tier_level"]] += 1

        return {
            "scenario": scenario_short_code,
            "tier_code": tier_short_code,
            "tier_name": tier_name,
            "tier_type": tier_type,
            "locations": locations,
            "metadata": {
                "total_locations": len(locations),
                "location_types": list(set(row["location_type"] for row in rows)),
                "tier_counts": tier_counts,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# =============================================================================
# GEOJSON MAP ENDPOINT (MUST COME LAST - catch-all route)
# =============================================================================


@router.get(
    "/{scenario_short_code}/{tier_short_code}",
    summary="Get tier map GeoJSON",
    response_model=TierMapResponse,
)
async def get_tier_map_data(
    scenario_short_code: str,
    tier_short_code: str,
    connection: asyncpg.Connection = Depends(get_db),
) -> TierMapResponse:
    """
    Get GeoJSON FeatureCollection for map visualization.

    **Status:** general-purpose. The main web app still pairs the
    lightweight `/locations` endpoint with Mapbox vector tiles for
    polygons, but server-rendered GeoJSON is now wired for every tier
    `location_type` whose entity table carries geometry: `network_node`
    resolves through `network_gis.short_code`; `demand_unit` resolves
    through the `du_*_entity.du_id` table picked by the tier
    (`du_urban_entity` for CWS_DEL, `du_agriculture_entity` for AG_REV,
    matching `LOCATION_ENTITY_MAP['demand_unit']` plus
    `TIER_GEOMETRY_OVERRIDES`); `reservoir` through
    `reservoir.calsim_short_code` (with `SLUIS_CVP`/`SLUIS_SWP` aliased
    to `SLUIS`); `wba` through `wba.wba_id`; and `compliance_station`
    through `compliance_station.station_code`. Coverage tracks the
    entity registry in `etl/common/tier_location_entities.py`; run
    `etl/tier_data/scripts/audit_tier_location_geometry.py` for the per-tier
    scorecard. There is no fallback for `demand_unit`s without a
    polygon. They are dropped from the FeatureCollection and listed in
    `docs/du_geometry_gap.md`.

    **Use case:** Render tier outcomes on a map with colored markers/polygons.

    **Example:** `GET /api/tier-map/s0020/RES_STOR`

    **Response:** Standard GeoJSON FeatureCollection
    ```json
    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": {"type": "Polygon", "coordinates": [...]},
          "properties": {
            "location_id": "SHSTA",
            "location_name": "Shasta",
            "tier_level": 2,
            "tier_color_class": "tier-2"
          }
        }
      ],
      "metadata": {
        "scenario": "s0020",
        "tier_code": "RES_STOR",
        "tier_name": "Reservoir storage",
        "feature_count": 8
      }
    }
    ```

    **Tier Indicators using this endpoint:**
    - RES_STOR (8 reservoirs)
    - GW_STOR (42 aquifers)
    - ENV_FLOWS (17 compliance nodes)
    - DELTA_ECO, FW_DELTA_USES, FW_EXP (region polygons)

    **Note:** For CWS_DEL and AG_REV, use `/locations` endpoint instead.
    """
    try:
        # Step 1: Fetch tier locations without geometry so we know which tables to query.
        # This avoids referencing geometry tables (reservoirs, wba, compliance_stations)
        # that may not yet be populated.PostgreSQL would fail at query planning time
        # even for CASE branches that are never executed.
        # Join tier_result so retired scenarios (e.g. s0029) return 404
        # instead of leaking GeoJSON for still-present but inactive rows.
        base_query = """
            SELECT
                tlr.location_type,
                tlr.location_id,
                tlr.location_name,
                tlr.tier_level,
                tlr.tier_value,
                tlr.display_order,
                td.name  AS tier_name,
                td.tier_type
            FROM tier_location_result tlr
            JOIN tier_definition td ON tlr.tier_short_code = td.short_code
            JOIN tier_result tr
              ON tr.scenario_short_code = tlr.scenario_short_code
             AND tr.tier_short_code = tlr.tier_short_code
             AND tr.tier_version_id = tlr.tier_version_id
             AND tr.is_active = TRUE
            WHERE tlr.scenario_short_code = $1
              AND tlr.tier_short_code = $2
            ORDER BY tlr.display_order, tlr.location_name
        """
        base_rows = await connection.fetch(base_query, scenario_short_code, tier_short_code)

        if not base_rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tier data found for scenario '{scenario_short_code}' and tier '{tier_short_code}'",
            )

        location_types = {row["location_type"] for row in base_rows}

        # Step 2: Fetch geometries per location type, only for tables that exist.
        # geometry_map: location_id -> GeoJSON geometry dict
        geometry_map: Dict[str, Any] = {}

        if "network_node" in location_types:
            # network_gis carries one short_code column matching location_id.
            # DISTINCT ON deduplicates multiple rows per short_code (e.g. different
            # precision levels), preferring 'precise' entries.
            node_ids = [
                row["location_id"]
                for row in base_rows
                if row["location_type"] == "network_node"
            ]
            geo_rows = await connection.fetch(
                """
                SELECT DISTINCT ON (short_code)
                    short_code,
                    ST_AsGeoJSON(geom)::jsonb AS geometry
                FROM network_gis
                WHERE short_code = ANY($1::text[])
                  AND geom IS NOT NULL
                ORDER BY short_code,
                         (precision_level = 'precise') DESC
                """,
                node_ids,
            )
            for geo_row in geo_rows:
                geometry_map[geo_row["short_code"]] = geo_row["geometry"]

        # Demand-unit polygons. The table is tier-routed: CWS_DEL uses
        # du_urban_entity, AG_REV uses du_agriculture_entity, matching
        # `LOCATION_ENTITY_MAP['demand_unit']` plus `TIER_GEOMETRY_OVERRIDES`
        # in etl/common/tier_location_entities.py. `26N_NA` is the one
        # du_id present in both urban and ag entity tables. The loader
        # writes the same polygon to both rows so either resolver returns
        # the same geometry. 54 du_ids have no polygon in the source
        # gpkg. They are dropped from the FeatureCollection. See
        # docs/du_geometry_gap.md.
        if "demand_unit" in location_types:
            du_ids = [
                row["location_id"]
                for row in base_rows
                if row["location_type"] == "demand_unit"
            ]
            du_table = (
                "du_agriculture_entity"
                if tier_short_code == "AG_REV"
                else "du_urban_entity"
            )
            geo_rows = await connection.fetch(
                f"""
                SELECT du_id, ST_AsGeoJSON(geom)::jsonb AS geometry
                FROM {du_table}
                WHERE du_id = ANY($1::text[])
                  AND geom IS NOT NULL
                """,
                du_ids,
            )
            for geo_row in geo_rows:
                geometry_map[geo_row["du_id"]] = geo_row["geometry"]

        # Reservoir polygons (RES_STOR). The legacy `reservoir` table carries
        # one polygon row per `calsim_short_code`. SLUIS_CVP and SLUIS_SWP
        # both render against the shared SLUIS polygon; the alias map below
        # mirrors `LOCATION_ENTITY_MAP['reservoir'].geometry.id_aliases` in
        # etl/common/tier_location_entities.py.
        if "reservoir" in location_types:
            alias_to_originals: Dict[str, List[str]] = {}
            for row in base_rows:
                if row["location_type"] != "reservoir":
                    continue
                lid = row["location_id"]
                target = "SLUIS" if lid in ("SLUIS_CVP", "SLUIS_SWP") else lid
                alias_to_originals.setdefault(target, []).append(lid)
            geo_rows = await connection.fetch(
                """
                SELECT calsim_short_code, ST_AsGeoJSON(geom)::jsonb AS geometry
                FROM reservoir
                WHERE calsim_short_code = ANY($1::text[])
                  AND geom IS NOT NULL
                """,
                list(alias_to_originals),
            )
            for geo_row in geo_rows:
                for original in alias_to_originals.get(geo_row["calsim_short_code"], []):
                    geometry_map[original] = geo_row["geometry"]

        # WBA polygons (GW_STOR, DELTA_ECO via the DETAW row).
        if "wba" in location_types:
            wba_ids = [
                row["location_id"]
                for row in base_rows
                if row["location_type"] == "wba"
            ]
            geo_rows = await connection.fetch(
                """
                SELECT wba_id, ST_AsGeoJSON(geom)::jsonb AS geometry
                FROM wba
                WHERE wba_id = ANY($1::text[])
                  AND geom IS NOT NULL
                """,
                wba_ids,
            )
            for geo_row in geo_rows:
                geometry_map[geo_row["wba_id"]] = geo_row["geometry"]

        # Compliance-station points (FW_DELTA_USES).
        if "compliance_station" in location_types:
            station_ids = [
                row["location_id"]
                for row in base_rows
                if row["location_type"] == "compliance_station"
            ]
            geo_rows = await connection.fetch(
                """
                SELECT station_code, ST_AsGeoJSON(geom)::jsonb AS geometry
                FROM compliance_station
                WHERE station_code = ANY($1::text[])
                  AND geom IS NOT NULL
                """,
                station_ids,
            )
            for geo_row in geo_rows:
                geometry_map[geo_row["station_code"]] = geo_row["geometry"]

        # Step 3: Assemble GeoJSON features
        _type_display = {
            "reservoir": "Reservoir",
            "wba": "Aquifer",
            "region": "Region",
            "compliance_station": "Compliance Station",
            "network_node": "Environmental Flow",
            "demand_unit": "Demand Unit",
        }

        features = []
        tier_name = None
        tier_type = None

        for row in base_rows:
            geom = geometry_map.get(row["location_id"])
            if not geom:
                continue

            if not tier_name:
                tier_name = row["tier_name"]
                tier_type = row["tier_type"]

            if isinstance(geom, str):
                geom = json.loads(geom)

            loc_type = row["location_type"]
            properties = {
                "location_id": row["location_id"],
                "location_name": row["location_name"],
                "location_type": loc_type,
                "location_type_display": _type_display.get(loc_type, loc_type),
                "tier_level": row["tier_level"],
                "tier_value": row["tier_value"],
                "display_order": row["display_order"],
                "tier_color_class": f"tier-{row['tier_level']}",
            }
            features.append(
                TierMapFeature(type="Feature", geometry=geom, properties=properties)
            )

        if not features:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No geometry available for tier '{tier_short_code}'. "
                    f"Location types present: {sorted(location_types)}. "
                    "Required GIS tables may not be populated yet."
                ),
            )

        metadata = {
            "scenario": scenario_short_code,
            "tier_code": tier_short_code,
            "tier_name": tier_name,
            "tier_type": tier_type,
            "feature_count": len(features),
            "location_types": sorted(location_types),
        }

        return TierMapResponse(
            type="FeatureCollection", features=features, metadata=metadata
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
