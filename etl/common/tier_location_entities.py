"""tier_location_entities.py - Registry of attribute and geometry resolution paths per tier `location_type`.

Single source of truth for "where does the display name live", "where does
the geometry live", and "what is the join key" for every tier location
type the public API and ETL consumers need to resolve.

Used by:
  - `etl/tier_data/scripts/audit_tier_location_geometry.py` (coverage scorecard)
  - `etl/tier_data/scripts/sync_tier_locations_from_staging.py` (membership validation)
  - `etl/tier_data/scripts/load_all_tier_results.py` (display-name lookup at load time)
  - `database/scripts/data_processing/load_du_geometries.py` (one-shot
    DU polygon writer; routes by `du_id` presence in each entity table,
    matching this registry)
  - `etl/tier_data/scripts/verify_tiers.py` (catalog gating)
  - `api/coeqwal-api/routes/tier_map_endpoints.py` mirrors the same map
    in SQL when assembling GeoJSON FeatureCollections

A `location_type` may resolve to one attribute table for names but a
different table for geometry (e.g. `reservoir` names come from
`reservoir_entity`, polygons from `reservoir`). Use the `attribute` and
`geometry` blocks to drive each lookup independently.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class GeometryResolver:
    """Where geometry lives in PostGIS for a given `location_type`.

    `table`/`id_column` participate in `SELECT ST_AsGeoJSON(geom) FROM
    <table> WHERE <id_column> = ANY($1)`. `geom_column` defaults to `geom`
    (the PostGIS geometry column convention used throughout the schema).

    `id_aliases` maps a catalog `location_id` to the row key the geometry
    table actually carries. Today only RES_STOR uses this: `SLUIS_CVP`
    and `SLUIS_SWP` both render against the single `SLUIS` reservoir
    polygon.
    """

    table: str
    id_column: str
    geom_column: str = "geom"
    geom_kind: str = "geometry"
    id_aliases: Dict[str, str] = field(default_factory=dict)
    notes: str = ""


@dataclass(frozen=True)
class AttributeResolver:
    """Where the display name and other attributes live for a `location_type`.

    `table`/`id_column` participate in `SELECT <name_column> FROM <table>
    WHERE <id_column> = $1`. Membership validation (sync script) uses the
    same query to confirm the staging `location_id` resolves.
    """

    table: str
    id_column: str
    name_column: str
    notes: str = ""


@dataclass(frozen=True)
class TierLocationEntity:
    """Per-`location_type` resolution: attribute table + geometry table."""

    location_type: str
    attribute: AttributeResolver
    geometry: Optional[GeometryResolver]
    description: str = ""


# Authoritative registry. Update when a new `location_type` is added to the
# tier_location CHECK constraint, or when an attribute/geometry path changes.
LOCATION_ENTITY_MAP: Dict[str, TierLocationEntity] = {
    "network_node": TierLocationEntity(
        location_type="network_node",
        attribute=AttributeResolver(
            table="network",
            id_column="short_code",
            name_column="name",
            notes="network_node.short_code mirrors network.short_code; both work as the join key.",
        ),
        geometry=GeometryResolver(
            table="network_gis",
            id_column="short_code",
            geom_kind="POINT",
            notes="DISTINCT ON (short_code) ... ORDER BY (precision_level = 'precise') DESC.",
        ),
        description="ENV_FLOWS stream gauges, FW_EXP pump stations, WRC_SALMON_AB.",
    ),
    "demand_unit": TierLocationEntity(
        location_type="demand_unit",
        attribute=AttributeResolver(
            table="du_urban_entity",
            id_column="du_id",
            name_column="du_id",
            notes=(
                "Default attribute table for `demand_unit` (urban). "
                "AG_REV resolves through `du_agriculture_entity` via TIER_ATTRIBUTE_OVERRIDES; "
                "a future refuge tier would add a `du_refuge_entity` override the same way. "
                "Live RDS has no name column distinct from `du_id` today."
            ),
        ),
        geometry=GeometryResolver(
            table="du_urban_entity",
            id_column="du_id",
            geom_kind="MULTIPOLYGON",
            notes=(
                "Default geometry table for `demand_unit` (urban). "
                "AG_REV polygons resolve through `du_agriculture_entity` via TIER_GEOMETRY_OVERRIDES; "
                "a future refuge tier would add a `du_refuge_entity` override the same way. "
                "Polygons load from "
                "`database/seed_tables/03_GIS/du_4326.gpkg` via "
                "`database/scripts/data_processing/load_du_geometries.py`. "
                "54 `du_id`s have no polygon in the source file. "
                "See `docs/du_geometry_gap.md`."
            ),
        ),
        description="CWS_DEL urban DUs, AG_REV agricultural DUs.",
    ),
    "reservoir": TierLocationEntity(
        location_type="reservoir",
        attribute=AttributeResolver(
            table="reservoir_entity",
            id_column="short_code",
            name_column="name",
            notes="reservoir_entity has 92 rows. Display name comes from .name; reservoir.reservoir_name is the polygon table label.",
        ),
        geometry=GeometryResolver(
            table="reservoir",
            id_column="calsim_short_code",
            geom_kind="POLYGON",
            id_aliases={
                "SLUIS_CVP": "SLUIS",
                "SLUIS_SWP": "SLUIS",
            },
            notes=(
                "Legacy reservoir table has 7 polygon rows. SLUIS_CVP/SLUIS_SWP both render against the shared SLUIS polygon. "
                "For dam-point geometry instead of polygons, fall back to network_gis.short_code matching."
            ),
        ),
        description="RES_STOR reservoirs.",
    ),
    "wba": TierLocationEntity(
        location_type="wba",
        attribute=AttributeResolver(
            table="wba",
            id_column="wba_id",
            name_column="wba_name",
        ),
        geometry=GeometryResolver(
            table="wba",
            id_column="wba_id",
            geom_kind="POLYGON",
        ),
        description="GW_STOR water-budget areas. DELTA_ECO uses the DETAW row.",
    ),
    "compliance_station": TierLocationEntity(
        location_type="compliance_station",
        attribute=AttributeResolver(
            table="compliance_station",
            id_column="station_code",
            name_column="station_name",
        ),
        geometry=GeometryResolver(
            table="compliance_station",
            id_column="station_code",
            geom_kind="POINT",
            notes="2 rows: Emmaton (EM), Jersey Point (JP). Seeded from database/seed_tables/03_GIS/compliance_stations.csv.",
        ),
        description="FW_DELTA_USES Delta compliance stations.",
    ),
    "region": TierLocationEntity(
        location_type="region",
        attribute=AttributeResolver(
            table="hydrologic_region",
            id_column="short_code",
            name_column="label",
            notes="No tier today uses location_type='region'; placeholder so the CHECK constraint enum stays documented.",
        ),
        geometry=None,
        description="Reserved for region-level tier outcomes.",
    ),
}


# Per-tier attribute resolver overrides. Keyed by `tier_short_code`, takes
# precedence over `LOCATION_ENTITY_MAP[location_type].attribute` when present.
# Today only AG_REV needs an override (ag DU ids live in
# `du_agriculture_entity`, not `du_urban_entity`). A future refuge tier
# would add `"REFUGE_DEL": AttributeResolver(table="du_refuge_entity", ...)`.
TIER_ATTRIBUTE_OVERRIDES: Dict[str, AttributeResolver] = {
    "AG_REV": AttributeResolver(
        table="du_agriculture_entity",
        id_column="du_id",
        name_column="du_id",
        notes=(
            "Agricultural DUs. Same shape as du_urban_entity; disjoint id sets "
            "(_PA/_SA/_NA/_PR suffixes vs _PU/_NU/_SU)."
        ),
    ),
}


# Per-tier geometry resolver overrides. Same shape as TIER_ATTRIBUTE_OVERRIDES
# but for the polygon table. AG_REV polygons live in `du_agriculture_entity.geom`,
# CWS_DEL polygons live in `du_urban_entity.geom` (the registry default).
# A future refuge tier would add `"REFUGE_DEL": GeometryResolver(table="du_refuge_entity", ...)`.
# `26N_NA` is the one `du_id` that exists in both the urban and ag entity
# tables. The loader writes the same dissolved polygon to both rows, so the
# tier-routed lookup returns the same geometry regardless of which override fires.
TIER_GEOMETRY_OVERRIDES: Dict[str, "GeometryResolver"] = {
    "AG_REV": GeometryResolver(
        table="du_agriculture_entity",
        id_column="du_id",
        geom_kind="MULTIPOLYGON",
        notes="Agricultural DU polygons (EPSG:4326).",
    ),
}


def attribute_resolver_for(tier_short_code: str, location_type: str) -> AttributeResolver:
    """Return the `AttributeResolver` for one (tier, location_type) pair.

    Falls through to `LOCATION_ENTITY_MAP[location_type].attribute` when the
    tier has no entry in `TIER_ATTRIBUTE_OVERRIDES`. Raises `KeyError` if
    `location_type` is not in the registry at all (a developer-side typo
    we want to surface, not silently default).
    """
    override = TIER_ATTRIBUTE_OVERRIDES.get(tier_short_code)
    if override is not None:
        return override
    return LOCATION_ENTITY_MAP[location_type].attribute


def geometry_resolver_for(
    tier_short_code: str, location_type: str
) -> Optional["GeometryResolver"]:
    """Return the `GeometryResolver` for one (tier, location_type) pair.

    Mirrors `attribute_resolver_for`: a tier-specific override in
    `TIER_GEOMETRY_OVERRIDES` wins, otherwise we fall through to
    `LOCATION_ENTITY_MAP[location_type].geometry`. Returns `None` when
    the registry entry has no geometry path (today: only `region`).
    Raises `KeyError` if `location_type` is not in the registry at all.
    """
    override = TIER_GEOMETRY_OVERRIDES.get(tier_short_code)
    if override is not None:
        return override
    return LOCATION_ENTITY_MAP[location_type].geometry


def fetch_tier_location_names(conn) -> Dict[str, Dict[str, str]]:
    """Return `{tier_short_code: {location_id: display_name}}` for every
    active row in `tier_location`, resolved via the entity registry.

    Uses `attribute_resolver_for(tier, location_type)` per row, so AG_REV
    DU names resolve against `du_agriculture_entity` while CWS_DEL ones
    resolve against `du_urban_entity`. Falls back to `location_id` when
    the entity row is missing or the resolver's `name_column` equals
    `id_column` (the case for `demand_unit` today).
    """
    out: Dict[str, Dict[str, str]] = {}
    catalog: List[tuple] = []
    # `(table, id_column, name_column)` keys collapse tiers that share a
    # resolver into one query (e.g. WBA / GW_STOR + DELTA_ECO).
    resolver_groups: Dict[Tuple[str, str, str], set] = {}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT tier_short_code, location_type, location_id "
            "FROM tier_location WHERE is_active = TRUE"
        )
        for tier, loc_type, loc_id in cur.fetchall():
            catalog.append((tier, loc_type, loc_id))
            out.setdefault(tier, {})[loc_id] = loc_id  # default; may overwrite below
            if loc_type not in LOCATION_ENTITY_MAP:
                continue
            resolver = attribute_resolver_for(tier, loc_type)
            if resolver.name_column == resolver.id_column:
                continue
            key = (resolver.table, resolver.id_column, resolver.name_column)
            resolver_groups.setdefault(key, set()).add(loc_id)

        resolved_by_group: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        for (table, id_column, name_column), ids in resolver_groups.items():
            cur.execute(
                f'SELECT "{id_column}", "{name_column}" FROM "{table}" '
                f'WHERE "{id_column}" = ANY(%s)',
                (sorted(ids),),
            )
            resolved_by_group[(table, id_column, name_column)] = {
                row[0]: row[1] or row[0] for row in cur.fetchall()
            }

    for tier, loc_type, loc_id in catalog:
        if loc_type not in LOCATION_ENTITY_MAP:
            continue
        resolver = attribute_resolver_for(tier, loc_type)
        if resolver.name_column == resolver.id_column:
            continue
        key = (resolver.table, resolver.id_column, resolver.name_column)
        name = resolved_by_group.get(key, {}).get(loc_id)
        if name:
            out[tier][loc_id] = name
    return out


def fetch_active_location_ids(conn) -> Dict[str, set]:
    """Return `{tier_short_code: {location_id}}` for active tier_location rows.

    Used by `verify_tiers.py` to derive `RES_STOR_LOCATION_IDS` and
    similar catalog-gating sets directly from the live DB.
    """
    out: Dict[str, set] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tier_short_code, location_id FROM tier_location WHERE is_active = TRUE"
        )
        for tier, lid in cur.fetchall():
            out.setdefault(tier, set()).add(lid)
    return out


@dataclass
class CoverageReport:
    """Per-`location_type` attribute + geometry hit / miss against the live DB.

    `attribute_missing` / `geometry_missing` carry the original catalog
    `location_id` values (not the geometry-table alias), so callers can
    cite the exact ids in alerts. `geometry_supported` is False for
    `location_type`s whose registry entry has no `geometry` resolver
    (today: only `region`); `geometry_missing` is empty in that case.
    """

    location_type: str
    ids_checked: List[str]
    attribute_missing: List[str]
    geometry_missing: List[str]
    geometry_supported: bool

    @property
    def attribute_resolved(self) -> int:
        return len(self.ids_checked) - len(self.attribute_missing)

    @property
    def geometry_resolved(self) -> int:
        if not self.geometry_supported:
            return 0
        return len(self.ids_checked) - len(self.geometry_missing)

    @property
    def has_gaps(self) -> bool:
        if self.attribute_missing:
            return True
        if self.geometry_supported and self.geometry_missing:
            return True
        return False


def assess_coverage(
    conn,
    tier_locations: Iterable[Tuple[str, str, str]],
) -> Dict[str, CoverageReport]:
    """Score attribute + geometry coverage per `location_type` against the live DB.

    `tier_locations` is an iterable of `(tier_short_code, location_type,
    location_id)` triples. Triples let both attribute and geometry
    lookups be tier-aware: AG_REV demand-unit ids query
    `du_agriculture_entity`, CWS_DEL ones query `du_urban_entity`, per
    `attribute_resolver_for` and `geometry_resolver_for`.

    Result is one `CoverageReport` per `location_type` seen in the
    input. `attribute_missing` / `geometry_missing` are unions across
    the underlying tier groupings; id suffixes already disambiguate
    which tier owns a miss (`_PA`/`_NA` for ag, `_PU`/`_NU` for urban).
    Applies any `id_aliases` from the geometry resolver
    (`SLUIS_CVP`/`SLUIS_SWP` -> `SLUIS`) so geometry_missing reports
    the original catalog ids.

    For a `location_type` whose registry entry has no geometry path
    (today: only `region`) the per-type report has
    `geometry_supported=False` and an empty `geometry_missing` list.
    """
    triples = [(t, lt, lid) for t, lt, lid in tier_locations if lid]

    # Per-`location_type` id set drives the final report shape. Attribute
    # and geometry queries are each grouped by (table, id_column) so
    # tiers sharing a resolver collapse into one round-trip. Triples
    # whose `location_type` is not in the registry surface as
    # fully-missing in the per-type report below (matches the previous
    # tolerant behavior so a developer-side typo in
    # `TIER_LOCATION_TYPE` is loud, not fatal).
    ids_by_type: Dict[str, set] = {}
    unregistered_types: set = set()
    attr_query_groups: Dict[Tuple[str, str], set] = {}
    # Geometry groups carry (table, id_column, geom_column) keys plus
    # the alias map of the underlying resolver. The alias map is
    # uniform across tiers that share a resolver (today only RES_STOR
    # uses one), so we keep a representative copy per group.
    geom_query_groups: Dict[Tuple[str, str, str], set] = {}
    geom_alias_by_group: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    geom_resolver_by_tier_type: Dict[Tuple[str, str], Optional[GeometryResolver]] = {}

    for tier, loc_type, lid in triples:
        ids_by_type.setdefault(loc_type, set()).add(lid)
        if loc_type not in LOCATION_ENTITY_MAP:
            unregistered_types.add(loc_type)
            continue
        attr = attribute_resolver_for(tier, loc_type)
        attr_query_groups.setdefault((attr.table, attr.id_column), set()).add(lid)

        geom = geometry_resolver_for(tier, loc_type)
        geom_resolver_by_tier_type[(tier, loc_type)] = geom
        if geom is None:
            continue
        key = (geom.table, geom.id_column, geom.geom_column)
        target_id = geom.id_aliases.get(lid, lid)
        geom_query_groups.setdefault(key, set()).add(target_id)
        geom_alias_by_group.setdefault(key, geom.id_aliases)

    out: Dict[str, CoverageReport] = {}
    if not ids_by_type:
        return out

    with conn.cursor() as cur:
        attr_hits: Dict[Tuple[str, str], set] = {}
        for (table, id_column), ids in attr_query_groups.items():
            cur.execute(
                f'SELECT "{id_column}" FROM "{table}" '
                f'WHERE "{id_column}" = ANY(%s)',
                (sorted(ids),),
            )
            attr_hits[(table, id_column)] = {row[0] for row in cur.fetchall()}

        attribute_missing_by_type: Dict[str, set] = {lt: set() for lt in ids_by_type}
        for tier, loc_type, lid in triples:
            if loc_type in unregistered_types:
                attribute_missing_by_type[loc_type].add(lid)
                continue
            resolver = attribute_resolver_for(tier, loc_type)
            if lid not in attr_hits.get((resolver.table, resolver.id_column), set()):
                attribute_missing_by_type[loc_type].add(lid)

        geom_hits: Dict[Tuple[str, str, str], set] = {}
        for key, target_ids in geom_query_groups.items():
            table, id_column, geom_column = key
            cur.execute(
                f'SELECT "{id_column}" FROM "{table}" '
                f'WHERE "{id_column}" = ANY(%s) '
                f'AND "{geom_column}" IS NOT NULL',
                (sorted(target_ids),),
            )
            geom_hits[key] = {row[0] for row in cur.fetchall()}

        geometry_missing_by_type: Dict[str, set] = {lt: set() for lt in ids_by_type}
        for tier, loc_type, lid in triples:
            if loc_type in unregistered_types:
                geometry_missing_by_type[loc_type].add(lid)
                continue
            geom = geom_resolver_by_tier_type.get((tier, loc_type))
            if geom is None:
                continue
            target = geom.id_aliases.get(lid, lid)
            if target not in geom_hits.get((geom.table, geom.id_column, geom.geom_column), set()):
                geometry_missing_by_type[loc_type].add(lid)

        for loc_type, ids in ids_by_type.items():
            id_list = sorted(ids)
            attr_missing = sorted(attribute_missing_by_type.get(loc_type, set()))
            entry = LOCATION_ENTITY_MAP.get(loc_type)

            if entry is None:
                out[loc_type] = CoverageReport(
                    location_type=loc_type,
                    ids_checked=id_list,
                    attribute_missing=list(id_list),
                    geometry_missing=list(id_list),
                    geometry_supported=False,
                )
                continue

            # `geometry_supported` is True when the registry default for
            # this location_type carries a geometry path. Per-tier
            # overrides only ever replace one resolver with another;
            # they cannot turn geometry on for a type whose default is
            # None (today: only `region`).
            if entry.geometry is None:
                out[loc_type] = CoverageReport(
                    location_type=loc_type,
                    ids_checked=id_list,
                    attribute_missing=attr_missing,
                    geometry_missing=[],
                    geometry_supported=False,
                )
                continue

            out[loc_type] = CoverageReport(
                location_type=loc_type,
                ids_checked=id_list,
                attribute_missing=attr_missing,
                geometry_missing=sorted(geometry_missing_by_type.get(loc_type, set())),
                geometry_supported=True,
            )
    return out


def _preview(ids: List[str], cap: int = 5) -> str:
    head = ", ".join(ids[:cap])
    tail = f" (+{len(ids) - cap} more)" if len(ids) > cap else ""
    return f"{head}{tail}"


def format_coverage_warnings(
    tier_locations: Iterable[Tuple[str, str, str]],
    reports: Dict[str, CoverageReport],
) -> List[str]:
    """Group missing attribute / geometry ids by tier. One WARNING line per tier.

    `tier_locations` is an iterable of `(tier_short_code,
    location_type, location_id)` tuples. Build it from staging
    inventory or from a `tier_location` catalog scan. Returns one
    "`WARNING: tier_location coverage gap in <tier>: ...`" line per
    tier with at least one miss. The line names the missing ids
    (truncated) and points the developer at
    `audit_tier_location_geometry.py --tier <tier>` for the full
    detail. Returns `[]` when coverage is clean.
    """
    by_type_attr: Dict[str, set] = {
        lt: set(r.attribute_missing) for lt, r in reports.items()
    }
    by_type_geom: Dict[str, set] = {
        lt: set(r.geometry_missing) if r.geometry_supported else set()
        for lt, r in reports.items()
    }

    missing_attr: Dict[str, List[str]] = {}
    missing_geom: Dict[str, List[str]] = {}
    seen_attr: Dict[str, set] = {}
    seen_geom: Dict[str, set] = {}

    for tier, loc_type, loc_id in tier_locations:
        if loc_id in by_type_attr.get(loc_type, set()):
            bucket = seen_attr.setdefault(tier, set())
            if loc_id not in bucket:
                bucket.add(loc_id)
                missing_attr.setdefault(tier, []).append(loc_id)
        if loc_id in by_type_geom.get(loc_type, set()):
            bucket = seen_geom.setdefault(tier, set())
            if loc_id not in bucket:
                bucket.add(loc_id)
                missing_geom.setdefault(tier, []).append(loc_id)

    lines: List[str] = []
    for tier in sorted(set(missing_attr) | set(missing_geom)):
        parts: List[str] = []
        if tier in missing_attr:
            ids = sorted(missing_attr[tier])
            parts.append(f"{len(ids)} missing attribute [{_preview(ids)}]")
        if tier in missing_geom:
            ids = sorted(missing_geom[tier])
            parts.append(f"{len(ids)} missing geometry [{_preview(ids)}]")
        lines.append(
            f"WARNING: tier_location coverage gap in {tier}: "
            f"{'; '.join(parts)}. "
            f"Run `python etl/tier_data/scripts/audit_tier_location_geometry.py --tier {tier}` for details."
        )
    return lines


def resolved_location_id(
    location_type: str,
    location_id: str,
    tier_short_code: Optional[str] = None,
) -> str:
    """Apply any `id_aliases` from the geometry resolver.

    Returns the geometry-table id for a given catalog id. For everything
    except SLUIS_CVP/SLUIS_SWP (which both alias to SLUIS), this is the
    identity.

    Pass `tier_short_code` to honor `TIER_GEOMETRY_OVERRIDES` (e.g.
    AG_REV demand units route to `du_agriculture_entity`). When omitted,
    the registry default geometry resolver is used.
    """
    if tier_short_code is not None and location_type in LOCATION_ENTITY_MAP:
        geom = geometry_resolver_for(tier_short_code, location_type)
    else:
        entry = LOCATION_ENTITY_MAP.get(location_type)
        geom = entry.geometry if entry is not None else None
    if geom is None:
        return location_id
    return geom.id_aliases.get(location_id, location_id)


def location_types() -> List[str]:
    return sorted(LOCATION_ENTITY_MAP)


__all__ = [
    "AttributeResolver",
    "CoverageReport",
    "GeometryResolver",
    "LOCATION_ENTITY_MAP",
    "TIER_ATTRIBUTE_OVERRIDES",
    "TIER_GEOMETRY_OVERRIDES",
    "TierLocationEntity",
    "assess_coverage",
    "attribute_resolver_for",
    "fetch_active_location_ids",
    "fetch_tier_location_names",
    "format_coverage_warnings",
    "geometry_resolver_for",
    "location_types",
    "resolved_location_id",
]
