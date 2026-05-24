"""du_gpkg_id_resolution.py - Maps entity-table `du_id` values to GeoPackage `DU_ID` keys.

The geopackage (`database/seed_tables/03_GIS/du_4326.gpkg`) uses
sub-area suffixes and slightly different stems than the entity tables and
tier staging CSVs. This module holds the explicit alias and dissolve rules
documented in `docs/du_polygon_mapping.md`.

Pattern A (alias): one entity id maps to one gpkg id with a different name.
Pattern B (dissolve): one entity id maps to several gpkg sub-area polygons
that are unioned at load time via PostGIS `ST_Union`.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

# Pattern A: entity du_id -> single gpkg DU_ID
GPKG_ALIAS: Dict[str, str] = {
    "60N_PU1": "60N_PU",
    "90_PU5": "90_PU",
    "72_PU": "72_PU2",
}

# Pattern B: entity du_id -> gpkg DU_IDs to ST_Union
GPKG_DISSOLVE: Dict[str, List[str]] = {
    "60S_PA": ["60S_PA1", "60S_PA2"],
    "61_PA": ["61_PA1", "61_PA2", "61_PA3"],
    "63_PR": ["63_PR1", "63_PR2", "63_PR3"],
    "64_PA": ["64_PA1", "64_PA2", "64_PA3"],
    "71_PA": [
        "71_PA1",
        "71_PA2",
        "71_PA3",
        "71_PA4",
        "71_PA5",
        "71_PA6",
        "71_PA7",
        "71_PA8",
    ],
}


def gpkg_sources_for_entity(du_id: str, gpkg_ids: Set[str]) -> Optional[List[str]]:
    """Return the gpkg `DU_ID`(s) that supply geometry for one entity `du_id`.

    Returns `None` when no polygon source exists in the gpkg for this entity id.
    """
    if du_id in GPKG_DISSOLVE:
        sources = [s for s in GPKG_DISSOLVE[du_id] if s in gpkg_ids]
        if len(sources) == len(GPKG_DISSOLVE[du_id]):
            return sources
        return None
    if du_id in GPKG_ALIAS:
        target = GPKG_ALIAS[du_id]
        return [target] if target in gpkg_ids else None
    return [du_id] if du_id in gpkg_ids else None


def plan_entity_gpkg_sources(
    entity_ids: Dict[str, Set[str]],
    gpkg_ids: Set[str],
) -> Tuple[Dict[str, List[str]], Dict[Tuple[str, str], List[str]], Set[str]]:
    """Plan which entity rows receive polygons and which gpkg ids they consume.

    Returns:
        updates_by_table: {table: [entity_du_id, ...]}
        source_map: {(table, entity_du_id): [gpkg_du_id, ...]}
        gpkg_ids_consumed: set of gpkg DU_IDs referenced by the plan
    """
    updates_by_table: Dict[str, List[str]] = {}
    source_map: Dict[Tuple[str, str], List[str]] = {}
    gpkg_ids_consumed: Set[str] = set()

    for table, ids in entity_ids.items():
        matched: List[str] = []
        for du_id in sorted(ids):
            sources = gpkg_sources_for_entity(du_id, gpkg_ids)
            if sources is None:
                continue
            matched.append(du_id)
            source_map[(table, du_id)] = sources
            gpkg_ids_consumed.update(sources)
        if matched:
            updates_by_table[table] = matched

    return updates_by_table, source_map, gpkg_ids_consumed
