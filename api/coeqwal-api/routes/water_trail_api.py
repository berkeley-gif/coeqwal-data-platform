"""
Water Trail API - Connect the dots approach for legible CalSim network visualization
Uses hardcoded water pathways based on actual network_topology.csv connectivity
Focuses on major reservoirs and their connections to Sacramento/San Joaquin river systems
"""

import asyncpg
from typing import List, Dict, Any, Set
import json

# HARDCODED CALIFORNIA WATER TRAILS based on actual network topology
# These represent the most important water infrastructure pathways

CALIFORNIA_WATER_TRAILS = {
    "shasta_sacramento": {
        "name": "Shasta Dam to Sacramento River System",
        "description": "Northern California's primary water source",
        "nodes": [
            "SHSTA", "SAC301", "KSWCK", "SAC299", "SAC296", "SAC294", "SAC289", 
            "SAC287", "SAC281", "SAC277", "SAC275", "SAC273", "SAC271", "SAC269",
            "SAC267", "SAC265", "SAC261", "SAC259", "SAC255", "SAC251", "SAC247",
            "SAC243", "SAC239", "SAC235", "SAC232", "SAC228", "SAC224", "SAC218",
            "SAC214", "SAC207", "SAC196", "SAC193", "SAC185", "SAC182", "SAC178",
            "SAC174", "SAC168", "SAC162", "SAC159", "SAC154", "SAC148", "SAC141",
            "SAC134", "SAC129", "SAC125", "SAC122", "SAC119", "SAC115", "SAC111",
            "SAC107", "SAC103", "SAC099", "SAC095", "SAC091", "SAC087", "SAC083",
            "SAC079", "SAC075", "SAC071", "SAC067", "SAC063", "SAC059", "SAC055",
            "SAC051", "SAC047", "SAC043", "SAC039", "SAC035", "SAC031", "SAC027",
            "SAC023", "SAC019", "SAC015", "SAC011", "SAC007", "SAC003", "SAC000"
        ],
        "key_infrastructure": ["SHSTA", "KSWCK", "SAC000"],
        "region": "SAC"
    },
    
    "oroville_feather": {
        "name": "Oroville Dam to Feather River to Sacramento",
        "description": "Central Valley water supply via Feather River",
        "nodes": [
            "OROVL", "FTR072", "FTR070", "FTR068", "FTR067", "FTR065", "FTR063",
            "FTR061", "FTR059", "FTR057", "FTR055", "FTR053", "FTR051", "FTR049",
            "FTR047", "FTR045", "FTR043", "FTR041", "FTR039", "FTR037", "FTR035",
            "FTR033", "FTR031", "FTR029", "FTR027", "FTR025", "FTR023", "FTR021",
            "FTR019", "FTR017", "FTR015", "FTR013", "FTR011", "FTR009", "FTR007",
            "FTR005", "FTR003", "SAC083"  # Feather joins Sacramento
        ],
        "key_infrastructure": ["OROVL", "FTR003", "SAC083"],
        "region": "SAC"
    },
    
    "folsom_american": {
        "name": "Folsom Lake to American River to Sacramento",
        "description": "American River system water supply",
        "nodes": [
            "FOLSM", "NTOMA", "AMR028", "AMR026", "AMR024", "AMR022", "AMR020",
            "AMR018", "AMR016", "AMR014", "AMR012", "AMR010", "AMR008", "AMR006",
            "AMR004", "AMR002", "SAC043"  # American joins Sacramento
        ],
        "key_infrastructure": ["FOLSM", "NTOMA", "SAC043"],
        "region": "SAC",
        "treatment_plants": ["WTPEDH", "WTPSJP", "WTPRSV", "WTPFOL"]
    },
    
    "san_luis_delta": {
        "name": "San Luis Reservoir to Delta System",
        "description": "Central Valley Project water distribution",
        "nodes": [
            "SLUIS", "SLUISC", "DMC", "CALA", "MENDOTA", "SJR070", "SJR062",
            "SJR056", "SJR053", "SJR048", "SJR043", "SJR042", "SJR038", "SJR033",
            "SJR028", "SJR026", "SJR023", "SJR022", "SJR013", "SJR009", "MDOTA",
            "SJRW", "SJRE"
        ],
        "key_infrastructure": ["SLUIS", "MENDOTA", "MDOTA", "SJRE"],
        "region": "SJR"
    },
    
    "hetch_hetchy_tuolumne": {
        "name": "Hetch Hetchy to Tuolumne River to San Joaquin",
        "description": "San Francisco water supply and Central Valley",
        "nodes": [
            "HETCH", "DMPDN", "EXCQR", "TULLK", "MODTO", "TUO054", "TUO040",
            "TUO026", "TUO009", "SJR070"  # Tuolumne joins San Joaquin
        ],
        "key_infrastructure": ["HETCH", "TULLK", "MODTO", "SJR070"],
        "region": "SJR"
    },
    
    "trinity_sacramento": {
        "name": "Trinity River to Sacramento via Whiskeytown",
        "description": "North Coast water transfer to Central Valley",
        "nodes": [
            "TRNTY", "WKYTN", "KSWCK", "SAC299"  # Trinity transfers to Sacramento system
        ],
        "key_infrastructure": ["TRNTY", "WKYTN", "KSWCK"],
        "region": "SAC"
    }
}

# Major pump stations and treatment facilities to always include
KEY_INFRASTRUCTURE_CODES = [
    # Major reservoirs
    "SHSTA", "OROVL", "FOLSM", "SLUIS", "HETCH", "TRNTY", "WKYTN",
    "AMADR", "MILLR", "DONPD", "MCCLR", "ENGLB", "CMPFW",
    
    # Key river junctions
    "SAC000", "SAC043", "SAC083", "SJRE", "SJRW", "MDOTA", "MENDOTA",
    "KSWCK", "NTOMA", "MODTO", "TULLK",
    
    # Major treatment plants
    "WTPEDH", "WTPSJP", "WTPRSV", "WTPFOL", "WTPJMS", "WTPSTK",
    
    # Key pump stations and diversions
    "CALA", "SLUISC", "DMPDN", "EXCQR"
]


async def get_water_trail_from_reservoir(
    db_pool: asyncpg.Pool,
    reservoir_short_code: str,
    trail_type: str = "infrastructure",
    max_depth: int = 6
) -> Dict[str, Any]:
    """
    Get a curated water trail from a major reservoir using hardcoded pathways
    Much more reliable than dynamic connectivity computation
    """
    
    # Find which trail this reservoir belongs to
    trail_data = None
    for trail_key, trail_info in CALIFORNIA_WATER_TRAILS.items():
        if reservoir_short_code in trail_info["nodes"]:
            trail_data = trail_info
            trail_data["trail_key"] = trail_key
            break
    
    if not trail_data:
        # Fallback to nearby infrastructure
        trail_nodes = await _get_nearby_infrastructure(db_pool, reservoir_short_code, max_depth)
        trail_data = {
            "name": f"Infrastructure near {reservoir_short_code}",
            "description": "Nearby water infrastructure",
            "nodes": trail_nodes,
            "key_infrastructure": [reservoir_short_code],
            "region": "UNKNOWN",
            "trail_key": "fallback"
        }
    
    # Get trail elements with geometry
    trail_features = await _get_trail_geojson(db_pool, trail_data["nodes"])
    
    # Filter by trail type
    if trail_type == "infrastructure":
        # Focus on key infrastructure nodes
        filtered_features = []
        for feature in trail_features:
            code = feature["properties"]["short_code"]
            element_type = feature["properties"]["element_type"]
            
            # Always include key infrastructure
            if code in KEY_INFRASTRUCTURE_CODES:
                filtered_features.append(feature)
            # Include reservoirs, pumps, treatment plants
            elif element_type in ["STR", "PS", "WTP", "WWTP"]:
                filtered_features.append(feature)
            # Include major river nodes (every 5th)
            elif element_type == "CH" and trail_data["nodes"].index(code) % 5 == 0:
                filtered_features.append(feature)
            # Include connecting arcs between infrastructure
            elif feature["properties"]["type"] == "arc":
                from_node = feature["properties"].get("from_node", "")
                to_node = feature["properties"].get("to_node", "")
                if (from_node in KEY_INFRASTRUCTURE_CODES or 
                    to_node in KEY_INFRASTRUCTURE_CODES):
                    filtered_features.append(feature)
        
        trail_features = filtered_features
    
    return {
        "type": "FeatureCollection",
        "features": trail_features,
        "metadata": {
            "start_reservoir": reservoir_short_code,
            "trail_name": trail_data["name"],
            "trail_description": trail_data["description"],
            "trail_type": trail_type,
            "trail_key": trail_data["trail_key"],
            "region": trail_data["region"],
            "total_features": len(trail_features),
            "full_trail_size": len(trail_data["nodes"]),
            "approach": "hardcoded_california_water_trails",
            "foundation": "network_topology_csv_analysis"
        }
    }


async def get_major_reservoir_trails(
    db_pool: asyncpg.Pool,
    trail_type: str = "infrastructure"
) -> Dict[str, Any]:
    """
    Get overview of all major California water trails
    Shows the big picture of California water system
    """
    
    all_features = []
    trail_summaries = []
    
    # Get trail for each major reservoir system
    major_reservoirs = ["SHSTA", "OROVL", "FOLSM", "SLUIS", "HETCH", "TRNTY"]
    
    for reservoir in major_reservoirs:
        try:
            trail_data = await get_water_trail_from_reservoir(
                db_pool, reservoir, trail_type, max_depth=4
            )
            
            # Add trail system metadata to features
            for feature in trail_data["features"]:
                feature["properties"]["trail_system"] = trail_data["metadata"]["trail_key"]
                feature["properties"]["trail_name"] = trail_data["metadata"]["trail_name"]
                feature["properties"]["region"] = trail_data["metadata"]["region"]
            
            all_features.extend(trail_data["features"])
            
            trail_summaries.append({
                "reservoir": reservoir,
                "trail_key": trail_data["metadata"]["trail_key"],
                "trail_name": trail_data["metadata"]["trail_name"],
                "region": trail_data["metadata"]["region"],
                "features": len(trail_data["features"])
            })
            
        except Exception as e:
            print(f"Error getting trail for {reservoir}: {e}")
            continue
    
    return {
        "type": "FeatureCollection",
        "features": all_features,
        "metadata": {
            "trail_type": trail_type,
            "total_features": len(all_features),
            "trail_systems": len(trail_summaries),
            "trail_summaries": trail_summaries,
            "approach": "california_water_system_overview",
            "foundation": "hardcoded_major_pathways"
        }
    }


async def _get_nearby_infrastructure(
    db_pool: asyncpg.Pool,
    start_code: str,
    max_depth: int
) -> List[str]:
    """Fallback: get nearby infrastructure if not in hardcoded trails"""
    
    query = """
    WITH RECURSIVE nearby AS (
        SELECT short_code, 0 as depth
        FROM network_topology 
        WHERE short_code = $1 AND is_active = true
        
        UNION ALL
        
        SELECT nt.short_code, n.depth + 1
        FROM nearby n
        JOIN network_topology nt ON (
            nt.from_node = n.short_code OR nt.to_node = n.short_code
        )
        WHERE n.depth < $2 
        AND nt.is_active = true
        AND nt.type IN ('STR', 'PS', 'WTP', 'WWTP', 'CH')
    )
    SELECT DISTINCT short_code FROM nearby ORDER BY short_code;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, start_code, max_depth)
    
    return [row["short_code"] for row in rows]


async def _get_trail_geojson(
    db_pool: asyncpg.Pool,
    trail_codes: List[str]
) -> List[Dict[str, Any]]:
    """Convert trail node codes to GeoJSON features with geometry"""
    
    if not trail_codes:
        return []
    
    # Build parameterized query
    placeholders = ','.join(f'${i+1}' for i in range(len(trail_codes)))
    
    query = f"""
    SELECT 
        nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
        nt.from_node, nt.to_node, nt.river_name, nt.arc_name,
        nt.hydrologic_region,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.short_code IN ({placeholders})
    AND nt.is_active = true
    ORDER BY nt.schematic_type, nt.type, nt.short_code;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *trail_codes)
    
    features = []
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if not geometry:
                continue
            
            properties = {
                'id': row['id'],
                'short_code': row['short_code'],
                'type': row['schematic_type'],
                'element_type': row['type'],
                'subtype': row['sub_type'],
                'connectivity_status': 'connected',
                'trail_element': True,
                'hydrologic_region': row['hydrologic_region']
            }
            
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'display_name': row['river_name'] or row['short_code'],
                    'infrastructure_type': _get_infrastructure_type(row['type']),
                    'is_key_infrastructure': row['short_code'] in KEY_INFRASTRUCTURE_CODES
                })
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'],
                    'to_node': row['to_node'],
                    'display_name': row['arc_name'] or f"{row['from_node']} → {row['to_node']}",
                    'is_key_connection': (row['from_node'] in KEY_INFRASTRUCTURE_CODES or 
                                        row['to_node'] in KEY_INFRASTRUCTURE_CODES)
                })
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing trail element {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return features


def _get_infrastructure_type(element_type: str) -> str:
    """Get human-readable infrastructure type"""
    type_mapping = {
        'STR': 'Major Reservoir',
        'PS': 'Pump Station', 
        'WTP': 'Water Treatment Plant',
        'WWTP': 'Wastewater Treatment',
        'CH': 'River Channel',
        'D': 'Water Delivery',
        'OM': 'Water Outlet',
        'NP': 'Water User',
        'PR': 'Water Project'
    }
    return type_mapping.get(element_type, element_type)