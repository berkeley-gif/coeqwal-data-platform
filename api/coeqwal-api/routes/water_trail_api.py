"""
Water Trail API - Systematic progression from sparse to comprehensive
Following the established pattern: Foundation -> Enhanced -> Complete -> Curated
"""

import asyncpg
from typing import List, Dict, Any, Set
import json

# FOUNDATION TRAILS - Simple, reliable backbone (like Pass 1: Geopackage-only)
FOUNDATION_TRAILS = {
    "shasta_sacramento": {
        "name": "Shasta Dam to Sacramento River System",
        "description": "Northern California backbone - most reliable connections",
        "nodes": ["SHSTA", "SAC301", "SAC083", "SAC043", "SAC000"]
    },
    
    "oroville_feather": {
        "name": "Oroville Dam to Feather River System", 
        "description": "Central Valley backbone via Feather River",
        "nodes": ["OROVL", "FTR003", "SAC083"]
    },
    
    "folsom_american": {
        "name": "Folsom Lake to American River System",
        "description": "American River backbone",
        "nodes": ["FOLSM", "AMR002", "SAC043"]
    },
    
    "san_luis_delta": {
        "name": "San Luis to Delta System",
        "description": "Central Valley Project backbone",
        "nodes": ["SLUIS", "DMC001", "MDOTA", "SJRE"]
    }
}

# ENHANCED TRAILS - More detailed pathways (like Pass 2: XML with geometry)
ENHANCED_TRAILS = {
    "shasta_sacramento": {
        "name": "Shasta Dam to Sacramento River System - Enhanced",
        "description": "Detailed Sacramento River pathway with major tributaries",
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
        ]
    },
    
    "oroville_feather": {
        "name": "Oroville Dam to Feather River System - Enhanced",
        "description": "Complete Feather River pathway",
        "nodes": [
            "OROVL", "FTR072", "FTR070", "FTR068", "FTR067", "FTR065", "FTR063",
            "FTR061", "FTR059", "FTR057", "FTR055", "FTR053", "FTR051", "FTR049",
            "FTR047", "FTR045", "FTR043", "FTR041", "FTR039", "FTR037", "FTR035",
            "FTR033", "FTR031", "FTR029", "FTR027", "FTR025", "FTR023", "FTR021",
            "FTR019", "FTR017", "FTR015", "FTR013", "FTR011", "FTR009", "FTR007",
            "FTR005", "FTR003", "SAC083"
        ]
    },
    
    "folsom_american": {
        "name": "Folsom Lake to American River System - Enhanced", 
        "description": "Complete American River pathway",
        "nodes": [
            "FOLSM", "NTOMA", "AMR028", "AMR026", "AMR024", "AMR022", "AMR020",
            "AMR018", "AMR016", "AMR014", "AMR012", "AMR010", "AMR008", "AMR006",
            "AMR004", "AMR002", "SAC043"
        ]
    },
    
    "san_luis_delta": {
        "name": "San Luis to Delta System - Enhanced",
        "description": "Complete Central Valley Project pathway",
        "nodes": [
            "SLUIS", "DMC001", "MENDOTA", "SJR070", "SJR062", "SJR056", "SJR053", 
            "SJR048", "SJR043", "SJR038", "SJR033", "SJR028", "SJR026", "SJR023", 
            "SJR013", "SJR009", "MDOTA", "SJRW", "SJRE"
        ]
    }
}


async def get_water_trail_from_reservoir(
    db_pool: asyncpg.Pool,
    reservoir_short_code: str,
    trail_type: str = "foundation",  # foundation, enhanced, complete
    max_depth: int = 6
) -> Dict[str, Any]:
    """
    Systematic trail progression: foundation -> enhanced -> complete
    Like the 3-pass traversal: geopackage -> xml+geometry -> xml+logical
    """
    
    # Select trail level based on type - handle both old and new parameter values
    if trail_type in ["foundation"]:
        trails = FOUNDATION_TRAILS
        description_suffix = "Foundation level - most reliable connections"
    elif trail_type in ["enhanced"]:
        trails = ENHANCED_TRAILS  
        description_suffix = "Enhanced level - detailed pathways"
    elif trail_type in ["comprehensive"]:
        trails = ENHANCED_TRAILS
        description_suffix = "Comprehensive level - extensive trail coverage"
    elif trail_type in ["complete"]:
        trails = ENHANCED_TRAILS  # Use enhanced as base for complete
        description_suffix = "Complete level - all infrastructure"
    else:  # Default for old "infrastructure" parameter
        trails = ENHANCED_TRAILS
        description_suffix = "Enhanced level - detailed pathways (default)"
    
    # Find matching trail
    matching_trail = None
    for trail_name, trail_data in trails.items():
        if reservoir_short_code in trail_data["nodes"]:
            matching_trail = trail_name
            break
    
    if not matching_trail:
        # Create dynamic trail
        trail_nodes = [reservoir_short_code]
        trail_name = f"dynamic_{reservoir_short_code}"
        trail_description = f"Dynamic trail from {reservoir_short_code} - {description_suffix}"
    else:
        trail_nodes = trails[matching_trail]["nodes"]
        trail_name = matching_trail
        trail_description = f"{trails[matching_trail]['description']} - {description_suffix}"
    
    # Get trail GeoJSON
    trail_features = await _get_trail_geojson(db_pool, trail_nodes)
    
    # Progressive enhancement based on trail type
    additional_features = []
    
    if trail_type == "enhanced":
        # Add key infrastructure (like Pass 2: XML with geometry)
        additional_features = await _get_key_infrastructure_with_geometry(db_pool)
        
    elif trail_type == "comprehensive":
        # Add comprehensive infrastructure (more than enhanced, less than complete)
        additional_features = await _get_comprehensive_infrastructure(db_pool)
        
    elif trail_type == "complete":
        # Add ALL infrastructure (like Pass 3: Complete coverage)
        additional_features = await _get_all_infrastructure_unlimited(db_pool)
    
    # Combine and deduplicate
    all_features = trail_features + additional_features
    unique_features = _deduplicate_features(all_features)
    
    return {
        "type": "FeatureCollection",
        "trail_info": {
            "name": trail_name,
            "description": trail_description,
            "reservoir_code": reservoir_short_code,
            "trail_type": trail_type,
            "progression_level": _get_progression_level(trail_type),
            "feature_count": len(unique_features),
            "backbone_nodes": len(trail_nodes),
            "additional_infrastructure": len(additional_features)
        },
        "features": unique_features
    }


async def get_major_reservoir_trails(
    db_pool: asyncpg.Pool,
    trail_type: str = "foundation"
) -> Dict[str, Any]:
    """
    Get all major California water trails with systematic progression
    foundation -> enhanced -> complete (like the 3-pass system)
    """
    
    # Select trail level - handle both old and new parameter values
    if trail_type in ["foundation"]:
        trails = FOUNDATION_TRAILS
        description = "Foundation level - reliable backbone connections"
    elif trail_type in ["enhanced"]: 
        trails = ENHANCED_TRAILS
        description = "Enhanced level - detailed river pathways"
    elif trail_type in ["comprehensive"]:
        trails = ENHANCED_TRAILS
        description = "Comprehensive level - extensive trail coverage"
    elif trail_type in ["complete"]:
        trails = ENHANCED_TRAILS
        description = "Complete level - all infrastructure network"
    else:  # Default for old "infrastructure" parameter
        trails = ENHANCED_TRAILS
        description = "Enhanced level - detailed river pathways (default)"
    
    # Get top 9 reservoirs for context
    top_reservoirs = await _get_top_9_reservoirs(db_pool)
    
    # Collect all trail nodes
    all_trail_nodes = []
    for trail_data in trails.values():
        all_trail_nodes.extend(trail_data["nodes"])
    all_trail_nodes.extend(top_reservoirs)
    
    # Get trail GeoJSON
    trail_features = await _get_trail_geojson(db_pool, all_trail_nodes)
    
    # Progressive enhancement
    additional_features = []
    
    if trail_type == "enhanced":
        # Add key infrastructure with geometry
        additional_features = await _get_key_infrastructure_with_geometry(db_pool)
        
    elif trail_type == "comprehensive":
        # Add comprehensive infrastructure (more than enhanced)
        additional_features = await _get_comprehensive_infrastructure(db_pool)
        
    elif trail_type == "complete":
        # Add ALL infrastructure (unlimited)
        additional_features = await _get_all_infrastructure_unlimited(db_pool)
    
    # Combine and deduplicate
    all_features = trail_features + additional_features
    unique_features = _deduplicate_features(all_features)
    
    return {
        "type": "FeatureCollection",
        "trail_info": {
            "name": f"California Water Trails - {trail_type.title()} Level",
            "description": description,
            "trail_type": trail_type,
            "progression_level": _get_progression_level(trail_type),
            "feature_count": len(unique_features),
            "trails_included": list(trails.keys()),
            "top_reservoirs": top_reservoirs,
            "backbone_nodes": len(all_trail_nodes),
            "additional_infrastructure": len(additional_features)
        },
        "features": unique_features
    }


def _get_progression_level(trail_type: str) -> Dict[str, Any]:
    """Get progression metadata like the 3-pass system"""
    
    levels = {
        "foundation": {
            "level": 1,
            "description": "Most reliable connections (like Pass 1: Geopackage-only)",
            "data_quality": "highest",
            "coverage": "backbone",
            "expected_features": "50-200"
        },
        "enhanced": {
            "level": 2, 
            "description": "Detailed pathways (like Pass 2: XML with geometry)",
            "data_quality": "high",
            "coverage": "detailed",
            "expected_features": "400-800"
        },
        "comprehensive": {
            "level": 3,
            "description": "More infrastructure - fast loading",
            "data_quality": "comprehensive",
            "coverage": "extensive",
            "expected_features": "400-600"
        },
        "complete": {
            "level": 4,
            "description": "All infrastructure (like Pass 3: Complete coverage)",
            "data_quality": "complete",
            "coverage": "full",
            "expected_features": "1500-3000+"
        }
    }
    
    return levels.get(trail_type, levels["foundation"])


async def _get_top_9_reservoirs(db_pool: asyncpg.Pool) -> List[str]:
    """Get top 9 reservoirs by capacity"""
    
    query = """
    SELECT nt.short_code
    FROM network_topology nt
    LEFT JOIN reservoir_entity re ON nt.short_code = re.short_code
    WHERE nt.type = 'STR'
    AND nt.is_active = true
    AND re.capacity_taf IS NOT NULL
    ORDER BY re.capacity_taf DESC
    LIMIT 9;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    return [row["short_code"] for row in rows]


async def _get_key_infrastructure_with_geometry(
    db_pool: asyncpg.Pool
) -> List[Dict[str, Any]]:
    """
    Enhanced level: Key infrastructure with geometry (like Pass 2)
    Reasonable limit for visualization
    """
    
    query = """
    SELECT 
        nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
        nt.from_node, nt.to_node, nt.river_name, nt.arc_name,
        nt.hydrologic_region,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.is_active = true
    AND ng.geom IS NOT NULL  -- Only with geometry
    AND (
        nt.type IN ('STR', 'PS', 'WTP', 'WWTP', 'CH', 'DD', 'DA') OR  -- More infrastructure types
        (nt.type = 'CH' AND nt.river_name IS NOT NULL) OR  -- All named rivers, not just 4
        nt.short_code LIKE 'SAC%' OR nt.short_code LIKE 'SJR%' OR  -- Major river systems
        nt.short_code LIKE 'AMR%' OR nt.short_code LIKE 'FTR%' OR
        nt.short_code LIKE 'TUO%' OR nt.short_code LIKE 'MER%'
    )
    ORDER BY 
        CASE nt.type 
            WHEN 'STR' THEN 1 
            WHEN 'PS' THEN 2 
            WHEN 'WTP' THEN 3 
            WHEN 'WWTP' THEN 4 
            WHEN 'CH' THEN 5
            WHEN 'DD' THEN 6
            WHEN 'DA' THEN 7
            ELSE 8 
        END,
        nt.short_code
    LIMIT 800;  -- Higher limit for more features
    """
    
    return await _execute_infrastructure_query(db_pool, query, "enhanced_infrastructure")


async def _get_comprehensive_infrastructure(
    db_pool: asyncpg.Pool
) -> List[Dict[str, Any]]:
    """
    Comprehensive level: More than enhanced, but still legible
    Sweet spot between enhanced (800) and complete (unlimited)
    """
    
    query = """
    SELECT 
        nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
        nt.from_node, nt.to_node, nt.river_name, nt.arc_name,
        nt.hydrologic_region,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.is_active = true
    AND ng.geom IS NOT NULL  -- Only with geometry for visualization
    AND (
        nt.type IN ('STR', 'PS', 'WTP', 'WWTP') OR  -- Just the core infrastructure types
        (nt.type = 'CH' AND nt.river_name IN ('Sacramento River', 'San Joaquin River', 'American River', 'Feather River')) OR  -- Only 4 major rivers
        nt.short_code IN ('SAC000', 'SAC043', 'SAC083', 'SAC301', 'SJRE', 'SJRW', 'MDOTA', 'AMR002', 'FTR003')  -- Key junction points
    )
    ORDER BY 
        CASE nt.type 
            WHEN 'STR' THEN 1 
            WHEN 'PS' THEN 2 
            WHEN 'WTP' THEN 3 
            WHEN 'WWTP' THEN 4 
            WHEN 'CH' THEN 5
            ELSE 6 
        END,
        nt.short_code
    LIMIT 600;  -- Much smaller limit for performance
    """
    
    return await _execute_infrastructure_query(db_pool, query, "comprehensive_infrastructure")


async def _get_all_infrastructure_unlimited(
    db_pool: asyncpg.Pool
) -> List[Dict[str, Any]]:
    """
    Complete level: ALL infrastructure (like Pass 3 - no limits)
    This is where we removed the LIMIT 100
    """
    
    query = """
    SELECT 
        nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
        nt.from_node, nt.to_node, nt.river_name, nt.arc_name,
        nt.hydrologic_region,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.is_active = true
    AND ng.geom IS NOT NULL  -- Only features with geometry for visualization
    AND (
        nt.type IN ('STR', 'PS', 'WTP', 'WWTP', 'CH', 'DD', 'DA', 'D') OR  -- ALL infrastructure types
        nt.schematic_type IN ('node', 'arc')  -- Include all schematic elements
    )
    ORDER BY 
        CASE nt.type 
            WHEN 'STR' THEN 1 
            WHEN 'PS' THEN 2 
            WHEN 'WTP' THEN 3 
            WHEN 'WWTP' THEN 4 
            WHEN 'CH' THEN 5
            WHEN 'DD' THEN 6
            WHEN 'DA' THEN 7
            WHEN 'D' THEN 8
            ELSE 9 
        END,
        nt.short_code;
    -- NO LIMIT - comprehensive coverage!
    """
    
    return await _execute_infrastructure_query(db_pool, query, "complete_infrastructure")


async def _execute_infrastructure_query(
    db_pool: asyncpg.Pool,
    query: str, 
    source_label: str
) -> List[Dict[str, Any]]:
    """Execute infrastructure query and format results"""
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    features = []
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if not geometry:
                continue
                
            feature = {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "id": row['id'],
                    "short_code": row['short_code'],
                    "schematic_type": row['schematic_type'],
                    "type": row['type'],
                    "sub_type": row['sub_type'],
                    "from_node": row['from_node'],
                    "to_node": row['to_node'],
                    "river_name": row['river_name'],
                    "arc_name": row['arc_name'],
                    "hydrologic_region": row['hydrologic_region'],
                    "geometry_type": row['geometry_type'],
                    "source": source_label
                }
            }
            features.append(feature)
            
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error processing geometry for {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return features


async def _get_trail_geojson(
    db_pool: asyncpg.Pool,
    trail_nodes: List[str]
) -> List[Dict[str, Any]]:
    """Get GeoJSON features for trail backbone nodes"""
    
    if not trail_nodes:
        return []
    
    query = """
    SELECT 
        nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
        nt.from_node, nt.to_node, nt.river_name, nt.arc_name,
        nt.hydrologic_region,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.short_code = ANY($1::text[])
    AND nt.is_active = true
    ORDER BY nt.type, nt.short_code;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, trail_nodes)
    
    features = []
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if not geometry:
                continue
                
            feature = {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "id": row['id'],
                    "short_code": row['short_code'],
                    "schematic_type": row['schematic_type'],
                    "type": row['type'],
                    "sub_type": row['sub_type'],
                    "from_node": row['from_node'],
                    "to_node": row['to_node'],
                    "river_name": row['river_name'],
                    "arc_name": row['arc_name'],
                    "hydrologic_region": row['hydrologic_region'],
                    "geometry_type": row['geometry_type'],
                    "source": "trail_backbone"
                }
            }
            features.append(feature)
            
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error processing geometry for {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return features


def _deduplicate_features(features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate features based on short_code (like the 3-pass deduplication)"""
    
    seen_codes = set()
    unique_features = []
    
    for feature in features:
        short_code = feature["properties"]["short_code"]
        if short_code not in seen_codes:
            seen_codes.add(short_code)
            unique_features.append(feature)
    
    return unique_features