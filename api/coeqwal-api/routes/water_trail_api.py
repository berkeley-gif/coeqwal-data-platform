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
        "description": "Northern California's primary water source - complete pathway",
        "nodes": [
            # Complete Shasta to Sacramento pathway with ALL connecting arcs
            "SHSTA", "C_SHSTA", "SAC301", "C_SAC301", "KSWCK", "C_KSWCK", 
            "SAC299", "C_SAC299", "SAC296", "C_SAC296", "SAC294", "C_SAC294", 
            "SAC289", "C_SAC289", "SAC287", "C_SAC287", "SAC281", "C_SAC281", 
            "SAC277", "C_SAC277", "SAC275", "C_SAC275", "SAC273", "C_SAC273", 
            "SAC271", "C_SAC271", "SAC269", "C_SAC269", "SAC267", "C_SAC267", 
            "SAC265", "C_SAC265", "SAC263", "C_SAC263", "SAC261", "C_SAC261", 
            "SAC259", "C_SAC259", "SAC257", "C_SAC257", "SAC255", "C_SAC255", 
            "SAC253", "C_SAC253", "SAC251", "C_SAC251", "SAC249", "C_SAC249", 
            "SAC247", "C_SAC247", "SAC245", "C_SAC245", "SAC243", "C_SAC243", 
            "SAC241", "C_SAC241", "SAC239", "C_SAC239", "SAC237", "C_SAC237", 
            "SAC235", "C_SAC235", "SAC233", "C_SAC233", "SAC232", "C_SAC232", 
            "SAC230", "C_SAC230", "SAC228", "C_SAC228", "SAC226", "C_SAC226", 
            "SAC224", "C_SAC224", "SAC222", "C_SAC222", "SAC220", "C_SAC220", 
            "SAC218", "C_SAC218", "SAC216", "C_SAC216", "SAC214", "C_SAC214", 
            "SAC212", "C_SAC212", "SAC210", "C_SAC210", "SAC208", "C_SAC208", 
            "SAC207", "C_SAC207", "SAC205", "C_SAC205", "SAC203", "C_SAC203", 
            "SAC201", "C_SAC201", "SAC199", "C_SAC199", "SAC197", "C_SAC197", 
            "SAC196", "C_SAC196", "SAC194", "C_SAC194", "SAC193", "C_SAC193", 
            "SAC191", "C_SAC191", "SAC189", "C_SAC189", "SAC187", "C_SAC187", 
            "SAC185", "C_SAC185", "SAC184", "C_SAC184", "SAC182", "C_SAC182", 
            "SAC180", "C_SAC180", "SAC178", "C_SAC178", "SAC176", "C_SAC176", 
            "SAC174", "C_SAC174", "SAC172", "C_SAC172", "SAC170", "C_SAC170", 
            "SAC168", "C_SAC168", "SAC166", "C_SAC166", "SAC164", "C_SAC164", 
            "SAC162", "C_SAC162", "SAC160", "C_SAC160", "SAC159", "C_SAC159", 
            "SAC157", "C_SAC157", "SAC155", "C_SAC155", "SAC154", "C_SAC154", 
            "SAC152", "C_SAC152", "SAC150", "C_SAC150", "SAC148", "C_SAC148", 
            "SAC146", "C_SAC146", "SAC144", "C_SAC144", "SAC143", "C_SAC143", 
            "SAC141", "C_SAC141", "SAC139", "C_SAC139", "SAC137", "C_SAC137", 
            "SAC135", "C_SAC135", "SAC134", "C_SAC134", "SAC132", "C_SAC132", 
            "SAC130", "C_SAC130", "SAC129", "C_SAC129", "SAC127", "C_SAC127", 
            "SAC125", "C_SAC125", "SAC124", "C_SAC124", "SAC122", "C_SAC122", 
            "SAC120", "C_SAC120", "SAC119", "C_SAC119", "SAC117", "C_SAC117", 
            "SAC115", "C_SAC115", "SAC113", "C_SAC113", "SAC111", "C_SAC111", 
            "SAC109", "C_SAC109", "SAC107", "C_SAC107", "SAC105", "C_SAC105", 
            "SAC103", "C_SAC103", "SAC101", "C_SAC101", "SAC099", "C_SAC099", 
            "SAC097", "C_SAC097", "SAC095", "C_SAC095", "SAC093", "C_SAC093", 
            "SAC091", "C_SAC091", "SAC089", "C_SAC089", "SAC087", "C_SAC087", 
            "SAC085", "C_SAC085", "SAC083", "C_SAC083", "SAC081", "C_SAC081", 
            "SAC079", "C_SAC079", "SAC077", "C_SAC077", "SAC075", "C_SAC075", 
            "SAC073", "C_SAC073", "SAC071", "C_SAC071", "SAC069", "C_SAC069", 
            "SAC067", "C_SAC067", "SAC065", "C_SAC065", "SAC063", "C_SAC063", 
            "SAC061", "C_SAC061", "SAC059", "C_SAC059", "SAC057", "C_SAC057", 
            "SAC055", "C_SAC055", "SAC053", "C_SAC053", "SAC051", "C_SAC051", 
            "SAC050", "C_SAC050", "SAC048", "C_SAC048", "SAC047", "C_SAC047", 
            "SAC045", "C_SAC045", "SAC043", "C_SAC043", "SAC041", "C_SAC041", 
            "SAC039", "C_SAC039", "SAC037", "C_SAC037", "SAC035", "C_SAC035", 
            "SAC033", "C_SAC033", "SAC031", "C_SAC031", "SAC030", "C_SAC030", 
            "SAC029", "C_SAC029", "SAC027", "C_SAC027", "SAC025", "C_SAC025", 
            "SAC023", "C_SAC023", "SAC021", "C_SAC021", "SAC019", "C_SAC019", 
            "SAC017", "C_SAC017", "SAC015", "C_SAC015", "SAC013", "C_SAC013", 
            "SAC011", "C_SAC011", "SAC009", "C_SAC009", "SAC007", "C_SAC007", 
            "SAC005", "C_SAC005", "SAC003", "C_SAC003", "SAC001", "C_SAC001", "SAC000"
        ],
        "key_infrastructure": ["SHSTA", "KSWCK", "SAC083", "SAC043", "SAC000"],
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
    },
    
    # SOUTH OF DELTA SYSTEMS
    
    "central_valley_project_south": {
        "name": "Central Valley Project - South of Delta",
        "description": "CVP water distribution to San Joaquin Valley and Southern California",
        "nodes": [
            # San Luis to Mendota Pool
            "SLUIS", "SLUISC", "DMC", "CALA", "MENDOTA", "MDOTA",
            # Friant-Kern Canal system  
            "MILLR", "FKC001", "FKC010", "FKC020", "FKC030", "FKC040", "FKC050",
            "FKC060", "FKC070", "FKC080", "FKC090", "FKC100", "FKC110", "FKC120",
            # Cross Valley Canal
            "CVC001", "CVC010", "CVC020", "CVC030", "CVC040", "CVC050",
            # Key delivery points
            "BAKRSF", "KRNRVR", "TULAR", "PIXLEY", "DELANO", "WASCO"
        ],
        "key_infrastructure": ["SLUIS", "MILLR", "MENDOTA", "MDOTA", "BAKRSF"],
        "region": "SJR"
    },
    
    "state_water_project_south": {
        "name": "State Water Project - Southern California",
        "description": "California Aqueduct to Southern California",
        "nodes": [
            # California Aqueduct main stem
            "PYRMD", "CSTLC", "QUNTO", "ANTLP", "TEHCP", "EDMNTN", 
            "MOJAVE", "SLVRK", "PEARBM", "PYRAMID", "CSTLC",
            # West Branch - Los Angeles
            "WBRNCH", "QUNTO", "ANTLP", "PYRAMID", "CSTLC",
            # East Branch - Inland Empire  
            "EBRNCH", "MOJAVE", "SLVRK", "PEARBM", "DVLAKE", "PRRIS",
            # San Diego connection
            "DMOND", "OLIVH", "SNVCNT"
        ],
        "key_infrastructure": ["PYRMD", "CSTLC", "ANTLP", "TEHCP", "PYRAMID"],
        "region": "SCLA"
    },
    
    "kern_river_system": {
        "name": "Kern River and Tulare Basin System", 
        "description": "Kern River water supply and Tulare Basin infrastructure",
        "nodes": [
            # Kern River main stem
            "ISBLK", "SUCCS", "KRNRVR", "KRN001", "KRN010", "KRN020", "KRN030",
            "KRN040", "KRN050", "KRN060", "KRN070", "KRN080", "KRN090",
            # Tulare Basin
            "TULAR", "TLB001", "TLB010", "TLB020", "TLB030", "TLB040",
            "PIXLEY", "DELANO", "WASCO", "BAKRSF",
            # Kings River connections
            "PINFT", "KGSRVR", "KGS001", "KGS010", "KGS020", "KGS030"
        ],
        "key_infrastructure": ["ISBLK", "SUCCS", "KRNRVR", "TULAR", "PINFT"],
        "region": "TLB"
    },
    
    "colorado_river_system": {
        "name": "Colorado River Aqueduct System",
        "description": "Colorado River water to Southern California",
        "nodes": [
            # Colorado River Aqueduct
            "HAVASU", "PARKE", "IRONMT", "EAGLMT", "GENMTN", "HINDS",
            "COPLND", "ALAMTN", "CSTLRC", "LKMAT", "SKINNR", "DVLAKE",
            # All-American Canal  
            "IMPRVL", "ALLAM", "AAC001", "AAC010", "AAC020", "AAC030",
            "AAC040", "AAC050", "AAC060", "AAC070", "AAC080",
            # Coachella Canal
            "COACHLL", "CCH001", "CCH010", "CCH020", "CCH030", "CCH040"
        ],
        "key_infrastructure": ["HAVASU", "PARKE", "IMPRVL", "DVLAKE", "COACHLL"],
        "region": "COLO"
    },
    
    "san_joaquin_comprehensive": {
        "name": "San Joaquin River Comprehensive System",
        "description": "Complete San Joaquin River basin with all tributaries",
        "nodes": [
            # Main San Joaquin River (extended)
            "SJR001", "SJR005", "SJR009", "SJR013", "SJR022", "SJR023", "SJR026", 
            "SJR028", "SJR033", "SJR038", "SJR042", "SJR043", "SJR048", "SJR053", 
            "SJR056", "SJR062", "SJR070", "SJRW", "SJRE",
            # Stanislaus River
            "DONPD", "STS001", "STS010", "STS020", "STS030", "STS040", "STS050",
            # Tuolumne River (extended)
            "HETCH", "DMPDN", "EXCQR", "TULLK", "MODTO", "TUO001", "TUO009", 
            "TUO026", "TUO040", "TUO054", "TUO070",
            # Merced River
            "MCCLR", "EXCQR", "MCD001", "MCD010", "MCD020", "MCD030", "MCD040", "MCD056",
            # Kings River
            "PINFT", "KGSRVR", "KGS001", "KGS010", "KGS020", "KGS030", "KGS040"
        ],
        "key_infrastructure": ["DONPD", "HETCH", "MCCLR", "PINFT", "SJRE", "SJRW"],
        "region": "SJR"
    }
}

# Major pump stations and treatment facilities to always include
KEY_INFRASTRUCTURE_CODES = [
    # Major reservoirs - North
    "SHSTA", "OROVL", "FOLSM", "SLUIS", "HETCH", "TRNTY", "WKYTN",
    "AMADR", "MILLR", "DONPD", "MCCLR", "ENGLB", "CMPFW",
    
    # Major reservoirs - South of Delta (ADDED)
    "ISBLK", "SUCCS", "PINFT", "PYRMD", "CSTLC", "ANTLP", "TEHCP",
    "HAVASU", "PARKE", "IMPRVL", "DVLAKE", "COACHLL",
    
    # Key river junctions - North
    "SAC000", "SAC043", "SAC083", "SJRE", "SJRW", "MDOTA", "MENDOTA",
    "KSWCK", "NTOMA", "MODTO", "TULLK",
    
    # Key river junctions - South of Delta (ADDED)
    "KRNRVR", "TULAR", "BAKRSF", "PIXLEY", "DELANO", "WASCO",
    "KGSRVR", "PYRAMID", "MOJAVE", "SLVRK", "PEARBM",
    
    # Major treatment plants
    "WTPEDH", "WTPSJP", "WTPRSV", "WTPFOL", "WTPJMS", "WTPSTK",
    
    # Key pump stations and diversions - North  
    "CALA", "SLUISC", "DMPDN", "EXCQR",
    
    # Key pump stations and diversions - South of Delta (ADDED)
    "QUNTO", "WBRNCH", "EBRNCH", "DMOND", "OLIVH", "SNVCNT",
    "IRONMT", "EAGLMT", "GENMTN", "HINDS", "COPLND", "ALAMTN"
]


async def get_water_trail_from_reservoir(
    db_pool: asyncpg.Pool,
    reservoir_short_code: str,
    trail_type: str = "infrastructure",
    max_depth: int = 6
) -> Dict[str, Any]:
    """
    Get a comprehensive water trail from a major reservoir using hybrid approach:
    1. Start with hardcoded backbone pathway (if available)
    2. Dynamically expand to include ALL connected infrastructure
    This ensures complete connectivity while maintaining curated major pathways
    """
    
    # Find which trail this reservoir belongs to
    trail_data = None
    for trail_key, trail_info in CALIFORNIA_WATER_TRAILS.items():
        if reservoir_short_code in trail_info["nodes"]:
            trail_data = trail_info.copy()  # Make a copy to avoid modifying original
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
    
    # HYBRID APPROACH: Expand hardcoded trails with all connected infrastructure
    # This addresses the sparseness issue by including tributaries, diversions, etc.
    if trail_data["trail_key"] != "fallback":
        print(f"🔄 Expanding {trail_data['name']} with connected infrastructure...")
        expanded_nodes = await _expand_trail_with_connected_infrastructure(
            db_pool, 
            trail_data["nodes"], 
            reservoir_short_code,
            max_depth=4  # Reasonable depth for connected infrastructure
        )
        
        # Combine hardcoded backbone with expanded infrastructure
        all_trail_nodes = list(set(trail_data["nodes"] + expanded_nodes))
        trail_data["nodes"] = all_trail_nodes
        trail_data["expansion_added"] = len(expanded_nodes)
        trail_data["total_nodes"] = len(all_trail_nodes)
        
        print(f"✅ Trail expansion: {len(trail_data['nodes']) - len(expanded_nodes)} backbone + {len(expanded_nodes)} connected = {len(all_trail_nodes)} total")
    
    # Get trail elements with geometry
    trail_features = await _get_trail_geojson(db_pool, trail_data["nodes"])
    
    # Deduplicate features by short_code (in case hardcoded trails have duplicates)
    seen_codes = set()
    deduplicated_features = []
    
    for feature in trail_features:
        short_code = feature["properties"]["short_code"]
        if short_code not in seen_codes:
            seen_codes.add(short_code)
            deduplicated_features.append(feature)
    
    if len(deduplicated_features) < len(trail_features):
        print(f"Individual trail deduplication: {len(trail_features)} → {len(deduplicated_features)} features")
        trail_features = deduplicated_features
    
    # Include ALL connectivity for complete trail visualization
    # No filtering - show the complete water pathway for each system
    # This will create proper connected trails instead of sparse nodes
    
    # Build comprehensive metadata including expansion info
    metadata = {
        "start_reservoir": reservoir_short_code,
        "trail_name": trail_data["name"],
        "trail_description": trail_data["description"],
        "trail_type": trail_type,
        "trail_key": trail_data["trail_key"],
        "region": trail_data["region"],
        "total_features": len(trail_features),
        "full_trail_size": len(trail_data["nodes"]),
        "approach": "hybrid_hardcoded_plus_expansion" if trail_data["trail_key"] != "fallback" else "dynamic_connectivity",
        "foundation": "network_topology_comprehensive_connectivity"
    }
    
    # Add expansion statistics if this was an expanded trail
    if "expansion_added" in trail_data:
        metadata["expansion_added"] = trail_data["expansion_added"]
        metadata["total_nodes"] = trail_data["total_nodes"]
        metadata["backbone_nodes"] = trail_data["total_nodes"] - trail_data["expansion_added"]
        metadata["expansion_ratio"] = round(trail_data["expansion_added"] / trail_data["total_nodes"], 2) if trail_data["total_nodes"] > 0 else 0
    
    return {
        "type": "FeatureCollection",
        "features": trail_features,
        "metadata": metadata
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
    
    # Get top 9 reservoirs by capacity + major system reservoirs
    top_reservoirs = await _get_top_9_reservoirs(db_pool)
    major_system_reservoirs = ["SHSTA", "OROVL", "FOLSM", "SLUIS", "HETCH", "TRNTY", "WKYTN", "AMADR", "MILLR"]
    
    # Combine and deduplicate
    all_major_reservoirs = list(set(top_reservoirs + major_system_reservoirs))
    
    for reservoir in all_major_reservoirs:
        try:
            # Use expanded trail approach for comprehensive connectivity
            trail_data = await get_water_trail_from_reservoir(
                db_pool, reservoir, trail_type, max_depth=5  # Slightly higher for overview
            )
            
            # Add trail system metadata to features
            for feature in trail_data["features"]:
                feature["properties"]["trail_system"] = trail_data["metadata"]["trail_key"]
                feature["properties"]["trail_name"] = trail_data["metadata"]["trail_name"]
                feature["properties"]["region"] = trail_data["metadata"]["region"]
                
                # Mark expanded infrastructure for frontend styling
                if "expansion_added" in trail_data["metadata"]:
                    feature["properties"]["is_expanded_infrastructure"] = True
            
            all_features.extend(trail_data["features"])
            
            # Enhanced trail summary with expansion info
            summary = {
                "reservoir": reservoir,
                "trail_key": trail_data["metadata"]["trail_key"],
                "trail_name": trail_data["metadata"]["trail_name"],
                "region": trail_data["metadata"]["region"],
                "features": len(trail_data["features"])
            }
            
            # Add expansion statistics if available
            if "expansion_added" in trail_data["metadata"]:
                summary["backbone_nodes"] = trail_data["metadata"]["total_nodes"] - trail_data["metadata"]["expansion_added"]
                summary["expanded_nodes"] = trail_data["metadata"]["expansion_added"]
                summary["expansion_ratio"] = round(trail_data["metadata"]["expansion_added"] / trail_data["metadata"]["total_nodes"], 2)
            
            trail_summaries.append(summary)
            
            print(f"✅ {reservoir}: {len(trail_data['features'])} features" + 
                  (f" (expanded: +{trail_data['metadata'].get('expansion_added', 0)})" if "expansion_added" in trail_data["metadata"] else ""))
            
        except Exception as e:
            print(f"❌ Error getting expanded trail for {reservoir}: {e}")
            continue
    
    # CRITICAL: Deduplicate features by short_code before returning
    # Multiple trail systems can include the same infrastructure (e.g., KSWCK, SAC083)
    # This was causing React key errors in the frontend
    seen_codes = set()
    deduplicated_features = []
    
    for feature in all_features:
        short_code = feature["properties"]["short_code"]
        if short_code not in seen_codes:
            seen_codes.add(short_code)
            deduplicated_features.append(feature)
        else:
            print(f"Deduplicating {short_code} - appears in multiple trail systems")
    
    print(f"Deduplication: {len(all_features)} → {len(deduplicated_features)} features")
    all_features = deduplicated_features
    
    # If we don't have enough features after deduplication, add more key infrastructure
    if len(all_features) < 50:
        print(f"Adding more infrastructure - only have {len(all_features)} features")
        additional_features = await _get_additional_key_infrastructure(db_pool)
        
        # Avoid duplicates (check against already deduplicated features)
        existing_codes = {f["properties"]["short_code"] for f in all_features}
        new_features = [f for f in additional_features if f["properties"]["short_code"] not in existing_codes]
        
        all_features.extend(new_features)
        print(f"Added {len(new_features)} additional infrastructure features")
    
    return {
        "type": "FeatureCollection",
        "features": all_features,
        "metadata": {
            "trail_type": trail_type,
            "total_features": len(all_features),
            "trail_systems": len(trail_summaries),
            "trail_summaries": trail_summaries,
            "approach": "california_water_system_overview",
            "foundation": "hardcoded_major_pathways_with_fallback"
        }
    }


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


async def _get_additional_key_infrastructure(
    db_pool: asyncpg.Pool
) -> List[Dict[str, Any]]:
    """Get additional key infrastructure when hardcoded trails are too sparse"""
    
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
    AND (
        nt.type IN ('STR', 'PS', 'WTP', 'WWTP') OR
        (nt.type = 'CH' AND nt.river_name IN ('Sacramento River', 'San Joaquin River', 'American River', 'Feather River')) OR
        nt.short_code IN ('SAC000', 'SAC043', 'SAC083', 'SJRE', 'SJRW', 'MDOTA')
    )
    ORDER BY 
        CASE nt.type 
            WHEN 'STR' THEN 1 
            WHEN 'PS' THEN 2 
            WHEN 'WTP' THEN 3 
            WHEN 'WWTP' THEN 4 
            ELSE 5 
        END,
        nt.short_code
    LIMIT 100;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
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
                'hydrologic_region': row['hydrologic_region'],
                'is_additional_infrastructure': True
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
                    'display_name': row['arc_name'] or f"{row['from_node']} → {row['to_node']}"
                })
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing additional infrastructure {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return features


async def _expand_trail_with_connected_infrastructure(
    db_pool: asyncpg.Pool,
    backbone_nodes: List[str],
    primary_reservoir: str,
    max_depth: int = 4
) -> List[str]:
    """
    Expand hardcoded trail backbone with ALL connected infrastructure
    This finds tributaries, diversions, pump stations, treatment plants, etc.
    that connect to the main trail pathway
    """
    
    if not backbone_nodes:
        return []
    
    # Build parameterized query for all backbone nodes
    placeholders = ','.join(f'${i+1}' for i in range(len(backbone_nodes)))
    
    query = f"""
    WITH RECURSIVE connected_infrastructure AS (
        -- Start from all backbone nodes
        SELECT nt.short_code, 0 as depth, 'backbone' as source_type
        FROM network_topology nt 
        WHERE nt.short_code IN ({placeholders})
        AND nt.is_active = true
        
        UNION ALL
        
        -- Find all infrastructure connected to backbone or previously found nodes
        SELECT nt.short_code, ci.depth + 1, 
               CASE 
                   WHEN nt.type IN ('STR', 'PS', 'WTP', 'WWTP') THEN 'key_infrastructure'
                   WHEN nt.type = 'D' THEN 'delivery'
                   WHEN nt.type = 'CH' AND nt.river_name IS NOT NULL THEN 'named_channel'
                   ELSE 'other_infrastructure'
               END as source_type
        FROM connected_infrastructure ci
        JOIN network_topology nt ON (
            nt.from_node = ci.short_code OR nt.to_node = ci.short_code OR
            ci.short_code = nt.from_node OR ci.short_code = nt.to_node
        )
        WHERE ci.depth < $1
        AND nt.is_active = true
        AND nt.short_code NOT IN (
            SELECT short_code FROM connected_infrastructure
        )
        -- Include all infrastructure types for comprehensive trails
        AND nt.type IN ('STR', 'PS', 'WTP', 'WWTP', 'CH', 'D', 'OM', 'NP', 'PR', 'RFS')
    )
    SELECT DISTINCT short_code, source_type
    FROM connected_infrastructure 
    WHERE source_type != 'backbone'  -- Don't return backbone nodes we already have
    ORDER BY 
        CASE source_type
            WHEN 'key_infrastructure' THEN 1
            WHEN 'delivery' THEN 2  
            WHEN 'named_channel' THEN 3
            ELSE 4
        END,
        short_code;
    """
    
    # Add max_depth as the first parameter, then all backbone nodes
    params = [max_depth] + backbone_nodes
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    
    expanded_nodes = [row["short_code"] for row in rows]
    
    # Log expansion details for debugging
    infrastructure_counts = {}
    for row in rows:
        source_type = row["source_type"]
        infrastructure_counts[source_type] = infrastructure_counts.get(source_type, 0) + 1
    
    print(f"🔍 Infrastructure expansion from {primary_reservoir}:")
    for infra_type, count in infrastructure_counts.items():
        print(f"  • {infra_type}: {count} elements")
    
    return expanded_nodes


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