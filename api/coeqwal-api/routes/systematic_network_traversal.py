"""
Systematic network traversal in three passes:
1. Geopackage-only connections (most reliable)
2. XML connections with geometry (fill gaps)
3. XML connections without geometry (logical connections)
"""

import asyncpg
from typing import List, Dict, Any, Set
import json


async def systematic_network_traversal(
    db_pool: asyncpg.Pool,
    short_code: str,
    direction: str = "both"
) -> Dict[str, Any]:
    """
    Systematic three-pass network traversal
    No depth limits - get the complete network
    """
    
    # Pass 1: Geopackage-only connections
    print(f"🔍 Pass 1: Geopackage-only connections for {short_code}")
    pass1_features = await _pass1_geopackage_only(db_pool, short_code, direction)
    
    # Pass 2: Add XML connections with geometry
    print(f"🔍 Pass 2: XML connections with geometry for {short_code}")
    pass2_features = await _pass2_xml_with_geometry(db_pool, short_code, direction, pass1_features)
    
    # Pass 3: Add XML connections without geometry
    print(f"🔍 Pass 3: XML connections without geometry for {short_code}")
    pass3_features = await _pass3_xml_without_geometry(db_pool, short_code, direction, pass1_features + pass2_features)
    
    # Combine all passes
    all_features = pass1_features + pass2_features + pass3_features
    
    # Deduplicate by short_code
    seen_codes = set()
    final_features = []
    for feature in all_features:
        short_code_key = feature['properties']['short_code']
        if short_code_key not in seen_codes:
            seen_codes.add(short_code_key)
            final_features.append(feature)
    
    return {
        "type": "FeatureCollection",
        "features": final_features,
        "metadata": {
            "start_element": short_code,
            "direction": direction,
            "total_features": len(final_features),
            "pass1_geopackage": len(pass1_features),
            "pass2_xml_with_geometry": len(pass2_features),
            "pass3_xml_without_geometry": len(pass3_features),
            "approach": "systematic_three_pass",
            "no_depth_limit": True
        }
    }


async def _pass1_geopackage_only(
    db_pool: asyncpg.Pool,
    start_code: str,
    direction: str
) -> List[Dict[str, Any]]:
    """
    Pass 1: Only use geopackage elements and their direct connections
    Most reliable data with guaranteed geometry
    """
    
    if direction == "upstream":
        connection_logic = "nt.to_node = prev.short_code"
    elif direction == "downstream":  
        connection_logic = "nt.from_node = prev.short_code"
    else:  # both
        connection_logic = "(nt.from_node = prev.short_code OR nt.to_node = prev.short_code)"
    
    query = f"""
    WITH RECURSIVE geopackage_traversal AS (
        -- Start: Must be geopackage element
        SELECT 
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.type, nt.subtype,
            nt.river_name, nt.arc_name, nt.river_mile,
            0 as depth, 'geopackage_direct' as strategy,
            ARRAY[nt.short_code] as path
        FROM network_topology nt
        WHERE nt.short_code = $1
        AND nt.geopackage_short_code IS NOT NULL  -- Must be geopackage
        AND nt.connectivity_status = 'connected'
        
        UNION ALL
        
        -- Recursive: Only follow geopackage connections
        SELECT 
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.type, nt.subtype,
            nt.river_name, nt.arc_name, nt.river_mile,
            prev.depth + 1, 'geopackage_direct',
            prev.path || nt.short_code
        FROM geopackage_traversal prev
        JOIN network_topology nt ON ({connection_logic})
        WHERE NOT (nt.short_code = ANY(prev.path))  -- Prevent cycles
        AND nt.geopackage_short_code IS NOT NULL  -- Only geopackage elements
        AND nt.connectivity_status = 'connected'
    )
    SELECT 
        gt.*,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM geopackage_traversal gt
    JOIN network_gis ng ON gt.short_code = ng.short_code  -- Must have geometry
    ORDER BY gt.depth, gt.schematic_type;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, start_code)
    
    features = _convert_rows_to_features(rows)
    print(f"✅ Pass 1: Found {len(features)} geopackage connections")
    return features


async def _pass2_xml_with_geometry(
    db_pool: asyncpg.Pool,
    start_code: str,
    direction: str,
    existing_features: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Pass 2: Add XML elements that have geometry and connect to existing network
    Fill gaps with reliable spatial data
    """
    
    # Get existing short codes to avoid duplicates
    existing_codes = {f['properties']['short_code'] for f in existing_features}
    
    if not existing_codes:
        print("⚠️ Pass 2: No existing features to connect to")
        return []
    
    # Convert to SQL array format
    existing_codes_list = list(existing_codes)
    placeholders = ','.join(f'${i+2}' for i in range(len(existing_codes_list)))
    
    query = f"""
    SELECT DISTINCT
        nt.id, nt.short_code, nt.schematic_type,
        nt.from_node, nt.to_node, nt.type, nt.subtype,
        nt.river_name, nt.arc_name, nt.river_mile,
        1 as depth, 'xml_with_geometry' as strategy,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    JOIN network_gis ng ON nt.short_code = ng.short_code  -- Must have geometry
    WHERE nt.xml_short_code IS NOT NULL  -- XML element
    AND nt.connectivity_status = 'connected'
    AND nt.short_code NOT IN ({placeholders})  -- Not already included
    AND (
        -- Connect to existing network via from_node/to_node
        nt.from_node IN ({placeholders}) OR
        nt.to_node IN ({placeholders}) OR
        -- Or existing elements connect to this one
        EXISTS (
            SELECT 1 FROM network_topology existing
            WHERE existing.short_code IN ({placeholders})
            AND (existing.from_node = nt.short_code OR existing.to_node = nt.short_code)
        )
    )
    LIMIT 200;  -- Reasonable limit to prevent explosion
    """
    
    params = [start_code] + existing_codes_list + existing_codes_list + existing_codes_list
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    
    features = _convert_rows_to_features(rows)
    print(f"✅ Pass 2: Found {len(features)} XML elements with geometry")
    return features


async def _pass3_xml_without_geometry(
    db_pool: asyncpg.Pool,
    start_code: str,
    direction: str,
    existing_features: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Pass 3: Add XML elements without geometry that connect to existing network
    Get logical connections for completeness
    """
    
    # Get existing short codes
    existing_codes = {f['properties']['short_code'] for f in existing_features}
    
    if not existing_codes:
        print("⚠️ Pass 3: No existing features to connect to")
        return []
    
    existing_codes_list = list(existing_codes)
    placeholders = ','.join(f'${i+2}' for i in range(len(existing_codes_list)))
    
    query = f"""
    SELECT DISTINCT
        nt.id, nt.short_code, nt.schematic_type,
        nt.from_node, nt.to_node, nt.type, nt.subtype,
        nt.river_name, nt.arc_name, nt.river_mile,
        2 as depth, 'xml_without_geometry' as strategy,
        NULL as geometry,
        'logical' as geometry_type
    FROM network_topology nt
    WHERE nt.xml_short_code IS NOT NULL  -- XML element
    AND nt.connectivity_status = 'connected'
    AND nt.short_code NOT IN ({placeholders})  -- Not already included
    AND NOT EXISTS (
        SELECT 1 FROM network_gis ng WHERE ng.short_code = nt.short_code
    )  -- No geometry
    AND (
        -- Connect to existing network
        nt.from_node IN ({placeholders}) OR
        nt.to_node IN ({placeholders}) OR
        EXISTS (
            SELECT 1 FROM network_topology existing
            WHERE existing.short_code IN ({placeholders})
            AND (existing.from_node = nt.short_code OR existing.to_node = nt.short_code)
        )
    )
    LIMIT 100;  -- Smaller limit for logical connections
    """
    
    params = [start_code] + existing_codes_list + existing_codes_list + existing_codes_list
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    
    # For elements without geometry, create a simple point feature at 0,0
    features = []
    for row in rows:
        try:
            properties = {
                'id': row['id'],
                'short_code': row['short_code'],
                'type': row['schematic_type'],
                'element_type': row['type'] or '',
                'subtype': row['subtype'],
                'depth': row['depth'],
                'connectivity_status': 'connected',
                'strategy': row['strategy'],
                'has_geometry': False
            }
            
            # Add type-specific properties
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'river_mile': float(row['river_mile']) if row['river_mile'] else None,
                    'display_name': row['river_name'] or row['short_code']
                })
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'] or '',
                    'to_node': row['to_node'] or '',
                    'display_name': row['arc_name'] or f"{row['from_node'] or ''} → {row['to_node'] or ''}"
                })
            
            # Create a logical feature without real geometry
            features.append({
                "type": "Feature",
                "geometry": None,  # No geometry for logical connections
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing {row.get('short_code', 'unknown')}: {e}")
            continue
    
    print(f"✅ Pass 3: Found {len(features)} XML logical connections")
    return features


def _convert_rows_to_features(rows) -> List[Dict[str, Any]]:
    """Convert database rows to GeoJSON features"""
    features = []
    
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if not geometry:
                continue  # Skip elements without geometry in passes 1 and 2
                
            properties = {
                'id': row['id'],
                'short_code': row['short_code'],
                'type': row['schematic_type'],
                'element_type': row['type'] or '',
                'subtype': row['subtype'],
                'depth': row['depth'],
                'connectivity_status': 'connected',
                'strategy': row['strategy'],
                'has_geometry': True
            }
            
            # Add type-specific properties
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'river_mile': float(row['river_mile']) if row['river_mile'] else None,
                    'display_name': row['river_name'] or row['short_code']
                })
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'] or '',
                    'to_node': row['to_node'] or '',
                    'display_name': row['arc_name'] or f"{row['from_node'] or ''} → {row['to_node'] or ''}"
                })
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return features
