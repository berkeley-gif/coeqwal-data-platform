"""
Enhanced network traversal with multiple connection strategies
Improves connectivity results by using multiple approaches to find network relationships
Todo: make accurate
"""

import asyncpg
from fastapi import HTTPException
from typing import Optional, Set, List, Dict, Any
import json


async def enhanced_network_traversal(
    db_pool: asyncpg.Pool,
    short_code: str,
    direction: str = "both",
    max_depth: int = 10,
    include_arcs: bool = True
) -> Dict[str, Any]:
    """
    Enhanced network traversal using multiple connectivity strategies:
    1. Direct from_node/to_node connections (primary)
    2. Spatial proximity connections (secondary)
    3. River mile sequence connections (tertiary)
    4. Same river/stream connections (quaternary)
    """
    try:
        visited_elements = set()
        all_features = []
        
        # Strategy 1: Direct connectivity traversal
        direct_features = await _direct_connectivity_traversal(
            db_pool, short_code, direction, max_depth, visited_elements
        )
        all_features.extend(direct_features)
        
        # Strategy 2: Enhanced proximity-based connections
        if len(all_features) < 20:  # If we didn't find many connections
            proximity_features = await _proximity_based_traversal(
                db_pool, short_code, direction, max_depth//2, visited_elements
            )
            all_features.extend(proximity_features)
        
        # Strategy 3: River mile sequence connections
        if len(all_features) < 15:  # Still need more connections
            river_features = await _river_sequence_traversal(
                db_pool, short_code, direction, max_depth//3, visited_elements
            )
            all_features.extend(river_features)
        
        # Remove duplicates and convert to GeoJSON
        unique_features = _deduplicate_features(all_features)
        
        return {
            "type": "FeatureCollection",
            "features": unique_features,
            "metadata": {
                "start_element": short_code,
                "direction": direction,
                "max_depth": max_depth,
                "total_features": len(unique_features),
                "strategies_used": ["direct", "proximity", "river_sequence"],
                "includes_arcs": include_arcs
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Enhanced traversal error: {str(e)}")


async def _direct_connectivity_traversal(
    db_pool: asyncpg.Pool,
    short_code: str,
    direction: str,
    max_depth: int,
    visited_elements: Set[str]
) -> List[Dict[str, Any]]:
    """Primary traversal using from_node/to_node connections"""
    
    # Build direction-specific traversal query
    if direction == "upstream":
        traversal_condition = """
        nt.to_node = prev.short_code OR 
        nt.short_code = prev.from_node
        """
    elif direction == "downstream":
        traversal_condition = """
        nt.from_node = prev.short_code OR 
        nt.short_code = prev.to_node
        """
    else:  # both
        traversal_condition = """
        nt.from_node = prev.short_code OR 
        nt.to_node = prev.short_code OR
        nt.short_code = prev.from_node OR
        nt.short_code = prev.to_node
        """
    
    query = f"""
    WITH RECURSIVE network_traversal AS (
        -- Base: Start with clicked element
        SELECT 
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.connectivity_status,
            nt.river_name, nt.arc_name, nt.type, nt.river_mile,
            0 as depth, 'direct' as strategy,
            ARRAY[nt.short_code] as path
        FROM network_topology nt
        WHERE nt.short_code = $1
        
        UNION ALL
        
        -- Recursive: Follow connections
        SELECT 
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.connectivity_status,
            nt.river_name, nt.arc_name, nt.type, nt.river_mile,
            prev.depth + 1, 'direct',
            prev.path || nt.short_code
        FROM network_traversal prev
        JOIN network_topology nt ON ({traversal_condition})
        WHERE prev.depth < $2
        AND NOT (nt.short_code = ANY(prev.path))
        AND nt.connectivity_status = 'connected'
    )
    SELECT 
        trav.*,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_traversal trav
    LEFT JOIN network_gis ng ON trav.short_code = ng.short_code
    WHERE ng.geom IS NOT NULL
    ORDER BY trav.depth, trav.schematic_type;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, short_code, max_depth)
    
    features = []
    for row in rows:
        visited_elements.add(row['short_code'])
        feature = _create_geojson_feature(row)
        if feature:
            features.append(feature)
    
    return features


async def _proximity_based_traversal(
    db_pool: asyncpg.Pool,
    start_code: str,
    direction: str,
    max_depth: int,
    visited_elements: Set[str]
) -> List[Dict[str, Any]]:
    """Secondary traversal using spatial proximity for disconnected elements"""
    
    query = """
    WITH start_element AS (
        SELECT nt.short_code, nt.river_name, nt.type, ng.geom
        FROM network_topology nt
        JOIN network_gis ng ON nt.short_code = ng.short_code
        WHERE nt.short_code = $1
    ),
    proximity_elements AS (
        SELECT DISTINCT
            nt.id, nt.short_code, nt.schematic_type,
            nt.from_node, nt.to_node, nt.connectivity_status,
            nt.river_name, nt.arc_name, nt.type, nt.river_mile,
            ST_Distance(ng.geom, se.geom) as distance_meters,
            'proximity' as strategy
        FROM network_topology nt
        JOIN network_gis ng ON nt.short_code = ng.short_code
        CROSS JOIN start_element se
        WHERE nt.short_code != $1
        AND (
            -- Same river system
            (nt.river_name = se.river_name AND nt.river_name IS NOT NULL) OR
            -- Same element type
            (nt.type = se.type AND nt.type IS NOT NULL) OR
            -- Within 5km spatial distance
            ST_Distance(ng.geom, se.geom) < 5000
        )
        AND ST_Distance(ng.geom, se.geom) < 10000  -- Max 10km
        ORDER BY distance_meters
        LIMIT $2
    )
    SELECT 
        pe.*,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM proximity_elements pe
    JOIN network_gis ng ON pe.short_code = ng.short_code;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, start_code, max_depth * 5)
    
    features = []
    for row in rows:
        if row['short_code'] not in visited_elements:
            visited_elements.add(row['short_code'])
            feature = _create_geojson_feature(row)
            if feature:
                features.append(feature)
    
    return features


async def _river_sequence_traversal(
    db_pool: asyncpg.Pool,
    start_code: str,
    direction: str,
    max_depth: int,
    visited_elements: Set[str]
) -> List[Dict[str, Any]]:
    """Tertiary traversal using river mile sequences"""
    
    # Get the starting element's river info
    start_query = """
    SELECT river_name, river_mile, type
    FROM network_topology
    WHERE short_code = $1 AND river_mile IS NOT NULL;
    """
    
    async with db_pool.acquire() as conn:
        start_row = await conn.fetchrow(start_query, start_code)
    
    if not start_row or not start_row['river_mile']:
        return []
    
    # Find elements on the same river within reasonable mile range
    river_query = """
    SELECT DISTINCT
        nt.id, nt.short_code, nt.schematic_type,
        nt.from_node, nt.to_node, nt.connectivity_status,
        nt.river_name, nt.arc_name, nt.type, nt.river_mile,
        ABS(nt.river_mile - $2) as mile_distance,
        'river_sequence' as strategy
    FROM network_topology nt
    JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.river_name = $3
    AND nt.river_mile IS NOT NULL
    AND nt.short_code != $1
    AND ABS(nt.river_mile - $2) <= 50  -- Within 50 river miles
    ORDER BY mile_distance
    LIMIT $4;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            river_query, 
            start_code, 
            float(start_row['river_mile']), 
            start_row['river_name'],
            max_depth * 3
        )
    
    features = []
    for row in rows:
        if row['short_code'] not in visited_elements:
            visited_elements.add(row['short_code'])
            
            # Add geometry
            geom_query = "SELECT ST_AsGeoJSON(geom) as geometry, geometry_type FROM network_gis WHERE short_code = $1;"
            async with db_pool.acquire() as conn:
                geom_row = await conn.fetchrow(geom_query, row['short_code'])
            
            if geom_row:
                feature = _create_geojson_feature({**dict(row), **dict(geom_row)})
                if feature:
                    features.append(feature)
    
    return features


def _create_geojson_feature(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Create a GeoJSON feature from database row"""
    try:
        geometry = json.loads(row['geometry']) if row.get('geometry') else None
        if not geometry:
            return None
        
        properties = {
            'id': row['id'],
            'short_code': row['short_code'],
            'type': row['schematic_type'],
            'connectivity_status': row['connectivity_status'],
            'element_type': row['type'],
            'strategy': row.get('strategy', 'direct')
        }
        
        # Add depth if available
        if 'depth' in row:
            properties['depth'] = row['depth']
        
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
                'from_node': row['from_node'],
                'to_node': row['to_node'],
                'display_name': row['arc_name'] or f"{row['from_node']} → {row['to_node']}"
            })
        
        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": properties
        }
        
    except Exception as e:
        print(f"Error creating feature for {row.get('short_code', 'unknown')}: {e}")
        return None


def _deduplicate_features(features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate features based on short_code"""
    seen_codes = set()
    unique_features = []
    
    for feature in features:
        short_code = feature['properties']['short_code']
        if short_code not in seen_codes:
            seen_codes.add(short_code)
            unique_features.append(feature)
    
    return unique_features


async def get_enhanced_connectivity_stats(db_pool: asyncpg.Pool) -> Dict[str, Any]:
    """Get comprehensive connectivity statistics for analysis"""
    
    query = """
    WITH connectivity_analysis AS (
        -- Arc connectivity
        SELECT 
            'arcs' as element_type,
            COUNT(*) as total_elements,
            COUNT(CASE WHEN from_node IS NOT NULL AND to_node IS NOT NULL THEN 1 END) as fully_connected,
            COUNT(CASE WHEN from_node IS NOT NULL OR to_node IS NOT NULL THEN 1 END) as partially_connected
        FROM network_topology 
        WHERE schematic_type = 'arc'
        
        UNION ALL
        
        -- Node connectivity via arcs
        SELECT 
            'nodes' as element_type,
            COUNT(DISTINCT n.short_code) as total_elements,
            COUNT(DISTINCT CASE WHEN a.short_code IS NOT NULL THEN n.short_code END) as fully_connected,
            COUNT(DISTINCT CASE WHEN a.short_code IS NOT NULL THEN n.short_code END) as partially_connected
        FROM network_topology n
        LEFT JOIN network_topology a ON (
            a.from_node = n.short_code OR a.to_node = n.short_code
        )
        WHERE n.schematic_type = 'node'
    ),
    river_systems AS (
        SELECT 
            river_name,
            COUNT(*) as element_count,
            COUNT(CASE WHEN river_mile IS NOT NULL THEN 1 END) as with_mile_markers
        FROM network_topology
        WHERE river_name IS NOT NULL
        GROUP BY river_name
        HAVING COUNT(*) >= 5  -- Focus on major river systems
        ORDER BY element_count DESC
        LIMIT 10
    )
    SELECT 
        json_build_object(
            'connectivity_summary', json_agg(ca.*),
            'major_river_systems', (SELECT json_agg(rs.*) FROM river_systems rs)
        ) as analysis_result
    FROM connectivity_analysis ca;
    """
    
    async with db_pool.acquire() as conn:
        result = await conn.fetchval(query)
    
    return json.loads(result) if result else {}


async def get_network_gaps_analysis(db_pool: asyncpg.Pool) -> Dict[str, Any]:
    """Identify specific network gaps and suggest improvements"""
    
    query = """
    WITH network_gaps AS (
        -- Find arcs without proper connectivity
        SELECT 
            'missing_arc_connections' as gap_type,
            short_code,
            from_node,
            to_node,
            river_name,
            arc_name,
            'Arc missing from_node or to_node' as issue
        FROM network_topology
        WHERE schematic_type = 'arc'
        AND (from_node IS NULL OR to_node IS NULL)
        
        UNION ALL
        
        -- Find isolated nodes (no connecting arcs)
        SELECT 
            'isolated_nodes' as gap_type,
            n.short_code,
            null as from_node,
            null as to_node,
            n.river_name,
            null as arc_name,
            'Node with no connecting arcs' as issue
        FROM network_topology n
        WHERE n.schematic_type = 'node'
        AND NOT EXISTS (
            SELECT 1 FROM network_topology a
            WHERE a.schematic_type = 'arc'
            AND (a.from_node = n.short_code OR a.to_node = n.short_code)
        )
        
        UNION ALL
        
        -- Find referenced but missing nodes
        SELECT 
            'missing_referenced_nodes' as gap_type,
            ref_node as short_code,
            null as from_node,
            null as to_node,
            null as river_name,
            null as arc_name,
            'Node referenced by arc but not found in topology' as issue
        FROM (
            SELECT from_node as ref_node FROM network_topology WHERE schematic_type = 'arc' AND from_node IS NOT NULL
            UNION
            SELECT to_node as ref_node FROM network_topology WHERE schematic_type = 'arc' AND to_node IS NOT NULL
        ) refs
        WHERE ref_node NOT IN (
            SELECT short_code FROM network_topology WHERE schematic_type = 'node'
        )
    )
    SELECT 
        gap_type,
        COUNT(*) as gap_count,
        json_agg(
            json_build_object(
                'short_code', short_code,
                'issue', issue,
                'river_name', river_name,
                'arc_name', arc_name
            )
        ) as examples
    FROM network_gaps
    GROUP BY gap_type
    ORDER BY gap_count DESC;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    return {
        'network_gaps': [dict(row) for row in rows],
        'total_gap_types': len(rows),
        'improvement_suggestions': [
            "Fill missing from_node/to_node connections for arcs",
            "Create logical connections for isolated nodes",
            "Add missing node records for referenced nodes",
            "Implement spatial proximity connections",
            "Use river mile sequences for river system connectivity"
        ]
    }
