"""
Simple Water Trail API - Direct query approach
Gets key California water infrastructure without complex hardcoded trails
"""

import asyncpg
from typing import List, Dict, Any
import json


async def get_california_water_infrastructure(
    db_pool: asyncpg.Pool,
    trail_type: str = "infrastructure"
) -> Dict[str, Any]:
    """
    Get all key California water infrastructure directly from database
    Much simpler than hardcoded trails - just query for what we want
    """
    
    query = """
    SELECT 
        nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
        nt.from_node, nt.to_node, nt.river_name, nt.arc_name,
        nt.hydrologic_region, nt.name,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.is_active = true
    AND (
        -- All reservoirs (STR)
        nt.type = 'STR' OR
        -- All pump stations (PS) 
        nt.type = 'PS' OR
        -- All treatment plants
        nt.type IN ('WTP', 'WWTP') OR
        -- Major river nodes (Sacramento, San Joaquin, American, Feather)
        (nt.type = 'CH' AND nt.river_name LIKE '%Sacramento River%') OR
        (nt.type = 'CH' AND nt.river_name LIKE '%San Joaquin River%') OR
        (nt.type = 'CH' AND nt.river_name LIKE '%American River%') OR
        (nt.type = 'CH' AND nt.river_name LIKE '%Feather River%') OR
        -- Key junction nodes
        nt.short_code IN ('SAC000', 'SAC043', 'SAC083', 'SJRE', 'SJRW', 'MDOTA', 'KSWCK', 'NTOMA') OR
        -- Major delivery/diversion arcs
        (nt.schematic_type = 'arc' AND nt.type = 'D' AND nt.from_node IN (
            'SHSTA', 'OROVL', 'FOLSM', 'SLUIS', 'HETCH', 'TRNTY', 'WKYTN'
        ))
    )
    ORDER BY 
        CASE nt.type 
            WHEN 'STR' THEN 1 
            WHEN 'PS' THEN 2 
            WHEN 'WTP' THEN 3 
            WHEN 'WWTP' THEN 4 
            WHEN 'CH' THEN 5
            WHEN 'D' THEN 6
            ELSE 7 
        END,
        nt.short_code
    LIMIT 200;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    features = []
    reservoirs = []
    pump_stations = []
    treatment_plants = []
    river_nodes = []
    delivery_arcs = []
    
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
                'name': row['name']
            }
            
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'display_name': row['name'] or row['river_name'] or row['short_code'],
                    'infrastructure_type': _get_infrastructure_type(row['type'])
                })
                
                # Categorize for summary
                if row['type'] == 'STR':
                    reservoirs.append(row['short_code'])
                elif row['type'] == 'PS':
                    pump_stations.append(row['short_code'])
                elif row['type'] in ('WTP', 'WWTP'):
                    treatment_plants.append(row['short_code'])
                elif row['type'] == 'CH':
                    river_nodes.append(row['short_code'])
                    
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'],
                    'to_node': row['to_node'],
                    'display_name': row['arc_name'] or row['name'] or f"{row['from_node']} → {row['to_node']}"
                })
                
                if row['type'] == 'D':
                    delivery_arcs.append(row['short_code'])
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing infrastructure {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "trail_type": trail_type,
            "total_features": len(features),
            "reservoirs": len(reservoirs),
            "pump_stations": len(pump_stations), 
            "treatment_plants": len(treatment_plants),
            "river_nodes": len(river_nodes),
            "delivery_arcs": len(delivery_arcs),
            "infrastructure_summary": {
                "reservoirs": reservoirs[:10],  # First 10
                "pump_stations": pump_stations[:5],
                "treatment_plants": treatment_plants[:5],
                "key_river_nodes": river_nodes[:10]
            },
            "approach": "direct_infrastructure_query",
            "foundation": "active_network_topology_elements"
        }
    }


async def get_reservoir_water_trail(
    db_pool: asyncpg.Pool,
    reservoir_short_code: str,
    max_depth: int = 5
) -> Dict[str, Any]:
    """
    Get water trail from a specific reservoir using direct connectivity
    Simpler than hardcoded trails - just follow the actual connections
    """
    
    query = """
    WITH RECURSIVE water_trail AS (
        -- Start from reservoir
        SELECT 
            nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
            nt.from_node, nt.to_node, nt.river_name, nt.arc_name, nt.name,
            0 as depth,
            ARRAY[nt.short_code] as path
        FROM network_topology nt
        WHERE nt.short_code = $1
        AND nt.is_active = true
        
        UNION ALL
        
        -- Follow downstream connections
        SELECT 
            nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
            nt.from_node, nt.to_node, nt.river_name, nt.arc_name, nt.name,
            wt.depth + 1,
            wt.path || nt.short_code
        FROM water_trail wt
        JOIN network_topology nt ON (
            nt.from_node = wt.short_code OR 
            (wt.schematic_type = 'arc' AND nt.short_code = wt.to_node)
        )
        WHERE wt.depth < $2
        AND NOT (nt.short_code = ANY(wt.path))
        AND nt.is_active = true
        AND (
            -- Include key infrastructure
            nt.type IN ('STR', 'PS', 'WTP', 'WWTP') OR
            -- Include major rivers
            nt.river_name LIKE '%Sacramento River%' OR
            nt.river_name LIKE '%San Joaquin River%' OR
            nt.river_name LIKE '%American River%' OR
            nt.river_name LIKE '%Feather River%' OR
            -- Include delivery arcs from reservoirs
            (nt.type = 'D' AND nt.from_node IN ('SHSTA', 'OROVL', 'FOLSM', 'SLUIS'))
        )
    )
    SELECT 
        wt.*, 
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM water_trail wt
    LEFT JOIN network_gis ng ON wt.short_code = ng.short_code
    ORDER BY wt.depth, wt.type, wt.short_code;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, reservoir_short_code, max_depth)
    
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
                'depth': row['depth'],
                'connectivity_status': 'connected',
                'trail_element': True,
                'name': row['name']
            }
            
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'display_name': row['name'] or row['river_name'] or row['short_code'],
                    'infrastructure_type': _get_infrastructure_type(row['type'])
                })
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'],
                    'to_node': row['to_node'],
                    'display_name': row['arc_name'] or row['name'] or f"{row['from_node']} → {row['to_node']}"
                })
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing trail element {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "start_reservoir": reservoir_short_code,
            "max_depth": max_depth,
            "total_features": len(features),
            "approach": "direct_connectivity_trail",
            "foundation": "recursive_network_traversal"
        }
    }


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
