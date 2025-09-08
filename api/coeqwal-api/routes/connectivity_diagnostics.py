"""
Diagnose connectivity issues in network_topology
"""

import asyncpg
from typing import Dict, Any


async def diagnose_connectivity_problems(db_pool: asyncpg.Pool) -> Dict[str, Any]:
    """
    Diagnose the real connectivity problems in our network data
    """
    
    query = """
    -- Comprehensive connectivity analysis
    WITH connectivity_stats AS (
        SELECT 
            schematic_type,
            COUNT(*) as total_elements,
            COUNT(CASE WHEN from_node IS NOT NULL THEN 1 END) as has_from_node,
            COUNT(CASE WHEN to_node IS NOT NULL THEN 1 END) as has_to_node,
            COUNT(CASE WHEN from_node IS NOT NULL AND to_node IS NOT NULL THEN 1 END) as has_both_nodes,
            COUNT(CASE WHEN connectivity_status = 'connected' THEN 1 END) as marked_connected,
            COUNT(CASE WHEN has_gis = true THEN 1 END) as has_geometry
        FROM network_topology
        GROUP BY schematic_type
    ),
    sample_connections AS (
        -- Get sample of actual connections
        SELECT 
            short_code, schematic_type, from_node, to_node, river_name, type
        FROM network_topology 
        WHERE from_node IS NOT NULL AND to_node IS NOT NULL
        LIMIT 10
    ),
    missing_connections AS (
        -- Elements that should connect but don't
        SELECT 
            short_code, schematic_type, type, river_name, river_mile,
            CASE 
                WHEN short_code LIKE 'D_%' THEN 'diversion_pattern'
                WHEN short_code LIKE 'C_%' THEN 'channel_pattern' 
                WHEN short_code LIKE 'I_%' THEN 'inflow_pattern'
                WHEN river_name IS NOT NULL THEN 'river_sequence'
                ELSE 'unknown'
            END as potential_connection_type
        FROM network_topology
        WHERE (from_node IS NULL OR to_node IS NULL)
        AND connectivity_status = 'connected'
        AND has_gis = true
        ORDER BY 
            CASE 
                WHEN short_code LIKE 'D_%' THEN 1
                WHEN short_code LIKE 'C_%' THEN 2
                WHEN river_name IS NOT NULL THEN 3
                ELSE 4
            END,
            short_code
        LIMIT 20
    )
    SELECT 
        'connectivity_stats' as analysis_type,
        json_agg(cs) as stats
    FROM connectivity_stats cs
    
    UNION ALL
    
    SELECT 
        'sample_connections' as analysis_type,
        json_agg(sc) as stats  
    FROM sample_connections sc
    
    UNION ALL
    
    SELECT 
        'missing_connections' as analysis_type,
        json_agg(mc) as stats
    FROM missing_connections mc;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    # Organize results
    result = {}
    for row in rows:
        result[row['analysis_type']] = row['stats']
    
    return result


async def suggest_connectivity_improvements(db_pool: asyncpg.Pool) -> Dict[str, Any]:
    """
    Suggest specific improvements for connectivity
    """
    
    query = """
    -- Analyze potential connections we could make
    WITH pattern_analysis AS (
        -- D_ arcs that should connect to their base nodes
        SELECT 
            'diversion_to_source' as connection_type,
            COUNT(*) as potential_connections,
            array_agg(short_code) as examples
        FROM network_topology d
        WHERE d.short_code LIKE 'D_%'
        AND (d.from_node IS NULL OR d.to_node IS NULL)
        AND EXISTS (
            SELECT 1 FROM network_topology n 
            WHERE n.short_code = SPLIT_PART(SUBSTRING(d.short_code, 3), '_', 1)
            AND n.schematic_type = 'node'
        )
        
        UNION ALL
        
        -- River sequences that could be connected by mile
        SELECT 
            'river_mile_sequence' as connection_type,
            COUNT(*) as potential_connections,
            array_agg(short_code ORDER BY river_mile LIMIT 5) as examples
        FROM network_topology
        WHERE river_name IS NOT NULL 
        AND river_mile IS NOT NULL
        AND (from_node IS NULL OR to_node IS NULL)
        AND schematic_type = 'node'
        
        UNION ALL
        
        -- Channel connections (C_ arcs)
        SELECT 
            'channel_connections' as connection_type,
            COUNT(*) as potential_connections,
            array_agg(short_code) as examples
        FROM network_topology
        WHERE short_code LIKE 'C_%'
        AND (from_node IS NULL OR to_node IS NULL)
    )
    SELECT json_agg(pa) as suggestions
    FROM pattern_analysis pa;
    """
    
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(query)
    
    return {"improvement_suggestions": row['suggestions'] if row else []}
