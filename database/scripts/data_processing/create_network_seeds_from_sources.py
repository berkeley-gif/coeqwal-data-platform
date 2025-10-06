#!/usr/bin/env python3

# Step 1 for creating database seed tables

"""
Create normalized network seed files from original data sources
Integrates geopackage (spatial + attributes) with XML schematic (connectivity)

Data Sources:
1. CalSim_arcs_geopackage.csv - 2,118 arcs with GIS + attributes
2. CalSim_nodes_geopackage.csv - 1,401 nodes with GIS + attributes  
3. CS3_NetworkSchematic_Integrated_11.28.23.xml - Complete connectivity graph

Output Files:
1. network_topology.csv - Master connectivity (arcs + nodes)
2. network_arc.csv - Arc-specific attributes + geometry
3. network_node.csv - Node-specific attributes + geometry
4. network_gis.csv - PostGIS spatial data for all elements
"""

import pandas as pd
import csv
import xml.etree.ElementTree as ET
from datetime import datetime
import json

# Increase CSV field size limit for large WKT geometries
csv.field_size_limit(1000000)

def analyze_arc_connectivity(from_node_text, to_node_text):
    """Analyze arc connectivity status based on from_node and to_node text values"""
    if from_node_text and to_node_text:
        return 'connected'
    elif from_node_text or to_node_text:
        return 'partial'  # Has one end but not both
    else:
        return 'unconnected'

def analyze_node_connectivity(node_id, all_arcs):
    """Analyze node connectivity by checking if it appears in arc from_node/to_node"""
    connected_arcs = []
    for _, arc in all_arcs.iterrows():  # iterate over DataFrame rows
        if arc.get('From_Node') == node_id or arc.get('To_Node') == node_id:
            connected_arcs.append(arc['Arc_ID'])
    
    if len(connected_arcs) > 0:
        return 'connected'
    else:
        return 'isolated'

def map_hydrologic_region(hr_code):
    """Map HR code to hydrologic_region_id"""
    hr_map = {
        'SAC': 1,     # Sacramento River Basin
        'SJR': 2,     # San Joaquin River Basin  
        'DELTA': 3,   # Sacramento–San Joaquin Delta
        'TULARE': 4,  # Tulare Basin
        'SOCAL': 5,   # Southern California
        'SC': 1       # Sacramento variant (likely typo/abbreviation)
    }
    
    if hr_code and hr_code.strip() in hr_map:
        return hr_map[hr_code.strip()]
    return None  # Unknown - NULL in database

def map_arc_type(type_val, subtype_val):
    """Map geopackage Type/Sub_Type to arc_type_id"""
    # Load arc type mappings
    arc_types = {}
    with open('database/seed_tables/02_entity_system/network_arc_type.csv', 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            arc_types[row['short_code']] = i
    
    # Try Type-SubType combination first
    if type_val and subtype_val:
        combo = f"{type_val}-{subtype_val}"
        if combo in arc_types:
            return arc_types[combo]
    
    # Try Type only
    if type_val and type_val in arc_types:
        return arc_types[type_val]
    
    return None  # Unknown - will be NULL in database

def map_node_type(type_val, subtype_val):
    """Map geopackage Type/Sub_Type to node_type_id"""
    # Load node type mappings
    node_types = {}
    with open('database/seed_tables/02_entity_system/network_node_type.csv', 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            node_types[row['short_code']] = i
    
    # Try Type-SubType combination first
    if type_val and subtype_val:
        combo = f"{type_val}-{subtype_val}"
        if combo in node_types:
            return node_types[combo]
    
    # Try Type only
    if type_val and type_val in node_types:
        return node_types[type_val]
    
    return None  # Unknown - will be NULL in database

def main():
    print("Creating normalized network seed files from original sources...")
    
    # Load geopackage data
    print("\n Loading geopackage data...")
    gp_arcs = pd.read_csv('database/seed_tables/04_calsim_data/CalSim_arcs_geopackage.csv')
    gp_nodes = pd.read_csv('database/seed_tables/04_calsim_data/CalSim_nodes_geopackage.csv')
    
    print(f"   Geopackage arcs: {len(gp_arcs):,}")
    print(f"   Geopackage nodes: {len(gp_nodes):,}")
    
    # Create network_gis.csv (PostGIS spatial data)
    print("\n  Creating network_gis.csv...")
    create_network_gis(gp_arcs, gp_nodes)
    
    # Create network_arc.csv (arc attributes)
    print("\n Creating network_arc.csv...")
    create_network_arc(gp_arcs)
    
    # Create network_node.csv (node attributes)
    print("\n Creating network_node.csv...")
    create_network_node(gp_nodes, gp_arcs)
    
    # Create network_topology.csv (connectivity master)
    print("\n Creating network_topology.csv...")
    create_network_topology(gp_arcs, gp_nodes)
    
    print("\n All network seed files created successfully!")
    print("\nFiles created:")
    print("  - network_topology.csv (connectivity master)")
    print("  - network_arc.csv (arc details)")
    print("  - network_node.csv (node details)")
    print("  - network_gis.csv (PostGIS spatial data)")

def create_network_gis(gp_arcs, gp_nodes):
    """Create network_gis.csv with PostGIS geometry from geopackage"""
    
    gis_records = []
    
    # Add arc geometries
    for _, arc in gp_arcs.iterrows():
        if pd.notna(arc['WKT']) and pd.notna(arc['Arc_ID']):
            gis_records.append({
                'short_code': arc['Arc_ID'],
                'geometry_type': 'multilinestring',
                'geom_wkt': arc['WKT'],
                'srid': 4326,
                'source_id': 4,  # geopackage source
                'model_source_id': 1,  # calsim3
                'source_record_id': arc['Arc_ID'],
                'network_version_id': 12,  # network version
                'entity_type': 'arc',
                'is_primary': True,
                'coordinate_precision': 'high',
                'notes': f"Imported from geopackage | Type: {arc.get('Type', '')}/{arc.get('Sub_Type', '')} | HR: {arc.get('HR', '')}"
            })
    
    # Add node geometries  
    for _, node in gp_nodes.iterrows():
        if pd.notna(node['WKT']) and pd.notna(node['CalSim_ID']):
            gis_records.append({
                'short_code': node['CalSim_ID'],
                'geometry_type': 'point',
                'geom_wkt': node['WKT'],
                'srid': 4326,
                'source_id': 4,  # geopackage source
                'model_source_id': 1,  # calsim3
                'source_record_id': node['CalSim_ID'],
                'network_version_id': 12,  # network version
                'entity_type': 'node',
                'is_primary': True,
                'coordinate_precision': 'high',
                'notes': f"Imported from geopackage | Type: {node.get('Type', '')}/{node.get('Sub_Type', '')} | HR: {node.get('HR', '')}"
            })
    
    # Write to CSV
    df_gis = pd.DataFrame(gis_records)
    df_gis.to_csv('database/seed_tables/04_calsim_data/network_gis.csv', index=False)
    print(f"     Created unified network_gis.csv with {len(gis_records):,} spatial records")
    print(f"      - Points: {len([r for r in gis_records if r['geometry_type'] == 'point']):,}")
    print(f"      - Linestrings: {len([r for r in gis_records if r['geometry_type'] == 'multilinestring']):,}")
    print(f"      - PostGIS will handle mixed geometry types efficiently")

def create_network_arc(gp_arcs):
    """Create network_arc.csv from geopackage arc data"""
    
    arc_records = []
    
    for _, arc in gp_arcs.iterrows():
        if pd.notna(arc['Arc_ID']):
            # Map hydrologic region to ID
            hr_id = map_hydrologic_region(arc.get('HR'))
            if hr_id is None:
                print(f"   WARNING: Unknown HR code '{arc.get('HR')}' for arc {arc.get('Arc_ID')} - setting to NULL")
            
            arc_records.append({
                'short_code': arc['Arc_ID'],
                'calsim_id': arc.get('CalSim_ID', ''),
                'arc_id': arc['Arc_ID'],
                'arc_type_id': map_arc_type(arc.get('Type'), arc.get('Sub_Type')),
                'name': arc.get('NAME', ''),
                'description': '',
                'type': arc.get('Type', ''),
                'sub_type': arc.get('Sub_Type', ''),
                'from_node_id': None,  # Will be populated after nodes are loaded
                'to_node_id': None,    # Will be populated after nodes are loaded
                'hydrologic_region_id': hr_id,
                'shape_length': arc.get('Shape_Leng', 0),
                'model_source_id': 1,  # calsim3
                'source_id': 4,        # geopackage
                'is_reversible': None, # Unknown from geopackage
                'flow_capacity': None, # Unknown from geopackage
                'parent_arc_id': None, # No hierarchy in geopackage
                'integration_status_id': 1,
                'network_version_id': 12,
                'is_active': True,
                'connectivity_status': analyze_arc_connectivity(arc.get('From_Node'), arc.get('To_Node')),
                'has_gis': True if pd.notna(arc.get('WKT')) else False
            })
    
    # Write to CSV
    df_arcs = pd.DataFrame(arc_records)
    df_arcs.to_csv('database/seed_tables/04_calsim_data/network_arc.csv', index=False)
    print(f"   Created network_arc.csv with {len(arc_records):,} arc records")

def create_network_node(gp_nodes, gp_arcs):
    """Create network_node.csv from geopackage node data with connectivity analysis"""
    
    node_records = []
    
    for _, node in gp_nodes.iterrows():
        if pd.notna(node['CalSim_ID']):
            # Map hydrologic region to ID
            hr_id = map_hydrologic_region(node.get('HR'))
            if hr_id is None:
                print(f"   WARNING: Unknown HR code '{node.get('HR')}' for node {node.get('CalSim_ID')} - setting to NULL")
            
            # Extract coordinates from WKT
            lat, lon = extract_coordinates(node.get('WKT', ''))
            
            node_records.append({
                'short_code': node['CalSim_ID'],
                'calsim_id': node['CalSim_ID'],
                'node_type_id': map_node_type(node.get('Type'), node.get('Sub_Type')),
                'name': node['CalSim_ID'],  # Use CalSim_ID as name for now
                'description': node.get('Comment', ''),
                'riv_mi': node.get('Riv_Mi', None),
                'riv_name': node.get('Riv_Name', ''),
                'comment': node.get('Comment', ''),
                'c2vsim_gw': node.get('C2VSIM_GW', ''),
                'c2vsim_sw': node.get('C2VSIM_SW', ''),
                'type': node.get('Type', ''),
                'sub_type': node.get('Sub_Type', ''),
                'nrest_gage': node.get('Nrest_Gage', ''),
                'strm_code': node.get('Strm_Code', ''),
                'rm_ii': node.get('RM_II', ''),  # River mile indicator?
                'hydrologic_region_id': hr_id,
                'latitude': lat,
                'longitude': lon,
                'model_source_id': 1,  # calsim3
                'source_id': 4,        # geopackage
                'parent_node_id': None, # No hierarchy in geopackage
                'integration_status_id': 1,
                'network_version_id': 12,
                'is_active': True,
                'connectivity_status': analyze_node_connectivity(node['CalSim_ID'], gp_arcs),
                'has_gis': True if pd.notna(node.get('WKT')) else False
            })
    
    # Write to CSV
    df_nodes = pd.DataFrame(node_records)
    df_nodes.to_csv('database/seed_tables/04_calsim_data/network_node.csv', index=False)
    print(f"   Created network_node.csv with {len(node_records):,} node records")

def create_network_topology(gp_arcs, gp_nodes):
    """Create network_topology.csv (connectivity master) from both sources"""
    
    topo_records = []
    
    # Add arc connectivity records
    for _, arc in gp_arcs.iterrows():
        if pd.notna(arc['Arc_ID']):
            hr_id = map_hydrologic_region(arc.get('HR'))
            
            topo_records.append({
                'short_code': arc['Arc_ID'],
                'schematic_type': 'arc',
                'type': arc.get('Type', ''),
                'sub_type': arc.get('Sub_Type', ''),
                'hydrologic_region_id': hr_id,
                'network_version_id': 12,
                'is_active': True,
                'connectivity_status': analyze_arc_connectivity(arc.get('From_Node'), arc.get('To_Node')),
                'has_gis': True if pd.notna(arc.get('WKT')) else False
            })
    
    # Add node records (nodes don't have from_node/to_node)
    for _, node in gp_nodes.iterrows():
        if pd.notna(node['CalSim_ID']):
            hr_map = {'SAC': 1, 'SJR': 2, 'DELTA': 3, 'TUL': 4, 'CC': 5}
            hr_id = hr_map.get(node.get('HR'), 1)
            
            topo_records.append({
                'short_code': node['CalSim_ID'],
                'schematic_type': 'node',
                'from_node': '',  # Nodes don't have connectivity
                'to_node': '',    # Nodes don't have connectivity
                'type': node.get('Type', ''),
                'sub_type': node.get('Sub_Type', ''),
                'hydrologic_region_id': hr_id,
                'network_version_id': 12,
                'is_active': True,
                'connectivity_status': 'connected',
                'has_gis': True if pd.notna(node.get('WKT')) else False
            })
    
    # Write to CSV
    df_topo = pd.DataFrame(topo_records)
    df_topo.to_csv('database/seed_tables/04_calsim_data/network_topology.csv', index=False)
    print(f"   Created network_topology.csv with {len(topo_records):,} connectivity records")

def extract_coordinates(wkt_string):
    """Extract lat/lon from POINT WKT string"""
    if not wkt_string or not isinstance(wkt_string, str):
        return None, None
    
    try:
        # Extract coordinates from "POINT (lon lat)" format
        if 'POINT' in wkt_string:
            coords = wkt_string.split('(')[1].split(')')[0].strip()
            lon, lat = coords.split()
            return float(lat), float(lon)
    except:
        pass
    
    return None, None

if __name__ == "__main__":
    main()
