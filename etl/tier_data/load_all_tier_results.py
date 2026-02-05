#!/usr/bin/env python3
"""
Load all tier results from staging CSVs into the database.

Processes 5 distribution-type tier outcomes:
1. CWS_DEL - Community Water System Deliveries (demand units)
2. AG_REV - Agricultural Revenue (demand units)
3. ENV_FLOWS - Environmental Flows (network nodes)
4. RES_STOR - Reservoir Storage (reservoirs)
5. GW_STOR - Groundwater Storage (water budget areas)

Uses UPSERT to preserve existing data while updating/adding new records.

Usage:
    # Preview what will be loaded (dry run)
    python load_all_tier_results.py --dry-run
    
    # Generate SQL file
    python load_all_tier_results.py --output-sql all_tiers.sql
    
    # Load directly to database
    DATABASE_URL=postgres://... python load_all_tier_results.py
"""

import argparse
import os
import sys
import pandas as pd
from pathlib import Path
from collections import Counter
from typing import Dict, List, Tuple

# Allowed scenarios - only load data for these
ALLOWED_SCENARIOS = {
    's0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0026', 's0027',
    's0028', 's0029', 's0030', 's0031', 's0032', 's0033', 's0039', 's0040',
    's0041', 's0042', 's0044', 's0045', 's0046', 's0065'
}

# Tier version ID (from existing data)
TIER_VERSION_ID = 8

# Staging directory
STAGING_DIR = Path(__file__).parent / 'staging'

# =============================================================================
# LOCATION NAME MAPPINGS
# =============================================================================

# Environmental flows locations (network nodes)
ENV_FLOWS_LOCATIONS = {
    'AMR004': 'American River at I-80 Bridge',
    'FTR003': 'Feather River',
    'FTR029': 'Feather River at Yuba City',
    'MCD005': 'Merced River at Stevinson',
    'MOK028': 'Mokelumne River',
    'SAC000': 'Sacramento at confluence',
    'SAC049': 'Sacramento River at Freeport',
    'SAC122': 'Sacramento River at Tisdale Weir',
    'SAC148': 'Sacramento River at Colusa Weir',
    'SAC257': 'Sacramento River above Bend Bridge',
    'SAC289': 'Sacramento River (South Bonnieville)',
    'SJR070': 'San Joaquin near Vernalis',
    'SJR127': 'San Joaquin at Salt Slough',
    'STS011': 'Stanislaus River',
    'TRN111': 'Trinity River at Lewiston',
    'TUO003': 'Tuolumne River',
    'YUB002': 'Yuba River at Marysville',
}

# Reservoir locations
RESERVOIR_LOCATIONS = {
    'S_SHSTA_Storage_Tier': ('SHSTA', 'Shasta Lake'),
    'S_TRNTY_Storage_Tier': ('TRNTY', 'Trinity Lake'),
    'S_OROVL_Storage_Tier': ('OROVL', 'Lake Oroville'),
    'S_FOLSM_Storage_Tier': ('FOLSM', 'Folsom Lake'),
    'S_MELON_Storage_Tier': ('MELON', 'New Melones Lake'),
    'S_MLRTN_Storage_Tier': ('MLRTN', 'Millerton Lake'),
    'S_SLUIS_CVP_Storage_Tier': ('SLUIS_CVP', 'San Luis CVP'),
    'S_SLUIS_SWP_Storage_Tier': ('SLUIS_SWP', 'San Luis SWP'),
}

# Water Budget Area names
WBA_NAMES = {
    'WBA2': 'WBA 2 - Upper Sacramento',
    'WBA3': 'WBA 3 - Redding',
    'WBA4': 'WBA 4 - Red Bluff',
    'WBA5': 'WBA 5 - Corning',
    'WBA6': 'WBA 6 - Orland',
    'WBA7N': 'WBA 7N - Chico North',
    'WBA7S': 'WBA 7S - Chico South',
    'WBA8N': 'WBA 8N - Colusa North',
    'WBA8S': 'WBA 8S - Colusa South',
    'WBA9': 'WBA 9 - Yolo',
    'WBA10': 'WBA 10 - American',
    'WBA11': 'WBA 11 - Sutter',
    'WBA12': 'WBA 12 - Yuba',
    'WBA13': 'WBA 13 - Bear',
    'WBA14': 'WBA 14 - Feather',
    'WBA15N': 'WBA 15N - Butte North',
    'WBA15S': 'WBA 15S - Butte South',
    'WBA16': 'WBA 16 - Stony',
    'WBA17N': 'WBA 17N - Cache North',
    'WBA17S': 'WBA 17S - Cache South',
    'WBA18': 'WBA 18 - Putah',
    'WBA19': 'WBA 19 - Solano',
    'WBA20': 'WBA 20 - Napa',
    'WBA21': 'WBA 21 - Suisun',
    'WBA22': 'WBA 22 - Contra Costa',
    'WBA23': 'WBA 23 - East Bay',
    'WBA24': 'WBA 24 - South Bay',
    'WBA25': 'WBA 25 - Peninsula',
    'WBA26N': 'WBA 26N - San Joaquin North',
    'WBA26S': 'WBA 26S - San Joaquin South',
    'WBA50': 'WBA 50 - Delta',
    'WBA60N': 'WBA 60N - SJR East North',
    'WBA60S': 'WBA 60S - SJR East South',
    'WBA61': 'WBA 61 - Stanislaus',
    'WBA62': 'WBA 62 - Tuolumne',
    'WBA63': 'WBA 63 - Merced',
    'WBA64': 'WBA 64 - Chowchilla',
    'WBA71': 'WBA 71 - Fresno',
    'WBA72': 'WBA 72 - Kings',
    'WBA73': 'WBA 73 - Kaweah',
    'WBA90': 'WBA 90 - Tulare',
    'DETAW': 'Delta',
}

# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def load_cws_del_data() -> Tuple[List[Dict], List[Dict]]:
    """
    Load CWS_DEL (Community Water Deliveries) tier data.
    
    Format: Rows = scenarios, Columns = demand unit IDs, Values = tier (1-4 or NA)
    """
    csv_path = STAGING_DIR / 'all_scenarios_tier_matrix.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping CWS_DEL")
        return [], []
    
    df = pd.read_csv(csv_path)
    
    # Fix column names (there's a line break issue in the header)
    df.columns = [c.strip().replace('\n', '') for c in df.columns]
    
    location_results = []
    tier_results = []
    
    # Get demand unit columns (all except scenario_id)
    scenario_col = df.columns[0]  # 'scenario_id'
    du_columns = [c for c in df.columns[1:] if c]
    
    for _, row in df.iterrows():
        scenario = row[scenario_col].strip().strip('"')
        
        if scenario not in ALLOWED_SCENARIOS:
            continue
        
        tier_counts = Counter()
        valid_count = 0
        
        for du_id in du_columns:
            tier_val = row[du_id]
            
            # Handle NA values
            if pd.isna(tier_val) or tier_val == 'NA':
                continue
            
            tier = int(tier_val)
            tier_counts[tier] += 1
            valid_count += 1
            
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'CWS_DEL',
                'location_type': 'demand_unit',
                'location_id': du_id,
                'location_name': du_id,  # Use ID as name for now
                'tier_level': tier,
                'tier_value': 1,
                'display_order': len(location_results) + 1,
            })
        
        if valid_count > 0:
            tier_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'CWS_DEL',
                'tier_1_value': tier_counts.get(1, 0),
                'tier_2_value': tier_counts.get(2, 0),
                'tier_3_value': tier_counts.get(3, 0),
                'tier_4_value': tier_counts.get(4, 0),
                'norm_tier_1': round(tier_counts.get(1, 0) / valid_count, 4),
                'norm_tier_2': round(tier_counts.get(2, 0) / valid_count, 4),
                'norm_tier_3': round(tier_counts.get(3, 0) / valid_count, 4),
                'norm_tier_4': round(tier_counts.get(4, 0) / valid_count, 4),
                'total_value': valid_count,
                'single_tier_level': None,
            })
    
    print(f"CWS_DEL: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_ag_rev_data() -> Tuple[List[Dict], List[Dict]]:
    """
    Load AG_REV (Agricultural Revenue) tier data.
    
    Format: index, scenario, region, revenue_baseline, average_revenue_scenario, 
            percentage_change_revenue, tier
    """
    csv_path = STAGING_DIR / 'ag_usecase_tier_results.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping AG_REV")
        return [], []
    
    df = pd.read_csv(csv_path)
    
    location_results = []
    tier_results = []
    
    # Group by scenario
    scenario_groups = df.groupby('scenario')
    
    for scenario, group in scenario_groups:
        if scenario not in ALLOWED_SCENARIOS:
            continue
        
        tier_counts = Counter()
        display_order = 1
        
        for _, row in group.iterrows():
            tier = int(row['tier'])
            region = row['region']
            
            tier_counts[tier] += 1
            
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'AG_REV',
                'location_type': 'demand_unit',
                'location_id': region,
                'location_name': region,
                'tier_level': tier,
                'tier_value': 1,
                'display_order': display_order,
            })
            display_order += 1
        
        total = len(group)
        if total > 0:
            tier_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'AG_REV',
                'tier_1_value': tier_counts.get(1, 0),
                'tier_2_value': tier_counts.get(2, 0),
                'tier_3_value': tier_counts.get(3, 0),
                'tier_4_value': tier_counts.get(4, 0),
                'norm_tier_1': round(tier_counts.get(1, 0) / total, 4),
                'norm_tier_2': round(tier_counts.get(2, 0) / total, 4),
                'norm_tier_3': round(tier_counts.get(3, 0) / total, 4),
                'norm_tier_4': round(tier_counts.get(4, 0) / total, 4),
                'total_value': total,
                'single_tier_level': None,
            })
    
    print(f"AG_REV: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_env_flows_data() -> Tuple[List[Dict], List[Dict]]:
    """
    Load ENV_FLOWS (Environmental Flows) tier data.
    
    Format: Rows = stations, Columns = scenarios, Values = tier (1-4)
    """
    csv_path = STAGING_DIR / 'Eflows_Tier_Results_02032026.xlsx - Sheet1.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping ENV_FLOWS")
        return [], []
    
    df = pd.read_csv(csv_path, index_col=0)
    
    location_results = []
    tier_results = []
    
    # Handle duplicate scenario columns like s0042(1), s0042(2)
    # Use the first one without parentheses
    scenario_mapping = {}
    for col in df.columns:
        base_scenario = col.split('(')[0]
        if base_scenario not in scenario_mapping:
            scenario_mapping[base_scenario] = col
    
    for scenario in ALLOWED_SCENARIOS:
        if scenario not in scenario_mapping:
            continue
        
        col = scenario_mapping[scenario]
        tier_counts = Counter()
        display_order = 1
        
        for station in df.index:
            tier = int(df.loc[station, col])
            tier_counts[tier] += 1
            
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'ENV_FLOWS',
                'location_type': 'network_node',
                'location_id': station,
                'location_name': ENV_FLOWS_LOCATIONS.get(station, station),
                'tier_level': tier,
                'tier_value': 1,
                'display_order': display_order,
            })
            display_order += 1
        
        total = len(df)
        if total > 0:
            tier_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'ENV_FLOWS',
                'tier_1_value': tier_counts.get(1, 0),
                'tier_2_value': tier_counts.get(2, 0),
                'tier_3_value': tier_counts.get(3, 0),
                'tier_4_value': tier_counts.get(4, 0),
                'norm_tier_1': round(tier_counts.get(1, 0) / total, 4),
                'norm_tier_2': round(tier_counts.get(2, 0) / total, 4),
                'norm_tier_3': round(tier_counts.get(3, 0) / total, 4),
                'norm_tier_4': round(tier_counts.get(4, 0) / total, 4),
                'total_value': total,
                'single_tier_level': None,
            })
    
    print(f"ENV_FLOWS: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_res_stor_data() -> Tuple[List[Dict], List[Dict]]:
    """
    Load RES_STOR (Reservoir Storage) tier data.
    
    Format: Rows = scenarios, Columns = reservoir storage tiers, Values = tier (1-4)
    """
    csv_path = STAGING_DIR / 'ReservoirStorage_Tiers.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping RES_STOR")
        return [], []
    
    df = pd.read_csv(csv_path)
    
    location_results = []
    tier_results = []
    
    # Get reservoir columns
    res_columns = [c for c in df.columns if c != 'Scenario']
    
    for _, row in df.iterrows():
        scenario = row['Scenario']
        
        if scenario not in ALLOWED_SCENARIOS:
            continue
        
        tier_counts = Counter()
        display_order = 1
        
        for res_col in res_columns:
            tier_val = row[res_col]
            if pd.isna(tier_val):
                continue
            
            tier = int(tier_val)
            tier_counts[tier] += 1
            
            res_id, res_name = RESERVOIR_LOCATIONS.get(res_col, (res_col, res_col))
            
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'RES_STOR',
                'location_type': 'reservoir',
                'location_id': res_id,
                'location_name': res_name,
                'tier_level': tier,
                'tier_value': 1,
                'display_order': display_order,
            })
            display_order += 1
        
        total = len(res_columns)
        if total > 0:
            tier_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'RES_STOR',
                'tier_1_value': tier_counts.get(1, 0),
                'tier_2_value': tier_counts.get(2, 0),
                'tier_3_value': tier_counts.get(3, 0),
                'tier_4_value': tier_counts.get(4, 0),
                'norm_tier_1': round(tier_counts.get(1, 0) / total, 4),
                'norm_tier_2': round(tier_counts.get(2, 0) / total, 4),
                'norm_tier_3': round(tier_counts.get(3, 0) / total, 4),
                'norm_tier_4': round(tier_counts.get(4, 0) / total, 4),
                'total_value': total,
                'single_tier_level': None,
            })
    
    print(f"RES_STOR: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def convert_wba_id_to_mapbox_format(wba_col: str) -> str:
    """
    Convert WBA column names to Mapbox tileset format.
    
    WBA column format: WBA2, WBA7N, WBA10, WBA60N, DETAW
    Mapbox format: 02, 07N, 10, 60N, DETAW (leading zeros for single digits)
    """
    if wba_col == 'DETAW':
        return 'DETAW'
    
    if wba_col.startswith('WBA'):
        suffix = wba_col[3:]  # Remove 'WBA' prefix
        # Check if it starts with a single digit (not followed by another digit)
        if len(suffix) >= 1 and suffix[0].isdigit():
            # Single digit cases: 2, 3, 4, 5, 6, 7N, 7S, 8N, 8S, 9
            if len(suffix) == 1 or (len(suffix) == 2 and suffix[1] in 'NS'):
                return '0' + suffix
        return suffix
    
    return wba_col


def load_gw_stor_data() -> Tuple[List[Dict], List[Dict]]:
    """
    Load GW_STOR (Groundwater Storage) tier data.
    
    Format: Rows = scenarios, Columns = WBA IDs + DETAW, Values = tier (0-4)
    """
    csv_path = STAGING_DIR / 'GroundWater_Tiers.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping GW_STOR")
        return [], []
    
    df = pd.read_csv(csv_path)
    
    location_results = []
    tier_results = []
    
    # Get WBA columns (all except 'scenario')
    wba_columns = [c for c in df.columns if c != 'scenario']
    
    for _, row in df.iterrows():
        scenario = row['scenario']
        
        if scenario not in ALLOWED_SCENARIOS:
            continue
        
        tier_counts = Counter()
        display_order = 1
        
        for wba_col in wba_columns:
            tier_val = row[wba_col]
            if pd.isna(tier_val):
                continue
            
            tier = int(tier_val)
            # Note: GW data has tier 0, but our system uses 1-4
            # Treat 0 as tier 1 (best/no impact)
            if tier == 0:
                tier = 1
            
            tier_counts[tier] += 1
            
            wba_name = WBA_NAMES.get(wba_col, wba_col)
            # Convert WBA2 -> 02, WBA10 -> 10, etc. to match Mapbox tileset
            mapbox_wba_id = convert_wba_id_to_mapbox_format(wba_col)
            
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'GW_STOR',
                'location_type': 'wba',
                'location_id': mapbox_wba_id,
                'location_name': wba_name,
                'tier_level': tier,
                'tier_value': 1,
                'display_order': display_order,
            })
            display_order += 1
        
        total = len(wba_columns)
        if total > 0:
            tier_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'GW_STOR',
                'tier_1_value': tier_counts.get(1, 0),
                'tier_2_value': tier_counts.get(2, 0),
                'tier_3_value': tier_counts.get(3, 0),
                'tier_4_value': tier_counts.get(4, 0),
                'norm_tier_1': round(tier_counts.get(1, 0) / total, 4),
                'norm_tier_2': round(tier_counts.get(2, 0) / total, 4),
                'norm_tier_3': round(tier_counts.get(3, 0) / total, 4),
                'norm_tier_4': round(tier_counts.get(4, 0) / total, 4),
                'total_value': total,
                'single_tier_level': None,
            })
    
    print(f"GW_STOR: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


# =============================================================================
# SQL GENERATION
# =============================================================================

def escape_sql(val):
    """Escape single quotes in SQL strings."""
    if val is None:
        return 'NULL'
    if isinstance(val, str):
        return "'" + val.replace("'", "''") + "'"
    return str(val)


def generate_location_result_sql(location_results: List[Dict]) -> str:
    """Generate SQL for tier_location_result UPSERT."""
    if not location_results:
        return ""
    
    lines = [
        "-- Tier Location Results",
        "-- Generated by load_all_tier_results.py",
        "",
        "INSERT INTO tier_location_result (",
        "    scenario_short_code, tier_short_code, location_type, location_id,",
        "    location_name, tier_level, tier_value, display_order, tier_version_id",
        ") VALUES"
    ]
    
    values = []
    for r in location_results:
        values.append(
            f"    ({escape_sql(r['scenario_short_code'])}, {escape_sql(r['tier_short_code'])}, "
            f"{escape_sql(r['location_type'])}, {escape_sql(r['location_id'])}, "
            f"{escape_sql(r['location_name'])}, {r['tier_level']}, {r['tier_value']}, "
            f"{r['display_order']}, {TIER_VERSION_ID})"
        )
    
    lines.append(',\n'.join(values))
    lines.append("ON CONFLICT (scenario_short_code, tier_short_code, location_id, tier_version_id)")
    lines.append("DO UPDATE SET")
    lines.append("    location_type = EXCLUDED.location_type,")
    lines.append("    location_name = EXCLUDED.location_name,")
    lines.append("    tier_level = EXCLUDED.tier_level,")
    lines.append("    tier_value = EXCLUDED.tier_value,")
    lines.append("    display_order = EXCLUDED.display_order,")
    lines.append("    updated_at = NOW();")
    lines.append("")
    
    return '\n'.join(lines)


def generate_tier_result_sql(tier_results: List[Dict]) -> str:
    """Generate SQL for tier_result UPSERT."""
    if not tier_results:
        return ""
    
    lines = [
        "-- Tier Result Aggregates",
        "-- Generated by load_all_tier_results.py",
        "",
        "INSERT INTO tier_result (",
        "    scenario_short_code, tier_short_code,",
        "    tier_1_value, tier_2_value, tier_3_value, tier_4_value,",
        "    norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4,",
        "    total_value, single_tier_level, tier_version_id",
        ") VALUES"
    ]
    
    values = []
    for r in tier_results:
        single_tier = 'NULL' if r['single_tier_level'] is None else r['single_tier_level']
        values.append(
            f"    ({escape_sql(r['scenario_short_code'])}, {escape_sql(r['tier_short_code'])}, "
            f"{r['tier_1_value']}, {r['tier_2_value']}, {r['tier_3_value']}, {r['tier_4_value']}, "
            f"{r['norm_tier_1']}, {r['norm_tier_2']}, {r['norm_tier_3']}, {r['norm_tier_4']}, "
            f"{r['total_value']}, {single_tier}, {TIER_VERSION_ID})"
        )
    
    lines.append(',\n'.join(values))
    lines.append("ON CONFLICT (scenario_short_code, tier_short_code, tier_version_id)")
    lines.append("DO UPDATE SET")
    lines.append("    tier_1_value = EXCLUDED.tier_1_value,")
    lines.append("    tier_2_value = EXCLUDED.tier_2_value,")
    lines.append("    tier_3_value = EXCLUDED.tier_3_value,")
    lines.append("    tier_4_value = EXCLUDED.tier_4_value,")
    lines.append("    norm_tier_1 = EXCLUDED.norm_tier_1,")
    lines.append("    norm_tier_2 = EXCLUDED.norm_tier_2,")
    lines.append("    norm_tier_3 = EXCLUDED.norm_tier_3,")
    lines.append("    norm_tier_4 = EXCLUDED.norm_tier_4,")
    lines.append("    total_value = EXCLUDED.total_value,")
    lines.append("    single_tier_level = EXCLUDED.single_tier_level,")
    lines.append("    updated_at = NOW();")
    lines.append("")
    
    return '\n'.join(lines)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Load all tier results from staging CSVs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tier outcomes loaded:
  CWS_DEL   - Community Water System Deliveries
  AG_REV    - Agricultural Revenue
  ENV_FLOWS - Environmental Flows
  RES_STOR  - Reservoir Storage
  GW_STOR   - Groundwater Storage
        """
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview data without generating SQL')
    parser.add_argument('--output-sql', type=str,
                        help='Write SQL to file')
    parser.add_argument('--only', type=str,
                        help='Comma-separated list of tiers to load (e.g., CWS_DEL,AG_REV)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("LOADING TIER DATA FROM STAGING")
    print(f"Allowed scenarios: {len(ALLOWED_SCENARIOS)}")
    print(f"Staging directory: {STAGING_DIR}")
    print("=" * 60)
    print()
    
    # Determine which tiers to load
    tiers_to_load = ['CWS_DEL', 'AG_REV', 'ENV_FLOWS', 'RES_STOR', 'GW_STOR']
    if args.only:
        tiers_to_load = [t.strip().upper() for t in args.only.split(',')]
    
    all_location_results = []
    all_tier_results = []
    
    # Load each tier type
    if 'CWS_DEL' in tiers_to_load:
        loc, agg = load_cws_del_data()
        all_location_results.extend(loc)
        all_tier_results.extend(agg)
    
    if 'AG_REV' in tiers_to_load:
        loc, agg = load_ag_rev_data()
        all_location_results.extend(loc)
        all_tier_results.extend(agg)
    
    if 'ENV_FLOWS' in tiers_to_load:
        loc, agg = load_env_flows_data()
        all_location_results.extend(loc)
        all_tier_results.extend(agg)
    
    if 'RES_STOR' in tiers_to_load:
        loc, agg = load_res_stor_data()
        all_location_results.extend(loc)
        all_tier_results.extend(agg)
    
    if 'GW_STOR' in tiers_to_load:
        loc, agg = load_gw_stor_data()
        all_location_results.extend(loc)
        all_tier_results.extend(agg)
    
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total location results: {len(all_location_results)}")
    print(f"Total tier aggregates: {len(all_tier_results)}")
    
    # Summary by tier type
    tier_summary = Counter(r['tier_short_code'] for r in all_tier_results)
    print("\nScenarios by tier type:")
    for tier, count in sorted(tier_summary.items()):
        print(f"  {tier}: {count} scenarios")
    
    if args.dry_run:
        print("\nDRY RUN - No SQL generated")
        return
    
    # Generate SQL
    location_sql = generate_location_result_sql(all_location_results)
    tier_sql = generate_tier_result_sql(all_tier_results)
    full_sql = f"""-- ============================================================================
-- TIER DATA UPSERT
-- Generated by load_all_tier_results.py
-- ============================================================================
-- This script uses UPSERT (INSERT ... ON CONFLICT DO UPDATE) to preserve
-- existing data while updating/adding new records.
-- ============================================================================

{tier_sql}

{location_sql}

-- Verification
SELECT tier_short_code, COUNT(*) as scenarios 
FROM tier_result 
WHERE tier_version_id = {TIER_VERSION_ID}
GROUP BY tier_short_code 
ORDER BY tier_short_code;

SELECT tier_short_code, COUNT(*) as locations 
FROM tier_location_result 
WHERE tier_version_id = {TIER_VERSION_ID}
GROUP BY tier_short_code 
ORDER BY tier_short_code;
"""
    
    if args.output_sql:
        output_path = Path(args.output_sql)
        with open(output_path, 'w') as f:
            f.write(full_sql)
        print(f"\nSQL written to: {output_path}")
        print("\nTo run on Cloud9:")
        print(f"  psql $DATABASE_URL -f {output_path}")
        return
    
    # Execute against database
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("\nError: DATABASE_URL environment variable not set")
        print("Use --dry-run or --output-sql instead")
        sys.exit(1)
    
    try:
        import psycopg2
        
        print("\nConnecting to database...")
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        # Execute tier results first
        print("Upserting tier_result records...")
        cur.execute(tier_sql)
        
        # Execute location results
        print("Upserting tier_location_result records...")
        cur.execute(location_sql)
        
        conn.commit()
        print("\nSuccessfully loaded:")
        print(f"  {len(all_tier_results)} tier_result records")
        print(f"  {len(all_location_results)} tier_location_result records")
        
        cur.close()
        conn.close()
        
    except ImportError:
        print("Error: psycopg2 not installed. Use --output-sql instead.")
        sys.exit(1)
    except Exception as e:
        print(f"Database error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
