#!/usr/bin/env python3
"""
Load all tier results from staging CSVs into the database.

Processes 9 tier outcomes:

Multi-value (distribution across locations):
  1. CWS_DEL   - Community Water System Deliveries
  2. AG_REV    - Agricultural Revenue
  3. ENV_FLOWS - Environmental Flows
  4. RES_STOR  - Reservoir Storage
  5. GW_STOR   - Groundwater Storage

Single-value (one tier level per scenario):
  6. DELTA_ECO      - Delta Ecology
  7. FW_DELTA_USES  - Freshwater for In-Delta Uses
  8. FW_EXP         - Freshwater for Delta Exports
  9. WRC_SALMON_AB  - Salmon Abundance (hardcoded tier 4; s0065 excluded)

Staging CSVs live in etl/tier_data/staging/ and are named by tier short code
(e.g. CWS_DEL.csv, ENV_FLOWS.csv). WRC_SALMON_AB has no CSV.

Uses UPSERT to preserve existing data while updating/adding new records.
Also deactivates tier data for retired scenario s0029.

Usage:
    # Preview what will be loaded (dry run, no SQL generated)
    python load_all_tier_results.py --dry-run

    # Generate SQL file (then run with psql)
    python load_all_tier_results.py --output-sql all_tiers.sql
    psql $DATABASE_URL -f all_tiers.sql

    # Load only specific tiers
    python load_all_tier_results.py --only ENV_FLOWS,RES_STOR --output-sql partial.sql

    # Load directly to database (requires DATABASE_URL env var)
    DATABASE_URL=postgres://... python load_all_tier_results.py
"""

import argparse
import os
import sys
import pandas as pd
from pathlib import Path
from collections import Counter
from typing import Dict, List, Tuple

# Active scenarios — update this list when new scenarios are loaded
ALLOWED_SCENARIOS = {
    's0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0026', 's0027',
    's0028', 's0030', 's0031', 's0032', 's0033', 's0035', 's0036', 's0037',
    's0039', 's0040', 's0041', 's0042', 's0044', 's0045', 's0046', 's0065',
}

# Scenario retired in this cycle — tier data is kept but marked inactive
DEACTIVATED_SCENARIOS = {'s0029'}

# Tier version ID — do not change without data team sign-off
TIER_VERSION_ID = 8

# Staging directory — CSVs named by tier short code
STAGING_DIR = Path(__file__).parent / 'staging'

# =============================================================================
# LOCATION NAME MAPPINGS
# =============================================================================

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
# HELPERS
# =============================================================================

def escape_sql(val) -> str:
    """Escape a value for safe inclusion in a SQL literal."""
    if val is None:
        return 'NULL'
    if isinstance(val, str):
        return "'" + val.replace("'", "''") + "'"
    return str(val)


def normalize_scenario_id(raw) -> str:
    """
    Convert a raw scenario identifier to the canonical s0XXX format.

    Handles both the standard string form ('s0011') and the numeric-only
    form used in DELTA_ECO.csv ('11' -> 's0011', '65' -> 's0065').
    """
    s = str(raw).strip()
    if s.startswith('s'):
        return s
    try:
        return f's{int(s):04d}'
    except ValueError:
        return s


def convert_wba_id_to_mapbox_format(wba_col: str) -> str:
    """
    Convert WBA column names to Mapbox tileset format.
    WBA2 -> 02, WBA7N -> 07N, WBA10 -> 10, DETAW -> DETAW.
    """
    if wba_col == 'DETAW':
        return 'DETAW'
    if wba_col.startswith('WBA'):
        suffix = wba_col[3:]
        if len(suffix) >= 1 and suffix[0].isdigit():
            if len(suffix) == 1 or (len(suffix) == 2 and suffix[1] in 'NS'):
                return '0' + suffix
        return suffix
    return wba_col


# =============================================================================
# MULTI-VALUE LOADERS
# =============================================================================

def load_cws_del_data() -> Tuple[List[Dict], List[Dict]]:
    """
    CWS_DEL — Community Water System Deliveries.
    Format: rows = scenarios, columns = demand unit short codes, values = tier 1-4 or NA.
    """
    csv_path = STAGING_DIR / 'CWS_DEL.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping CWS_DEL")
        return [], []

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().replace('\n', '') for c in df.columns]

    location_results = []
    tier_results = []

    scenario_col = df.columns[0]
    du_columns = [c for c in df.columns[1:] if c]

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row[scenario_col])
        if scenario not in ALLOWED_SCENARIOS:
            continue

        tier_counts = Counter()
        valid_count = 0

        for du_id in du_columns:
            tier_val = row[du_id]
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
                'location_name': du_id,
                'tier_level': tier,
                'tier_value': 1,
                'display_order': len(location_results) + 1,
            })

        if valid_count > 0:
            tier_results.append(_multi_value_aggregate(scenario, 'CWS_DEL', tier_counts, valid_count))

    print(f"CWS_DEL: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_ag_rev_data() -> Tuple[List[Dict], List[Dict]]:
    """
    AG_REV — Agricultural Revenue.
    Format: (index), scenario, region, tier.
    """
    csv_path = STAGING_DIR / 'AG_REV.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping AG_REV")
        return [], []

    df = pd.read_csv(csv_path)

    location_results = []
    tier_results = []

    for scenario, group in df.groupby('scenario'):
        scenario = normalize_scenario_id(scenario)
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
            tier_results.append(_multi_value_aggregate(scenario, 'AG_REV', tier_counts, total))

    print(f"AG_REV: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_env_flows_data() -> Tuple[List[Dict], List[Dict]]:
    """
    ENV_FLOWS — Environmental Flows.
    Format: rows = station IDs (index col), columns = scenario codes, values = tier 1-4.
    """
    csv_path = STAGING_DIR / 'ENV_FLOWS.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping ENV_FLOWS")
        return [], []

    # First column (Station IDs) becomes the row index
    df = pd.read_csv(csv_path, index_col=0)

    location_results = []
    tier_results = []

    # Handle duplicate scenario columns like s0042(1) — use first occurrence
    scenario_mapping = {}
    for col in df.columns:
        base = col.split('(')[0].strip()
        if base not in scenario_mapping:
            scenario_mapping[base] = col

    for scenario in ALLOWED_SCENARIOS:
        if scenario not in scenario_mapping:
            continue

        col = scenario_mapping[scenario]
        tier_counts = Counter()
        display_order = 1

        for station in df.index:
            tier_val = df.loc[station, col]
            if pd.isna(tier_val):
                continue
            tier = int(tier_val)
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
            tier_results.append(_multi_value_aggregate(scenario, 'ENV_FLOWS', tier_counts, total))

    print(f"ENV_FLOWS: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_res_stor_data() -> Tuple[List[Dict], List[Dict]]:
    """
    RES_STOR — Reservoir Storage.
    Format: rows = scenarios (col 'Scenario'), columns = reservoir tier names.
    """
    csv_path = STAGING_DIR / 'RES_STOR.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping RES_STOR")
        return [], []

    df = pd.read_csv(csv_path)
    res_columns = [c for c in df.columns if c != 'Scenario']

    location_results = []
    tier_results = []

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['Scenario'])
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
            tier_results.append(_multi_value_aggregate(scenario, 'RES_STOR', tier_counts, total))

    print(f"RES_STOR: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_gw_stor_data() -> Tuple[List[Dict], List[Dict]]:
    """
    GW_STOR — Groundwater Storage.
    Format: rows = scenarios (col 'scenario'), columns = WBA IDs + DETAW, values = tier 0-4.
    Tier 0 is treated as tier 1 (no impact).
    """
    csv_path = STAGING_DIR / 'GW_STOR.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping GW_STOR")
        return [], []

    df = pd.read_csv(csv_path)
    wba_columns = [c for c in df.columns if c != 'scenario']

    location_results = []
    tier_results = []

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            continue

        tier_counts = Counter()
        display_order = 1

        for wba_col in wba_columns:
            tier_val = row[wba_col]
            if pd.isna(tier_val):
                continue
            tier = int(tier_val)
            if tier == 0:
                tier = 1  # tier 0 maps to tier 1 (no impact)
            tier_counts[tier] += 1
            mapbox_id = convert_wba_id_to_mapbox_format(wba_col)
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'GW_STOR',
                'location_type': 'wba',
                'location_id': mapbox_id,
                'location_name': WBA_NAMES.get(wba_col, wba_col),
                'tier_level': tier,
                'tier_value': 1,
                'display_order': display_order,
            })
            display_order += 1

        total = len(wba_columns)
        if total > 0:
            tier_results.append(_multi_value_aggregate(scenario, 'GW_STOR', tier_counts, total))

    print(f"GW_STOR: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def _multi_value_aggregate(scenario: str, short_code: str, tier_counts: Counter, total: int) -> Dict:
    """Build a tier_result row for a multi-value tier."""
    return {
        'scenario_short_code': scenario,
        'tier_short_code': short_code,
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
    }


# =============================================================================
# SINGLE-VALUE LOADERS
# =============================================================================

def load_delta_eco_data() -> Tuple[List[Dict], List[Dict]]:
    """
    DELTA_ECO — Delta Ecology.
    Format: Scenario (numeric, e.g. '11' for s0011), TierValue.
    One location row per scenario: wba DETAW.
    """
    csv_path = STAGING_DIR / 'DELTA_ECO.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping DELTA_ECO")
        return [], []

    df = pd.read_csv(csv_path)

    location_results = []
    tier_results = []

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['Scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            continue
        tier = int(row['TierValue'])
        tier_results.append(_single_value_aggregate(scenario, 'DELTA_ECO', tier))
        location_results.append({
            'scenario_short_code': scenario,
            'tier_short_code': 'DELTA_ECO',
            'location_type': 'wba',
            'location_id': 'DETAW',
            'location_name': 'Sacramento-San Joaquin Delta (DETAW)',
            'tier_level': tier,
            'tier_value': 1,
            'display_order': 1,
        })

    print(f"DELTA_ECO: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_fw_delta_uses_data() -> Tuple[List[Dict], List[Dict]]:
    """
    FW_DELTA_USES — Freshwater for In-Delta Uses.
    Format: ScenarioID (s0XXX), Salinity_Tier.
    Two compliance station locations per scenario: Emmaton (EM) and Jersey Point (JP).
    """
    csv_path = STAGING_DIR / 'FW_DELTA_USES.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping FW_DELTA_USES")
        return [], []

    df = pd.read_csv(csv_path)

    location_results = []
    tier_results = []

    stations = [
        ('EM', 'Emmaton', 1),
        ('JP', 'Jersey Point', 2),
    ]

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['ScenarioID'])
        if scenario not in ALLOWED_SCENARIOS:
            continue
        tier = int(row['Salinity_Tier'])
        tier_results.append(_single_value_aggregate(scenario, 'FW_DELTA_USES', tier))
        for loc_id, loc_name, order in stations:
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'FW_DELTA_USES',
                'location_type': 'compliance_station',
                'location_id': loc_id,
                'location_name': loc_name,
                'tier_level': tier,
                'tier_value': 1,
                'display_order': order,
            })

    print(f"FW_DELTA_USES: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_fw_exp_data() -> Tuple[List[Dict], List[Dict]]:
    """
    FW_EXP — Freshwater for Delta Exports.
    Format: Scenario (s0XXX), Salinity_Export_Tier.
    Two network node locations per scenario: Banks (CAA003) and Jones (DMC000).
    """
    csv_path = STAGING_DIR / 'FW_EXP.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping FW_EXP")
        return [], []

    df = pd.read_csv(csv_path)

    location_results = []
    tier_results = []

    pumps = [
        ('CAA003', 'Banks Pumping Plant', 1),
        ('DMC000', 'Jones Pumping Plant', 2),
    ]

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['Scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            continue
        tier = int(row['Salinity_Export_Tier'])
        tier_results.append(_single_value_aggregate(scenario, 'FW_EXP', tier))
        for loc_id, loc_name, order in pumps:
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'FW_EXP',
                'location_type': 'network_node',
                'location_id': loc_id,
                'location_name': loc_name,
                'tier_level': tier,
                'tier_value': 1,
                'display_order': order,
            })

    print(f"FW_EXP: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_salmon_data() -> Tuple[List[Dict], List[Dict]]:
    """
    WRC_SALMON_AB — Salmon Abundance.
    No CSV — hardcoded as tier 4 for all active scenarios.
    s0065 is excluded (not reported).
    One network node location per qualifying scenario: SAC299 (Sacramento at Keswick).
    """
    excluded = {'s0065'}
    qualifying = ALLOWED_SCENARIOS - excluded

    location_results = []
    tier_results = []

    for scenario in sorted(qualifying):
        tier = 4
        tier_results.append(_single_value_aggregate(scenario, 'WRC_SALMON_AB', tier))
        location_results.append({
            'scenario_short_code': scenario,
            'tier_short_code': 'WRC_SALMON_AB',
            'location_type': 'network_node',
            'location_id': 'SAC299',
            'location_name': 'Sacramento River at Keswick',
            'tier_level': tier,
            'tier_value': 1,
            'display_order': 1,
        })

    print(f"WRC_SALMON_AB: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def _single_value_aggregate(scenario: str, short_code: str, tier_level: int) -> Dict:
    """Build a tier_result row for a single-value tier."""
    return {
        'scenario_short_code': scenario,
        'tier_short_code': short_code,
        'tier_1_value': None,
        'tier_2_value': None,
        'tier_3_value': None,
        'tier_4_value': None,
        'norm_tier_1': None,
        'norm_tier_2': None,
        'norm_tier_3': None,
        'norm_tier_4': None,
        'total_value': None,
        'single_tier_level': tier_level,
    }


# =============================================================================
# SQL GENERATION
# =============================================================================

def generate_location_result_sql(location_results: List[Dict]) -> str:
    """Generate UPSERT SQL for tier_location_result."""
    if not location_results:
        return ""

    lines = [
        "-- Tier Location Results",
        "-- Generated by load_all_tier_results.py",
        "",
        "INSERT INTO tier_location_result (",
        "    scenario_short_code, tier_short_code, location_type, location_id,",
        "    location_name, tier_level, tier_value, display_order, tier_version_id",
        ") VALUES",
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
    lines.append("    display_order = EXCLUDED.display_order;")
    lines.append("")
    return '\n'.join(lines)


def generate_tier_result_sql(tier_results: List[Dict]) -> str:
    """Generate UPSERT SQL for tier_result."""
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
        ") VALUES",
    ]

    values = []
    for r in tier_results:
        values.append(
            f"    ({escape_sql(r['scenario_short_code'])}, {escape_sql(r['tier_short_code'])}, "
            # Use escape_sql for numeric fields so None -> NULL in SQL
            f"{escape_sql(r['tier_1_value'])}, {escape_sql(r['tier_2_value'])}, "
            f"{escape_sql(r['tier_3_value'])}, {escape_sql(r['tier_4_value'])}, "
            f"{escape_sql(r['norm_tier_1'])}, {escape_sql(r['norm_tier_2'])}, "
            f"{escape_sql(r['norm_tier_3'])}, {escape_sql(r['norm_tier_4'])}, "
            f"{escape_sql(r['total_value'])}, {escape_sql(r['single_tier_level'])}, "
            f"{TIER_VERSION_ID})"
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
    lines.append("    is_active = TRUE,")
    lines.append("    updated_at = NOW();")
    lines.append("")
    return '\n'.join(lines)


def generate_deactivation_sql() -> str:
    """Generate SQL to mark retired scenarios as inactive."""
    if not DEACTIVATED_SCENARIOS:
        return ""

    codes = ', '.join(escape_sql(s) for s in sorted(DEACTIVATED_SCENARIOS))
    return f"""-- Deactivate retired scenarios in tier_result
-- (tier_location_result has no is_active column; retired rows remain but
--  are not surfaced since the API filters on tier_result.is_active)
UPDATE tier_result
   SET is_active = FALSE, updated_at = NOW()
 WHERE scenario_short_code IN ({codes});

"""


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Load all tier results from staging CSVs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tier outcomes loaded:
  CWS_DEL      - Community Water System Deliveries  (multi-value)
  AG_REV       - Agricultural Revenue               (multi-value)
  ENV_FLOWS    - Environmental Flows                (multi-value)
  RES_STOR     - Reservoir Storage                  (multi-value)
  GW_STOR      - Groundwater Storage                (multi-value)
  DELTA_ECO    - Delta Ecology                      (single-value)
  FW_DELTA_USES- Freshwater for In-Delta Uses       (single-value)
  FW_EXP       - Freshwater for Delta Exports       (single-value)
  WRC_SALMON_AB- Salmon Abundance                   (single-value, hardcoded)
        """
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview data counts without generating SQL')
    parser.add_argument('--output-sql', type=str,
                        help='Write SQL to this file instead of executing')
    parser.add_argument('--only', type=str,
                        help='Comma-separated tier short codes to load (e.g. ENV_FLOWS,RES_STOR)')

    args = parser.parse_args()

    print("=" * 60)
    print("LOADING TIER DATA FROM STAGING")
    print(f"Active scenarios : {len(ALLOWED_SCENARIOS)}")
    print(f"Retired scenarios: {sorted(DEACTIVATED_SCENARIOS)} (will be deactivated)")
    print(f"Staging directory: {STAGING_DIR}")
    print("=" * 60)
    print()

    all_tiers = [
        'CWS_DEL', 'AG_REV', 'ENV_FLOWS', 'RES_STOR', 'GW_STOR',
        'DELTA_ECO', 'FW_DELTA_USES', 'FW_EXP', 'WRC_SALMON_AB',
    ]
    tiers_to_load = all_tiers
    if args.only:
        tiers_to_load = [t.strip().upper() for t in args.only.split(',')]

    loaders = {
        'CWS_DEL':       load_cws_del_data,
        'AG_REV':        load_ag_rev_data,
        'ENV_FLOWS':     load_env_flows_data,
        'RES_STOR':      load_res_stor_data,
        'GW_STOR':       load_gw_stor_data,
        'DELTA_ECO':     load_delta_eco_data,
        'FW_DELTA_USES': load_fw_delta_uses_data,
        'FW_EXP':        load_fw_exp_data,
        'WRC_SALMON_AB': load_salmon_data,
    }

    all_location_results = []
    all_tier_results = []

    for tier_code in tiers_to_load:
        if tier_code not in loaders:
            print(f"WARNING: Unknown tier code '{tier_code}', skipping")
            continue
        loc, agg = loaders[tier_code]()
        all_location_results.extend(loc)
        all_tier_results.extend(agg)

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total location results : {len(all_location_results)}")
    print(f"Total tier aggregates  : {len(all_tier_results)}")
    print()
    print("Scenario aggregates by tier:")
    tier_summary = Counter(r['tier_short_code'] for r in all_tier_results)
    for tier_code in all_tiers:
        if tier_code in tier_summary:
            print(f"  {tier_code:<16} {tier_summary[tier_code]} scenarios")

    if args.dry_run:
        print("\nDRY RUN — no SQL generated")
        return

    # Generate SQL
    tier_sql = generate_tier_result_sql(all_tier_results)
    location_sql = generate_location_result_sql(all_location_results)
    deactivation_sql = generate_deactivation_sql()

    full_sql = f"""-- ============================================================================
-- TIER DATA UPSERT
-- Generated by load_all_tier_results.py
-- Tier version: {TIER_VERSION_ID}
-- Active scenarios: {len(ALLOWED_SCENARIOS)}
-- ============================================================================
-- UPSERT preserves existing rows; re-running is safe.
-- is_active is explicitly set to TRUE on every upserted row so that
-- any previously deactivated rows for these scenarios are restored.
-- ============================================================================

{tier_sql}

{location_sql}

{deactivation_sql}
-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT tier_short_code,
       COUNT(*) AS scenarios,
       SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active
FROM tier_result
WHERE tier_version_id = {TIER_VERSION_ID}
GROUP BY tier_short_code
ORDER BY tier_short_code;

SELECT tier_short_code,
       COUNT(*) AS location_rows
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
        print("\nTo apply on Cloud9:")
        print(f"  psql $DATABASE_URL -f {output_path.name}")
        return

    # Execute directly
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("\nError: DATABASE_URL not set. Use --output-sql to generate a file instead.")
        sys.exit(1)

    try:
        import psycopg2

        print("\nConnecting to database...")
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        print("Upserting tier_result records...")
        cur.execute(tier_sql)

        print("Upserting tier_location_result records...")
        cur.execute(location_sql)

        print("Deactivating retired scenarios...")
        cur.execute(deactivation_sql)

        conn.commit()
        print("\nSuccessfully loaded:")
        print(f"  {len(all_tier_results)} tier_result records")
        print(f"  {len(all_location_results)} tier_location_result records")
        print(f"  Deactivated: {sorted(DEACTIVATED_SCENARIOS)}")

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
