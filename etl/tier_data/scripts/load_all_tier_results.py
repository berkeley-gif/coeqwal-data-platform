#!/usr/bin/env python3
"""
load_all_tier_results.py - Load all tier results from staging CSVs into the database.

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
  9. WRC_SALMON_AB  - Salmon Abundance (from WRC_SALMON_AB.csv)

Staging CSVs live in etl/tier_data/staging/ and are named by tier short code
(e.g. CWS_DEL.csv, ENV_FLOWS.csv, WRC_SALMON_AB.csv). The staging files are
produced from the data team's raw drops by stage_tier_results.py.

Uses UPSERT to preserve existing data while updating/adding new records.
Also deactivates tier data for retired scenario s0029.

Usage:
    # Preview what will be loaded (dry run, no SQL generated)
    python load_all_tier_results.py --dry-run

    # Generate SQL file (then run with psql)
    python load_all_tier_results.py --output-sql all_tiers.sql
    psql $DATABASE_URL -f etl/tier_data/output/all_tiers.sql

    # Bare filenames passed to --output-sql are auto-routed into
    # etl/tier_data/output/ (gitignored). Pass a path with a '/' or an
    # absolute path to write somewhere else.

    # Load only specific tiers
    python load_all_tier_results.py --only ENV_FLOWS,RES_STOR --output-sql partial.sql

    # Load directly to database (requires DATABASE_URL env var)
    DATABASE_URL=postgres://... python load_all_tier_results.py

    # Pre-flight a brand-new scenario before flipping is_active=1.
    # Replaces ACTIVE_SCENARIOS for this invocation only.
    python load_all_tier_results.py --scenarios-override s0070 --dry-run
"""

import argparse
import csv
import math
import os
import sys
import pandas as pd
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from etl.common import (  # noqa: E402
    assess_coverage,
    fetch_tier_location_names,
    format_coverage_warnings,
    get_db_connection,
    resolve_active_scenarios,
)
from etl.common.active_scenarios import ACTIVE_SCENARIOS  # noqa: E402
from etl.tier_data.staging_inventory import (  # noqa: E402
    convert_wba_id_to_mapbox_format,
    parse_res_stor_column as _res_stor_location_id,
)

# Rebound inside main() when --scenarios-override is passed
ALLOWED_SCENARIOS: frozenset[str] = ACTIVE_SCENARIOS

DEACTIVATED_SCENARIOS: set = set()

REPO_ROOT = Path(__file__).resolve().parents[3]

# Populated from the database in main() before any loader runs. Empty at
# import time so this module can still be imported without DATABASE_URL.
TIER_LOCATION_NAMES: Dict[str, Dict[str, str]] = {}

# Tier version ID — do not change without data team sign-off
TIER_VERSION_ID = 8

# Staging directory — CSVs named by tier short code
STAGING_DIR = Path(__file__).parent.parent / 'staging'

# Default output directory for generated SQL. Gitignored via etl/**/output/.
# Bare filenames passed to --output-sql land here; absolute paths or paths
# with a directory component are respected verbatim.
OUTPUT_DIR = Path(__file__).parent.parent / 'output'

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

class TierSums():
    """
    Class to keep track of continous tier values for each tier level
    as well as total sum and total count of tier locations.
    """
    total_sum = 0.000
    total_count = 0

    def __init__(self):
        self.tier_sums = {
            1: 0.000,
            2: 0.000,
            3: 0.000,
            4: 0.000
        }

    def add_value(self, value):
        self.total_sum += value
        self.total_count += 1
        self.tier_sums[math.trunc(value)] += value

    def get_sums(self):
        return self.tier_sums

# =============================================================================
# MULTI-VALUE LOADERS
# =============================================================================

def load_cws_del_data() -> Tuple[List[Dict], List[Dict]]:
    """
    CWS_DEL — Community Water System Deliveries.
    Format: rows = scenarios, columns = demand unit short codes, values = tier 1.0-5.0 or NA.
    """
    csv_path = STAGING_DIR / 'CWS_DEL.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping CWS_DEL")
        return [], []

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().replace('\n', '') for c in df.columns]
    df = _ensure_unique_axes(df, csv_path)

    location_results = []
    tier_results = []

    scenario_col = df.columns[0]
    du_columns = [c for c in df.columns[1:] if c]

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row[scenario_col])
        if scenario not in ALLOWED_SCENARIOS:
            continue

        tier_sums = TierSums()

        for du_id in du_columns:
            tier_val = row[du_id]
            if pd.isna(tier_val) or str(tier_val).strip().upper() == 'NA':
                continue
            tier_continuous = float(tier_val)
            tier = math.trunc(tier_continuous)
            tier_sums.add_value(tier_continuous)
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'CWS_DEL',
                'location_type': 'demand_unit',
                'location_id': du_id,
                'location_name': du_id,
                'tier_level': tier,
                'tier_value': 1,
                'tier_continuous': tier_continuous,
                'display_order': len(location_results) + 1,
                '_source_file': 'CWS_DEL.csv',
            })

        if tier_sums.total_count > 0:
            agg = _multi_value_aggregate(scenario, 'CWS_DEL', tier_sums.get_sums(), tier_sums.total_sum, tier_sums.total_count)
            agg['_source_file'] = 'CWS_DEL.csv'
            tier_results.append(agg)

    print(f"CWS_DEL: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_ag_rev_data() -> Tuple[List[Dict], List[Dict]]:
    """
    AG_REV — Agricultural Revenue.
    Auto-detects format:
      - Long format: columns (index), scenario, region, tier
      - Wide format: first column = scenario, remaining columns = DU region IDs, values = tier 1-4
    """
    csv_path = STAGING_DIR / 'AG_REV.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping AG_REV")
        return [], []

    df = pd.read_csv(csv_path)
    df = _ensure_unique_axes(df, csv_path)

    location_results = []
    tier_results = []

    scenario_col = df.columns[0]
    du_columns = [c for c in df.columns[1:] if c]

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row[scenario_col])
        if scenario not in ALLOWED_SCENARIOS:
            continue

        tier_sums = TierSums()

        for du_id in du_columns:
            tier_val = row[du_id]
            if pd.isna(tier_val) or str(tier_val).strip().upper() == 'NA':
                continue
            tier_continuous = float(tier_val)
            tier = math.trunc(tier_continuous)
            tier_sums.add_value(tier_continuous)
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'AG_REV',
                'location_type': 'demand_unit',
                'location_id': du_id,
                'location_name': du_id,
                'tier_level': tier,
                'tier_value': 1,
                'tier_continuous': tier_continuous,
                'display_order': len(location_results) + 1,
                '_source_file': 'AG_REV.csv',
            })

        if tier_sums.total_count > 0:
            agg = _multi_value_aggregate(scenario, 'AG_REV', tier_sums.get_sums(), tier_sums.total_sum, tier_sums.total_count)
            agg['_source_file'] = 'AG_REV.csv'
            tier_results.append(agg)

    print(f"AG_REV: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def _ensure_unique_axes(df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    """Detect duplicate row or column labels in a tier staging frame.

    Identical duplicates are dropped with a `NOTICE`. Conflicting
    duplicates raise `ValueError` naming the labels so the upstream CSV
    can be fixed. Without this, `df.loc[row, col]` silently returns a
    Series on the duplicated axis and downstream `pd.isna(...)` blows up
    with the unhelpful "truth value of a Series is ambiguous" error.
    """
    def _conflicts(frame: pd.DataFrame) -> bool:
        # `.fillna('__NA__')` collapses NaN-vs-NaN, which would otherwise
        # compare unequal under `.eq` and look like a conflict.
        ref = frame.iloc[0].fillna('__NA__')
        return not frame.fillna('__NA__').eq(ref).all().all()

    if df.index.has_duplicates:
        dup_labels = sorted({str(v) for v in df.index[df.index.duplicated()]})
        conflicts = [d for d in dup_labels if _conflicts(df.loc[[d]])]
        if conflicts:
            raise ValueError(
                f"{csv_path.name}: duplicate row labels with conflicting "
                f"values: {conflicts}. Resolve in the upstream CSV "
                f"(one row per scenario)."
            )
        print(
            f"  NOTICE: {csv_path.name} has duplicate but identical rows "
            f"for {dup_labels}; keeping first occurrence."
        )
        df = df[~df.index.duplicated(keep='first')]

    if df.columns.has_duplicates:
        dup_labels = sorted({str(v) for v in df.columns[df.columns.duplicated()]})
        # Transpose so each duplicate group becomes consecutive rows; reuse
        # the same row-conflict check.
        conflicts = [d for d in dup_labels if _conflicts(df.loc[:, [d]].T)]
        if conflicts:
            raise ValueError(
                f"{csv_path.name}: duplicate column labels with conflicting "
                f"values: {conflicts}. Resolve in the upstream CSV "
                f"(one column per location)."
            )
        print(
            f"  NOTICE: {csv_path.name} has duplicate but identical columns "
            f"for {dup_labels}; keeping first occurrence."
        )
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

    return df


def load_env_flows_data() -> Tuple[List[Dict], List[Dict]]:
    """
    ENV_FLOWS — Environmental Flows.
    Format: rows = scenarios, columns = demand unit short codes, values = tier 1.0-5.0 or NA.
    """
    csv_path = STAGING_DIR / 'ENV_FLOWS.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping ENV_FLOWS")
        return [], []

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().replace('\n', '') for c in df.columns]
    df = _ensure_unique_axes(df, csv_path)

    location_results = []
    tier_results = []

    scenario_col = df.columns[0]
    du_columns = [c for c in df.columns[1:] if c]

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row[scenario_col])
        if scenario not in ALLOWED_SCENARIOS:
            continue

        tier_sums = TierSums()

        for du_id in du_columns:
            tier_val = row[du_id]
            if pd.isna(tier_val) or str(tier_val).strip().upper() == 'NA':
                continue
            tier_continuous = float(tier_val)
            tier = math.trunc(tier_continuous)
            tier_sums.add_value(tier_continuous)
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'ENV_FLOWS',
                'location_type': 'demand_unit',
                'location_id': du_id,
                'location_name': TIER_LOCATION_NAMES.get('ENV_FLOWS', {}).get(du_id, du_id),
                'tier_level': tier,
                'tier_value': 1,
                'tier_continuous': tier_continuous,
                'display_order': len(location_results) + 1,
                '_source_file': 'ENV_FLOWS.csv',
            })

        if tier_sums.total_count > 0:
            agg = _multi_value_aggregate(scenario, 'ENV_FLOWS', tier_sums.get_sums(), tier_sums.total_sum, tier_sums.total_count)
            agg['_source_file'] = 'ENV_FLOWS.csv'
            tier_results.append(agg)

    print(f"ENV_FLOWS: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_res_stor_data() -> Tuple[List[Dict], List[Dict]]:
    """
    RES_STOR — Reservoir Storage.
    Format: rows = scenarios (col 'scenario'), columns = reservoir tier names.
    """
    csv_path = STAGING_DIR / 'RES_STOR.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping RES_STOR")
        return [], []

    df = pd.read_csv(csv_path)
    df = _ensure_unique_axes(df, csv_path)
    res_columns = [c for c in df.columns if c != 'scenario']

    location_results = []
    tier_results = []

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            continue

        tier_sums = TierSums()
        display_order = 1

        for res_col in res_columns:
            tier_val = row[res_col]
            if pd.isna(tier_val):
                continue
            tier_continuous = float(tier_val)
            tier = math.trunc(tier_continuous)
            tier_sums.add_value(tier_continuous)
            res_id = _res_stor_location_id(res_col)
            res_name = TIER_LOCATION_NAMES.get('RES_STOR', {}).get(res_id, res_id)
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'RES_STOR',
                'location_type': 'reservoir',
                'location_id': res_id,
                'location_name': res_name,
                'tier_level': tier,
                'tier_value': 1,
                'tier_continuous': tier_continuous,
                'display_order': display_order,
                '_source_file': 'RES_STOR.csv',
            })
            display_order += 1

        if tier_sums.total_count > 0:
            agg = _multi_value_aggregate(scenario, 'RES_STOR', tier_sums.get_sums(), tier_sums.total_sum, tier_sums.total_count)
            agg['_source_file'] = 'RES_STOR.csv'
            tier_results.append(agg)

    print(f"RES_STOR: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_gw_stor_data() -> Tuple[List[Dict], List[Dict]]:
    """
    GW_STOR — Groundwater Storage.
    Format: rows = scenarios (col 'scenario'), columns = WBA IDs + DETAW, values = tier 0.0-5.0.
    Tier 0 is treated as tier 1 (no impact).
    """
    csv_path = STAGING_DIR / 'GW_STOR.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping GW_STOR")
        return [], []

    df = pd.read_csv(csv_path)
    df = _ensure_unique_axes(df, csv_path)
    wba_columns = [c for c in df.columns if c != 'scenario']

    location_results = []
    tier_results = []

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            continue

        tier_sums = TierSums()
        display_order = 1

        for wba_col in wba_columns:
            tier_val = row[wba_col]
            if pd.isna(tier_val):
                continue
            tier_continuous = float(tier_val)
            tier = math.trunc(tier_continuous)
            if tier == 0:
                tier_continuous = float(1)
                tier = 1  # tier 0 maps to tier 1 (no impact)
            tier_sums.add_value(tier_continuous)
            mapbox_id = convert_wba_id_to_mapbox_format(wba_col)
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'GW_STOR',
                'location_type': 'wba',
                'location_id': mapbox_id,
                'location_name': TIER_LOCATION_NAMES.get('GW_STOR', {}).get(mapbox_id, wba_col),
                'tier_level': tier,
                'tier_value': 1,
                'tier_continuous': tier_continuous,
                'display_order': display_order,
                '_source_file': 'GW_STOR.csv',
            })
            display_order += 1

        if tier_sums.total_count > 0:
            agg = _multi_value_aggregate(scenario, 'GW_STOR', tier_sums.get_sums(), tier_sums.total_sum, tier_sums.total_count)
            agg['_source_file'] = 'GW_STOR.csv'
            tier_results.append(agg)

    print(f"GW_STOR: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def _multi_value_aggregate(scenario: str, short_code: str, tier_sums: dict, total_sum: float, total_count: int) -> Dict:
    """Build a tier_result row for a multi-value tier."""
    return {
        'scenario_short_code': scenario,
        'tier_short_code': short_code,
        'tier_1_value': tier_sums[1],
        'tier_2_value': tier_sums[2],
        'tier_3_value': tier_sums[3],
        'tier_4_value': tier_sums[4],
        'norm_tier_1': round(tier_sums[1] / total_sum, 4),
        'norm_tier_2': round(tier_sums[2] / total_sum, 4),
        'norm_tier_3': round(tier_sums[3] / total_sum, 4),
        'norm_tier_4': round(tier_sums[4] / total_sum, 4),
        'total_value': total_sum,
        'total_count': total_count,
        'single_tier_level': None,
    }


# =============================================================================
# SINGLE-VALUE LOADERS
# =============================================================================

def load_delta_eco_data() -> Tuple[List[Dict], List[Dict]]:
    """
    DELTA_ECO — Delta Ecology.
    Format: scenario (numeric, e.g. '11' for s0011), TierValue.
    One location row per scenario: wba DETAW.
    """
    csv_path = STAGING_DIR / 'DELTA_ECO.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping DELTA_ECO")
        return [], []

    df = pd.read_csv(csv_path)
    df = _ensure_unique_axes(df, csv_path)

    location_results = []
    tier_results = []

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            continue
        tier_continuous = float(row['TierScore'])
        tier = math.trunc(tier_continuous)
        agg = _single_value_aggregate(scenario, 'DELTA_ECO', tier)
        agg['_source_file'] = 'DELTA_ECO.csv'
        tier_results.append(agg)
        location_results.append({
            'scenario_short_code': scenario,
            'tier_short_code': 'DELTA_ECO',
            'location_type': 'wba',
            'location_id': 'DETAW',
            'location_name': TIER_LOCATION_NAMES.get('DELTA_ECO', {}).get('DETAW', 'DETAW'),
            'tier_level': tier,
            'tier_value': 1,
            'tier_continuous': tier_continuous,
            'display_order': 1,
            '_source_file': 'DELTA_ECO.csv',
        })

    print(f"DELTA_ECO: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_fw_delta_uses_data() -> Tuple[List[Dict], List[Dict]]:
    """
    FW_DELTA_USES — Freshwater for In-Delta Uses.
    Format: scenario (s0XXX), Salinity_InDelta_Tier.
    Two compliance station locations per scenario: Emmaton (EM) and Jersey Point (JP).
    """
    csv_path = STAGING_DIR / 'FW_DELTA_USES.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping FW_DELTA_USES")
        return [], []

    df = pd.read_csv(csv_path)
    df = _ensure_unique_axes(df, csv_path)

    location_results = []
    tier_results = []

    stations = [('EM', 1), ('JP', 2)]
    names = TIER_LOCATION_NAMES.get('FW_DELTA_USES', {})

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            continue
        tier_continuous = float(row['Salinity_InDelta_Tier'])
        tier = math.trunc(tier_continuous)
        agg = _single_value_aggregate(scenario, 'FW_DELTA_USES', tier)
        agg['_source_file'] = 'FW_DELTA_USES.csv'
        tier_results.append(agg)
        for loc_id, order in stations:
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'FW_DELTA_USES',
                'location_type': 'compliance_station',
                'location_id': loc_id,
                'location_name': names.get(loc_id, loc_id),
                'tier_level': tier,
                'tier_value': 1,
                'tier_continuous': tier_continuous,
                'display_order': order,
                '_source_file': 'FW_DELTA_USES.csv',
            })

    print(f"FW_DELTA_USES: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_fw_exp_data() -> Tuple[List[Dict], List[Dict]]:
    """
    FW_EXP — Freshwater for Delta Exports.
    Format: scenario (s0XXX), Salinity_Export_Tier.
    Two network node locations per scenario: Banks (CAA003) and Jones (DMC000).
    """
    csv_path = STAGING_DIR / 'FW_EXP.csv'
    if not csv_path.exists():
        print(f"WARNING: {csv_path} not found, skipping FW_EXP")
        return [], []

    df = pd.read_csv(csv_path)
    df = _ensure_unique_axes(df, csv_path)

    location_results = []
    tier_results = []

    pumps = [('CAA003', 1), ('DMC000', 2)]
    names = TIER_LOCATION_NAMES.get('FW_EXP', {})

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            continue
        tier_continuous = float(row['Salinity_Export_Tier'])
        tier = math.trunc(tier_continuous)
        agg = _single_value_aggregate(scenario, 'FW_EXP', tier)
        agg['_source_file'] = 'FW_EXP.csv'
        tier_results.append(agg)
        for loc_id, order in pumps:
            location_results.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'FW_EXP',
                'location_type': 'network_node',
                'location_id': loc_id,
                'location_name': names.get(loc_id, loc_id),
                'tier_level': tier,
                'tier_value': 1,
                'tier_continuous': tier_continuous,
                'display_order': order,
                '_source_file': 'FW_EXP.csv',
            })

    print(f"FW_EXP: {len(location_results)} location records, {len(tier_results)} scenario aggregates")
    return location_results, tier_results


def load_salmon_data() -> Tuple[List[Dict], List[Dict]]:
    """
    WRC_SALMON_AB - Salmon Abundance.

    Reads staging/WRC_SALMON_AB.csv (produced by stage_tier_results.py from
    the data team's salmon/TIERS_WRLCM_01_BestYearSummary_*.csv drop).
    Expected columns: scenario, tier_score_cont.

    All scenarios filtered through ALLOWED_SCENARIOS. Single representative
    location per scenario: network node SAC299 (Sacramento River at Keswick).

    Fails fast if the CSV is missing or malformed. The previous hardcoded
    tier-4 fallback was removed once real salmon data was delivered, to
    prevent a missing file from silently corrupting the DB.
    """
    csv_path = STAGING_DIR / 'WRC_SALMON_AB.csv'
    if not csv_path.exists():
        raise FileNotFoundError(
            f"{csv_path} not found. Run stage_tier_results.py first, or pass "
            f"--skip WRC_SALMON_AB if you explicitly want to omit salmon."
        )

    df = pd.read_csv(csv_path)
    df = _ensure_unique_axes(df, csv_path)
    missing_cols = [c for c in ('scenario', 'tier_score_cont') if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"{csv_path.name} missing expected columns {missing_cols}; "
            f"got {list(df.columns)}. Check the upstream salmon CSV."
        )

    location_results: List[Dict] = []
    tier_results: List[Dict] = []
    skipped_scenarios: List[str] = []
    parse_errors: List[str] = []

    for _, row in df.iterrows():
        scenario = normalize_scenario_id(row['scenario'])
        if scenario not in ALLOWED_SCENARIOS:
            skipped_scenarios.append(scenario)
            continue
        try:
            tier_continuous = row['tier_score_cont']
            tier = math.trunc(tier_continuous)
        except ValueError as exc:
            parse_errors.append(f"{scenario}: {exc}")
            continue
        agg = _single_value_aggregate(scenario, 'WRC_SALMON_AB', tier)
        agg['_source_file'] = csv_path.name
        tier_results.append(agg)
        location_results.append({
            'scenario_short_code': scenario,
            'tier_short_code': 'WRC_SALMON_AB',
            'location_type': 'network_node',
            'location_id': 'SAC299',
            'location_name': TIER_LOCATION_NAMES.get('WRC_SALMON_AB', {}).get('SAC299', 'SAC299'),
            'tier_level': tier,
            'tier_value': 1,
            'tier_continuous': tier_continuous,
            'display_order': 1,
            '_source_file': csv_path.name,
        })

    if skipped_scenarios:
        preview = ', '.join(sorted(set(skipped_scenarios))[:10])
        more = '...' if len(set(skipped_scenarios)) > 10 else ''
        print(f"  WRC_SALMON_AB skipped (not in ALLOWED_SCENARIOS): {preview}{more}")
    if parse_errors:
        raise ValueError(
            f"WRC_SALMON_AB parse errors in {csv_path.name}: "
            f"{'; '.join(parse_errors[:5])}"
            f"{'...' if len(parse_errors) > 5 else ''}"
        )

    print(f"WRC_SALMON_AB: {len(location_results)} location records, "
          f"{len(tier_results)} scenario aggregates  (from {csv_path.name})")
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
        'total_count': None,
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
        "    location_name, tier_level, tier_value, tier_continuous, display_order, tier_version_id",
        ") VALUES",
    ]

    values = []
    for r in location_results:
        values.append(
            f"    ({escape_sql(r['scenario_short_code'])}, {escape_sql(r['tier_short_code'])}, "
            f"{escape_sql(r['location_type'])}, {escape_sql(r['location_id'])}, "
            f"{escape_sql(r['location_name'])}, {r['tier_level']}, {r['tier_value']}, {r['tier_continuous']}, "
            f"{r['display_order']}, {TIER_VERSION_ID})"
        )

    lines.append(',\n'.join(values))
    lines.append("ON CONFLICT (scenario_short_code, tier_short_code, location_id, tier_version_id)")
    lines.append("DO UPDATE SET")
    lines.append("    location_type = EXCLUDED.location_type,")
    lines.append("    location_name = EXCLUDED.location_name,")
    lines.append("    tier_level = EXCLUDED.tier_level,")
    lines.append("    tier_value = EXCLUDED.tier_value,")
    lines.append("    tier_continuous = EXCLUDED.tier_continuous,")
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
        "    total_value, total_count, single_tier_level, tier_version_id",
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
            f"{escape_sql(r['total_value'])}, {escape_sql(r['total_count'])}, {escape_sql(r['single_tier_level'])}, "
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
    lines.append("    total_count = EXCLUDED.total_count,")
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
# MANIFEST & VERIFICATION
# =============================================================================

def write_manifest(location_results: List[Dict], tier_results: List[Dict],
                   output_dir: Path) -> Path:
    """
    Write a CSV manifest of every row that will be upserted.
    Returns the manifest file path.
    """
    manifest_path = output_dir / 'tier_upload_manifest.csv'

    with open(manifest_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'table', 'tier_short_code', 'scenario_short_code',
            'location_id', 'tier_level', 'source_file',
        ])
        for r in tier_results:
            writer.writerow([
                'tier_result',
                r['tier_short_code'],
                r['scenario_short_code'],
                '',
                r.get('single_tier_level', ''),
                r.get('_source_file', ''),
            ])
        for r in location_results:
            writer.writerow([
                'tier_location_result',
                r['tier_short_code'],
                r['scenario_short_code'],
                r['location_id'],
                r['tier_level'],
                r.get('_source_file', ''),
            ])

    print(f"\nManifest written: {manifest_path}")
    print(f"  tier_result rows      : {len(tier_results)}")
    print(f"  tier_location_result  : {len(location_results)}")
    return manifest_path


def generate_verification_sql() -> str:
    """Generate extended verification SQL queries."""
    return f"""
-- ============================================================================
-- EXTENDED VERIFICATION
-- ============================================================================

-- Sanity check: norm_tier values should sum to ~1.0 for multi-value tiers
-- (returns zero rows if data is consistent)
SELECT tier_short_code, scenario_short_code,
       norm_tier_1 + norm_tier_2 + norm_tier_3 + norm_tier_4 AS norm_sum
FROM tier_result
WHERE tier_version_id = {TIER_VERSION_ID}
  AND single_tier_level IS NULL
  AND ABS((norm_tier_1 + norm_tier_2 + norm_tier_3 + norm_tier_4) - 1.0) > 0.01;

-- Spot-check: 2 random location rows per tier type
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY tier_short_code ORDER BY RANDOM()) AS rn
    FROM tier_location_result
    WHERE tier_version_id = {TIER_VERSION_ID}
) sub WHERE rn <= 2
ORDER BY tier_short_code, rn;
"""


def run_verify(manifest_path: Path):
    """
    Post-upload verification: compare manifest against live database.
    Requires DATABASE_URL env var.
    """
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("Error: DATABASE_URL not set. Cannot run verification.")
        sys.exit(1)

    if not manifest_path.exists():
        print(f"Error: Manifest not found at {manifest_path}")
        print("Run with --dry-run or --output-sql first to generate the manifest.")
        sys.exit(1)

    try:
        import psycopg2
    except ImportError:
        print("Error: psycopg2 not installed.")
        sys.exit(1)

    print(f"Reading manifest: {manifest_path}")
    manifest_rows = []
    with open(manifest_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            manifest_rows.append(row)

    tier_manifest = [r for r in manifest_rows if r['table'] == 'tier_result']
    loc_manifest = [r for r in manifest_rows if r['table'] == 'tier_location_result']

    print(f"Manifest: {len(tier_manifest)} tier_result, {len(loc_manifest)} tier_location_result")

    conn = psycopg2.connect(database_url)
    cur = conn.cursor()

    mismatches = 0
    missing = 0
    checked = 0

    print("\nVerifying tier_result rows...")
    for r in tier_manifest:
        cur.execute(
            "SELECT single_tier_level FROM tier_result "
            "WHERE tier_version_id = %s AND tier_short_code = %s AND scenario_short_code = %s",
            (TIER_VERSION_ID, r['tier_short_code'], r['scenario_short_code'])
        )
        row = cur.fetchone()
        checked += 1
        if row is None:
            missing += 1
            print(f"  MISSING: {r['tier_short_code']} / {r['scenario_short_code']}")
        elif r['tier_level'] and row[0] is not None:
            if int(r['tier_level']) != row[0]:
                mismatches += 1
                print(f"  MISMATCH: {r['tier_short_code']} / {r['scenario_short_code']} "
                      f"expected={r['tier_level']} got={row[0]}")

    print("\nVerifying tier_location_result rows...")
    for r in loc_manifest:
        cur.execute(
            "SELECT tier_level FROM tier_location_result "
            "WHERE tier_version_id = %s AND tier_short_code = %s "
            "AND scenario_short_code = %s AND location_id = %s",
            (TIER_VERSION_ID, r['tier_short_code'], r['scenario_short_code'], r['location_id'])
        )
        row = cur.fetchone()
        checked += 1
        if row is None:
            missing += 1
            if missing <= 10:
                print(f"  MISSING: {r['tier_short_code']} / {r['scenario_short_code']} / {r['location_id']}")
        elif int(r['tier_level']) != row[0]:
            mismatches += 1
            if mismatches <= 10:
                print(f"  MISMATCH: {r['tier_short_code']} / {r['scenario_short_code']} / {r['location_id']} "
                      f"expected={r['tier_level']} got={row[0]}")

    cur.close()
    conn.close()

    print(f"\n{'=' * 60}")
    print("VERIFICATION RESULT")
    print(f"{'=' * 60}")
    print(f"  Rows checked : {checked}")
    print(f"  Missing      : {missing}")
    print(f"  Mismatches   : {mismatches}")
    if missing == 0 and mismatches == 0:
        print("  Status       : PASS")
    else:
        print("  Status       : FAIL")
        sys.exit(1)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Load all tier results from staging CSVs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tier outcomes loaded:
  CWS_DEL       - Community Water System Deliveries  (multi-value)
  AG_REV        - Agricultural Revenue               (multi-value)
  ENV_FLOWS     - Environmental Flows                (multi-value)
  RES_STOR      - Reservoir Storage                  (multi-value)
  GW_STOR       - Groundwater Storage                (multi-value)
  DELTA_ECO     - Delta Ecology                      (single-value)
  FW_DELTA_USES - Freshwater for In-Delta Uses       (single-value)
  FW_EXP        - Freshwater for Delta Exports       (single-value)
  WRC_SALMON_AB - Winter-run Salmon Abundance        (single-value; from staging/WRC_SALMON_AB.csv)
        """
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview data counts without generating SQL')
    parser.add_argument('--output-sql', type=str,
                        help='Write SQL to this file instead of executing. '
                             'Bare filenames (no "/") are auto-routed into '
                             'etl/tier_data/output/ which is gitignored.')
    parser.add_argument('--only', type=str,
                        help='Comma-separated tier short codes to load (e.g. ENV_FLOWS,RES_STOR)')
    parser.add_argument('--verify', type=str, nargs='?', const='auto',
                        help='Post-upload verification against manifest CSV (optionally specify manifest path)')
    parser.add_argument('--scenarios-override', nargs='*', default=[],
                        help='Per-invocation replacement for ACTIVE_SCENARIOS. '
                             'Comma/whitespace/newline separated. Use to pre-flight a '
                             'scenario before flipping is_active=1. Logs a WARNING when active.')

    args = parser.parse_args()

    global ALLOWED_SCENARIOS, TIER_LOCATION_NAMES  # noqa: PLW0603
    ALLOWED_SCENARIOS = resolve_active_scenarios(args.scenarios_override)

    # Resolve display names from tier_location joined to the entity tables.
    # Falls back silently when DATABASE_URL is unset: loaders will then write
    # location_id as location_name. The dry-run / output-sql UX still works.
    conn = get_db_connection(required=False)
    if conn is not None:
        try:
            TIER_LOCATION_NAMES = fetch_tier_location_names(conn)
            print(f"Loaded tier-location names from DB ({sum(len(v) for v in TIER_LOCATION_NAMES.values())} rows across {len(TIER_LOCATION_NAMES)} tiers)")

            # Coverage scan over the active catalog. Warn-only: if an
            # entity row or geometry is missing, the loader continues
            # and the location_name falls back to location_id; the
            # alert tells the developer to run the audit script.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tier_short_code, location_type, location_id "
                    "FROM tier_location WHERE is_active = TRUE"
                )
                catalog_rows = cur.fetchall()
            # catalog_rows is already a list of (tier, location_type, location_id)
            # triples, which is exactly what assess_coverage expects.
            reports = assess_coverage(conn, catalog_rows)
            for line in format_coverage_warnings(catalog_rows, reports):
                print(line)
        finally:
            conn.close()
    else:
        print("WARNING: DATABASE_URL not set. location_name will fall back to location_id.")

    if args.verify:
        manifest_path = (STAGING_DIR / 'tier_upload_manifest.csv'
                         if args.verify == 'auto' else Path(args.verify))
        run_verify(manifest_path)
        return

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

    write_manifest(all_location_results, all_tier_results, STAGING_DIR)

    if args.dry_run:
        print("\nDRY RUN — no SQL generated. Manifest written for review.")
        return

    tier_sql = generate_tier_result_sql(all_tier_results)
    location_sql = generate_location_result_sql(all_location_results)
    deactivation_sql = generate_deactivation_sql()
    extended_verification = generate_verification_sql()

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
{extended_verification}
"""

    if args.output_sql:
        raw = Path(args.output_sql)
        if raw.is_absolute() or len(raw.parts) > 1:
            output_path = raw
        else:
            output_path = OUTPUT_DIR / raw
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(full_sql)
        print(f"\nSQL written to: {output_path}")
        print("\nTo apply on Cloud9:")
        print(f"  psql $DATABASE_URL -f {output_path}")
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
