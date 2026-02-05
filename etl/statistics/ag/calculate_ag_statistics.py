#!/usr/bin/env python3
"""
Calculate delivery and shortage statistics for agricultural demand units.

This module processes agricultural demand unit data using:
- AW_{DU_ID} delivery variables (Applied Water)
- GW_SHORT_{DU_ID} shortage variables (SJR/Tulare regions only)
- DEL_*_PAG aggregate delivery variables

Note: Sacramento region DUs (WBAs 02-26) do NOT have shortage data in CalSim output.

Usage:
    python calculate_ag_statistics.py --scenario s0020
    python calculate_ag_statistics.py --scenario s0020 --csv-path /path/to/calsim_output.csv
"""

import argparse
import csv
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Optional: boto3 for S3 access
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

# Optional: psycopg2 for database access
try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("ag_statistics")

# Known scenarios
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027', 's0029']

# S3 bucket configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

# Paths relative to project
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DU_AGRICULTURE_CSV = PROJECT_ROOT / "database/seed_tables/04_calsim_data/du_agriculture_entity.csv"

# Percentiles for statistics
DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# Minimum threshold for counting a year as having a "shortage" (in TAF)
# This filters out floating-point precision artifacts from CalSim's linear programming solver.
# 0.1 TAF = 100 acre-feet, which is < 0.05% of typical delivery
SHORTAGE_THRESHOLD_TAF = 0.1

# Pre-computed aggregate definitions
# These aggregates have direct CalSim variables for both delivery and shortage
AG_AGGREGATES = {
    'swp_pag': {
        'delivery_var': 'DEL_SWP_PAG',
        'shortage_var': 'SHORT_SWP_PAG',
        'description': 'SWP Project AG - Total',
    },
    'swp_pag_n': {
        'delivery_var': 'DEL_SWP_PAG_N',
        'shortage_var': 'SHORT_SWP_PAG_N',
        'description': 'SWP Project AG - North of Delta',
    },
    'swp_pag_s': {
        'delivery_var': 'DEL_SWP_PAG_S',
        'shortage_var': 'SHORT_SWP_PAG_S',
        'description': 'SWP Project AG - South of Delta',
    },
    'cvp_pag_n': {
        'delivery_var': 'DEL_CVP_PAG_N',
        'shortage_var': 'SHORT_CVP_PAG_N',
        'description': 'CVP Project AG - North of Delta',
    },
    'cvp_pag_s': {
        'delivery_var': 'DEL_CVP_PAG_S',
        'shortage_var': 'SHORT_CVP_PAG_S',
        'description': 'CVP Project AG - South of Delta',
    },
}

# Sacramento region WBAs (do NOT have GW_SHORT data)
SACRAMENTO_WBAS = [
    '02', '03', '04', '05', '06', '07N', '07S',
    '08N', '08S', '09', '10', '11', '12', '13',
    '14', '15N', '15S', '16', '17N', '17S', '18',
    '19', '20', '21', '22', '23', '24', '25', '26N', '26S'
]


# =============================================================================
# DATA LOADING
# =============================================================================

def load_ag_demand_units(csv_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """
    Load agricultural demand unit metadata from du_agriculture_entity.csv.

    Returns dict keyed by du_id with unit details.
    """
    if csv_path is None:
        csv_path = DU_AGRICULTURE_CSV

    if not csv_path.exists():
        log.warning(f"du_agriculture_entity.csv not found at {csv_path}")
        return {}

    demand_units = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            du_id = row.get('DU_ID', '')
            if du_id:
                demand_units[du_id] = {
                    'wba_id': row.get('WBA_ID', ''),
                    'hydrologic_region': row.get('hydrologic_region', ''),
                    'cs3_type': row.get('CS3_Type', ''),
                    'agency': row.get('agency', ''),
                    'provider': row.get('provider', ''),
                    'gw': row.get('gw', '1') == '1',
                    'sw': row.get('sw', '1') == '1',
                    'has_gis_data': row.get('has_gis_data', 'True') == 'True',
                }

    log.info(f"Loaded {len(demand_units)} agricultural demand units from {csv_path}")
    return demand_units


def load_calsim_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """
    Load CalSim output CSV from S3 bucket.

    Handles the DSS export format with 7 header rows.
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access. Install with: pip install boto3")

    s3 = boto3.client('s3')

    possible_keys = [
        f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv",
        f"scenario/{scenario_id}/csv/{scenario_id}_DV.csv",
    ]

    for key in possible_keys:
        try:
            log.info(f"Trying S3 key: s3://{S3_BUCKET}/{key}")
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_csv(response['Body'], header=None, nrows=8)

            col_names = df.iloc[1].tolist()

            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data_df = pd.read_csv(response['Body'], header=None, skiprows=7, low_memory=False)
            data_df.columns = col_names

            log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
            return data_df

        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            log.warning(f"Error loading {key}: {e}")
            continue

    raise FileNotFoundError(f"Could not find CalSim output for {scenario_id} in S3")


def load_calsim_csv_from_file(file_path: str) -> pd.DataFrame:
    """
    Load CalSim output CSV from local file.

    Handles the DSS export format with 7 header rows.
    """
    log.info(f"Loading from file: {file_path}")

    header_df = pd.read_csv(file_path, header=None, nrows=8)
    col_names = header_df.iloc[1].tolist()

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    data_df.columns = col_names

    log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add water year and water month columns.

    Water month: Oct(10)->1, Nov(11)->2, ..., Sep(9)->12
    Water year: Oct-Dec belong to next water year
    """
    df = df.copy()

    first_col = df.columns[0]
    date_values = df[first_col]

    try:
        df['DateTime'] = pd.to_datetime(date_values, errors='coerce')

        if df['DateTime'].notna().sum() > 0:
            df['CalendarMonth'] = df['DateTime'].dt.month
            df['CalendarYear'] = df['DateTime'].dt.year

            df['WaterMonth'] = ((df['CalendarMonth'] - 10) % 12) + 1

            df['WaterYear'] = df['CalendarYear']
            df.loc[df['CalendarMonth'] >= 10, 'WaterYear'] += 1

            log.info(f"Detected monthly data: {df['DateTime'].min()} to {df['DateTime'].max()}")
            return df
    except Exception as e:
        log.debug(f"Could not parse as datetime: {e}")

    date_numeric = pd.to_numeric(date_values, errors='coerce')
    if date_numeric.notna().all() and (date_numeric >= 1900).all() and (date_numeric <= 2100).all():
        df['WaterYear'] = date_numeric.astype(int)
        df['WaterMonth'] = 0
        log.info(f"Detected annual data: years {df['WaterYear'].min()}-{df['WaterYear'].max()}")
        return df

    raise ValueError(f"Could not parse date column '{first_col}' as datetime or year values")


# =============================================================================
# DEMAND UNIT STATISTICS
# =============================================================================

def calculate_du_delivery_monthly(
    df: pd.DataFrame,
    du_id: str
) -> List[Dict[str, Any]]:
    """
    Calculate monthly delivery statistics for an agricultural demand unit.

    Uses AW_{DU_ID} variable from CalSim output.
    """
    delivery_var = f"AW_{du_id}"

    if delivery_var not in df.columns:
        log.debug(f"No delivery variable found for {du_id}: {delivery_var}")
        return []

    df_copy = df.copy()
    df_copy['delivery'] = df_copy[delivery_var]

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        data = df_copy['delivery'].dropna()
        if data.empty:
            return []

        row = {
            'du_id': du_id,
            'water_month': 0,
            'delivery_avg_taf': round(float(data.mean()), 2),
            'delivery_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'sample_count': len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)

        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(data, p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy['WaterMonth'] == wm]['delivery'].dropna()
            if month_data.empty:
                continue

            row = {
                'du_id': du_id,
                'water_month': wm,
                'delivery_avg_taf': round(float(month_data.mean()), 2),
                'delivery_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'sample_count': len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)

            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(month_data, p)), 2)

            results.append(row)

    return results


def calculate_du_shortage_monthly(
    df: pd.DataFrame,
    du_id: str,
    du_info: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Calculate monthly groundwater restriction shortage for an agricultural demand unit.

    IMPORTANT: Uses GW_SHORT_{DU_ID} (Groundwater Restriction Shortage), which represents
    shortage due to groundwater pumping restrictions, NOT total agricultural delivery shortage.
    This is a COEQWAL-specific variable added for testing groundwater restrictions.
    
    For aggregate delivery shortage (shortage = target - actual delivery), use the aggregate
    statistics with SHORT_CVP_PAG_N/S and SHORT_SWP_PAG_N/S variables instead.

    Note: Only SJR/Tulare regions have GW_SHORT data; Sacramento WBAs do not.
    Not all scenarios include GW_SHORT variables (e.g., s0023, s0024 are missing them).

    Also calculates shortage_pct_of_demand = shortage / (delivery + shortage) * 100
    """
    # Check if this DU should have shortage data
    wba_id = du_info.get('wba_id', '')
    if wba_id in SACRAMENTO_WBAS:
        log.debug(f"Sacramento region DU {du_id} - no shortage data expected")
        return []

    shortage_var = f"GW_SHORT_{du_id}"
    delivery_var = f"AW_{du_id}"

    if shortage_var not in df.columns:
        log.debug(f"No shortage variable found for {du_id}: {shortage_var}")
        return []

    df_copy = df.copy()
    df_copy['shortage'] = df_copy[shortage_var]

    # Get delivery for calculating shortage % of demand
    if delivery_var in df.columns:
        df_copy['delivery'] = df_copy[delivery_var]
    else:
        df_copy['delivery'] = 0

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        shortage_data = df_copy['shortage'].dropna()
        delivery_data = df_copy['delivery'].dropna()

        if shortage_data.empty:
            return []

        # Use threshold to filter out floating-point noise from CalSim solver
        shortage_count = (shortage_data > SHORTAGE_THRESHOLD_TAF).sum()

        # Calculate demand and shortage % of demand
        demand_data = delivery_data + shortage_data
        shortage_pct = []
        for s, d in zip(shortage_data, demand_data):
            if d > 0:
                shortage_pct.append((s / d) * 100)
            else:
                shortage_pct.append(0)
        avg_shortage_pct = np.mean(shortage_pct) if shortage_pct else 0

        row = {
            'du_id': du_id,
            'water_month': 0,
            'shortage_avg_taf': round(float(shortage_data.mean()), 2),
            'shortage_cv': round(float(shortage_data.std() / shortage_data.mean()), 4) if shortage_data.mean() > 0 else 0,
            'shortage_frequency_pct': round((shortage_count / len(shortage_data)) * 100, 2),
            'shortage_pct_of_demand_avg': round(float(avg_shortage_pct), 2),
            'sample_count': len(shortage_data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(shortage_data, p)), 2)

        # Add exceedance percentiles for shortage
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(shortage_data, p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            mask = df_copy['WaterMonth'] == wm
            shortage_data = df_copy.loc[mask, 'shortage'].dropna()
            delivery_data = df_copy.loc[mask, 'delivery'].dropna()

            if shortage_data.empty:
                continue

            # Use threshold to filter out floating-point noise from CalSim solver
            shortage_count = (shortage_data > SHORTAGE_THRESHOLD_TAF).sum()

            # Calculate shortage % of demand for this month
            demand_data = delivery_data.values + shortage_data.values
            shortage_pct = []
            for s, d in zip(shortage_data.values, demand_data):
                if d > 0:
                    shortage_pct.append((s / d) * 100)
                else:
                    shortage_pct.append(0)
            avg_shortage_pct = np.mean(shortage_pct) if shortage_pct else 0

            row = {
                'du_id': du_id,
                'water_month': wm,
                'shortage_avg_taf': round(float(shortage_data.mean()), 2),
                'shortage_cv': round(float(shortage_data.std() / shortage_data.mean()), 4) if shortage_data.mean() > 0 else 0,
                'shortage_frequency_pct': round((shortage_count / len(shortage_data)) * 100, 2),
                'shortage_pct_of_demand_avg': round(float(avg_shortage_pct), 2),
                'sample_count': len(shortage_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(shortage_data, p)), 2)

            # Add exceedance percentiles for shortage
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(shortage_data, p)), 2)

            results.append(row)

    return results


def calculate_du_period_summary(
    df: pd.DataFrame,
    du_id: str,
    du_info: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary for an agricultural demand unit.
    """
    delivery_var = f"AW_{du_id}"
    shortage_var = f"GW_SHORT_{du_id}"

    if delivery_var not in df.columns:
        return None

    df_copy = df.copy()
    df_copy['delivery'] = df_copy[delivery_var]

    water_years = sorted(df_copy['WaterYear'].unique())

    result = {
        'du_id': du_id,
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),
    }

    # Annual delivery statistics
    annual_delivery = df_copy.groupby('WaterYear')['delivery'].sum()
    result['annual_delivery_avg_taf'] = round(float(annual_delivery.mean()), 2)
    if annual_delivery.mean() > 0:
        result['annual_delivery_cv'] = round(float(annual_delivery.std() / annual_delivery.mean()), 4)
    else:
        result['annual_delivery_cv'] = 0

    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, p)), 2)

    # Shortage statistics (only for non-Sacramento regions)
    wba_id = du_info.get('wba_id', '')
    has_shortage = shortage_var in df.columns and wba_id not in SACRAMENTO_WBAS

    if has_shortage:
        df_copy['shortage'] = df_copy[shortage_var]
        annual_shortage = df_copy.groupby('WaterYear')['shortage'].sum()
        # Use threshold to filter out floating-point noise from CalSim solver
        shortage_years = (annual_shortage > SHORTAGE_THRESHOLD_TAF).sum()

        result['annual_shortage_avg_taf'] = round(float(annual_shortage.mean()), 2)
        result['shortage_years_count'] = int(shortage_years)
        result['shortage_frequency_pct'] = round((shortage_years / len(water_years)) * 100, 2)

        # Calculate annual shortage % of demand
        annual_demand = annual_delivery + annual_shortage
        shortage_pct = []
        for s, d in zip(annual_shortage.values, annual_demand.values):
            if d > 0:
                shortage_pct.append((s / d) * 100)
            else:
                shortage_pct.append(0)
        result['annual_shortage_pct_of_demand'] = round(float(np.mean(shortage_pct)), 2)

        # Reliability = % of demand met = delivery / demand * 100
        if result['annual_delivery_avg_taf'] + result['annual_shortage_avg_taf'] > 0:
            demand_avg = result['annual_delivery_avg_taf'] + result['annual_shortage_avg_taf']
            result['reliability_pct'] = round((result['annual_delivery_avg_taf'] / demand_avg) * 100, 2)
            result['avg_pct_demand_met'] = result['reliability_pct']
            result['annual_demand_avg_taf'] = round(float(demand_avg), 2)
        else:
            result['reliability_pct'] = 100.0
            result['avg_pct_demand_met'] = 100.0
            result['annual_demand_avg_taf'] = result['annual_delivery_avg_taf']
    else:
        result['annual_shortage_avg_taf'] = None
        result['shortage_years_count'] = None
        result['shortage_frequency_pct'] = None
        result['annual_shortage_pct_of_demand'] = None
        result['reliability_pct'] = None
        result['avg_pct_demand_met'] = None
        result['annual_demand_avg_taf'] = None

    return result


# =============================================================================
# AGGREGATE STATISTICS
# =============================================================================

def calculate_aggregate_monthly(
    df: pd.DataFrame,
    aggregate_code: str,
    delivery_var: str,
    shortage_var: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Calculate monthly statistics for an agricultural aggregate.

    Uses pre-computed aggregate variables like DEL_SWP_PAG and SHORT_SWP_PAG.
    """
    if delivery_var not in df.columns:
        log.debug(f"No aggregate delivery variable found: {delivery_var}")
        return []

    df_copy = df.copy()
    df_copy['delivery'] = df_copy[delivery_var]

    has_shortage = shortage_var and shortage_var in df.columns
    if has_shortage:
        df_copy['shortage'] = df_copy[shortage_var]

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        data = df_copy['delivery'].dropna()
        if data.empty:
            return []

        row = {
            'aggregate_code': aggregate_code,
            'water_month': 0,
            'delivery_avg_taf': round(float(data.mean()), 2),
            'delivery_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'sample_count': len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)

        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(data, p)), 2)

        # Shortage statistics
        if has_shortage:
            shortage_data = df_copy['shortage'].dropna()
            if not shortage_data.empty:
                row['shortage_avg_taf'] = round(float(shortage_data.mean()), 2)
                row['shortage_cv'] = round(float(shortage_data.std() / shortage_data.mean()), 4) if shortage_data.mean() > 0 else 0
                # Use threshold to filter floating-point noise
                row['shortage_frequency_pct'] = round(((shortage_data > SHORTAGE_THRESHOLD_TAF).sum() / len(shortage_data)) * 100, 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy['WaterMonth'] == wm]['delivery'].dropna()
            if month_data.empty:
                continue

            row = {
                'aggregate_code': aggregate_code,
                'water_month': wm,
                'delivery_avg_taf': round(float(month_data.mean()), 2),
                'delivery_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'sample_count': len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)

            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(month_data, p)), 2)

            # Shortage statistics
            if has_shortage:
                shortage_month = df_copy[df_copy['WaterMonth'] == wm]['shortage'].dropna()
                if not shortage_month.empty:
                    row['shortage_avg_taf'] = round(float(shortage_month.mean()), 2)
                    row['shortage_cv'] = round(float(shortage_month.std() / shortage_month.mean()), 4) if shortage_month.mean() > 0 else 0
                    # Use threshold to filter floating-point noise
                    row['shortage_frequency_pct'] = round(((shortage_month > SHORTAGE_THRESHOLD_TAF).sum() / len(shortage_month)) * 100, 2)

            results.append(row)

    return results


def calculate_aggregate_period_summary(
    df: pd.DataFrame,
    aggregate_code: str,
    delivery_var: str,
    shortage_var: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary for an agricultural aggregate.
    
    Uses SHORT_CVP_PAG_N/S and SHORT_SWP_PAG_N/S for shortage statistics.
    """
    if delivery_var not in df.columns:
        return None

    df_copy = df.copy()
    df_copy['delivery'] = df_copy[delivery_var]

    has_shortage = shortage_var and shortage_var in df.columns
    if has_shortage:
        df_copy['shortage'] = df_copy[shortage_var]

    water_years = sorted(df_copy['WaterYear'].unique())

    result = {
        'aggregate_code': aggregate_code,
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),
    }

    annual_delivery = df_copy.groupby('WaterYear')['delivery'].sum()
    result['annual_delivery_avg_taf'] = round(float(annual_delivery.mean()), 2)
    if annual_delivery.mean() > 0:
        result['annual_delivery_cv'] = round(float(annual_delivery.std() / annual_delivery.mean()), 4)
    else:
        result['annual_delivery_cv'] = 0

    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, p)), 2)

    # Shortage statistics
    if has_shortage:
        annual_shortage = df_copy.groupby('WaterYear')['shortage'].sum()
        # Use threshold to filter floating-point noise from CalSim solver
        shortage_years = (annual_shortage > SHORTAGE_THRESHOLD_TAF).sum()

        result['annual_shortage_avg_taf'] = round(float(annual_shortage.mean()), 2)
        result['shortage_years_count'] = int(shortage_years)
        result['shortage_frequency_pct'] = round((shortage_years / len(water_years)) * 100, 2)

        for p in EXCEEDANCE_PERCENTILES:
            result[f'shortage_exc_p{p}'] = round(float(np.percentile(annual_shortage, p)), 2)

        # Reliability = 1 - (avg shortage / avg delivery)
        if result['annual_delivery_avg_taf'] > 0:
            result['reliability_pct'] = round(
                (1 - result['annual_shortage_avg_taf'] / result['annual_delivery_avg_taf']) * 100, 2
            )
        else:
            result['reliability_pct'] = None
    else:
        result['annual_shortage_avg_taf'] = None
        result['shortage_years_count'] = None
        result['shortage_frequency_pct'] = None
        result['reliability_pct'] = None

    return result


# =============================================================================
# MAIN CALCULATION FUNCTION
# =============================================================================

def calculate_all_ag_statistics(
    scenario_id: str,
    csv_path: Optional[str] = None
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Calculate all statistics for agricultural demand units for a scenario.

    Returns:
        Tuple of (
            du_delivery_monthly_rows,
            du_shortage_monthly_rows,
            du_period_summary_rows,
            aggregate_monthly_rows,
            aggregate_period_summary_rows
        )
    """
    log.info(f"Processing scenario: {scenario_id}")

    # Load demand unit metadata
    demand_units = load_ag_demand_units()

    # Load CalSim output
    if csv_path:
        df = load_calsim_csv_from_file(csv_path)
    else:
        df = load_calsim_csv_from_s3(scenario_id)

    # Add water year/month
    df = add_water_year_month(df)

    available_columns = list(df.columns)
    log.info(f"Available columns: {len(available_columns)}")

    # Find all AW_* columns to get the list of DUs in this scenario
    aw_columns = [c for c in available_columns if c.startswith('AW_') and not any(
        suffix in c for suffix in ['_ANN_DV', '_WLOSS', '_ADD_DV', '_ANNDV']
    )]
    du_ids_in_data = [c.replace('AW_', '') for c in aw_columns]
    log.info(f"Found {len(du_ids_in_data)} agricultural demand units with delivery data")

    du_delivery_monthly_rows = []
    du_shortage_monthly_rows = []
    du_period_summary_rows = []

    delivery_count = 0
    shortage_count = 0

    for du_id in du_ids_in_data:
        # Get DU info from entity table (if available)
        du_info = demand_units.get(du_id, {
            'wba_id': du_id.split('_')[0] if '_' in du_id else '',
            'hydrologic_region': '',
            'cs3_type': '',
        })

        # Calculate delivery monthly
        monthly_rows = calculate_du_delivery_monthly(df, du_id)
        if monthly_rows:
            delivery_count += 1
            for row in monthly_rows:
                row['scenario_short_code'] = scenario_id
            du_delivery_monthly_rows.extend(monthly_rows)

        # Calculate shortage monthly (only for non-Sacramento)
        shortage_rows = calculate_du_shortage_monthly(df, du_id, du_info)
        if shortage_rows:
            shortage_count += 1
            for row in shortage_rows:
                row['scenario_short_code'] = scenario_id
            du_shortage_monthly_rows.extend(shortage_rows)

        # Calculate period summary
        summary = calculate_du_period_summary(df, du_id, du_info)
        if summary:
            summary['scenario_short_code'] = scenario_id
            du_period_summary_rows.append(summary)

    log.info(f"Processed {delivery_count} DUs with delivery data, {shortage_count} DUs with shortage data")

    # Calculate aggregate statistics
    aggregate_monthly_rows = []
    aggregate_period_summary_rows = []

    for agg_code, agg_info in AG_AGGREGATES.items():
        delivery_var = agg_info['delivery_var']
        shortage_var = agg_info.get('shortage_var')  # Now using SHORT_CVP_PAG_N/S etc.

        # Monthly
        monthly_rows = calculate_aggregate_monthly(df, agg_code, delivery_var, shortage_var)
        for row in monthly_rows:
            row['scenario_short_code'] = scenario_id
        aggregate_monthly_rows.extend(monthly_rows)

        # Period summary
        summary = calculate_aggregate_period_summary(df, agg_code, delivery_var, shortage_var)
        if summary:
            summary['scenario_short_code'] = scenario_id
            aggregate_period_summary_rows.append(summary)

    log.info(f"Processed {len(AG_AGGREGATES)} aggregates")

    log.info(f"Generated: {len(du_delivery_monthly_rows)} DU delivery monthly, "
             f"{len(du_shortage_monthly_rows)} DU shortage monthly, "
             f"{len(du_period_summary_rows)} DU period summary, "
             f"{len(aggregate_monthly_rows)} aggregate monthly, "
             f"{len(aggregate_period_summary_rows)} aggregate period summary rows")

    return (
        du_delivery_monthly_rows,
        du_shortage_monthly_rows,
        du_period_summary_rows,
        aggregate_monthly_rows,
        aggregate_period_summary_rows
    )


def convert_numpy(val):
    """Convert numpy types to Python native types."""
    if val is None:
        return None
    if isinstance(val, (np.integer, np.int64, np.int32)):
        return int(val)
    if isinstance(val, (np.floating, np.float64, np.float32)):
        return float(val)
    return val


def save_to_database(
    scenario_ids: List[str],
    du_delivery_monthly: List[Dict],
    du_shortage_monthly: List[Dict],
    du_period_summary: List[Dict],
    aggregate_monthly: List[Dict],
    aggregate_period_summary: List[Dict]
):
    """Save all statistics to database."""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        log.error("DATABASE_URL not set. Cannot save to database.")
        return False

    if not HAS_PSYCOPG2:
        log.error("psycopg2 not installed. Cannot save to database.")
        return False

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        # Delete existing data for these scenarios
        for scenario_id in scenario_ids:
            cur.execute("DELETE FROM ag_du_delivery_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_du_shortage_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_du_period_summary WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_aggregate_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_aggregate_period_summary WHERE scenario_short_code = %s", (scenario_id,))
            log.info(f"Cleared existing data for scenario {scenario_id}")

        # Insert DU delivery monthly
        if du_delivery_monthly:
            cols = [
                'scenario_short_code', 'du_id', 'water_month',
                'delivery_avg_taf', 'delivery_cv',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
                'sample_count'
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_delivery_monthly
            ]
            insert_sql = f"INSERT INTO ag_du_delivery_monthly ({', '.join(cols)}) VALUES %s"
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU delivery monthly rows")

        # Insert DU shortage monthly
        if du_shortage_monthly:
            cols = [
                'scenario_short_code', 'du_id', 'water_month',
                'shortage_avg_taf', 'shortage_cv', 'shortage_frequency_pct', 'shortage_pct_of_demand_avg',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
                'sample_count'
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_shortage_monthly
            ]
            insert_sql = f"INSERT INTO ag_du_shortage_monthly ({', '.join(cols)}) VALUES %s"
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU shortage monthly rows")

        # Insert DU period summary
        if du_period_summary:
            cols = [
                'scenario_short_code', 'du_id',
                'simulation_start_year', 'simulation_end_year', 'total_years',
                'annual_delivery_avg_taf', 'annual_delivery_cv',
                'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25',
                'delivery_exc_p50', 'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
                'annual_shortage_avg_taf', 'shortage_years_count', 'shortage_frequency_pct',
                'annual_shortage_pct_of_demand', 'reliability_pct', 'avg_pct_demand_met', 'annual_demand_avg_taf'
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_period_summary
            ]
            insert_sql = f"INSERT INTO ag_du_period_summary ({', '.join(cols)}) VALUES %s"
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU period summary rows")

        # Insert aggregate monthly
        if aggregate_monthly:
            cols = [
                'scenario_short_code', 'aggregate_code', 'water_month',
                'delivery_avg_taf', 'delivery_cv',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
                'shortage_avg_taf', 'shortage_cv', 'shortage_frequency_pct',
                'sample_count'
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in aggregate_monthly
            ]
            insert_sql = f"INSERT INTO ag_aggregate_monthly ({', '.join(cols)}) VALUES %s"
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} aggregate monthly rows")

        # Insert aggregate period summary
        if aggregate_period_summary:
            cols = [
                'scenario_short_code', 'aggregate_code',
                'simulation_start_year', 'simulation_end_year', 'total_years',
                'annual_delivery_avg_taf', 'annual_delivery_cv',
                'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25',
                'delivery_exc_p50', 'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
                'annual_shortage_avg_taf', 'shortage_years_count', 'shortage_frequency_pct',
                'shortage_exc_p5', 'shortage_exc_p10', 'shortage_exc_p25',
                'shortage_exc_p50', 'shortage_exc_p75', 'shortage_exc_p90', 'shortage_exc_p95',
                'reliability_pct'
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in aggregate_period_summary
            ]
            insert_sql = f"INSERT INTO ag_aggregate_period_summary ({', '.join(cols)}) VALUES %s"
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} aggregate period summary rows")

        conn.commit()
        cur.close()
        conn.close()
        log.info("Database save complete")
        return True

    except Exception as e:
        log.error(f"Database error: {e}")
        raise


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate delivery and shortage statistics for agricultural demand units'
    )
    parser.add_argument(
        '--scenario', '-s',
        help='Scenario ID (e.g., s0020)'
    )
    parser.add_argument(
        '--all-scenarios',
        action='store_true',
        help='Process all known scenarios'
    )
    parser.add_argument(
        '--csv-path',
        help='Local CalSim output CSV file path (instead of S3)'
    )
    parser.add_argument(
        '--output-json',
        action='store_true',
        help='Output results as JSON'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Calculate but do not save output'
    )

    args = parser.parse_args()

    if not args.scenario and not args.all_scenarios:
        parser.error("Either --scenario or --all-scenarios is required")

    scenarios_to_process = SCENARIOS if args.all_scenarios else [args.scenario]

    all_du_delivery = []
    all_du_shortage = []
    all_du_summary = []
    all_agg_monthly = []
    all_agg_summary = []

    for scenario_id in scenarios_to_process:
        try:
            results = calculate_all_ag_statistics(
                scenario_id,
                csv_path=args.csv_path
            )
            du_delivery, du_shortage, du_summary, agg_monthly, agg_summary = results

            all_du_delivery.extend(du_delivery)
            all_du_shortage.extend(du_shortage)
            all_du_summary.extend(du_summary)
            all_agg_monthly.extend(agg_monthly)
            all_agg_summary.extend(agg_summary)

        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise

    if args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")
        log.info(f"Total: {len(all_du_delivery)} DU delivery monthly, "
                 f"{len(all_du_shortage)} DU shortage monthly, "
                 f"{len(all_du_summary)} DU period summary, "
                 f"{len(all_agg_monthly)} aggregate monthly, "
                 f"{len(all_agg_summary)} aggregate period summary rows")
        return

    if args.output_json:
        output = {
            'du_delivery_monthly': all_du_delivery,
            'du_shortage_monthly': all_du_shortage,
            'du_period_summary': all_du_summary,
            'aggregate_monthly': all_agg_monthly,
            'aggregate_period_summary': all_agg_summary,
        }
        print(json.dumps(output, indent=2))
        return

    # Save to database
    scenario_ids = list(set(row['scenario_short_code'] for row in all_du_delivery))
    save_to_database(
        scenario_ids,
        all_du_delivery,
        all_du_shortage,
        all_du_summary,
        all_agg_monthly,
        all_agg_summary
    )

    log.info("Total rows saved:")
    log.info(f"  DU delivery monthly: {len(all_du_delivery)}")
    log.info(f"  DU shortage monthly: {len(all_du_shortage)}")
    log.info(f"  DU period summary: {len(all_du_summary)}")
    log.info(f"  Aggregate monthly: {len(all_agg_monthly)}")
    log.info(f"  Aggregate period summary: {len(all_agg_summary)}")


if __name__ == '__main__':
    main()
