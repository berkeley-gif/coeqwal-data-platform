#!/usr/bin/env python3
"""
Calculate demand, delivery, and shortage statistics for agricultural demand units.

IMPORTANT - CalSim Variable Semantics (from COEQWAL modeler notebooks):
- AW_{DU_ID} = Applied Water = DEMAND
- DN_{DU_ID} = Net Delivery = SURFACE WATER DELIVERY
- GP_{DU_ID} = Groundwater Pumping (explicit for some DUs)
- Groundwater Pumping = AW - DN (calculated for most DUs)
- GW_SHORT_{DU_ID} = Groundwater RESTRICTION Shortage (COEQWAL-specific)

In CalSim, agricultural demand is assumed to be fully met:
  Demand (AW) = Surface Water Delivery (DN) + Groundwater Pumping (GP)

DATA SOURCES (Multi-Source Loading):
This ETL loads data from THREE separate sources to ensure correct units:

1. Main CalSim Output (scenario/{id}/csv/{id}_coeqwal_calsim_output.csv):
   - GP_* (Groundwater Pumping) - in CFS, converted to TAF
   - GW_SHORT_* (GW Restriction Shortage) - in CFS, converted to TAF
   - DEL_*, SHORT_* (Aggregate variables) - in CFS, converted to TAF

2. Demands CSV (reference/{id}_demand.csv):
   - AW_* (Applied Water/Demand) - columns 283+ are in TAF, used directly
   - Source is authoritative for demand values

3. Deliveries CSV (reference/{id}_deliveries.csv):
   - DN_* (Net Delivery) - columns 279+ are in TAF, used directly
   - Contains DN_* variables MISSING from Main Output:
     DN_06_NA, DN_07N_NA, DN_07S_NA, DN_15N_NA1, DN_15S_NA1, DN_16_NA1,
     DN_17N_NA, DN_20_NA2, DN_26S_NA, DN_60S_NA1, DN_60S_NA2

Note: Sacramento region DUs (WBAs 02-26) do NOT have GW_SHORT shortage data.

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

# S3 paths for separate data files (Demands/Deliveries have TAF columns)
# These files are in the reference/ directory alongside the main output
DEMANDS_S3_KEY = "reference/{scenario}_demand.csv"
DELIVERIES_S3_KEY = "reference/{scenario}_deliveries.csv"

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

# Unit conversion: CFS (cubic feet per second) to TAF (thousand acre-feet)
# TAF = CFS * seconds_per_day * days / (43560 sq ft per acre) / 1000
# Simplified: CFS * days * 86400 / 43560 / 1000 = CFS * days * 0.001983471
#
# UNIT HANDLING (Multi-Source):
# - AW_* from Demands CSV (TAF columns 283+): already in TAF, NO conversion
# - DN_* from Deliveries CSV (TAF columns 279+): already in TAF, NO conversion
# - GP_*, GW_SHORT_*, DEL_*, SHORT_* from Main Output: in CFS, NEEDS conversion
#
# The code tracks which columns are in TAF via the `columns_in_taf` set.
CFS_TO_TAF_PER_DAY = 0.001983471

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


def load_demands_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """
    Load AW_* columns from Demands CSV (using TAF columns starting at col 283).
    
    The Demands CSV has the same 7-row header as the main output.
    Columns 0-282 are in CFS, columns 283+ are the same variables in TAF.
    We extract only the TAF columns and rename them to match the CFS column names.
    
    Returns DataFrame with AW_* columns already in TAF units.
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access. Install with: pip install boto3")

    s3 = boto3.client('s3')
    key = DEMANDS_S3_KEY.format(scenario=scenario_id)
    
    try:
        log.info(f"Loading Demands CSV: s3://{S3_BUCKET}/{key}")
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        
        # Read header rows to get column names
        header_df = pd.read_csv(response['Body'], header=None, nrows=8)
        col_names = header_df.iloc[1].tolist()
        
        # Re-fetch to read data rows
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data_df = pd.read_csv(response['Body'], header=None, skiprows=7, low_memory=False)
        data_df.columns = col_names
        
        # Find the TAF column start - look for columns with '_TAF' suffix or at position 283+
        # The TAF columns mirror the CFS columns but with _TAF suffix or just duplicated names
        total_cols = len(col_names)
        
        # If the file has duplicate column names (CFS then TAF), use the second half
        # Otherwise, check for _TAF suffix pattern
        date_col = col_names[0]
        
        # For Demands CSV, TAF columns start around col 283
        # We'll take the date column and the TAF portion
        taf_start_idx = 283
        if total_cols > taf_start_idx:
            # Get date column (first column) and TAF columns (283+)
            result_df = pd.DataFrame()
            result_df[date_col] = data_df.iloc[:, 0]
            
            # Get TAF columns - they have same names as CFS columns
            taf_col_names = col_names[taf_start_idx:]
            for i, taf_name in enumerate(taf_col_names):
                if taf_name.startswith('AW_'):
                    result_df[taf_name] = data_df.iloc[:, taf_start_idx + i]
            
            log.info(f"Loaded {len(result_df.columns) - 1} AW_* columns in TAF from Demands CSV")
            return result_df
        else:
            log.warning(f"Demands CSV has only {total_cols} columns, expected TAF columns at 283+")
            return pd.DataFrame()
            
    except s3.exceptions.NoSuchKey:
        log.warning(f"Demands CSV not found at s3://{S3_BUCKET}/{key}")
        return pd.DataFrame()
    except Exception as e:
        log.warning(f"Error loading Demands CSV: {e}")
        return pd.DataFrame()


def load_deliveries_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """
    Load DN_* columns from Deliveries CSV (using TAF columns starting at col 279).
    
    The Deliveries CSV has the same 7-row header as the main output.
    Columns 0-278 are in CFS, columns 279+ are the same variables in TAF.
    We extract only the TAF columns and rename them to match the CFS column names.
    
    This file contains DN_* variables that may be MISSING from the main CalSim output,
    including: DN_06_NA, DN_07N_NA, DN_07S_NA, DN_15N_NA1, DN_15S_NA1, DN_16_NA1,
    DN_17N_NA, DN_20_NA2, DN_26S_NA, DN_60S_NA1, DN_60S_NA2
    
    Returns DataFrame with DN_* columns already in TAF units.
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access. Install with: pip install boto3")

    s3 = boto3.client('s3')
    key = DELIVERIES_S3_KEY.format(scenario=scenario_id)
    
    try:
        log.info(f"Loading Deliveries CSV: s3://{S3_BUCKET}/{key}")
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        
        # Read header rows to get column names
        header_df = pd.read_csv(response['Body'], header=None, nrows=8)
        col_names = header_df.iloc[1].tolist()
        
        # Re-fetch to read data rows
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data_df = pd.read_csv(response['Body'], header=None, skiprows=7, low_memory=False)
        data_df.columns = col_names
        
        total_cols = len(col_names)
        date_col = col_names[0]
        
        # For Deliveries CSV, TAF columns start around col 279
        taf_start_idx = 279
        if total_cols > taf_start_idx:
            # Get date column (first column) and TAF columns (279+)
            result_df = pd.DataFrame()
            result_df[date_col] = data_df.iloc[:, 0]
            
            # Get TAF columns - they have same names as CFS columns
            taf_col_names = col_names[taf_start_idx:]
            for i, taf_name in enumerate(taf_col_names):
                if taf_name.startswith('DN_'):
                    result_df[taf_name] = data_df.iloc[:, taf_start_idx + i]
            
            log.info(f"Loaded {len(result_df.columns) - 1} DN_* columns in TAF from Deliveries CSV")
            return result_df
        else:
            log.warning(f"Deliveries CSV has only {total_cols} columns, expected TAF columns at 279+")
            return pd.DataFrame()
            
    except s3.exceptions.NoSuchKey:
        log.warning(f"Deliveries CSV not found at s3://{S3_BUCKET}/{key}")
        return pd.DataFrame()
    except Exception as e:
        log.warning(f"Error loading Deliveries CSV: {e}")
        return pd.DataFrame()


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
    Add water year, water month, and days in month columns.

    Water month: Oct(10)->1, Nov(11)->2, ..., Sep(9)->12
    Water year: Oct-Dec belong to next water year
    DaysInMonth: Number of days in each month (for CFS to TAF conversion)
    """
    df = df.copy()

    first_col = df.columns[0]
    date_values = df[first_col]

    try:
        df['DateTime'] = pd.to_datetime(date_values, errors='coerce')

        if df['DateTime'].notna().sum() > 0:
            df['CalendarMonth'] = df['DateTime'].dt.month
            df['CalendarYear'] = df['DateTime'].dt.year
            df['DaysInMonth'] = df['DateTime'].dt.daysinmonth

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
        df['DaysInMonth'] = 365  # Annual data - use full year
        log.info(f"Detected annual data: years {df['WaterYear'].min()}-{df['WaterYear'].max()}")
        return df

    raise ValueError(f"Could not parse date column '{first_col}' as datetime or year values")


# =============================================================================
# DEMAND UNIT STATISTICS
# =============================================================================

def calculate_du_demand_monthly(
    df: pd.DataFrame,
    du_id: str,
    columns_in_taf: Optional[set] = None
) -> List[Dict[str, Any]]:
    """
    Calculate monthly DEMAND statistics for an agricultural demand unit.

    Uses AW_{DU_ID} (Applied Water) variable.
    This is the DEMAND (water requirement), not delivery.

    When loaded from Demands CSV, AW_* is already in TAF (no conversion needed).
    When loaded from Main CalSim Output, AW_* is in CFS (needs conversion).
    The columns_in_taf set indicates which columns are already in TAF.
    """
    if columns_in_taf is None:
        columns_in_taf = set()

    demand_var = f"AW_{du_id}"

    if demand_var not in df.columns:
        log.debug(f"No demand variable found for {du_id}: {demand_var}")
        return []

    df_copy = df.copy()
    
    # Check if AW_* is already in TAF (from Demands CSV) or needs conversion (from Main Output)
    if demand_var in columns_in_taf:
        # AW_* from Demands CSV is already in TAF - no conversion needed
        df_copy['demand'] = df_copy[demand_var]
    else:
        # AW_* from Main Output is in CFS - convert to TAF
        df_copy['demand'] = df_copy[demand_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        data = df_copy['demand'].dropna()
        if data.empty:
            return []

        row = {
            'du_id': du_id,
            'water_month': 0,
            'demand_avg_taf': round(float(data.mean()), 2),
            'demand_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'sample_count': len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(data, 100 - p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy['WaterMonth'] == wm]['demand'].dropna()
            if month_data.empty:
                continue

            row = {
                'du_id': du_id,
                'water_month': wm,
                'demand_avg_taf': round(float(month_data.mean()), 2),
                'demand_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'sample_count': len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(month_data, 100 - p)), 2)

            results.append(row)

    return results


def calculate_du_sw_delivery_monthly(
    df: pd.DataFrame,
    du_id: str,
    columns_in_taf: Optional[set] = None
) -> List[Dict[str, Any]]:
    """
    Calculate monthly SURFACE WATER DELIVERY statistics for an agricultural demand unit.

    Uses DN_{DU_ID} (Net Delivery) variable.
    For groundwater-only DUs (no DN_* variable), returns empty list.

    When loaded from Deliveries CSV, DN_* is already in TAF (no conversion needed).
    When loaded from Main CalSim Output, DN_* is in CFS (needs conversion).
    The columns_in_taf set indicates which columns are already in TAF.
    """
    if columns_in_taf is None:
        columns_in_taf = set()

    sw_delivery_var = f"DN_{du_id}"

    if sw_delivery_var not in df.columns:
        log.debug(f"No SW delivery variable found for {du_id}: {sw_delivery_var} (may be GW-only DU)")
        return []

    df_copy = df.copy()
    
    # Check if DN_* is already in TAF (from Deliveries CSV) or needs conversion (from Main Output)
    if sw_delivery_var in columns_in_taf:
        # DN_* from Deliveries CSV is already in TAF - no conversion needed
        df_copy['sw_delivery'] = df_copy[sw_delivery_var]
    else:
        # DN_* from Main Output is in CFS - convert to TAF
        df_copy['sw_delivery'] = df_copy[sw_delivery_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        data = df_copy['sw_delivery'].dropna()
        if data.empty:
            return []

        row = {
            'du_id': du_id,
            'water_month': 0,
            'sw_delivery_avg_taf': round(float(data.mean()), 2),
            'sw_delivery_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'sample_count': len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(data, 100 - p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy['WaterMonth'] == wm]['sw_delivery'].dropna()
            if month_data.empty:
                continue

            row = {
                'du_id': du_id,
                'water_month': wm,
                'sw_delivery_avg_taf': round(float(month_data.mean()), 2),
                'sw_delivery_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'sample_count': len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(month_data, 100 - p)), 2)

            results.append(row)

    return results


def calculate_du_gw_pumping_monthly(
    df: pd.DataFrame,
    du_id: str,
    columns_in_taf: Optional[set] = None
) -> List[Dict[str, Any]]:
    """
    Calculate monthly GROUNDWATER PUMPING statistics for an agricultural demand unit.

    Uses GP_{DU_ID} if available (explicit GW pumping variable, always in CFS from Main Output).
    Otherwise calculates as AW_{DU_ID} - DN_{DU_ID} (Demand - SW Delivery).
    
    For groundwater-only DUs (no DN_*), GW pumping equals demand (AW_*).

    Unit handling:
    - GP_* is always from Main Output (CFS, needs conversion)
    - AW_* may be TAF (from Demands CSV) or CFS (from Main Output)
    - DN_* may be TAF (from Deliveries CSV) or CFS (from Main Output)
    """
    if columns_in_taf is None:
        columns_in_taf = set()

    demand_var = f"AW_{du_id}"
    sw_delivery_var = f"DN_{du_id}"
    gw_pumping_var = f"GP_{du_id}"

    # Determine source of GW pumping data
    has_explicit_gp = gw_pumping_var in df.columns
    has_demand = demand_var in df.columns
    has_sw_delivery = sw_delivery_var in df.columns

    if not has_demand and not has_explicit_gp:
        log.debug(f"No data to calculate GW pumping for {du_id}")
        return []

    df_copy = df.copy()

    if has_explicit_gp:
        # Use explicit GP_* variable - always from Main Output (CFS), convert to TAF
        df_copy['gw_pumping'] = df_copy[gw_pumping_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY
        is_calculated = False
        log.debug(f"Using explicit GP variable for {du_id}")
    elif has_demand:
        # Calculate as AW - DN
        # AW may be TAF (from Demands CSV) or CFS (from Main Output)
        if demand_var in columns_in_taf:
            df_copy['demand'] = df_copy[demand_var]  # Already TAF
        else:
            df_copy['demand'] = df_copy[demand_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY
        
        if has_sw_delivery:
            # DN may be TAF (from Deliveries CSV) or CFS (from Main Output)
            if sw_delivery_var in columns_in_taf:
                df_copy['sw_delivery'] = df_copy[sw_delivery_var]  # Already TAF
            else:
                df_copy['sw_delivery'] = df_copy[sw_delivery_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY
            df_copy['gw_pumping'] = df_copy['demand'] - df_copy['sw_delivery']
        else:
            # Groundwater-only DU: GW = Demand
            df_copy['gw_pumping'] = df_copy['demand']
        is_calculated = True
    else:
        return []

    # Ensure non-negative (handle floating-point artifacts)
    df_copy['gw_pumping'] = df_copy['gw_pumping'].clip(lower=0)

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        data = df_copy['gw_pumping'].dropna()
        if data.empty:
            return []

        row = {
            'du_id': du_id,
            'water_month': 0,
            'gw_pumping_avg_taf': round(float(data.mean()), 2),
            'gw_pumping_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'is_calculated': is_calculated,
            'sample_count': len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(data, 100 - p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy['WaterMonth'] == wm]['gw_pumping'].dropna()
            if month_data.empty:
                continue

            row = {
                'du_id': du_id,
                'water_month': wm,
                'gw_pumping_avg_taf': round(float(month_data.mean()), 2),
                'gw_pumping_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'is_calculated': is_calculated,
                'sample_count': len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(month_data, 100 - p)), 2)

            results.append(row)

    return results


def calculate_du_shortage_monthly(
    df: pd.DataFrame,
    du_id: str,
    du_info: Dict[str, Any],
    columns_in_taf: Optional[set] = None
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

    Unit handling:
    - GW_SHORT_* is always from Main Output (CFS, needs conversion)
    - AW_* may be TAF (from Demands CSV) or CFS (from Main Output)

    Also calculates shortage_pct_of_demand = shortage / (delivery + shortage) * 100
    """
    if columns_in_taf is None:
        columns_in_taf = set()

    # Check if this DU should have shortage data
    wba_id = du_info.get('wba_id', '')
    if wba_id in SACRAMENTO_WBAS:
        log.debug(f"Sacramento region DU {du_id} - no shortage data expected")
        return []

    shortage_var = f"GW_SHORT_{du_id}"
    demand_var = f"AW_{du_id}"

    if shortage_var not in df.columns:
        log.debug(f"No shortage variable found for {du_id}: {shortage_var}")
        return []

    df_copy = df.copy()
    # GW_SHORT_* is always from Main Output (CFS) - convert to TAF
    df_copy['shortage'] = df_copy[shortage_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY

    # Get demand for calculating shortage % of demand
    if demand_var in df.columns:
        # AW_* may be TAF (from Demands CSV) or CFS (from Main Output)
        if demand_var in columns_in_taf:
            df_copy['demand'] = df_copy[demand_var]  # Already TAF
        else:
            df_copy['demand'] = df_copy[demand_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY
    else:
        df_copy['demand'] = 0

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        shortage_data = df_copy['shortage'].dropna()
        demand_data = df_copy['demand'].dropna()

        if shortage_data.empty:
            return []

        # Use threshold to filter out floating-point noise from CalSim solver
        shortage_count = (shortage_data > SHORTAGE_THRESHOLD_TAF).sum()

        # Calculate shortage % of demand
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

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(shortage_data, 100 - p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            mask = df_copy['WaterMonth'] == wm
            shortage_data = df_copy.loc[mask, 'shortage'].dropna()
            demand_month = df_copy.loc[mask, 'demand'].dropna()

            if shortage_data.empty:
                continue

            # Use threshold to filter out floating-point noise from CalSim solver
            shortage_count = (shortage_data > SHORTAGE_THRESHOLD_TAF).sum()

            # Calculate shortage % of demand for this month
            shortage_pct = []
            for s, d in zip(shortage_data.values, demand_month.values):
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

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(shortage_data, 100 - p)), 2)

            results.append(row)

    return results


def calculate_du_period_summary(
    df: pd.DataFrame,
    du_id: str,
    du_info: Dict[str, Any],
    columns_in_taf: Optional[set] = None
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary for an agricultural demand unit.
    
    Correctly distinguishes:
    - AW_* = Demand (applied water requirement)
    - DN_* = Surface Water Delivery
    - GP_* or (AW - DN) = Groundwater Pumping
    - GW_SHORT_* = Groundwater Restriction Shortage

    Unit handling:
    - AW_* may be TAF (from Demands CSV) or CFS (from Main Output)
    - DN_* may be TAF (from Deliveries CSV) or CFS (from Main Output)
    - GP_* and GW_SHORT_* are always from Main Output (CFS, need conversion)
    """
    if columns_in_taf is None:
        columns_in_taf = set()

    demand_var = f"AW_{du_id}"
    sw_delivery_var = f"DN_{du_id}"
    gw_pumping_var = f"GP_{du_id}"
    shortage_var = f"GW_SHORT_{du_id}"

    if demand_var not in df.columns:
        return None

    df_copy = df.copy()
    
    # AW_* may be TAF (from Demands CSV) or CFS (from Main Output)
    if demand_var in columns_in_taf:
        df_copy['demand'] = df_copy[demand_var]  # Already TAF
    else:
        df_copy['demand'] = df_copy[demand_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY

    # Surface water delivery - DN_* may be TAF (from Deliveries CSV) or CFS (from Main Output)
    has_sw_delivery = sw_delivery_var in df.columns
    if has_sw_delivery:
        if sw_delivery_var in columns_in_taf:
            df_copy['sw_delivery'] = df_copy[sw_delivery_var]  # Already TAF
        else:
            df_copy['sw_delivery'] = df_copy[sw_delivery_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY
    else:
        df_copy['sw_delivery'] = 0

    # Groundwater pumping: use GP_* if available, otherwise calculate as AW - DN
    # GP_* is always from Main Output (CFS, needs conversion)
    has_explicit_gp = gw_pumping_var in df.columns
    if has_explicit_gp:
        df_copy['gw_pumping'] = df_copy[gw_pumping_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY
    else:
        # Both demand and sw_delivery are now in TAF
        df_copy['gw_pumping'] = (df_copy['demand'] - df_copy['sw_delivery']).clip(lower=0)

    water_years = sorted(df_copy['WaterYear'].unique())

    result = {
        'du_id': du_id,
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),
    }

    # Annual DEMAND statistics (from AW_*)
    annual_demand = df_copy.groupby('WaterYear')['demand'].sum()
    result['annual_demand_avg_taf'] = round(float(annual_demand.mean()), 2)
    if annual_demand.mean() > 0:
        result['annual_demand_cv'] = round(float(annual_demand.std() / annual_demand.mean()), 4)
    else:
        result['annual_demand_cv'] = 0

    # Exceedance percentiles for DEMAND: exc_pX = value exceeded X% of time = (100-X)th percentile
    for p in EXCEEDANCE_PERCENTILES:
        result[f'demand_exc_p{p}'] = round(float(np.percentile(annual_demand, 100 - p)), 2)

    # Annual SW DELIVERY statistics (from DN_*)
    annual_sw_delivery = df_copy.groupby('WaterYear')['sw_delivery'].sum()
    result['annual_sw_delivery_avg_taf'] = round(float(annual_sw_delivery.mean()), 2)
    if annual_sw_delivery.mean() > 0:
        result['annual_sw_delivery_cv'] = round(float(annual_sw_delivery.std() / annual_sw_delivery.mean()), 4)
    else:
        result['annual_sw_delivery_cv'] = 0

    # Annual GW PUMPING statistics (from GP_* or calculated)
    annual_gw_pumping = df_copy.groupby('WaterYear')['gw_pumping'].sum()
    result['annual_gw_pumping_avg_taf'] = round(float(annual_gw_pumping.mean()), 2)
    if annual_gw_pumping.mean() > 0:
        result['annual_gw_pumping_cv'] = round(float(annual_gw_pumping.std() / annual_gw_pumping.mean()), 4)
    else:
        result['annual_gw_pumping_cv'] = 0

    # GW pumping as percentage of demand
    if result['annual_demand_avg_taf'] > 0:
        result['gw_pumping_pct_of_demand'] = round(
            (result['annual_gw_pumping_avg_taf'] / result['annual_demand_avg_taf']) * 100, 2
        )
    else:
        result['gw_pumping_pct_of_demand'] = 0

    # Shortage statistics (only for non-Sacramento regions with GW_SHORT data)
    wba_id = du_info.get('wba_id', '')
    has_shortage = shortage_var in df.columns and wba_id not in SACRAMENTO_WBAS

    if has_shortage:
        # GW_SHORT_* from DV output is in CFS - convert to TAF
        df_copy['shortage'] = df_copy[shortage_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY
        annual_shortage = df_copy.groupby('WaterYear')['shortage'].sum()
        # Use threshold to filter out floating-point noise from CalSim solver
        shortage_years = (annual_shortage > SHORTAGE_THRESHOLD_TAF).sum()

        result['annual_shortage_avg_taf'] = round(float(annual_shortage.mean()), 2)
        result['shortage_years_count'] = int(shortage_years)
        result['shortage_frequency_pct'] = round((shortage_years / len(water_years)) * 100, 2)

        # Calculate annual shortage % of demand
        shortage_pct = []
        for s, d in zip(annual_shortage.values, annual_demand.values):
            if d > 0:
                shortage_pct.append((s / d) * 100)
            else:
                shortage_pct.append(0)
        result['annual_shortage_pct_of_demand'] = round(float(np.mean(shortage_pct)), 2)

        # Reliability = % of demand met (considering GW restriction shortage)
        # In CalSim, AG demand is always met (via SW + GW), but with GW restrictions
        # there can be shortage. Reliability = (demand - shortage) / demand
        if result['annual_demand_avg_taf'] > 0:
            met = result['annual_demand_avg_taf'] - result['annual_shortage_avg_taf']
            result['reliability_pct'] = round((met / result['annual_demand_avg_taf']) * 100, 2)
            result['avg_pct_demand_met'] = result['reliability_pct']
        else:
            result['reliability_pct'] = 100.0
            result['avg_pct_demand_met'] = 100.0
    else:
        result['annual_shortage_avg_taf'] = None
        result['shortage_years_count'] = None
        result['shortage_frequency_pct'] = None
        result['annual_shortage_pct_of_demand'] = None
        # Without shortage data, assume 100% reliability (CalSim assumption)
        result['reliability_pct'] = 100.0
        result['avg_pct_demand_met'] = 100.0

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
    
    NOTE: Aggregate delivery/shortage variables from DV output are in CFS.
    Must be converted to TAF: TAF = CFS × days_in_month × 0.001984
    """
    if delivery_var not in df.columns:
        log.debug(f"No aggregate delivery variable found: {delivery_var}")
        return []

    df_copy = df.copy()
    # Aggregate delivery variables from DV output are in CFS - convert to TAF
    df_copy['delivery'] = df_copy[delivery_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY

    has_shortage = shortage_var and shortage_var in df.columns
    if has_shortage:
        # Aggregate shortage variables from DV output are in CFS - convert to TAF
        df_copy['shortage'] = df_copy[shortage_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY

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

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(data, 100 - p)), 2)

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

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(month_data, 100 - p)), 2)

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
    
    NOTE: Aggregate delivery/shortage variables from DV output are in CFS.
    Must be converted to TAF: TAF = CFS × days_in_month × 0.001984
    """
    if delivery_var not in df.columns:
        return None

    df_copy = df.copy()
    # Aggregate delivery variables from DV output are in CFS - convert to TAF
    df_copy['delivery'] = df_copy[delivery_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY

    has_shortage = shortage_var and shortage_var in df.columns
    if has_shortage:
        # Aggregate shortage variables from DV output are in CFS - convert to TAF
        df_copy['shortage'] = df_copy[shortage_var] * df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY

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

    # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, 100 - p)), 2)

    # Shortage statistics
    if has_shortage:
        annual_shortage = df_copy.groupby('WaterYear')['shortage'].sum()
        # Use threshold to filter floating-point noise from CalSim solver
        shortage_years = (annual_shortage > SHORTAGE_THRESHOLD_TAF).sum()

        result['annual_shortage_avg_taf'] = round(float(annual_shortage.mean()), 2)
        result['shortage_years_count'] = int(shortage_years)
        result['shortage_frequency_pct'] = round((shortage_years / len(water_years)) * 100, 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            result[f'shortage_exc_p{p}'] = round(float(np.percentile(annual_shortage, 100 - p)), 2)

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
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Calculate all statistics for agricultural demand units for a scenario.

    Data sources (multi-source loading for correct units):
    - Main CalSim Output: GP_*, GW_SHORT_*, DEL_*, SHORT_* (in CFS, need conversion)
    - Demands CSV: AW_* (already in TAF, no conversion needed)
    - Deliveries CSV: DN_* (already in TAF, no conversion needed)

    Returns:
        Tuple of (
            du_demand_monthly_rows,      # AW_* demand data (from Demands CSV, TAF)
            du_sw_delivery_monthly_rows, # DN_* surface water delivery (from Deliveries CSV, TAF)
            du_gw_pumping_monthly_rows,  # GP_* or calculated GW pumping data
            du_shortage_monthly_rows,    # GW_SHORT_* shortage data
            du_period_summary_rows,
            aggregate_monthly_rows,
            aggregate_period_summary_rows
        )
    """
    log.info(f"Processing scenario: {scenario_id}")

    # Load demand unit metadata
    demand_units = load_ag_demand_units()

    # Track which columns are already in TAF (don't need conversion)
    columns_in_taf: set = set()

    # Load CalSim main output (for GP_*, GW_SHORT_*, aggregates - all in CFS)
    if csv_path:
        df = load_calsim_csv_from_file(csv_path)
    else:
        df = load_calsim_csv_from_s3(scenario_id)

    # Add water year/month (includes DaysInMonth for CFS conversion)
    df = add_water_year_month(df)

    # Load Demands CSV for AW_* columns (already in TAF)
    # This overwrites any AW_* from main output with correct TAF values
    if not csv_path:  # Only from S3, not local files
        demands_df = load_demands_csv_from_s3(scenario_id)
        if not demands_df.empty:
            # Add water year/month to demands data
            demands_df = add_water_year_month(demands_df)
            
            # Merge AW_* columns from demands CSV into main df (overwriting CFS values)
            aw_cols_from_demands = [c for c in demands_df.columns if c.startswith('AW_')]
            for col in aw_cols_from_demands:
                df[col] = demands_df[col].values
                columns_in_taf.add(col)
            log.info(f"Merged {len(aw_cols_from_demands)} AW_* columns from Demands CSV (TAF)")

    # Load Deliveries CSV for DN_* columns (already in TAF)
    # This includes DN_* variables missing from main output
    if not csv_path:  # Only from S3, not local files
        deliveries_df = load_deliveries_csv_from_s3(scenario_id)
        if not deliveries_df.empty:
            # Add water year/month to deliveries data
            deliveries_df = add_water_year_month(deliveries_df)
            
            # Merge DN_* columns from deliveries CSV into main df
            dn_cols_from_deliveries = [c for c in deliveries_df.columns if c.startswith('DN_')]
            for col in dn_cols_from_deliveries:
                df[col] = deliveries_df[col].values
                columns_in_taf.add(col)
            log.info(f"Merged {len(dn_cols_from_deliveries)} DN_* columns from Deliveries CSV (TAF)")

    available_columns = list(df.columns)
    log.info(f"Available columns after merge: {len(available_columns)}")
    log.info(f"Columns already in TAF: {len(columns_in_taf)}")

    # Find all AW_* columns to get the list of DUs in this scenario (demand data)
    aw_columns = [c for c in available_columns if c.startswith('AW_') and not any(
        suffix in c for suffix in ['_ANN_DV', '_WLOSS', '_ADD_DV', '_ANNDV']
    )]
    du_ids_in_data = [c.replace('AW_', '') for c in aw_columns]
    log.info(f"Found {len(du_ids_in_data)} agricultural demand units with demand data")

    # Also find DN_* columns (surface water delivery)
    dn_columns = [c for c in available_columns if c.startswith('DN_') and not c.endswith('_ANN_DV')]
    log.info(f"Found {len(dn_columns)} DN_* columns for surface water delivery")

    # Find GP_* columns (explicit groundwater pumping)
    gp_columns = [c for c in available_columns if c.startswith('GP_') and not c.endswith('_NU')]
    log.info(f"Found {len(gp_columns)} GP_* columns for explicit GW pumping")

    du_demand_monthly_rows = []
    du_sw_delivery_monthly_rows = []
    du_gw_pumping_monthly_rows = []
    du_shortage_monthly_rows = []
    du_period_summary_rows = []

    demand_count = 0
    sw_delivery_count = 0
    gw_pumping_count = 0
    shortage_count = 0

    for du_id in du_ids_in_data:
        # Get DU info from entity table (if available)
        du_info = demand_units.get(du_id, {
            'wba_id': du_id.split('_')[0] if '_' in du_id else '',
            'hydrologic_region': '',
            'cs3_type': '',
        })

        # Calculate DEMAND monthly (from AW_* - TAF if from Demands CSV)
        demand_rows = calculate_du_demand_monthly(df, du_id, columns_in_taf)
        if demand_rows:
            demand_count += 1
            for row in demand_rows:
                row['scenario_short_code'] = scenario_id
            du_demand_monthly_rows.extend(demand_rows)

        # Calculate SW DELIVERY monthly (from DN_* - TAF if from Deliveries CSV)
        sw_delivery_rows = calculate_du_sw_delivery_monthly(df, du_id, columns_in_taf)
        if sw_delivery_rows:
            sw_delivery_count += 1
            for row in sw_delivery_rows:
                row['scenario_short_code'] = scenario_id
            du_sw_delivery_monthly_rows.extend(sw_delivery_rows)

        # Calculate GW PUMPING monthly (from GP_* or calculated as AW - DN)
        gw_pumping_rows = calculate_du_gw_pumping_monthly(df, du_id, columns_in_taf)
        if gw_pumping_rows:
            gw_pumping_count += 1
            for row in gw_pumping_rows:
                row['scenario_short_code'] = scenario_id
            du_gw_pumping_monthly_rows.extend(gw_pumping_rows)

        # Calculate SHORTAGE monthly (from GW_SHORT_*, only for non-Sacramento)
        shortage_rows = calculate_du_shortage_monthly(df, du_id, du_info, columns_in_taf)
        if shortage_rows:
            shortage_count += 1
            for row in shortage_rows:
                row['scenario_short_code'] = scenario_id
            du_shortage_monthly_rows.extend(shortage_rows)

        # Calculate period summary
        summary = calculate_du_period_summary(df, du_id, du_info, columns_in_taf)
        if summary:
            summary['scenario_short_code'] = scenario_id
            du_period_summary_rows.append(summary)

    log.info(f"Processed {demand_count} DUs with demand, {sw_delivery_count} with SW delivery, "
             f"{gw_pumping_count} with GW pumping, {shortage_count} with shortage data")

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

    log.info(f"Generated: {len(du_demand_monthly_rows)} DU demand monthly, "
             f"{len(du_sw_delivery_monthly_rows)} DU SW delivery monthly, "
             f"{len(du_gw_pumping_monthly_rows)} DU GW pumping monthly, "
             f"{len(du_shortage_monthly_rows)} DU shortage monthly, "
             f"{len(du_period_summary_rows)} DU period summary, "
             f"{len(aggregate_monthly_rows)} aggregate monthly, "
             f"{len(aggregate_period_summary_rows)} aggregate period summary rows")

    return (
        du_demand_monthly_rows,
        du_sw_delivery_monthly_rows,
        du_gw_pumping_monthly_rows,
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
    du_demand_monthly: List[Dict],
    du_sw_delivery_monthly: List[Dict],
    du_gw_pumping_monthly: List[Dict],
    du_shortage_monthly: List[Dict],
    du_period_summary: List[Dict],
    aggregate_monthly: List[Dict],
    aggregate_period_summary: List[Dict]
):
    """Save all statistics to database.
    
    Tables used (after migration 04_add_sw_delivery_gw_pumping_tables.sql):
    - ag_du_demand_monthly (renamed from ag_du_delivery_monthly)
    - ag_du_sw_delivery_monthly (NEW)
    - ag_du_gw_pumping_monthly (NEW)
    - ag_du_shortage_monthly
    - ag_du_period_summary
    - ag_aggregate_monthly
    - ag_aggregate_period_summary
    """
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
            cur.execute("DELETE FROM ag_du_demand_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_du_sw_delivery_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_du_gw_pumping_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_du_shortage_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_du_period_summary WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_aggregate_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM ag_aggregate_period_summary WHERE scenario_short_code = %s", (scenario_id,))
            log.info(f"Cleared existing data for scenario {scenario_id}")

        # Insert DU DEMAND monthly (from AW_* - renamed from delivery)
        if du_demand_monthly:
            cols = [
                'scenario_short_code', 'du_id', 'water_month',
                'demand_avg_taf', 'demand_cv',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
                'sample_count'
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_demand_monthly
            ]
            insert_sql = f"INSERT INTO ag_du_demand_monthly ({', '.join(cols)}) VALUES %s"
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU demand monthly rows")

        # Insert DU SW DELIVERY monthly (from DN_* - NEW)
        if du_sw_delivery_monthly:
            cols = [
                'scenario_short_code', 'du_id', 'water_month',
                'sw_delivery_avg_taf', 'sw_delivery_cv',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
                'sample_count'
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_sw_delivery_monthly
            ]
            insert_sql = f"INSERT INTO ag_du_sw_delivery_monthly ({', '.join(cols)}) VALUES %s"
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU SW delivery monthly rows")

        # Insert DU GW PUMPING monthly (from GP_* or calculated - NEW)
        if du_gw_pumping_monthly:
            cols = [
                'scenario_short_code', 'du_id', 'water_month',
                'gw_pumping_avg_taf', 'gw_pumping_cv',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
                'is_calculated', 'sample_count'
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_gw_pumping_monthly
            ]
            insert_sql = f"INSERT INTO ag_du_gw_pumping_monthly ({', '.join(cols)}) VALUES %s"
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU GW pumping monthly rows")

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

        # Insert DU period summary (with updated column names)
        if du_period_summary:
            cols = [
                'scenario_short_code', 'du_id',
                'simulation_start_year', 'simulation_end_year', 'total_years',
                'annual_demand_avg_taf', 'annual_demand_cv',
                'demand_exc_p5', 'demand_exc_p10', 'demand_exc_p25',
                'demand_exc_p50', 'demand_exc_p75', 'demand_exc_p90', 'demand_exc_p95',
                'annual_sw_delivery_avg_taf', 'annual_sw_delivery_cv',
                'annual_gw_pumping_avg_taf', 'annual_gw_pumping_cv', 'gw_pumping_pct_of_demand',
                'annual_shortage_avg_taf', 'shortage_years_count', 'shortage_frequency_pct',
                'annual_shortage_pct_of_demand', 'reliability_pct', 'avg_pct_demand_met'
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
        description='Calculate demand, delivery, and shortage statistics for agricultural demand units'
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

    all_du_demand = []
    all_du_sw_delivery = []
    all_du_gw_pumping = []
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
            (du_demand, du_sw_delivery, du_gw_pumping, du_shortage, 
             du_summary, agg_monthly, agg_summary) = results

            all_du_demand.extend(du_demand)
            all_du_sw_delivery.extend(du_sw_delivery)
            all_du_gw_pumping.extend(du_gw_pumping)
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
        log.info(f"Total: {len(all_du_demand)} DU demand monthly, "
                 f"{len(all_du_sw_delivery)} DU SW delivery monthly, "
                 f"{len(all_du_gw_pumping)} DU GW pumping monthly, "
                 f"{len(all_du_shortage)} DU shortage monthly, "
                 f"{len(all_du_summary)} DU period summary, "
                 f"{len(all_agg_monthly)} aggregate monthly, "
                 f"{len(all_agg_summary)} aggregate period summary rows")
        return

    if args.output_json:
        output = {
            'du_demand_monthly': all_du_demand,
            'du_sw_delivery_monthly': all_du_sw_delivery,
            'du_gw_pumping_monthly': all_du_gw_pumping,
            'du_shortage_monthly': all_du_shortage,
            'du_period_summary': all_du_summary,
            'aggregate_monthly': all_agg_monthly,
            'aggregate_period_summary': all_agg_summary,
        }
        print(json.dumps(output, indent=2))
        return

    # Save to database
    scenario_ids = list(set(row['scenario_short_code'] for row in all_du_demand))
    save_to_database(
        scenario_ids,
        all_du_demand,
        all_du_sw_delivery,
        all_du_gw_pumping,
        all_du_shortage,
        all_du_summary,
        all_agg_monthly,
        all_agg_summary
    )

    log.info("Total rows saved:")
    log.info(f"  DU demand monthly: {len(all_du_demand)}")
    log.info(f"  DU SW delivery monthly: {len(all_du_sw_delivery)}")
    log.info(f"  DU GW pumping monthly: {len(all_du_gw_pumping)}")
    log.info(f"  DU shortage monthly: {len(all_du_shortage)}")
    log.info(f"  DU period summary: {len(all_du_summary)}")
    log.info(f"  Aggregate monthly: {len(all_agg_monthly)}")
    log.info(f"  Aggregate period summary: {len(all_agg_summary)}")


if __name__ == '__main__':
    main()
