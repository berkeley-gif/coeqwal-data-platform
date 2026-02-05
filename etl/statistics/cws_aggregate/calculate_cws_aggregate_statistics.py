#!/usr/bin/env python3
"""
Calculate delivery and shortage statistics for CWS system-level aggregates.

This module processes CalSim output to calculate statistics for:
- SWP Total M&I (DEL_SWP_PMI, SHORT_SWP_PMI)
- SWP North of Delta (DEL_SWP_PMI_N, SHORT_SWP_PMI_N)
- SWP South of Delta (DEL_SWP_PMI_S, SHORT_SWP_PMI_S)
- CVP North (DEL_CVP_PMI_N, SHORT_CVP_PMI_N)
- CVP South (DEL_CVP_PMI_S, SHORT_CVP_PMI_S)
- MWD (DEL_SWP_MWD, SHORT_MWD_PMI)

Usage:
    python calculate_cws_aggregate_statistics.py --scenario s0020
    python calculate_cws_aggregate_statistics.py --scenario s0020 --csv-path /path/to/calsim_output.csv
"""

import argparse
import json
import logging
import os
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
log = logging.getLogger("cws_aggregate_statistics")

# Known scenarios
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027', 's0029']

# S3 bucket configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

# Percentiles for statistics
DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# Minimum threshold for counting a year as having a "shortage" (in TAF)
# This filters out floating-point precision artifacts from CalSim's linear programming solver.
# 0.1 TAF = 100 acre-feet, which is < 0.05% of typical CVP North M&I delivery (~240 TAF/yr)
SHORTAGE_THRESHOLD_TAF = 0.1

# Unit conversion: CFS (cubic feet per second) to TAF (thousand acre-feet)
# TAF = CFS * seconds_per_day * days / (43560 sq ft per acre) / 1000
# Simplified: CFS * days * 86400 / 43560 / 1000 = CFS * days * 0.001983471
CFS_TO_TAF_PER_DAY = 0.001983471

# Paths to local data
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
LOCAL_PIPELINES_DIR = PROJECT_ROOT / "etl/pipelines"
LOCAL_DEMANDS_DIR = PROJECT_ROOT / "etl/demands"


# =============================================================================
# CWS AGGREGATE DEFINITIONS
# =============================================================================

CWS_AGGREGATES = {
    # SWP Total - demand based on Table A contracts
    'swp_total': {
        'id': 1,
        'label': 'SWP Total M&I',
        'delivery_var': 'DEL_SWP_PMI',
        'shortage_var': 'SHORT_SWP_PMI',
        'demand_var': 'DEM_SWP_PMI',  # May need to sum constituents if not available
        'description': 'Total State Water Project M&I deliveries',
    },
    # SWP North of Delta
    'swp_nod': {
        'id': 5,
        'label': 'SWP North',
        'delivery_var': 'DEL_SWP_PMI_N',
        'shortage_var': 'SHORT_SWP_PMI_N',
        'demand_var': 'DEM_SWP_PMI_N',  # May need to sum constituents
        'description': 'SWP M&I deliveries - North of Delta',
    },
    # SWP South of Delta
    'swp_sod': {
        'id': 6,
        'label': 'SWP South',
        'delivery_var': 'DEL_SWP_PMI_S',
        'shortage_var': 'SHORT_SWP_PMI_S',
        'demand_var': 'DEM_SWP_PMI_S',  # May need to sum constituents
        'description': 'SWP M&I deliveries - South of Delta',
    },
    # CVP North of Delta
    'cvp_nod': {
        'id': 2,
        'label': 'CVP North',
        'delivery_var': 'DEL_CVP_PMI_N',
        'shortage_var': 'SHORT_CVP_PMI_N',
        'demand_var': 'DEM_CVP_PMI_N',  # May need to sum constituents
        'description': 'CVP M&I deliveries - North of Delta',
    },
    # CVP South of Delta
    'cvp_sod': {
        'id': 3,
        'label': 'CVP South',
        'delivery_var': 'DEL_CVP_PMI_S',
        'shortage_var': 'SHORT_CVP_PMI_S',
        'demand_var': 'DEM_CVP_PMI_S',  # May need to sum constituents
        'description': 'CVP M&I deliveries - South of Delta',
    },
    # MWD - Metropolitan Water District
    'mwd': {
        'id': 4,
        'label': 'Metropolitan Water District',
        'delivery_var': 'DEL_SWP_MWD',
        'shortage_var': 'SHORT_SWP_MWD',
        'demand_var': 'TABLEA_CONTRACT_MWD',  # MWD uses Table A contract
        'description': 'MWD Southern California aggregate',
    },
}


def load_calsim_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """Load CalSim output CSV from S3 bucket."""
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access")

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

            # Get variable names from row 1
            col_names = df.iloc[1].tolist()

            # Re-fetch and read data portion
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data_df = pd.read_csv(response['Body'], header=None, skiprows=7)
            data_df.columns = col_names

            log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
            return data_df

        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            log.warning(f"Error loading {key}: {e}")
            continue

    raise FileNotFoundError(f"Could not find CalSim output for {scenario_id} in S3")


def load_calsim_csv_from_file(file_path: str, dedupe_columns: bool = False) -> pd.DataFrame:
    """
    Load CalSim output CSV from local file.

    Handles the 7-header-row DSS format.
    
    Args:
        file_path: Path to the CSV file
        dedupe_columns: If True, remove duplicate column names (keeping first occurrence).
                       Useful for DEMANDS CSV files which may have duplicates.
    """
    log.info(f"Loading from file: {file_path}")

    # Read header to get column names
    header_df = pd.read_csv(file_path, header=None, nrows=8)
    col_names = header_df.iloc[1].tolist()

    # Handle duplicate column names if requested
    unique_col_names = col_names
    duplicate_indices = []
    
    if dedupe_columns:
        seen = set()
        unique_col_names = []
        for i, name in enumerate(col_names):
            if name in seen:
                duplicate_indices.append(i)
            else:
                seen.add(name)
                unique_col_names.append(name)
        
        if duplicate_indices:
            log.info(f"Found {len(duplicate_indices)} duplicate columns, keeping first occurrence")

    # Read data portion
    data_df = pd.read_csv(file_path, header=None, skiprows=7)
    
    # Drop duplicate columns if needed
    if duplicate_indices:
        data_df = data_df.drop(columns=data_df.columns[duplicate_indices])
    
    data_df.columns = unique_col_names

    log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df


def load_demands_csv(
    scenario_id: str,
    use_local: bool = False,
    demand_csv_path: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Load DEMANDS CSV for a scenario.
    
    The DEMANDS CSV contains demand variables (DEM_*, TABLEA_CONTRACT_*, etc.)
    that are used to calculate percent of demand metrics.
    
    Args:
        scenario_id: Scenario ID (e.g., 's0020')
        use_local: Use local files instead of S3
        demand_csv_path: Override path for demand CSV
    
    Returns:
        DataFrame with demand data, or None if not found
    """
    if demand_csv_path:
        # Use provided path
        if not Path(demand_csv_path).exists():
            log.warning(f"Demand CSV not found at: {demand_csv_path}")
            return None
        # Demand CSV files often have duplicate columns - dedupe them
        return load_calsim_csv_from_file(demand_csv_path, dedupe_columns=True)
    
    if use_local:
        # Try local paths - check both pipelines and demands folders
        possible_paths = [
            # Full DEMANDS CSV with scenario suffix
            LOCAL_PIPELINES_DIR / f"{scenario_id}_DCRadjBL_2020LU_wTUCP_DEMANDS.csv",
            LOCAL_PIPELINES_DIR / f"{scenario_id}_adjBL_wTUCP_DEMANDS.csv",
            LOCAL_PIPELINES_DIR / f"{scenario_id}_DEMANDS.csv",
            # Simplified demand CSV
            LOCAL_DEMANDS_DIR / f"{scenario_id}_demand.csv",
        ]
        
        for path in possible_paths:
            if path.exists():
                log.info(f"Loading demands from: {path}")
                # Demand CSV files often have duplicate columns - dedupe them
                return load_calsim_csv_from_file(str(path), dedupe_columns=True)
        
        log.warning(f"No DEMANDS CSV found for scenario {scenario_id} locally")
        return None
    
    # S3 access
    if not HAS_BOTO3:
        log.warning("boto3 not available for S3 access")
        return None
    
    s3 = boto3.client('s3')
    
    # Try different possible S3 locations
    possible_keys = [
        f"reference/{scenario_id}_demand.csv",
        f"scenario/{scenario_id}/csv/{scenario_id}_DEMANDS.csv",
    ]
    
    for key in possible_keys:
        try:
            log.info(f"Trying S3 key: s3://{S3_BUCKET}/{key}")
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            
            # Read header
            import io
            content = response['Body'].read()
            header_df = pd.read_csv(io.BytesIO(content), header=None, nrows=8)
            col_names = header_df.iloc[1].tolist()
            
            # Handle duplicate column names by keeping only the first occurrence
            seen = set()
            unique_col_names = []
            duplicate_indices = []
            for i, name in enumerate(col_names):
                if name in seen:
                    duplicate_indices.append(i)
                else:
                    seen.add(name)
                    unique_col_names.append(name)
            
            if duplicate_indices:
                log.info(f"Found {len(duplicate_indices)} duplicate columns in demand CSV, keeping first occurrence")
            
            # Read data
            data_df = pd.read_csv(io.BytesIO(content), header=None, skiprows=7, low_memory=False)
            
            # Drop duplicate columns
            if duplicate_indices:
                data_df = data_df.drop(columns=data_df.columns[duplicate_indices])
            
            data_df.columns = unique_col_names
            
            log.info(f"Loaded demands from S3: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
            return data_df
            
        except Exception as e:
            log.debug(f"Could not load {key}: {e}")
            continue
    
    log.warning(f"No DEMANDS CSV found for scenario {scenario_id} in S3")
    return None


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """Add water year and water month columns."""
    df = df.copy()

    # Find date column (first column)
    first_col = df.columns[0]
    date_values = df[first_col]

    # Try to parse as datetime
    try:
        df['DateTime'] = pd.to_datetime(date_values, errors='coerce')
        df['CalendarMonth'] = df['DateTime'].dt.month
        df['CalendarYear'] = df['DateTime'].dt.year

        # Water month: Oct(10)->1, Nov(11)->2, ..., Sep(9)->12
        df['WaterMonth'] = ((df['CalendarMonth'] - 10) % 12) + 1

        # Water year: Oct-Dec belong to next water year
        df['WaterYear'] = df['CalendarYear']
        df.loc[df['CalendarMonth'] >= 10, 'WaterYear'] += 1

        log.info(f"Date range: {df['DateTime'].min()} to {df['DateTime'].max()}")
    except Exception as e:
        log.warning(f"Could not parse dates: {e}")
        # Fallback: assume annual data
        df['WaterYear'] = pd.to_numeric(date_values, errors='coerce').astype(int)
        df['WaterMonth'] = 0

    return df


def calculate_aggregate_monthly(
    df: pd.DataFrame,
    short_code: str,
    aggregate_id: int,
    delivery_var: str,
    shortage_var: str,
    demand_var: Optional[str] = None,
    demand_df: Optional[pd.DataFrame] = None
) -> List[Dict[str, Any]]:
    """
    Calculate monthly statistics for a CWS aggregate.
    
    Args:
        df: Main CalSim output DataFrame with delivery/shortage data
        short_code: Aggregate short code (e.g., 'swp_total')
        aggregate_id: Database ID for this aggregate
        delivery_var: Name of delivery variable
        shortage_var: Name of shortage variable
        demand_var: Name of demand variable in demand_df (optional)
        demand_df: DataFrame containing demand data (optional)

    Returns list of dicts for cws_aggregate_monthly table.
    """
    results = []

    # Check if variables exist
    has_delivery = delivery_var in df.columns
    has_shortage = shortage_var in df.columns
    has_demand = demand_var is not None and demand_df is not None and demand_var in demand_df.columns

    if not has_delivery:
        log.warning(f"Delivery variable {delivery_var} not found for {short_code}")
        return []

    is_annual = (df['WaterMonth'] == 0).all() if 'WaterMonth' in df.columns else False

    if is_annual:
        # Annual data - single aggregated row
        delivery_data = df[delivery_var].dropna()
        shortage_data = df[shortage_var].dropna() if has_shortage else pd.Series()

        if delivery_data.empty:
            return []

        row = {
            'cws_aggregate_id': aggregate_id,
            'water_month': 0,
            'delivery_avg_taf': round(float(delivery_data.mean()), 2),
            'delivery_cv': round(float(delivery_data.std() / delivery_data.mean()), 4) if delivery_data.mean() > 0 else 0,
            'sample_count': len(delivery_data),
        }

        # Delivery percentiles
        for p in DELIVERY_PERCENTILES:
            row[f'delivery_q{p}'] = round(float(np.percentile(delivery_data, p)), 2)

        # Delivery exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f'delivery_exc_p{p}'] = round(float(np.percentile(delivery_data, 100 - p)), 2)

        # Shortage statistics
        if not shortage_data.empty:
            row['shortage_avg_taf'] = round(float(shortage_data.mean()), 2)
            row['shortage_cv'] = round(float(shortage_data.std() / shortage_data.mean()), 4) if shortage_data.mean() > 0 else 0
            row['shortage_frequency_pct'] = round(((shortage_data > SHORTAGE_THRESHOLD_TAF).sum() / len(shortage_data)) * 100, 2)

            for p in DELIVERY_PERCENTILES:
                row[f'shortage_q{p}'] = round(float(np.percentile(shortage_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f'shortage_exc_p{p}'] = round(float(np.percentile(shortage_data, 100 - p)), 2)
        else:
            row['shortage_avg_taf'] = None
            row['shortage_cv'] = None
            row['shortage_frequency_pct'] = None

        # Demand and percent of demand (annual)
        row['demand_avg_taf'] = None
        row['percent_of_demand_avg'] = None
        if has_demand:
            try:
                demand_data = demand_df[demand_var].dropna()
                if not demand_data.empty:
                    row['demand_avg_taf'] = round(float(demand_data.mean()), 2)
                    if row['demand_avg_taf'] > 0:
                        pct = (row['delivery_avg_taf'] / row['demand_avg_taf']) * 100
                        row['percent_of_demand_avg'] = round(min(100.0, max(0.0, pct)), 2)
            except Exception as e:
                log.warning(f"Error calculating demand for {short_code}: {e}")

        results.append(row)
    else:
        # Monthly data - 12 rows
        for wm in range(1, 13):
            month_df = df[df['WaterMonth'] == wm]
            delivery_data = month_df[delivery_var].dropna()

            if delivery_data.empty:
                continue

            shortage_data = month_df[shortage_var].dropna() if has_shortage else pd.Series()

            row = {
                'cws_aggregate_id': aggregate_id,
                'water_month': wm,
                'delivery_avg_taf': round(float(delivery_data.mean()), 2),
                'delivery_cv': round(float(delivery_data.std() / delivery_data.mean()), 4) if delivery_data.mean() > 0 else 0,
                'sample_count': len(delivery_data),
            }

            # Delivery percentiles
            for p in DELIVERY_PERCENTILES:
                row[f'delivery_q{p}'] = round(float(np.percentile(delivery_data, p)), 2)

            # Delivery exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f'delivery_exc_p{p}'] = round(float(np.percentile(delivery_data, 100 - p)), 2)

            # Shortage statistics
            if not shortage_data.empty:
                row['shortage_avg_taf'] = round(float(shortage_data.mean()), 2)
                row['shortage_cv'] = round(float(shortage_data.std() / shortage_data.mean()), 4) if shortage_data.mean() > 0 else 0
                row['shortage_frequency_pct'] = round(((shortage_data > SHORTAGE_THRESHOLD_TAF).sum() / len(shortage_data)) * 100, 2)

                for p in DELIVERY_PERCENTILES:
                    row[f'shortage_q{p}'] = round(float(np.percentile(shortage_data, p)), 2)

                # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
                for p in EXCEEDANCE_PERCENTILES:
                    row[f'shortage_exc_p{p}'] = round(float(np.percentile(shortage_data, 100 - p)), 2)
            else:
                row['shortage_avg_taf'] = None
                row['shortage_cv'] = None
                row['shortage_frequency_pct'] = None

            # Demand and percent of demand (monthly)
            row['demand_avg_taf'] = None
            row['percent_of_demand_avg'] = None
            if has_demand:
                try:
                    # Get demand data for this month
                    if 'WaterMonth' in demand_df.columns:
                        month_demand_df = demand_df[demand_df['WaterMonth'] == wm]
                        demand_data = month_demand_df[demand_var].dropna()
                    else:
                        demand_data = demand_df[demand_var].dropna()
                    
                    if not demand_data.empty:
                        # Convert CFS to TAF if demand_df has DateTime column
                        if 'DateTime' in demand_df.columns:
                            # Get days in month for conversion
                            days_in_month = month_demand_df['DateTime'].dt.daysinmonth.mean() if 'DateTime' in month_demand_df.columns else 30
                            demand_taf = float(demand_data.mean()) * days_in_month * CFS_TO_TAF_PER_DAY
                        else:
                            demand_taf = float(demand_data.mean())
                        
                        row['demand_avg_taf'] = round(demand_taf, 2)
                        if demand_taf > 0:
                            pct = (row['delivery_avg_taf'] / row['demand_avg_taf']) * 100
                            row['percent_of_demand_avg'] = round(min(100.0, max(0.0, pct)), 2)
                except Exception as e:
                    log.warning(f"Error calculating monthly demand for {short_code} month {wm}: {e}")

            results.append(row)

    return results


def calculate_aggregate_period_summary(
    df: pd.DataFrame,
    short_code: str,
    aggregate_id: int,
    delivery_var: str,
    shortage_var: str,
    demand_var: Optional[str] = None,
    demand_df: Optional[pd.DataFrame] = None
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary for a CWS aggregate.
    
    Args:
        df: Main CalSim output DataFrame with delivery/shortage data
        short_code: Aggregate short code (e.g., 'swp_total')
        aggregate_id: Database ID for this aggregate
        delivery_var: Name of delivery variable
        shortage_var: Name of shortage variable
        demand_var: Name of demand variable in demand_df (optional)
        demand_df: DataFrame containing demand data (optional)

    Returns dict for cws_aggregate_period_summary table.
    """
    if delivery_var not in df.columns:
        return None

    delivery_data = df[delivery_var].dropna()
    if delivery_data.empty:
        return None

    water_years = sorted(df['WaterYear'].unique())

    result = {
        'cws_aggregate_id': aggregate_id,
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),
    }

    # Annual delivery statistics
    annual_delivery = df.groupby('WaterYear')[delivery_var].sum()
    result['annual_delivery_avg_taf'] = round(float(annual_delivery.mean()), 2)
    result['annual_delivery_cv'] = round(float(annual_delivery.std() / annual_delivery.mean()), 4) if annual_delivery.mean() > 0 else 0
    result['annual_delivery_min_taf'] = round(float(annual_delivery.min()), 2)
    result['annual_delivery_max_taf'] = round(float(annual_delivery.max()), 2)

    # Delivery exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, 100 - p)), 2)

    # Shortage statistics
    has_shortage = shortage_var in df.columns
    if has_shortage:
        annual_shortage = df.groupby('WaterYear')[shortage_var].sum()
        # Use threshold to filter out floating-point noise from CalSim solver
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
            result['avg_pct_allocation_met'] = result['reliability_pct']
        else:
            result['reliability_pct'] = None
            result['avg_pct_allocation_met'] = None
    else:
        result['annual_shortage_avg_taf'] = None
        result['shortage_years_count'] = None
        result['shortage_frequency_pct'] = None
        result['reliability_pct'] = None
        result['avg_pct_allocation_met'] = None

    # Demand and percent of demand
    result['annual_demand_avg_taf'] = None
    result['avg_pct_demand_met'] = None
    
    has_demand = demand_var is not None and demand_df is not None and demand_var in demand_df.columns
    if has_demand:
        try:
            # Ensure demand_df has water year column
            if 'WaterYear' not in demand_df.columns:
                demand_df_copy = add_water_year_month(demand_df.copy())
            else:
                demand_df_copy = demand_df.copy()
            
            # Calculate annual demand
            if 'DateTime' in demand_df_copy.columns:
                # Convert CFS to TAF
                demand_df_copy['DaysInMonth'] = demand_df_copy['DateTime'].dt.daysinmonth
                demand_df_copy['demand_taf'] = (
                    pd.to_numeric(demand_df_copy[demand_var], errors='coerce') * 
                    demand_df_copy['DaysInMonth'] * CFS_TO_TAF_PER_DAY
                )
                annual_demand = demand_df_copy.groupby('WaterYear')['demand_taf'].sum()
            else:
                annual_demand = demand_df_copy.groupby('WaterYear')[demand_var].sum()
            
            if not annual_demand.empty and annual_demand.mean() > 0:
                result['annual_demand_avg_taf'] = round(float(annual_demand.mean()), 2)
                
                # Calculate percent of demand met
                if result['annual_delivery_avg_taf'] and result['annual_demand_avg_taf'] > 0:
                    pct = (result['annual_delivery_avg_taf'] / result['annual_demand_avg_taf']) * 100
                    # Clip to 0-100 range (can exceed 100% if carryover/surplus is used)
                    result['avg_pct_demand_met'] = round(min(100.0, max(0.0, pct)), 2)
                    
                log.debug(f"{short_code}: demand_avg={result['annual_demand_avg_taf']}, pct_met={result['avg_pct_demand_met']}")
        except Exception as e:
            log.warning(f"Error calculating demand for {short_code}: {e}")

    return result


def calculate_all_cws_aggregate_statistics(
    scenario_id: str,
    csv_path: Optional[str] = None,
    demand_csv_path: Optional[str] = None,
    use_local: bool = False
) -> Tuple[List[Dict], List[Dict]]:
    """
    Calculate all statistics for CWS aggregates for a scenario.
    
    Args:
        scenario_id: Scenario ID (e.g., 's0020')
        csv_path: Optional path to main CalSim output CSV
        demand_csv_path: Optional path to DEMANDS CSV
        use_local: Use local files instead of S3

    Returns:
        Tuple of (monthly_rows, period_summary_rows)
    """
    log.info(f"Processing scenario: {scenario_id}")

    # Load CalSim output
    if csv_path:
        df = load_calsim_csv_from_file(csv_path)
    else:
        df = load_calsim_csv_from_s3(scenario_id)

    # Add water year/month
    df = add_water_year_month(df)

    available_columns = list(df.columns)
    log.info(f"Available columns: {len(available_columns)}")

    # Load DEMANDS CSV for percent of demand calculations
    demand_df = load_demands_csv(scenario_id, use_local=use_local, demand_csv_path=demand_csv_path)
    if demand_df is not None:
        demand_df = add_water_year_month(demand_df)
        log.info(f"Loaded demand data with {len(demand_df)} rows, {len(demand_df.columns)} columns")
    else:
        log.warning("No demand data available - percent of demand will not be calculated")

    monthly_rows = []
    period_summary_rows = []

    mapped_count = 0
    demand_count = 0

    for short_code, info in CWS_AGGREGATES.items():
        delivery_var = info['delivery_var']
        shortage_var = info['shortage_var']
        demand_var = info.get('demand_var')
        aggregate_id = info['id']

        if delivery_var not in available_columns:
            log.warning(f"Delivery variable {delivery_var} not found for {short_code}")
            continue

        mapped_count += 1

        # Calculate monthly statistics (with demand if available)
        monthly = calculate_aggregate_monthly(
            df, short_code, aggregate_id, delivery_var, shortage_var,
            demand_var=demand_var,
            demand_df=demand_df
        )
        for row in monthly:
            row['scenario_short_code'] = scenario_id
        monthly_rows.extend(monthly)

        # Calculate period summary (with demand if available)
        summary = calculate_aggregate_period_summary(
            df, short_code, aggregate_id, delivery_var, shortage_var,
            demand_var=demand_var,
            demand_df=demand_df
        )
        if summary:
            summary['scenario_short_code'] = scenario_id
            period_summary_rows.append(summary)
            if summary.get('annual_demand_avg_taf') is not None:
                demand_count += 1

    log.info(f"Processed {mapped_count}/{len(CWS_AGGREGATES)} aggregates")
    log.info(f"Calculated demand for {demand_count}/{mapped_count} aggregates")
    log.info(f"Generated: {len(monthly_rows)} monthly, {len(period_summary_rows)} period summary rows")

    return monthly_rows, period_summary_rows


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate delivery and shortage statistics for CWS system-level aggregates'
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
        '--demand-csv',
        help='Local DEMANDS CSV file path'
    )
    parser.add_argument(
        '--use-local',
        action='store_true',
        help='Use local files from etl/pipelines instead of S3'
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

    all_monthly = []
    all_period_summary = []

    for scenario_id in scenarios_to_process:
        try:
            monthly, period_summary = calculate_all_cws_aggregate_statistics(
                scenario_id,
                csv_path=args.csv_path,
                demand_csv_path=args.demand_csv,
                use_local=args.use_local
            )

            all_monthly.extend(monthly)
            all_period_summary.extend(period_summary)

        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise

    if args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")
        log.info(f"Total: {len(all_monthly)} monthly, {len(all_period_summary)} period summary rows")
        return

    if args.output_json:
        output = {
            'monthly': all_monthly,
            'period_summary': all_period_summary,
        }
        print(json.dumps(output, indent=2))
        return

    # Save to database
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        log.error("DATABASE_URL not set. Cannot save to database.")
        log.info("Use --output-json to output results as JSON instead.")
        return

    if not HAS_PSYCOPG2:
        log.error("psycopg2 not installed. Cannot save to database.")
        return

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        # Delete existing data for these scenarios
        scenario_ids = list(set(row['scenario_short_code'] for row in all_monthly))
        for scenario_id in scenario_ids:
            cur.execute("DELETE FROM cws_aggregate_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM cws_aggregate_period_summary WHERE scenario_short_code = %s", (scenario_id,))
            log.info(f"Cleared existing data for scenario {scenario_id}")

        # Insert monthly rows
        if all_monthly:
            monthly_cols = [
                'scenario_short_code', 'cws_aggregate_id', 'water_month',
                'delivery_avg_taf', 'delivery_cv',
                'delivery_q0', 'delivery_q10', 'delivery_q30', 'delivery_q50',
                'delivery_q70', 'delivery_q90', 'delivery_q100',
                'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25', 'delivery_exc_p50',
                'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
                'shortage_avg_taf', 'shortage_cv', 'shortage_frequency_pct',
                'shortage_q0', 'shortage_q10', 'shortage_q30', 'shortage_q50',
                'shortage_q70', 'shortage_q90', 'shortage_q100',
                'shortage_exc_p5', 'shortage_exc_p10', 'shortage_exc_p25', 'shortage_exc_p50',
                'shortage_exc_p75', 'shortage_exc_p90', 'shortage_exc_p95',
                'demand_avg_taf', 'percent_of_demand_avg',  # Demand metrics
                'sample_count'
            ]
            def convert_numpy(val):
                """Convert numpy types to Python native types."""
                if val is None:
                    return None
                if isinstance(val, (np.integer, np.int64, np.int32)):
                    return int(val)
                if isinstance(val, (np.floating, np.float64, np.float32)):
                    return float(val)
                return val

            monthly_values = [
                tuple(convert_numpy(row.get(col)) for col in monthly_cols)
                for row in all_monthly
            ]
            insert_sql = f"""
                INSERT INTO cws_aggregate_monthly ({', '.join(monthly_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, monthly_values)
            log.info(f"Inserted {len(monthly_values)} monthly rows")

        # Insert period summary rows
        if all_period_summary:
            summary_cols = [
                'scenario_short_code', 'cws_aggregate_id',
                'simulation_start_year', 'simulation_end_year', 'total_years',
                'annual_delivery_avg_taf', 'annual_delivery_cv',
                'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25',
                'delivery_exc_p50', 'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
                'annual_shortage_avg_taf', 'shortage_years_count', 'shortage_frequency_pct',
                'shortage_exc_p5', 'shortage_exc_p10', 'shortage_exc_p25',
                'shortage_exc_p50', 'shortage_exc_p75', 'shortage_exc_p90', 'shortage_exc_p95',
                'reliability_pct', 'avg_pct_allocation_met',
                'annual_demand_avg_taf', 'avg_pct_demand_met'  # Demand metrics
            ]
            summary_values = [
                tuple(convert_numpy(row.get(col)) for col in summary_cols)
                for row in all_period_summary
            ]
            insert_sql = f"""
                INSERT INTO cws_aggregate_period_summary ({', '.join(summary_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, summary_values)
            log.info(f"Inserted {len(summary_values)} period summary rows")

        conn.commit()
        cur.close()
        conn.close()
        log.info("Database save complete")

    except Exception as e:
        log.error(f"Database error: {e}")
        raise

    log.info("Total rows saved:")
    log.info(f"  Monthly: {len(all_monthly)}")
    log.info(f"  Period summary: {len(all_period_summary)}")


if __name__ == '__main__':
    main()
