#!/usr/bin/env python3
"""
Calculate delivery and shortage statistics for M&I contractors.

This module processes SWP/CVP contractor-level data using:
- D_*_PMI delivery variables (Project Municipal & Industrial)
- SHORT_D_*_PMI shortage variables

The contractor variable mappings are derived from:
- CWS_shortage_variables.csv
- swp_contractor_perdel_A.wresl (SWP contractor definitions)

Usage:
    python calculate_mi_statistics.py --scenario s0020
    python calculate_mi_statistics.py --scenario s0020 --csv-path /path/to/calsim_output.csv
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
log = logging.getLogger("mi_statistics")

# Known scenarios
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027']

# S3 bucket configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

# Paths relative to project
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
MI_CONTRACTORS_CSV = PROJECT_ROOT / "database/seed_tables/04_calsim_data/mi_contractor.csv"
CWS_SHORTAGE_CSV = PROJECT_ROOT / "etl/pipelines/CWS/CWS_shortage_variables.csv"

# Percentiles for statistics
DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]


# =============================================================================
# CONTRACTOR VARIABLE MAPPINGS
# =============================================================================

# SWP Contractor short codes mapped to CalSim delivery/shortage variable patterns
# Source: CWS_shortage_variables.csv and swp_contractor_perdel_A.wresl
MI_CONTRACTOR_VARIABLES = {
    # Format: short_code -> {delivery_vars: [...], shortage_vars: [...], description: ...}

    # Alameda County FC&WCD-Zone 7
    "ACFC": {
        "delivery_vars": ["D_SBA009_ACFC_PMI", "D_SBA020_ACFC_PMI"],
        "shortage_vars": ["SHORT_D_SBA009_ACFC_PMI", "SHORT_D_SBA020_ACFC_PMI"],
        "description": "Alameda County Flood Control & Water Conservation District - Zone 7",
    },

    # Alameda County WD
    "ACWD": {
        "delivery_vars": ["D_SBA029_ACWD_PMI"],
        "shortage_vars": ["SHORT_D_SBA029_ACWD_PMI"],
        "description": "Alameda County Water District",
    },

    # Antelope Valley-East Kern WA
    "AVEK": {
        "delivery_vars": ["D_ESB324_AVEK_PMI"],
        "shortage_vars": ["SHORT_D_ESB324_AVEK_PMI"],
        "description": "Antelope Valley-East Kern Water Agency",
    },

    # Santa Clara Valley WD
    "SCVWD": {
        "delivery_vars": ["D_SBA036_SCVWD_PMI"],
        "shortage_vars": ["SHORT_D_SBA036_SCVWD_PMI"],
        "description": "Santa Clara Valley Water District",
    },

    # Metropolitan WDSC (multiple delivery points)
    "MWD": {
        "delivery_vars": [
            "D_ESB413_MWDSC_PMI",
            "D_ESB433_MWDSC_PMI",
            "D_PRRIS_MWDSC_PMI",
            "D_WSB031_MWDSC_PMI",
            "DEL_SWP_MWD",  # aggregate
        ],
        "shortage_vars": [
            "SHORT_D_ESB413_MWDSC_PMI",
            "SHORT_D_ESB433_MWDSC_PMI",
            "SHORT_D_PRRIS_MWDSC_PMI",
            "SHORT_D_WSB031_MWDSC_PMI",
        ],
        "description": "Metropolitan Water District of Southern California",
    },

    # San Luis Obispo
    "OBISPO": {
        "delivery_vars": ["D_CSB038_OBISPO_PMI"],
        "shortage_vars": ["SHORT_D_CSB038_OBISPO_PMI"],
        "description": "San Luis Obispo County FC&WCD",
    },

    # Santa Barbara
    "BRBRA": {
        "delivery_vars": ["D_CSB103_BRBRA_PMI"],
        "shortage_vars": ["SHORT_D_CSB103_BRBRA_PMI"],
        "description": "Santa Barbara County FC&WCD",
    },

    # Ventura County
    "VNTRA": {
        "delivery_vars": ["D_CSTIC_VNTRA_PMI", "D_PYRMD_VNTRA_PMI"],
        "shortage_vars": ["SHORT_D_CSTIC_VNTRA_PMI", "SHORT_D_PYRMD_VNTRA_PMI"],
        "description": "Ventura County Watershed Protection District",
    },

    # Palmdale
    "PLMDL": {
        "delivery_vars": ["D_ESB347_PLMDL_PMI"],
        "shortage_vars": ["SHORT_D_ESB347_PLMDL_PMI"],
        "description": "Palmdale Water District",
    },

    # Littlerock Creek ID
    "LROCK": {
        "delivery_vars": ["D_ESB355_LROCK_PMI"],
        "shortage_vars": ["SHORT_D_ESB355_LROCK_PMI"],
        "description": "Littlerock Creek Irrigation District",
    },

    # Mojave WA
    "MOJVE": {
        "delivery_vars": ["D_ESB403_MOJVE_PMI"],
        "shortage_vars": ["SHORT_D_ESB403_MOJVE_PMI"],
        "description": "Mojave Water Agency",
    },

    # Castaic Lake (LA area)
    "CCHLA": {
        "delivery_vars": ["D_ESB407_CCHLA_PMI"],
        "shortage_vars": ["SHORT_D_ESB407_CCHLA_PMI"],
        "description": "Castaic Lake Water Agency (LA area)",
    },

    # Desert WA
    "DESRT": {
        "delivery_vars": ["D_ESB408_DESRT_PMI"],
        "shortage_vars": ["SHORT_D_ESB408_DESRT_PMI"],
        "description": "Desert Water Agency",
    },

    # San Bernardino Valley MWD
    "BRDNO": {
        "delivery_vars": ["D_ESB414_BRDNO_PMI"],
        "shortage_vars": ["SHORT_D_ESB414_BRDNO_PMI"],
        "description": "San Bernardino Valley Municipal Water District",
    },

    # San Gabriel Valley MWD
    "GABRL": {
        "delivery_vars": ["D_ESB415_GABRL_PMI"],
        "shortage_vars": ["SHORT_D_ESB415_GABRL_PMI"],
        "description": "San Gabriel Valley Municipal Water District",
    },

    # San Gorgonio Pass WA
    "GRGNO": {
        "delivery_vars": ["D_ESB420_GRGNO_PMI"],
        "shortage_vars": ["SHORT_D_ESB420_GRGNO_PMI"],
        "description": "San Gorgonio Pass Water Agency",
    },

    # Kern County WA
    "KERN": {
        "delivery_vars": ["D_CAA194_KERNA_PMI", "D_CAA194_KERNB_PMI"],
        "shortage_vars": ["SHORT_D_CAA194_KERNA_PMI", "SHORT_D_CAA194_KERNB_PMI"],
        "description": "Kern County Water Agency",
    },

    # SVRWD (Castaic Lake)
    "CSTLN": {
        "delivery_vars": ["D_SVRWD_CSTLN_PMI"],
        "shortage_vars": ["SHORT_D_SVRWD_CSTLN_PMI"],
        "description": "Castaic Lake Water Agency (SVRWD)",
    },

    # Aggregate SWP totals
    "SWP_PMI_TOTAL": {
        "delivery_vars": ["DEL_SWP_PMI"],  # if exists
        "shortage_vars": ["SHORT_SWP_PMI"],
        "description": "Total SWP Project M&I (aggregate)",
    },

    "SWP_PMI_N": {
        "delivery_vars": ["DEL_SWP_PMI_N"],  # if exists
        "shortage_vars": ["SHORT_SWP_PMI_N"],
        "description": "SWP Project M&I - North of Delta (aggregate)",
    },

    "SWP_PMI_S": {
        "delivery_vars": ["DEL_SWP_PMI_S"],  # if exists
        "shortage_vars": ["SHORT_SWP_PMI_S"],
        "description": "SWP Project M&I - South of Delta (aggregate)",
    },

    "CVP_PMI_N": {
        "delivery_vars": ["DEL_CVP_PMI_N"],  # if exists
        "shortage_vars": ["SHORT_CVP_PMI_N"],
        "description": "CVP Project M&I - North (aggregate)",
    },

    "CVP_PMI_S": {
        "delivery_vars": ["DEL_CVP_PMI_S"],  # if exists
        "shortage_vars": ["SHORT_CVP_PMI_S"],
        "description": "CVP Project M&I - South (aggregate)",
    },
}


def load_mi_contractors(csv_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """
    Load M&I contractor metadata from mi_contractor.csv.

    Returns dict keyed by short_code with contractor details.
    """
    if csv_path is None:
        csv_path = MI_CONTRACTORS_CSV

    if not csv_path.exists():
        log.warning(f"mi_contractor.csv not found at {csv_path}, using built-in mappings")
        return MI_CONTRACTOR_VARIABLES

    contractors = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            short_code = row.get('short_code', '')
            if short_code:
                contractors[short_code] = {
                    'id': int(row['id']) if row.get('id') else None,
                    'contractor_name': row.get('contractor_name', ''),
                    'project': row.get('project', ''),
                    'contractor_type': row.get('contractor_type', ''),
                    'contract_amount_taf': float(row['contract_amount_taf']) if row.get('contract_amount_taf') else None,
                }

    log.info(f"Loaded {len(contractors)} contractors from {csv_path}")
    return contractors


def load_calsim_csv_from_s3(scenario_id: str, variables: List[str]) -> pd.DataFrame:
    """
    Load CalSim output CSV from S3 bucket.

    Handles the DSS export format with 7 header rows.
    Variable names are in row 1 (0-indexed).
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access. Install with: pip install boto3")

    s3 = boto3.client('s3')

    # Try different possible CSV locations
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

            # Re-fetch and read data portion (skip 7 header rows)
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

    # Read header to get column names
    header_df = pd.read_csv(file_path, header=None, nrows=8)
    col_names = header_df.iloc[1].tolist()

    # Read data portion (skip 7 header rows)
    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    data_df.columns = col_names

    log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add water year and water month columns.

    Handles both:
    - DSS format dates (e.g., "31OCT1921 2400")
    - Simple year values (e.g., 1921, 1922)
    """
    df = df.copy()

    # Find date column (first column in DSS format)
    first_col = df.columns[0]
    date_values = df[first_col]

    # Try to parse as datetime (handles DSS format like "31OCT1921 2400")
    try:
        df['DateTime'] = pd.to_datetime(date_values, errors='coerce')

        if df['DateTime'].notna().sum() > 0:
            # Successfully parsed as datetime - monthly data
            df['CalendarMonth'] = df['DateTime'].dt.month
            df['CalendarYear'] = df['DateTime'].dt.year

            # Water month: Oct(10)->1, Nov(11)->2, ..., Sep(9)->12
            df['WaterMonth'] = ((df['CalendarMonth'] - 10) % 12) + 1

            # Water year: Oct-Dec belong to next water year
            df['WaterYear'] = df['CalendarYear']
            df.loc[df['CalendarMonth'] >= 10, 'WaterYear'] += 1

            log.info(f"Detected monthly data: {df['DateTime'].min()} to {df['DateTime'].max()}")
            return df
    except Exception as e:
        log.debug(f"Could not parse as datetime: {e}")

    # Fallback: check if values are years (annual data)
    date_numeric = pd.to_numeric(date_values, errors='coerce')
    if date_numeric.notna().all() and (date_numeric >= 1900).all() and (date_numeric <= 2100).all():
        df['WaterYear'] = date_numeric.astype(int)
        df['WaterMonth'] = 0  # 0 indicates annual data
        log.info(f"Detected annual data: years {df['WaterYear'].min()}-{df['WaterYear'].max()}")
        return df

    raise ValueError(f"Could not parse date column '{first_col}' as datetime or year values")

    return df


def calculate_contractor_delivery_monthly(
    df: pd.DataFrame,
    contractor_code: str,
    delivery_vars: List[str]
) -> List[Dict[str, Any]]:
    """
    Calculate monthly delivery statistics for a contractor.

    Aggregates multiple delivery points if contractor has several.
    """
    # Find which delivery vars exist in the data
    available_vars = [v for v in delivery_vars if v in df.columns]
    if not available_vars:
        log.debug(f"No delivery variables found for {contractor_code}")
        return []

    # Sum all delivery points for this contractor
    df_copy = df.copy()
    df_copy['total_delivery'] = df_copy[available_vars].sum(axis=1)

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        data = df_copy['total_delivery'].dropna()
        if data.empty:
            return []

        row = {
            'mi_contractor_code': contractor_code,
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
            month_data = df_copy[df_copy['WaterMonth'] == wm]['total_delivery'].dropna()
            if month_data.empty:
                continue

            row = {
                'mi_contractor_code': contractor_code,
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


def calculate_contractor_shortage_monthly(
    df: pd.DataFrame,
    contractor_code: str,
    shortage_vars: List[str]
) -> List[Dict[str, Any]]:
    """Calculate monthly shortage statistics for a contractor."""
    available_vars = [v for v in shortage_vars if v in df.columns]
    if not available_vars:
        log.debug(f"No shortage variables found for {contractor_code}")
        return []

    df_copy = df.copy()
    df_copy['total_shortage'] = df_copy[available_vars].sum(axis=1)

    results = []
    is_annual = (df_copy['WaterMonth'] == 0).all()

    if is_annual:
        data = df_copy['total_shortage'].dropna()
        if data.empty:
            return []

        shortage_count = (data > 0).sum()

        row = {
            'mi_contractor_code': contractor_code,
            'water_month': 0,
            'shortage_avg_taf': round(float(data.mean()), 2),
            'shortage_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'shortage_frequency_pct': round((shortage_count / len(data)) * 100, 2),
            'sample_count': len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy['WaterMonth'] == wm]['total_shortage'].dropna()
            if month_data.empty:
                continue

            shortage_count = (month_data > 0).sum()

            row = {
                'mi_contractor_code': contractor_code,
                'water_month': wm,
                'shortage_avg_taf': round(float(month_data.mean()), 2),
                'shortage_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'shortage_frequency_pct': round((shortage_count / len(month_data)) * 100, 2),
                'sample_count': len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)

            results.append(row)

    return results


def calculate_contractor_period_summary(
    df: pd.DataFrame,
    contractor_code: str,
    delivery_vars: List[str],
    shortage_vars: List[str]
) -> Optional[Dict[str, Any]]:
    """Calculate period-of-record summary for a contractor."""
    available_delivery = [v for v in delivery_vars if v in df.columns]
    available_shortage = [v for v in shortage_vars if v in df.columns]

    if not available_delivery:
        return None

    df_copy = df.copy()
    df_copy['total_delivery'] = df_copy[available_delivery].sum(axis=1)

    water_years = sorted(df_copy['WaterYear'].unique())

    result = {
        'mi_contractor_code': contractor_code,
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),
    }

    # Annual delivery statistics
    annual_delivery = df_copy.groupby('WaterYear')['total_delivery'].sum()
    result['annual_delivery_avg_taf'] = round(float(annual_delivery.mean()), 2)
    if annual_delivery.mean() > 0:
        result['annual_delivery_cv'] = round(float(annual_delivery.std() / annual_delivery.mean()), 4)
    else:
        result['annual_delivery_cv'] = 0

    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, p)), 2)

    # Shortage statistics
    if available_shortage:
        df_copy['total_shortage'] = df_copy[available_shortage].sum(axis=1)
        annual_shortage = df_copy.groupby('WaterYear')['total_shortage'].sum()
        shortage_years = (annual_shortage > 0).sum()

        result['annual_shortage_avg_taf'] = round(float(annual_shortage.mean()), 2)
        result['shortage_years_count'] = int(shortage_years)
        result['shortage_frequency_pct'] = round((shortage_years / len(water_years)) * 100, 2)

        for p in EXCEEDANCE_PERCENTILES:
            result[f'shortage_exc_p{p}'] = round(float(np.percentile(annual_shortage, p)), 2)

        # Reliability = 1 - (shortage / delivery)
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


def calculate_all_mi_statistics(
    scenario_id: str,
    contractors: Optional[Dict[str, Dict]] = None,
    csv_path: Optional[str] = None
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Calculate all statistics for M&I contractors for a scenario.

    Returns:
        Tuple of (delivery_monthly_rows, shortage_monthly_rows, period_summary_rows)
    """
    log.info(f"Processing scenario: {scenario_id}")

    # Use built-in contractor mappings
    if contractors is None:
        contractors = MI_CONTRACTOR_VARIABLES

    # Load CalSim output
    if csv_path:
        df = load_calsim_csv_from_file(csv_path)
    else:
        df = load_calsim_csv_from_s3(scenario_id, [])

    # Add water year/month
    df = add_water_year_month(df)

    available_columns = list(df.columns)
    log.info(f"Available columns: {len(available_columns)}")

    delivery_monthly_rows = []
    shortage_monthly_rows = []
    period_summary_rows = []

    mapped_count = 0

    for code, info in contractors.items():
        delivery_vars = info.get('delivery_vars', [])
        shortage_vars = info.get('shortage_vars', [])

        # Check if any variables exist
        has_delivery = any(v in available_columns for v in delivery_vars)
        has_shortage = any(v in available_columns for v in shortage_vars)

        if not has_delivery and not has_shortage:
            continue

        mapped_count += 1

        # Calculate delivery monthly
        if has_delivery:
            monthly_rows = calculate_contractor_delivery_monthly(df, code, delivery_vars)
            for row in monthly_rows:
                row['scenario_short_code'] = scenario_id
            delivery_monthly_rows.extend(monthly_rows)

        # Calculate shortage monthly
        if has_shortage:
            shortage_rows = calculate_contractor_shortage_monthly(df, code, shortage_vars)
            for row in shortage_rows:
                row['scenario_short_code'] = scenario_id
            shortage_monthly_rows.extend(shortage_rows)

        # Calculate period summary
        summary = calculate_contractor_period_summary(df, code, delivery_vars, shortage_vars)
        if summary:
            summary['scenario_short_code'] = scenario_id
            period_summary_rows.append(summary)

    log.info(f"Mapped {mapped_count}/{len(contractors)} contractors with data")
    log.info(f"Generated: {len(delivery_monthly_rows)} delivery monthly, "
             f"{len(shortage_monthly_rows)} shortage monthly, "
             f"{len(period_summary_rows)} period summary rows")

    return delivery_monthly_rows, shortage_monthly_rows, period_summary_rows


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate delivery and shortage statistics for M&I contractors'
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

    all_delivery_monthly = []
    all_shortage_monthly = []
    all_period_summary = []

    for scenario_id in scenarios_to_process:
        try:
            delivery_monthly, shortage_monthly, period_summary = calculate_all_mi_statistics(
                scenario_id,
                csv_path=args.csv_path
            )

            all_delivery_monthly.extend(delivery_monthly)
            all_shortage_monthly.extend(shortage_monthly)
            all_period_summary.extend(period_summary)

        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise

    if args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")
        log.info(f"Total: {len(all_delivery_monthly)} delivery monthly, "
                 f"{len(all_shortage_monthly)} shortage monthly, "
                 f"{len(all_period_summary)} period summary rows")
        return

    if args.output_json:
        output = {
            'delivery_monthly': all_delivery_monthly,
            'shortage_monthly': all_shortage_monthly,
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

    def convert_numpy(val):
        """Convert numpy types to Python native types."""
        if val is None:
            return None
        if isinstance(val, (np.integer, np.int64, np.int32)):
            return int(val)
        if isinstance(val, (np.floating, np.float64, np.float32)):
            return float(val)
        return val

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        # Delete existing data for these scenarios
        scenario_ids = list(set(row['scenario_short_code'] for row in all_delivery_monthly))
        for scenario_id in scenario_ids:
            cur.execute("DELETE FROM mi_delivery_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM mi_shortage_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM mi_contractor_period_summary WHERE scenario_short_code = %s", (scenario_id,))
            log.info(f"Cleared existing data for scenario {scenario_id}")

        # Insert delivery monthly rows
        if all_delivery_monthly:
            monthly_cols = [
                'scenario_short_code', 'mi_contractor_code', 'water_month',
                'delivery_avg_taf', 'delivery_cv',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
                'sample_count'
            ]
            monthly_values = [
                tuple(convert_numpy(row.get(col)) for col in monthly_cols)
                for row in all_delivery_monthly
            ]
            insert_sql = f"""
                INSERT INTO mi_delivery_monthly ({', '.join(monthly_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, monthly_values)
            log.info(f"Inserted {len(monthly_values)} delivery monthly rows")

        # Insert shortage monthly rows
        if all_shortage_monthly:
            shortage_cols = [
                'scenario_short_code', 'mi_contractor_code', 'water_month',
                'shortage_avg_taf', 'shortage_cv', 'shortage_frequency_pct',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'sample_count'
            ]
            shortage_values = [
                tuple(convert_numpy(row.get(col)) for col in shortage_cols)
                for row in all_shortage_monthly
            ]
            insert_sql = f"""
                INSERT INTO mi_shortage_monthly ({', '.join(shortage_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, shortage_values)
            log.info(f"Inserted {len(shortage_values)} shortage monthly rows")

        # Insert period summary rows
        if all_period_summary:
            summary_cols = [
                'scenario_short_code', 'mi_contractor_code',
                'simulation_start_year', 'simulation_end_year', 'total_years',
                'annual_delivery_avg_taf', 'annual_delivery_cv',
                'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25',
                'delivery_exc_p50', 'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
                'annual_shortage_avg_taf', 'shortage_years_count', 'shortage_frequency_pct',
                'shortage_exc_p5', 'shortage_exc_p10', 'shortage_exc_p25',
                'shortage_exc_p50', 'shortage_exc_p75', 'shortage_exc_p90', 'shortage_exc_p95',
                'reliability_pct'
            ]
            summary_values = [
                tuple(convert_numpy(row.get(col)) for col in summary_cols)
                for row in all_period_summary
            ]
            insert_sql = f"""
                INSERT INTO mi_contractor_period_summary ({', '.join(summary_cols)})
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
    log.info(f"  Delivery monthly: {len(all_delivery_monthly)}")
    log.info(f"  Shortage monthly: {len(all_shortage_monthly)}")
    log.info(f"  Period summary: {len(all_period_summary)}")


if __name__ == '__main__':
    main()
