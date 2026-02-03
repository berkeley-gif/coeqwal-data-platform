#!/usr/bin/env python3
"""
Calculate delivery statistics for urban demand units (tier matrix DUs).

Approach:
1. Read tier matrix to get list of 71 DU_IDs
2. Read CalSim output CSV from S3 (DSS format with 7 header rows)
3. Map DU_IDs to column names (DN_*, D_*, GP_*)
4. Calculate statistics (percentiles, averages, etc.)
5. Return data for database insertion

Usage:
    python calculate_du_statistics.py --scenario s0020
    python calculate_du_statistics.py --scenario s0020 --csv-path /path/to/calsim_output.csv
"""

import argparse
import csv
import json
import logging
import os
import re
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
log = logging.getLogger("du_statistics")

# Known scenarios
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027']

# S3 bucket configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

# Paths relative to project
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
TIER_MATRIX_CSV = PROJECT_ROOT / "etl/pipelines/all_scenarios_tier_matrix.csv"

# Percentiles for monthly statistics (same as reservoir)
DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]

# Percentiles for exceedance (period summary)
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]


# =============================================================================
# DU_ID TO COLUMN MAPPING
# =============================================================================

# Special case mappings for DUs that don't follow standard patterns
SPECIAL_COLUMN_MAPPINGS = {
    # Metropolitan Water District - special delivery variable
    "MWD": "DEL_SWP_MWD",
    # Named location DUs that map to D_* columns (without _NU suffix in source)
    "AMCYN": "D_AMCYN",
    "ANTOC": "D_ANTOC",
    "FRFLD": "D_FRFLD",
    "GRSVL": "D_GRSVL",
    # Named location DUs that need _NU suffix
    "AMADR": "D_AMADR_NU",
    "BNCIA": "D_WTPBNC_BNCIA",
    "NAPA": "D_WTPJAC_NAPA",
    "VLLJO": "D_WTPFMH_VLLJO",
    # DUs that don't have delivery columns (placeholder - may return None)
    "CCWD": None,
    "JLIND": None,
    "PLMAS": None,
    "SUISN": None,
    "TVAFB": None,
    "UPANG": None,
    "WLDWD": None,
    "NAPA2": None,
    "CSB038": None,
    "CSB103": None,
    "CSTIC": None,
    "ESB324": None,
    "ESB347": None,
    "ESB414": None,
    "ESB415": None,
    "ESB420": None,
    "SBA029": None,
    "SBA036": None,
    "SCVWD": None,
}


def map_du_to_column(du_id: str, available_columns: List[str]) -> Optional[str]:
    """
    Map a DU_ID from tier matrix to the corresponding DELIVERIES file column.

    Mapping rules (in order of priority):
    1. Check special mappings first
    2. Zone-based DU_IDs (##_XX, ##X_XX) → DN_ prefix (surface delivery)
    3. Zone-based _NU DU_IDs → GP_ prefix (groundwater pumping) as fallback
    4. Named DU_IDs with _NU suffix → DN_ prefix
    5. Other named DU_IDs → D_ prefix (check with _NU suffix)

    Args:
        du_id: DU identifier from tier matrix (e.g., "16_PU", "AMADR")
        available_columns: List of column names in DELIVERIES file

    Returns:
        Matching column name or None if not found
    """
    # Check special mappings first
    if du_id in SPECIAL_COLUMN_MAPPINGS:
        mapped = SPECIAL_COLUMN_MAPPINGS[du_id]
        if mapped is None:
            return None
        if mapped in available_columns:
            return mapped
        log.debug(f"Special mapping {du_id} -> {mapped} not in columns")
        return None

    # Pattern 1: Zone-based DU_IDs (##_XX, ##X_XX like 02_PU, 26N_NU1)
    zone_pattern = r"^(\d+[NS]?)_(PU|NU|SU)\d*$"
    if re.match(zone_pattern, du_id):
        # Try DN_ first (surface delivery)
        col_name = f"DN_{du_id}"
        if col_name in available_columns:
            return col_name

        # For _NU DUs, try GP_ (groundwater pumping) as fallback
        if "_NU" in du_id:
            col_name = f"GP_{du_id}"
            if col_name in available_columns:
                return col_name

    # Pattern 2: Named DU_IDs with existing underscore (ELDID_NU1, GDPUD_NU, PCWA3)
    if "_" in du_id or du_id.endswith("3"):  # PCWA3 is special
        col_name = f"DN_{du_id}"
        if col_name in available_columns:
            return col_name

    # Pattern 3: Try D_ prefix with _NU suffix
    col_name = f"D_{du_id}_NU"
    if col_name in available_columns:
        return col_name

    # Pattern 4: Try D_ prefix without suffix
    col_name = f"D_{du_id}"
    if col_name in available_columns:
        return col_name

    # Pattern 5: Try DN_ prefix without modification
    col_name = f"DN_{du_id}"
    if col_name in available_columns:
        return col_name

    # Pattern 6: Try GP_ prefix for groundwater (for _NU DUs not caught earlier)
    if "_NU" in du_id:
        col_name = f"GP_{du_id}"
        if col_name in available_columns:
            return col_name

    log.debug(f"No column mapping found for DU_ID: {du_id}")
    return None


def load_tier_matrix_dus(csv_path: Optional[Path] = None) -> List[str]:
    """
    Load list of DU_IDs from tier matrix CSV.

    The tier matrix has DU_IDs as column headers (after scenario_id).

    Returns:
        List of DU_ID strings (e.g., ["02_PU", "02_SU", ..., "AMADR", ...])
    """
    if csv_path is None:
        csv_path = TIER_MATRIX_CSV

    if not csv_path.exists():
        raise FileNotFoundError(f"Tier matrix not found at {csv_path}")

    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)

    # First column is scenario_id, rest are DU_IDs
    du_ids = [col.strip().strip('"') for col in header[1:] if col.strip()]

    log.info(f"Loaded {len(du_ids)} DU_IDs from tier matrix")
    return du_ids


def load_calsim_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """
    Load CalSim output CSV from S3 bucket.

    Handles the DSS export format with 7 header rows.
    Variable names are in row 1 (0-indexed).

    Returns:
        DataFrame with date column and all CalSim variables
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

            # Get variable names from row 1
            col_names = df.iloc[1].tolist()

            # Re-fetch and read data portion
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data_df = pd.read_csv(response['Body'], header=None, skiprows=7)
            data_df.columns = col_names

            log.info(f"Loaded CalSim output: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
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

    # Read data portion
    data_df = pd.read_csv(file_path, header=None, skiprows=7)
    data_df.columns = col_names

    log.info(f"Loaded CalSim output: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add water year and water month columns.

    Handles both:
    - DSS format dates (e.g., "31OCT1921 2400")
    - Simple year values (e.g., 1921, 1922)
    """
    df = df.copy()

    # Find date column (first column)
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


def calculate_delivery_monthly(
    df: pd.DataFrame,
    du_id: str,
    column_name: str
) -> List[Dict[str, Any]]:
    """
    Calculate monthly delivery statistics for a single DU.

    For annual data, returns a single row with water_month=0.
    For monthly data, returns 12 rows (one per water month).

    Returns list of dicts for du_delivery_monthly table.
    """
    if column_name not in df.columns:
        log.warning(f"Column {column_name} not found for DU {du_id}")
        return []

    results = []
    is_annual = (df['WaterMonth'] == 0).all()

    if is_annual:
        # Annual data - single aggregated row
        data = df[column_name].dropna()
        if data.empty:
            return []

        row = {
            'du_id': du_id,
            'water_month': 0,  # 0 = annual
            'delivery_avg_taf': round(float(data.mean()), 2),
            'delivery_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'sample_count': len(data),
        }

        # Add percentiles
        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)

        # Add exceedance percentiles
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(data, p)), 2)

        results.append(row)
    else:
        # Monthly data - 12 rows
        for wm in range(1, 13):
            month_data = df[df['WaterMonth'] == wm][column_name].dropna()

            if month_data.empty:
                continue

            row = {
                'du_id': du_id,
                'water_month': wm,
                'delivery_avg_taf': round(float(month_data.mean()), 2),
                'delivery_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'sample_count': len(month_data),
            }

            # Add percentiles
            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)

            # Add exceedance percentiles
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(month_data, p)), 2)

            results.append(row)

    return results


def calculate_period_summary(
    df: pd.DataFrame,
    du_id: str,
    column_name: str
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary statistics for a single DU.

    Returns dict for du_period_summary table.
    """
    if column_name not in df.columns:
        return None

    data = df[column_name].dropna()
    if data.empty:
        return None

    water_years = sorted(df['WaterYear'].unique())

    result = {
        'du_id': du_id,
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),
    }

    # Annual delivery statistics
    annual_delivery = df.groupby('WaterYear')[column_name].sum()
    result['annual_delivery_avg_taf'] = round(float(annual_delivery.mean()), 2)
    if annual_delivery.mean() > 0:
        result['annual_delivery_cv'] = round(float(annual_delivery.std() / annual_delivery.mean()), 4)
    else:
        result['annual_delivery_cv'] = 0

    # Exceedance percentiles (annual)
    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, p)), 2)

    # Note: shortage statistics would require shortage columns (SHORT_DN_*, etc.)
    # These may be in a separate file or need to be calculated
    result['annual_shortage_avg_taf'] = None
    result['shortage_years_count'] = None
    result['shortage_frequency_pct'] = None
    result['reliability_pct'] = None
    result['avg_pct_demand_met'] = None
    result['annual_demand_avg_taf'] = None

    return result


def calculate_all_du_statistics(
    scenario_id: str,
    du_ids: Optional[List[str]] = None,
    csv_path: Optional[str] = None
) -> Tuple[List[Dict], List[Dict]]:
    """
    Calculate all statistics for tier matrix DUs for a scenario.

    Args:
        scenario_id: Scenario identifier (e.g., 's0020')
        du_ids: Optional list of DU_IDs (loads from tier matrix if not provided)
        csv_path: Optional local CSV file path (uses S3 if not provided)

    Returns:
        Tuple of (delivery_monthly_rows, period_summary_rows)
    """
    log.info(f"Processing scenario: {scenario_id}")

    # Load tier matrix DU_IDs
    if du_ids is None:
        du_ids = load_tier_matrix_dus()

    # Load CalSim output CSV
    if csv_path:
        df = load_calsim_csv_from_file(csv_path)
    else:
        df = load_calsim_csv_from_s3(scenario_id)

    # Add water year/month
    df = add_water_year_month(df)

    available_columns = list(df.columns)
    log.info(f"Data range: {df['WaterYear'].min()} to {df['WaterYear'].max()}")
    log.info(f"Available columns: {len(available_columns)}")

    delivery_monthly_rows = []
    period_summary_rows = []

    # Track mapping results
    mapped_count = 0
    unmapped_dus = []

    for du_id in du_ids:
        column_name = map_du_to_column(du_id, available_columns)

        if column_name is None:
            unmapped_dus.append(du_id)
            continue

        mapped_count += 1

        # Calculate delivery monthly
        monthly_rows = calculate_delivery_monthly(df, du_id, column_name)
        for row in monthly_rows:
            row['scenario_short_code'] = scenario_id
        delivery_monthly_rows.extend(monthly_rows)

        # Calculate period summary
        summary = calculate_period_summary(df, du_id, column_name)
        if summary:
            summary['scenario_short_code'] = scenario_id
            period_summary_rows.append(summary)

    log.info(f"Mapped {mapped_count}/{len(du_ids)} DU_IDs to columns")
    if unmapped_dus:
        log.info(f"Unmapped DUs ({len(unmapped_dus)}): {unmapped_dus[:10]}...")

    log.info(f"Generated: {len(delivery_monthly_rows)} delivery monthly, "
             f"{len(period_summary_rows)} period summary rows")

    return delivery_monthly_rows, period_summary_rows


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate delivery statistics for urban demand units (tier matrix DUs)'
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
    all_period_summary = []

    for scenario_id in scenarios_to_process:
        try:
            delivery_monthly, period_summary = calculate_all_du_statistics(
                scenario_id,
                csv_path=args.csv_path
            )

            all_delivery_monthly.extend(delivery_monthly)
            all_period_summary.extend(period_summary)

        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise

    if args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")
        log.info(f"Total: {len(all_delivery_monthly)} delivery monthly, "
                 f"{len(all_period_summary)} period summary rows")
        return

    if args.output_json:
        output = {
            'delivery_monthly': all_delivery_monthly,
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
            cur.execute("DELETE FROM du_delivery_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM du_period_summary WHERE scenario_short_code = %s", (scenario_id,))
            log.info(f"Cleared existing data for scenario {scenario_id}")

        # Insert delivery monthly rows
        if all_delivery_monthly:
            monthly_cols = [
                'scenario_short_code', 'du_id', 'water_month',
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
                INSERT INTO du_delivery_monthly ({', '.join(monthly_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, monthly_values)
            log.info(f"Inserted {len(monthly_values)} delivery monthly rows")

        # Insert period summary rows
        if all_period_summary:
            summary_cols = [
                'scenario_short_code', 'du_id',
                'simulation_start_year', 'simulation_end_year', 'total_years',
                'annual_delivery_avg_taf', 'annual_delivery_cv',
                'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25',
                'delivery_exc_p50', 'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
                'annual_shortage_avg_taf', 'shortage_years_count', 'shortage_frequency_pct',
                'reliability_pct', 'avg_pct_demand_met', 'annual_demand_avg_taf'
            ]
            summary_values = [
                tuple(convert_numpy(row.get(col)) for col in summary_cols)
                for row in all_period_summary
            ]
            insert_sql = f"""
                INSERT INTO du_period_summary ({', '.join(summary_cols)})
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
    log.info(f"  Period summary: {len(all_period_summary)}")


if __name__ == '__main__':
    main()
