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
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027']

# S3 bucket configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

# Percentiles for statistics
DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]


# =============================================================================
# CWS AGGREGATE DEFINITIONS
# =============================================================================

CWS_AGGREGATES = {
    # SWP Total - available in CalSim output
    'swp_total': {
        'id': 1,
        'label': 'SWP Total M&I',
        'delivery_var': 'DEL_SWP_PMI',
        'shortage_var': 'SHORT_SWP_PMI',
        'description': 'Total State Water Project M&I deliveries',
    },
    # NOTE: SWP regional breakdown (NOD/SOD) not available in CalSim output
    # Only the total DEL_SWP_PMI exists, not DEL_SWP_PMI_N/S

    # CVP North - available in CalSim output
    'cvp_nod': {
        'id': 2,
        'label': 'CVP North',
        'delivery_var': 'DEL_CVP_PMI_N',
        'shortage_var': 'SHORT_CVP_PMI_N',
        'description': 'CVP M&I deliveries - North',
    },
    # CVP South - available in CalSim output
    'cvp_sod': {
        'id': 3,
        'label': 'CVP South',
        'delivery_var': 'DEL_CVP_PMI_S',
        'shortage_var': 'SHORT_CVP_PMI_S',
        'description': 'CVP M&I deliveries - South',
    },
    # MWD - delivery available, shortage may not be in this exact form
    'mwd': {
        'id': 4,
        'label': 'Metropolitan Water District',
        'delivery_var': 'DEL_SWP_MWD',
        'shortage_var': None,  # SHORT_MWD_PMI may need to be calculated from components
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


def load_calsim_csv_from_file(file_path: str) -> pd.DataFrame:
    """
    Load CalSim output CSV from local file.

    Handles the 7-header-row DSS format.
    """
    log.info(f"Loading from file: {file_path}")

    # Read header to get column names
    header_df = pd.read_csv(file_path, header=None, nrows=8)
    col_names = header_df.iloc[1].tolist()

    # Read data portion
    data_df = pd.read_csv(file_path, header=None, skiprows=7)
    data_df.columns = col_names

    log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df


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
    shortage_var: str
) -> List[Dict[str, Any]]:
    """
    Calculate monthly statistics for a CWS aggregate.

    Returns list of dicts for cws_aggregate_monthly table.
    """
    results = []

    # Check if variables exist
    has_delivery = delivery_var in df.columns
    has_shortage = shortage_var in df.columns

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

        # Delivery exceedance percentiles
        for p in EXCEEDANCE_PERCENTILES:
            row[f'delivery_exc_p{p}'] = round(float(np.percentile(delivery_data, p)), 2)

        # Shortage statistics
        if not shortage_data.empty:
            row['shortage_avg_taf'] = round(float(shortage_data.mean()), 2)
            row['shortage_cv'] = round(float(shortage_data.std() / shortage_data.mean()), 4) if shortage_data.mean() > 0 else 0
            row['shortage_frequency_pct'] = round(((shortage_data > 0).sum() / len(shortage_data)) * 100, 2)

            for p in DELIVERY_PERCENTILES:
                row[f'shortage_q{p}'] = round(float(np.percentile(shortage_data, p)), 2)
        else:
            row['shortage_avg_taf'] = None
            row['shortage_cv'] = None
            row['shortage_frequency_pct'] = None

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

            # Delivery exceedance percentiles
            for p in EXCEEDANCE_PERCENTILES:
                row[f'delivery_exc_p{p}'] = round(float(np.percentile(delivery_data, p)), 2)

            # Shortage statistics
            if not shortage_data.empty:
                row['shortage_avg_taf'] = round(float(shortage_data.mean()), 2)
                row['shortage_cv'] = round(float(shortage_data.std() / shortage_data.mean()), 4) if shortage_data.mean() > 0 else 0
                row['shortage_frequency_pct'] = round(((shortage_data > 0).sum() / len(shortage_data)) * 100, 2)

                for p in DELIVERY_PERCENTILES:
                    row[f'shortage_q{p}'] = round(float(np.percentile(shortage_data, p)), 2)
            else:
                row['shortage_avg_taf'] = None
                row['shortage_cv'] = None
                row['shortage_frequency_pct'] = None

            results.append(row)

    return results


def calculate_aggregate_period_summary(
    df: pd.DataFrame,
    short_code: str,
    aggregate_id: int,
    delivery_var: str,
    shortage_var: str
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary for a CWS aggregate.

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

    # Delivery exceedance percentiles
    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, p)), 2)

    # Shortage statistics
    has_shortage = shortage_var in df.columns
    if has_shortage:
        annual_shortage = df.groupby('WaterYear')[shortage_var].sum()
        shortage_years = (annual_shortage > 0).sum()

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

    return result


def calculate_all_cws_aggregate_statistics(
    scenario_id: str,
    csv_path: Optional[str] = None
) -> Tuple[List[Dict], List[Dict]]:
    """
    Calculate all statistics for CWS aggregates for a scenario.

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

    monthly_rows = []
    period_summary_rows = []

    mapped_count = 0

    for short_code, info in CWS_AGGREGATES.items():
        delivery_var = info['delivery_var']
        shortage_var = info['shortage_var']
        aggregate_id = info['id']

        if delivery_var not in available_columns:
            log.warning(f"Delivery variable {delivery_var} not found for {short_code}")
            continue

        mapped_count += 1

        # Calculate monthly statistics
        monthly = calculate_aggregate_monthly(
            df, short_code, aggregate_id, delivery_var, shortage_var
        )
        for row in monthly:
            row['scenario_short_code'] = scenario_id
        monthly_rows.extend(monthly)

        # Calculate period summary
        summary = calculate_aggregate_period_summary(
            df, short_code, aggregate_id, delivery_var, shortage_var
        )
        if summary:
            summary['scenario_short_code'] = scenario_id
            period_summary_rows.append(summary)

    log.info(f"Processed {mapped_count}/{len(CWS_AGGREGATES)} aggregates")
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
                csv_path=args.csv_path
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
                'shortage_avg_taf', 'shortage_cv', 'shortage_frequency_pct',
                'shortage_q0', 'shortage_q10', 'shortage_q30', 'shortage_q50',
                'shortage_q70', 'shortage_q90', 'shortage_q100',
                'sample_count'
            ]
            monthly_values = [
                tuple(row.get(col) for col in monthly_cols)
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
                'reliability_pct', 'avg_pct_demand_met'
            ]
            summary_values = [
                tuple(row.get(col) for col in summary_cols)
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
