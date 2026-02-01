#!/usr/bin/env python3
"""
Calculate monthly percentile statistics for reservoir storage.

Reads scenario CSV files from S3, calculates percentiles for each water month,
and outputs results for database loading or direct API response.

Usage:
    python calculate_reservoir_percentiles.py --scenario s0020
    python calculate_reservoir_percentiles.py --all-scenarios
    python calculate_reservoir_percentiles.py --scenario s0020 --output-json
"""

import argparse
import csv
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# Optional: boto3 for S3 access
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("reservoir_percentiles")

# Percentiles to calculate (for band charts)
# q0 = minimum, q50 = median, q100 = maximum
PERCENTILES = [0, 10, 30, 50, 70, 90, 100]

# Path to reservoir_entity.csv (relative to project root)
# From: etl/statistics/reservoirs/ -> need to go up 3 levels to reach database/
RESERVOIR_ENTITY_CSV = Path(__file__).parent.parent.parent.parent / \
    "database/seed_tables/04_calsim_data/reservoir_entity.csv"


def load_reservoir_entities(
    csv_path: Optional[Path] = None,
    filter_codes: Optional[List[str]] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Load reservoir metadata from reservoir_entity.csv.

    Args:
        csv_path: Path to reservoir_entity.csv
        filter_codes: Optional list of short_codes to filter to. If None, loads all 92 reservoirs.

    Returns dict keyed by short_code with id, capacity_taf, and dead_pool_taf.
    """
    if csv_path is None:
        csv_path = RESERVOIR_ENTITY_CSV

    if not csv_path.exists():
        raise FileNotFoundError(f"reservoir_entity.csv not found at {csv_path}")

    reservoirs = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            short_code = row['short_code']

            # Skip if filtering and not in filter list
            if filter_codes is not None and short_code not in filter_codes:
                continue

            # Skip reservoirs with zero capacity (can't calculate percentiles)
            capacity = float(row['capacity_taf']) if row['capacity_taf'] else 0
            if capacity <= 0:
                continue

            reservoirs[short_code] = {
                'id': int(row['id']),  # reservoir_entity_id for FK
                'name': row['name'],
                'capacity_taf': capacity,
                'dead_pool_taf': float(row['dead_pool_taf']) if row['dead_pool_taf'] else 0,
            }

    log.info(f"Loaded {len(reservoirs)} reservoirs from {csv_path}")
    return reservoirs

# Known scenarios
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027', 's0029']

# S3 bucket configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')


def load_scenario_csv_from_s3(scenario_id: str, reservoirs: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    """
    Load scenario CSV from S3 bucket.

    Memory optimization: Only loads the columns we need (DateTime + 8 reservoir columns)
    instead of the entire 10,000+ column CSV.
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access. Install with: pip install boto3")

    s3 = boto3.client('s3')

    # Build list of storage variable names (S_{short_code})
    storage_vars = {f'S_{code}': code for code in reservoirs.keys()}

    # Try different possible CSV locations
    possible_keys = [
        f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv",
        f"scenario/{scenario_id}/csv/{scenario_id}_DV.csv",
        f"scenario/{scenario_id}/csv/coeqwal_{scenario_id}_DV.csv",
        f"scenario/{scenario_id}/{scenario_id}_output.csv",
    ]

    for key in possible_keys:
        try:
            log.info(f"Trying S3 key: s3://{S3_BUCKET}/{key}")
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)

            # First, read just the header rows to find column indices
            # Read small chunk to get column names from row 1 (b row)
            log.info("Reading header rows to identify column indices...")
            header_df = pd.read_csv(
                response['Body'],
                header=None,
                nrows=8  # Just header rows
            )

            # Variable names are in row 1 (the 'b' row)
            col_names = header_df.iloc[1].tolist()

            # Find indices of columns we need: DateTime (col 0) + reservoir columns
            cols_to_load = [0]  # DateTime column
            reservoir_col_indices = {}
            for i, name in enumerate(col_names):
                if i == 0:
                    continue
                name_str = str(name).strip()
                if name_str in storage_vars:
                    cols_to_load.append(i)
                    reservoir_col_indices[name_str] = i
                    log.info(f"Found {name_str} at column {i}")

            log.info(f"Loading only {len(cols_to_load)} columns instead of {len(col_names)}")

            # Re-fetch the file and read only needed columns
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_csv(
                response['Body'],
                header=None,
                usecols=cols_to_load
            )
            df.attrs['storage_vars'] = storage_vars
            log.info(f"Successfully loaded from: {key}")
            log.info(f"DataFrame shape: {df.shape}")
            return df

        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            log.warning(f"Error loading {key}: {e}")
            continue

    raise FileNotFoundError(f"Could not find CSV for scenario {scenario_id} in S3")


def load_scenario_csv_from_file(file_path: str) -> pd.DataFrame:
    """Load scenario CSV from local file."""
    log.info(f"Loading from file: {file_path}")
    return pd.read_csv(file_path, header=None)


def parse_scenario_csv(df: pd.DataFrame, reservoirs: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    """
    Parse the DSS-format CSV with header rows.

    The CSV format has:
    - Row 0 (a): Source identifier (CALSIM)
    - Row 1 (b): Variable names (S_SHSTA, S_FOLSM, etc.)
    - Row 2 (c): Description
    - Row 3 (e): Time step (1MON)
    - Row 4 (f): Dataset (L2020A)
    - Row 5 (type): Data type (PER-AVER)
    - Row 6 (units): Units (TAF, CFS, etc.)
    - Row 7+: DateTime + data values
    """
    # Standard format: 7 header rows, variable names in row 1 (b row)
    header_rows = 7

    # Build mapping of storage variable names to short_codes
    storage_vars = {f'S_{code}': code for code in reservoirs.keys()}

    # Verify by checking if first column of row 7 looks like a date
    if len(df) > 7:
        try:
            test_date = pd.to_datetime(df.iloc[7, 0], errors='coerce')
            if pd.isna(test_date):
                # Try to find where dates start
                for i in range(min(15, len(df))):
                    try:
                        test = pd.to_datetime(df.iloc[i, 0], errors='coerce')
                        if not pd.isna(test):
                            header_rows = i
                            break
                    except (ValueError, TypeError):
                        continue
        except (ValueError, TypeError, IndexError):
            pass

    log.info(f"Using {header_rows} header rows")

    # Variable names are in row 1 (the 'b' row)
    col_names = df.iloc[1].tolist()
    log.info(f"Total columns: {len(col_names)}")

    # Parse the data portion
    data_df = df.iloc[header_rows:].copy()
    data_df.columns = range(len(data_df.columns))

    # First column is DateTime
    data_df.rename(columns={0: 'DateTime'}, inplace=True)
    data_df['DateTime'] = pd.to_datetime(data_df['DateTime'], errors='coerce')
    data_df = data_df.dropna(subset=['DateTime'])

    # Find storage columns - look for EXACT matches to avoid picking up variants
    storage_cols = {}
    for i, name in enumerate(col_names):
        if i == 0:
            continue
        name_str = str(name).strip()
        # Only match exact storage variable names (S_SHSTA, not S_SHSTA_DELTA)
        if name_str in storage_vars:
            storage_cols[name_str] = i
            log.info(f"Found {name_str} at column {i}")

    log.info(f"Found storage columns: {list(storage_cols.keys())}")

    # Rename columns to storage variable names (S_SHSTA, etc.)
    for var_name, col_idx in storage_cols.items():
        data_df.rename(columns={col_idx: var_name}, inplace=True)
        data_df[var_name] = pd.to_numeric(data_df[var_name], errors='coerce')

    return data_df


def add_water_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add water month column (Oct=1, Nov=2, ..., Sep=12).

    Water year runs from October 1 to September 30.
    """
    df = df.copy()
    df['Month'] = df['DateTime'].dt.month
    # Convert calendar month to water month: Oct(10)->1, Nov(11)->2, ..., Sep(9)->12
    df['WaterMonth'] = ((df['Month'] - 10) % 12) + 1
    df['WaterYear'] = df['DateTime'].dt.year
    # Adjust water year for Oct-Dec (they belong to next water year)
    df.loc[df['Month'] >= 10, 'WaterYear'] += 1
    return df


def calculate_percentiles_for_reservoir(
    df: pd.DataFrame,
    short_code: str,
    capacity_taf: float
) -> Dict[int, Dict[str, float]]:
    """
    Calculate percentile statistics for a single reservoir.

    Args:
        df: DataFrame with storage data (columns are S_{short_code})
        short_code: Reservoir short_code (e.g., SHSTA)
        capacity_taf: Reservoir capacity for percent calculation

    Returns dict of water_month -> {
        q0, q10, ..., q100, mean: values as percent of capacity
        q0_taf, q10_taf, ..., q100_taf, mean_taf: values in TAF
    }
    """
    storage_col = f'S_{short_code}'
    if storage_col not in df.columns:
        log.warning(f"Storage column {storage_col} not found in data")
        return {}

    df = df.copy()

    monthly_stats = {}
    for wm in range(1, 13):
        # Get raw TAF values for this water month
        month_data_taf = df[df['WaterMonth'] == wm][storage_col].dropna()

        if month_data_taf.empty:
            log.warning(f"No data for {short_code} water month {wm}")
            continue

        stats = {}

        # Calculate percentiles in both TAF and percent of capacity
        for p in PERCENTILES:
            taf_value = float(np.percentile(month_data_taf, p))
            pct_value = (taf_value / capacity_taf) * 100

            stats[f'q{p}'] = round(pct_value, 2)           # Percent of capacity
            stats[f'q{p}_taf'] = round(taf_value, 2)       # TAF

        # Mean in both units
        mean_taf = float(month_data_taf.mean())
        stats['mean'] = round((mean_taf / capacity_taf) * 100, 2)  # Percent
        stats['mean_taf'] = round(mean_taf, 2)                      # TAF

        monthly_stats[wm] = stats

    return monthly_stats


def calculate_all_reservoir_percentiles(
    scenario_id: str,
    reservoirs: Dict[str, Dict[str, Any]],
    csv_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calculate percentiles for all reservoirs for a scenario.

    Args:
        scenario_id: Scenario identifier (e.g., 's0020')
        reservoirs: Dict of reservoir metadata keyed by short_code
        csv_path: Optional local file path (uses S3 if not provided)

    Returns:
        Dict with structure:
        {
            'scenario_id': 's0020',
            'reservoirs': {
                'SHSTA': {
                    'id': 1,
                    'name': 'Shasta',
                    'capacity_taf': 4552,
                    'monthly_percentiles': {
                        1: {'q10': 45.2, 'q20': 52.1, ..., 'mean': 65.3},
                        ...
                    }
                },
                ...
            }
        }
    """
    log.info(f"Processing scenario: {scenario_id}")

    # Load CSV
    if csv_path:
        raw_df = load_scenario_csv_from_file(csv_path)
    else:
        raw_df = load_scenario_csv_from_s3(scenario_id, reservoirs)

    # Parse CSV
    df = parse_scenario_csv(raw_df, reservoirs)

    # Add water month
    df = add_water_month(df)

    log.info(f"Data range: {df['DateTime'].min()} to {df['DateTime'].max()}")
    log.info(f"Total rows: {len(df)}")

    # Calculate percentiles for each reservoir
    results = {
        'scenario_id': scenario_id,
        'reservoirs': {}
    }

    for short_code, meta in reservoirs.items():
        log.info(f"Calculating percentiles for {meta['name']} ({short_code})")

        monthly_stats = calculate_percentiles_for_reservoir(
            df, short_code, meta['capacity_taf']
        )

        if monthly_stats:
            results['reservoirs'][short_code] = {
                'id': meta['id'],
                'name': meta['name'],
                'capacity_taf': meta['capacity_taf'],
                'dead_pool_taf': meta['dead_pool_taf'],
                'monthly_percentiles': monthly_stats
            }

    return results


def format_for_database(results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert results to flat list of rows for database insertion.

    Returns list of dicts matching reservoir_monthly_percentile table columns.
    Table uses q0 (min), q10, q30, q50 (median), q70, q90, q100 (max).
    Includes both percent-of-capacity and TAF values.
    """
    rows = []
    scenario_id = results['scenario_id']

    for short_code, res_data in results['reservoirs'].items():
        entity_id = res_data['id']
        capacity_taf = res_data['capacity_taf']

        for water_month, stats in res_data['monthly_percentiles'].items():
            row = {
                'scenario_short_code': scenario_id,
                'reservoir_entity_id': entity_id,  # FK to reservoir_entity.id
                'water_month': water_month,
                # Percent of capacity values
                'q0': stats.get('q0'),      # minimum
                'q10': stats.get('q10'),
                'q30': stats.get('q30'),
                'q50': stats.get('q50'),    # median
                'q70': stats.get('q70'),
                'q90': stats.get('q90'),
                'q100': stats.get('q100'),  # maximum
                'mean_value': stats.get('mean'),
                # TAF values
                'q0_taf': stats.get('q0_taf'),
                'q10_taf': stats.get('q10_taf'),
                'q30_taf': stats.get('q30_taf'),
                'q50_taf': stats.get('q50_taf'),
                'q70_taf': stats.get('q70_taf'),
                'q90_taf': stats.get('q90_taf'),
                'q100_taf': stats.get('q100_taf'),
                'mean_taf': stats.get('mean_taf'),
                # Reference
                'capacity_taf': capacity_taf,
            }
            rows.append(row)

    return rows


def generate_sql_inserts(rows: List[Dict[str, Any]]) -> str:
    """
    Generate a single bulk INSERT statement with ON CONFLICT upsert.

    Uses multi-row VALUES syntax for clean console output (one INSERT message).
    Includes both percent-of-capacity and TAF columns.
    """
    lines = [
        "-- Generated SQL for reservoir_monthly_percentile data",
        "-- Run this script after creating the table with 03_create_reservoir_percentile_table.sql",
        "-- and applying 09_add_taf_percentile_columns.sql migration",
        f"-- Total rows: {len(rows)}",
        "",
        "BEGIN;",
        "",
        "INSERT INTO reservoir_monthly_percentile (",
        "    scenario_short_code, reservoir_entity_id, water_month,",
        "    -- Percent of capacity values",
        "    q0, q10, q30, q50, q70, q90, q100, mean_value,",
        "    -- TAF values",
        "    q0_taf, q10_taf, q30_taf, q50_taf, q70_taf, q90_taf, q100_taf, mean_taf,",
        "    capacity_taf, created_by, updated_by",
        ") VALUES",
    ]

    # Build VALUES rows
    value_rows = []
    for row in rows:
        value_row = (
            f"    ('{row['scenario_short_code']}', {row['reservoir_entity_id']}, {row['water_month']}, "
            # Percent values
            f"{row['q0']}, {row['q10']}, {row['q30']}, {row['q50']}, "
            f"{row['q70']}, {row['q90']}, {row['q100']}, {row['mean_value']}, "
            # TAF values
            f"{row['q0_taf']}, {row['q10_taf']}, {row['q30_taf']}, {row['q50_taf']}, "
            f"{row['q70_taf']}, {row['q90_taf']}, {row['q100_taf']}, {row['mean_taf']}, "
            f"{row['capacity_taf']}, 1, 1)"
        )
        value_rows.append(value_row)

    # Join with commas, last row gets no comma
    lines.append(",\n".join(value_rows))

    # ON CONFLICT clause
    lines.append("ON CONFLICT (scenario_short_code, reservoir_entity_id, water_month)")
    lines.append("DO UPDATE SET")
    lines.append("    -- Percent of capacity values")
    lines.append("    q0 = EXCLUDED.q0, q10 = EXCLUDED.q10, q30 = EXCLUDED.q30,")
    lines.append("    q50 = EXCLUDED.q50, q70 = EXCLUDED.q70, q90 = EXCLUDED.q90,")
    lines.append("    q100 = EXCLUDED.q100, mean_value = EXCLUDED.mean_value,")
    lines.append("    -- TAF values")
    lines.append("    q0_taf = EXCLUDED.q0_taf, q10_taf = EXCLUDED.q10_taf, q30_taf = EXCLUDED.q30_taf,")
    lines.append("    q50_taf = EXCLUDED.q50_taf, q70_taf = EXCLUDED.q70_taf, q90_taf = EXCLUDED.q90_taf,")
    lines.append("    q100_taf = EXCLUDED.q100_taf, mean_taf = EXCLUDED.mean_taf,")
    lines.append("    capacity_taf = EXCLUDED.capacity_taf,")
    lines.append("    updated_at = NOW(), updated_by = 1;")
    lines.append("")
    lines.append("COMMIT;")
    lines.append("")
    lines.append(f"\\echo 'Loaded {len(rows)} rows into reservoir_monthly_percentile'")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Calculate monthly percentile statistics for reservoir storage'
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
        help='Local CSV file path (instead of S3)'
    )
    parser.add_argument(
        '--reservoir-csv',
        help='Path to reservoir_entity.csv (default: auto-detect)'
    )
    parser.add_argument(
        '--output-json',
        action='store_true',
        help='Output results as JSON'
    )
    parser.add_argument(
        '--output-csv',
        help='Output results as CSV to specified path'
    )
    parser.add_argument(
        '--output-sql',
        help='Output SQL INSERT statements to specified path'
    )

    args = parser.parse_args()

    if not args.scenario and not args.all_scenarios:
        parser.error("Either --scenario or --all-scenarios is required")

    # Load reservoir metadata (with entity IDs)
    reservoir_csv = Path(args.reservoir_csv) if args.reservoir_csv else None
    reservoirs = load_reservoir_entities(reservoir_csv)

    scenarios_to_process = SCENARIOS if args.all_scenarios else [args.scenario]
    all_results = []

    for scenario_id in scenarios_to_process:
        try:
            results = calculate_all_reservoir_percentiles(
                scenario_id,
                reservoirs,
                csv_path=args.csv_path
            )

            if args.output_json:
                print(json.dumps(results, indent=2))

            db_rows = format_for_database(results)
            all_results.extend(db_rows)

            log.info(f"Generated {len(db_rows)} rows for {scenario_id}")

        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise

    if args.output_csv and all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(args.output_csv, index=False)
        log.info(f"Saved {len(all_results)} rows to {args.output_csv}")

    if args.output_sql and all_results:
        sql_content = generate_sql_inserts(all_results)
        with open(args.output_sql, 'w') as f:
            f.write(sql_content)
        log.info(f"Saved SQL to {args.output_sql}")

    log.info(f"Total rows generated: {len(all_results)}")
    return all_results


if __name__ == '__main__':
    main()
