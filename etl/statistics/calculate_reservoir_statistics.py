#!/usr/bin/env python3
"""
Calculate comprehensive reservoir statistics for all 92 reservoirs.

Populates three database tables:
- reservoir_storage_monthly: Monthly storage statistics (mean, cv, percentiles)
- reservoir_spill_monthly: Monthly spill/flood release statistics
- reservoir_period_summary: Period-of-record summary with storage exceedance and spill metrics

Usage:
    python calculate_reservoir_statistics.py --scenario s0020
    python calculate_reservoir_statistics.py --all-scenarios
    python calculate_reservoir_statistics.py --scenario s0020 --output-sql output.sql
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

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("reservoir_statistics")

# Known scenarios
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027']

# S3 bucket configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

# Path to reservoir_entity.csv (relative to project root)
RESERVOIR_ENTITY_CSV = Path(__file__).parent.parent.parent / \
    "database/seed_tables/04_calsim_data/reservoir_entity.csv"

# Percentiles for storage monthly statistics
STORAGE_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]

# Percentiles for storage exceedance (period summary)
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# CFS to TAF conversion factor (approximate for monthly data)
# CFS * days_in_month * 1.9835 / 1000
# Using average month of 30.4 days: CFS * 30.4 * 1.9835 / 1000 ≈ CFS * 0.0603
CFS_TO_TAF_MONTHLY = 0.0603


def load_reservoir_entities(csv_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """
    Load reservoir metadata from reservoir_entity.csv.

    Returns dict keyed by short_code with id, capacity_taf, and dead_pool_taf.
    The 'id' field is the reservoir_entity_id used as FK in statistics tables.
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
            reservoirs[short_code] = {
                'id': int(row['id']),  # reservoir_entity_id for FK
                'name': row['name'],
                'capacity_taf': float(row['capacity_taf']) if row['capacity_taf'] else 0,
                'dead_pool_taf': float(row['dead_pool_taf']) if row['dead_pool_taf'] else 0,
            }

    log.info(f"Loaded {len(reservoirs)} reservoirs from {csv_path}")
    return reservoirs


def load_scenario_csv_from_s3(scenario_id: str, reservoir_codes: List[str]) -> pd.DataFrame:
    """
    Load scenario CSV from S3 bucket.

    Memory optimization: Only loads the columns we need (DateTime + reservoir columns).

    Args:
        scenario_id: Scenario identifier (e.g., 's0020')
        reservoir_codes: List of codes to load (e.g., ['SHSTA', 'FOLSM'])
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access. Install with: pip install boto3")

    s3 = boto3.client('s3')

    # Build list of variable names to load
    # Storage: S_{code}, Spill: C_{code}_FLOOD
    vars_to_find = set()
    for code in reservoir_codes:
        vars_to_find.add(f'S_{code}')          # storage
        vars_to_find.add(f'C_{code}_FLOOD')    # flood spill

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

            # Read header rows to find column indices
            log.info("Reading header rows to identify column indices...")
            header_df = pd.read_csv(
                response['Body'],
                header=None,
                nrows=8
            )

            # Variable names are in row 1 (the 'b' row)
            col_names = header_df.iloc[1].tolist()

            # Find indices of columns we need
            cols_to_load = [0]  # DateTime column
            found_vars = {}
            for i, name in enumerate(col_names):
                if i == 0:
                    continue
                name_str = str(name).strip()
                if name_str in vars_to_find:
                    cols_to_load.append(i)
                    found_vars[name_str] = i

            log.info(f"Found {len(found_vars)} of {len(vars_to_find)} variables")
            log.info(f"Loading {len(cols_to_load)} columns")

            # Re-fetch and read only needed columns
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_csv(
                response['Body'],
                header=None,
                usecols=cols_to_load
            )

            # Store found variables for later reference
            df.attrs['found_vars'] = found_vars
            df.attrs['col_names'] = col_names

            log.info(f"Successfully loaded from: {key}")
            log.info(f"DataFrame shape: {df.shape}")
            return df

        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            log.warning(f"Error loading {key}: {e}")
            continue

    raise FileNotFoundError(f"Could not find CSV for scenario {scenario_id} in S3")


def load_scenario_csv_from_file(file_path: str, reservoir_codes: List[str]) -> pd.DataFrame:
    """Load scenario CSV from local file."""
    log.info(f"Loading from file: {file_path}")

    # First pass: read headers to find column indices
    with open(file_path, 'r') as f:
        header_df = pd.read_csv(f, header=None, nrows=8)

    col_names = header_df.iloc[1].tolist()

    # Build list of variable names to load
    vars_to_find = set()
    for code in reservoir_codes:
        vars_to_find.add(f'S_{code}')
        vars_to_find.add(f'C_{code}_FLOOD')

    # Find column indices
    cols_to_load = [0]  # DateTime
    found_vars = {}
    for i, name in enumerate(col_names):
        if i == 0:
            continue
        name_str = str(name).strip()
        if name_str in vars_to_find:
            cols_to_load.append(i)
            found_vars[name_str] = i

    log.info(f"Found {len(found_vars)} of {len(vars_to_find)} variables")

    # Second pass: load only needed columns
    df = pd.read_csv(file_path, header=None, usecols=cols_to_load)
    df.attrs['found_vars'] = found_vars
    df.attrs['col_names'] = col_names

    return df


def parse_scenario_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse the DSS-format CSV with header rows.

    The CSV format has:
    - Row 0 (a): Source identifier (CALSIM)
    - Row 1 (b): Variable names (S_SHSTA, C_SHSTA_FLOOD, etc.)
    - Row 2 (c): Description
    - Row 3 (e): Time step (1MON)
    - Row 4 (f): Dataset (L2020A)
    - Row 5 (type): Data type (PER-AVER)
    - Row 6 (units): Units (TAF, CFS, etc.)
    - Row 7+: DateTime + data values
    """
    header_rows = 7
    found_vars = df.attrs.get('found_vars', {})

    # Parse data portion
    data_df = df.iloc[header_rows:].copy()
    data_df.columns = range(len(data_df.columns))

    # First column is DateTime
    data_df.rename(columns={0: 'DateTime'}, inplace=True)
    data_df['DateTime'] = pd.to_datetime(data_df['DateTime'], errors='coerce')
    data_df = data_df.dropna(subset=['DateTime'])

    # Rename columns to variable names
    col_idx_to_name = {}
    for var_name, orig_idx in found_vars.items():
        # Find which position this column ended up in after usecols filtering
        for new_idx, col in enumerate(df.columns):
            if col == orig_idx:
                col_idx_to_name[new_idx] = var_name
                break

    for new_idx, var_name in col_idx_to_name.items():
        if new_idx in data_df.columns:
            data_df.rename(columns={new_idx: var_name}, inplace=True)
            data_df[var_name] = pd.to_numeric(data_df[var_name], errors='coerce')

    return data_df


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add water year and water month columns.

    Water year runs from October 1 to September 30.
    Water month: Oct=1, Nov=2, ..., Sep=12
    """
    df = df.copy()
    df['CalendarMonth'] = df['DateTime'].dt.month
    df['CalendarYear'] = df['DateTime'].dt.year

    # Water month: Oct(10)->1, Nov(11)->2, ..., Sep(9)->12
    df['WaterMonth'] = ((df['CalendarMonth'] - 10) % 12) + 1

    # Water year: Oct-Dec belong to next water year
    df['WaterYear'] = df['CalendarYear']
    df.loc[df['CalendarMonth'] >= 10, 'WaterYear'] += 1

    return df


def calculate_storage_monthly(
    df: pd.DataFrame,
    reservoir_code: str,
    capacity_taf: float
) -> List[Dict[str, Any]]:
    """
    Calculate monthly storage statistics for a single reservoir.

    Returns list of dicts (one per water month) for reservoir_storage_monthly table.
    """
    storage_col = f'S_{reservoir_code}'

    if storage_col not in df.columns:
        log.debug(f"Storage column {storage_col} not found")
        return []

    results = []

    for wm in range(1, 13):
        month_data = df[df['WaterMonth'] == wm][storage_col].dropna()

        if month_data.empty:
            continue

        # Convert to percent of capacity
        storage_pct = (month_data / capacity_taf) * 100

        # Calculate statistics
        mean_taf = float(month_data.mean())
        std_taf = float(month_data.std())
        cv = std_taf / mean_taf if mean_taf > 0 else 0

        row = {
            'water_month': wm,
            'storage_avg_taf': round(mean_taf, 2),
            'storage_cv': round(cv, 4),
            'storage_pct_capacity': round((mean_taf / capacity_taf) * 100, 2),
            'capacity_taf': capacity_taf,
            'sample_count': len(month_data),
        }

        # Add percentiles (as % of capacity)
        for p in STORAGE_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(storage_pct, p)), 2)

        results.append(row)

    return results


def calculate_spill_monthly(
    df: pd.DataFrame,
    reservoir_code: str,
    capacity_taf: float
) -> List[Dict[str, Any]]:
    """
    Calculate monthly spill (flood release) statistics for a single reservoir.

    Returns list of dicts (one per water month) for reservoir_spill_monthly table.
    """
    spill_col = f'C_{reservoir_code}_FLOOD'
    storage_col = f'S_{reservoir_code}'

    if spill_col not in df.columns:
        log.debug(f"Spill column {spill_col} not found")
        return []

    results = []

    for wm in range(1, 13):
        month_df = df[df['WaterMonth'] == wm].copy()
        spill_data = month_df[spill_col].dropna()

        if spill_data.empty:
            continue

        total_months = len(spill_data)
        spill_nonzero = spill_data[spill_data > 0]
        spill_count = len(spill_nonzero)

        row = {
            'water_month': wm,
            'spill_months_count': spill_count,
            'total_months': total_months,
            'spill_frequency_pct': round((spill_count / total_months) * 100, 2) if total_months > 0 else 0,
        }

        if spill_count > 0:
            row['spill_avg_cfs'] = round(float(spill_nonzero.mean()), 2)
            row['spill_max_cfs'] = round(float(spill_nonzero.max()), 2)
            row['spill_q50'] = round(float(np.percentile(spill_nonzero, 50)), 2)
            row['spill_q90'] = round(float(np.percentile(spill_nonzero, 90)), 2)
            row['spill_q100'] = round(float(spill_nonzero.max()), 2)

            # Storage at spill threshold
            if storage_col in month_df.columns:
                storage_at_spill = month_df[month_df[spill_col] > 0][storage_col].dropna()
                if not storage_at_spill.empty:
                    row['storage_at_spill_avg_pct'] = round(
                        (storage_at_spill.mean() / capacity_taf) * 100, 2
                    )
                else:
                    row['storage_at_spill_avg_pct'] = None
            else:
                row['storage_at_spill_avg_pct'] = None
        else:
            row['spill_avg_cfs'] = 0
            row['spill_max_cfs'] = 0
            row['spill_q50'] = 0
            row['spill_q90'] = 0
            row['spill_q100'] = 0
            row['storage_at_spill_avg_pct'] = None

        results.append(row)

    return results


def calculate_period_summary(
    df: pd.DataFrame,
    reservoir_code: str,
    capacity_taf: float,
    dead_pool_taf: float
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary statistics for a single reservoir.

    Returns dict for reservoir_period_summary table.
    """
    storage_col = f'S_{reservoir_code}'
    spill_col = f'C_{reservoir_code}_FLOOD'

    if storage_col not in df.columns:
        log.debug(f"Storage column {storage_col} not found")
        return None

    # Get water years
    water_years = sorted(df['WaterYear'].unique())
    if not water_years:
        return None

    result = {
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),
        'capacity_taf': capacity_taf,
    }

    # ========== Storage Exceedance ==========
    storage_data = df[storage_col].dropna()
    if not storage_data.empty:
        storage_pct = (storage_data / capacity_taf) * 100

        # Exceedance percentiles
        for p in EXCEEDANCE_PERCENTILES:
            result[f'storage_exc_p{p}'] = round(float(np.percentile(storage_pct, p)), 2)
    else:
        for p in EXCEEDANCE_PERCENTILES:
            result[f'storage_exc_p{p}'] = None

    # ========== Threshold Markers ==========
    result['dead_pool_taf'] = dead_pool_taf
    result['dead_pool_pct'] = round((dead_pool_taf / capacity_taf) * 100, 2) if capacity_taf > 0 else 0

    # ========== Spill Statistics ==========
    if spill_col in df.columns:
        spill_data = df[spill_col].dropna()

        # Annual max spill per water year
        annual_max_spill = df.groupby('WaterYear')[spill_col].max()
        spill_years = (annual_max_spill > 0).sum()

        result['spill_years_count'] = int(spill_years)
        result['spill_frequency_pct'] = round((spill_years / len(water_years)) * 100, 2)

        # All spill events (non-zero)
        all_spill = spill_data[spill_data > 0]
        if len(all_spill) > 0:
            result['spill_mean_cfs'] = round(float(all_spill.mean()), 2)
            result['spill_peak_cfs'] = round(float(all_spill.max()), 2)
        else:
            result['spill_mean_cfs'] = 0
            result['spill_peak_cfs'] = 0

        # Annual spill volume (TAF)
        annual_spill_taf = df.groupby('WaterYear')[spill_col].apply(
            lambda x: (x * CFS_TO_TAF_MONTHLY).sum()
        )

        result['annual_spill_avg_taf'] = round(float(annual_spill_taf.mean()), 2)
        if annual_spill_taf.mean() > 0:
            result['annual_spill_cv'] = round(
                float(annual_spill_taf.std() / annual_spill_taf.mean()), 4
            )
        else:
            result['annual_spill_cv'] = 0
        result['annual_spill_max_taf'] = round(float(annual_spill_taf.max()), 2)

        # Annual max spill distribution
        annual_max_nonzero = annual_max_spill[annual_max_spill > 0]
        if len(annual_max_nonzero) > 0:
            result['annual_max_spill_q50'] = round(float(np.percentile(annual_max_nonzero, 50)), 2)
            result['annual_max_spill_q90'] = round(float(np.percentile(annual_max_nonzero, 90)), 2)
            result['annual_max_spill_q100'] = round(float(annual_max_nonzero.max()), 2)
        else:
            result['annual_max_spill_q50'] = 0
            result['annual_max_spill_q90'] = 0
            result['annual_max_spill_q100'] = 0

        # Spill threshold (avg storage % when spill occurs)
        if storage_col in df.columns:
            storage_at_spill = df[df[spill_col] > 0][storage_col].dropna()
            if not storage_at_spill.empty:
                result['spill_threshold_pct'] = round(
                    (storage_at_spill.mean() / capacity_taf) * 100, 2
                )
            else:
                result['spill_threshold_pct'] = None
        else:
            result['spill_threshold_pct'] = None
    else:
        # No spill data available
        result['spill_years_count'] = 0
        result['spill_frequency_pct'] = 0
        result['spill_mean_cfs'] = 0
        result['spill_peak_cfs'] = 0
        result['annual_spill_avg_taf'] = 0
        result['annual_spill_cv'] = 0
        result['annual_spill_max_taf'] = 0
        result['annual_max_spill_q50'] = 0
        result['annual_max_spill_q90'] = 0
        result['annual_max_spill_q100'] = 0
        result['spill_threshold_pct'] = None

    return result


def calculate_all_statistics(
    scenario_id: str,
    reservoirs: Dict[str, Dict[str, Any]],
    csv_path: Optional[str] = None
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Calculate all statistics for all reservoirs for a scenario.

    Returns three lists:
    - storage_monthly_rows: List of dicts for reservoir_storage_monthly
    - spill_monthly_rows: List of dicts for reservoir_spill_monthly
    - period_summary_rows: List of dicts for reservoir_period_summary
    """
    log.info(f"Processing scenario: {scenario_id}")

    reservoir_codes = list(reservoirs.keys())

    # Load CSV
    if csv_path:
        raw_df = load_scenario_csv_from_file(csv_path, reservoir_codes)
    else:
        raw_df = load_scenario_csv_from_s3(scenario_id, reservoir_codes)

    # Parse CSV
    df = parse_scenario_csv(raw_df)

    # Add water year/month
    df = add_water_year_month(df)

    log.info(f"Data range: {df['DateTime'].min()} to {df['DateTime'].max()}")
    log.info(f"Total rows: {len(df)}")

    storage_monthly_rows = []
    spill_monthly_rows = []
    period_summary_rows = []

    for code, meta in reservoirs.items():
        entity_id = meta['id']
        capacity_taf = meta['capacity_taf']
        dead_pool_taf = meta['dead_pool_taf']

        if capacity_taf <= 0:
            log.warning(f"Skipping {code}: invalid capacity {capacity_taf}")
            continue

        # Calculate storage monthly
        storage_rows = calculate_storage_monthly(df, code, capacity_taf)
        for row in storage_rows:
            row['scenario_short_code'] = scenario_id
            row['reservoir_entity_id'] = entity_id
        storage_monthly_rows.extend(storage_rows)

        # Calculate spill monthly
        spill_rows = calculate_spill_monthly(df, code, capacity_taf)
        for row in spill_rows:
            row['scenario_short_code'] = scenario_id
            row['reservoir_entity_id'] = entity_id
        spill_monthly_rows.extend(spill_rows)

        # Calculate period summary
        summary = calculate_period_summary(df, code, capacity_taf, dead_pool_taf)
        if summary:
            summary['scenario_short_code'] = scenario_id
            summary['reservoir_entity_id'] = entity_id
            period_summary_rows.append(summary)

    log.info(f"Generated: {len(storage_monthly_rows)} storage monthly, "
             f"{len(spill_monthly_rows)} spill monthly, "
             f"{len(period_summary_rows)} period summary rows")

    return storage_monthly_rows, spill_monthly_rows, period_summary_rows


def generate_sql_inserts(
    storage_monthly: List[Dict],
    spill_monthly: List[Dict],
    period_summary: List[Dict]
) -> str:
    """Generate SQL INSERT statements for all three tables."""

    lines = [
        "-- Generated SQL for reservoir statistics tables",
        "-- Run after creating tables with 04, 05, 06 DDL scripts",
        f"-- Storage monthly rows: {len(storage_monthly)}",
        f"-- Spill monthly rows: {len(spill_monthly)}",
        f"-- Period summary rows: {len(period_summary)}",
        "",
        "BEGIN;",
        "",
    ]

    # ========== reservoir_storage_monthly ==========
    if storage_monthly:
        lines.append("-- reservoir_storage_monthly")
        lines.append("INSERT INTO reservoir_storage_monthly (")
        lines.append("    scenario_short_code, reservoir_entity_id, water_month,")
        lines.append("    storage_avg_taf, storage_cv, storage_pct_capacity,")
        lines.append("    q0, q10, q30, q50, q70, q90, q100,")
        lines.append("    capacity_taf, sample_count, created_by, updated_by")
        lines.append(") VALUES")

        value_rows = []
        for row in storage_monthly:
            value_row = (
                f"    ('{row['scenario_short_code']}', {row['reservoir_entity_id']}, {row['water_month']}, "
                f"{row['storage_avg_taf']}, {row['storage_cv']}, {row['storage_pct_capacity']}, "
                f"{row['q0']}, {row['q10']}, {row['q30']}, {row['q50']}, "
                f"{row['q70']}, {row['q90']}, {row['q100']}, "
                f"{row['capacity_taf']}, {row['sample_count']}, 1, 1)"
            )
            value_rows.append(value_row)

        lines.append(",\n".join(value_rows))
        lines.append("ON CONFLICT (scenario_short_code, reservoir_entity_id, water_month)")
        lines.append("DO UPDATE SET")
        lines.append("    storage_avg_taf = EXCLUDED.storage_avg_taf,")
        lines.append("    storage_cv = EXCLUDED.storage_cv,")
        lines.append("    storage_pct_capacity = EXCLUDED.storage_pct_capacity,")
        lines.append("    q0 = EXCLUDED.q0, q10 = EXCLUDED.q10, q30 = EXCLUDED.q30,")
        lines.append("    q50 = EXCLUDED.q50, q70 = EXCLUDED.q70, q90 = EXCLUDED.q90,")
        lines.append("    q100 = EXCLUDED.q100,")
        lines.append("    capacity_taf = EXCLUDED.capacity_taf,")
        lines.append("    sample_count = EXCLUDED.sample_count,")
        lines.append("    updated_at = NOW(), updated_by = 1;")
        lines.append("")

    # ========== reservoir_spill_monthly ==========
    if spill_monthly:
        lines.append("-- reservoir_spill_monthly")
        lines.append("INSERT INTO reservoir_spill_monthly (")
        lines.append("    scenario_short_code, reservoir_entity_id, water_month,")
        lines.append("    spill_months_count, total_months, spill_frequency_pct,")
        lines.append("    spill_avg_cfs, spill_max_cfs,")
        lines.append("    spill_q50, spill_q90, spill_q100,")
        lines.append("    storage_at_spill_avg_pct, created_by, updated_by")
        lines.append(") VALUES")

        value_rows = []
        for row in spill_monthly:
            storage_at_spill = row['storage_at_spill_avg_pct']
            storage_at_spill_sql = 'NULL' if storage_at_spill is None else str(storage_at_spill)

            value_row = (
                f"    ('{row['scenario_short_code']}', {row['reservoir_entity_id']}, {row['water_month']}, "
                f"{row['spill_months_count']}, {row['total_months']}, {row['spill_frequency_pct']}, "
                f"{row['spill_avg_cfs']}, {row['spill_max_cfs']}, "
                f"{row['spill_q50']}, {row['spill_q90']}, {row['spill_q100']}, "
                f"{storage_at_spill_sql}, 1, 1)"
            )
            value_rows.append(value_row)

        lines.append(",\n".join(value_rows))
        lines.append("ON CONFLICT (scenario_short_code, reservoir_entity_id, water_month)")
        lines.append("DO UPDATE SET")
        lines.append("    spill_months_count = EXCLUDED.spill_months_count,")
        lines.append("    total_months = EXCLUDED.total_months,")
        lines.append("    spill_frequency_pct = EXCLUDED.spill_frequency_pct,")
        lines.append("    spill_avg_cfs = EXCLUDED.spill_avg_cfs,")
        lines.append("    spill_max_cfs = EXCLUDED.spill_max_cfs,")
        lines.append("    spill_q50 = EXCLUDED.spill_q50,")
        lines.append("    spill_q90 = EXCLUDED.spill_q90,")
        lines.append("    spill_q100 = EXCLUDED.spill_q100,")
        lines.append("    storage_at_spill_avg_pct = EXCLUDED.storage_at_spill_avg_pct,")
        lines.append("    updated_at = NOW(), updated_by = 1;")
        lines.append("")

    # ========== reservoir_period_summary ==========
    if period_summary:
        lines.append("-- reservoir_period_summary")
        lines.append("INSERT INTO reservoir_period_summary (")
        lines.append("    scenario_short_code, reservoir_entity_id,")
        lines.append("    simulation_start_year, simulation_end_year, total_years,")
        lines.append("    storage_exc_p5, storage_exc_p10, storage_exc_p25, storage_exc_p50,")
        lines.append("    storage_exc_p75, storage_exc_p90, storage_exc_p95,")
        lines.append("    dead_pool_taf, dead_pool_pct, spill_threshold_pct,")
        lines.append("    spill_years_count, spill_frequency_pct,")
        lines.append("    spill_mean_cfs, spill_peak_cfs,")
        lines.append("    annual_spill_avg_taf, annual_spill_cv, annual_spill_max_taf,")
        lines.append("    annual_max_spill_q50, annual_max_spill_q90, annual_max_spill_q100,")
        lines.append("    capacity_taf, created_by, updated_by")
        lines.append(") VALUES")

        value_rows = []
        for row in period_summary:
            def sql_val(v):
                return 'NULL' if v is None else str(v)

            value_row = (
                f"    ('{row['scenario_short_code']}', {row['reservoir_entity_id']}, "
                f"{row['simulation_start_year']}, {row['simulation_end_year']}, {row['total_years']}, "
                f"{sql_val(row.get('storage_exc_p5'))}, {sql_val(row.get('storage_exc_p10'))}, "
                f"{sql_val(row.get('storage_exc_p25'))}, {sql_val(row.get('storage_exc_p50'))}, "
                f"{sql_val(row.get('storage_exc_p75'))}, {sql_val(row.get('storage_exc_p90'))}, "
                f"{sql_val(row.get('storage_exc_p95'))}, "
                f"{row['dead_pool_taf']}, {row['dead_pool_pct']}, {sql_val(row.get('spill_threshold_pct'))}, "
                f"{row['spill_years_count']}, {row['spill_frequency_pct']}, "
                f"{row['spill_mean_cfs']}, {row['spill_peak_cfs']}, "
                f"{row['annual_spill_avg_taf']}, {row['annual_spill_cv']}, {row['annual_spill_max_taf']}, "
                f"{row['annual_max_spill_q50']}, {row['annual_max_spill_q90']}, {row['annual_max_spill_q100']}, "
                f"{row['capacity_taf']}, 1, 1)"
            )
            value_rows.append(value_row)

        lines.append(",\n".join(value_rows))
        lines.append("ON CONFLICT (scenario_short_code, reservoir_entity_id)")
        lines.append("DO UPDATE SET")
        lines.append("    simulation_start_year = EXCLUDED.simulation_start_year,")
        lines.append("    simulation_end_year = EXCLUDED.simulation_end_year,")
        lines.append("    total_years = EXCLUDED.total_years,")
        lines.append("    storage_exc_p5 = EXCLUDED.storage_exc_p5,")
        lines.append("    storage_exc_p10 = EXCLUDED.storage_exc_p10,")
        lines.append("    storage_exc_p25 = EXCLUDED.storage_exc_p25,")
        lines.append("    storage_exc_p50 = EXCLUDED.storage_exc_p50,")
        lines.append("    storage_exc_p75 = EXCLUDED.storage_exc_p75,")
        lines.append("    storage_exc_p90 = EXCLUDED.storage_exc_p90,")
        lines.append("    storage_exc_p95 = EXCLUDED.storage_exc_p95,")
        lines.append("    dead_pool_taf = EXCLUDED.dead_pool_taf,")
        lines.append("    dead_pool_pct = EXCLUDED.dead_pool_pct,")
        lines.append("    spill_threshold_pct = EXCLUDED.spill_threshold_pct,")
        lines.append("    spill_years_count = EXCLUDED.spill_years_count,")
        lines.append("    spill_frequency_pct = EXCLUDED.spill_frequency_pct,")
        lines.append("    spill_mean_cfs = EXCLUDED.spill_mean_cfs,")
        lines.append("    spill_peak_cfs = EXCLUDED.spill_peak_cfs,")
        lines.append("    annual_spill_avg_taf = EXCLUDED.annual_spill_avg_taf,")
        lines.append("    annual_spill_cv = EXCLUDED.annual_spill_cv,")
        lines.append("    annual_spill_max_taf = EXCLUDED.annual_spill_max_taf,")
        lines.append("    annual_max_spill_q50 = EXCLUDED.annual_max_spill_q50,")
        lines.append("    annual_max_spill_q90 = EXCLUDED.annual_max_spill_q90,")
        lines.append("    annual_max_spill_q100 = EXCLUDED.annual_max_spill_q100,")
        lines.append("    capacity_taf = EXCLUDED.capacity_taf,")
        lines.append("    updated_at = NOW(), updated_by = 1;")
        lines.append("")

    lines.append("COMMIT;")
    lines.append("")
    lines.append(f"\\echo 'Loaded {len(storage_monthly)} storage monthly rows'")
    lines.append(f"\\echo 'Loaded {len(spill_monthly)} spill monthly rows'")
    lines.append(f"\\echo 'Loaded {len(period_summary)} period summary rows'")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Calculate comprehensive reservoir statistics for all 92 reservoirs'
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
        '--output-sql',
        help='Output SQL INSERT statements to specified path'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse CSV and show stats without generating output'
    )

    args = parser.parse_args()

    if not args.scenario and not args.all_scenarios:
        parser.error("Either --scenario or --all-scenarios is required")

    # Load reservoir metadata
    reservoir_csv = Path(args.reservoir_csv) if args.reservoir_csv else None
    reservoirs = load_reservoir_entities(reservoir_csv)

    scenarios_to_process = SCENARIOS if args.all_scenarios else [args.scenario]

    all_storage_monthly = []
    all_spill_monthly = []
    all_period_summary = []

    for scenario_id in scenarios_to_process:
        try:
            storage_monthly, spill_monthly, period_summary = calculate_all_statistics(
                scenario_id,
                reservoirs,
                csv_path=args.csv_path
            )

            all_storage_monthly.extend(storage_monthly)
            all_spill_monthly.extend(spill_monthly)
            all_period_summary.extend(period_summary)

        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise

    if args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")
        log.info(f"Total: {len(all_storage_monthly)} storage monthly, "
                 f"{len(all_spill_monthly)} spill monthly, "
                 f"{len(all_period_summary)} period summary rows")
        return

    if args.output_json:
        output = {
            'storage_monthly': all_storage_monthly,
            'spill_monthly': all_spill_monthly,
            'period_summary': all_period_summary,
        }
        print(json.dumps(output, indent=2))

    if args.output_sql:
        sql_content = generate_sql_inserts(
            all_storage_monthly,
            all_spill_monthly,
            all_period_summary
        )
        with open(args.output_sql, 'w') as f:
            f.write(sql_content)
        log.info(f"Saved SQL to {args.output_sql}")

    log.info("Total rows generated:")
    log.info(f"  Storage monthly: {len(all_storage_monthly)}")
    log.info(f"  Spill monthly: {len(all_spill_monthly)}")
    log.info(f"  Period summary: {len(all_period_summary)}")


if __name__ == '__main__':
    main()
