#!/usr/bin/env python3
"""
Calculate delivery, shortage, and reliability statistics for wildlife refuge demand units.

COEQWAL — Wildlife Refuge Water Delivery ETL
============================================

Computes four metrics for 18 refuge demand units:
  1. Monthly SW delivery statistics (TAF)
  2. Monthly delivery shortage statistics (TAF)
  3. Monthly delivery shortage statistics (% of demand)
  4. Period-of-record reliability (95th percentile of annual shortage %)

DATA SOURCES:
  - Demand: SV input CSV (AWO_{DU_ID}, TAF, APPLIED-WATER)
      S3: scenario/{id}/csv/{id}_coeqwal_sv_input.csv
      NOTE: DSS variable name is AWO_* (Applied Water Output). The staged CSV
      may expose these as either AWO_* or AW_*. This module handles both.

  - Delivery: CalSim DV output CSV (DN_{DU_ID}, CFS, SW-DELIVERY-NET)
      S3: scenario/{id}/csv/{id}_coeqwal_calsim_output.csv
      This file is always present for every scenario. DN_* columns are in CFS
      and must be converted to TAF before use (see CFS_TO_TAF_PER_DAY below).
      The script also falls back to {id}_DV.csv if the primary key is absent.

UNITS:
  - SV input:  AWO_* already in TAF — no conversion needed.
  - DV output: DN_* in CFS — conversion applied per row:
      TAF = CFS × CFS_TO_TAF_PER_DAY × days_in_month

SHORTAGE: Derived as max(demand - delivery, 0). No native CalSim shortage variable
exists for refuge demand units.

RELIABILITY: 95th percentile of annual shortage % across all simulated water years.
"In 95 of 100 years, annual shortage is at or below this value."

Usage:
    python calculate_refuge_statistics.py --scenario s0020
    python calculate_refuge_statistics.py --scenario s0020 --dry-run
    python calculate_refuge_statistics.py --all-scenarios
"""

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from units import CFS_TO_TAF_PER_DAY  # noqa: E402
from scenarios import SCENARIOS  # noqa: E402

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("refuge_statistics")

# ─── Constants ────────────────────────────────────────────────────────────────


S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

SV_INPUT_S3_KEY = "scenario/{scenario}/csv/{scenario}_coeqwal_sv_input.csv"
DV_OUTPUT_S3_KEYS = [
    "scenario/{scenario}/csv/{scenario}_coeqwal_calsim_output.csv",
    "scenario/{scenario}/csv/{scenario}_DV.csv",
]


PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DU_REFUGE_CSV = PROJECT_ROOT / "database/seed_tables/04_calsim_data/du_refuge_entity.csv"

DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# Threshold to filter CalSim floating-point artifacts (100 acre-feet)
SHORTAGE_THRESHOLD_TAF = 0.1

# Developer attribution — must match developer.id in the database
ETL_OPERATOR_ID = 2  # jfantauzza

# All 18 refuge demand unit IDs (from du_refuge_entity.csv)
REFUGE_DU_IDS = [
    '08N_PR1', '08N_PR2', '08S_PR', '09_PR', '11_PR',
    '17N_NR', '17N_PR', '17S_PR',
    '63_PR1', '63_PR2', '63_PR3',
    '72_PR1', '72_PR2', '72_PR3', '72_PR4', '72_PR5', '72_PR6',
    '91_PR',
]


# =============================================================================
# DATA LOADING
# =============================================================================

def load_refuge_demand_units(csv_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Load refuge demand unit metadata from du_refuge_entity.csv."""
    if csv_path is None:
        csv_path = DU_REFUGE_CSV

    if not csv_path.exists():
        log.warning(f"du_refuge_entity.csv not found at {csv_path}")
        return {}

    demand_units = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            du_id = row.get('DU_ID', '').strip()
            if du_id:
                demand_units[du_id] = {
                    'wba_id': row.get('WBA_ID', '').strip(),
                    'hydrologic_region': row.get('hydrologic_region', '').strip(),
                    'cs3_type': row.get('CS3_Type', '').strip(),
                    'refuge_or_wildlife_area': row.get('refuge_or_wildlife_area', '').strip(),
                    'managed_by': row.get('managed_by', '').strip(),
                    'provider': row.get('provider', '').strip(),
                    'sw': row.get('sw', '1').strip() == '1',
                    'has_gis_data': row.get('has_gis_data', 'True').strip() == 'True',
                }

    log.info(f"Loaded {len(demand_units)} refuge demand units from {csv_path}")
    return demand_units


def _read_csv_header(body) -> Tuple[List[str], List[str]]:
    """
    Read the 7 header rows of a CalSim DSS-export CSV.

    Returns (var_names, units) where both are lists indexed by column position.
    Row 1 (B) = variable names, Row 6 = units.
    """
    header_df = pd.read_csv(body, header=None, nrows=7, low_memory=False)
    var_names = [str(v) for v in header_df.iloc[1].tolist()]
    units = [str(u) for u in header_df.iloc[6].tolist()]
    return var_names, units


def _select_taf_columns(
    data_df: pd.DataFrame,
    var_names: List[str],
    units: List[str],
    prefix: str,
) -> pd.DataFrame:
    """
    From a full data DataFrame, extract TAF-block columns for variables matching `prefix`.

    The deliveries CSV and SV input both have a two-block layout: CFS columns then TAF
    columns. The Units row (row 6) labels each column as 'CFS' or 'TAF'. We select the
    TAF occurrence for each variable.

    Returns a DataFrame with the date column and one TAF column per matching variable.
    """
    date_col = var_names[0]

    # Build a dict: variable_name -> list of (col_index, units_label)
    seen: Dict[str, List[Tuple[int, str]]] = {}
    for i, (vname, unit) in enumerate(zip(var_names, units)):
        if vname.startswith(prefix):
            seen.setdefault(vname, []).append((i, unit))

    # Collect all series at once then concat — avoids fragmentation warnings
    series_list = [data_df.iloc[:, 0].rename(date_col)]
    for vname, occurrences in seen.items():
        # Prefer explicit TAF label; fall back to last occurrence
        taf_col = next(
            (idx for idx, unit in occurrences if unit.upper() == 'TAF'),
            occurrences[-1][0],
        )
        series_list.append(
            pd.to_numeric(data_df.iloc[:, taf_col], errors='coerce').rename(vname)
        )

    return pd.concat(series_list, axis=1)


def load_sv_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """
    Load AWO_{DU_ID} demand columns from SV input CSV (TAF, APPLIED-WATER).

    Handles both AWO_* (raw DSS naming) and AW_* (possible staging rename).
    Returns DataFrame with demand columns in TAF.
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 required. Install with: pip install boto3")

    s3 = boto3.client('s3')
    key = SV_INPUT_S3_KEY.format(scenario=scenario_id)

    log.info(f"Loading SV input: s3://{S3_BUCKET}/{key}")
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    var_names, units = _read_csv_header(response['Body'])

    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data_df = pd.read_csv(response['Body'], header=None, skiprows=7, low_memory=False)

    # Try AWO_* first (raw DSS naming), then AW_* (possible staging rename)
    result = _select_taf_columns(data_df, var_names, units, prefix='AWO_')
    if len(result.columns) <= 1:
        log.info("No AWO_* columns found — trying AW_* (staging may rename variables)")
        result = _select_taf_columns(data_df, var_names, units, prefix='AW_')

    # Normalize column names: strip AWO_ prefix to AW_ for consistent internal use
    renames = {col: col.replace('AWO_', 'AW_') for col in result.columns if col.startswith('AWO_')}
    if renames:
        result = result.rename(columns=renames)
        log.info(f"Renamed {len(renames)} AWO_* columns to AW_* for internal consistency")

    log.info(f"Loaded {len(result.columns) - 1} demand columns from SV input")
    return result


def load_sv_csv_from_file(file_path: str) -> pd.DataFrame:
    """Load SV input CSV from a local file path."""
    log.info(f"Loading SV input from file: {file_path}")
    header_df = pd.read_csv(file_path, header=None, nrows=7, low_memory=False)
    var_names = [str(v) for v in header_df.iloc[1].tolist()]
    units = [str(u) for u in header_df.iloc[6].tolist()]

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)

    result = _select_taf_columns(data_df, var_names, units, prefix='AWO_')
    if len(result.columns) <= 1:
        result = _select_taf_columns(data_df, var_names, units, prefix='AW_')

    renames = {col: col.replace('AWO_', 'AW_') for col in result.columns if col.startswith('AWO_')}
    if renames:
        result = result.rename(columns=renames)

    log.info(f"Loaded {len(result.columns) - 1} demand columns")
    return result


def _load_dv_dn_columns(var_names: List[str], data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the date column and all DN_* refuge delivery columns (CFS) from a
    parsed DV output DataFrame.

    The DV output is a single-block CFS file — no TAF block, no unit-row
    selection needed.  CFS→TAF conversion is applied later in the orchestrator
    once DaysInMonth is available.
    """
    date_col = var_names[0]
    result = pd.DataFrame()
    result[date_col] = data_df.iloc[:, 0]

    refuge_ids = set(REFUGE_DU_IDS)
    for i, vname in enumerate(var_names[1:], start=1):
        if vname.startswith('DN_') and vname[3:] in refuge_ids:
            result[vname] = pd.to_numeric(data_df.iloc[:, i], errors='coerce')

    log.info(f"Extracted {len(result.columns) - 1} DN_* columns (CFS) from DV output")
    return result


def load_dv_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """
    Load DN_{DU_ID} delivery columns (CFS) from the CalSim DV output on S3.

    Tries {scenario}_coeqwal_calsim_output.csv first, falls back to {scenario}_DV.csv.
    Returns a DataFrame with the date column and DN_* columns still in CFS units.
    CFS→TAF conversion must be applied after add_water_year_month() supplies DaysInMonth.
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 required. Install with: pip install boto3")

    s3 = boto3.client('s3')
    keys = [k.format(scenario=scenario_id) for k in DV_OUTPUT_S3_KEYS]

    for key in keys:
        try:
            log.info(f"Trying DV output: s3://{S3_BUCKET}/{key}")
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            header_df = pd.read_csv(response['Body'], header=None, nrows=7, low_memory=False)
            var_names = [str(v) for v in header_df.iloc[1].tolist()]

            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data_df = pd.read_csv(response['Body'], header=None, skiprows=7, low_memory=False)

            return _load_dv_dn_columns(var_names, data_df)

        except s3.exceptions.NoSuchKey:
            log.warning(f"Not found: s3://{S3_BUCKET}/{key}")
            continue
        except Exception as exc:
            log.warning(f"Error loading {key}: {exc}")
            continue

    raise FileNotFoundError(
        f"Could not find DV output for scenario {scenario_id} in s3://{S3_BUCKET}. "
        f"Tried: {keys}"
    )


def load_dv_csv_from_file(file_path: str) -> pd.DataFrame:
    """
    Load DN_{DU_ID} delivery columns (CFS) from a local DV output CSV.

    Returns DataFrame with DN_* columns in CFS.  CFS→TAF conversion must be
    applied after add_water_year_month() supplies DaysInMonth.
    """
    log.info(f"Loading DV output from file: {file_path}")
    header_df = pd.read_csv(file_path, header=None, nrows=7, low_memory=False)
    var_names = [str(v) for v in header_df.iloc[1].tolist()]

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    return _load_dv_dn_columns(var_names, data_df)


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add WaterYear, WaterMonth (1=Oct ... 12=Sep), and DaysInMonth columns.

    DSS date convention normalisation
    ----------------------------------
    CalSim DSS files use two different period conventions depending on the file
    type, both of which must be mapped to the same calendar month:

    - Period-ending (DV / calsim_output): the date stamp is the LAST day of the
      month the data represents.  Example: 1921-10-31 → October 1921 (WM=1).

    - Period-beginning (SV input): the date stamp is the FIRST day of the
      FOLLOWING month.  Example: 1920-11-01 → October 1920 (WM=1), not November.

    Detection: if the date's day-of-month == 1, subtract one day to get the
    actual data period before deriving CalendarMonth/Year/DaysInMonth.  Dates
    with day ≠ 1 (end-of-month) are already correct and are used as-is.

    This normalisation ensures that October is always WaterMonth=1 regardless of
    which source file is being processed, and that the WaterYear+WaterMonth merge
    between SV and DV DataFrames produces the expected row count.
    """
    df = df.copy()
    first_col = df.columns[0]
    df['DateTime'] = pd.to_datetime(df[first_col], errors='coerce')

    # Shift period-beginning dates (day == 1) back by one day so that the
    # calendar month/year reflect the actual data period, not the label period.
    period_date = df['DateTime'].where(
        df['DateTime'].dt.day != 1,
        df['DateTime'] - pd.Timedelta(days=1),
    )

    df['CalendarMonth'] = period_date.dt.month
    df['CalendarYear'] = period_date.dt.year
    df['DaysInMonth'] = period_date.dt.daysinmonth
    df['WaterMonth'] = ((df['CalendarMonth'] - 10) % 12) + 1
    df['WaterYear'] = df['CalendarYear']
    df.loc[df['CalendarMonth'] >= 10, 'WaterYear'] += 1

    log.info(
        f"Date range: {df['DateTime'].min().date()} to {df['DateTime'].max().date()} "
        f"({df['WaterYear'].nunique()} water years)"
    )
    return df


# =============================================================================
# CALCULATIONS
# =============================================================================

def _safe_cv(data: pd.Series) -> float:
    """Coefficient of variation, returning 0 when mean is zero."""
    mean = float(data.mean())
    if mean == 0:
        return 0.0
    return round(float(data.std() / mean), 4)


def _percentile_row(data: pd.Series, prefix: str = '') -> Dict[str, float]:
    """Compute percentile bands and exceedance percentiles for a series."""
    row: Dict[str, float] = {}
    arr = data.dropna().values
    if len(arr) == 0:
        return row
    for p in DELIVERY_PERCENTILES:
        row[f'{prefix}q{p}'] = round(float(np.percentile(arr, p)), 2)
    for p in EXCEEDANCE_PERCENTILES:
        # exc_pX = value exceeded X% of the time = (100-X)th percentile
        row[f'{prefix}exc_p{p}'] = round(float(np.percentile(arr, 100 - p)), 2)
    return row


def calculate_delivery_monthly(
    df: pd.DataFrame,
    du_id: str,
) -> List[Dict[str, Any]]:
    """
    Monthly delivery statistics for one refuge demand unit.

    Returns one row per water month (1–12).
    """
    delivery_var = f"DN_{du_id}"
    if delivery_var not in df.columns:
        log.debug(f"No delivery variable for {du_id}: {delivery_var} not in DataFrame")
        return []

    results = []
    for wm in range(1, 13):
        month_data = pd.to_numeric(
            df.loc[df['WaterMonth'] == wm, delivery_var], errors='coerce'
        ).dropna()

        if month_data.empty:
            continue

        row: Dict[str, Any] = {
            'du_id': du_id,
            'water_month': wm,
            'delivery_avg_taf': round(float(month_data.mean()), 2),
            'delivery_cv': _safe_cv(month_data),
            'sample_count': len(month_data),
        }
        row.update(_percentile_row(month_data))
        results.append(row)

    return results


def calculate_shortage_monthly(
    df: pd.DataFrame,
    du_id: str,
) -> List[Dict[str, Any]]:
    """
    Monthly shortage statistics for one refuge demand unit.

    Shortage = max(demand - delivery, 0). Both values are in TAF.
    Returns one row per water month.
    """
    demand_var = f"AW_{du_id}"
    delivery_var = f"DN_{du_id}"

    if demand_var not in df.columns:
        log.debug(f"No demand variable for {du_id}: {demand_var}")
        return []
    if delivery_var not in df.columns:
        log.debug(f"No delivery variable for {du_id}: {delivery_var}")
        return []

    df_work = df.copy()
    df_work['demand'] = pd.to_numeric(df_work[demand_var], errors='coerce')
    df_work['delivery'] = pd.to_numeric(df_work[delivery_var], errors='coerce')
    df_work['shortage_taf'] = (df_work['demand'] - df_work['delivery']).clip(lower=0)
    df_work['shortage_pct'] = np.where(
        df_work['demand'] > 0,
        (df_work['shortage_taf'] / df_work['demand']) * 100,
        0.0,
    )

    results = []
    for wm in range(1, 13):
        mask = df_work['WaterMonth'] == wm
        s_taf = df_work.loc[mask, 'shortage_taf'].dropna()
        s_pct = df_work.loc[mask, 'shortage_pct'].dropna()

        if s_taf.empty:
            continue

        frequency = float((s_taf > SHORTAGE_THRESHOLD_TAF).sum()) / len(s_taf) * 100

        row: Dict[str, Any] = {
            'du_id': du_id,
            'water_month': wm,
            'shortage_avg_taf': round(float(s_taf.mean()), 2),
            'shortage_cv': _safe_cv(s_taf),
            'shortage_pct_avg': round(float(s_pct.mean()), 4),
            'shortage_pct_cv': _safe_cv(s_pct),
            'shortage_frequency_pct': round(frequency, 4),
            'sample_count': len(s_taf),
        }
        row.update(_percentile_row(s_taf))
        results.append(row)

    return results


def calculate_period_summary(
    df: pd.DataFrame,
    du_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Period-of-record summary for one refuge demand unit.

    Includes annual delivery/shortage stats and reliability_pct_95.
    """
    demand_var = f"AW_{du_id}"
    delivery_var = f"DN_{du_id}"

    if demand_var not in df.columns or delivery_var not in df.columns:
        log.debug(f"Missing demand or delivery variable for {du_id}")
        return None

    df_work = df.copy()
    df_work['demand'] = pd.to_numeric(df_work[demand_var], errors='coerce')
    df_work['delivery'] = pd.to_numeric(df_work[delivery_var], errors='coerce')
    df_work['shortage_taf'] = (df_work['demand'] - df_work['delivery']).clip(lower=0)

    # Annual aggregates (sum over water year)
    annual_delivery = df_work.groupby('WaterYear')['delivery'].sum()
    annual_demand = df_work.groupby('WaterYear')['demand'].sum()
    annual_shortage = df_work.groupby('WaterYear')['shortage_taf'].sum()

    annual_shortage_pct = np.where(
        annual_demand > 0,
        (annual_shortage / annual_demand) * 100,
        0.0,
    )

    water_years = sorted(df_work['WaterYear'].dropna().unique())

    result: Dict[str, Any] = {
        'du_id': du_id,
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),

        # Delivery
        'annual_delivery_avg_taf': round(float(annual_delivery.mean()), 2),
        'annual_delivery_cv': _safe_cv(annual_delivery),

        # Shortage (TAF)
        'annual_shortage_avg_taf': round(float(annual_shortage.mean()), 2),
        'annual_shortage_cv': _safe_cv(annual_shortage),

        # Shortage (%)
        'annual_shortage_pct_avg': round(float(np.mean(annual_shortage_pct)), 4),
        'annual_shortage_pct_cv': round(
            float(np.std(annual_shortage_pct) / np.mean(annual_shortage_pct))
            if np.mean(annual_shortage_pct) > 0 else 0.0, 4
        ),

        # Reliability: 95th percentile of annual shortage %
        # "In 95 of 100 years, shortage <= this value"
        'reliability_pct_95': round(float(np.percentile(annual_shortage_pct, 95)), 4),
    }

    # Annual delivery exceedance curve
    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, 100 - p)), 2)

    return result


# =============================================================================
# ORCHESTRATION
# =============================================================================

def calculate_all_refuge_statistics(
    scenario_id: str,
    sv_path: Optional[str] = None,
    dv_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load source files and calculate all refuge statistics for one scenario.

    Returns dict with keys: delivery_monthly, shortage_monthly, period_summary.

    sv_path:  optional local path to the SV input CSV (overrides S3).
    dv_path:  optional local path to the CalSim DV output CSV (overrides S3).
              DN_* columns in the DV file are in CFS; conversion to TAF is
              applied here using DaysInMonth derived from the date column.
    """
    log.info(f"=== Processing scenario: {scenario_id} ===")

    # Load source data
    if sv_path:
        sv_df = load_sv_csv_from_file(sv_path)
    else:
        sv_df = load_sv_csv_from_s3(scenario_id)

    if dv_path:
        dv_df = load_dv_csv_from_file(dv_path)
    else:
        dv_df = load_dv_csv_from_s3(scenario_id)

    # Add time columns to both DataFrames
    sv_df = add_water_year_month(sv_df)
    dv_df = add_water_year_month(dv_df)

    # Convert DN_* columns from CFS to TAF using per-row DaysInMonth
    dn_cols = [c for c in dv_df.columns if c.startswith('DN_')]
    if not dn_cols:
        log.warning(f"No DN_* columns found in DV output for scenario {scenario_id}")
    else:
        dv_df[dn_cols] = dv_df[dn_cols].multiply(
            dv_df['DaysInMonth'] * CFS_TO_TAF_PER_DAY, axis=0
        )
        log.info(f"Converted {len(dn_cols)} DN_* columns from CFS to TAF")

    # Merge SV (demand) and DV (delivery) on WaterYear + WaterMonth.
    # The SV file uses start-of-month dates and the DV file uses end-of-month
    # dates, so joining on the raw date string produces 0 rows. Joining on the
    # derived water-year calendar (WaterYear, WaterMonth) is date-format agnostic.
    aux_cols = ['DateTime', 'CalendarMonth', 'CalendarYear', 'DaysInMonth']
    merged = pd.merge(
        sv_df.drop(columns=aux_cols, errors='ignore'),
        dv_df.drop(columns=aux_cols + [dv_df.columns[0]], errors='ignore'),
        on=['WaterYear', 'WaterMonth'],
        how='inner',
    )
    log.info(f"Merged DataFrame: {len(merged)} rows, {len(merged.columns)} columns")

    demand_units = load_refuge_demand_units()

    delivery_monthly_rows: List[Dict[str, Any]] = []
    shortage_monthly_rows: List[Dict[str, Any]] = []
    period_summary_rows: List[Dict[str, Any]] = []

    for du_id in REFUGE_DU_IDS:
        if du_id not in demand_units:
            log.warning(f"DU {du_id} not found in du_refuge_entity.csv — skipping")
            continue

        # Monthly delivery
        rows = calculate_delivery_monthly(merged, du_id)
        for r in rows:
            r['scenario_short_code'] = scenario_id
            r['created_by'] = ETL_OPERATOR_ID
            r['updated_by'] = ETL_OPERATOR_ID
        delivery_monthly_rows.extend(rows)

        # Monthly shortage
        rows = calculate_shortage_monthly(merged, du_id)
        for r in rows:
            r['scenario_short_code'] = scenario_id
            r['created_by'] = ETL_OPERATOR_ID
            r['updated_by'] = ETL_OPERATOR_ID
        shortage_monthly_rows.extend(rows)

        # Period summary
        summary = calculate_period_summary(merged, du_id)
        if summary:
            summary['scenario_short_code'] = scenario_id
            summary['created_by'] = ETL_OPERATOR_ID
            summary['updated_by'] = ETL_OPERATOR_ID
            period_summary_rows.append(summary)

    log.info(
        f"Scenario {scenario_id}: "
        f"{len(delivery_monthly_rows)} delivery-monthly rows, "
        f"{len(shortage_monthly_rows)} shortage-monthly rows, "
        f"{len(period_summary_rows)} period-summary rows"
    )

    return {
        'delivery_monthly': delivery_monthly_rows,
        'shortage_monthly': shortage_monthly_rows,
        'period_summary': period_summary_rows,
    }


# =============================================================================
# DATABASE WRITE
# =============================================================================

DELIVERY_MONTHLY_COLS = [
    'scenario_short_code', 'du_id', 'water_month',
    'delivery_avg_taf', 'delivery_cv',
    'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
    'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
    'sample_count', 'created_by', 'updated_by',
]

SHORTAGE_MONTHLY_COLS = [
    'scenario_short_code', 'du_id', 'water_month',
    'shortage_avg_taf', 'shortage_cv',
    'shortage_pct_avg', 'shortage_pct_cv',
    'shortage_frequency_pct',
    'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
    'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
    'sample_count', 'created_by', 'updated_by',
]

PERIOD_SUMMARY_COLS = [
    'scenario_short_code', 'du_id',
    'simulation_start_year', 'simulation_end_year', 'total_years',
    'annual_delivery_avg_taf', 'annual_delivery_cv',
    'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25',
    'delivery_exc_p50', 'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
    'annual_shortage_avg_taf', 'annual_shortage_cv',
    'annual_shortage_pct_avg', 'annual_shortage_pct_cv',
    'reliability_pct_95',
    'created_by', 'updated_by',
]


def _rows_to_tuples(rows: List[Dict], columns: List[str]) -> List[tuple]:
    return [tuple(row.get(col) for col in columns) for row in rows]


def save_to_database(scenario_id: str, stats: Dict[str, Any], db_url: str) -> None:
    """
    Write all statistics for one scenario to the database.

    Uses DELETE + bulk INSERT (not upsert) to ensure clean replacement.
    created_by/updated_by are set explicitly by the ETL (ETL_OPERATOR_ID = 2).
    """
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 required. Install with: pip install psycopg2-binary")

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            for table in [
                'refuge_du_delivery_monthly',
                'refuge_du_shortage_monthly',
                'refuge_du_period_summary',
            ]:
                cur.execute(
                    f"DELETE FROM {table} WHERE scenario_short_code = %s",
                    (scenario_id,),
                )
                log.info(f"Deleted existing rows for {scenario_id} from {table}")

            # Insert delivery monthly
            del_rows = _rows_to_tuples(stats['delivery_monthly'], DELIVERY_MONTHLY_COLS)
            if del_rows:
                execute_values(
                    cur,
                    f"INSERT INTO refuge_du_delivery_monthly ({', '.join(DELIVERY_MONTHLY_COLS)}) VALUES %s",
                    del_rows,
                )
                log.info(f"Inserted {len(del_rows)} rows into refuge_du_delivery_monthly")

            # Insert shortage monthly
            sht_rows = _rows_to_tuples(stats['shortage_monthly'], SHORTAGE_MONTHLY_COLS)
            if sht_rows:
                execute_values(
                    cur,
                    f"INSERT INTO refuge_du_shortage_monthly ({', '.join(SHORTAGE_MONTHLY_COLS)}) VALUES %s",
                    sht_rows,
                )
                log.info(f"Inserted {len(sht_rows)} rows into refuge_du_shortage_monthly")

            # Insert period summary
            ps_rows = _rows_to_tuples(stats['period_summary'], PERIOD_SUMMARY_COLS)
            if ps_rows:
                execute_values(
                    cur,
                    f"INSERT INTO refuge_du_period_summary ({', '.join(PERIOD_SUMMARY_COLS)}) VALUES %s",
                    ps_rows,
                )
                log.info(f"Inserted {len(ps_rows)} rows into refuge_du_period_summary")

        conn.commit()
        log.info(f"Successfully committed statistics for scenario {scenario_id}")
    except Exception as e:
        conn.rollback()
        log.error(f"Database write failed for {scenario_id}: {e}")
        raise
    finally:
        conn.close()


# =============================================================================
# CLI
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calculate wildlife refuge delivery statistics from CalSim outputs"
    )
    parser.add_argument('--scenario', help='Single scenario ID (e.g. s0020)')
    parser.add_argument('--all-scenarios', action='store_true',
                        help=f'Process all known scenarios: {SCENARIOS}')
    parser.add_argument('--sv-path', help='Local path to SV input CSV (overrides S3)')
    parser.add_argument('--dv-path', help='Local path to CalSim DV output CSV (overrides S3)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Calculate statistics without writing to database')
    parser.add_argument('--output-json', action='store_true',
                        help='Print results as JSON (implies --dry-run)')
    args = parser.parse_args()

    if args.output_json:
        args.dry_run = True

    db_url = os.getenv('DATABASE_URL')
    if not args.dry_run and not db_url:
        parser.error("DATABASE_URL environment variable required unless --dry-run is set")

    scenarios = SCENARIOS if args.all_scenarios else [args.scenario] if args.scenario else []
    if not scenarios:
        parser.error("Provide --scenario SCENARIO_ID or --all-scenarios")

    for scenario_id in scenarios:
        try:
            stats = calculate_all_refuge_statistics(
                scenario_id,
                sv_path=args.sv_path,
                dv_path=args.dv_path,
            )

            if args.output_json:
                print(json.dumps(stats, indent=2, default=str))
                continue

            if args.dry_run:
                log.info(
                    f"[DRY RUN] {scenario_id}: "
                    f"delivery_monthly={len(stats['delivery_monthly'])}, "
                    f"shortage_monthly={len(stats['shortage_monthly'])}, "
                    f"period_summary={len(stats['period_summary'])}"
                )
            else:
                save_to_database(scenario_id, stats, db_url)

        except Exception as e:
            log.error(f"Failed to process scenario {scenario_id}: {e}")
            raise


if __name__ == "__main__":
    main()
