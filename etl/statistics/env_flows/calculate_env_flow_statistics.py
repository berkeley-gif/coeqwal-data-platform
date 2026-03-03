#!/usr/bin/env python3
"""
Calculate environmental river flow statistics for 60 CalSim channel reaches.

COEQWAL — Environmental River Flow ETL
=======================================

Computes three metrics per reach per scenario:

  Metric 1 — Monthly % of natural unimpaired flow
    pct_unimpaired = C_{reach}[CFS] / UNIMP_{watershed}[CFS] × 100
    Output: one row per (reach, scenario, water_month) → env_flow_channel_monthly
    Coverage: 58 of 60 channels (MOK019, MOK028 excluded — no UNIMP variable)

  Metric 2 — Seasonal % of functional flow (EFLOWS) target
    pct_ff = C_{reach}[CFS] / EFLOWS_{reach}[CFS] × 100
    Aggregated by CEFF 5-season calendar, per water year
    Output: one row per (reach, scenario, CEFF season) → env_flow_channel_seasonal
    Coverage: ~17 channels with has_eflows = true

  Metric 3 — Flow alteration index (period of record)
    pearson_r = Pearson correlation between C_{reach} and UNIMP_{watershed} monthly series
    Also: mif_met_pct (% months where C >= MIF), avg_pct_unimpaired, avg_pct_ff
    Output: one row per (reach, scenario) → env_flow_channel_period_summary
    Coverage: same 58 channels as Metric 1 (+ MIF/EFLOWS subsets)

DATA SOURCES:
  DV (CalSim output):
    S3: scenario/{id}/csv/{id}_coeqwal_calsim_output.csv
    Variables: C_{reach} (CHANNEL, CFS), C_{reach}_MIF (FLOW-MIN-INSTREAM, CFS)
    Note: C_SAC122 appears twice — first occurrence used.

  SV (CalSim input):
    S3: scenario/{id}/csv/{id}_coeqwal_sv_input.csv
    Variables: UNIMP_{watershed} (FLOW-UNIMPAIRED, CFS), EFLOWS_{reach} (FLOW-MIN-EFLOW, CFS)
    Note: Do not use UNIMP_*_UHH variants.

SEASON DEFINITIONS (CEFF — California Environmental Flows Framework):
  wet_peak        (id=1): WY months 3,4,5     = Dec, Jan, Feb
  wet_base        (id=2): WY months 6,7       = Mar, Apr
  spring_recession(id=3): WY months 8,9       = May, Jun
  dry             (id=4): WY months 10,11,12,1= Jul, Aug, Sep, Oct*
  fall_pulse      (id=5): WY month  2         = Nov
  * October (WY month 1) belongs to the DRY season of the preceding water year.

Usage:
    python calculate_env_flow_statistics.py --scenario s0020
    python calculate_env_flow_statistics.py --scenario s0020 --dry-run
    python calculate_env_flow_statistics.py --scenario s0020 --dv-path /tmp/s0020_DV.csv --sv-path /tmp/s0020_SV.csv
    python calculate_env_flow_statistics.py --all-scenarios
"""

import argparse
import csv
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

try:
    from scipy.stats import pearsonr
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

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
log = logging.getLogger("env_flow_statistics")

# ─── Unit conversion ─────────────────────────────────────────────────────────
# 1 CFS for 1 day = 86 400 cubic feet
# 1 TAF = 43 560 × 1000 cubic feet = 43 560 000 cubic feet
# => TAF/month = CFS × days_in_month × 86400 / 43_560_000
CFS_PER_DAY_TO_TAF = 86_400 / 43_560_000  # ≈ 0.0019835 TAF per CFS-day

# ─── Constants ────────────────────────────────────────────────────────────────

SCENARIOS = [
    's0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0026', 's0027',
    's0028', 's0029', 's0030', 's0031', 's0032', 's0033', 's0039', 's0040',
    's0041', 's0042', 's0044',
]

S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

SV_INPUT_S3_KEY = "scenario/{scenario}/csv/{scenario}_coeqwal_sv_input.csv"
DV_OUTPUT_S3_KEYS = [
    "scenario/{scenario}/csv/{scenario}_coeqwal_calsim_output.csv",
    "scenario/{scenario}/csv/{scenario}_DV.csv",
]

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
CHANNEL_ENTITY_CSV = PROJECT_ROOT / "database/seed_tables/04_calsim_data/channel_entity.csv"

ETL_OPERATOR_ID = 2  # jfantauzza

DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# ─── CEFF season definitions ──────────────────────────────────────────────────
# season_id values match the sort_order seed in env_flow_season (migration 24).
# wy_months are water year month numbers (Oct=1, Nov=2, ..., Sep=12).

CEFF_SEASONS: Dict[str, Dict] = {
    'wet_peak':         {'id': 1, 'wy_months': {3, 4, 5}},
    'wet_base':         {'id': 2, 'wy_months': {6, 7}},
    'spring_recession': {'id': 3, 'wy_months': {8, 9}},
    'dry':              {'id': 4, 'wy_months': {10, 11, 12, 1}},  # Oct spans WY boundary
    'fall_pulse':       {'id': 5, 'wy_months': {2}},
}

# Map WY month → season short_code
WY_MONTH_TO_SEASON: Dict[int, str] = {
    wm: season
    for season, info in CEFF_SEASONS.items()
    for wm in info['wy_months']
}


# =============================================================================
# CHANNEL ENTITY LOADING
# =============================================================================

def load_channel_entities(csv_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """
    Load the 60 environmental flow channels from channel_entity.csv.

    Returns only rows where channel_class is populated (the 60 DV channels).
    Each record contains: network_arc_id, unimp_sv_variable, has_mif, has_eflows,
    channel_class.
    """
    if csv_path is None:
        csv_path = CHANNEL_ENTITY_CSV

    channels = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            channel_class = row.get('channel_class', '').strip()
            if not channel_class:
                continue

            network_arc_id = row.get('network_arc_id', '').strip()
            if not network_arc_id:
                continue

            unimp = row.get('unimp_sv_variable', '').strip() or None
            has_mif = row.get('has_mif', 'false').strip().lower() in ('true', 't', '1')
            has_eflows = row.get('has_eflows', 'false').strip().lower() in ('true', 't', '1')

            # EFLOWS variable name: EFLOWS_{reach_code}, where reach_code strips the C_ prefix
            reach_code = network_arc_id[2:] if network_arc_id.startswith('C_') else network_arc_id
            eflows_var = f"EFLOWS_{reach_code}" if has_eflows else None
            mif_var = f"{network_arc_id}_MIF" if has_mif else None

            channels.append({
                'network_arc_id': network_arc_id,
                'unimp_sv_variable': unimp,
                'has_mif': has_mif,
                'has_eflows': has_eflows,
                'eflows_var': eflows_var,
                'mif_var': mif_var,
                'channel_class': channel_class,
            })

    log.info(f"Loaded {len(channels)} env-flow channels from {csv_path.name}")
    mif_count = sum(1 for c in channels if c['has_mif'])
    eflow_count = sum(1 for c in channels if c['has_eflows'])
    log.info(f"  {mif_count} with MIF, {eflow_count} with EFLOWS")
    return channels


# =============================================================================
# DATA LOADING
# =============================================================================

def _read_dv_header(body) -> Tuple[List[str], List[str]]:
    """
    Read the 7 header rows of a CalSim DSS-export DV CSV.

    Returns (var_names, part_c) indexed by column position.
    Row index 1 = Part B (variable names), Row index 2 = Part C (CHANNEL, etc.).
    """
    header_df = pd.read_csv(body, header=None, nrows=7, low_memory=False)
    var_names = [str(v) for v in header_df.iloc[1].tolist()]
    part_c    = [str(p) for p in header_df.iloc[2].tolist()]
    return var_names, part_c


def _read_sv_header(body) -> Tuple[List[str], List[str]]:
    """
    Read the 7 header rows of a CalSim DSS-export SV CSV.

    Returns (var_names, units) indexed by column position.
    Row index 1 = Part B (variable names), Row index 6 = units (CFS/TAF).
    """
    header_df = pd.read_csv(body, header=None, nrows=7, low_memory=False)
    var_names = [str(v) for v in header_df.iloc[1].tolist()]
    units     = [str(u) for u in header_df.iloc[6].tolist()]
    return var_names, units


def _extract_dv_columns(
    data_df: pd.DataFrame,
    var_names: List[str],
    part_c: List[str],
    target_ids: Set[str],
) -> pd.DataFrame:
    """
    Extract date + target DV columns (CFS) from a parsed DV DataFrame.

    Handles duplicate variable names (e.g. C_SAC122) by using the first
    occurrence only (as specified in the env-flow README).

    target_ids: set of Part B variable names to extract (exact match).
    """
    date_col = var_names[0]
    seen: Set[str] = set()
    series_list = [data_df.iloc[:, 0].rename(date_col)]

    for i, (vname, pc) in enumerate(zip(var_names[1:], part_c[1:]), start=1):
        if vname in target_ids and vname not in seen:
            series_list.append(
                pd.to_numeric(data_df.iloc[:, i], errors='coerce').rename(vname)
            )
            seen.add(vname)

    found = len(series_list) - 1
    missing = target_ids - seen
    if missing:
        log.debug(f"DV: {len(missing)} target variables not found: {sorted(missing)[:10]}...")
    log.info(f"Extracted {found} channel-flow columns (CFS) from DV output")
    return pd.concat(series_list, axis=1)


def _extract_sv_cfs_columns(
    data_df: pd.DataFrame,
    var_names: List[str],
    units: List[str],
    target_ids: Set[str],
) -> pd.DataFrame:
    """
    Extract date + target SV columns at CFS precision.

    The SV file has a two-block layout (CFS block, then TAF block). For
    env-flow ratio metrics (pct_unimpaired, pct_ff) we want the CFS values
    so that units cancel with the CFS DV channel flows.

    Falls back to the first occurrence if no column is explicitly labeled CFS.
    """
    date_col = var_names[0]
    seen: Dict[str, List[Tuple[int, str]]] = {}
    for i, (vname, unit) in enumerate(zip(var_names[1:], units[1:]), start=1):
        if vname in target_ids:
            seen.setdefault(vname, []).append((i, unit))

    series_list = [data_df.iloc[:, 0].rename(date_col)]
    for vname, occurrences in seen.items():
        # Prefer CFS; fall back to first occurrence
        cfs_col = next(
            (idx for idx, unit in occurrences if unit.upper() == 'CFS'),
            occurrences[0][0],
        )
        series_list.append(
            pd.to_numeric(data_df.iloc[:, cfs_col], errors='coerce').rename(vname)
        )

    found = len(series_list) - 1
    missing = target_ids - set(seen.keys())
    if missing:
        log.debug(f"SV: {len(missing)} target variables not found: {sorted(missing)[:10]}...")
    log.info(f"Extracted {found} SV columns (CFS) from SV input")
    return pd.concat(series_list, axis=1)


def _load_dv_from_body(body, dv_target_ids: Set[str]) -> pd.DataFrame:
    """Parse a DV CSV body (file-like or S3 response Body) into a DataFrame."""
    var_names, part_c = _read_dv_header(body)
    # Re-read body (S3 streams are consumed on first read)
    return var_names, part_c


def load_dv_csv_from_s3(scenario_id: str, channels: List[Dict]) -> pd.DataFrame:
    """Load C_* and C_*_MIF channel-flow columns (CFS) from DV output on S3."""
    if not HAS_BOTO3:
        raise ImportError("boto3 required")

    dv_ids, _ = _build_target_ids(channels)
    s3 = boto3.client('s3')
    keys = [k.format(scenario=scenario_id) for k in DV_OUTPUT_S3_KEYS]

    for key in keys:
        try:
            log.info(f"Loading DV: s3://{S3_BUCKET}/{key}")
            resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
            var_names, part_c = _read_dv_header(resp['Body'])

            resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data_df = pd.read_csv(resp['Body'], header=None, skiprows=7, low_memory=False)

            return _extract_dv_columns(data_df, var_names, part_c, dv_ids)

        except Exception as exc:
            log.warning(f"Could not load {key}: {exc}")
            continue

    raise FileNotFoundError(
        f"DV output not found for {scenario_id} in s3://{S3_BUCKET}. Tried: {keys}"
    )


def load_dv_csv_from_file(file_path: str, channels: List[Dict]) -> pd.DataFrame:
    """Load C_* and C_*_MIF channel-flow columns (CFS) from a local DV CSV."""
    dv_ids, _ = _build_target_ids(channels)
    log.info(f"Loading DV from file: {file_path}")
    header_df = pd.read_csv(file_path, header=None, nrows=7, low_memory=False)
    var_names = [str(v) for v in header_df.iloc[1].tolist()]
    part_c    = [str(p) for p in header_df.iloc[2].tolist()]
    data_df   = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    return _extract_dv_columns(data_df, var_names, part_c, dv_ids)


def load_sv_csv_from_s3(scenario_id: str, channels: List[Dict]) -> pd.DataFrame:
    """Load UNIMP_* and EFLOWS_* columns (CFS) from SV input on S3."""
    if not HAS_BOTO3:
        raise ImportError("boto3 required")

    _, sv_ids = _build_target_ids(channels)
    s3 = boto3.client('s3')
    key = SV_INPUT_S3_KEY.format(scenario=scenario_id)

    log.info(f"Loading SV: s3://{S3_BUCKET}/{key}")
    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
    var_names, units = _read_sv_header(resp['Body'])

    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data_df = pd.read_csv(resp['Body'], header=None, skiprows=7, low_memory=False)

    return _extract_sv_cfs_columns(data_df, var_names, units, sv_ids)


def load_sv_csv_from_file(file_path: str, channels: List[Dict]) -> pd.DataFrame:
    """Load UNIMP_* and EFLOWS_* columns (CFS) from a local SV CSV."""
    _, sv_ids = _build_target_ids(channels)
    log.info(f"Loading SV from file: {file_path}")
    header_df = pd.read_csv(file_path, header=None, nrows=7, low_memory=False)
    var_names = [str(v) for v in header_df.iloc[1].tolist()]
    units     = [str(u) for u in header_df.iloc[6].tolist()]
    data_df   = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    return _extract_sv_cfs_columns(data_df, var_names, units, sv_ids)


def _build_target_ids(channels: List[Dict]) -> Tuple[Set[str], Set[str]]:
    """
    Build the sets of variable names to extract from DV and SV.

    dv_ids: C_{reach} + C_{reach}_MIF (where has_mif=True)
    sv_ids: unique UNIMP_{watershed} + EFLOWS_{reach} (where has_eflows=True)
    """
    dv_ids: Set[str] = set()
    sv_ids: Set[str] = set()

    for ch in channels:
        dv_ids.add(ch['network_arc_id'])
        if ch['mif_var']:
            dv_ids.add(ch['mif_var'])
        if ch['unimp_sv_variable']:
            sv_ids.add(ch['unimp_sv_variable'])
        if ch['eflows_var']:
            sv_ids.add(ch['eflows_var'])

    log.info(f"Target IDs — DV: {len(dv_ids)}, SV: {len(sv_ids)}")
    return dv_ids, sv_ids


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add WaterYear, WaterMonth (1=Oct ... 12=Sep), and DaysInMonth columns.

    Handles both period-ending dates (DV: last day of month) and
    period-beginning dates (SV: first day of following month) by normalising
    to the actual data month — see full explanation in calculate_refuge_statistics.py.
    """
    df = df.copy()
    first_col = df.columns[0]
    df['DateTime'] = pd.to_datetime(df[first_col], errors='coerce')

    period_date = df['DateTime'].where(
        df['DateTime'].dt.day != 1,
        df['DateTime'] - pd.Timedelta(days=1),
    )

    df['CalendarMonth'] = period_date.dt.month
    df['CalendarYear']  = period_date.dt.year
    df['DaysInMonth']   = period_date.dt.daysinmonth
    df['WaterMonth']    = ((df['CalendarMonth'] - 10) % 12) + 1
    df['WaterYear']     = df['CalendarYear']
    df.loc[df['CalendarMonth'] >= 10, 'WaterYear'] += 1

    log.info(
        f"Date range: {df['DateTime'].min().date()} to {df['DateTime'].max().date()} "
        f"({df['WaterYear'].nunique()} water years)"
    )
    return df


# =============================================================================
# CALCULATIONS — SHARED HELPERS
# =============================================================================

def _safe_cv(data: pd.Series) -> Optional[float]:
    """Coefficient of variation; None when mean is zero or data is empty."""
    arr = data.dropna().values
    if len(arr) == 0:
        return None
    mean = float(np.mean(arr))
    if mean == 0:
        return 0.0
    return round(float(np.std(arr) / mean), 4)


def _round_or_none(value, ndigits: int = 3) -> Optional[float]:
    if value is None or np.isnan(value):
        return None
    return round(float(value), ndigits)


def _percentile_stats(data: pd.Series) -> Dict[str, Optional[float]]:
    """Percentile bands and exceedance percentiles for a series."""
    row: Dict[str, Optional[float]] = {}
    arr = data.dropna().values
    if len(arr) == 0:
        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = None
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = None
        return row
    for p in DELIVERY_PERCENTILES:
        row[f'q{p}'] = round(float(np.percentile(arr, p)), 3)
    for p in EXCEEDANCE_PERCENTILES:
        row[f'exc_p{p}'] = round(float(np.percentile(arr, 100 - p)), 3)
    return row


# =============================================================================
# CALCULATIONS — METRIC 1: Monthly % unimpaired
# =============================================================================

def _flow_volume_stats(
    flow_cfs: pd.Series,
    days_in_month: pd.Series,
) -> Dict[str, Optional[float]]:
    """
    Compute flow-volume percentile statistics in both CFS and TAF.

    flow_cfs       — per-year monthly mean flow (CFS), already dropna'd, one value per year
    days_in_month  — days in that calendar month for the matching rows (same index as flow_cfs)

    Returns a flat dict with keys:
        flow_avg_taf,
        flow_q{p}_cfs / flow_exc_p{p}_cfs  (p in DELIVERY_PERCENTILES / EXCEEDANCE_PERCENTILES)
        flow_q{p}_taf / flow_exc_p{p}_taf
    """
    out: Dict[str, Optional[float]] = {}

    if flow_cfs.empty:
        out['flow_avg_taf'] = None
        for p in DELIVERY_PERCENTILES:
            out[f'flow_q{p}_cfs'] = None
            out[f'flow_q{p}_taf'] = None
        for p in EXCEEDANCE_PERCENTILES:
            out[f'flow_exc_p{p}_cfs'] = None
            out[f'flow_exc_p{p}_taf'] = None
        return out

    arr_cfs = flow_cfs.values.astype(float)

    # TAF per month for each year: CFS × actual_days × CFS_PER_DAY_TO_TAF
    aligned_days = days_in_month.reindex(flow_cfs.index).fillna(30.4375)
    arr_taf = arr_cfs * aligned_days.values * CFS_PER_DAY_TO_TAF

    out['flow_avg_taf'] = round(float(np.mean(arr_taf)), 3)

    for p in DELIVERY_PERCENTILES:
        out[f'flow_q{p}_cfs'] = round(float(np.percentile(arr_cfs, p)), 3)
        out[f'flow_q{p}_taf'] = round(float(np.percentile(arr_taf, p)), 3)
    for p in EXCEEDANCE_PERCENTILES:
        out[f'flow_exc_p{p}_cfs'] = round(float(np.percentile(arr_cfs, 100 - p)), 3)
        out[f'flow_exc_p{p}_taf'] = round(float(np.percentile(arr_taf, 100 - p)), 3)

    return out


def calculate_monthly_statistics(
    df: pd.DataFrame,
    network_arc_id: str,
    unimp_sv_variable: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Monthly flow-volume and % unimpaired statistics for one channel reach.

    For each water month 1–12, across all simulated years, computes:

      Flow volume (all channels):
        flow_avg_cfs, flow_cv  — mean CFS and coefficient of variation
        flow_avg_taf           — mean TAF/month (CFS × actual_days × CFS_PER_DAY_TO_TAF)
        flow_q{p}_cfs, flow_q{p}_taf, flow_exc_p{p}_cfs, flow_exc_p{p}_taf
          — percentile bands and exceedance percentiles of per-year monthly flow

      % unimpaired (channels with unimp_sv_variable only):
        pct_unimpaired = C_{reach}[CFS] / UNIMP_{watershed}[CFS] × 100
        pct_unimpaired_avg, pct_unimpaired_cv
        q{p}, exc_p{p}  — percentile bands of pct_unimpaired
        NaN where UNIMP == 0 (divide-by-zero guard).

    Returns one row per water month (up to 12 rows).
    """
    if network_arc_id not in df.columns:
        log.debug(f"Monthly: {network_arc_id} not in DataFrame — skipped")
        return []

    has_unimp = unimp_sv_variable and unimp_sv_variable in df.columns
    results = []

    for wm in range(1, 13):
        mask = df['WaterMonth'] == wm
        month_df = df.loc[mask]
        flow = pd.to_numeric(month_df[network_arc_id], errors='coerce').dropna()

        if flow.empty:
            continue

        # DaysInMonth for CFS→TAF conversion (same index as flow after dropna)
        days = month_df['DaysInMonth'].reindex(flow.index)

        row: Dict[str, Any] = {
            'network_arc_id':    network_arc_id,
            'water_month':       wm,
            'flow_avg_cfs':      _round_or_none(flow.mean()),
            'flow_cv':           _safe_cv(flow),
            'unimp_avg_cfs':     None,
            'pct_unimpaired_avg': None,
            'pct_unimpaired_cv':  None,
            'sample_count':      int(len(flow)),
        }

        # Flow-volume percentile bands (CFS + TAF)
        row.update(_flow_volume_stats(flow, days))

        if has_unimp:
            unimp = pd.to_numeric(month_df[unimp_sv_variable], errors='coerce')
            aligned_flow  = flow.reindex(unimp.index)
            aligned_unimp = unimp.reindex(flow.index)

            row['unimp_avg_cfs'] = _round_or_none(aligned_unimp.dropna().mean())

            # pct_unimpaired per year; guard against zero denominator
            with np.errstate(divide='ignore', invalid='ignore'):
                pct_vals = np.where(
                    aligned_unimp.values > 0,
                    (aligned_flow.values / aligned_unimp.values) * 100,
                    np.nan,
                )
            pct = pd.Series(pct_vals, index=flow.index).dropna()

            if not pct.empty:
                row['pct_unimpaired_avg'] = _round_or_none(pct.mean())
                row['pct_unimpaired_cv']  = _safe_cv(pct)
                row.update(_percentile_stats(pct))
            else:
                for p in DELIVERY_PERCENTILES:
                    row[f'q{p}'] = None
                for p in EXCEEDANCE_PERCENTILES:
                    row[f'exc_p{p}'] = None
        else:
            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = None
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = None

        results.append(row)

    return results


# =============================================================================
# CALCULATIONS — METRIC 2: Seasonal statistics
# (raw flow volume + % unimpaired + % functional flows)
# =============================================================================

def calculate_seasonal_statistics(
    df: pd.DataFrame,
    network_arc_id: str,
    unimp_sv_variable: Optional[str] = None,
    eflows_var: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Seasonal statistics for one channel reach, aggregated by CEFF 5-season calendar.

    Always computed (all 60 channels):
      - flow_avg_cfs, flow_cv, flow percentile bands
        Per-year seasonal mean of C_{reach} CFS, distributed across years.

    Computed where unimp_sv_variable is available (58 channels):
      - unimp_avg_cfs
        Mean of UNIMP_{watershed} seasonal averages — natural flow reference.
      - pct_unimpaired_avg, pct_unimpaired_cv, unimp percentile bands
        Metric 1 (seasonal): (C_{reach} / UNIMP) × 100, distributed across years.

    Computed where eflows_var is available (~17 channels, has_eflows=true):
      - pct_ff_avg, pct_ff_cv, deviation_avg, target_met_pct, pct_ff percentile bands
        Metric 2: (C_{reach} / EFLOWS) × 100, distributed across years.

    Dry season handling: October (WY month 1) belongs to the DRY season of the
    PRECEDING water year (WaterYear - 1), keeping the dry season intact.

    Returns up to 5 rows (one per CEFF season). Returns [] if C_{reach} not present.
    """
    if network_arc_id not in df.columns:
        log.debug(f"Seasonal: {network_arc_id} not in DataFrame — skipped")
        return []

    has_unimp = unimp_sv_variable and unimp_sv_variable in df.columns
    has_ef    = eflows_var and eflows_var in df.columns

    # Build working dataframe with only the columns we need
    cols_to_use = ['WaterYear', 'WaterMonth', network_arc_id]
    if has_unimp:
        cols_to_use.append(unimp_sv_variable)
    if has_ef:
        cols_to_use.append(eflows_var)

    work = df[cols_to_use].copy()
    work['flow'] = pd.to_numeric(work[network_arc_id], errors='coerce')

    if has_unimp:
        work['unimp'] = pd.to_numeric(work[unimp_sv_variable], errors='coerce')
        with np.errstate(divide='ignore', invalid='ignore'):
            work['pct_unimpaired'] = np.where(
                work['unimp'] > 0,
                (work['flow'] / work['unimp']) * 100,
                np.nan,
            )

    if has_ef:
        work['eflows'] = pd.to_numeric(work[eflows_var], errors='coerce')
        with np.errstate(divide='ignore', invalid='ignore'):
            work['pct_ff'] = np.where(
                work['eflows'] > 0,
                (work['flow'] / work['eflows']) * 100,
                np.nan,
            )

    # Assign season and season_year (dry season October belongs to WY-1)
    work['season'] = work['WaterMonth'].map(WY_MONTH_TO_SEASON)
    work['season_year'] = work['WaterYear']
    dry_october_mask = (work['season'] == 'dry') & (work['WaterMonth'] == 1)
    work.loc[dry_october_mask, 'season_year'] = work.loc[dry_october_mask, 'WaterYear'] - 1

    results = []

    for season_code, season_info in CEFF_SEASONS.items():
        season_mask = work['season'] == season_code
        season_data = work[season_mask]

        flow_data = season_data['flow'].dropna()
        if flow_data.empty:
            continue

        # ── Per-year seasonal means ─────────────────────────────────────
        # Compute mean within each year for this season, then distribute across years
        flow_by_year = season_data.groupby('season_year')['flow'].mean().dropna()
        if flow_by_year.empty:
            continue

        row: Dict[str, Any] = {
            'network_arc_id': network_arc_id,
            'season_id':      season_info['id'],
            'sample_count':   int(len(flow_by_year)),

            # Raw flow (CFS) distribution
            'flow_avg_cfs':   _round_or_none(flow_by_year.mean()),
            'flow_cv':        _safe_cv(flow_by_year),
        }

        # Flow percentile bands (prefixed)
        for p in DELIVERY_PERCENTILES:
            row[f'flow_q{p}'] = round(float(np.percentile(flow_by_year, p)), 3)
        for p in EXCEEDANCE_PERCENTILES:
            row[f'flow_exc_p{p}'] = round(float(np.percentile(flow_by_year, 100 - p)), 3)

        # ── Unimpaired reference + % unimpaired ─────────────────────────
        row['unimp_avg_cfs']        = None
        row['pct_unimpaired_avg']   = None
        row['pct_unimpaired_cv']    = None
        for p in DELIVERY_PERCENTILES:
            row[f'unimp_q{p}'] = None
        for p in EXCEEDANCE_PERCENTILES:
            row[f'unimp_exc_p{p}'] = None

        if has_unimp:
            unimp_by_year = season_data.groupby('season_year')['unimp'].mean().dropna()
            row['unimp_avg_cfs'] = _round_or_none(unimp_by_year.mean())

            pct_u_by_year = season_data.groupby('season_year')['pct_unimpaired'].mean().dropna()
            if not pct_u_by_year.empty:
                row['pct_unimpaired_avg'] = _round_or_none(pct_u_by_year.mean())
                row['pct_unimpaired_cv']  = _safe_cv(pct_u_by_year)
                for p in DELIVERY_PERCENTILES:
                    row[f'unimp_q{p}'] = round(float(np.percentile(pct_u_by_year, p)), 3)
                for p in EXCEEDANCE_PERCENTILES:
                    row[f'unimp_exc_p{p}'] = round(float(np.percentile(pct_u_by_year, 100 - p)), 3)

        # ── % Functional flows ───────────────────────────────────────────
        row['pct_ff_avg']      = None
        row['pct_ff_cv']       = None
        row['deviation_avg']   = None
        row['target_met_pct']  = None
        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = None
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = None

        if has_ef:
            pct_ff_by_year = season_data.groupby('season_year')['pct_ff'].mean().dropna()
            if not pct_ff_by_year.empty:
                arr = pct_ff_by_year.values
                row['pct_ff_avg']     = _round_or_none(np.mean(arr))
                row['pct_ff_cv']      = _safe_cv(pct_ff_by_year)
                row['deviation_avg']  = _round_or_none(np.mean(arr) - 100.0)
                row['target_met_pct'] = round(float((arr >= 100.0).sum() / len(arr) * 100), 2)
                pcts = _percentile_stats(pct_ff_by_year)
                row.update(pcts)

        results.append(row)

    return results


# =============================================================================
# CALCULATIONS — METRIC 3: Period-of-record flow alteration index
# =============================================================================

def calculate_period_summary(
    df: pd.DataFrame,
    network_arc_id: str,
    unimp_sv_variable: Optional[str],
    mif_var: Optional[str],
    eflows_var: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Period-of-record summary for one channel reach.

    Includes:
      - Pearson r between C_{reach} and UNIMP monthly series (full record)
      - mif_met_pct: % of months where C_{reach} >= C_{reach}_MIF
      - avg_pct_unimpaired, annual_cv_pct_unimpaired
      - avg_pct_ff, annual_cv_pct_ff (if EFLOWS variable available)

    r ≈ +1: simulated flow closely tracks natural seasonal timing
    r ≈  0: flow substantially altered by reservoir operations
    """
    if network_arc_id not in df.columns:
        log.debug(f"Period: {network_arc_id} not in DataFrame — skipped")
        return None

    flow = pd.to_numeric(df[network_arc_id], errors='coerce')
    water_years = df['WaterYear']

    result: Dict[str, Any] = {
        'network_arc_id':            network_arc_id,
        'simulation_start_year':     int(water_years.dropna().min()),
        'simulation_end_year':       int(water_years.dropna().max()),
        'total_months':              int(flow.dropna().count()),
        'pearson_r':                 None,
        'p_value':                   None,
        'avg_pct_unimpaired':        None,
        'annual_cv_pct_unimpaired':  None,
        'avg_pct_ff':                None,
        'annual_cv_pct_ff':          None,
        'mif_met_pct':               None,
    }

    has_unimp = unimp_sv_variable and unimp_sv_variable in df.columns
    has_mif   = mif_var and mif_var in df.columns
    has_ef    = eflows_var and eflows_var in df.columns

    # ── Metric 3: Pearson r ────────────────────────────────────────────────
    if has_unimp and HAS_SCIPY:
        unimp = pd.to_numeric(df[unimp_sv_variable], errors='coerce')
        valid = (~flow.isna()) & (~unimp.isna()) & (unimp > 0)
        if valid.sum() >= 10:
            r, p = pearsonr(flow[valid].values, unimp[valid].values)
            result['pearson_r'] = _round_or_none(r, 4)
            result['p_value']   = _round_or_none(p, 6)
    elif not HAS_SCIPY:
        log.warning("scipy not installed — Pearson r skipped. pip install scipy")

    # ── % Unimpaired: avg and annual CV ───────────────────────────────────
    if has_unimp:
        unimp = pd.to_numeric(df[unimp_sv_variable], errors='coerce')
        with np.errstate(divide='ignore', invalid='ignore'):
            pct = pd.Series(
                np.where(unimp > 0, (flow / unimp) * 100, np.nan),
                index=df.index,
            )
        result['avg_pct_unimpaired'] = _round_or_none(pct.dropna().mean())

        # Annual mean pct_unimpaired per year
        annual_mean = (
            pd.DataFrame({'WaterYear': water_years, 'pct': pct})
            .groupby('WaterYear')['pct'].mean()
        )
        result['annual_cv_pct_unimpaired'] = _safe_cv(annual_mean.dropna())

    # ── % Functional flows: avg and annual CV ─────────────────────────────
    if has_ef:
        eflows = pd.to_numeric(df[eflows_var], errors='coerce')
        pct_ff = pd.Series(
            np.where(eflows > 0, (flow / eflows) * 100, np.nan),
            index=df.index,
        )
        result['avg_pct_ff'] = _round_or_none(pct_ff.dropna().mean())

        annual_ff = (
            pd.DataFrame({'WaterYear': water_years, 'pct': pct_ff})
            .groupby('WaterYear')['pct'].mean()
        )
        result['annual_cv_pct_ff'] = _safe_cv(annual_ff.dropna())

    # ── MIF compliance ────────────────────────────────────────────────────
    if has_mif:
        mif = pd.to_numeric(df[mif_var], errors='coerce')
        valid_mif = (~flow.isna()) & (~mif.isna())
        if valid_mif.sum() > 0:
            mif_met = (flow[valid_mif] >= mif[valid_mif]).sum()
            result['mif_met_pct'] = round(float(mif_met / valid_mif.sum() * 100), 2)

    return result


# =============================================================================
# ORCHESTRATION
# =============================================================================

def calculate_all_env_flow_statistics(
    scenario_id: str,
    dv_path: Optional[str] = None,
    sv_path: Optional[str] = None,
    channels: Optional[List[Dict]] = None,
) -> Dict[str, Any]:
    """
    Load DV and SV source files and calculate all three env-flow metrics
    for all 60 channels in one scenario.

    Returns dict with keys: monthly, seasonal, period_summary.
    """
    log.info(f"=== Processing scenario: {scenario_id} ===")

    if channels is None:
        channels = load_channel_entities()

    # Load source data
    if dv_path:
        dv_df = load_dv_csv_from_file(dv_path, channels)
    else:
        dv_df = load_dv_csv_from_s3(scenario_id, channels)

    if sv_path:
        sv_df = load_sv_csv_from_file(sv_path, channels)
    else:
        sv_df = load_sv_csv_from_s3(scenario_id, channels)

    # Add time columns to both DataFrames (handles period-ending vs period-beginning dates)
    dv_df = add_water_year_month(dv_df)
    sv_df = add_water_year_month(sv_df)

    # Merge on WaterYear + WaterMonth (date-format agnostic)
    aux_cols = ['DateTime', 'CalendarMonth', 'CalendarYear', 'DaysInMonth']
    merged = pd.merge(
        dv_df.drop(columns=aux_cols, errors='ignore'),
        sv_df.drop(columns=aux_cols + [sv_df.columns[0]], errors='ignore'),
        on=['WaterYear', 'WaterMonth'],
        how='inner',
    )
    log.info(f"Merged DataFrame: {len(merged)} rows, {len(merged.columns)} columns")

    monthly_rows:      List[Dict[str, Any]] = []
    seasonal_rows:     List[Dict[str, Any]] = []
    period_rows:       List[Dict[str, Any]] = []

    for ch in channels:
        arc = ch['network_arc_id']

        # ── Metric 1: monthly statistics ──────────────────────────────────
        rows = calculate_monthly_statistics(merged, arc, ch['unimp_sv_variable'])
        for r in rows:
            r['scenario_short_code'] = scenario_id
            r['created_by'] = ETL_OPERATOR_ID
            r['updated_by'] = ETL_OPERATOR_ID
        monthly_rows.extend(rows)

        # ── Metric 2: seasonal statistics — all channels ──────────────────
        # Computes flow volume + % unimpaired for all 60 channels.
        # Adds pct_ff columns for the ~17 EFLOWS channels.
        rows = calculate_seasonal_statistics(
            merged, arc,
            unimp_sv_variable=ch['unimp_sv_variable'],
            eflows_var=ch['eflows_var'] if ch['has_eflows'] else None,
        )
        for r in rows:
            r['scenario_short_code'] = scenario_id
            r['created_by'] = ETL_OPERATOR_ID
            r['updated_by'] = ETL_OPERATOR_ID
        seasonal_rows.extend(rows)

        # ── Metric 3: period summary ───────────────────────────────────────
        summary = calculate_period_summary(
            merged, arc,
            ch['unimp_sv_variable'],
            ch['mif_var'] if ch['has_mif'] else None,
            ch['eflows_var'] if ch['has_eflows'] else None,
        )
        if summary:
            summary['scenario_short_code'] = scenario_id
            summary['created_by'] = ETL_OPERATOR_ID
            summary['updated_by'] = ETL_OPERATOR_ID
            period_rows.append(summary)

    log.info(
        f"Scenario {scenario_id}: "
        f"{len(monthly_rows)} monthly rows, "
        f"{len(seasonal_rows)} seasonal rows, "
        f"{len(period_rows)} period-summary rows"
    )

    return {
        'monthly':        monthly_rows,
        'seasonal':       seasonal_rows,
        'period_summary': period_rows,
    }


# =============================================================================
# DATABASE WRITE
# =============================================================================

MONTHLY_COLS = [
    'network_arc_id', 'scenario_short_code', 'water_month',

    # Raw flow — mean CFS and CV
    'flow_avg_cfs', 'flow_cv',
    # Raw flow — mean TAF/month
    'flow_avg_taf',
    # Raw flow — percentile bands CFS (added migration 28)
    'flow_q0_cfs', 'flow_q10_cfs', 'flow_q30_cfs', 'flow_q50_cfs',
    'flow_q70_cfs', 'flow_q90_cfs', 'flow_q100_cfs',
    # Raw flow — exceedance percentiles CFS
    'flow_exc_p5_cfs', 'flow_exc_p10_cfs', 'flow_exc_p25_cfs', 'flow_exc_p50_cfs',
    'flow_exc_p75_cfs', 'flow_exc_p90_cfs', 'flow_exc_p95_cfs',
    # Raw flow — percentile bands TAF (added migration 28)
    'flow_q0_taf', 'flow_q10_taf', 'flow_q30_taf', 'flow_q50_taf',
    'flow_q70_taf', 'flow_q90_taf', 'flow_q100_taf',
    # Raw flow — exceedance percentiles TAF
    'flow_exc_p5_taf', 'flow_exc_p10_taf', 'flow_exc_p25_taf', 'flow_exc_p50_taf',
    'flow_exc_p75_taf', 'flow_exc_p90_taf', 'flow_exc_p95_taf',

    # Unimpaired reference
    'unimp_avg_cfs',
    # % unimpaired
    'pct_unimpaired_avg', 'pct_unimpaired_cv',
    'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
    'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',

    'sample_count', 'created_by', 'updated_by',
]

SEASONAL_COLS = [
    'network_arc_id', 'scenario_short_code', 'season_id',

    # Raw flow volume (CFS) — all 60 channels
    'flow_avg_cfs', 'flow_cv',
    'flow_q0', 'flow_q10', 'flow_q30', 'flow_q50', 'flow_q70', 'flow_q90', 'flow_q100',
    'flow_exc_p5', 'flow_exc_p10', 'flow_exc_p25', 'flow_exc_p50',
    'flow_exc_p75', 'flow_exc_p90', 'flow_exc_p95',

    # Natural flow reference and % unimpaired — 58 channels
    'unimp_avg_cfs',
    'pct_unimpaired_avg', 'pct_unimpaired_cv',
    'unimp_q0', 'unimp_q10', 'unimp_q30', 'unimp_q50',
    'unimp_q70', 'unimp_q90', 'unimp_q100',
    'unimp_exc_p5', 'unimp_exc_p10', 'unimp_exc_p25', 'unimp_exc_p50',
    'unimp_exc_p75', 'unimp_exc_p90', 'unimp_exc_p95',

    # % Functional flows — ~17 EFLOWS channels
    'pct_ff_avg', 'pct_ff_cv',
    'deviation_avg',
    'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
    'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
    'target_met_pct',

    'sample_count',
    'created_by', 'updated_by',
]

PERIOD_COLS = [
    'network_arc_id', 'scenario_short_code',
    'simulation_start_year', 'simulation_end_year', 'total_months',
    'pearson_r', 'p_value',
    'avg_pct_unimpaired', 'annual_cv_pct_unimpaired',
    'avg_pct_ff', 'annual_cv_pct_ff',
    'mif_met_pct',
    'created_by', 'updated_by',
]


def _rows_to_tuples(rows: List[Dict], columns: List[str]) -> List[tuple]:
    return [tuple(row.get(col) for col in columns) for row in rows]


def save_to_database(scenario_id: str, stats: Dict[str, Any], db_url: str) -> None:
    """
    Write all env-flow statistics for one scenario to the database.

    Uses DELETE + bulk INSERT (not upsert) for clean per-scenario replacement.
    """
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 required. pip install psycopg2-binary")

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            for table in [
                'env_flow_channel_monthly',
                'env_flow_channel_seasonal',
                'env_flow_channel_period_summary',
            ]:
                cur.execute(
                    f"DELETE FROM {table} WHERE scenario_short_code = %s",
                    (scenario_id,),
                )
                log.info(f"Deleted existing rows for {scenario_id} from {table}")

            # Monthly
            mon_rows = _rows_to_tuples(stats['monthly'], MONTHLY_COLS)
            if mon_rows:
                execute_values(
                    cur,
                    f"INSERT INTO env_flow_channel_monthly ({', '.join(MONTHLY_COLS)}) VALUES %s",
                    mon_rows,
                )
                log.info(f"Inserted {len(mon_rows)} rows → env_flow_channel_monthly")

            # Seasonal
            sea_rows = _rows_to_tuples(stats['seasonal'], SEASONAL_COLS)
            if sea_rows:
                execute_values(
                    cur,
                    f"INSERT INTO env_flow_channel_seasonal ({', '.join(SEASONAL_COLS)}) VALUES %s",
                    sea_rows,
                )
                log.info(f"Inserted {len(sea_rows)} rows → env_flow_channel_seasonal")

            # Period summary
            per_rows = _rows_to_tuples(stats['period_summary'], PERIOD_COLS)
            if per_rows:
                execute_values(
                    cur,
                    f"INSERT INTO env_flow_channel_period_summary ({', '.join(PERIOD_COLS)}) VALUES %s",
                    per_rows,
                )
                log.info(f"Inserted {len(per_rows)} rows → env_flow_channel_period_summary")

        conn.commit()
        log.info(f"Committed statistics for scenario {scenario_id}")
    except Exception as exc:
        conn.rollback()
        log.error(f"Database write failed for {scenario_id}: {exc}")
        raise
    finally:
        conn.close()


# =============================================================================
# CLI
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calculate environmental river flow statistics from CalSim outputs"
    )
    parser.add_argument('--scenario', help='Single scenario ID (e.g. s0020)')
    parser.add_argument('--all-scenarios', action='store_true',
                        help=f'Process all known scenarios: {SCENARIOS}')
    parser.add_argument('--dv-path', help='Local path to DV output CSV (overrides S3)')
    parser.add_argument('--sv-path', help='Local path to SV input CSV (overrides S3)')
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

    # Load channel entities once, shared across all scenarios
    channels = load_channel_entities()

    for scenario_id in scenarios:
        try:
            stats = calculate_all_env_flow_statistics(
                scenario_id,
                dv_path=args.dv_path,
                sv_path=args.sv_path,
                channels=channels,
            )

            if args.output_json:
                print(json.dumps(stats, indent=2, default=str))
                continue

            if args.dry_run:
                log.info(
                    f"[DRY RUN] {scenario_id}: "
                    f"monthly={len(stats['monthly'])}, "
                    f"seasonal={len(stats['seasonal'])}, "
                    f"period_summary={len(stats['period_summary'])}"
                )
            else:
                save_to_database(scenario_id, stats, db_url)

        except Exception as exc:
            log.error(f"Failed to process scenario {scenario_id}: {exc}")
            raise


if __name__ == "__main__":
    main()
