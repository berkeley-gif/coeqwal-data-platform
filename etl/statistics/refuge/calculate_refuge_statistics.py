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

DATA SOURCE:
  All data from the CalSim DV output CSV:
    S3: scenario/{id}/csv/{id}_coeqwal_calsim_output.csv

  Variables (CFS in raw CSV, converted to TAF before analysis):
    AW_{DU_ID}    — Applied Water = DEMAND (model's optimised water application)
    DN_{DU_ID}    — Net Delivery  = SURFACE WATER DELIVERY
    SHRTG_{DU_ID} — Shortage, Sacramento region (kind='SHORTAGE')
    GW_SHORT_{DU_ID} — GW Restriction Shortage, SJR/Tulare (kind='GW-RESTRICT-SHORT')

  The COEQWAL notebook (DataExtraction.py) uses AW_* from the DV output as
  the demand variable for refuge DUs.  AWO_* in the SV input is the pre-model
  demand order/target — a different (higher) quantity.

SHORTAGE: Uses model-computed shortage variables when available:
  Sacramento _PR DUs: SHRTG_* (kind='SHORTAGE')
  SJR/Tulare _PR DUs: GW_SHORT_* (kind='GW-RESTRICT-SHORT')
  Fallback: derived as max(demand - delivery, 0) if no model variable exists.

  WRESL water balance for refuge DUs is identical to AG:
    AW + RP = DN + GP + RU + SHORTAGE

RELIABILITY: 95th percentile of annual shortage % across all simulated water years.
"In 95 of 100 years, annual shortage is at or below this value."

Usage:
    python calculate_refuge_statistics.py --scenario s0020
    python calculate_refuge_statistics.py --scenario s0020 --dry-run
    python calculate_refuge_statistics.py --all-scenarios
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from units import (  # noqa: E402
    CFS_TO_TAF_PER_DAY,
    CV_MIN_MEAN_TAF,
    apply_columns_and_dedup,
    build_units_map_first,
    parse_dss_csv_header,
    check_post_conversion_magnitude,
)
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


S3_BUCKET = os.getenv("S3_BUCKET", "coeqwal-model-run")

DV_OUTPUT_S3_KEYS = [
    "scenario/{scenario}/csv/{scenario}_coeqwal_calsim_output.csv",
    "scenario/{scenario}/csv/{scenario}_DV.csv",
]


PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DU_REFUGE_CSV = (
    PROJECT_ROOT / "database/seed_tables/04_calsim_data/du_refuge_entity.csv"
)

DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# Threshold to filter CalSim floating-point artifacts (100 acre-feet)
SHORTAGE_THRESHOLD_TAF = 0.1

SACRAMENTO_REFUGE_WBAS = {"08N", "08S", "09", "11", "17N", "17S"}

# Developer attribution — must match developer.id in the database
ETL_OPERATOR_ID = 2  # jfantauzza

# All 18 refuge demand unit IDs (from du_refuge_entity.csv)
REFUGE_DU_IDS = [
    "08N_PR1",
    "08N_PR2",
    "08S_PR",
    "09_PR",
    "11_PR",
    "17N_NR",
    "17N_PR",
    "17S_PR",
    "63_PR1",
    "63_PR2",
    "63_PR3",
    "72_PR1",
    "72_PR2",
    "72_PR3",
    "72_PR4",
    "72_PR5",
    "72_PR6",
    "91_PR",
]


# =============================================================================
# DATA LOADING
# =============================================================================


def load_refuge_demand_units(
    csv_path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load refuge demand unit metadata from du_refuge_entity.csv."""
    if csv_path is None:
        csv_path = DU_REFUGE_CSV

    if not csv_path.exists():
        log.warning(f"du_refuge_entity.csv not found at {csv_path}")
        return {}

    demand_units = {}
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            du_id = row.get("DU_ID", "").strip()
            if du_id:
                demand_units[du_id] = {
                    "wba_id": row.get("WBA_ID", "").strip(),
                    "hydrologic_region": row.get("hydrologic_region", "").strip(),
                    "cs3_type": row.get("CS3_Type", "").strip(),
                    "refuge_or_wildlife_area": row.get(
                        "refuge_or_wildlife_area", ""
                    ).strip(),
                    "managed_by": row.get("managed_by", "").strip(),
                    "provider": row.get("provider", "").strip(),
                    "sw": row.get("sw", "1").strip() == "1",
                    "has_gis_data": row.get("has_gis_data", "True").strip() == "True",
                }

    log.info(f"Loaded {len(demand_units)} refuge demand units from {csv_path}")
    return demand_units


def _load_dv_columns(
    var_names: List[str],
    data_df: pd.DataFrame,
    c_parts: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Extract the date column and all AW_* / DN_* refuge columns from the
    DV output DataFrame.

    CFS→TAF conversion is applied later in the orchestrator once DaysInMonth
    is available.
    """
    data_df = apply_columns_and_dedup(data_df, var_names, c_parts)

    date_col = var_names[0]
    refuge_ids = set(REFUGE_DU_IDS)
    cols_to_keep = [date_col]
    for vname in var_names[1:]:
        suffix = None
        if vname.startswith("AW_"):
            suffix = vname[3:]
        elif vname.startswith("DN_"):
            suffix = vname[3:]
        elif vname.startswith("SHRTG_"):
            suffix = vname[6:]
        elif vname.startswith("GW_SHORT_"):
            suffix = vname[9:]
        if suffix and suffix in refuge_ids:
            cols_to_keep.append(vname)

    result = data_df[cols_to_keep].copy()
    for c in result.columns[1:]:
        result[c] = pd.to_numeric(result[c], errors="coerce")

    aw_count = sum(1 for c in cols_to_keep if c.startswith("AW_"))
    dn_count = sum(1 for c in cols_to_keep if c.startswith("DN_"))
    shrt_count = sum(
        1 for c in cols_to_keep if c.startswith("SHRTG_") or c.startswith("GW_SHORT_")
    )
    log.info(
        f"Extracted {aw_count} AW_* + {dn_count} DN_* + {shrt_count} shortage refuge columns from DV output"
    )
    return result


def load_dv_csv_from_s3(scenario_id: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Load AW_* and DN_* refuge columns from the CalSim DV output on S3.

    Uses shared header parsing from units.py.
    Returns (data_df, units_map).  CFS→TAF conversion must be applied after
    add_water_year_month() supplies DaysInMonth.
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 required. Install with: pip install boto3")

    s3 = boto3.client("s3")
    keys = [k.format(scenario=scenario_id) for k in DV_OUTPUT_S3_KEYS]

    for key in keys:
        try:
            log.info(f"Trying DV output: s3://{S3_BUCKET}/{key}")
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            raw_bytes = response["Body"].read()
            var_names, units_row, c_parts = parse_dss_csv_header(io.BytesIO(raw_bytes))
            units_map = build_units_map_first(var_names, units_row)

            data_df = pd.read_csv(io.BytesIO(raw_bytes), header=None, skiprows=7, low_memory=False)
            result = _load_dv_columns(var_names, data_df, c_parts)

            kept_map = {c: units_map.get(c, "") for c in result.columns}
            return result, kept_map

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


def load_dv_csv_from_file(file_path: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Load AW_* and DN_* refuge columns from a local DV output CSV.

    Returns (data_df, units_map).  CFS→TAF conversion must be applied after
    add_water_year_month() supplies DaysInMonth.
    """
    log.info(f"Loading DV output from file: {file_path}")
    var_names, units_row, c_parts = parse_dss_csv_header(file_path)
    units_map = build_units_map_first(var_names, units_row)

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    result = _load_dv_columns(var_names, data_df, c_parts)

    kept_map = {c: units_map.get(c, "") for c in result.columns}
    return result, kept_map


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
    df["DateTime"] = pd.to_datetime(df[first_col], errors="coerce")

    # Shift period-beginning dates (day == 1) back by one day so that the
    # calendar month/year reflect the actual data period, not the label period.
    period_date = df["DateTime"].where(
        df["DateTime"].dt.day != 1,
        df["DateTime"] - pd.Timedelta(days=1),
    )

    df["CalendarMonth"] = period_date.dt.month
    df["CalendarYear"] = period_date.dt.year
    df["DaysInMonth"] = period_date.dt.daysinmonth
    df["WaterMonth"] = ((df["CalendarMonth"] - 10) % 12) + 1
    df["WaterYear"] = df["CalendarYear"]
    df.loc[df["CalendarMonth"] >= 10, "WaterYear"] += 1

    log.info(
        f"Date range: {df['DateTime'].min().date()} to {df['DateTime'].max().date()} "
        f"({df['WaterYear'].nunique()} water years)"
    )
    return df


# =============================================================================
# CALCULATIONS
# =============================================================================


def _safe_cv(data: pd.Series) -> float:
    """Coefficient of variation, returning 0 when mean is near-zero."""
    mean = float(data.mean())
    if abs(mean) < CV_MIN_MEAN_TAF:
        return 0.0
    cv = round(float(data.std() / abs(mean)), 4)
    if cv > 99.0:
        return 0.0
    return cv


def _percentile_row(data: pd.Series, prefix: str = "") -> Dict[str, float]:
    """Compute percentile bands and exceedance percentiles for a series."""
    row: Dict[str, float] = {}
    arr = data.dropna().values
    if len(arr) == 0:
        return row
    for p in DELIVERY_PERCENTILES:
        row[f"{prefix}q{p}"] = round(float(np.percentile(arr, p)), 2)
    for p in EXCEEDANCE_PERCENTILES:
        # exc_pX = value exceeded X% of the time = (100-X)th percentile
        row[f"{prefix}exc_p{p}"] = round(float(np.percentile(arr, 100 - p)), 2)
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
            df.loc[df["WaterMonth"] == wm, delivery_var], errors="coerce"
        ).dropna()

        if month_data.empty:
            continue

        row: Dict[str, Any] = {
            "du_id": du_id,
            "water_month": wm,
            "delivery_avg_taf": round(float(month_data.mean()), 2),
            "delivery_cv": _safe_cv(month_data),
            "sample_count": len(month_data),
        }
        row.update(_percentile_row(month_data))
        results.append(row)

    return results


def _find_shortage_var(df: pd.DataFrame, du_id: str) -> Optional[str]:
    """Return the model shortage column for a refuge DU, or None.

    Sacramento _PR DUs use SHRTG_{DU_ID}, SJR/Tulare use GW_SHORT_{DU_ID}.
    """
    wba = du_id.split("_")[0]
    if wba in SACRAMENTO_REFUGE_WBAS:
        col = f"SHRTG_{du_id}"
    else:
        col = f"GW_SHORT_{du_id}"
    return col if col in df.columns else None


def calculate_shortage_monthly(
    df: pd.DataFrame,
    du_id: str,
) -> List[Dict[str, Any]]:
    """
    Monthly shortage statistics for one refuge demand unit.

    Prefers the model's actual shortage variable (SHRTG_* or GW_SHORT_*)
    from the WRESL meetAW constraint.  Falls back to derived
    max(demand - delivery, 0) if the model variable is unavailable.
    Returns one row per water month.
    """
    demand_var = f"AW_{du_id}"
    delivery_var = f"DN_{du_id}"
    model_shortage_var = _find_shortage_var(df, du_id)

    if demand_var not in df.columns:
        log.debug(f"No demand variable for {du_id}: {demand_var}")
        return []
    if delivery_var not in df.columns and model_shortage_var is None:
        log.debug(f"No delivery or shortage variable for {du_id}")
        return []

    df_work = df.copy()
    df_work["demand"] = pd.to_numeric(df_work[demand_var], errors="coerce")

    if model_shortage_var is not None:
        df_work["shortage_taf"] = pd.to_numeric(
            df_work[model_shortage_var], errors="coerce"
        ).clip(lower=0)
        log.debug(f"Using model shortage variable {model_shortage_var} for {du_id}")
    else:
        df_work["delivery"] = pd.to_numeric(df_work[delivery_var], errors="coerce")
        df_work["shortage_taf"] = (df_work["demand"] - df_work["delivery"]).clip(
            lower=0
        )
        log.debug(f"Using derived shortage (AW - DN) for {du_id}")

    df_work["shortage_pct"] = np.where(
        df_work["demand"] > 0,
        (df_work["shortage_taf"] / df_work["demand"]) * 100,
        0.0,
    )
    max_pct = float(df_work["shortage_pct"].max())
    if max_pct > 200:
        log.warning(f"Suspicious refuge shortage %% for {du_id}: max={max_pct:.1f}%%")

    results = []
    for wm in range(1, 13):
        mask = df_work["WaterMonth"] == wm
        s_taf = df_work.loc[mask, "shortage_taf"].dropna()
        s_pct = df_work.loc[mask, "shortage_pct"].dropna()

        if s_taf.empty:
            continue

        frequency = float((s_taf > SHORTAGE_THRESHOLD_TAF).sum()) / len(s_taf) * 100

        row: Dict[str, Any] = {
            "du_id": du_id,
            "water_month": wm,
            "shortage_avg_taf": round(float(s_taf.mean()), 2),
            "shortage_cv": _safe_cv(s_taf),
            "shortage_pct_avg": round(float(s_pct.mean()), 4),
            "shortage_pct_cv": _safe_cv(s_pct),
            "shortage_frequency_pct": round(frequency, 4),
            "sample_count": len(s_taf),
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
    model_shortage_var = _find_shortage_var(df, du_id)

    if demand_var not in df.columns or delivery_var not in df.columns:
        log.debug(f"Missing demand or delivery variable for {du_id}")
        return None

    df_work = df.copy()
    df_work["demand"] = pd.to_numeric(df_work[demand_var], errors="coerce")
    df_work["delivery"] = pd.to_numeric(df_work[delivery_var], errors="coerce")

    if model_shortage_var is not None:
        df_work["shortage_taf"] = pd.to_numeric(
            df_work[model_shortage_var], errors="coerce"
        ).clip(lower=0)
    else:
        df_work["shortage_taf"] = (df_work["demand"] - df_work["delivery"]).clip(
            lower=0
        )

    # Annual aggregates (sum over water year)
    annual_delivery = df_work.groupby("WaterYear")["delivery"].sum()
    annual_demand = df_work.groupby("WaterYear")["demand"].sum()
    annual_shortage = df_work.groupby("WaterYear")["shortage_taf"].sum()

    annual_shortage_pct = np.where(
        annual_demand > 0,
        (annual_shortage / annual_demand) * 100,
        0.0,
    )

    water_years = sorted(df_work["WaterYear"].dropna().unique())

    result: Dict[str, Any] = {
        "du_id": du_id,
        "simulation_start_year": int(water_years[0]),
        "simulation_end_year": int(water_years[-1]),
        "total_years": len(water_years),
        # Delivery
        "annual_delivery_avg_taf": round(float(annual_delivery.mean()), 2),
        "annual_delivery_cv": _safe_cv(annual_delivery),
        # Shortage (TAF)
        "annual_shortage_avg_taf": round(float(annual_shortage.mean()), 2),
        "annual_shortage_cv": _safe_cv(annual_shortage),
        # Shortage (%)
        "annual_shortage_pct_avg": round(float(np.mean(annual_shortage_pct)), 4),
        "annual_shortage_pct_cv": round(
            float(np.std(annual_shortage_pct) / np.mean(annual_shortage_pct))
            if np.mean(annual_shortage_pct) > 0
            else 0.0,
            4,
        ),
        # Reliability: 95th percentile of annual shortage %
        # "In 95 of 100 years, shortage <= this value"
        "reliability_pct_95": round(float(np.percentile(annual_shortage_pct, 95)), 4),
    }

    # Annual delivery exceedance curve
    for p in EXCEEDANCE_PERCENTILES:
        result[f"delivery_exc_p{p}"] = round(
            float(np.percentile(annual_delivery, 100 - p)), 2
        )

    # Annual shortage exceedance curve
    for p in EXCEEDANCE_PERCENTILES:
        result[f"shortage_exc_p{p}"] = round(
            float(np.percentile(annual_shortage, 100 - p)), 2
        )

    return result


# =============================================================================
# ORCHESTRATION
# =============================================================================


def calculate_all_refuge_statistics(
    scenario_id: str,
    dv_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load DV output and calculate all refuge statistics for one scenario.

    All data (AW_* demand and DN_* delivery) comes from the DV output CSV.
    CFS columns are converted to TAF using per-row DaysInMonth.

    Returns dict with keys: delivery_monthly, shortage_monthly, period_summary.
    """
    log.info(f"=== Processing scenario: {scenario_id} ===")

    if dv_path:
        dv_df, dv_units = load_dv_csv_from_file(dv_path)
    else:
        dv_df, dv_units = load_dv_csv_from_s3(scenario_id)

    dv_df = add_water_year_month(dv_df)

    # Convert CFS columns to TAF using per-row DaysInMonth
    REFUGE_CFS_PREFIXES = ("AW_", "DN_", "SHRTG_", "GW_SHORT_")
    cfs_cols = []
    taf_already = []
    for c in dv_df.columns:
        if not any(c.startswith(p) for p in REFUGE_CFS_PREFIXES):
            continue
        unit = dv_units.get(c, "").upper()
        if unit == "CFS":
            cfs_cols.append(c)
        elif unit == "TAF":
            taf_already.append(c)

    for col in cfs_cols:
        dv_df[col] = (
            pd.to_numeric(dv_df[col], errors="coerce")
            * dv_df["DaysInMonth"]
            * CFS_TO_TAF_PER_DAY
        )
    for col in taf_already:
        dv_df[col] = pd.to_numeric(dv_df[col], errors="coerce")

    log.info(
        f"Converted {len(cfs_cols)} CFS→TAF columns; {len(taf_already)} already TAF"
    )

    merged = dv_df
    log.info(f"DV DataFrame: {len(merged)} rows, {len(merged.columns)} columns")

    # Safeguards — post-conversion magnitude check only.
    # Note: validate_water_balance (GP <= AW) is intentionally NOT called
    # for refuge DUs.  The WRESL water balance IS the same as AG
    # (AW + RP = DN + GP + RU + SHORTAGE), but the V3 notebook never uses
    # GP for refuge DUs.  Shortage now uses actual SHRTG_*/GW_SHORT_*
    # model variables when available, falling back to max(AW - DN, 0).
    if cfs_cols:
        check_post_conversion_magnitude(merged, cfs_cols, logger=log)

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
            r["scenario_short_code"] = scenario_id
            r["created_by"] = ETL_OPERATOR_ID
            r["updated_by"] = ETL_OPERATOR_ID
        delivery_monthly_rows.extend(rows)

        # Monthly shortage
        rows = calculate_shortage_monthly(merged, du_id)
        for r in rows:
            r["scenario_short_code"] = scenario_id
            r["created_by"] = ETL_OPERATOR_ID
            r["updated_by"] = ETL_OPERATOR_ID
        shortage_monthly_rows.extend(rows)

        # Period summary
        summary = calculate_period_summary(merged, du_id)
        if summary:
            summary["scenario_short_code"] = scenario_id
            summary["created_by"] = ETL_OPERATOR_ID
            summary["updated_by"] = ETL_OPERATOR_ID
            period_summary_rows.append(summary)

    log.info(
        f"Scenario {scenario_id}: "
        f"{len(delivery_monthly_rows)} delivery-monthly rows, "
        f"{len(shortage_monthly_rows)} shortage-monthly rows, "
        f"{len(period_summary_rows)} period-summary rows"
    )

    return {
        "delivery_monthly": delivery_monthly_rows,
        "shortage_monthly": shortage_monthly_rows,
        "period_summary": period_summary_rows,
    }


# =============================================================================
# DATABASE WRITE
# =============================================================================

DELIVERY_MONTHLY_COLS = [
    "scenario_short_code",
    "du_id",
    "water_month",
    "delivery_avg_taf",
    "delivery_cv",
    "q0",
    "q10",
    "q30",
    "q50",
    "q70",
    "q90",
    "q100",
    "exc_p5",
    "exc_p10",
    "exc_p25",
    "exc_p50",
    "exc_p75",
    "exc_p90",
    "exc_p95",
    "sample_count",
    "created_by",
    "updated_by",
]

SHORTAGE_MONTHLY_COLS = [
    "scenario_short_code",
    "du_id",
    "water_month",
    "shortage_avg_taf",
    "shortage_cv",
    "shortage_pct_avg",
    "shortage_pct_cv",
    "shortage_frequency_pct",
    "q0",
    "q10",
    "q30",
    "q50",
    "q70",
    "q90",
    "q100",
    "exc_p5",
    "exc_p10",
    "exc_p25",
    "exc_p50",
    "exc_p75",
    "exc_p90",
    "exc_p95",
    "sample_count",
    "created_by",
    "updated_by",
]

PERIOD_SUMMARY_COLS = [
    "scenario_short_code",
    "du_id",
    "simulation_start_year",
    "simulation_end_year",
    "total_years",
    "annual_delivery_avg_taf",
    "annual_delivery_cv",
    "delivery_exc_p5",
    "delivery_exc_p10",
    "delivery_exc_p25",
    "delivery_exc_p50",
    "delivery_exc_p75",
    "delivery_exc_p90",
    "delivery_exc_p95",
    "annual_shortage_avg_taf",
    "annual_shortage_cv",
    "shortage_exc_p5",
    "shortage_exc_p10",
    "shortage_exc_p25",
    "shortage_exc_p50",
    "shortage_exc_p75",
    "shortage_exc_p90",
    "shortage_exc_p95",
    "annual_shortage_pct_avg",
    "annual_shortage_pct_cv",
    "reliability_pct_95",
    "created_by",
    "updated_by",
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
        raise ImportError(
            "psycopg2 required. Install with: pip install psycopg2-binary"
        )

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            for table in [
                "refuge_du_delivery_monthly",
                "refuge_du_shortage_monthly",
                "refuge_du_period_summary",
            ]:
                cur.execute(
                    f"DELETE FROM {table} WHERE scenario_short_code = %s",
                    (scenario_id,),
                )
                log.info(f"Deleted existing rows for {scenario_id} from {table}")

            # Insert delivery monthly
            del_rows = _rows_to_tuples(stats["delivery_monthly"], DELIVERY_MONTHLY_COLS)
            if del_rows:
                execute_values(
                    cur,
                    f"INSERT INTO refuge_du_delivery_monthly ({', '.join(DELIVERY_MONTHLY_COLS)}) VALUES %s",
                    del_rows,
                )
                log.info(
                    f"Inserted {len(del_rows)} rows into refuge_du_delivery_monthly"
                )

            # Insert shortage monthly
            sht_rows = _rows_to_tuples(stats["shortage_monthly"], SHORTAGE_MONTHLY_COLS)
            if sht_rows:
                execute_values(
                    cur,
                    f"INSERT INTO refuge_du_shortage_monthly ({', '.join(SHORTAGE_MONTHLY_COLS)}) VALUES %s",
                    sht_rows,
                )
                log.info(
                    f"Inserted {len(sht_rows)} rows into refuge_du_shortage_monthly"
                )

            # Insert period summary
            ps_rows = _rows_to_tuples(stats["period_summary"], PERIOD_SUMMARY_COLS)
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
    parser.add_argument("--scenario", help="Single scenario ID (e.g. s0020)")
    parser.add_argument(
        "--all-scenarios",
        action="store_true",
        help=f"Process all known scenarios: {SCENARIOS}",
    )
    parser.add_argument(
        "--dv-path", help="Local path to CalSim DV output CSV (overrides S3)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calculate statistics without writing to database",
    )
    parser.add_argument(
        "--output-json",
        action="store_true",
        help="Print results as JSON (implies --dry-run)",
    )
    args = parser.parse_args()

    if args.output_json:
        args.dry_run = True

    db_url = os.getenv("DATABASE_URL")
    if not args.dry_run and not db_url:
        parser.error(
            "DATABASE_URL environment variable required unless --dry-run is set"
        )

    scenarios = (
        SCENARIOS if args.all_scenarios else [args.scenario] if args.scenario else []
    )
    if not scenarios:
        parser.error("Provide --scenario SCENARIO_ID or --all-scenarios")

    for scenario_id in scenarios:
        try:
            stats = calculate_all_refuge_statistics(
                scenario_id,
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
