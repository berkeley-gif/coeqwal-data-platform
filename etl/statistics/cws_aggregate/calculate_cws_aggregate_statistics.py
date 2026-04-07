#!/usr/bin/env python3
"""
Calculate delivery and shortage statistics for CWS system-level aggregates.

This module processes CalSim output to calculate statistics for:
- SWP Total M&I (DEL_SWP_PMI, SHORT_SWP_PMI)
- SWP North of Delta (DEL_SWP_PMI_N, SHORT_SWP_PMI_N)
- SWP South of Delta (DEL_SWP_PMI_S, SHORT_SWP_PMI_S)
- CVP North (DEL_CVP_PMI_N, SHORT_CVP_PMI_N)
- CVP South (DEL_CVP_PMI_S, SHORT_CVP_PMI_S)
- MWD (DEL_SWP_MWD, SHORT_SWP_MWD)

Usage:
    python calculate_cws_aggregate_statistics.py --scenario s0020
    python calculate_cws_aggregate_statistics.py --scenario s0020 --csv-path /path/to/calsim_output.csv
"""

import argparse
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
    MWD_TABLE_A_ANNUAL_TAF,
    check_post_conversion_magnitude,
    deduplicate_columns,
    parse_dss_csv_header,
)
from scenarios import SCENARIOS  # noqa: E402

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
# S3 bucket configuration
S3_BUCKET = os.getenv("S3_BUCKET", "coeqwal-model-run")

# Percentiles for statistics
DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# Minimum threshold for counting a year as having a "shortage" (in TAF)
# This filters out floating-point precision artifacts from CalSim's linear programming solver.
# 0.1 TAF = 100 acre-feet, which is < 0.05% of typical CVP North M&I delivery (~240 TAF/yr)
SHORTAGE_THRESHOLD_TAF = 0.1


# =============================================================================
# CWS AGGREGATE DEFINITIONS
# =============================================================================

CWS_AGGREGATES = {
    "swp_total": {
        "id": 1,
        "label": "SWP Total M&I",
        "delivery_var": "DEL_SWP_PMI",
        "shortage_var": "SHORT_SWP_PMI",
        "demand_mode": "del_plus_short",
        "description": "Total State Water Project M&I deliveries",
    },
    "swp_nod": {
        "id": 5,
        "label": "SWP North",
        "delivery_var": "DEL_SWP_PMI_N",
        "shortage_var": "SHORT_SWP_PMI_N",
        "demand_mode": "del_plus_short",
        "description": "SWP M&I deliveries - North of Delta",
    },
    "swp_sod": {
        "id": 6,
        "label": "SWP South",
        "delivery_var": "DEL_SWP_PMI_S",
        "shortage_var": "SHORT_SWP_PMI_S",
        "demand_mode": "del_plus_short",
        "description": "SWP M&I deliveries - South of Delta",
    },
    "cvp_nod": {
        "id": 2,
        "label": "CVP North",
        "delivery_var": "DEL_CVP_PMI_N",
        "shortage_var": "SHORT_CVP_PMI_N",
        "demand_mode": "del_plus_short",
        "description": "CVP M&I deliveries - North of Delta",
    },
    "cvp_sod": {
        "id": 3,
        "label": "CVP South",
        "delivery_var": "DEL_CVP_PMI_S",
        "shortage_var": "SHORT_CVP_PMI_S",
        "demand_mode": "del_plus_short",
        "description": "CVP M&I deliveries - South of Delta",
    },
    "mwd": {
        "id": 4,
        "label": "Metropolitan Water District",
        "delivery_var": "DEL_SWP_MWD",
        "shortage_var": "SHORT_SWP_MWD",
        "demand_mode": "table_a",
        "description": "MWD Southern California aggregate",
    },
}


def load_calsim_csv_from_s3(scenario_id: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Load CalSim output CSV from S3 bucket with deduplication.

    Returns (DataFrame, units_map) where units_map maps column names to declared units.
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access")

    import io

    s3 = boto3.client("s3")

    possible_keys = [
        f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv",
        f"scenario/{scenario_id}/csv/{scenario_id}_DV.csv",
    ]

    for key in possible_keys:
        try:
            log.info(f"Trying S3 key: s3://{S3_BUCKET}/{key}")
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            raw_bytes = response["Body"].read()

            var_names, units_row = parse_dss_csv_header(io.BytesIO(raw_bytes))
            keep_indices, units_map = deduplicate_columns(
                var_names, units_row, prefer_cfs=True
            )

            data_df = pd.read_csv(io.BytesIO(raw_bytes), header=None, skiprows=7)
            data_df = data_df.iloc[:, keep_indices]
            data_df.columns = [var_names[i] for i in keep_indices]

            n_dupes = len(var_names) - len(keep_indices)
            if n_dupes > 0:
                log.info(f"Deduplicated {n_dupes} duplicate columns")

            log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
            return data_df, units_map

        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            log.warning(f"Error loading {key}: {e}")
            continue

    raise FileNotFoundError(f"Could not find CalSim output for {scenario_id} in S3")


def load_calsim_csv_from_file(file_path: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Load CalSim output CSV from local file with deduplication.

    Handles the 7-header-row DSS format.
    Uses parse_dss_csv_header + deduplicate_columns to safely handle
    V3 CSVs that may contain both CFS and TAF versions of a column.

    Returns (DataFrame, units_map) where units_map maps column names to declared units.
    """
    log.info(f"Loading from file: {file_path}")

    var_names, units_row = parse_dss_csv_header(file_path)
    keep_indices, units_map = deduplicate_columns(var_names, units_row, prefer_cfs=True)

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    data_df = data_df.iloc[:, keep_indices]
    data_df.columns = [var_names[i] for i in keep_indices]

    n_dupes = len(var_names) - len(keep_indices)
    if n_dupes > 0:
        log.info(f"Deduplicated {n_dupes} duplicate columns")

    log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df, units_map


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """Add water year and water month columns."""
    df = df.copy()

    # Find date column (first column)
    first_col = df.columns[0]
    date_values = df[first_col]

    # Try to parse as datetime
    try:
        df["DateTime"] = pd.to_datetime(date_values, errors="coerce")
        df["CalendarMonth"] = df["DateTime"].dt.month
        df["CalendarYear"] = df["DateTime"].dt.year
        df["DaysInMonth"] = df["DateTime"].dt.daysinmonth

        # Water month: Oct(10)->1, Nov(11)->2, ..., Sep(9)->12
        df["WaterMonth"] = ((df["CalendarMonth"] - 10) % 12) + 1

        # Water year: Oct-Dec belong to next water year
        df["WaterYear"] = df["CalendarYear"]
        df.loc[df["CalendarMonth"] >= 10, "WaterYear"] += 1

        log.info(f"Date range: {df['DateTime'].min()} to {df['DateTime'].max()}")
    except Exception as e:
        log.warning(f"Could not parse dates: {e}")
        # Fallback: assume annual data
        df["WaterYear"] = pd.to_numeric(date_values, errors="coerce").astype(int)
        df["WaterMonth"] = 0

    return df


def _to_taf(
    series: pd.Series,
    col_name: str,
    units_map: Dict[str, str],
    days_in_month: pd.Series,
) -> pd.Series:
    """Convert a column to TAF based on its declared unit."""
    vals = pd.to_numeric(series, errors="coerce")
    unit = units_map.get(col_name, "").upper()
    if unit == "CFS":
        return vals * days_in_month * CFS_TO_TAF_PER_DAY
    if unit == "TAF":
        return vals
    log.debug(f"Column {col_name} has unit '{unit}'; assuming CFS for conversion")
    return vals * days_in_month * CFS_TO_TAF_PER_DAY


def calculate_aggregate_monthly(
    df: pd.DataFrame,
    short_code: str,
    aggregate_id: int,
    delivery_var: str,
    shortage_var: str,
    demand_mode: str = "del_plus_short",
    units_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Calculate monthly statistics for a CWS aggregate.

    Args:
        df: Main CalSim DV output DataFrame with delivery/shortage data
        short_code: Aggregate short code (e.g., 'swp_total')
        aggregate_id: Database ID for this aggregate
        delivery_var: Name of delivery variable
        shortage_var: Name of shortage variable
        demand_mode: 'del_plus_short' (default) or 'table_a' (MWD constant)
        units_map: Column name → declared unit from CSV header

    Returns list of dicts for cws_aggregate_monthly table.
    """
    if units_map is None:
        units_map = {}

    results = []

    has_delivery = delivery_var in df.columns
    has_shortage = shortage_var in df.columns

    if not has_delivery:
        log.warning(f"Delivery variable {delivery_var} not found for {short_code}")
        return []

    is_annual = (df["WaterMonth"] == 0).all() if "WaterMonth" in df.columns else False

    df = df.copy()
    df["delivery_taf"] = _to_taf(
        df[delivery_var], delivery_var, units_map, df["DaysInMonth"]
    )
    if has_shortage:
        df["shortage_taf"] = _to_taf(
            df[shortage_var], shortage_var, units_map, df["DaysInMonth"]
        )

    # Safeguard: check converted TAF values for plausible magnitudes
    taf_cols_to_check = ["delivery_taf"]
    if has_shortage:
        taf_cols_to_check.append("shortage_taf")
    check_post_conversion_magnitude(df, taf_cols_to_check, logger=log)

    if is_annual:
        # Annual data - single aggregated row
        delivery_data = df["delivery_taf"].dropna()
        shortage_data = df["shortage_taf"].dropna() if has_shortage else pd.Series()

        if delivery_data.empty:
            return []

        row = {
            "cws_aggregate_id": aggregate_id,
            "water_month": 0,
            "delivery_avg_taf": round(float(delivery_data.mean()), 2),
            "delivery_cv": round(float(delivery_data.std() / delivery_data.mean()), 4)
            if delivery_data.mean() > 0
            else 0,
            "sample_count": len(delivery_data),
        }

        # Delivery percentiles
        for p in DELIVERY_PERCENTILES:
            row[f"delivery_q{p}"] = round(float(np.percentile(delivery_data, p)), 2)

        # Delivery exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f"delivery_exc_p{p}"] = round(
                float(np.percentile(delivery_data, 100 - p)), 2
            )

        # Shortage statistics
        if not shortage_data.empty:
            row["shortage_avg_taf"] = round(float(shortage_data.mean()), 2)
            row["shortage_cv"] = (
                round(float(shortage_data.std() / shortage_data.mean()), 4)
                if shortage_data.mean() > 0
                else 0
            )
            row["shortage_frequency_pct"] = round(
                ((shortage_data > SHORTAGE_THRESHOLD_TAF).sum() / len(shortage_data))
                * 100,
                2,
            )

            for p in DELIVERY_PERCENTILES:
                row[f"shortage_q{p}"] = round(float(np.percentile(shortage_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f"shortage_exc_p{p}"] = round(
                    float(np.percentile(shortage_data, 100 - p)), 2
                )
        else:
            row["shortage_avg_taf"] = None
            row["shortage_cv"] = None
            row["shortage_frequency_pct"] = None

        # Demand computation
        row["demand_avg_taf"] = None
        row["percent_of_demand_avg"] = None
        if demand_mode == "table_a":
            row["demand_avg_taf"] = round(MWD_TABLE_A_ANNUAL_TAF / 12.0, 2)
            if row["delivery_avg_taf"] is not None and row["demand_avg_taf"] > 0:
                pct = (row["delivery_avg_taf"] / row["demand_avg_taf"]) * 100
                row["percent_of_demand_avg"] = round(min(100.0, max(0.0, pct)), 2)
        elif has_shortage:
            demand_data = (df["delivery_taf"] + df["shortage_taf"]).dropna()
            if not demand_data.empty:
                row["demand_avg_taf"] = round(float(demand_data.mean()), 2)
                if row["demand_avg_taf"] > 0:
                    pct = (row["delivery_avg_taf"] / row["demand_avg_taf"]) * 100
                    row["percent_of_demand_avg"] = round(min(100.0, max(0.0, pct)), 2)

        results.append(row)
    else:
        # Monthly data - 12 rows
        for wm in range(1, 13):
            month_df = df[df["WaterMonth"] == wm]
            delivery_data = month_df["delivery_taf"].dropna()

            if delivery_data.empty:
                continue

            shortage_data = (
                month_df["shortage_taf"].dropna() if has_shortage else pd.Series()
            )

            row = {
                "cws_aggregate_id": aggregate_id,
                "water_month": wm,
                "delivery_avg_taf": round(float(delivery_data.mean()), 2),
                "delivery_cv": round(
                    float(delivery_data.std() / delivery_data.mean()), 4
                )
                if delivery_data.mean() > 0
                else 0,
                "sample_count": len(delivery_data),
            }

            # Delivery percentiles
            for p in DELIVERY_PERCENTILES:
                row[f"delivery_q{p}"] = round(float(np.percentile(delivery_data, p)), 2)

            # Delivery exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f"delivery_exc_p{p}"] = round(
                    float(np.percentile(delivery_data, 100 - p)), 2
                )

            # Shortage statistics
            if not shortage_data.empty:
                row["shortage_avg_taf"] = round(float(shortage_data.mean()), 2)
                row["shortage_cv"] = (
                    round(float(shortage_data.std() / shortage_data.mean()), 4)
                    if shortage_data.mean() > 0
                    else 0
                )
                row["shortage_frequency_pct"] = round(
                    (
                        (shortage_data > SHORTAGE_THRESHOLD_TAF).sum()
                        / len(shortage_data)
                    )
                    * 100,
                    2,
                )

                for p in DELIVERY_PERCENTILES:
                    row[f"shortage_q{p}"] = round(
                        float(np.percentile(shortage_data, p)), 2
                    )

                # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
                for p in EXCEEDANCE_PERCENTILES:
                    row[f"shortage_exc_p{p}"] = round(
                        float(np.percentile(shortage_data, 100 - p)), 2
                    )
            else:
                row["shortage_avg_taf"] = None
                row["shortage_cv"] = None
                row["shortage_frequency_pct"] = None

            # Demand computation (monthly)
            row["demand_avg_taf"] = None
            row["percent_of_demand_avg"] = None
            if demand_mode == "table_a":
                row["demand_avg_taf"] = round(MWD_TABLE_A_ANNUAL_TAF / 12.0, 2)
                if row["delivery_avg_taf"] is not None and row["demand_avg_taf"] > 0:
                    pct = (row["delivery_avg_taf"] / row["demand_avg_taf"]) * 100
                    row["percent_of_demand_avg"] = round(min(100.0, max(0.0, pct)), 2)
            elif has_shortage and not shortage_data.empty:
                demand_month = (
                    month_df["delivery_taf"] + month_df["shortage_taf"]
                ).dropna()
                if not demand_month.empty:
                    row["demand_avg_taf"] = round(float(demand_month.mean()), 2)
                    if row["demand_avg_taf"] > 0:
                        pct = (row["delivery_avg_taf"] / row["demand_avg_taf"]) * 100
                        row["percent_of_demand_avg"] = round(
                            min(100.0, max(0.0, pct)), 2
                        )

            results.append(row)

    return results


def calculate_aggregate_period_summary(
    df: pd.DataFrame,
    short_code: str,
    aggregate_id: int,
    delivery_var: str,
    shortage_var: str,
    demand_mode: str = "del_plus_short",
    units_map: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary for a CWS aggregate.

    Args:
        df: Main CalSim DV output DataFrame with delivery/shortage data
        short_code: Aggregate short code (e.g., 'swp_total')
        aggregate_id: Database ID for this aggregate
        delivery_var: Name of delivery variable
        shortage_var: Name of shortage variable
        demand_mode: 'del_plus_short' (default) or 'table_a' (MWD constant)
        units_map: Column name → declared unit from CSV header

    Returns dict for cws_aggregate_period_summary table.
    """
    if units_map is None:
        units_map = {}

    if delivery_var not in df.columns:
        return None

    delivery_vals = pd.to_numeric(df[delivery_var], errors="coerce")
    if delivery_vals.dropna().empty:
        return None

    df = df.copy()
    df["delivery_taf"] = _to_taf(
        df[delivery_var], delivery_var, units_map, df["DaysInMonth"]
    )

    water_years = sorted(df["WaterYear"].unique())

    result = {
        "cws_aggregate_id": aggregate_id,
        "simulation_start_year": int(water_years[0]),
        "simulation_end_year": int(water_years[-1]),
        "total_years": len(water_years),
    }

    # Annual delivery statistics (now in TAF)
    annual_delivery = df.groupby("WaterYear")["delivery_taf"].sum()
    result["annual_delivery_avg_taf"] = round(float(annual_delivery.mean()), 2)
    result["annual_delivery_cv"] = (
        round(float(annual_delivery.std() / annual_delivery.mean()), 4)
        if annual_delivery.mean() > 0
        else 0
    )
    result["annual_delivery_min_taf"] = round(float(annual_delivery.min()), 2)
    result["annual_delivery_max_taf"] = round(float(annual_delivery.max()), 2)

    # Delivery exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
    for p in EXCEEDANCE_PERCENTILES:
        result[f"delivery_exc_p{p}"] = round(
            float(np.percentile(annual_delivery, 100 - p)), 2
        )

    # Shortage statistics
    has_shortage = shortage_var in df.columns
    if has_shortage:
        df["shortage_taf"] = _to_taf(
            df[shortage_var], shortage_var, units_map, df["DaysInMonth"]
        )
        annual_shortage = df.groupby("WaterYear")["shortage_taf"].sum()
        # Use threshold to filter out floating-point noise from CalSim solver
        shortage_years = (annual_shortage > SHORTAGE_THRESHOLD_TAF).sum()

        result["annual_shortage_avg_taf"] = round(float(annual_shortage.mean()), 2)
        result["shortage_years_count"] = int(shortage_years)
        result["shortage_frequency_pct"] = round(
            (shortage_years / len(water_years)) * 100, 2
        )

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            result[f"shortage_exc_p{p}"] = round(
                float(np.percentile(annual_shortage, 100 - p)), 2
            )

    else:
        result["annual_shortage_avg_taf"] = None
        result["shortage_years_count"] = None
        result["shortage_frequency_pct"] = None

    # Demand computation
    result["annual_demand_avg_taf"] = None
    result["avg_pct_demand_met"] = None
    result["reliability_pct"] = None
    result["avg_pct_allocation_met"] = None

    if demand_mode == "table_a":
        result["annual_demand_avg_taf"] = MWD_TABLE_A_ANNUAL_TAF
    elif has_shortage:
        df["demand_taf"] = df["delivery_taf"] + df["shortage_taf"]
        annual_demand = df.groupby("WaterYear")["demand_taf"].sum()
        if not annual_demand.empty and annual_demand.mean() > 0:
            result["annual_demand_avg_taf"] = round(float(annual_demand.mean()), 2)

    if result["annual_demand_avg_taf"] and result["annual_demand_avg_taf"] > 0:
        if result["annual_delivery_avg_taf"]:
            pct = (
                result["annual_delivery_avg_taf"] / result["annual_demand_avg_taf"]
            ) * 100
            result["avg_pct_demand_met"] = round(min(100.0, max(0.0, pct)), 2)
        if result.get("annual_shortage_avg_taf") is not None:
            result["reliability_pct"] = round(
                (
                    1
                    - result["annual_shortage_avg_taf"]
                    / result["annual_demand_avg_taf"]
                )
                * 100,
                2,
            )
            result["avg_pct_allocation_met"] = result["reliability_pct"]

    return result


def calculate_all_cws_aggregate_statistics(
    scenario_id: str,
    csv_path: Optional[str] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Calculate all statistics for CWS aggregates for a scenario.

    Demand is computed as delivery + shortage from the DV output.
    No external DEMANDS CSV is required.

    Args:
        scenario_id: Scenario ID (e.g., 's0020')
        csv_path: Optional path to main CalSim DV output CSV

    Returns:
        Tuple of (monthly_rows, period_summary_rows)
    """
    log.info(f"Processing scenario: {scenario_id}")

    if csv_path:
        df, units_map = load_calsim_csv_from_file(csv_path)
    else:
        df, units_map = load_calsim_csv_from_s3(scenario_id)

    df = add_water_year_month(df)

    available_columns = list(df.columns)
    log.info(f"Available columns: {len(available_columns)}")

    monthly_rows = []
    period_summary_rows = []

    mapped_count = 0
    demand_count = 0

    for short_code, info in CWS_AGGREGATES.items():
        delivery_var = info["delivery_var"]
        shortage_var = info["shortage_var"]
        aggregate_id = info["id"]
        demand_mode = info.get("demand_mode", "del_plus_short")

        if delivery_var not in available_columns:
            log.warning(f"Delivery variable {delivery_var} not found for {short_code}")
            continue

        mapped_count += 1

        monthly = calculate_aggregate_monthly(
            df,
            short_code,
            aggregate_id,
            delivery_var,
            shortage_var,
            demand_mode=demand_mode,
            units_map=units_map,
        )
        for row in monthly:
            row["scenario_short_code"] = scenario_id
        monthly_rows.extend(monthly)

        summary = calculate_aggregate_period_summary(
            df,
            short_code,
            aggregate_id,
            delivery_var,
            shortage_var,
            demand_mode=demand_mode,
            units_map=units_map,
        )
        if summary:
            summary["scenario_short_code"] = scenario_id
            period_summary_rows.append(summary)
            if summary.get("annual_demand_avg_taf") is not None:
                demand_count += 1

    log.info(f"Processed {mapped_count}/{len(CWS_AGGREGATES)} aggregates")
    log.info(f"Calculated demand for {demand_count}/{mapped_count} aggregates")
    log.info(
        f"Generated: {len(monthly_rows)} monthly, {len(period_summary_rows)} period summary rows"
    )

    return monthly_rows, period_summary_rows


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Calculate delivery and shortage statistics for CWS system-level aggregates"
    )
    parser.add_argument("--scenario", "-s", help="Scenario ID (e.g., s0020)")
    parser.add_argument(
        "--all-scenarios", action="store_true", help="Process all known scenarios"
    )
    parser.add_argument(
        "--csv-path", help="Local CalSim output CSV file path (instead of S3)"
    )

    parser.add_argument(
        "--output-json", action="store_true", help="Output results as JSON"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Calculate but do not save output"
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
            )

            all_monthly.extend(monthly)
            all_period_summary.extend(period_summary)

        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise

    if args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")
        log.info(
            f"Total: {len(all_monthly)} monthly, {len(all_period_summary)} period summary rows"
        )
        return

    if args.output_json:
        output = {
            "monthly": all_monthly,
            "period_summary": all_period_summary,
        }
        print(json.dumps(output, indent=2))
        return

    # Save to database
    database_url = os.getenv("DATABASE_URL")
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
        scenario_ids = list(set(row["scenario_short_code"] for row in all_monthly))
        for scenario_id in scenario_ids:
            cur.execute(
                "DELETE FROM cws_aggregate_monthly WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            cur.execute(
                "DELETE FROM cws_aggregate_period_summary WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            log.info(f"Cleared existing data for scenario {scenario_id}")

        # Insert monthly rows
        if all_monthly:
            monthly_cols = [
                "scenario_short_code",
                "cws_aggregate_id",
                "water_month",
                "delivery_avg_taf",
                "delivery_cv",
                "delivery_q0",
                "delivery_q10",
                "delivery_q30",
                "delivery_q50",
                "delivery_q70",
                "delivery_q90",
                "delivery_q100",
                "delivery_exc_p5",
                "delivery_exc_p10",
                "delivery_exc_p25",
                "delivery_exc_p50",
                "delivery_exc_p75",
                "delivery_exc_p90",
                "delivery_exc_p95",
                "shortage_avg_taf",
                "shortage_cv",
                "shortage_frequency_pct",
                "shortage_q0",
                "shortage_q10",
                "shortage_q30",
                "shortage_q50",
                "shortage_q70",
                "shortage_q90",
                "shortage_q100",
                "shortage_exc_p5",
                "shortage_exc_p10",
                "shortage_exc_p25",
                "shortage_exc_p50",
                "shortage_exc_p75",
                "shortage_exc_p90",
                "shortage_exc_p95",
                "demand_avg_taf",
                "percent_of_demand_avg",  # Demand metrics
                "sample_count",
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
                INSERT INTO cws_aggregate_monthly ({", ".join(monthly_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, monthly_values)
            log.info(f"Inserted {len(monthly_values)} monthly rows")

        # Insert period summary rows
        if all_period_summary:
            summary_cols = [
                "scenario_short_code",
                "cws_aggregate_id",
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
                "shortage_years_count",
                "shortage_frequency_pct",
                "shortage_exc_p5",
                "shortage_exc_p10",
                "shortage_exc_p25",
                "shortage_exc_p50",
                "shortage_exc_p75",
                "shortage_exc_p90",
                "shortage_exc_p95",
                "reliability_pct",
                "avg_pct_allocation_met",
                "annual_demand_avg_taf",
                "avg_pct_demand_met",  # Demand metrics
            ]
            summary_values = [
                tuple(convert_numpy(row.get(col)) for col in summary_cols)
                for row in all_period_summary
            ]
            insert_sql = f"""
                INSERT INTO cws_aggregate_period_summary ({", ".join(summary_cols)})
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


if __name__ == "__main__":
    main()
