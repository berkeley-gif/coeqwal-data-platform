#!/usr/bin/env python3
"""
Calculate demand, delivery, and shortage statistics for agricultural demand units.

DATA SOURCE:
  All data from the CalSim DV output CSV:
    S3: scenario/{id}/csv/{id}_coeqwal_calsim_output.csv

  Variables (all CFS in the raw CSV, converted to TAF before analysis):
    AW_{DU_ID}       — Applied Water = DEMAND (model's optimized water application)
    DN_{DU_ID}       — Net Delivery  = SURFACE WATER DELIVERY
    GP_{DU_ID}       — Groundwater Pumping
    SHRTG_{DU_ID}    — Shortage (Sacramento region, kind='SHORTAGE')
    GW_SHORT_{DU_ID} — GW Restriction Shortage (SJR/Tulare, kind='GW-RESTRICT-SHORT')
    DEL_*, SHORT_*   — Project-level aggregate delivery/shortage

CalSim 3 Water Balance (from WRESL constraints-Deliveries.wresl):
  AW + RP = DN + GP + RU + SHORTAGE
  where RP = Riparian/misc ET = AW × RPF (typically 5-15% of AW)
  GP is bounded by GPmax × AW × (1 + RPF - RUF), so GP > AW is expected.

  The COEQWAL notebook (DataExtraction.py) uses AW_* from the DV output as
  the demand variable for agricultural DUs.  AWO_* in the SV input is the
  pre-model demand order/target — a different (higher) quantity.

  18 GW-only DUs have no DN in the WRESL meetAW constraint — their entire
  supply is GP + RU.  The ETL synthesizes delivery as GP + RU for these DUs,
  matching the COEQWAL notebook (DataExtraction.py) approach.

  Sacramento (9): 06_NA, 07N_NA, 07S_NA, 15N_NA1, 15S_NA1, 16_NA1, 17N_NA,
                  20_NA2, 26N_NA
  SJR/Tulare (9): 60S_NA1, 60S_NA2, 61_NA1, 62_NA1, 63_NA1, 64_NA1,
                  72_NA2, 73_NA
  Note: 26S_NA is commented out in WRESL (moved to Lower Mokelumne).

Note: Sacramento region DUs (WBAs 02-26) use SHRTG_* (kind='SHORTAGE'),
  while SJR/Tulare DUs use GW_SHORT_* (kind='GW-RESTRICT-SHORT').
  Both are the slack variable in the meetAW water balance constraint.

Usage:
    python calculate_ag_statistics.py --scenario s0020
    python calculate_ag_statistics.py --scenario s0020 --dv-path /path/to/DV.csv
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
    apply_columns_and_dedup,
    build_units_map_first,
    compute_cv,
    parse_dss_csv_header,
    safe_pct,
    validate_water_balance,
    check_post_conversion_magnitude,
)

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
log = logging.getLogger("ag_statistics")

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from etl.common import S3_BUCKET  # noqa: E402
from etl.common.etl_scenarios import ETL_SCENARIOS as SCENARIOS  # noqa: E402

DV_OUTPUT_S3_KEYS = [
    "scenario/{scenario}/csv/{scenario}_coeqwal_calsim_output.csv",
    "scenario/{scenario}/csv/{scenario}_DV.csv",
]

# Paths relative to project
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DU_AGRICULTURE_CSV = (
    PROJECT_ROOT / "database/seed_tables/04_calsim_data/du_agriculture_entity.csv"
)

# Percentiles for statistics
DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# Minimum threshold for counting a year as having a "shortage" (in TAF)
# This filters out floating-point precision artifacts from CalSim's linear programming solver.
# 0.1 TAF = 100 acre-feet, which is < 0.05% of typical delivery
SHORTAGE_THRESHOLD_TAF = 0.1

# GW-only DUs have no DN_* in the WRESL meetAW constraint. Their supply is
# entirely GP + RU. The notebook (DataExtraction.py) synthesizes DN = GP + RU
# for these DUs. Source: CalSim3 WRESL constraints-Deliveries.
GW_ONLY_DU_IDS = frozenset([
    # Sacramento (9)
    "06_NA", "07N_NA", "07S_NA", "15N_NA1", "15S_NA1",
    "16_NA1", "17N_NA", "20_NA2", "26N_NA",
    # SJR/Tulare (9)
    "60S_NA1", "60S_NA2", "61_NA1", "62_NA1", "63_NA1",
    "64_NA1", "72_NA2", "73_NA",
])


# Aggregate definitions — direct DV variables
AG_AGGREGATES = {
    # SWP Project AG
    "swp_pag": {
        "delivery_var": "DEL_SWP_PAG",
        "shortage_var": "SHORT_SWP_PAG",
        "description": "SWP Project AG - Total",
    },
    "swp_pag_n": {
        "delivery_var": "DEL_SWP_PAG_N",
        "shortage_var": "SHORT_SWP_PAG_N",
        "description": "SWP Project AG - North of Delta",
    },
    "swp_pag_s": {
        "delivery_var": "DEL_SWP_PAG_S",
        "shortage_var": "SHORT_SWP_PAG_S",
        "description": "SWP Project AG - South of Delta",
    },
    # CVP Project AG
    "cvp_pag_n": {
        "delivery_var": "DEL_CVP_PAG_N",
        "shortage_var": "SHORT_CVP_PAG_N",
        "description": "CVP Project AG - North of Delta",
    },
    "cvp_pag_s": {
        "delivery_var": "DEL_CVP_PAG_S",
        "shortage_var": "SHORT_CVP_PAG_S",
        "description": "CVP Project AG - South of Delta",
    },
    # CVP Settlement Contractors (North only in CalSim)
    "cvp_psc_n": {
        "delivery_var": "DEL_CVP_PSC_N",
        "shortage_var": "SHORT_CVP_PSC_N",
        "description": "CVP Settlement Contractors - North of Delta",
    },
    # CVP Exchange Contractors (South only in CalSim)
    "cvp_pex_s": {
        "delivery_var": "DEL_CVP_PEX_S",
        "shortage_var": "SHORT_CVP_PEX_S",
        "description": "CVP Exchange Contractors - South of Delta",
    },
}

# Computed aggregates — sum of component columns (matching V3 DataExtraction.py lines 282-302)
AG_COMPUTED_AGGREGATES = {
    "nod_ag": {
        "delivery_components": ["DEL_CVP_PAG_N", "DEL_SWP_PAG_N", "DEL_CVP_PSC_N"],
        "shortage_components": [
            "SHORT_CVP_PAG_N",
            "SHORT_SWP_PAG_N",
            "SHORT_CVP_PSC_N",
        ],
        "description": "Total NOD AG (Project + Settlement)",
    },
    "sod_ag": {
        "delivery_components": ["DEL_CVP_PAG_S", "DEL_SWP_PAG_S", "DEL_CVP_PEX_S"],
        "shortage_components": [
            "SHORT_CVP_PAG_S",
            "SHORT_SWP_PAG_S",
            "SHORT_CVP_PEX_S",
        ],
        "description": "Total SOD AG (Project + Exchange)",
    },
}

# Sacramento region WBAs (do NOT have GW_SHORT data)
SACRAMENTO_WBAS = [
    "02",
    "03",
    "04",
    "05",
    "06",
    "07N",
    "07S",
    "08N",
    "08S",
    "09",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15N",
    "15S",
    "16",
    "17N",
    "17S",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26N",
    "26S",
]


# =============================================================================
# DATA LOADING
# =============================================================================


def load_ag_demand_units(csv_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """
    Load agricultural demand unit metadata from du_agriculture_entity.csv.

    Returns dict keyed by du_id with unit details.
    """
    if csv_path is None:
        csv_path = DU_AGRICULTURE_CSV

    if not csv_path.exists():
        log.warning(f"du_agriculture_entity.csv not found at {csv_path}")
        return {}

    demand_units = {}
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            du_id = row.get("DU_ID", "")
            if du_id:
                demand_units[du_id] = {
                    "wba_id": row.get("WBA_ID", ""),
                    "hydrologic_region": row.get("hydrologic_region", ""),
                    "cs3_type": row.get("CS3_Type", ""),
                    "agency": row.get("agency", ""),
                    "provider": row.get("provider", ""),
                    "gw": row.get("gw", "1") == "1",
                    "sw": row.get("sw", "1") == "1",
                    "has_gis_data": row.get("has_gis_data", "True") == "True",
                }

    log.info(f"Loaded {len(demand_units)} agricultural demand units from {csv_path}")
    return demand_units


def load_dv_csv_from_s3(scenario_id: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Load CalSim DV output CSV from S3 bucket.

    Handles the DSS export format with 7 header rows.

    Returns (data_df, units_map) where *units_map* maps each column
    name to its declared unit string (e.g. ``"CFS"``, ``"TAF"``).
    """
    if not HAS_BOTO3:
        raise ImportError(
            "boto3 is required for S3 access. Install with: pip install boto3"
        )

    s3 = boto3.client("s3")
    keys = [k.format(scenario=scenario_id) for k in DV_OUTPUT_S3_KEYS]

    for key in keys:
        try:
            log.info(f"Trying S3 key: s3://{S3_BUCKET}/{key}")
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            raw_bytes = response["Body"].read()

            var_names, units_row, c_parts = parse_dss_csv_header(io.BytesIO(raw_bytes))
            units_map = build_units_map_first(var_names, units_row)

            data_df = pd.read_csv(
                io.BytesIO(raw_bytes), header=None, skiprows=7, low_memory=False
            )
            data_df = apply_columns_and_dedup(data_df, var_names, c_parts)

            log.info(f"Loaded DV: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
            return data_df, units_map

        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            log.warning(f"Error loading {key}: {e}")
            continue

    raise FileNotFoundError(f"Could not find CalSim output for {scenario_id} in S3")


def load_dv_csv_from_file(file_path: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Load CalSim DV output CSV from local file.

    Returns (data_df, units_map).
    """
    log.info(f"Loading DV from file: {file_path}")
    var_names, units_row, c_parts = parse_dss_csv_header(file_path)
    units_map = build_units_map_first(var_names, units_row)

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    data_df = apply_columns_and_dedup(data_df, var_names, c_parts)

    log.info(f"Loaded DV: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df, units_map


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add WaterYear, WaterMonth (1=Oct ... 12=Sep), and DaysInMonth columns.

    Handles both period-ending (DV: last day of month) and period-beginning
    (SV: first day of following month) date conventions. If day-of-month == 1,
    subtract one day to get the actual data period before deriving calendar fields.
    """
    df = df.copy()
    first_col = df.columns[0]
    df["DateTime"] = pd.to_datetime(df[first_col], errors="coerce")

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
        f"Date range: {df['DateTime'].min()} to {df['DateTime'].max()} "
        f"({df['WaterYear'].nunique()} water years)"
    )
    return df


# =============================================================================
# DEMAND UNIT STATISTICS
# =============================================================================


def calculate_du_demand_monthly(
    df: pd.DataFrame,
    du_id: str,
) -> List[Dict[str, Any]]:
    """
    Calculate monthly DEMAND statistics for an agricultural demand unit.

    Uses AW_{DU_ID} from the DV output. AW is the model's optimised Applied
    Water (= demand).  Originally CFS in the raw CSV, already converted to TAF
    by the time this function is called.
    """
    demand_var = f"AW_{du_id}"

    if demand_var not in df.columns:
        log.debug(f"No demand variable found for {du_id}: {demand_var}")
        return []

    df_copy = df.copy()
    df_copy["demand"] = df_copy[demand_var]

    results = []
    is_annual = (df_copy["WaterMonth"] == 0).all()

    if is_annual:
        data = df_copy["demand"].dropna()
        if data.empty:
            return []

        row = {
            "du_id": du_id,
            "water_month": 0,
            "demand_avg_taf": round(float(data.mean()), 2),
            "demand_cv": compute_cv(data),
            "sample_count": len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f"q{p}"] = round(float(np.percentile(data, p)), 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f"exc_p{p}"] = round(float(np.percentile(data, 100 - p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy["WaterMonth"] == wm]["demand"].dropna()
            if month_data.empty:
                continue

            row = {
                "du_id": du_id,
                "water_month": wm,
                "demand_avg_taf": round(float(month_data.mean()), 2),
                "demand_cv": compute_cv(month_data),
                "sample_count": len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f"q{p}"] = round(float(np.percentile(month_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f"exc_p{p}"] = round(float(np.percentile(month_data, 100 - p)), 2)

            results.append(row)

    return results


def calculate_du_sw_delivery_monthly(
    df: pd.DataFrame,
    du_id: str,
) -> List[Dict[str, Any]]:
    """
    Calculate monthly SURFACE WATER DELIVERY statistics for an agricultural demand unit.

    Uses DN_{DU_ID} from the merged DataFrame. DV columns are pre-converted
    to TAF before the merge.
    """
    sw_delivery_var = f"DN_{du_id}"

    if sw_delivery_var not in df.columns:
        if du_id in GW_ONLY_DU_IDS:
            gp_var = f"GP_{du_id}"
            ru_var = f"RU_{du_id}"
            if gp_var in df.columns:
                gp = pd.to_numeric(df[gp_var], errors="coerce").fillna(0)
                ru = pd.to_numeric(df[ru_var], errors="coerce").fillna(0) if ru_var in df.columns else 0
                df_copy = df.copy()
                df_copy["sw_delivery"] = gp + ru
                log.info(f"{du_id}: GW-only DU, synthesized delivery = GP + RU")
            else:
                log.debug(f"{du_id}: GW-only DU but GP_{du_id} not found")
                return []
        else:
            log.debug(
                f"No SW delivery variable found for {du_id}: {sw_delivery_var}"
            )
            return []
    else:
        df_copy = df.copy()
        df_copy["sw_delivery"] = df_copy[sw_delivery_var]

    results = []
    is_annual = (df_copy["WaterMonth"] == 0).all()

    if is_annual:
        data = df_copy["sw_delivery"].dropna()
        if data.empty:
            return []

        row = {
            "du_id": du_id,
            "water_month": 0,
            "sw_delivery_avg_taf": round(float(data.mean()), 2),
            "sw_delivery_cv": compute_cv(data),
            "sample_count": len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f"q{p}"] = round(float(np.percentile(data, p)), 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f"exc_p{p}"] = round(float(np.percentile(data, 100 - p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy["WaterMonth"] == wm]["sw_delivery"].dropna()
            if month_data.empty:
                continue

            row = {
                "du_id": du_id,
                "water_month": wm,
                "sw_delivery_avg_taf": round(float(month_data.mean()), 2),
                "sw_delivery_cv": compute_cv(month_data),
                "sample_count": len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f"q{p}"] = round(float(np.percentile(month_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f"exc_p{p}"] = round(float(np.percentile(month_data, 100 - p)), 2)

            results.append(row)

    return results


def calculate_du_gw_pumping_monthly(
    df: pd.DataFrame,
    du_id: str,
) -> List[Dict[str, Any]]:
    """
    Calculate monthly GROUNDWATER PUMPING statistics for an agricultural demand unit.

    Uses GP_{DU_ID} if available. Otherwise calculates as AW - DN.
    For groundwater-only DUs (no DN_*), GW pumping equals demand (AW_*).
    All values in the merged DataFrame are already in TAF.
    """
    demand_var = f"AW_{du_id}"
    sw_delivery_var = f"DN_{du_id}"
    gw_pumping_var = f"GP_{du_id}"

    has_explicit_gp = gw_pumping_var in df.columns
    has_demand = demand_var in df.columns
    has_sw_delivery = sw_delivery_var in df.columns

    if not has_demand and not has_explicit_gp:
        log.debug(f"No data to calculate GW pumping for {du_id}")
        return []

    df_copy = df.copy()

    if has_explicit_gp:
        df_copy["gw_pumping"] = df_copy[gw_pumping_var]
        is_calculated = False
    elif has_demand:
        df_copy["demand"] = df_copy[demand_var]
        if has_sw_delivery:
            df_copy["sw_delivery"] = df_copy[sw_delivery_var]
            df_copy["gw_pumping"] = df_copy["demand"] - df_copy["sw_delivery"]
        else:
            df_copy["gw_pumping"] = df_copy["demand"]
        is_calculated = True
    else:
        return []

    # Ensure non-negative (handle floating-point artifacts)
    df_copy["gw_pumping"] = df_copy["gw_pumping"].clip(lower=0)

    results = []
    is_annual = (df_copy["WaterMonth"] == 0).all()

    if is_annual:
        data = df_copy["gw_pumping"].dropna()
        if data.empty:
            return []

        row = {
            "du_id": du_id,
            "water_month": 0,
            "gw_pumping_avg_taf": round(float(data.mean()), 2),
            "gw_pumping_cv": compute_cv(data),
            "is_calculated": is_calculated,
            "sample_count": len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f"q{p}"] = round(float(np.percentile(data, p)), 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f"exc_p{p}"] = round(float(np.percentile(data, 100 - p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy["WaterMonth"] == wm]["gw_pumping"].dropna()
            if month_data.empty:
                continue

            row = {
                "du_id": du_id,
                "water_month": wm,
                "gw_pumping_avg_taf": round(float(month_data.mean()), 2),
                "gw_pumping_cv": compute_cv(month_data),
                "is_calculated": is_calculated,
                "sample_count": len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f"q{p}"] = round(float(np.percentile(month_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f"exc_p{p}"] = round(float(np.percentile(month_data, 100 - p)), 2)

            results.append(row)

    return results


def _find_shortage_var(df: pd.DataFrame, du_id: str, wba_id: str) -> Optional[str]:
    """Return the shortage column name for a DU, or None if unavailable.

    Sacramento region (WBAs 02-26): SHRTG_{DU_ID}  (kind='SHORTAGE')
    SJR/Tulare region:              GW_SHORT_{DU_ID} (kind='GW-RESTRICT-SHORT')
    """
    if wba_id in SACRAMENTO_WBAS:
        col = f"SHRTG_{du_id}"
    else:
        col = f"GW_SHORT_{du_id}"
    return col if col in df.columns else None


def calculate_du_shortage_monthly(
    df: pd.DataFrame,
    du_id: str,
    du_info: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Calculate monthly shortage for an agricultural demand unit.

    WRESL water balance: AW + RP = DN + GP + RU + SHORTAGE
    Sacramento region uses SHRTG_{DU_ID} (kind='SHORTAGE').
    SJR/Tulare region uses GW_SHORT_{DU_ID} (kind='GW-RESTRICT-SHORT').
    Both represent the slack variable in the meetAW constraint.

    Demand denominator for shortage % uses AW_{DU_ID} from DV (converted to TAF).
    """
    wba_id = du_info.get("wba_id", "")
    shortage_var = _find_shortage_var(df, du_id, wba_id)
    demand_var = f"AW_{du_id}"

    if shortage_var is None:
        log.debug(
            f"No shortage variable found for {du_id} "
            f"(tried {'SHRTG_' if wba_id in SACRAMENTO_WBAS else 'GW_SHORT_'}{du_id})"
        )
        return []

    df_copy = df.copy()
    df_copy["shortage"] = pd.to_numeric(
        df_copy[shortage_var], errors="coerce"
    ).clip(lower=0)

    if demand_var in df.columns:
        df_copy["demand"] = df_copy[demand_var]
    else:
        df_copy["demand"] = 0

    results = []
    is_annual = (df_copy["WaterMonth"] == 0).all()

    if is_annual:
        shortage_data = df_copy["shortage"].dropna()
        demand_data = df_copy["demand"].dropna()

        if shortage_data.empty:
            return []

        # Use threshold to filter out floating-point noise from CalSim solver
        shortage_count = (shortage_data > SHORTAGE_THRESHOLD_TAF).sum()

        shortage_pct = [
            safe_pct(s, d, label=f"{du_id} shortage%demand", logger=log)
            for s, d in zip(shortage_data, demand_data)
        ]
        avg_shortage_pct = np.mean(shortage_pct) if shortage_pct else 0

        row = {
            "du_id": du_id,
            "water_month": 0,
            "shortage_avg_taf": round(float(shortage_data.mean()), 2),
            "shortage_cv": compute_cv(shortage_data),
            "shortage_frequency_pct": round(
                (shortage_count / len(shortage_data)) * 100, 2
            ),
            "shortage_pct_of_demand_avg": round(float(avg_shortage_pct), 2),
            "sample_count": len(shortage_data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f"q{p}"] = round(float(np.percentile(shortage_data, p)), 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f"exc_p{p}"] = round(float(np.percentile(shortage_data, 100 - p)), 2)

        results.append(row)
    else:
        for wm in range(1, 13):
            mask = df_copy["WaterMonth"] == wm
            shortage_data = df_copy.loc[mask, "shortage"].dropna()
            demand_month = df_copy.loc[mask, "demand"].dropna()

            if shortage_data.empty:
                continue

            # Use threshold to filter out floating-point noise from CalSim solver
            shortage_count = (shortage_data > SHORTAGE_THRESHOLD_TAF).sum()

            shortage_pct = [
                safe_pct(s, d, label=f"{du_id} m{wm} shortage%demand", logger=log)
                for s, d in zip(shortage_data.values, demand_month.values)
            ]
            avg_shortage_pct = np.mean(shortage_pct) if shortage_pct else 0

            row = {
                "du_id": du_id,
                "water_month": wm,
                "shortage_avg_taf": round(float(shortage_data.mean()), 2),
                "shortage_cv": compute_cv(shortage_data),
                "shortage_frequency_pct": round(
                    (shortage_count / len(shortage_data)) * 100, 2
                ),
                "shortage_pct_of_demand_avg": round(float(avg_shortage_pct), 2),
                "sample_count": len(shortage_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f"q{p}"] = round(float(np.percentile(shortage_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f"exc_p{p}"] = round(
                    float(np.percentile(shortage_data, 100 - p)), 2
                )

            results.append(row)

    return results


def calculate_du_period_summary(
    df: pd.DataFrame,
    du_id: str,
    du_info: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary for an agricultural demand unit.

    All values in the merged DataFrame are already in TAF.
    """
    demand_var = f"AW_{du_id}"
    sw_delivery_var = f"DN_{du_id}"
    gw_pumping_var = f"GP_{du_id}"
    wba_id = du_info.get("wba_id", "")
    shortage_var = _find_shortage_var(df, du_id, wba_id)

    if demand_var not in df.columns:
        return None

    df_copy = df.copy()
    df_copy["demand"] = df_copy[demand_var]

    has_sw_delivery = sw_delivery_var in df.columns
    if has_sw_delivery:
        df_copy["sw_delivery"] = df_copy[sw_delivery_var]
    elif du_id in GW_ONLY_DU_IDS:
        gp_var = f"GP_{du_id}"
        ru_var = f"RU_{du_id}"
        gp = pd.to_numeric(df_copy.get(gp_var, 0), errors="coerce").fillna(0) if gp_var in df_copy.columns else 0
        ru = pd.to_numeric(df_copy.get(ru_var, 0), errors="coerce").fillna(0) if ru_var in df_copy.columns else 0
        df_copy["sw_delivery"] = gp + ru
        has_sw_delivery = True
    else:
        df_copy["sw_delivery"] = 0

    has_explicit_gp = gw_pumping_var in df.columns
    if has_explicit_gp:
        df_copy["gw_pumping"] = df_copy[gw_pumping_var]
    else:
        df_copy["gw_pumping"] = (df_copy["demand"] - df_copy["sw_delivery"]).clip(
            lower=0
        )

    water_years = sorted(df_copy["WaterYear"].unique())

    result = {
        "du_id": du_id,
        "simulation_start_year": int(water_years[0]),
        "simulation_end_year": int(water_years[-1]),
        "total_years": len(water_years),
    }

    # Annual DEMAND statistics (from DV AW_*)
    annual_demand = df_copy.groupby("WaterYear")["demand"].sum()
    result["annual_demand_avg_taf"] = round(float(annual_demand.mean()), 2)
    result["annual_demand_cv"] = compute_cv(annual_demand)

    for p in EXCEEDANCE_PERCENTILES:
        result[f"demand_exc_p{p}"] = round(
            float(np.percentile(annual_demand, 100 - p)), 2
        )

    # Annual SW DELIVERY statistics (from DV DN_*)
    annual_sw_delivery = df_copy.groupby("WaterYear")["sw_delivery"].sum()
    result["annual_sw_delivery_avg_taf"] = round(float(annual_sw_delivery.mean()), 2)
    result["annual_sw_delivery_cv"] = compute_cv(annual_sw_delivery)

    for p in EXCEEDANCE_PERCENTILES:
        result[f"sw_delivery_exc_p{p}"] = round(
            float(np.percentile(annual_sw_delivery, 100 - p)), 2
        )

    # Annual GW PUMPING statistics (from GP_* or calculated)
    annual_gw_pumping = df_copy.groupby("WaterYear")["gw_pumping"].sum()
    result["annual_gw_pumping_avg_taf"] = round(float(annual_gw_pumping.mean()), 2)
    result["annual_gw_pumping_cv"] = compute_cv(annual_gw_pumping)

    for p in EXCEEDANCE_PERCENTILES:
        result[f"gw_pumping_exc_p{p}"] = round(
            float(np.percentile(annual_gw_pumping, 100 - p)), 2
        )

    # GW pumping as percentage of demand
    result["gw_pumping_pct_of_demand"] = round(
        safe_pct(
            result["annual_gw_pumping_avg_taf"],
            result["annual_demand_avg_taf"],
            label=f"{du_id} GW%demand",
            logger=log,
        ),
        2,
    )

    # Shortage statistics (SHRTG_* for Sacramento, GW_SHORT_* for SJR/Tulare)
    has_shortage = shortage_var is not None

    if has_shortage:
        df_copy["shortage"] = pd.to_numeric(
            df_copy[shortage_var], errors="coerce"
        ).clip(lower=0)
        annual_shortage = df_copy.groupby("WaterYear")["shortage"].sum()
        shortage_years = (annual_shortage > SHORTAGE_THRESHOLD_TAF).sum()

        result["annual_shortage_avg_taf"] = round(float(annual_shortage.mean()), 2)
        result["shortage_years_count"] = int(shortage_years)
        result["shortage_frequency_pct"] = round(
            (shortage_years / len(water_years)) * 100, 2
        )

        for p in EXCEEDANCE_PERCENTILES:
            result[f"shortage_exc_p{p}"] = round(
                float(np.percentile(annual_shortage, 100 - p)), 2
            )

        shortage_pct = [
            safe_pct(s, d, label=f"{du_id} shortage%demand", logger=log)
            for s, d in zip(annual_shortage.values, annual_demand.values)
        ]
        result["annual_shortage_pct_of_demand"] = round(float(np.mean(shortage_pct)), 2)

        met = result["annual_demand_avg_taf"] - result["annual_shortage_avg_taf"]
        result["reliability_pct"] = round(
            safe_pct(
                met,
                result["annual_demand_avg_taf"],
                label=f"{du_id} reliability",
                logger=log,
            ),
            2,
        )
        result["avg_pct_demand_met"] = result["reliability_pct"]
    else:
        result["annual_shortage_avg_taf"] = None
        result["shortage_years_count"] = None
        result["shortage_frequency_pct"] = None
        result["annual_shortage_pct_of_demand"] = None
        for p in EXCEEDANCE_PERCENTILES:
            result[f"shortage_exc_p{p}"] = None
        result["reliability_pct"] = 100.0
        result["avg_pct_demand_met"] = 100.0

    return result


# =============================================================================
# AGGREGATE STATISTICS
# =============================================================================


def calculate_aggregate_monthly(
    df: pd.DataFrame,
    aggregate_code: str,
    delivery_var: str,
    shortage_var: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Calculate monthly statistics for an agricultural aggregate.

    Uses pre-computed aggregate variables like DEL_SWP_PAG and SHORT_SWP_PAG.
    All values are pre-converted to TAF before this function is called.
    """
    if delivery_var not in df.columns:
        log.debug(f"No aggregate delivery variable found: {delivery_var}")
        return []

    df_copy = df.copy()
    df_copy["delivery"] = df_copy[delivery_var]

    has_shortage = shortage_var and shortage_var in df.columns
    if has_shortage:
        df_copy["shortage"] = pd.to_numeric(
            df_copy[shortage_var], errors="coerce"
        ).clip(lower=0)

    results = []
    is_annual = (df_copy["WaterMonth"] == 0).all()

    if is_annual:
        data = df_copy["delivery"].dropna()
        if data.empty:
            return []

        row = {
            "aggregate_code": aggregate_code,
            "water_month": 0,
            "delivery_avg_taf": round(float(data.mean()), 2),
            "delivery_cv": compute_cv(data),
            "sample_count": len(data),
        }

        for p in DELIVERY_PERCENTILES:
            row[f"q{p}"] = round(float(np.percentile(data, p)), 2)

        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f"exc_p{p}"] = round(float(np.percentile(data, 100 - p)), 2)

        # Shortage statistics
        if has_shortage:
            shortage_data = df_copy["shortage"].dropna()
            if not shortage_data.empty:
                row["shortage_avg_taf"] = round(float(shortage_data.mean()), 2)
                row["shortage_cv"] = compute_cv(shortage_data)
                # Use threshold to filter floating-point noise
                row["shortage_frequency_pct"] = round(
                    (
                        (shortage_data > SHORTAGE_THRESHOLD_TAF).sum()
                        / len(shortage_data)
                    )
                    * 100,
                    2,
                )

        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_copy[df_copy["WaterMonth"] == wm]["delivery"].dropna()
            if month_data.empty:
                continue

            row = {
                "aggregate_code": aggregate_code,
                "water_month": wm,
                "delivery_avg_taf": round(float(month_data.mean()), 2),
                "delivery_cv": compute_cv(month_data),
                "sample_count": len(month_data),
            }

            for p in DELIVERY_PERCENTILES:
                row[f"q{p}"] = round(float(np.percentile(month_data, p)), 2)

            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f"exc_p{p}"] = round(float(np.percentile(month_data, 100 - p)), 2)

            # Shortage statistics
            if has_shortage:
                shortage_month = df_copy[df_copy["WaterMonth"] == wm][
                    "shortage"
                ].dropna()
                if not shortage_month.empty:
                    row["shortage_avg_taf"] = round(float(shortage_month.mean()), 2)
                    row["shortage_cv"] = compute_cv(shortage_month)
                    # Use threshold to filter floating-point noise
                    row["shortage_frequency_pct"] = round(
                        (
                            (shortage_month > SHORTAGE_THRESHOLD_TAF).sum()
                            / len(shortage_month)
                        )
                        * 100,
                        2,
                    )

            results.append(row)

    return results


def calculate_aggregate_period_summary(
    df: pd.DataFrame,
    aggregate_code: str,
    delivery_var: str,
    shortage_var: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Calculate period-of-record summary for an agricultural aggregate.

    All values are pre-converted to TAF before this function is called.
    """
    if delivery_var not in df.columns:
        return None

    df_copy = df.copy()
    df_copy["delivery"] = df_copy[delivery_var]

    has_shortage = shortage_var and shortage_var in df.columns
    if has_shortage:
        df_copy["shortage"] = pd.to_numeric(
            df_copy[shortage_var], errors="coerce"
        ).clip(lower=0)

    water_years = sorted(df_copy["WaterYear"].unique())

    result = {
        "aggregate_code": aggregate_code,
        "simulation_start_year": int(water_years[0]),
        "simulation_end_year": int(water_years[-1]),
        "total_years": len(water_years),
    }

    annual_delivery = df_copy.groupby("WaterYear")["delivery"].sum()
    result["annual_delivery_avg_taf"] = round(float(annual_delivery.mean()), 2)
    result["annual_delivery_cv"] = compute_cv(annual_delivery)

    # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
    for p in EXCEEDANCE_PERCENTILES:
        result[f"delivery_exc_p{p}"] = round(
            float(np.percentile(annual_delivery, 100 - p)), 2
        )

    # Shortage statistics
    if has_shortage:
        annual_shortage = df_copy.groupby("WaterYear")["shortage"].sum()
        # Use threshold to filter floating-point noise from CalSim solver
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

        demand_avg = (
            result["annual_delivery_avg_taf"] + result["annual_shortage_avg_taf"]
        )
        if demand_avg > 0:
            result["reliability_pct"] = round(
                (result["annual_delivery_avg_taf"] / demand_avg) * 100, 2
            )
        else:
            result["reliability_pct"] = None
    else:
        result["annual_shortage_avg_taf"] = None
        result["shortage_years_count"] = None
        result["shortage_frequency_pct"] = None
        result["reliability_pct"] = None

    return result


# =============================================================================
# MAIN CALCULATION FUNCTION
# =============================================================================


def calculate_all_ag_statistics(
    scenario_id: str,
    dv_path: Optional[str] = None,
) -> Tuple[
    List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]
]:
    """
    Calculate all statistics for agricultural demand units for a scenario.

    All data comes from the CalSim DV output CSV:
      AW_*       — Applied Water = DEMAND (CFS, converted to TAF)
      DN_*       — Net Delivery = SURFACE WATER DELIVERY (CFS → TAF)
      GP_*       — Groundwater Pumping (CFS → TAF)
      SHRTG_*    — Shortage, Sacramento region (CFS → TAF)
      GW_SHORT_* — GW Restriction Shortage, SJR/Tulare (CFS → TAF)
      DEL_*, SHORT_* — Project-level aggregate delivery/shortage (CFS → TAF)

    Returns:
        Tuple of (
            du_demand_monthly_rows,
            du_sw_delivery_monthly_rows,
            du_gw_pumping_monthly_rows,
            du_shortage_monthly_rows,
            du_period_summary_rows,
            aggregate_monthly_rows,
            aggregate_period_summary_rows
        )
    """
    log.info(f"Processing scenario: {scenario_id}")

    demand_units = load_ag_demand_units()

    # ── Load DV (all variables: AW, DN, GP, GW_SHORT, DEL, SHORT) ──
    if dv_path:
        dv_df, dv_units = load_dv_csv_from_file(dv_path)
    else:
        dv_df, dv_units = load_dv_csv_from_s3(scenario_id)

    dv_df = add_water_year_month(dv_df)

    # Convert CFS columns to TAF, using the units declared in the header.
    # Only convert columns whose header unit is CFS; skip those already in TAF.
    AG_CFS_PREFIXES = ("AW_", "DN_", "GP_", "RU_", "GW_SHORT_", "SHRTG_", "DEL_", "SHORT_")
    cfs_cols = []
    taf_already = []
    other_unit = []
    for c in dv_df.columns:
        if not any(c.startswith(p) for p in AG_CFS_PREFIXES):
            continue
        unit = dv_units.get(c, "").upper()
        if unit == "CFS":
            cfs_cols.append(c)
        elif unit == "TAF":
            taf_already.append(c)
        else:
            other_unit.append((c, unit))

    for col in cfs_cols:
        dv_df[col] = (
            pd.to_numeric(dv_df[col], errors="coerce")
            * dv_df["DaysInMonth"]
            * CFS_TO_TAF_PER_DAY
        )
    for col in taf_already:
        dv_df[col] = pd.to_numeric(dv_df[col], errors="coerce")

    log.info(
        f"Converted {len(cfs_cols)} DV CFS→TAF columns (prefixes: {AG_CFS_PREFIXES})"
    )
    if taf_already:
        log.info(f"Kept {len(taf_already)} DV columns already in TAF (no conversion)")
    if other_unit:
        log.warning(
            f"Unexpected units for AG columns: {[(c, u) for c, u in other_unit[:5]]}"
        )

    # All data comes from DV — no SV merge needed
    df = dv_df
    log.info(f"DV DataFrame: {len(df)} rows, {len(df.columns)} columns")

    available_columns = list(df.columns)

    # Find all AW_* columns (from DV) to get the list of DUs
    aw_columns = [
        c
        for c in available_columns
        if c.startswith("AW_")
        and not any(
            suffix in c for suffix in ["_ANN_DV", "_WLOSS", "_ADD_DV", "_ANNDV"]
        )
    ]
    all_du_ids_in_data = [c.replace("AW_", "") for c in aw_columns]

    # Filter to only DUs in du_agriculture_entity.csv.  The DV CSV contains
    # AW_* columns for ALL demand units (ag, refuge, urban).  Processing
    # refuge DUs here would produce wrong results because CalSim water
    # accounting differs for refuge DUs (GP has different semantics, the
    # AW = DN + GP + RU identity does not hold).
    ag_entity_ids = set(demand_units.keys())
    du_ids_in_data = [d for d in all_du_ids_in_data if d in ag_entity_ids]
    skipped = [d for d in all_du_ids_in_data if d not in ag_entity_ids]
    if skipped:
        log.info(
            f"Skipped {len(skipped)} non-ag DUs found in DV data "
            f"(e.g. refuge): {skipped[:5]}{'...' if len(skipped) > 5 else ''}"
        )
    log.info(
        f"Found {len(du_ids_in_data)} agricultural demand units with demand data (from DV)"
    )

    # Also find DN_* columns (surface water delivery)
    dn_columns = [
        c
        for c in available_columns
        if c.startswith("DN_") and not c.endswith("_ANN_DV")
    ]
    log.info(f"Found {len(dn_columns)} DN_* columns for surface water delivery")

    # Find GP_* columns (explicit groundwater pumping)
    gp_columns = [
        c for c in available_columns if c.startswith("GP_") and not c.endswith("_NU")
    ]
    log.info(f"Found {len(gp_columns)} GP_* columns for explicit GW pumping")

    # ── Safeguards: validate converted data before computing statistics ──
    validate_water_balance(df, du_ids_in_data, log)
    converted_cols = [c for c in cfs_cols if c in df.columns]
    if converted_cols:
        flagged = check_post_conversion_magnitude(df, converted_cols, logger=log)
        if flagged:
            log.warning(
                f"{flagged} columns exceed monthly TAF sanity limit after conversion"
            )

    du_demand_monthly_rows = []
    du_sw_delivery_monthly_rows = []
    du_gw_pumping_monthly_rows = []
    du_shortage_monthly_rows = []
    du_period_summary_rows = []

    demand_count = 0
    sw_delivery_count = 0
    gw_pumping_count = 0
    shortage_count = 0

    for du_id in du_ids_in_data:
        # Get DU info from entity table (if available)
        du_info = demand_units.get(
            du_id,
            {
                "wba_id": du_id.split("_")[0] if "_" in du_id else "",
                "hydrologic_region": "",
                "cs3_type": "",
            },
        )

        demand_rows = calculate_du_demand_monthly(df, du_id)
        if demand_rows:
            demand_count += 1
            for row in demand_rows:
                row["scenario_short_code"] = scenario_id
            du_demand_monthly_rows.extend(demand_rows)

        sw_delivery_rows = calculate_du_sw_delivery_monthly(df, du_id)
        if sw_delivery_rows:
            sw_delivery_count += 1
            for row in sw_delivery_rows:
                row["scenario_short_code"] = scenario_id
            du_sw_delivery_monthly_rows.extend(sw_delivery_rows)

        gw_pumping_rows = calculate_du_gw_pumping_monthly(df, du_id)
        if gw_pumping_rows:
            gw_pumping_count += 1
            for row in gw_pumping_rows:
                row["scenario_short_code"] = scenario_id
            du_gw_pumping_monthly_rows.extend(gw_pumping_rows)

        shortage_rows = calculate_du_shortage_monthly(df, du_id, du_info)
        if shortage_rows:
            shortage_count += 1
            for row in shortage_rows:
                row["scenario_short_code"] = scenario_id
            du_shortage_monthly_rows.extend(shortage_rows)

        summary = calculate_du_period_summary(df, du_id, du_info)
        if summary:
            summary["scenario_short_code"] = scenario_id
            du_period_summary_rows.append(summary)

    log.info(
        f"Processed {demand_count} DUs with demand, {sw_delivery_count} with SW delivery, "
        f"{gw_pumping_count} with GW pumping, {shortage_count} with shortage data"
    )

    # Calculate aggregate statistics
    aggregate_monthly_rows = []
    aggregate_period_summary_rows = []

    # Direct aggregates (single DV column each)
    for agg_code, agg_info in AG_AGGREGATES.items():
        delivery_var = agg_info["delivery_var"]
        shortage_var = agg_info.get("shortage_var")

        monthly_rows = calculate_aggregate_monthly(
            df, agg_code, delivery_var, shortage_var
        )
        for row in monthly_rows:
            row["scenario_short_code"] = scenario_id
        aggregate_monthly_rows.extend(monthly_rows)

        summary = calculate_aggregate_period_summary(
            df, agg_code, delivery_var, shortage_var
        )
        if summary:
            summary["scenario_short_code"] = scenario_id
            aggregate_period_summary_rows.append(summary)

    # Computed aggregates (sum of component columns — matching V3)
    for agg_code, agg_info in AG_COMPUTED_AGGREGATES.items():
        del_cols = [c for c in agg_info["delivery_components"] if c in df.columns]
        short_cols = [c for c in agg_info["shortage_components"] if c in df.columns]

        if not del_cols:
            log.warning(
                f"No delivery components found for computed aggregate {agg_code}"
            )
            continue

        computed_del_var = f"_COMPUTED_DEL_{agg_code.upper()}"
        df[computed_del_var] = sum(
            pd.to_numeric(df[c], errors="coerce").fillna(0) for c in del_cols
        )

        computed_short_var = None
        if short_cols:
            computed_short_var = f"_COMPUTED_SHORT_{agg_code.upper()}"
            df[computed_short_var] = sum(
                pd.to_numeric(df[c], errors="coerce").fillna(0) for c in short_cols
            )

        log.info(
            f"Computed aggregate {agg_code}: DEL from {del_cols}, SHORT from {short_cols}"
        )

        monthly_rows = calculate_aggregate_monthly(
            df, agg_code, computed_del_var, computed_short_var
        )
        for row in monthly_rows:
            row["scenario_short_code"] = scenario_id
        aggregate_monthly_rows.extend(monthly_rows)

        summary = calculate_aggregate_period_summary(
            df, agg_code, computed_del_var, computed_short_var
        )
        if summary:
            summary["scenario_short_code"] = scenario_id
            aggregate_period_summary_rows.append(summary)

    total_aggs = len(AG_AGGREGATES) + len(AG_COMPUTED_AGGREGATES)
    log.info(
        f"Processed {total_aggs} aggregates ({len(AG_AGGREGATES)} direct + {len(AG_COMPUTED_AGGREGATES)} computed)"
    )

    log.info(
        f"Generated: {len(du_demand_monthly_rows)} DU demand monthly, "
        f"{len(du_sw_delivery_monthly_rows)} DU SW delivery monthly, "
        f"{len(du_gw_pumping_monthly_rows)} DU GW pumping monthly, "
        f"{len(du_shortage_monthly_rows)} DU shortage monthly, "
        f"{len(du_period_summary_rows)} DU period summary, "
        f"{len(aggregate_monthly_rows)} aggregate monthly, "
        f"{len(aggregate_period_summary_rows)} aggregate period summary rows"
    )

    return (
        du_demand_monthly_rows,
        du_sw_delivery_monthly_rows,
        du_gw_pumping_monthly_rows,
        du_shortage_monthly_rows,
        du_period_summary_rows,
        aggregate_monthly_rows,
        aggregate_period_summary_rows,
    )


def convert_numpy(val):
    """Convert numpy types to Python native types."""
    if val is None:
        return None
    if isinstance(val, (np.integer, np.int64, np.int32)):
        return int(val)
    if isinstance(val, (np.floating, np.float64, np.float32)):
        return float(val)
    return val


def save_to_database(
    scenario_ids: List[str],
    du_demand_monthly: List[Dict],
    du_sw_delivery_monthly: List[Dict],
    du_gw_pumping_monthly: List[Dict],
    du_shortage_monthly: List[Dict],
    du_period_summary: List[Dict],
    aggregate_monthly: List[Dict],
    aggregate_period_summary: List[Dict],
):
    """Save all statistics to database.

    Tables used (after migration 04_add_sw_delivery_gw_pumping_tables.sql):
    - ag_du_demand_monthly (renamed from ag_du_delivery_monthly)
    - ag_du_sw_delivery_monthly (NEW)
    - ag_du_gw_pumping_monthly (NEW)
    - ag_du_shortage_monthly
    - ag_du_period_summary
    - ag_aggregate_monthly
    - ag_aggregate_period_summary
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        log.error("DATABASE_URL not set. Cannot save to database.")
        return False

    if not HAS_PSYCOPG2:
        log.error("psycopg2 not installed. Cannot save to database.")
        return False

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        # Delete existing data for these scenarios
        for scenario_id in scenario_ids:
            cur.execute(
                "DELETE FROM ag_du_demand_monthly WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            cur.execute(
                "DELETE FROM ag_du_sw_delivery_monthly WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            cur.execute(
                "DELETE FROM ag_du_gw_pumping_monthly WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            cur.execute(
                "DELETE FROM ag_du_shortage_monthly WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            cur.execute(
                "DELETE FROM ag_du_period_summary WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            cur.execute(
                "DELETE FROM ag_aggregate_monthly WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            cur.execute(
                "DELETE FROM ag_aggregate_period_summary WHERE scenario_short_code = %s",
                (scenario_id,),
            )
            log.info(f"Cleared existing data for scenario {scenario_id}")

        # Insert DU DEMAND monthly (from AW_* - renamed from delivery)
        if du_demand_monthly:
            cols = [
                "scenario_short_code",
                "du_id",
                "water_month",
                "demand_avg_taf",
                "demand_cv",
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
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_demand_monthly
            ]
            insert_sql = (
                f"INSERT INTO ag_du_demand_monthly ({', '.join(cols)}) VALUES %s"
            )
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU demand monthly rows")

        # Insert DU SW DELIVERY monthly (from DN_* - NEW)
        if du_sw_delivery_monthly:
            cols = [
                "scenario_short_code",
                "du_id",
                "water_month",
                "sw_delivery_avg_taf",
                "sw_delivery_cv",
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
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_sw_delivery_monthly
            ]
            insert_sql = (
                f"INSERT INTO ag_du_sw_delivery_monthly ({', '.join(cols)}) VALUES %s"
            )
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU SW delivery monthly rows")

        # Insert DU GW PUMPING monthly (from GP_* or calculated - NEW)
        if du_gw_pumping_monthly:
            cols = [
                "scenario_short_code",
                "du_id",
                "water_month",
                "gw_pumping_avg_taf",
                "gw_pumping_cv",
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
                "is_calculated",
                "sample_count",
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_gw_pumping_monthly
            ]
            insert_sql = (
                f"INSERT INTO ag_du_gw_pumping_monthly ({', '.join(cols)}) VALUES %s"
            )
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU GW pumping monthly rows")

        # Insert DU shortage monthly
        if du_shortage_monthly:
            cols = [
                "scenario_short_code",
                "du_id",
                "water_month",
                "shortage_avg_taf",
                "shortage_cv",
                "shortage_frequency_pct",
                "shortage_pct_of_demand_avg",
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
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_shortage_monthly
            ]
            insert_sql = (
                f"INSERT INTO ag_du_shortage_monthly ({', '.join(cols)}) VALUES %s"
            )
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU shortage monthly rows")

        # Insert DU period summary (with updated column names)
        if du_period_summary:
            cols = [
                "scenario_short_code",
                "du_id",
                "simulation_start_year",
                "simulation_end_year",
                "total_years",
                "annual_demand_avg_taf",
                "annual_demand_cv",
                "demand_exc_p5",
                "demand_exc_p10",
                "demand_exc_p25",
                "demand_exc_p50",
                "demand_exc_p75",
                "demand_exc_p90",
                "demand_exc_p95",
                "annual_sw_delivery_avg_taf",
                "annual_sw_delivery_cv",
                "sw_delivery_exc_p5",
                "sw_delivery_exc_p10",
                "sw_delivery_exc_p25",
                "sw_delivery_exc_p50",
                "sw_delivery_exc_p75",
                "sw_delivery_exc_p90",
                "sw_delivery_exc_p95",
                "annual_gw_pumping_avg_taf",
                "annual_gw_pumping_cv",
                "gw_pumping_exc_p5",
                "gw_pumping_exc_p10",
                "gw_pumping_exc_p25",
                "gw_pumping_exc_p50",
                "gw_pumping_exc_p75",
                "gw_pumping_exc_p90",
                "gw_pumping_exc_p95",
                "gw_pumping_pct_of_demand",
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
                "annual_shortage_pct_of_demand",
                "reliability_pct",
                "avg_pct_demand_met",
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in du_period_summary
            ]
            insert_sql = (
                f"INSERT INTO ag_du_period_summary ({', '.join(cols)}) VALUES %s"
            )
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} DU period summary rows")

        # Insert aggregate monthly
        if aggregate_monthly:
            cols = [
                "scenario_short_code",
                "aggregate_code",
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
                "shortage_avg_taf",
                "shortage_cv",
                "shortage_frequency_pct",
                "sample_count",
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in aggregate_monthly
            ]
            insert_sql = (
                f"INSERT INTO ag_aggregate_monthly ({', '.join(cols)}) VALUES %s"
            )
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} aggregate monthly rows")

        # Insert aggregate period summary
        if aggregate_period_summary:
            cols = [
                "scenario_short_code",
                "aggregate_code",
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
            ]
            values = [
                tuple(convert_numpy(row.get(col)) for col in cols)
                for row in aggregate_period_summary
            ]
            insert_sql = (
                f"INSERT INTO ag_aggregate_period_summary ({', '.join(cols)}) VALUES %s"
            )
            execute_values(cur, insert_sql, values)
            log.info(f"Inserted {len(values)} aggregate period summary rows")

        conn.commit()
        cur.close()
        conn.close()
        log.info("Database save complete")
        return True

    except Exception as e:
        log.error(f"Database error: {e}")
        raise


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Calculate demand, delivery, and shortage statistics for agricultural demand units"
    )
    parser.add_argument("--scenario", "-s", help="Scenario ID (e.g., s0020)")
    parser.add_argument(
        "--all-scenarios", action="store_true", help="Process all known scenarios"
    )
    parser.add_argument(
        "--dv-path", help="Local CalSim DV output CSV file path (instead of S3)"
    )
    parser.add_argument(
        "--csv-path",
        help="(Deprecated) Alias for --dv-path for backwards compatibility",
        dest="csv_path_legacy",
    )
    parser.add_argument(
        "--output-json", action="store_true", help="Output results as JSON"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Calculate but do not save output"
    )

    args = parser.parse_args()

    if args.csv_path_legacy and not args.dv_path:
        args.dv_path = args.csv_path_legacy

    if not args.scenario and not args.all_scenarios:
        parser.error("Either --scenario or --all-scenarios is required")

    scenarios_to_process = SCENARIOS if args.all_scenarios else [args.scenario]

    all_du_demand = []
    all_du_sw_delivery = []
    all_du_gw_pumping = []
    all_du_shortage = []
    all_du_summary = []
    all_agg_monthly = []
    all_agg_summary = []

    for scenario_id in scenarios_to_process:
        try:
            results = calculate_all_ag_statistics(
                scenario_id,
                dv_path=args.dv_path,
            )
            (
                du_demand,
                du_sw_delivery,
                du_gw_pumping,
                du_shortage,
                du_summary,
                agg_monthly,
                agg_summary,
            ) = results

            all_du_demand.extend(du_demand)
            all_du_sw_delivery.extend(du_sw_delivery)
            all_du_gw_pumping.extend(du_gw_pumping)
            all_du_shortage.extend(du_shortage)
            all_du_summary.extend(du_summary)
            all_agg_monthly.extend(agg_monthly)
            all_agg_summary.extend(agg_summary)

        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise

    if args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")
        log.info(
            f"Total: {len(all_du_demand)} DU demand monthly, "
            f"{len(all_du_sw_delivery)} DU SW delivery monthly, "
            f"{len(all_du_gw_pumping)} DU GW pumping monthly, "
            f"{len(all_du_shortage)} DU shortage monthly, "
            f"{len(all_du_summary)} DU period summary, "
            f"{len(all_agg_monthly)} aggregate monthly, "
            f"{len(all_agg_summary)} aggregate period summary rows"
        )
        return

    if args.output_json:
        output = {
            "du_demand_monthly": all_du_demand,
            "du_sw_delivery_monthly": all_du_sw_delivery,
            "du_gw_pumping_monthly": all_du_gw_pumping,
            "du_shortage_monthly": all_du_shortage,
            "du_period_summary": all_du_summary,
            "aggregate_monthly": all_agg_monthly,
            "aggregate_period_summary": all_agg_summary,
        }
        print(json.dumps(output, indent=2))
        return

    # Save to database
    scenario_ids = list(set(row["scenario_short_code"] for row in all_du_demand))
    save_to_database(
        scenario_ids,
        all_du_demand,
        all_du_sw_delivery,
        all_du_gw_pumping,
        all_du_shortage,
        all_du_summary,
        all_agg_monthly,
        all_agg_summary,
    )

    log.info("Total rows saved:")
    log.info(f"  DU demand monthly: {len(all_du_demand)}")
    log.info(f"  DU SW delivery monthly: {len(all_du_sw_delivery)}")
    log.info(f"  DU GW pumping monthly: {len(all_du_gw_pumping)}")
    log.info(f"  DU shortage monthly: {len(all_du_shortage)}")
    log.info(f"  DU period summary: {len(all_du_summary)}")
    log.info(f"  Aggregate monthly: {len(all_agg_monthly)}")
    log.info(f"  Aggregate period summary: {len(all_agg_summary)}")


if __name__ == "__main__":
    main()
