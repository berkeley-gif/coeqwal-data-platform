#!/usr/bin/env python3
"""
calculate_delta_statistics.py - outflow, X2, and salinity at compliance points.

Variables (verified against COEQWAL_V3 variable_groupings.csv and metrics.py):

  Delta Outflow:
    NDO              — Net Delta Outflow (CFS, FLOW-NDO)

  X2 Position (2 ppt salinity intrusion distance from Golden Gate):
    X2_PRV_KM        — Previous month X2 position (KM)
    April X2: X2_PRV_KM filtered to month=4
    September X2: X2_PRV_KM filtered to month=9

  Salinity at Compliance Points (EC = electrical conductivity, UMHOS/CM):
    EM_EC_MONTH      — Emmaton
    JP_EC_MONTH      — Jersey Point
    RS_EC_MONTH      — Rock Slough
    CO_EC_MONTH      — Collinsville

  Salinity at Pumping Plants (EC, UMHOS/CM, 14-day max):
    BANKSEC_MAX14DAY — Banks Pumping Plant (SWP)
    TRACYEC_MAX14DAY — Tracy / Jones Pumping Plant (CVP)

V3 metrics (metrics.py lines 627-665):
  - NDO: annual avg, Sept avg, Sept CV
  - X2_PRV_KM: Fall (Sep-Nov) avg/CV, Spring (Mar-May) avg/CV
  - EM_EC_MONTH, JP_EC_MONTH: Fall avg, Spring avg

Usage:
    python calculate_delta_statistics.py --scenario s0020
    python calculate_delta_statistics.py --scenario s0020 --csv-path /path/to/dv.csv
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from units import CFS_TO_TAF_PER_DAY, CV_MIN_MEAN_TAF  # noqa: E402

try:
    import boto3
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False
    ClientError = None

# Optional: only needed for DB writes. Dry-run skips the writer path entirely.
try:
    from psycopg2.extras import execute_values  # noqa: F401
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("delta_statistics")

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from etl.common import S3_BUCKET, get_db_connection  # noqa: E402
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# =============================================================================
# DELTA VARIABLE DEFINITIONS (verified against V3 and s0020 DV CSV)
# =============================================================================

DELTA_VARIABLES = {
    "ndo": {
        "var": "NDO",
        "label": "Net Delta Outflow",
        "native_unit": "CFS",
        "convert_to_taf": True,
        "category": "outflow",
    },
    "x2": {
        "var": "X2_PRV_KM",
        "label": "X2 Position (2 ppt isohaline)",
        "native_unit": "KM",
        "convert_to_taf": False,
        "category": "x2",
    },
    "em_ec": {
        "var": "EM_EC_MONTH",
        "label": "Emmaton EC",
        "native_unit": "UMHOS/CM",
        "convert_to_taf": False,
        "category": "salinity_compliance",
    },
    "jp_ec": {
        "var": "JP_EC_MONTH",
        "label": "Jersey Point EC",
        "native_unit": "UMHOS/CM",
        "convert_to_taf": False,
        "category": "salinity_compliance",
    },
    "rs_ec": {
        "var": "RS_EC_MONTH",
        "label": "Rock Slough EC",
        "native_unit": "UMHOS/CM",
        "convert_to_taf": False,
        "category": "salinity_compliance",
    },
    "co_ec": {
        "var": "CO_EC_MONTH",
        "label": "Collinsville EC",
        "native_unit": "UMHOS/CM",
        "convert_to_taf": False,
        "category": "salinity_compliance",
    },
    "banks_ec": {
        "var": "BANKSEC_MAX14DAY",
        "label": "Banks Pumping Plant EC (14-day max)",
        "native_unit": "UMHOS/CM",
        "convert_to_taf": False,
        "category": "salinity_pumps",
    },
    "tracy_ec": {
        "var": "TRACYEC_MAX14DAY",
        "label": "Tracy/Jones Pumping Plant EC (14-day max)",
        "native_unit": "UMHOS/CM",
        "convert_to_taf": False,
        "category": "salinity_pumps",
    },
}


# =============================================================================
# DATA LOADING
# =============================================================================


def load_calsim_csv_from_file(csv_path: str) -> pd.DataFrame:
    """Load only the Delta-relevant columns from a CalSim DV CSV.

    Two-pass approach to avoid loading the entire (500+ column) CSV into RAM:
      Pass 1 — read header row 1 (variable names) to locate column indices.
      Pass 2 — pd.read_csv with usecols for only those indices.
    """
    import csv as csv_mod

    needed_vars = {v["var"] for v in DELTA_VARIABLES.values()}

    # --- Pass 1: scan header row 1 for column indices ---
    with open(csv_path, "r") as f:
        reader = csv_mod.reader(f)
        _row0 = next(reader)  # row 0 (path names)
        var_names = next(reader)  # row 1 (variable names)

    cols_to_load = [0]  # always need the date column
    col_index_to_name: Dict[int, str] = {0: "date"}
    for idx, name in enumerate(var_names):
        if name in needed_vars:
            cols_to_load.append(idx)
            col_index_to_name[idx] = name
            needed_vars.discard(name)

    if needed_vars:
        log.warning(f"Delta variables not found in CSV header: {needed_vars}")

    log.info(f"Loading {len(cols_to_load)} of {len(var_names)} columns from DV CSV")

    # --- Pass 2: load only selected columns ---
    df = pd.read_csv(csv_path, skiprows=7, header=None, usecols=cols_to_load)

    # Rename columns from positional ints to variable names
    rename_map = {}
    for new_pos, orig_idx in enumerate(sorted(cols_to_load)):
        rename_map[new_pos] = col_index_to_name[orig_idx]
    df.columns = [rename_map.get(i, df.columns[i]) for i in range(len(df.columns))]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.set_index("date")

    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.assign(
        CalendarMonth=df.index.month,
        CalendarYear=df.index.year,
        DaysInMonth=df.index.days_in_month,
    )

    return df


def load_calsim_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """Load CalSim output CSV from S3, cleaning up temp file after load."""
    if not HAS_BOTO3:
        raise ImportError("boto3 required for S3 access")
    s3 = boto3.client("s3")
    import tempfile

    possible_keys = [
        f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv",
        f"scenario/{scenario_id}/csv/{scenario_id}_DV.csv",
    ]
    for key in possible_keys:
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp_path = tmp.name
                s3.download_file(S3_BUCKET, key, tmp_path)
            df = load_calsim_csv_from_file(tmp_path)
            return df
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                continue
            raise
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
    raise FileNotFoundError(f"No DV CSV found in S3 for {scenario_id}")


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """Add WaterYear and WaterMonth columns."""
    df = df.copy()
    df["WaterMonth"] = ((df["CalendarMonth"] - 10) % 12) + 1
    df["WaterYear"] = df["CalendarYear"].copy()
    df.loc[df["CalendarMonth"] >= 10, "WaterYear"] += 1
    return df


# =============================================================================
# STATISTICS HELPERS
# =============================================================================


def _safe_cv(data: pd.Series) -> Optional[float]:
    arr = data.dropna().values
    if len(arr) == 0:
        return None
    mean = float(np.mean(arr))
    if abs(mean) < CV_MIN_MEAN_TAF:
        return None
    cv = round(float(np.std(arr, ddof=1) / abs(mean)), 4)
    if cv > 99.0:
        return None
    return cv


def _percentiles(
    data: pd.Series, percentile_list: List[int], prefix: str = "q"
) -> Dict:
    arr = data.dropna().values
    if len(arr) == 0:
        return {}
    result = {}
    for p in percentile_list:
        result[f"{prefix}{p}"] = round(float(np.percentile(arr, p)), 3)
    return result


def _exceedance(data: pd.Series, percentile_list: List[int]) -> Dict:
    arr = data.dropna().values
    if len(arr) == 0:
        return {}
    result = {}
    for p in percentile_list:
        result[f"exc_p{p}"] = round(float(np.percentile(arr, 100 - p)), 3)
    return result


# =============================================================================
# DELTA MONTHLY STATISTICS
# =============================================================================


def calculate_delta_monthly(
    df: pd.DataFrame,
    var_code: str,
    var_info: Dict,
) -> List[Dict]:
    """
    Calculate monthly statistics for a Delta variable.

    Returns one row per water month (12 rows).
    """
    col = var_info["var"]
    if col not in df.columns:
        log.warning(f"Variable {col} not found for {var_code}")
        return []

    raw = pd.to_numeric(df[col], errors="coerce")
    native_unit = var_info["native_unit"]
    convert = var_info.get("convert_to_taf", False)

    if convert:
        values = raw * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    else:
        values = raw

    results = []
    for wm in range(1, 13):
        mask = df["WaterMonth"] == wm
        month_data = values[mask].dropna()
        if month_data.empty:
            continue

        row = {
            "variable_code": var_code,
            "water_month": wm,
            "avg": round(float(month_data.mean()), 3),
            "cv": _safe_cv(month_data),
            "sample_count": len(month_data),
            "unit": "TAF" if convert else native_unit,
        }

        if native_unit == "CFS":
            raw_month = raw[mask].dropna()
            row["avg_cfs"] = round(float(raw_month.mean()), 2)

        row.update(_percentiles(month_data, PERCENTILES))
        row.update(_exceedance(month_data, EXCEEDANCE_PERCENTILES))

        results.append(row)

    return results


# =============================================================================
# DELTA PERIOD SUMMARY
# =============================================================================


def calculate_delta_period_summary(
    df: pd.DataFrame,
    var_code: str,
    var_info: Dict,
) -> Optional[Dict]:
    """
    Calculate period-of-record summary for a Delta variable.

    For NDO: annual total in TAF, plus annual avg in CFS.
    For X2: annual April value, September value, Spring/Fall means (matching V3).
    For salinity: annual mean, Spring/Fall seasonal means (matching V3).
    """
    col = var_info["var"]
    if col not in df.columns:
        return None

    raw = pd.to_numeric(df[col], errors="coerce")
    native_unit = var_info["native_unit"]
    convert = var_info.get("convert_to_taf", False)
    category = var_info.get("category", "")

    if convert:
        monthly_values = raw * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    else:
        monthly_values = raw

    water_years = sorted(df["WaterYear"].unique())

    result = {
        "variable_code": var_code,
        "label": var_info["label"],
        "category": category,
        "native_unit": native_unit,
        "simulation_start_year": int(water_years[0]),
        "simulation_end_year": int(water_years[-1]),
        "total_years": len(water_years),
    }

    # --- Outflow (NDO): annual totals in TAF + avg CFS ---
    if category == "outflow":
        annual_taf = df.assign(_val=monthly_values).groupby("WaterYear")["_val"].sum()
        result["annual_avg_taf"] = round(float(annual_taf.mean()), 2)
        result["annual_cv"] = _safe_cv(annual_taf)
        result.update(_exceedance(annual_taf, EXCEEDANCE_PERCENTILES))

        result["avg_cfs"] = round(float(raw.dropna().mean()), 2)

        sept_mask = df["CalendarMonth"] == 9
        sept_data = monthly_values[sept_mask].dropna()
        if not sept_data.empty:
            result["sept_avg_taf"] = round(float(sept_data.mean()), 3)
            result["sept_cv"] = _safe_cv(sept_data)

    # --- X2 position ---
    elif category == "x2":
        all_data = monthly_values.dropna()
        result["avg_km"] = round(float(all_data.mean()), 2)
        result["cv"] = _safe_cv(all_data)
        result.update(_exceedance(all_data, EXCEEDANCE_PERCENTILES))

        apr_mask = df["CalendarMonth"] == 4
        apr_data = raw[apr_mask].dropna()
        if not apr_data.empty:
            result["april_avg_km"] = round(float(apr_data.mean()), 2)
            result["april_cv"] = _safe_cv(apr_data)
            result.update(
                {
                    f"april_{k}": v
                    for k, v in _exceedance(apr_data, EXCEEDANCE_PERCENTILES).items()
                }
            )

        sep_mask = df["CalendarMonth"] == 9
        sep_data = raw[sep_mask].dropna()
        if not sep_data.empty:
            result["sept_avg_km"] = round(float(sep_data.mean()), 2)
            result["sept_cv"] = _safe_cv(sep_data)
            result.update(
                {
                    f"sept_{k}": v
                    for k, v in _exceedance(sep_data, EXCEEDANCE_PERCENTILES).items()
                }
            )

        spring_mask = df["CalendarMonth"].isin([3, 4, 5])
        spring = raw[spring_mask].dropna()
        if not spring.empty:
            result["spring_avg_km"] = round(float(spring.mean()), 2)
            result["spring_cv"] = _safe_cv(spring)

        fall_mask = df["CalendarMonth"].isin([9, 10, 11])
        fall = raw[fall_mask].dropna()
        if not fall.empty:
            result["fall_avg_km"] = round(float(fall.mean()), 2)
            result["fall_cv"] = _safe_cv(fall)

    # --- Salinity (compliance + pumps) ---
    elif category.startswith("salinity"):
        all_data = monthly_values.dropna()
        result["avg_ec"] = round(float(all_data.mean()), 2)
        result["cv"] = _safe_cv(all_data)
        result.update(_exceedance(all_data, EXCEEDANCE_PERCENTILES))

        spring_mask = df["CalendarMonth"].isin([3, 4, 5])
        spring = raw[spring_mask].dropna()
        if not spring.empty:
            result["spring_avg_ec"] = round(float(spring.mean()), 2)
            result["spring_cv"] = _safe_cv(spring)

        fall_mask = df["CalendarMonth"].isin([9, 10, 11])
        fall = raw[fall_mask].dropna()
        if not fall.empty:
            result["fall_avg_ec"] = round(float(fall.mean()), 2)
            result["fall_cv"] = _safe_cv(fall)

        for threshold_name, threshold_val in [
            ("d1641", 450),
            ("high", 2500),
            ("mid", 1600),
            ("low", 900),
        ]:
            exceed_pct = (
                (all_data > threshold_val).sum() / len(all_data) * 100
                if len(all_data) > 0
                else 0
            )
            result[f"exceed_{threshold_name}_pct"] = round(float(exceed_pct), 2)

    return result


# =============================================================================
# MAIN
# =============================================================================


def calculate_all_delta_statistics(
    scenario_id: str,
    csv_path: Optional[str] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Calculate all Delta statistics for a scenario.

    Returns:
        (monthly_rows, period_summary_rows)
    """
    log.info(f"Processing Delta statistics for scenario: {scenario_id}")

    if csv_path:
        df = load_calsim_csv_from_file(csv_path)
    else:
        df = load_calsim_csv_from_s3(scenario_id)

    df = add_water_year_month(df)
    log.info(f"Loaded {len(df)} months of data, {len(df.columns)} columns")

    monthly_rows = []
    period_summary_rows = []
    found = 0

    for var_code, var_info in DELTA_VARIABLES.items():
        col = var_info["var"]
        if col not in df.columns:
            log.warning(f"  {var_code} ({col}): NOT FOUND in CSV")
            continue
        found += 1
        log.info(f"  {var_code} ({col}): found — {var_info['label']}")

        monthly = calculate_delta_monthly(df, var_code, var_info)
        for row in monthly:
            row["scenario_short_code"] = scenario_id
        monthly_rows.extend(monthly)

        summary = calculate_delta_period_summary(df, var_code, var_info)
        if summary:
            summary["scenario_short_code"] = scenario_id
            period_summary_rows.append(summary)

    log.info(f"Found {found}/{len(DELTA_VARIABLES)} Delta variables")
    log.info(
        f"Generated {len(monthly_rows)} monthly rows, {len(period_summary_rows)} period summaries"
    )

    return monthly_rows, period_summary_rows


# =============================================================================
# DATABASE PERSISTENCE
# =============================================================================


def _convert_numpy(val):
    """Convert numpy types to Python natives for psycopg2."""
    if val is None:
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return round(float(val), 6) if not np.isnan(val) else None
    if isinstance(val, np.ndarray):
        return val.tolist()
    return val


def save_to_database(
    scenario_id: str,
    monthly_rows: List[Dict],
    period_summary_rows: List[Dict],
    database_url: Optional[str] = None,
) -> bool:
    """
    Write delta statistics to PostgreSQL.

    Deletes existing data for the scenario before inserting.
    """
    import json as _json

    conn = get_db_connection(db_url=database_url)
    cur = conn.cursor()

    try:
        cur.execute(
            "DELETE FROM delta_monthly WHERE scenario_short_code = %s", (scenario_id,)
        )
        cur.execute(
            "DELETE FROM delta_period_summary WHERE scenario_short_code = %s",
            (scenario_id,),
        )
        log.info(f"Cleared existing delta data for {scenario_id}")

        if monthly_rows:
            monthly_cols = [
                "scenario_short_code",
                "variable_code",
                "water_month",
                "avg",
                "cv",
                "unit",
                "sample_count",
                "avg_cfs",
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
            ]
            values = [
                tuple(_convert_numpy(row.get(c)) for c in monthly_cols)
                for row in monthly_rows
            ]
            sql = f"INSERT INTO delta_monthly ({', '.join(monthly_cols)}) VALUES %s"
            execute_values(cur, sql, values)
            log.info(f"Inserted {len(values)} delta_monthly rows")

        if period_summary_rows:
            meta_keys = {
                "variable_code",
                "label",
                "category",
                "native_unit",
                "simulation_start_year",
                "simulation_end_year",
                "total_years",
                "scenario_short_code",
            }
            for row in period_summary_rows:
                summary_data = {
                    k: _convert_numpy(v) for k, v in row.items() if k not in meta_keys
                }
                cur.execute(
                    """INSERT INTO delta_period_summary
                       (scenario_short_code, variable_code, label, category, native_unit,
                        simulation_start_year, simulation_end_year, total_years, summary_data)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        row["scenario_short_code"],
                        row["variable_code"],
                        row.get("label"),
                        row.get("category"),
                        row.get("native_unit"),
                        _convert_numpy(row.get("simulation_start_year")),
                        _convert_numpy(row.get("simulation_end_year")),
                        _convert_numpy(row.get("total_years")),
                        _json.dumps(summary_data),
                    ),
                )
            log.info(f"Inserted {len(period_summary_rows)} delta_period_summary rows")

        conn.commit()
        log.info("Database save complete")
        return True

    except Exception as e:
        conn.rollback()
        log.error(f"Database error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="Calculate Delta statistics")
    parser.add_argument("--scenario", required=True, help="Scenario ID (e.g., s0020)")
    parser.add_argument("--csv-path", help="Path to local DV CSV")
    parser.add_argument("--output-json", help="Write results to JSON file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calculate statistics without writing to database",
    )
    parser.add_argument(
        "--devdb",
        action="store_true",
        help="Use development Postgres DB, instead of production",
    )
    args = parser.parse_args()

    monthly, summaries = calculate_all_delta_statistics(args.scenario, args.csv_path)

    database_url = None
    if args.devdb:
        database_url = os.getenv("DEVDB_URL")
        if not database_url:
            log.error("DEVDB_URL not set. Cannot save to database.")
            log.info("Use --output-json to output results as JSON instead.")
            return
    else:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            log.error("DATABASE_URL not set. Cannot save to database.")
            log.info("Use --output-json to output results as JSON instead.")
            return

    if args.output_json:
        import json

        def convert(o):
            if isinstance(o, (np.integer,)):
                return int(o)
            if isinstance(o, (np.floating,)):
                return float(o)
            if isinstance(o, np.ndarray):
                return o.tolist()
            return o

        output = {
            "scenario": args.scenario,
            "monthly_count": len(monthly),
            "summary_count": len(summaries),
            "monthly": monthly,
            "period_summaries": summaries,
        }
        with open(args.output_json, "w") as f:
            json.dump(output, f, indent=2, default=convert)
        log.info(f"Wrote results to {args.output_json}")

    elif args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")

    else:
        save_to_database(args.scenario, monthly, summaries, database_url)

    log.info(f"Total: {len(monthly)} monthly, {len(summaries)} period summary rows")


if __name__ == "__main__":
    main()
