#!/usr/bin/env python3
"""
Cross-section verification script for COEQWAL data explorer.

Computes expected values from reference CalSim CSVs and prints them
so they can be compared against the database / website.

Usage:
    python verify_all_sections.py --scenario s0020

Requires reference CSVs in audits/notebooks_reference/:
    - {run_id}_DELIVERIES.csv     (DN_*, GP_*, D_* in CFS + TAF)
    - {run_id}_DEMANDS-ANNUAL.csv (UD_*, AW_* in TAF)
    - {run_id}_DV_*.csv           (trend variables: S_*, C_*, DEL_*, SHORT_*)
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import numpy as np

CFS_TO_TAF_PER_DAY = 0.001983471

SCENARIO_RUN_IDS = {
    "s0020": "s0020_DCRadjBL_2020LU_wTUCP",
    "s0028": "s0028_CVgwLimit_SGMALU_wTUCP",
}

# ── CSV Parsing ──────────────────────────────────────────────────────────────

def parse_calsim_csv(file_path: str) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Parse a CalSim DSS-export CSV.
    Returns (data_df, units_series) where units_series maps column name -> unit string.
    Handles duplicate variable names by keeping both and tracking units.
    """
    header_df = pd.read_csv(file_path, header=None, nrows=7, low_memory=False)
    var_names = header_df.iloc[1].tolist()
    units_row = header_df.iloc[6].tolist() if len(header_df) >= 7 else []

    col_names = []
    seen = {}
    col_units = []
    for idx, var in enumerate(var_names):
        unit = units_row[idx] if idx < len(units_row) else "UNKNOWN"
        if var in seen:
            suffix = f"_{seen[var]}"
            col_names.append(f"{var}{suffix}")
            seen[var] += 1
        else:
            col_names.append(str(var))
            seen[var] = 1
        col_units.append(unit)

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    if len(data_df.columns) > len(col_names):
        col_names.extend([f"_extra_{i}" for i in range(len(data_df.columns) - len(col_names))])
    elif len(data_df.columns) < len(col_names):
        col_names = col_names[:len(data_df.columns)]
        col_units = col_units[:len(data_df.columns)]

    data_df.columns = col_names

    first_col = col_names[0]
    data_df["DateTime"] = pd.to_datetime(data_df[first_col], errors="coerce")
    data_df = data_df.dropna(subset=["DateTime"])

    period_date = data_df["DateTime"].where(
        data_df["DateTime"].dt.day != 1,
        data_df["DateTime"] - pd.Timedelta(days=1),
    )
    data_df["CalendarMonth"] = period_date.dt.month
    data_df["CalendarYear"] = period_date.dt.year
    data_df["WaterYear"] = data_df["CalendarYear"].where(
        data_df["CalendarMonth"] < 10,
        data_df["CalendarYear"] + 1,
    )
    data_df["DaysInMonth"] = period_date.dt.days_in_month

    units_series = pd.Series(col_units, index=col_names)
    return data_df, units_series


def get_column_taf(df: pd.DataFrame, units: pd.Series, col_name: str) -> Optional[pd.Series]:
    """Get a column converted to TAF. Returns None if column doesn't exist."""
    if col_name not in df.columns:
        return None
    raw = pd.to_numeric(df[col_name], errors="coerce")
    unit = units.get(col_name, "UNKNOWN")
    if unit == "TAF":
        return raw
    elif unit == "CFS":
        return raw * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    else:
        return raw


def get_column_cfs(df: pd.DataFrame, units: pd.Series, col_name: str) -> Optional[pd.Series]:
    """Get a column in CFS. Returns None if column doesn't exist."""
    if col_name not in df.columns:
        return None
    raw = pd.to_numeric(df[col_name], errors="coerce")
    return raw


def annual_avg_taf(series: pd.Series, water_years: pd.Series) -> float:
    """Compute annual average TAF from monthly TAF series."""
    annual = series.groupby(water_years).sum()
    return round(float(annual.mean()), 2)


def monthly_avg(series: pd.Series, months: pd.Series, month: int) -> float:
    """Compute average value for a specific month."""
    mask = months == month
    return round(float(series[mask].mean()), 2)


# ── Section Verifiers ────────────────────────────────────────────────────────

def verify_cws_du(deliveries_df, del_units, demands_df, dem_units):
    """Verify CWS / urban demand unit values."""
    print("\n" + "=" * 80)
    print("SECTION: CWS / Urban Demand Units")
    print("=" * 80)

    sample_dus = [
        ("02_PU", "DN_02_PU", "UD_02_PU"),
        ("26S_PU1", "DN_26S_PU1", "UD_26S_PU1"),
        ("71_PU1", "DN_71_PU1", "UD_71_PU1"),
        ("GDPUD_NU", "DN_GDPUD_NU", "UD_GDPUD_NU"),
    ]

    print(f"\n{'DU':<16} {'Del CFS avg':>12} {'Del TAF/yr':>12} {'Demand TAF/yr':>14} {'Ratio CFS/TAF':>14}")
    print("-" * 70)

    for du_label, del_col, dem_col in sample_dus:
        del_cfs = get_column_cfs(deliveries_df, del_units, del_col)
        del_taf = get_column_taf(deliveries_df, del_units, del_col)

        # Also check if TAF version exists as a duplicate
        del_col_taf = f"{del_col}_1"
        del_taf_direct = get_column_taf(deliveries_df, del_units, del_col_taf)

        dem_taf = get_column_taf(demands_df, dem_units, dem_col)

        cfs_avg = round(float(del_cfs.mean()), 2) if del_cfs is not None else None
        taf_yr = annual_avg_taf(del_taf, deliveries_df["WaterYear"]) if del_taf is not None else None
        taf_yr_direct = annual_avg_taf(del_taf_direct, deliveries_df["WaterYear"]) if del_taf_direct is not None else None
        dem_yr = annual_avg_taf(dem_taf, demands_df["WaterYear"]) if dem_taf is not None else None

        # The ratio between raw CFS mean and TAF/yr shows the conversion factor
        cfs_annual = round(float(del_cfs.groupby(deliveries_df["WaterYear"]).sum().mean()), 2) if del_cfs is not None else None
        ratio = round(cfs_annual / taf_yr, 1) if cfs_annual and taf_yr and taf_yr > 0 else None

        # Use direct TAF if available, otherwise converted
        best_taf = taf_yr_direct if taf_yr_direct is not None else taf_yr

        print(f"{du_label:<16} {str(cfs_avg):>12} {str(best_taf):>12} {str(dem_yr):>14} {str(ratio):>14}")

    print("\nNote: 'Del CFS avg' = monthly average CFS (what buggy ETL stores as TAF)")
    print("      'Del TAF/yr' = correct annual TAF (converted or from TAF columns)")
    print("      'Ratio CFS/TAF' = annual sum of raw CFS / annual TAF -- should be ~60")
    print("      If DB stores raw CFS as TAF, values are inflated by this ratio.")


def verify_ag(deliveries_df, del_units, demands_df, dem_units):
    """Verify agricultural demand unit values."""
    print("\n" + "=" * 80)
    print("SECTION: Agricultural Demand Units")
    print("=" * 80)

    sample_dus = [
        ("02_PA", "DN_02_PA", "AW_02_PA"),
        ("08N_PA", "DN_08N_PA", "AW_08N_PA"),
        ("61_PA1", "DN_61_PA1", "AW_61_PA1"),
        ("71_PA1", "DN_71_PA1", "AW_71_PA1"),
    ]

    print(f"\n{'DU':<16} {'Del CFS avg':>12} {'Del TAF/yr':>12} {'Demand TAF/yr':>14} {'Fulfillment%':>13}")
    print("-" * 70)

    for du_label, del_col, dem_col in sample_dus:
        del_taf = get_column_taf(deliveries_df, del_units, del_col)
        del_cfs = get_column_cfs(deliveries_df, del_units, del_col)

        del_col_taf = f"{del_col}_1"
        del_taf_direct = get_column_taf(deliveries_df, del_units, del_col_taf)

        dem_taf = get_column_taf(demands_df, dem_units, dem_col)

        cfs_avg = round(float(del_cfs.mean()), 2) if del_cfs is not None else None
        taf_yr = annual_avg_taf(del_taf, deliveries_df["WaterYear"]) if del_taf is not None else None
        taf_yr_direct = annual_avg_taf(del_taf_direct, deliveries_df["WaterYear"]) if del_taf_direct is not None else None
        dem_yr = annual_avg_taf(dem_taf, demands_df["WaterYear"]) if dem_taf is not None else None

        best_taf = taf_yr_direct if taf_yr_direct is not None else taf_yr
        fulfillment = round(best_taf / dem_yr * 100, 1) if best_taf and dem_yr and dem_yr > 0 else None

        print(f"{du_label:<16} {str(cfs_avg):>12} {str(best_taf):>12} {str(dem_yr):>14} {str(fulfillment):>13}")


def verify_mi_contractors(dv_df, dv_units):
    """Verify M&I contractor aggregate deliveries."""
    print("\n" + "=" * 80)
    print("SECTION: M&I Contractors / CWS Aggregates")
    print("=" * 80)

    agg_vars = [
        ("DEL_SWP_PMI", "SWP M&I Total"),
        ("DEL_SWP_PMI_N", "SWP M&I NOD"),
        ("DEL_SWP_PMI_S", "SWP M&I SOD"),
        ("DEL_SWP_MWD", "SWP MWD"),
        ("DEL_CVP_PMI_N", "CVP M&I NOD"),
        ("DEL_CVP_PMI_S", "CVP M&I SOD"),
        ("SHORT_SWP_TOTA", "SWP Shortage Total"),
        ("SHORT_CVP_TOT_N", "CVP Shortage NOD"),
        ("SHORT_CVP_TOT_S", "CVP Shortage SOD"),
        ("DEL_SWP_PAG", "SWP AG Total"),
        ("DEL_SWP_PAG_N", "SWP AG NOD"),
        ("DEL_SWP_PAG_S", "SWP AG SOD"),
        ("DEL_CVP_PAG_N", "CVP AG NOD"),
        ("DEL_CVP_PAG_S", "CVP AG SOD"),
    ]

    print(f"\n{'Variable':<22} {'Label':<22} {'Unit':>6} {'Monthly CFS':>12} {'Annual TAF':>12}")
    print("-" * 76)

    for var, label in agg_vars:
        if var not in dv_df.columns:
            print(f"{var:<22} {label:<22} {'--':>6} {'MISSING':>12} {'MISSING':>12}")
            continue
        unit = dv_units.get(var, "?")
        raw = pd.to_numeric(dv_df[var], errors="coerce")
        monthly_cfs = round(float(raw.mean()), 2)
        taf_series = get_column_taf(dv_df, dv_units, var)
        taf_yr = annual_avg_taf(taf_series, dv_df["WaterYear"]) if taf_series is not None else None
        print(f"{var:<22} {label:<22} {unit:>6} {monthly_cfs:>12} {str(taf_yr):>12}")

    print("\nNote: If the ETL stores raw CFS in _taf columns, DB values ≈ 'Monthly CFS' column.")
    print("      Correct values should match 'Annual TAF' column.")


def verify_reservoirs(dv_df, dv_units):
    """Verify reservoir storage values."""
    print("\n" + "=" * 80)
    print("SECTION: Reservoirs")
    print("=" * 80)

    reservoirs = [
        ("S_SHSTA", "Shasta", 4552.0),
        ("S_OROVL", "Oroville", 3538.0),
        ("S_FOLSM", "Folsom", 977.0),
        ("S_TRNTY", "Trinity", 2448.0),
        ("S_MELON", "New Melones", 2420.0),
        ("S_MLRTN", "Millerton", 520.5),
        ("S_SLUIS_CVP", "San Luis CVP", 966.0),
        ("S_SLUIS_SWP", "San Luis SWP", 1062.0),
    ]

    print(f"\n{'Reservoir':<16} {'Unit':>6} {'Apr Avg TAF':>12} {'Sep Avg TAF':>12} {'Annual Avg':>12} {'%Cap Apr':>9} {'%Cap Sep':>9}")
    print("-" * 78)

    for var, label, capacity in reservoirs:
        if var not in dv_df.columns:
            print(f"{label:<16} {'--':>6} {'MISSING':>12} {'MISSING':>12} {'MISSING':>12}")
            continue
        unit = dv_units.get(var, "?")
        raw = pd.to_numeric(dv_df[var], errors="coerce")
        apr_avg = monthly_avg(raw, dv_df["CalendarMonth"], 4)
        sep_avg = monthly_avg(raw, dv_df["CalendarMonth"], 9)
        ann_avg = round(float(raw.mean()), 2)
        pct_apr = round(apr_avg / capacity * 100, 1) if capacity > 0 else None
        pct_sep = round(sep_avg / capacity * 100, 1) if capacity > 0 else None
        print(f"{label:<16} {unit:>6} {apr_avg:>12} {sep_avg:>12} {ann_avg:>12} {str(pct_apr):>9} {str(pct_sep):>9}")


def verify_env_flows(dv_df, dv_units):
    """Verify environmental flow channel values."""
    print("\n" + "=" * 80)
    print("SECTION: Environmental Flows (Channels)")
    print("=" * 80)

    channels = [
        ("C_SAC041", "Sacramento @ Keswick"),
        ("C_AMR004", "American @ mouth"),
        ("C_SJR070", "San Joaquin @ Vernalis"),
        ("C_SAC000", "Sacramento @ Freeport"),
        ("C_KSWCK", "Keswick release"),
        ("C_OROVL", "Oroville release"),
        ("C_FOLSM", "Folsom release"),
        ("C_TRNTY", "Trinity release"),
    ]

    print(f"\n{'Variable':<16} {'Location':<26} {'Unit':>6} {'Avg CFS':>10} {'Annual TAF':>12}")
    print("-" * 72)

    for var, label in channels:
        if var not in dv_df.columns:
            print(f"{var:<16} {label:<26} {'--':>6} {'MISSING':>10} {'MISSING':>12}")
            continue
        unit = dv_units.get(var, "?")
        raw = pd.to_numeric(dv_df[var], errors="coerce")
        avg_cfs = round(float(raw.mean()), 2)
        taf_series = get_column_taf(dv_df, dv_units, var)
        taf_yr = annual_avg_taf(taf_series, dv_df["WaterYear"]) if taf_series is not None else None
        print(f"{var:<16} {label:<26} {unit:>6} {avg_cfs:>10} {str(taf_yr):>12}")


def verify_deliveries_unit_check(deliveries_df, del_units):
    """
    Critical check: compare CFS columns vs TAF columns in the DELIVERIES CSV
    to confirm the conversion factor.
    """
    print("\n" + "=" * 80)
    print("UNIT CONVERSION VALIDATION")
    print("=" * 80)
    print("\nComparing CFS (first half) vs TAF (second half) of DELIVERIES CSV")
    print("for the same variables to confirm CFS-to-TAF conversion factor.\n")

    # Find variables that appear both with CFS and TAF units
    test_vars = ["DN_02_PU", "DN_71_PU1", "DN_08N_PA", "DEL_SWP_MWD", "DN_61_PA1"]

    print(f"{'Variable':<18} {'CFS col avg':>12} {'TAF col avg':>12} {'Manual TAF':>12} {'CFS/TAF ratio':>14}")
    print("-" * 70)

    for var in test_vars:
        cfs_col = var
        taf_col = f"{var}_1"

        if cfs_col not in deliveries_df.columns:
            print(f"{var:<18} {'MISSING':>12}")
            continue

        cfs_unit = del_units.get(cfs_col, "?")
        taf_unit = del_units.get(taf_col, "?") if taf_col in del_units.index else "?"

        cfs_raw = pd.to_numeric(deliveries_df[cfs_col], errors="coerce")
        cfs_annual = float(cfs_raw.groupby(deliveries_df["WaterYear"]).sum().mean())

        manual_taf = float((cfs_raw * deliveries_df["DaysInMonth"] * CFS_TO_TAF_PER_DAY).groupby(deliveries_df["WaterYear"]).sum().mean())

        if taf_col in deliveries_df.columns:
            taf_raw = pd.to_numeric(deliveries_df[taf_col], errors="coerce")
            taf_annual = float(taf_raw.groupby(deliveries_df["WaterYear"]).sum().mean())
            ratio = round(cfs_annual / taf_annual, 1) if taf_annual > 0 else None
        else:
            taf_annual = None
            ratio = None

        print(f"{var:<18} {round(cfs_annual, 2):>12} {str(round(taf_annual, 2) if taf_annual else 'N/A'):>12} {round(manual_taf, 2):>12} {str(ratio):>14}")

    print("\nIf CFS/TAF ratio ≈ 60, the CFS→TAF conversion factor is confirmed correct.")
    print("Manual TAF should closely match TAF col avg.")


# ── Main ─────────────────────────────────────────────────────────────────────

def find_file(base_dir: Path, run_id: str, suffix: str) -> Optional[Path]:
    """Find a reference CSV by suffix pattern."""
    for f in base_dir.glob(f"{run_id}*{suffix}*"):
        if f.suffix == ".csv":
            return f
    return None


def main():
    parser = argparse.ArgumentParser(description="Verify data explorer values against reference CSVs")
    parser.add_argument("--scenario", default="s0020", help="Scenario ID (default: s0020)")
    parser.add_argument("--ref-dir", default=None, help="Path to reference CSV directory")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    ref_dir = Path(args.ref_dir) if args.ref_dir else script_dir.parent.parent / "audits" / "notebooks_reference"

    if not ref_dir.exists():
        print(f"ERROR: Reference directory not found: {ref_dir}")
        sys.exit(1)

    run_id = SCENARIO_RUN_IDS.get(args.scenario, args.scenario)
    print(f"Scenario: {args.scenario} (run_id: {run_id})")
    print(f"Reference dir: {ref_dir}")

    # Find files
    deliveries_path = find_file(ref_dir, run_id, "DELIVERIES")
    demands_path = find_file(ref_dir, run_id, "DEMANDS")
    dv_path = find_file(ref_dir, run_id, "DV")

    print(f"\nFiles found:")
    print(f"  DELIVERIES: {deliveries_path.name if deliveries_path else 'NOT FOUND'}")
    print(f"  DEMANDS:    {demands_path.name if demands_path else 'NOT FOUND'}")
    print(f"  DV:         {dv_path.name if dv_path else 'NOT FOUND'}")

    # Parse CSVs
    deliveries_df, del_units = (None, None)
    demands_df, dem_units = (None, None)
    dv_df, dv_units = (None, None)

    if deliveries_path:
        print("\nParsing DELIVERIES CSV...")
        deliveries_df, del_units = parse_calsim_csv(str(deliveries_path))
        print(f"  Rows: {len(deliveries_df)}, Columns: {len(deliveries_df.columns)}")
        cfs_count = (del_units == "CFS").sum()
        taf_count = (del_units == "TAF").sum()
        print(f"  CFS columns: {cfs_count}, TAF columns: {taf_count}")

    if demands_path:
        print("\nParsing DEMANDS CSV...")
        demands_df, dem_units = parse_calsim_csv(str(demands_path))
        print(f"  Rows: {len(demands_df)}, Columns: {len(demands_df.columns)}")

    if dv_path:
        print("\nParsing DV (trend) CSV...")
        dv_df, dv_units = parse_calsim_csv(str(dv_path))
        print(f"  Rows: {len(dv_df)}, Columns: {len(dv_df.columns)}")

    # Run verifications
    if deliveries_df is not None:
        verify_deliveries_unit_check(deliveries_df, del_units)

    if deliveries_df is not None and demands_df is not None:
        verify_cws_du(deliveries_df, del_units, demands_df, dem_units)
        verify_ag(deliveries_df, del_units, demands_df, dem_units)

    if dv_df is not None:
        verify_mi_contractors(dv_df, dv_units)
        verify_reservoirs(dv_df, dv_units)
        verify_env_flows(dv_df, dv_units)

    print("\n" + "=" * 80)
    print("DONE. Compare values above against database / website.")
    print("=" * 80)


if __name__ == "__main__":
    main()
