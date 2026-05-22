#!/usr/bin/env python3
"""
validate_csvs.py - DSS-style CSV validator.

Compares two DSS-style CSVs (7-row header containing A, B, C, E, F, TYPE,
UNITS labels in column 0, dates in column 0 of the data section, numeric
series in the remaining columns) on (B-part, C-part) keys over the
overlapping date range.

The Batch container calls this script as a subprocess to verify that
DSS-to-CSV extraction matches the modeling team's trend report. The same
entry point is also useful to pass `--show-unmatched` for a
console report when triaging a failure.
"""

import argparse
import json
import os
import sys
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

HEADER_ROWS = 7  # A, B, C, E, F, TYPE, UNITS


def _read_dss_csv(path: str):
    """
    Read a DSS-style CSV, such as the modeling team's Trend Report.

    Rows whose first-column timestamp cannot be parsed are dropped and a
    `[WARN]` line is written to stderr. The Batch wrapper captures the
    validator subprocess with `2>&1`, so the warning lands in CloudWatch.
    Comparison runs only over the surviving rows.

    This is defensive. It hasn't been a problem yet, but date strings can
    drift across xlsx-to-csv conversions, so a loud signal is worth the three lines.

    Returns:
      idx_dates: pd.DatetimeIndex
      data: pd.DataFrame with columns keyed by (B, C) tuples
      meta: dict with helper info
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    raw = pd.read_csv(path, header=None, dtype=object, na_values=['NaN'], keep_default_na=True)
    if raw.shape[0] < HEADER_ROWS:
        raise ValueError(f"{path}: not enough rows for DSS header")

    header = raw.iloc[:HEADER_ROWS].copy()
    data = raw.iloc[HEADER_ROWS:].copy()

    labels = [str(x).strip().lower() if pd.notna(x) else "" for x in header.iloc[:, 0].tolist()]
    try:
        b_idx = labels.index('b')
        c_idx = labels.index('c')
    except ValueError:
        # Header rows are unlabeled. Fall back to the documented row order
        # (row 1 = B, row 2 = C)
        b_idx = 1 if len(labels) > 1 else 0
        c_idx = 2 if len(labels) > 2 else 0

    b_names = header.iloc[b_idx, 1:].astype(str).str.strip().tolist()
    c_names = header.iloc[c_idx, 1:].astype(str).str.strip().tolist()

    col_keys: Dict[int, Tuple[str, str]] = {}
    for j, (b, c) in enumerate(zip(b_names, c_names), start=1):
        b_norm = (b or "").strip().upper()
        c_norm = (c or "").strip().upper()
        if b_norm and c_norm and b_norm != "NAN" and c_norm != "NAN":
            col_keys[j] = (b_norm, c_norm)

    dt_series = pd.to_datetime(data.iloc[:, 0], errors='coerce')
    dropped = int(dt_series.isna().sum())
    if dropped > 0:
        print(f"[WARN] {path}: dropped {dropped} row(s) with unparseable dates", file=sys.stderr)

    data = data.drop(columns=data.columns[0])
    data.index = dt_series
    data = data[~data.index.isna()]

    series_dict = {}
    for j, key in col_keys.items():
        vals = pd.to_numeric(raw.iloc[HEADER_ROWS:, j], errors='coerce')
        vals.index = dt_series
        series_dict[key] = vals.loc[~vals.index.isna()]

    df = pd.DataFrame(series_dict)

    meta = {
        "num_columns": len(series_dict),
        "keys": list(series_dict.keys()),
        "b_row": b_idx,
        "c_row": c_idx,
        "path": path,
    }
    return df.index, df, meta


def compare(ref_path: str,
            file_path: str,
            abs_tol: float,
            rel_tol: float,
            verbose: bool = False):
    """
    Compare two DSS-style CSVs by (B, C) columns over their overlapping
    date range.

    Returns:
      summary: nested dict (see module docstring for shape)
      mismatches_df: per-row mismatch table with columns
        date, B, C, ref_value, file_value, abs_diff, mismatch_type
      column_summaries: per-column rollup, one entry per column that had
        at least one mismatch
    """
    idx1, df1, meta1 = _read_dss_csv(ref_path)
    idx2, df2, meta2 = _read_dss_csv(file_path)

    if verbose:
        print(f"[INFO] {meta1['path']} columns: {meta1['num_columns']}")
        print(f"[INFO] {meta2['path']} columns: {meta2['num_columns']}")

    common_keys = sorted(set(df1.columns).intersection(set(df2.columns)))
    only_in_ref = sorted(set(df1.columns) - set(df2.columns))
    only_in_file = sorted(set(df2.columns) - set(df1.columns))

    overlap_start = max(idx1.min(), idx2.min()) if len(idx1) and len(idx2) else None
    overlap_end = min(idx1.max(), idx2.max()) if len(idx1) and len(idx2) else None

    if overlap_start is None or overlap_end is None or overlap_start > overlap_end:
        raise ValueError("No overlapping date range between the two files.")

    df1o = df1.loc[(df1.index >= overlap_start) & (df1.index <= overlap_end), common_keys].copy()
    df2o = df2.loc[(df2.index >= overlap_start) & (df2.index <= overlap_end), common_keys].copy()

    common_dates = sorted(set(df1o.index).intersection(set(df2o.index)))
    df1o = df1o.loc[common_dates]
    df2o = df2o.loc[common_dates]

    mismatch_details: List[dict] = []
    column_summaries: List[dict] = []
    total_mismatch_cells = 0
    cols_with_mismatch = 0

    for key in common_keys:
        s1 = pd.to_numeric(df1o[key], errors='coerce')
        s2 = pd.to_numeric(df2o[key], errors='coerce')

        eq = s1.eq(s2) | (s1.isna() & s2.isna()) | np.isclose(s1, s2, atol=abs_tol, rtol=rel_tol, equal_nan=True)
        mismask = ~eq

        if mismask.any():
            cols_with_mismatch += 1
            column_mismatches = int(mismask.sum())
            total_mismatch_cells += column_mismatches

            column_summaries.append({
                "B": key[0],
                "C": key[1],
                "mismatches": column_mismatches,
                "total_cells": int(len(s1)),
                "mismatch_rate": float(column_mismatches / len(s1)) if len(s1) else 0.0,
            })

            for dt in s1.index[mismask]:
                v1 = s1.loc[dt]
                v2 = s2.loc[dt]
                diff = (abs(v1 - v2) if (pd.notna(v1) and pd.notna(v2)) else np.nan)

                if pd.isna(v1) and pd.notna(v2):
                    mismatch_type = "missing_in_ref"
                elif pd.notna(v1) and pd.isna(v2):
                    mismatch_type = "missing_in_file"
                elif pd.notna(v1) and pd.notna(v2):
                    # Distinguishes "outside both abs and rel tolerance" from
                    # "outside abs but within rel" (or vice versa). The
                    # mismask above already established at least one bound
                    # was violated.
                    denom = max(abs(v1), abs(v2), 1e-10)
                    if abs(v1 - v2) > abs_tol and abs(v1 - v2) / denom > rel_tol:
                        mismatch_type = "value_difference"
                    else:
                        mismatch_type = "tolerance_exceeded"
                else:
                    mismatch_type = "both_nan"

                mismatch_details.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "B": key[0],
                    "C": key[1],
                    "ref_value": v1 if pd.notna(v1) else "NaN",
                    "file_value": v2 if pd.notna(v2) else "NaN",
                    "abs_diff": diff if pd.notna(diff) else "NaN",
                    "mismatch_type": mismatch_type,
                })

    mismatches_df = pd.DataFrame(mismatch_details)

    total_cells_compared = int(len(common_keys) * len(common_dates))

    summary = {
        # PASSED requires zero mismatches AND a non-empty key intersection.
        # Without the second guard, two files with disjoint (B, C) sets would
        # pass vacuously
        "status": "PASSED" if (total_mismatch_cells == 0 and len(common_keys) > 0) else "FAILED",
        "validation_summary": {
            "total_columns_compared": int(len(common_keys)),
            "columns_with_mismatches": int(cols_with_mismatch),
            "total_mismatch_cells": int(total_mismatch_cells),
            "total_cells_compared": total_cells_compared,
            "mismatch_rate": float(total_mismatch_cells / total_cells_compared) if total_cells_compared else 0.0,
        },
        "file_comparison": {
            "columns_ref": int(df1.shape[1]),
            "columns_file": int(df2.shape[1]),
            "columns_common": int(len(common_keys)),
            "columns_only_in_ref": len(only_in_ref),
            "columns_only_in_file": len(only_in_file),
        },
        "date_range": {
            "ref_start": str(idx1.min()) if len(idx1) else None,
            "ref_end": str(idx1.max()) if len(idx1) else None,
            "file_start": str(idx2.min()) if len(idx2) else None,
            "file_end": str(idx2.max()) if len(idx2) else None,
            "overlap_start": str(pd.Timestamp(overlap_start)) if overlap_start is not None else None,
            "overlap_end": str(pd.Timestamp(overlap_end)) if overlap_end is not None else None,
            "rows_in_overlap": int(len(common_dates)),
        },
        "tolerances": {
            "absolute_tolerance": abs_tol,
            "relative_tolerance": rel_tol,
        },
        "files": {
            "ref_path": meta1["path"],
            "file_path": meta2["path"],
        },
    }

    if column_summaries:
        summary["column_mismatches"] = column_summaries

    if only_in_ref:
        summary["sample_only_in_ref"] = [f"{b}|{c}" for (b, c) in only_in_ref[:10]]
    if only_in_file:
        summary["sample_only_in_file"] = [f"{b}|{c}" for (b, c) in only_in_file[:10]]

    return summary, mismatches_df, column_summaries


def print_console_report(summary: dict,
                         mismatches_df: pd.DataFrame,
                         column_summaries: List[dict],
                         show_unmatched: bool = True,
                         max_unmatched: int = 50):
    """Human-readable report. Pair with `--show-unmatched` or `--console-only`."""

    print("\n" + "=" * 60)
    print("CSV VALIDATION REPORT")
    print("=" * 60)

    status = summary["status"]
    print(f"Status: {status}")

    if status == "PASSED":
        print("All values match within specified tolerances.")
        return

    val_summary = summary["validation_summary"]
    print("\nSummary:")
    print(f"  Total cells compared: {val_summary['total_cells_compared']:,}")
    print(f"  Mismatch cells: {val_summary['total_mismatch_cells']:,}")
    print(f"  Mismatch rate: {val_summary['mismatch_rate']:.4%}")
    print(f"  Columns with mismatches: {val_summary['columns_with_mismatches']}/{val_summary['total_columns_compared']}")

    if column_summaries:
        print("\nColumns with highest mismatch rates:")
        sorted_cols = sorted(column_summaries, key=lambda x: x['mismatch_rate'], reverse=True)
        for col in sorted_cols[:10]:
            print(f"  {col['B']}|{col['C']}: {col['mismatches']:,}/{col['total_cells']:,} ({col['mismatch_rate']:.2%})")

    if show_unmatched and not mismatches_df.empty:
        print(f"\nUnmatched cells (showing first {max_unmatched}):")
        print("-" * 80)

        mismatch_types = mismatches_df['mismatch_type'].value_counts()
        print("Mismatch types:")
        for mtype, count in mismatch_types.items():
            print(f"  {mtype}: {count:,} cells")

        print("\nDetailed mismatches:")
        display_df = mismatches_df.head(max_unmatched)[['date', 'B', 'C', 'ref_value', 'file_value', 'abs_diff', 'mismatch_type']]
        for _, row in display_df.iterrows():
            print(f"  {row['date']} | {row['B']}|{row['C']} | Ref: {row['ref_value']} | File: {row['file_value']} | Diff: {row['abs_diff']} | Type: {row['mismatch_type']}")

        if len(mismatches_df) > max_unmatched:
            print(f"  ... and {len(mismatches_df) - max_unmatched:,} more mismatches")

    file_comp = summary["file_comparison"]
    if file_comp["columns_only_in_ref"] > 0:
        print(f"\nColumns only in reference: {file_comp['columns_only_in_ref']}")
        if "sample_only_in_ref" in summary:
            print(f"  Examples: {', '.join(summary['sample_only_in_ref'][:5])}")

    if file_comp["columns_only_in_file"] > 0:
        print(f"\nColumns only in test file: {file_comp['columns_only_in_file']}")
        if "sample_only_in_file" in summary:
            print(f"  Examples: {', '.join(summary['sample_only_in_file'][:5])}")


def main():
    parser = argparse.ArgumentParser(description="Validate DSS-style CSVs on (B, C) keys and overlapping dates.")
    parser.add_argument("--ref", required=True, help="Reference CSV (e.g., Trend Report)")
    parser.add_argument("--file", required=True, help="CSV produced by pipeline")
    parser.add_argument("--abs-tol", type=float, default=1e-6, help="Absolute tolerance (default 1e-6)")
    parser.add_argument("--rel-tol", type=float, default=1e-6, help="Relative tolerance (default 1e-6)")
    parser.add_argument("--out-json", default="", help="Write summary JSON here (optional)")
    parser.add_argument("--out-csv", default="", help="Write mismatches CSV here (optional)")
    parser.add_argument("--show-unmatched", action="store_true", help="Print detailed unmatched cells to the console")
    parser.add_argument("--max-unmatched", type=int, default=50, help="Max unmatched cells to print (default 50)")
    parser.add_argument("--console-only", action="store_true", help="Print the human report only. Suppress the JSON dump.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    try:
        summary, mismatches, column_summaries = compare(
            args.ref, args.file, args.abs_tol, args.rel_tol, args.verbose
        )

        if args.show_unmatched or args.console_only:
            print_console_report(summary, mismatches, column_summaries,
                                 args.show_unmatched, args.max_unmatched)

        # The Batch container captures stdout into VAL_OUT on failure and
        # uses it as the validation summary text, so JSON has to be the
        # default. Pass --console-only to suppress it for human runs.
        if not args.console_only:
            if args.show_unmatched:
                print("\nJSON Summary:")
            print(json.dumps(summary, indent=2))

        if args.out_json:
            os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
            with open(args.out_json, "w") as f:
                json.dump(summary, f, indent=2)

        if args.out_csv:
            os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
            empty_cols = ["date", "B", "C", "ref_value", "file_value", "abs_diff", "mismatch_type"]
            (mismatches if not mismatches.empty else pd.DataFrame(columns=empty_cols)).to_csv(args.out_csv, index=False)

        exit(0 if summary["status"] == "PASSED" else 1)

    except Exception as e:
        # Exit 2 distinguishes "the validator itself errored" from
        # "validation ran and found mismatches" (exit 1). The Batch wrapper
        # treats any non-zero as FAILED, but the distinction shows up in
        # the captured stdout
        print(f"ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        exit(2)


if __name__ == "__main__":
    main()
