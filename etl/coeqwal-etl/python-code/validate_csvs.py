#!/usr/bin/env python3
# validate_csvs.py
import argparse, json, os
from typing import Dict, Tuple
import numpy as np
import pandas as pd

HEADER_ROWS = 7  # A,B,C,E,F,TYPE,UNITS (and first col contains those labels)

def _read_dss_csv(path: str):
    """
    Read a DSS-style CSV exported by your pipeline.
    Returns:
      idx_dates: pd.DatetimeIndex
      data: pd.DataFrame with columns keyed by (B,C) tuples
      meta: dict with helper info
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    raw = pd.read_csv(path, header=None, dtype=object, na_values=['NaN'], keep_default_na=True)
    if raw.shape[0] < HEADER_ROWS:
        raise ValueError(f"{path}: not enough rows for DSS header")

    header = raw.iloc[:HEADER_ROWS].copy()
    data = raw.iloc[HEADER_ROWS:].copy()

    # Normalize the header row labels (first column)
    labels = [str(x).strip().lower() if pd.notna(x) else "" for x in header.iloc[:,0].tolist()]
    # Find the index (row) for 'b' and 'c' parts
    try:
        b_idx = labels.index('b')
        c_idx = labels.index('c')
    except ValueError:
        # Fallback to row 1 for B and row 2 for C if not labeled
        b_idx = 1 if len(labels) > 1 else 0
        c_idx = 2 if len(labels) > 2 else 0

    # Extract B and C names for each column (skip first DateTime column)
    b_names = header.iloc[b_idx, 1:].astype(str).str.strip().tolist()
    c_names = header.iloc[c_idx, 1:].astype(str).str.strip().tolist()

    # Build column key map: csv_col_index -> (B,C)
    col_keys: Dict[int, Tuple[str,str]] = {}
    for j, (b, c) in enumerate(zip(b_names, c_names), start=1):
        b_norm = (b or "").strip().upper()
        c_norm = (c or "").strip().upper()
        if b_norm and c_norm and b_norm != "NAN" and c_norm != "NAN":
            col_keys[j] = (b_norm, c_norm)

    # Parse dates (first column in data)
    dt_series = pd.to_datetime(data.iloc[:,0], errors='coerce')
    data = data.drop(columns=data.columns[0])
    data.index = dt_series
    data = data[~data.index.isna()]  # drop non-parsable date rows

    # Build a (B,C)-keyed DataFrame of numeric values
    series_dict = {}
    for j, key in col_keys.items():
        vals = pd.to_numeric(raw.iloc[HEADER_ROWS:, j], errors='coerce')
        vals.index = dt_series
        series_dict[key] = vals.loc[~vals.index.isna()]

    # Combine to DataFrame, align by index union (we’ll restrict later)
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
    Compare two DSS-style CSVs by (B,C) columns and overlapping dates.
    Returns (summary_dict, mismatches_df).
    """
    idx1, df1, meta1 = _read_dss_csv(ref_path)
    idx2, df2, meta2 = _read_dss_csv(file_path)

    if verbose:
        print(f"[INFO] {meta1['path']} columns: {meta1['num_columns']}")
        print(f"[INFO] {meta2['path']} columns: {meta2['num_columns']}")

    # Overlapping (B,C) keys (subset in trend reports is fine)
    common_keys = sorted(set(df1.columns).intersection(set(df2.columns)))
    only1 = sorted(set(df1.columns) - set(df2.columns))
    only2 = sorted(set(df2.columns) - set(df1.columns))

    # Overlapping dates
    overlap_start = max(idx1.min(), idx2.min()) if len(idx1) and len(idx2) else None
    overlap_end   = min(idx1.max(), idx2.max()) if len(idx1) and len(idx2) else None

    if overlap_start is None or overlap_end is None or overlap_start > overlap_end:
        raise ValueError("No overlapping date range between the two files.")

    # Slice to overlapping date range
    df1o = df1.loc[(df1.index >= overlap_start) & (df1.index <= overlap_end), common_keys].copy()
    df2o = df2.loc[(df2.index >= overlap_start) & (df2.index <= overlap_end), common_keys].copy()

    # Ensure same index (inner join on dates)
    common_dates = sorted(set(df1o.index).intersection(set(df2o.index)))
    df1o = df1o.loc[common_dates]
    df2o = df2o.loc[common_dates]

    # Build mismatches report
    rows = []
    total_mismatch_cells = 0
    cols_with_mismatch = 0

    for key in common_keys:
        s1 = pd.to_numeric(df1o[key], errors='coerce')
        s2 = pd.to_numeric(df2o[key], errors='coerce')

        # Equal if both NaN OR close within tolerances
        eq = s1.eq(s2) | (s1.isna() & s2.isna()) | np.isclose(s1, s2, atol=abs_tol, rtol=rel_tol, equal_nan=True)
        mismask = ~eq

        if mismask.any():
            cols_with_mismatch += 1
            for dt in s1.index[mismask]:
                v1 = s1.loc[dt]
                v2 = s2.loc[dt]
                diff = (abs(v1 - v2) if (pd.notna(v1) and pd.notna(v2)) else np.nan)
                rows.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "B": key[0],
                    "C": key[1],
                    "ref_value": v1 if pd.notna(v1) else "NaN",
                    "file_value": v2 if pd.notna(v2) else "NaN",
                    "abs_diff": diff if pd.notna(diff) else "NaN",
                })
            total_mismatch_cells += mismask.sum()

    mismatches_df = pd.DataFrame(rows)

    summary = {
        "status": "PASSED" if total_mismatch_cells == 0 else "FAILED",
        "columns_ref": int(df1.shape[1]),
        "columns_file": int(df2.shape[1]),
        "columns_common": int(len(common_keys)),
        "columns_only_in_ref": len(only1),
        "columns_only_in_file": len(only2),
        "dates_ref_start": str(idx1.min()) if len(idx1) else None,
        "dates_ref_end": str(idx1.max()) if len(idx1) else None,
        "dates_file_start": str(idx2.min()) if len(idx2) else None,
        "dates_file_end": str(idx2.max()) if len(idx2) else None,
        "overlap_start": str(pd.Timestamp(overlap_start)) if overlap_start is not None else None,
        "overlap_end": str(pd.Timestamp(overlap_end)) if overlap_end is not None else None,
        "rows_in_overlap": int(len(common_dates)),
        "mismatch_columns": int(cols_with_mismatch),
        "mismatch_cells": int(total_mismatch_cells),
        "note": "Compared on (B,C); F is ignored.",
        "ref_path": meta1["path"],
        "file_path": meta2["path"],
    }

    # Optionally list some missing keys for debugging
    if only1:
        summary["sample_only_in_ref"] = [f"{b}|{c}" for (b,c) in only1[:10]]
    if only2:
        summary["sample_only_in_file"] = [f"{b}|{c}" for (b,c) in only2[:10]]

    return summary, mismatches_df

def main():
    ap = argparse.ArgumentParser(description="Validate DSS-style CSVs on (B,C) keys and overlapping dates.")
    ap.add_argument("--ref", required=True, help="Reference CSV (e.g., Trend Report)")
    ap.add_argument("--file", required=True, help="CSV produced by pipeline")
    ap.add_argument("--abs-tol", type=float, default=1e-6, help="Absolute tolerance (default 1e-6)")
    ap.add_argument("--rel-tol", type=float, default=1e-6, help="Relative tolerance (default 1e-6)")
    ap.add_argument("--out-json", default="", help="Write summary JSON here (optional)")
    ap.add_argument("--out-csv", default="", help="Write mismatches CSV here (optional)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    summary, mismatches = compare(args.ref, args.file, args.abs_tol, args.rel_tol, args.verbose)

    # Emit console summary
    print(json.dumps(summary, indent=2))

    # Write artifacts if requested
    if args.out_json:
        os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
        with open(args.out_json, "w") as f:
            json.dump(summary, f, indent=2)

    if args.out_csv:
        os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
        (mismatches if not mismatches.empty else pd.DataFrame(
            columns=["date","B","C","ref_value","file_value","abs_diff"]
        )).to_csv(args.out_csv, index=False)

    # Exit non-zero if failed (so callers can choose to act on it)
    if summary["status"] != "PASSED":
        # non-fatal in our Batch pipeline because we call with "|| true"
        exit(1)

if __name__ == "__main__":
    main()