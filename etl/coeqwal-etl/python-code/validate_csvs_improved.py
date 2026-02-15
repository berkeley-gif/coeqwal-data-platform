#!/usr/bin/env python3
"""
Improved CSV Validation with Enhanced Reporting

Validates DSS-style CSVs and provides detailed unmatched cell listings
and improved console output for better debugging.
"""

import argparse
import json
import os
from typing import Dict, Tuple, List
import numpy as np
import pandas as pd

HEADER_ROWS = 7  # A,B,C,E,F,TYPE,UNITS (and first col contains those labels)


def _read_dss_csv(path: str):
    """
    Read a DSS-style CSV exported by your pipeline.
    Returns: idx_dates, data_df, meta_dict
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

    # Rename columns to (B,C) tuples
    df = pd.DataFrame(index=data.index)
    for j, key in col_keys.items():
        if j-1 < data.shape[1]:  # j is 1-based, data columns are 0-based
            df[key] = pd.to_numeric(data.iloc[:, j-1], errors='coerce')

    meta = {
        "num_columns": len(col_keys),
        "b_row": b_idx,
        "c_row": c_idx,
        "path": path,
    }
    return df.index, df, meta


def compare_with_detailed_reporting(ref_path: str, file_path: str, abs_tol: float, rel_tol: float, 
                                  verbose: bool = False, show_unmatched: bool = True):
    """
    Enhanced comparison with detailed unmatched cell reporting.
    """
    idx1, df1, meta1 = _read_dss_csv(ref_path)
    idx2, df2, meta2 = _read_dss_csv(file_path)

    if verbose:
        print(f"Reference file: {meta1['path']} ({meta1['num_columns']} columns)")
        print(f"Test file: {meta2['path']} ({meta2['num_columns']} columns)")

    # Overlapping (B,C) keys
    common_keys = sorted(set(df1.columns).intersection(set(df2.columns)))
    only_in_ref = sorted(set(df1.columns) - set(df2.columns))
    only_in_file = sorted(set(df2.columns) - set(df1.columns))

    # Overlapping dates
    overlap_start = max(idx1.min(), idx2.min()) if len(idx1) and len(idx2) else None
    overlap_end = min(idx1.max(), idx2.max()) if len(idx1) and len(idx2) else None

    if overlap_start is None or overlap_end is None or overlap_start > overlap_end:
        raise ValueError("No overlapping date range between the two files.")

    # Slice to overlapping date range
    df1o = df1.loc[(df1.index >= overlap_start) & (df1.index <= overlap_end), common_keys].copy()
    df2o = df2.loc[(df2.index >= overlap_start) & (df2.index <= overlap_end), common_keys].copy()

    # Ensure same index (inner join on dates)
    common_dates = sorted(set(df1o.index).intersection(set(df2o.index)))
    df1o = df1o.loc[common_dates]
    df2o = df2o.loc[common_dates]

    # Enhanced mismatch analysis
    mismatch_details = []
    column_summaries = []
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
            column_mismatches = mismask.sum()
            total_mismatch_cells += column_mismatches
            
            # Column-level summary
            column_summaries.append({
                "B": key[0],
                "C": key[1],
                "mismatches": int(column_mismatches),
                "total_cells": int(len(s1)),
                "mismatch_rate": float(column_mismatches / len(s1))
            })
            
            # Detailed cell-level mismatches
            for dt in s1.index[mismask]:
                v1 = s1.loc[dt]
                v2 = s2.loc[dt]
                diff = (abs(v1 - v2) if (pd.notna(v1) and pd.notna(v2)) else np.nan)
                
                # Categorize mismatch type
                if pd.isna(v1) and pd.notna(v2):
                    mismatch_type = "missing_in_ref"
                elif pd.notna(v1) and pd.isna(v2):
                    mismatch_type = "missing_in_file"
                elif pd.notna(v1) and pd.notna(v2):
                    if abs(v1 - v2) > abs_tol and abs(v1 - v2) / max(abs(v1), abs(v2), 1e-10) > rel_tol:
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
                    "mismatch_type": mismatch_type
                })

    mismatches_df = pd.DataFrame(mismatch_details)

    # Enhanced summary with more details
    summary = {
        "status": "PASSED" if total_mismatch_cells == 0 else "FAILED",
        "validation_summary": {
            "total_columns_compared": len(common_keys),
            "columns_with_mismatches": cols_with_mismatch,
            "total_mismatch_cells": int(total_mismatch_cells),
            "total_cells_compared": int(len(common_keys) * len(common_dates)),
            "mismatch_rate": float(total_mismatch_cells / (len(common_keys) * len(common_dates))) if common_keys and common_dates else 0
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
            "relative_tolerance": rel_tol
        },
        "files": {
            "ref_path": meta1["path"],
            "file_path": meta2["path"],
        }
    }

    # Add column-level summaries
    if column_summaries:
        summary["column_mismatches"] = column_summaries

    # Add sample missing keys for debugging
    if only_in_ref:
        summary["sample_only_in_ref"] = [f"{b}|{c}" for (b,c) in only_in_ref[:10]]
    if only_in_file:
        summary["sample_only_in_file"] = [f"{b}|{c}" for (b,c) in only_in_file[:10]]

    return summary, mismatches_df, column_summaries


def print_console_report(summary: dict, mismatches_df: pd.DataFrame, column_summaries: List[dict], 
                        show_unmatched: bool = True, max_unmatched: int = 50):
    """Print a user-friendly console report of validation results."""
    
    print("\n" + "="*60)
    print("CSV VALIDATION REPORT")
    print("="*60)
    
    # Overall status
    status = summary["status"]
    print(f"Status: {status}")
    
    if status == "PASSED":
        print("All values match within specified tolerances.")
        return
    
    # Summary statistics
    val_summary = summary["validation_summary"]
    print(f"\nSummary:")
    print(f"  Total cells compared: {val_summary['total_cells_compared']:,}")
    print(f"  Mismatch cells: {val_summary['total_mismatch_cells']:,}")
    print(f"  Mismatch rate: {val_summary['mismatch_rate']:.4%}")
    print(f"  Columns with mismatches: {val_summary['columns_with_mismatches']}/{val_summary['total_columns_compared']}")
    
    # Column-level breakdown
    if column_summaries:
        print(f"\nColumns with highest mismatch rates:")
        sorted_cols = sorted(column_summaries, key=lambda x: x['mismatch_rate'], reverse=True)
        for col in sorted_cols[:10]:  # Top 10 worst columns
            print(f"  {col['B']}|{col['C']}: {col['mismatches']:,}/{col['total_cells']:,} ({col['mismatch_rate']:.2%})")
    
    # Unmatched cell details
    if show_unmatched and not mismatches_df.empty:
        print(f"\nUnmatched cells (showing first {max_unmatched}):")
        print("-" * 80)
        
        # Group by mismatch type for better organization
        mismatch_types = mismatches_df['mismatch_type'].value_counts()
        print("Mismatch types:")
        for mtype, count in mismatch_types.items():
            print(f"  {mtype}: {count:,} cells")
        
        print(f"\nDetailed mismatches:")
        display_df = mismatches_df.head(max_unmatched)[['date', 'B', 'C', 'ref_value', 'file_value', 'abs_diff', 'mismatch_type']]
        
        # Format for better readability
        for _, row in display_df.iterrows():
            print(f"  {row['date']} | {row['B']}|{row['C']} | Ref: {row['ref_value']} | File: {row['file_value']} | Diff: {row['abs_diff']} | Type: {row['mismatch_type']}")
        
        if len(mismatches_df) > max_unmatched:
            print(f"  ... and {len(mismatches_df) - max_unmatched:,} more mismatches")
    
    # Missing columns
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
    parser = argparse.ArgumentParser(description="Enhanced DSS CSV validation with detailed reporting")
    parser.add_argument("--ref", required=True, help="Reference CSV (e.g., Trend Report)")
    parser.add_argument("--file", required=True, help="CSV produced by pipeline")
    parser.add_argument("--abs-tol", type=float, default=1e-6, help="Absolute tolerance (default 1e-6)")
    parser.add_argument("--rel-tol", type=float, default=1e-6, help="Relative tolerance (default 1e-6)")
    parser.add_argument("--out-json", default="", help="Write summary JSON here (optional)")
    parser.add_argument("--out-csv", default="", help="Write mismatches CSV here (optional)")
    parser.add_argument("--show-unmatched", action="store_true", help="Show detailed unmatched cells in console")
    parser.add_argument("--max-unmatched", type=int, default=50, help="Max unmatched cells to show (default 50)")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--console-only", action="store_true", help="Only show console report, no JSON")
    
    args = parser.parse_args()

    try:
        summary, mismatches, column_summaries = compare_with_detailed_reporting(
            args.ref, args.file, args.abs_tol, args.rel_tol, args.verbose
        )
        
        # Console report (always show if requested or if console-only)
        if args.show_unmatched or args.console_only:
            print_console_report(summary, mismatches, column_summaries, 
                                args.show_unmatched, args.max_unmatched)
        
        # JSON output (unless console-only)
        if not args.console_only:
            print("\nJSON Summary:")
            print(json.dumps(summary, indent=2))

        # Write artifacts if requested
        if args.out_json:
            os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
            with open(args.out_json, "w") as f:
                json.dump(summary, f, indent=2)
            print(f"\nSummary saved to: {args.out_json}")

        if args.out_csv:
            os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
            if not mismatches.empty:
                mismatches.to_csv(args.out_csv, index=False)
                print(f"Mismatches saved to: {args.out_csv}")
            else:
                # Create empty file with headers
                pd.DataFrame(columns=["date","B","C","ref_value","file_value","abs_diff","mismatch_type"]).to_csv(args.out_csv, index=False)
                print(f"No mismatches - empty file saved to: {args.out_csv}")

        # Exit with appropriate code
        exit(0 if summary["status"] == "PASSED" else 1)
        
    except Exception as e:
        print(f"ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        exit(2)


if __name__ == "__main__":
    main()
