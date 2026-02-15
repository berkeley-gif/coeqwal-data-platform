#!/usr/bin/env python3
"""
Validation Reporter

Enhances the existing validate_csvs.py with better console reporting
and unmatched cell listings without changing the core validation logic.
"""

import argparse
import json
import pandas as pd
from validate_csvs import compare


def print_validation_report(summary: dict, mismatches_df: pd.DataFrame, show_details: bool = True, max_details: int = 20):
    """Print a clean, informative validation report."""
    
    print("\nValidation Results")
    print("-" * 40)
    
    # Status
    status = summary["status"]
    print(f"Status: {status}")
    
    if status == "PASSED":
        print("All values match within tolerances")
        return
    
    # Quick stats
    print(f"Mismatch cells: {summary['mismatch_cells']:,}")
    print(f"Columns with mismatches: {summary['mismatch_columns']}")
    print(f"Total cells compared: {summary['rows_in_overlap'] * summary['columns_common']:,}")
    
    if summary['mismatch_cells'] > 0:
        total_cells = summary['rows_in_overlap'] * summary['columns_common']
        mismatch_rate = summary['mismatch_cells'] / total_cells * 100
        print(f"Mismatch rate: {mismatch_rate:.3f}%")
    
    # Missing columns
    if summary.get('columns_only_in_ref', 0) > 0:
        print(f"Columns only in reference: {summary['columns_only_in_ref']}")
        if 'sample_only_in_ref' in summary:
            print(f"  Examples: {', '.join(summary['sample_only_in_ref'][:3])}")
    
    if summary.get('columns_only_in_file', 0) > 0:
        print(f"Columns only in test file: {summary['columns_only_in_file']}")
        if 'sample_only_in_file' in summary:
            print(f"  Examples: {', '.join(summary['sample_only_in_file'][:3])}")
    
    # Detailed unmatched cells
    if show_details and not mismatches_df.empty:
        print(f"\nUnmatched cells (showing first {max_details}):")
        print("-" * 60)
        
        # Sort by absolute difference (largest first) for most important mismatches
        display_df = mismatches_df.copy()
        display_df['abs_diff_numeric'] = pd.to_numeric(display_df['abs_diff'], errors='coerce')
        display_df = display_df.sort_values('abs_diff_numeric', ascending=False, na_position='last')
        
        for i, (_, row) in enumerate(display_df.head(max_details).iterrows()):
            ref_val = row['ref_value']
            file_val = row['file_value']
            diff = row['abs_diff']
            
            print(f"{i+1:2d}. {row['date']} | {row['B']}|{row['C']}")
            print(f"    Reference: {ref_val}")
            print(f"    File:      {file_val}")
            print(f"    Difference: {diff}")
            print()
        
        if len(mismatches_df) > max_details:
            print(f"... and {len(mismatches_df) - max_details:,} more mismatches")
            print(f"Use --out-csv to save all mismatches to file")


def main():
    parser = argparse.ArgumentParser(description="Enhanced validation reporting")
    parser.add_argument("--ref", required=True, help="Reference CSV")
    parser.add_argument("--file", required=True, help="Test CSV")
    parser.add_argument("--abs-tol", type=float, default=1e-6, help="Absolute tolerance")
    parser.add_argument("--rel-tol", type=float, default=1e-6, help="Relative tolerance")
    parser.add_argument("--show-details", action="store_true", help="Show detailed unmatched cells")
    parser.add_argument("--max-details", type=int, default=20, help="Max unmatched cells to show")
    parser.add_argument("--out-csv", help="Save all mismatches to CSV file")
    parser.add_argument("--out-json", help="Save summary to JSON file")
    parser.add_argument("--verbose", action="store_true")
    
    args = parser.parse_args()
    
    try:
        # Use existing validation logic
        summary, mismatches = compare(args.ref, args.file, args.abs_tol, args.rel_tol, args.verbose)
        
        # Enhanced console reporting
        print_validation_report(summary, mismatches, args.show_details, args.max_details)
        
        # Save outputs if requested
        if args.out_csv and not mismatches.empty:
            mismatches.to_csv(args.out_csv, index=False)
            print(f"\nAll mismatches saved to: {args.out_csv}")
        
        if args.out_json:
            with open(args.out_json, 'w') as f:
                json.dump(summary, f, indent=2)
            print(f"Summary saved to: {args.out_json}")
        
        # Exit with validation status
        exit(0 if summary["status"] == "PASSED" else 1)
        
    except Exception as e:
        print(f"ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        exit(2)


if __name__ == "__main__":
    main()
