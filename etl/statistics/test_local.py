#!/usr/bin/env python3
"""
Local test script for statistics ETL.

Runs a quick sanity check using the local CSV file to verify calculations work.

Usage:
    python test_local.py
    python test_local.py --verbose
    python test_local.py --reservoir SHSTA
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))


from reservoirs.reservoir_metrics import (
    summarize_probability_metrics,
    RESERVOIR_THRESHOLDS,
)
from reservoirs.calculate_reservoir_statistics import (
    load_reservoir_entities,
    load_scenario_csv_from_file,
    parse_scenario_csv,
    add_water_year_month,
    calculate_storage_monthly,
    calculate_period_summary,
)
from reservoirs.calculate_reservoir_percentiles import (
    calculate_percentiles_for_reservoir,
)


def main():
    parser = argparse.ArgumentParser(description='Test local ETL calculations')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--reservoir', '-r', default='SHSTA', help='Reservoir to test (default: SHSTA)')
    parser.add_argument('--csv-path', default=None, help='Path to CSV file')
    args = parser.parse_args()

    # Paths
    script_dir = Path(__file__).parent
    default_csv = script_dir / '../pipelines/s0020_coeqwal_calsim_output.csv'
    csv_path = Path(args.csv_path) if args.csv_path else default_csv

    print("=" * 60)
    print("COEQWAL Statistics ETL - Local Test")
    print("=" * 60)

    # Check CSV exists
    if not csv_path.exists():
        print(f"\nERROR: CSV file not found at {csv_path}")
        print("Please specify a valid path with --csv-path")
        return 1

    print(f"\nCSV File: {csv_path}")
    print(f"Test Reservoir: {args.reservoir}")

    # Load reservoir metadata
    print("\n1. Loading reservoir metadata...")
    try:
        reservoirs = load_reservoir_entities()
        print(f"   Loaded {len(reservoirs)} reservoirs")
    except Exception as e:
        print(f"   ERROR: {e}")
        return 1

    # Check test reservoir exists
    if args.reservoir not in reservoirs:
        print(f"\nERROR: Reservoir {args.reservoir} not found in metadata")
        print(f"Available: {', '.join(list(reservoirs.keys())[:10])}...")
        return 1

    res_meta = reservoirs[args.reservoir]
    print(f"   {args.reservoir}: capacity={res_meta['capacity_taf']} TAF, "
          f"dead_pool={res_meta['dead_pool_taf']} TAF")

    # Load and parse CSV
    print("\n2. Loading CSV data...")
    try:
        reservoir_codes = [args.reservoir]
        raw_df = load_scenario_csv_from_file(str(csv_path), reservoir_codes)
        df = parse_scenario_csv(raw_df)
        df = add_water_year_month(df)
        print(f"   Loaded {len(df)} rows")
        print(f"   Date range: {df['DateTime'].min()} to {df['DateTime'].max()}")
    except Exception as e:
        print(f"   ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    # Check if storage column exists
    storage_col = f'S_{args.reservoir}'
    if storage_col not in df.columns:
        print(f"\nERROR: Storage column {storage_col} not found in data")
        print(f"Available columns: {list(df.columns)}")
        return 1

    # Test storage monthly calculations
    print("\n3. Testing storage monthly calculations...")
    try:
        storage_monthly = calculate_storage_monthly(
            df, args.reservoir, res_meta['capacity_taf']
        )
        print(f"   Generated {len(storage_monthly)} monthly records")

        if args.verbose and storage_monthly:
            print("\n   Sample (Water Month 1 = October):")
            row = storage_monthly[0]
            print(f"   Mean TAF: {row['storage_avg_taf']}")
            print(f"   CV: {row['storage_cv']}")
            print(f"   % Capacity: {row['storage_pct_capacity']}%")
            print(f"   q50 (median): {row['q50']}% / {row.get('q50_taf', 'N/A')} TAF")
    except Exception as e:
        print(f"   ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    # Test period summary calculations
    print("\n4. Testing period summary calculations...")
    try:
        summary = calculate_period_summary(
            df, args.reservoir,
            res_meta['capacity_taf'],
            res_meta['dead_pool_taf']
        )
        if summary:
            print(f"   Period: WY{summary['simulation_start_year']}-{summary['simulation_end_year']}")
            print(f"   Years: {summary['total_years']}")

            if args.verbose:
                print("\n   Probability Metrics:")
                flood_prob = summary.get('flood_pool_prob_all')
                dead_prob = summary.get('dead_pool_prob_all')
                cv_all = summary.get('storage_cv_all')

                if flood_prob is not None:
                    print(f"   Flood Pool Probability (all): {flood_prob:.4f} ({flood_prob*100:.2f}%)")
                else:
                    print("   Flood Pool Probability: Not calculated (no threshold)")

                if dead_prob is not None:
                    print(f"   Dead Pool Probability (all): {dead_prob:.4f} ({dead_prob*100:.2f}%)")
                else:
                    print("   Dead Pool Probability: Not calculated (no threshold)")

                if cv_all is not None:
                    print(f"   Storage CV (all): {cv_all:.4f}")

                # Risk summary
                risk = summarize_probability_metrics(summary)
                print("\n   Risk Assessment:")
                for key, value in risk.items():
                    print(f"   - {key}: {value}")
        else:
            print("   No summary generated")
    except Exception as e:
        print(f"   ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    # Test percentile calculations
    print("\n5. Testing percentile calculations...")
    try:
        percentiles = calculate_percentiles_for_reservoir(
            df, args.reservoir, res_meta['capacity_taf']
        )
        print(f"   Generated percentiles for {len(percentiles)} months")

        if args.verbose and percentiles:
            print("\n   Water Month 1 (October) percentiles:")
            oct_pct = percentiles.get(1, {})
            for key in ['q0', 'q10', 'q50', 'q90', 'q100', 'mean']:
                pct_val = oct_pct.get(key, 'N/A')
                taf_val = oct_pct.get(f'{key}_taf', 'N/A')
                print(f"   {key}: {pct_val}% / {taf_val} TAF")
    except Exception as e:
        print(f"   ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    # Show threshold info
    print("\n6. Threshold configuration...")
    if args.reservoir in RESERVOIR_THRESHOLDS:
        thresh = RESERVOIR_THRESHOLDS[args.reservoir]
        print(f"   {args.reservoir} has custom thresholds:")
        print(f"   - Flood: {thresh.get('flood_var', 'None')}")
        print(f"   - Dead: {thresh.get('dead_var', 'None (uses entity dead_pool)')}")
    else:
        print(f"   {args.reservoir} uses default thresholds (entity dead_pool_taf)")

    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)

    if not args.verbose:
        print("\nTip: Run with --verbose for detailed output")

    return 0


if __name__ == '__main__':
    sys.exit(main())
