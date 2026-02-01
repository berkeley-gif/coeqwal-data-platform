#!/usr/bin/env python3
"""
Verification script to compare ETL metrics against COEQWAL notebook outputs.

This script calculates the same metrics as Metrics.ipynb and can compare
against the notebook's all_metrics_output.csv when available.

Metrics calculated (matching notebook column names):
- All_Prob_S_{RES}_flood   - Flood pool probability (all months)
- All_Prob_S_{RES}_dead    - Dead pool probability (all months)
- Sept_Prob_S_{RES}_flood  - September flood probability
- Sept_Prob_S_{RES}_dead   - September dead pool probability
- Apr_Avg_S_{RES}_TAF      - April average storage
- Sep_Avg_S_{RES}_TAF      - September average storage
- AprS_{RES}__CV           - April coefficient of variation
- SeptS_{RES}__CV          - September coefficient of variation

Usage:
    python verify_metrics.py                    # Calculate and display metrics
    python verify_metrics.py --compare FILE    # Compare against notebook output
    python verify_metrics.py --output FILE     # Save metrics to CSV
"""

import argparse
import csv
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from reservoirs.reservoir_metrics import (
    calculate_flood_pool_probability,
    calculate_dead_pool_probability,
    calculate_cv,
    calculate_monthly_average,
    RESERVOIR_THRESHOLDS,
)
from reservoirs.calculate_reservoir_statistics import (
    load_reservoir_entities,
    load_scenario_csv_from_file,
    parse_scenario_csv,
    add_water_year_month,
)

# Reservoirs that the notebook calculates metrics for
NOTEBOOK_RESERVOIRS = ['SHSTA', 'OROVL', 'TRNTY', 'FOLSM', 'MELON', 'MLRTN', 'SLUIS_CVP', 'SLUIS_SWP']


def calculate_notebook_metrics(df: pd.DataFrame, reservoir_code: str) -> dict:
    """
    Calculate metrics matching the notebook's output columns.

    Returns dict with keys matching notebook column names EXACTLY.

    Notebook naming conventions (from all_metrics_output.csv):
    - Sep_Prob_* (not Sept_Prob_*)
    - Apr_S_{RES}_CV (not AprS_{RES}__CV)
    - Sep_S_{RES}_CV (not SeptS_{RES}__CV)
    """
    storage_col = f'S_{reservoir_code}'
    if storage_col not in df.columns:
        return {}

    storage = df[storage_col]
    date_idx = df['DateTime']

    metrics = {}

    # Get threshold values
    flood_threshold = None
    dead_threshold = None

    if reservoir_code in RESERVOIR_THRESHOLDS:
        threshold_info = RESERVOIR_THRESHOLDS[reservoir_code]

        # Flood threshold
        flood_var = threshold_info.get('flood_var')
        if isinstance(flood_var, str) and flood_var in df.columns:
            flood_threshold = df[flood_var]
        elif isinstance(flood_var, (int, float)):
            flood_threshold = float(flood_var)

        # Dead pool threshold
        dead_var = threshold_info.get('dead_var')
        if isinstance(dead_var, str) and dead_var in df.columns:
            dead_threshold = df[dead_var]
        elif isinstance(dead_var, (int, float)):
            dead_threshold = float(dead_var)

    # Flood pool probabilities
    if flood_threshold is not None:
        # All months
        fp_all = calculate_flood_pool_probability(storage, flood_threshold)
        metrics[f'All_Prob_S_{reservoir_code}_flood'] = fp_all['probability']

        # September only (notebook uses "Sep_" not "Sept_")
        fp_sep = calculate_flood_pool_probability(
            storage, flood_threshold, months=[9], date_index=date_idx
        )
        metrics[f'Sep_Prob_S_{reservoir_code}_flood'] = fp_sep['probability']

    # Dead pool probabilities
    if dead_threshold is not None:
        # All months
        dp_all = calculate_dead_pool_probability(storage, dead_threshold)
        metrics[f'All_Prob_S_{reservoir_code}_dead'] = dp_all['probability']

        # September only (notebook uses "Sep_" not "Sept_")
        dp_sep = calculate_dead_pool_probability(
            storage, dead_threshold, months=[9], date_index=date_idx
        )
        metrics[f'Sep_Prob_S_{reservoir_code}_dead'] = dp_sep['probability']

    # Monthly averages (TAF)
    # Note: SLUIS_CVP and SLUIS_SWP use different naming in notebook (no underscore before TAF)
    if reservoir_code in ['SLUIS_CVP', 'SLUIS_SWP']:
        # Notebook: Apr_Avg_S_SLUIS_SWPTAF (no underscore)
        metrics[f'Apr_Avg_S_{reservoir_code}TAF'] = calculate_monthly_average(
            storage, date_idx, month=4
        )
        metrics[f'Sep_Avg_S_{reservoir_code}TAF'] = calculate_monthly_average(
            storage, date_idx, month=9
        )
        # Notebook: Apr_S_SLUIS_SWPCV (no underscore)
        metrics[f'Apr_S_{reservoir_code}CV'] = calculate_cv(
            storage, months=[4], date_index=date_idx
        )
        metrics[f'Sep_S_{reservoir_code}CV'] = calculate_cv(
            storage, months=[9], date_index=date_idx
        )
    else:
        metrics[f'Apr_Avg_S_{reservoir_code}_TAF'] = calculate_monthly_average(
            storage, date_idx, month=4
        )
        metrics[f'Sep_Avg_S_{reservoir_code}_TAF'] = calculate_monthly_average(
            storage, date_idx, month=9
        )
        # Coefficient of Variation (notebook uses "Apr_S_{RES}_CV" format)
        metrics[f'Apr_S_{reservoir_code}_CV'] = calculate_cv(
            storage, months=[4], date_index=date_idx
        )
        metrics[f'Sep_S_{reservoir_code}_CV'] = calculate_cv(
            storage, months=[9], date_index=date_idx
        )

    return metrics


def load_notebook_output(csv_path: str) -> pd.DataFrame:
    """Load the notebook's all_metrics_output.csv."""
    return pd.read_csv(csv_path, index_col=0)


def compare_metrics(calculated: dict, expected: pd.Series, tolerance: float = 0.0001) -> dict:
    """
    Compare calculated metrics against expected values.

    Args:
        calculated: Dict of metric_name -> value from our ETL
        expected: Series of expected values from notebook (index = metric names)
        tolerance: Relative tolerance (default 1%)

    Returns:
        Dict with comparison results
    """
    results = {
        'passed': 0,
        'failed': 0,
        'missing': 0,
        'details': [],
    }

    for metric_name, calc_value in calculated.items():
        if metric_name not in expected.index:
            results['missing'] += 1
            results['details'].append({
                'metric': metric_name,
                'status': 'MISSING',
                'calculated': calc_value,
                'expected': None,
            })
            continue

        exp_value = expected[metric_name]

        # Handle probability conversion (notebook converts to 0-100)
        if 'Prob_' in metric_name:
            # Notebook stores as 0-100, we calculate as 0-1
            exp_value_normalized = exp_value / 100.0 if exp_value > 1 else exp_value
        else:
            exp_value_normalized = exp_value

        # Calculate difference
        if exp_value_normalized == 0:
            passed = abs(calc_value) < tolerance
        else:
            rel_diff = abs((calc_value - exp_value_normalized) / exp_value_normalized)
            passed = rel_diff < tolerance

        if passed:
            results['passed'] += 1
            status = 'PASS'
        else:
            results['failed'] += 1
            status = 'FAIL'

        results['details'].append({
            'metric': metric_name,
            'status': status,
            'calculated': calc_value,
            'expected': exp_value_normalized,
            'diff': calc_value - exp_value_normalized if exp_value_normalized else calc_value,
        })

    return results


def main():
    parser = argparse.ArgumentParser(description='Verify ETL metrics against notebook output')
    parser.add_argument('--scenario', '-s', default='s0020', help='Scenario ID')
    parser.add_argument('--csv-path', default=None, help='Path to scenario CSV')
    parser.add_argument('--compare', help='Path to notebook all_metrics_output.csv for comparison')
    parser.add_argument('--output', '-o', help='Save calculated metrics to CSV')
    parser.add_argument('--reservoirs', nargs='+', default=NOTEBOOK_RESERVOIRS,
                        help='Reservoirs to calculate (default: notebook reservoirs)')
    args = parser.parse_args()

    # Paths
    script_dir = Path(__file__).parent
    default_csv = script_dir / '../pipelines/s0020_coeqwal_calsim_output.csv'
    csv_path = Path(args.csv_path) if args.csv_path else default_csv

    print("=" * 70)
    print("COEQWAL Metrics Verification")
    print("=" * 70)
    print(f"Scenario: {args.scenario}")
    print(f"CSV: {csv_path}")
    print(f"Reservoirs: {', '.join(args.reservoirs)}")
    print()

    if not csv_path.exists():
        print(f"ERROR: CSV file not found at {csv_path}")
        return 1

    # Load reservoir metadata
    reservoirs = load_reservoir_entities()

    # Load and parse CSV
    print("Loading data...")
    raw_df = load_scenario_csv_from_file(str(csv_path), args.reservoirs)
    df = parse_scenario_csv(raw_df)
    df = add_water_year_month(df)
    print(f"Loaded {len(df)} rows ({df['DateTime'].min()} to {df['DateTime'].max()})")
    print()

    # Calculate metrics for each reservoir
    all_metrics = {}
    print("Calculating metrics...")
    print("-" * 70)

    for res_code in args.reservoirs:
        if res_code not in reservoirs:
            print(f"  {res_code}: Not found in reservoir metadata, skipping")
            continue

        metrics = calculate_notebook_metrics(df, res_code)
        if not metrics:
            print(f"  {res_code}: No storage data found")
            continue

        all_metrics.update(metrics)

        # Display key metrics
        print(f"\n  {res_code}:")
        flood_prob = metrics.get(f'All_Prob_S_{res_code}_flood')
        dead_prob = metrics.get(f'All_Prob_S_{res_code}_dead')
        apr_avg = metrics.get(f'Apr_Avg_S_{res_code}_TAF')
        sep_avg = metrics.get(f'Sep_Avg_S_{res_code}_TAF')
        apr_cv = metrics.get(f'AprS_{res_code}__CV')
        sep_cv = metrics.get(f'SeptS_{res_code}__CV')

        if flood_prob is not None:
            print(f"    Flood Pool Prob (all): {flood_prob:.4f} ({flood_prob*100:.2f}%)")
        if dead_prob is not None:
            print(f"    Dead Pool Prob (all):  {dead_prob:.4f} ({dead_prob*100:.2f}%)")
        if apr_avg is not None:
            print(f"    April Avg:             {apr_avg:.2f} TAF")
        if sep_avg is not None:
            print(f"    September Avg:         {sep_avg:.2f} TAF")
        if apr_cv is not None:
            print(f"    April CV:              {apr_cv:.4f}")
        if sep_cv is not None:
            print(f"    September CV:          {sep_cv:.4f}")

    print()
    print("-" * 70)
    print(f"Total metrics calculated: {len(all_metrics)}")

    # Compare against notebook output if provided
    if args.compare:
        print()
        print("=" * 70)
        print("COMPARISON WITH NOTEBOOK OUTPUT")
        print("=" * 70)

        try:
            notebook_df = load_notebook_output(args.compare)
            print(f"Loaded notebook output: {args.compare}")
            print(f"Scenarios in notebook: {list(notebook_df.index)}")

            # Find matching scenario
            scenario_row = None
            for idx in notebook_df.index:
                if args.scenario in str(idx):
                    scenario_row = notebook_df.loc[idx]
                    print(f"Using scenario row: {idx}")
                    break

            if scenario_row is None:
                print(f"ERROR: Scenario {args.scenario} not found in notebook output")
            else:
                results = compare_metrics(all_metrics, scenario_row)
                print()
                print(f"Results: {results['passed']} passed, {results['failed']} failed, "
                      f"{results['missing']} missing from notebook")
                print()

                if results['failed'] > 0:
                    print("Failed comparisons:")
                    for detail in results['details']:
                        if detail['status'] == 'FAIL':
                            print(f"  {detail['metric']}:")
                            print(f"    Calculated: {detail['calculated']:.6f}")
                            print(f"    Expected:   {detail['expected']:.6f}")
                            print(f"    Diff:       {detail['diff']:.6f}")

                if results['missing'] > 0:
                    print("\nMissing from notebook (ETL calculates, notebook doesn't):")
                    for detail in results['details']:
                        if detail['status'] == 'MISSING':
                            print(f"  {detail['metric']}: {detail['calculated']:.6f}")

        except Exception as e:
            print(f"ERROR loading notebook output: {e}")

    # Save output if requested
    if args.output:
        output_df = pd.DataFrame([all_metrics], index=[args.scenario])
        output_df.to_csv(args.output)
        print(f"\nSaved metrics to: {args.output}")

    print()
    print("=" * 70)
    print("Verification complete")
    print("=" * 70)

    return 0


if __name__ == '__main__':
    sys.exit(main())
