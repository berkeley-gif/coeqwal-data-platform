#!/usr/bin/env python3
"""
Scan all scenario CSVs in S3 for duplicate B-part (variable name) columns.

When two DSS pathnames share the same B-part but differ in their C-part
(e.g. SHRTG_PCWA3/DELIVERY-SHORTAGE vs SHRTG_PCWA3/SHORTAGE), the CSV
will contain two columns whose header row 1 is identical.  This script
detects those duplicates across every scenario and optionally compares the
underlying data values.

Usage:
    python scan_dupes.py                       # scan all known scenarios
    python scan_dupes.py --scenario s0025      # scan one scenario
    python scan_dupes.py --compare-values      # also compare data in duplicates
    python scan_dupes.py --workers 4           # parallel scanning
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

import boto3
import numpy as np
import pandas as pd

from scenarios import SCENARIOS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("scan_dupes")

BUCKET = "coeqwal-model-run"


def _csv_key(scenario_id: str) -> str:
    return f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv"


def scan_scenario_header(
    s3_client,
    scenario_id: str,
) -> List[Dict]:
    """Read only the 7-row header and report duplicate B-parts.

    Returns a list of dicts, one per duplicate variable name found:
        {scenario, variable, count, columns: [{index, a, b, c, e, f, type, units}]}
    """
    key = _csv_key(scenario_id)
    try:
        raw = s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except s3_client.exceptions.NoSuchKey:
        log.warning(f"{scenario_id}: CSV not found at {key}")
        return []
    except Exception as e:
        log.error(f"{scenario_id}: error reading CSV — {e}")
        return []

    hdr = pd.read_csv(io.BytesIO(raw), header=None, nrows=7, low_memory=False)
    b_row = [str(v) for v in hdr.iloc[1].tolist()]

    counts = Counter(b_row)
    dupes = {name: cnt for name, cnt in counts.items() if cnt > 1}

    if not dupes:
        return []

    results = []
    for var_name, cnt in sorted(dupes.items()):
        indices = [i for i, v in enumerate(b_row) if v == var_name]
        columns = []
        for i in indices:
            columns.append({
                "index": i,
                "a": str(hdr.iloc[0, i]),
                "b": str(hdr.iloc[1, i]),
                "c": str(hdr.iloc[2, i]),
                "e": str(hdr.iloc[3, i]),
                "f": str(hdr.iloc[4, i]),
                "type": str(hdr.iloc[5, i]),
                "units": str(hdr.iloc[6, i]),
            })
        results.append({
            "scenario": scenario_id,
            "variable": var_name,
            "count": cnt,
            "columns": columns,
        })

    return results


def compare_duplicate_values(
    s3_client,
    scenario_id: str,
    duplicates: List[Dict],
) -> List[Dict]:
    """Load full data for duplicate columns and compare values.

    Returns a list of comparison dicts per duplicate variable:
        {scenario, variable, n_rows, identical, max_abs_diff, correlation,
         col1_c_part, col2_c_part, col1_mean, col2_mean}
    """
    if not duplicates:
        return []

    key = _csv_key(scenario_id)
    try:
        raw = s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception as e:
        log.error(f"{scenario_id}: error reading CSV for value comparison — {e}")
        return []

    all_indices = set()
    for dup in duplicates:
        for col_info in dup["columns"]:
            all_indices.add(col_info["index"])

    data_df = pd.read_csv(
        io.BytesIO(raw),
        header=None,
        skiprows=7,
        usecols=sorted(all_indices),
        low_memory=False,
    )

    comparisons = []
    for dup in duplicates:
        cols = dup["columns"]
        if len(cols) < 2:
            continue

        col_a_idx = cols[0]["index"]
        col_b_idx = cols[1]["index"]

        series_a = pd.to_numeric(data_df[col_a_idx], errors="coerce")
        series_b = pd.to_numeric(data_df[col_b_idx], errors="coerce")

        both_valid = series_a.notna() & series_b.notna()
        a_valid = series_a[both_valid]
        b_valid = series_b[both_valid]

        n_rows = int(both_valid.sum())
        if n_rows == 0:
            comparisons.append({
                "scenario": scenario_id,
                "variable": dup["variable"],
                "n_rows": 0,
                "identical": False,
                "max_abs_diff": None,
                "correlation": None,
                "col1_c_part": cols[0]["c"],
                "col2_c_part": cols[1]["c"],
                "col1_mean": None,
                "col2_mean": None,
            })
            continue

        diff = (a_valid.values - b_valid.values)
        max_abs_diff = float(np.max(np.abs(diff)))
        identical = bool(np.allclose(a_valid.values, b_valid.values, atol=1e-10))

        corr = float(np.corrcoef(a_valid.values, b_valid.values)[0, 1]) if n_rows > 1 else None

        comparisons.append({
            "scenario": scenario_id,
            "variable": dup["variable"],
            "n_rows": n_rows,
            "identical": identical,
            "max_abs_diff": max_abs_diff,
            "correlation": corr,
            "col1_c_part": cols[0]["c"],
            "col2_c_part": cols[1]["c"],
            "col1_mean": float(a_valid.mean()),
            "col2_mean": float(b_valid.mean()),
        })

    return comparisons


def _scan_one(
    scenario_id: str,
    compare: bool,
) -> Tuple[List[Dict], List[Dict]]:
    """Scan a single scenario (thread-safe: creates its own S3 client)."""
    s3 = boto3.client("s3")
    dupes = scan_scenario_header(s3, scenario_id)
    comparisons = []
    if dupes and compare:
        comparisons = compare_duplicate_values(s3, scenario_id, dupes)
    return dupes, comparisons


def main():
    parser = argparse.ArgumentParser(
        description="Scan scenario CSVs for duplicate B-part columns"
    )
    parser.add_argument(
        "--scenario", "-s",
        help="Scan a single scenario (default: all known scenarios)",
    )
    parser.add_argument(
        "--compare-values",
        action="store_true",
        help="Also load data and compare values for any duplicates found",
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=4,
        help="Parallel workers for scanning (default: 4)",
    )
    parser.add_argument(
        "--output", "-o",
        default="duplicate_scan_results.csv",
        help="Output CSV path (default: duplicate_scan_results.csv)",
    )
    args = parser.parse_args()

    scenarios = [args.scenario] if args.scenario else list(SCENARIOS)
    workers = max(1, args.workers)

    log.info(f"Scanning {len(scenarios)} scenario(s) with {workers} worker(s)")
    if args.compare_values:
        log.info("Value comparison enabled for any duplicates found")

    t0 = time.time()
    all_dupes: List[Dict] = []
    all_comparisons: List[Dict] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_scan_one, sid, args.compare_values): sid
            for sid in scenarios
        }
        for future in as_completed(futures):
            sid = futures[future]
            try:
                dupes, comparisons = future.result()
                if dupes:
                    log.info(
                        f"  {sid}: {len(dupes)} duplicate variable(s) found"
                    )
                    all_dupes.extend(dupes)
                    all_comparisons.extend(comparisons)
                else:
                    log.info(f"  {sid}: clean")
            except Exception as e:
                log.error(f"  {sid}: scan error — {e}")

    elapsed = time.time() - t0
    log.info(f"Scan complete in {elapsed:.1f}s")

    # Print summary
    print(f"\n{'=' * 70}")
    print("  DUPLICATE B-PART SCAN RESULTS")
    print(f"{'=' * 70}")
    print(f"  Scenarios scanned: {len(scenarios)}")
    print(f"  Scenarios with duplicates: "
          f"{len(set(d['scenario'] for d in all_dupes))}")
    print(f"  Total duplicate variables: {len(all_dupes)}")
    print()

    if all_dupes:
        print("  DUPLICATES FOUND:")
        print(f"  {'─' * 66}")
        for dup in sorted(all_dupes, key=lambda d: (d["scenario"], d["variable"])):
            print(f"  {dup['scenario']}  {dup['variable']} ({dup['count']}x)")
            for col in dup["columns"]:
                print(
                    f"    col {col['index']:>5}: "
                    f"C={col['c']:<25}  units={col['units']}"
                )
        print()

    if all_comparisons:
        print("  VALUE COMPARISONS:")
        print(f"  {'─' * 66}")
        for comp in sorted(all_comparisons, key=lambda c: (c["scenario"], c["variable"])):
            ident_str = "IDENTICAL" if comp["identical"] else "DIFFERENT"
            print(
                f"  {comp['scenario']}  {comp['variable']}: "
                f"{ident_str}  "
                f"(max_diff={comp['max_abs_diff']:.6g}, "
                f"corr={comp['correlation']:.6f})"
                if comp["correlation"] is not None
                else f"  {comp['scenario']}  {comp['variable']}: "
                f"{ident_str}  (max_diff={comp['max_abs_diff']})"
            )
            print(
                f"    C-parts: {comp['col1_c_part']} vs {comp['col2_c_part']}  "
                f"means: {comp['col1_mean']:.4f} vs {comp['col2_mean']:.4f}"
            )
        print()

    print(f"{'=' * 70}\n")

    # Write CSV report
    if all_dupes:
        fieldnames = [
            "scenario", "variable", "count",
            "col1_index", "col1_c_part", "col1_units",
            "col2_index", "col2_c_part", "col2_units",
        ]
        if all_comparisons:
            fieldnames.extend([
                "identical", "max_abs_diff", "correlation",
                "col1_mean", "col2_mean",
            ])

        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for dup in sorted(all_dupes, key=lambda d: (d["scenario"], d["variable"])):
                row = {
                    "scenario": dup["scenario"],
                    "variable": dup["variable"],
                    "count": dup["count"],
                    "col1_index": dup["columns"][0]["index"],
                    "col1_c_part": dup["columns"][0]["c"],
                    "col1_units": dup["columns"][0]["units"],
                    "col2_index": dup["columns"][1]["index"] if len(dup["columns"]) > 1 else "",
                    "col2_c_part": dup["columns"][1]["c"] if len(dup["columns"]) > 1 else "",
                    "col2_units": dup["columns"][1]["units"] if len(dup["columns"]) > 1 else "",
                }
                comp = next(
                    (c for c in all_comparisons
                     if c["scenario"] == dup["scenario"]
                     and c["variable"] == dup["variable"]),
                    None,
                )
                if comp:
                    row.update({
                        "identical": comp["identical"],
                        "max_abs_diff": comp["max_abs_diff"],
                        "correlation": comp["correlation"],
                        "col1_mean": comp["col1_mean"],
                        "col2_mean": comp["col2_mean"],
                    })
                writer.writerow(row)

        log.info(f"Results written to {args.output}")
    else:
        log.info("No duplicates found — no output file written.")

    if all_dupes:
        sys.exit(1)


if __name__ == "__main__":
    main()
