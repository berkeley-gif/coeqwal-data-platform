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
from pathlib import Path
from typing import Dict, List, Tuple

import boto3
import numpy as np
import pandas as pd

from scenarios import SCENARIOS

# Default directory for scan results. Gitignored via etl/**/audit_reports/.
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "audit_reports"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("scan_dupes")

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from etl.common import S3_BUCKET as BUCKET  # noqa: E402


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


def audit_scenario_units(
    s3_client,
    scenario_id: str,
) -> Dict[str, str]:
    """Read the header and return a {variable_name: unit} map.

    Only includes variables with well-known ETL prefixes so the audit
    focuses on columns the pipeline actually consumes.
    """
    _ETL_PREFIXES = (
        "AW_", "DN_", "GP_", "SHRTG_", "GW_SHORT_", "DEL_", "SHORT_",
        "S_", "C_", "D_", "E_", "F_",
    )
    key = _csv_key(scenario_id)
    try:
        raw = s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception:
        return {}

    hdr = pd.read_csv(io.BytesIO(raw), header=None, nrows=7, low_memory=False)
    b_row = [str(v) for v in hdr.iloc[1].tolist()]
    units_row = [str(u).strip().upper() for u in hdr.iloc[6].tolist()]

    result = {}
    for name, unit in zip(b_row, units_row):
        if any(name.startswith(p) for p in _ETL_PREFIXES):
            if name not in result:
                result[name] = unit
    return result


def _scan_one(
    scenario_id: str,
    compare: bool,
    audit_units: bool,
) -> Tuple[List[Dict], List[Dict], Dict[str, str]]:
    """Scan a single scenario (thread-safe: creates its own S3 client)."""
    s3 = boto3.client("s3")
    dupes = scan_scenario_header(s3, scenario_id)
    comparisons = []
    if dupes and compare:
        comparisons = compare_duplicate_values(s3, scenario_id, dupes)
    units = {}
    if audit_units:
        units = audit_scenario_units(s3, scenario_id)
    return dupes, comparisons, units


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
        "--audit-units",
        action="store_true",
        help="Audit unit declarations across scenarios. Reports any variable "
        "whose declared unit differs between scenarios.",
    )
    parser.add_argument(
        "--output", "-o",
        default=str(DEFAULT_OUTPUT_DIR / "duplicate_scan_results.csv"),
        help=f"Output CSV path (default: "
             f"{DEFAULT_OUTPUT_DIR / 'duplicate_scan_results.csv'}). "
             "Parent dir is auto-created. Sibling _units.csv is written "
             "alongside.",
    )
    args = parser.parse_args()

    scenarios = [args.scenario] if args.scenario else list(SCENARIOS)
    workers = max(1, args.workers)

    log.info(f"Scanning {len(scenarios)} scenario(s) with {workers} worker(s)")
    if args.compare_values:
        log.info("Value comparison enabled for any duplicates found")
    if args.audit_units:
        log.info("Unit audit enabled")

    t0 = time.time()
    all_dupes: List[Dict] = []
    all_comparisons: List[Dict] = []
    # {variable_name: {unit: [scenario_ids]}}
    unit_registry: Dict[str, Dict[str, List[str]]] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _scan_one, sid, args.compare_values, args.audit_units
            ): sid
            for sid in scenarios
        }
        for future in as_completed(futures):
            sid = futures[future]
            try:
                dupes, comparisons, units = future.result()
                if dupes:
                    log.info(
                        f"  {sid}: {len(dupes)} duplicate variable(s) found"
                    )
                    all_dupes.extend(dupes)
                    all_comparisons.extend(comparisons)
                else:
                    log.info(f"  {sid}: clean")

                for var_name, unit in units.items():
                    unit_registry.setdefault(var_name, {}).setdefault(
                        unit, []
                    ).append(sid)
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

    # Unit audit results
    if unit_registry:
        inconsistent = {
            var: units_dict
            for var, units_dict in unit_registry.items()
            if len(units_dict) > 1
        }
        consistent_count = len(unit_registry) - len(inconsistent)

        print("  UNIT AUDIT:")
        print(f"  {'─' * 66}")
        print(f"  Variables checked: {len(unit_registry)}")
        print(f"  Consistent: {consistent_count}")
        print(f"  Inconsistent: {len(inconsistent)}")

        if inconsistent:
            print()
            print("  UNIT INCONSISTENCIES (variable has different units across scenarios):")
            for var in sorted(inconsistent):
                units_dict = inconsistent[var]
                parts = []
                for unit, sids in sorted(units_dict.items()):
                    if len(sids) <= 5:
                        parts.append(f"{unit} ({', '.join(sorted(sids))})")
                    else:
                        parts.append(f"{unit} ({len(sids)} scenarios)")
                print(f"    {var}: {' vs '.join(parts)}")
        else:
            print("  All ETL-relevant variables have consistent units across scenarios.")
        print()

    print(f"{'=' * 70}\n")

    # Write unit audit CSV
    if unit_registry:
        unit_audit_path = args.output.replace(".csv", "_units.csv")
        Path(unit_audit_path).parent.mkdir(parents=True, exist_ok=True)
        with open(unit_audit_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["variable", "unit", "scenario_count", "scenarios"])
            for var in sorted(unit_registry):
                for unit, sids in sorted(unit_registry[var].items()):
                    writer.writerow([
                        var, unit, len(sids),
                        ";".join(sorted(sids)),
                    ])
        log.info(f"Unit audit written to {unit_audit_path}")

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

        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
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
