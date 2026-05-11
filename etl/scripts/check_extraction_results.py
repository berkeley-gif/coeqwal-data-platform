#!/usr/bin/env python3
"""
Post-extraction audit: read manifests and validation summaries from S3
and produce a single report (console table + CSV).

Auto-discovers scenarios from s3://<bucket>/scenario/<id>/ prefixes, or
accepts an explicit list via --scenarios.

Usage:
    # Check all scenarios
    python check_extraction_results.py --bucket coeqwal-model-run

    # Check specific scenarios
    python check_extraction_results.py --bucket coeqwal-model-run --scenarios s0021,s0022

    # Include cross-scenario mismatch pattern analysis
    python check_extraction_results.py --bucket coeqwal-model-run --mismatches

    # Write audit CSV to a custom path (default: etl/scripts/output/extraction_audit.csv)
    python check_extraction_results.py --bucket coeqwal-model-run -o /tmp/foo.csv
"""

import argparse
import csv
import io
import json
import sys
from pathlib import Path
from typing import Optional

import boto3

S3_BUCKET = "coeqwal-model-run"
REGION = "us-west-2"

# Default output directory for audit CSVs. Gitignored via etl/**/output/.
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "output"

AUDIT_COLUMNS = [
    "scenario_id",
    "extraction_status",
    "sv_detected",
    "sv_csv_written",
    "calsim_detected",
    "calsim_csv_written",
    "validation_result",
    "validation_target",
    "mismatch_columns",
    "mismatch_cells",
    "processed_at",
    "job_id",
    "issue",
]


def discover_scenarios(s3, bucket: str) -> list[str]:
    """List all scenario IDs that have a folder under scenario/."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix="scenario/", Delimiter="/")
    ids = []
    for cp in resp.get("CommonPrefixes", []):
        sid = cp["Prefix"].split("/")[1]
        if sid:
            ids.append(sid)
    ids.sort()
    return ids


def read_json_from_s3(s3, bucket: str, key: str) -> Optional[dict]:
    """Download and parse a JSON object from S3. Returns None on failure."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception:
        return None


def read_csv_from_s3(s3, bucket: str, key: str) -> Optional[str]:
    """Download CSV content as a string from S3. Returns None on failure."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read().decode("utf-8")
    except Exception:
        return None


def gather_results(s3, bucket: str, scenario_ids: list[str]) -> list[dict]:
    """Read manifest + validation summary for each scenario."""
    rows = []
    for sid in scenario_ids:
        manifest_key = f"scenario/{sid}/{sid}_manifest.json"
        manifest = read_json_from_s3(s3, bucket, manifest_key)

        if manifest is None:
            rows.append({
                "scenario_id": sid,
                "extraction_status": "NO_MANIFEST",
                "sv_detected": "",
                "sv_csv_written": "",
                "calsim_detected": "",
                "calsim_csv_written": "",
                "validation_result": "",
                "validation_target": "",
                "mismatch_columns": "",
                "mismatch_cells": "",
                "processed_at": "",
                "job_id": "",
                "issue": "Manifest not found -- extraction may not have run",
            })
            continue

        status_summary = manifest.get("status_summary", {})
        validation = manifest.get("validation", {})

        val_result = validation.get("result", "")
        val_target = validation.get("target", "")

        # Read the detailed validation summary for mismatch counts
        mismatch_cols = ""
        mismatch_cells = ""
        val_summary_key = (
            validation.get("detailed_reports", {}).get("summary_json_key", "")
        )
        if val_summary_key:
            val_summary = read_json_from_s3(s3, bucket, val_summary_key)
            if val_summary:
                mismatch_cols = val_summary.get("mismatch_columns", "")
                mismatch_cells = val_summary.get("mismatch_cells", "")

        issues = []
        ext_status = manifest.get("status", "UNKNOWN")
        if ext_status == "SUCCEEDED_PARTIAL":
            if not status_summary.get("sv_csv_written"):
                issues.append("SV CSV missing")
            if not status_summary.get("calsim_csv_written"):
                issues.append("CalSim CSV missing")
        elif ext_status not in ("SUCCEEDED", "SUCCEEDED_PARTIAL"):
            issues.append(f"Extraction status: {ext_status}")

        if val_result == "failed":
            issues.append(
                f"Validation failed ({mismatch_cells} mismatched cells)"
            )
        elif val_result == "skipped":
            issues.append("Validation skipped (no trend report)")
        elif val_result in ("skipped_no_targets", "download_failed",
                            "skipped_no_script"):
            issues.append(f"Validation issue: {val_result}")

        rows.append({
            "scenario_id": sid,
            "extraction_status": ext_status,
            "sv_detected": str(status_summary.get("sv_detected", "")),
            "sv_csv_written": str(status_summary.get("sv_csv_written", "")),
            "calsim_detected": str(status_summary.get("calsim_detected", "")),
            "calsim_csv_written": str(
                status_summary.get("calsim_csv_written", "")
            ),
            "validation_result": val_result,
            "validation_target": val_target,
            "mismatch_columns": str(mismatch_cols),
            "mismatch_cells": str(mismatch_cells),
            "processed_at": manifest.get("processed_at", ""),
            "job_id": manifest.get("job_id", ""),
            "issue": "; ".join(issues) if issues else "",
        })

    return rows


def gather_mismatches(s3, bucket: str, scenario_ids: list[str]) -> list[dict]:
    """Download mismatch CSVs and combine into a single list of dicts."""
    all_rows: list[dict] = []
    for sid in scenario_ids:
        key = f"scenario/{sid}/validation/{sid}_validation_mismatches.csv"
        content = read_csv_from_s3(s3, bucket, key)
        if content is None:
            continue
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            row["scenario_id"] = sid
            all_rows.append(row)
    return all_rows


def print_summary(rows: list[dict]) -> None:
    """Print the formatted console report."""
    total = len(rows)
    succeeded = sum(1 for r in rows if r["extraction_status"] == "SUCCEEDED")
    partial = sum(
        1 for r in rows if r["extraction_status"] == "SUCCEEDED_PARTIAL"
    )
    no_manifest = sum(
        1 for r in rows if r["extraction_status"] == "NO_MANIFEST"
    )
    failed = total - succeeded - partial - no_manifest

    val_passed = sum(1 for r in rows if r["validation_result"] == "passed")
    val_failed = sum(1 for r in rows if r["validation_result"] == "failed")
    val_skipped = sum(
        1 for r in rows
        if r["validation_result"] and r["validation_result"] not in (
            "passed", "failed"
        )
    )

    sep = "=" * 100
    print(f"\n{sep}")
    print("  EXTRACTION & VALIDATION AUDIT")
    print(sep)
    print(f"  Total scenarios:       {total}")
    print(f"  Succeeded:             {succeeded}")
    if partial:
        print(f"  Succeeded (partial):   {partial}")
    if failed:
        print(f"  Failed:                {failed}")
    if no_manifest:
        print(f"  No manifest (pending): {no_manifest}")
    print()
    print(f"  Validation passed:     {val_passed}")
    if val_failed:
        print(f"  Validation failed:     {val_failed}")
    if val_skipped:
        print(f"  Validation skipped:    {val_skipped}")
    print()

    # Table header
    fmt = "  {:<10s} {:<18s} {:<10s} {:<10s} {:<12s} {:<40s}"
    print(fmt.format(
        "Scenario", "Extraction", "Validatn", "Mismatch", "Processed",
        "Issue",
    ))
    print("  " + "-" * 96)

    for r in rows:
        mismatch_str = ""
        if r["mismatch_cells"] and r["mismatch_cells"] != "0":
            mismatch_str = f"{r['mismatch_cells']} cells"

        processed = r["processed_at"][:10] if r["processed_at"] else "-"

        print(fmt.format(
            r["scenario_id"],
            r["extraction_status"],
            r["validation_result"] or "-",
            mismatch_str or "-",
            processed,
            (r["issue"] or "")[:40],
        ))

    # Attention section
    attention = [r for r in rows if r["issue"]]
    if attention:
        print()
        print("  SCENARIOS REQUIRING ATTENTION:")
        print("  " + "-" * 96)
        for r in attention:
            print(f"  {r['scenario_id']}: {r['issue']}")

    print(sep)


def print_mismatch_analysis(mismatch_rows: list[dict]) -> None:
    """Cross-scenario mismatch pattern analysis."""
    if not mismatch_rows:
        print("\n  No mismatches to analyze -- all validations passed or were skipped.")
        return

    sep = "=" * 100
    print(f"\n{sep}")
    print("  MISMATCH PATTERN ANALYSIS")
    print(sep)

    total = len(mismatch_rows)
    scenarios = set(r.get("scenario_id", "") for r in mismatch_rows)
    print(f"  Total mismatch rows:  {total}")
    print(f"  Scenarios affected:   {len(scenarios)}")
    print()

    # Top variables (C parts) with mismatches
    c_counts: dict[str, int] = {}
    b_counts: dict[str, int] = {}
    scenario_counts: dict[str, int] = {}
    diffs: list[float] = []

    for r in mismatch_rows:
        c_val = r.get("C", "")
        b_val = r.get("B", "")
        sid = r.get("scenario_id", "")
        c_counts[c_val] = c_counts.get(c_val, 0) + 1
        b_counts[b_val] = b_counts.get(b_val, 0) + 1
        scenario_counts[sid] = scenario_counts.get(sid, 0) + 1
        try:
            diffs.append(float(r.get("abs_diff", "nan")))
        except (ValueError, TypeError):
            pass

    print("  Top variables (C part) with mismatches:")
    for var, count in sorted(c_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {var}: {count}")

    print()
    print("  Top locations (B part) with mismatches:")
    for loc, count in sorted(b_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {loc}: {count}")

    print()
    print("  Mismatches per scenario:")
    for sid, count in sorted(scenario_counts.items()):
        print(f"    {sid}: {count}")

    valid_diffs = [d for d in diffs if d == d]  # filter NaN
    if valid_diffs:
        print()
        print("  Magnitude statistics:")
        print(f"    Mean absolute diff:   {sum(valid_diffs) / len(valid_diffs):.6f}")
        sorted_diffs = sorted(valid_diffs)
        median = sorted_diffs[len(sorted_diffs) // 2]
        print(f"    Median absolute diff: {median:.6f}")
        print(f"    Max absolute diff:    {max(valid_diffs):.6f}")

    print(sep)


def write_audit_csv(rows: list[dict], path: str) -> None:
    """Write the audit results to a CSV file."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=AUDIT_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in AUDIT_COLUMNS})
    print(f"  Audit CSV written to {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Check extraction and validation results across scenarios"
    )
    parser.add_argument(
        "--bucket", default=S3_BUCKET,
        help=f"S3 bucket (default: {S3_BUCKET})",
    )
    parser.add_argument(
        "--scenarios", "-s",
        help="Comma-separated scenario IDs (default: auto-discover from S3)",
    )
    parser.add_argument(
        "--mismatches", action="store_true",
        help="Download mismatch CSVs and show cross-scenario pattern analysis",
    )
    parser.add_argument(
        "--mismatch-output",
        help="Write combined mismatch CSV to this path (implies --mismatches)",
    )
    parser.add_argument(
        "-o", "--output",
        default=str(DEFAULT_OUTPUT_DIR / "extraction_audit.csv"),
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_DIR / 'extraction_audit.csv'}). "
             "Parent dir is auto-created.",
    )
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=REGION)

    if args.scenarios:
        scenario_ids = [s.strip() for s in args.scenarios.split(",")]
        print(f"Checking {len(scenario_ids)} specified scenario(s)...")
    else:
        print(f"Discovering scenarios in s3://{args.bucket}/scenario/ ...")
        scenario_ids = discover_scenarios(s3, args.bucket)
        print(f"Found {len(scenario_ids)} scenario(s).")

    if not scenario_ids:
        print("No scenarios found. Nothing to check.")
        return 0

    rows = gather_results(s3, args.bucket, scenario_ids)
    print_summary(rows)
    write_audit_csv(rows, args.output)

    do_mismatches = args.mismatches or args.mismatch_output
    if do_mismatches:
        failed_ids = [
            r["scenario_id"] for r in rows
            if r["validation_result"] == "failed"
        ]
        if failed_ids:
            print(
                f"\n  Downloading mismatch details for {len(failed_ids)}"
                " failed scenario(s)..."
            )
            mismatch_rows = gather_mismatches(s3, args.bucket, failed_ids)
            print_mismatch_analysis(mismatch_rows)

            if args.mismatch_output and mismatch_rows:
                Path(args.mismatch_output).parent.mkdir(parents=True, exist_ok=True)
                fieldnames = list(mismatch_rows[0].keys())
                with open(args.mismatch_output, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(mismatch_rows)
                print(f"  Mismatch CSV written to {args.mismatch_output}")
        else:
            print("\n  No failed validations -- skipping mismatch analysis.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
