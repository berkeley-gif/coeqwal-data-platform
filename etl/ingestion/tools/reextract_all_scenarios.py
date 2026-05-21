#!/usr/bin/env python3
"""
Re-trigger DSS extraction for all (or specific) scenarios.

Submits AWS Batch jobs that re-run dss_to_csv.py on the existing ZIP files
already stored at scenario/{id}/run/ in S3. The new CSVs overwrite the old
ones at scenario/{id}/csv/.

Usage:
    # Dry run.list what would be submitted
    python etl/ingestion/tools/reextract_all_scenarios.py --dry-run

    # Re-extract all scenarios
    python etl/ingestion/tools/reextract_all_scenarios.py

    # Re-extract specific scenarios
    python etl/ingestion/tools/reextract_all_scenarios.py --scenarios s0020,s0028

    # Re-extract only the SV input (skip CalSim output)
    python etl/ingestion/tools/reextract_all_scenarios.py --sv-only

    # Re-extract only the CalSim (DV) output (skip SV input)
    python etl/ingestion/tools/reextract_all_scenarios.py --dv-only

    # Include validation against reference CSVs in scenario/{id}/verify/
    python etl/ingestion/tools/reextract_all_scenarios.py --validate
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from etl.common import (  # noqa: E402
    AWS_REGION as REGION,
    BATCH_JOB_DEFINITION as JOB_DEFINITION,
    BATCH_QUEUE as JOB_QUEUE,
    S3_BUCKET,
)


def find_scenario_zips(s3, bucket: str, scenario_ids: list[str] | None = None):
    """Find ZIP files in scenario/{id}/run/ for each scenario."""
    results = []

    if scenario_ids:
        prefixes = [f"scenario/{sid}/run/" for sid in scenario_ids]
    else:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="scenario/", Delimiter="/")
        prefixes = []
        for cp in resp.get("CommonPrefixes", []):
            prefix = cp["Prefix"]
            sid = prefix.split("/")[1]
            prefixes.append(f"scenario/{sid}/run/")

    for prefix in prefixes:
        sid = prefix.split("/")[1]
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".zip"):
                    results.append({"scenario_id": sid, "zip_key": key})
                    break  # one ZIP per scenario

    results.sort(key=lambda x: x["scenario_id"])
    return results


def find_validation_csv(s3, bucket: str, scenario_id: str) -> str:
    """Find a reference CSV in scenario/{id}/verify/."""
    prefix = f"scenario/{scenario_id}/verify/"
    paginator = s3.get_paginator("list_objects_v2")
    candidates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".csv"):
                candidates.append(obj)

    if not candidates:
        return ""
    candidates.sort(key=lambda x: x.get("LastModified", 0), reverse=True)
    return candidates[0]["Key"]


def submit_job(batch_client, scenario_id: str, zip_key: str,
               validation_csv_key: str = "",
               extract_targets: str = "sv,calsim",
               memory_mb: int | None = None, vcpus: int | None = None):
    """Submit an AWS Batch job matching the Lambda's format."""
    job_name = f"reextract-{scenario_id}-{int(time.time())}"

    environment = [
        {"name": "SCENARIO_ID", "value": scenario_id},
        {"name": "ZIP_BUCKET", "value": S3_BUCKET},
        {"name": "ZIP_KEY", "value": zip_key},
        {"name": "VALIDATION_REF_CSV_KEY", "value": validation_csv_key},
        {"name": "EXTRACT_TARGETS", "value": extract_targets},
        {"name": "ABS_TOL", "value": "1e-6"},
        {"name": "REL_TOL", "value": "1e-6"},
    ]

    container_override = {
        "name": "main",
        "environment": environment,
    }
    if memory_mb or vcpus:
        reqs = []
        if memory_mb:
            reqs.append({"type": "MEMORY", "value": str(memory_mb)})
        if vcpus:
            reqs.append({"type": "VCPU", "value": str(vcpus)})
        container_override["resourceRequirements"] = reqs

    resp = batch_client.submit_job(
        jobName=job_name,
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        ecsPropertiesOverride={
            "taskProperties": [
                {
                    "containers": [container_override]
                }
            ]
        },
    )
    return resp["jobId"]


def main():
    parser = argparse.ArgumentParser(
        description="Re-trigger DSS extraction for scenarios"
    )
    parser.add_argument(
        "--scenarios", "-s",
        help="Comma-separated scenario IDs (default: all found in S3)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List jobs that would be submitted without submitting"
    )
    parser.add_argument(
        "--validate", action="store_true",
        help="Include validation against reference CSVs in scenario/{id}/verify/"
    )
    targets_group = parser.add_mutually_exclusive_group()
    targets_group.add_argument(
        "--sv-only", action="store_true",
        help="Re-extract only the SV input (skip CalSim output)"
    )
    targets_group.add_argument(
        "--dv-only", action="store_true",
        help="Re-extract only the CalSim (DV) output (skip SV input)"
    )
    parser.add_argument(
        "--memory", type=int, default=None,
        help="Override memory allocation in MB (default: use job definition, currently 8192)"
    )
    parser.add_argument(
        "--vcpus", type=int, default=None,
        help="Override vCPU allocation (default: use job definition, currently 2)"
    )
    parser.add_argument(
        "--bucket", default=S3_BUCKET,
        help=f"S3 bucket (default: {S3_BUCKET})"
    )
    args = parser.parse_args()

    scenario_ids = None
    if args.scenarios:
        scenario_ids = [s.strip() for s in args.scenarios.split(",")]

    s3 = boto3.client("s3", region_name=REGION)
    batch_client = boto3.client("batch", region_name=REGION)

    print(f"Scanning s3://{args.bucket}/ for scenario ZIPs...")
    zips = find_scenario_zips(s3, args.bucket, scenario_ids)

    if not zips:
        print("No ZIP files found. Nothing to do.")
        return

    print(f"Found {len(zips)} scenario(s):\n")

    jobs = []
    for entry in zips:
        sid = entry["scenario_id"]
        zip_key = entry["zip_key"]
        val_key = ""
        if args.validate:
            val_key = find_validation_csv(s3, args.bucket, sid)

        jobs.append({
            "scenario_id": sid,
            "zip_key": zip_key,
            "validation_csv_key": val_key,
        })

        val_status = f"  validation: {val_key}" if val_key else "  validation: none"
        print(f"  {sid}: {zip_key}")
        print(val_status)

    print()

    if args.dry_run:
        print(f"DRY RUN: Would submit {len(jobs)} batch job(s). Exiting.")
        return

    confirm = input(f"Submit {len(jobs)} batch job(s)? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    extract_targets = (
        "sv" if args.sv_only
        else "calsim" if args.dv_only
        else "sv,calsim"
    )

    print()
    for job in jobs:
        job_id = submit_job(
            batch_client,
            job["scenario_id"],
            job["zip_key"],
            job["validation_csv_key"],
            extract_targets=extract_targets,
            memory_mb=args.memory,
            vcpus=args.vcpus,
        )
        print(f"  {job['scenario_id']}: submitted job {job_id}")

    print(f"\nDone. {len(jobs)} job(s) submitted to queue '{JOB_QUEUE}'.")
    print("Monitor in AWS Batch console or with:")
    print(f"  aws batch list-jobs --job-queue {JOB_QUEUE} --job-status RUNNABLE")


if __name__ == "__main__":
    main()
