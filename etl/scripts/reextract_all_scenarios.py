#!/usr/bin/env python3
"""
Re-trigger DSS extraction for all (or specific) scenarios.

Submits AWS Batch jobs that re-run dss_to_csv.py on the existing ZIP files
already stored at scenario/{id}/run/ in S3. The new CSVs overwrite the old
ones at scenario/{id}/csv/.

Usage:
    # Dry run — list what would be submitted
    python reextract_all_scenarios.py --dry-run

    # Re-extract all scenarios
    python reextract_all_scenarios.py

    # Re-extract specific scenarios
    python reextract_all_scenarios.py --scenarios s0020,s0028

    # Re-extract only the SV input (skip CalSim output)
    python reextract_all_scenarios.py --sv-only

    # Include validation against reference CSVs in scenario/{id}/verify/
    python reextract_all_scenarios.py --validate
"""

import argparse
import time
import boto3

S3_BUCKET = "coeqwal-model-run"
JOB_QUEUE = "coeqwal-dss-queue"
JOB_DEFINITION = "coeqwal-dss-jobdef"
REGION = "us-west-2"


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
               validation_csv_key: str = "", sv_only: bool = False):
    """Submit an AWS Batch job matching the Lambda's format."""
    job_name = f"reextract-{scenario_id}-{int(time.time())}"

    environment = [
        {"name": "SCENARIO_ID", "value": scenario_id},
        {"name": "ZIP_BUCKET", "value": S3_BUCKET},
        {"name": "ZIP_KEY", "value": zip_key},
        {"name": "VALIDATION_REF_CSV_KEY", "value": validation_csv_key},
        {"name": "ABS_TOL", "value": "1e-6"},
        {"name": "REL_TOL", "value": "1e-6"},
    ]

    resp = batch_client.submit_job(
        jobName=job_name,
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        ecsPropertiesOverride={
            "taskProperties": [
                {
                    "containers": [
                        {
                            "name": "main",
                            "environment": environment,
                        }
                    ]
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
    parser.add_argument(
        "--sv-only", action="store_true",
        help="(Not yet implemented) Re-extract only SV input files"
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

    print()
    for job in jobs:
        job_id = submit_job(
            batch_client,
            job["scenario_id"],
            job["zip_key"],
            job["validation_csv_key"],
        )
        print(f"  {job['scenario_id']}: submitted job {job_id}")

    print(f"\nDone. {len(jobs)} job(s) submitted to queue '{JOB_QUEUE}'.")
    print("Monitor in AWS Batch console or with:")
    print(f"  aws batch list-jobs --job-queue {JOB_QUEUE} --job-status RUNNABLE")


if __name__ == "__main__":
    main()
