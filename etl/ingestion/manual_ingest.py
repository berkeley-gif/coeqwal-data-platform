#!/usr/bin/env python3
"""
manual_ingest.py - operator helper for manual ingestion paths.

Pure drag-and-drop in the AWS console is the simplest manual path and does
not require this script. Reach for `manual_ingest.py` when one of these is
true: the ZIP is ambiguous and you want to pin DV/SV explicitly, you want
SHA-256 computed locally before upload, or you need to recover a scenario
whose ZIP is already in S3 without a sidecar.

Two subcommands:

  sidecar  - Build a sidecar.json from operator-supplied basenames (and
             optionally compute SHA-256 hashes by streaming an existing ZIP
             in S3), PUT it at scenario/<id>/run/sidecar.json, and
             optionally resubmit the Batch job. Use this to fix a NO_SIDECAR
             error without having to re-upload the ZIP.

  upload   - Upload a ZIP (+ optional trend CSV + sidecar) to S3 in the safe
             order: sidecar -> trend -> ZIP. The ZIP PUT is the Lambda
             trigger, so everything else lands first.

Examples:

  # Fix a missing sidecar for a scenario whose ZIP already exists in S3
  python etl/ingestion/manual_ingest.py sidecar \\
      --short-code s0030 \\
      --dv-basename s0030_dcradjhist_2020lu_noflowreqt_dv_20260126v02.dss \\
      --sv-basename coeqwal_s9999_sv_v0.1.4.dss \\
      --zip-basename s0030_dcradjhist_2020lu_noflowreqt.zip \\
      --compute-hashes \\
      --retrigger-batch

  # Manual full upload (operator already has a local ZIP)
  python etl/ingestion/manual_ingest.py upload \\
      --short-code s0042 \\
      --zip-path ./s0042.zip \\
      --trend-csv-path ./s0042_trend.csv \\
      --dv-basename s0042_dv.dss \\
      --sv-basename coeqwal_s9999_sv_v0.1.4.dss

"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import socket
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import boto3

# Share constants with gdrive_bulk_download.py.
INGEST_DIR = Path(__file__).parent
sys.path.insert(0, str(INGEST_DIR))

from gdrive_bulk_download import (  # noqa: E402
    AWS_REGION,
    DEFAULT_S3_BUCKET,
    JOB_DEFINITION,
    JOB_QUEUE,
    READY_PREFIX,
    SCENARIO_RUN_PREFIX,
    SIDECAR_SCHEMA_VERSION,
    SCRIPT_VERSION,
    STAGING_PREFIX,
)

log = logging.getLogger("manual_ingest")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _operator_tag() -> str:
    user = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
    try:
        host = socket.gethostname()
    except OSError:
        host = "unknown"
    return f"{user}@{host}"


def _sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_of_local_zip_entry(zip_path: str, basename: str) -> Tuple[str, int, str]:
    """Find a basename inside a local ZIP and return (sha, size, path_in_zip)."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        candidates = [n for n in zf.namelist()
                      if os.path.basename(n).lower() == basename.lower()]
        if not candidates:
            raise SystemExit(
                f"\n'{basename}' not found in ZIP {zip_path}.\n"
                f"Available DSS basenames: "
                f"{sorted({os.path.basename(n) for n in zf.namelist() if n.lower().endswith('.dss')})}\n"
            )
        if len(candidates) > 1:
            raise SystemExit(
                f"\n'{basename}' matched {len(candidates)} paths in ZIP {zip_path}: {candidates}\n"
                f"Refusing to guess.\n"
            )
        path_in_zip = candidates[0]
        h = hashlib.sha256()
        size = 0
        with zf.open(path_in_zip) as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
                size += len(chunk)
        return h.hexdigest(), size, path_in_zip


def _stream_sha_from_s3_zip(
    s3, bucket: str, zip_key: str, basename: str,
) -> Tuple[str, int, str, str, int]:
    """Stream a ZIP from S3 to a temp file and hash a single entry.

    Returns (entry_sha, entry_size, path_in_zip, zip_sha, zip_size).
    """
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="manual_ingest_")
    local = Path(tmpdir) / Path(zip_key).name
    log.info("Downloading s3://%s/%s -> %s for hashing ...", bucket, zip_key, local)
    s3.download_file(bucket, zip_key, str(local))
    try:
        zip_sha = _sha256_of_file(str(local))
        zip_size = local.stat().st_size
        entry_sha, entry_size, path_in_zip = _sha256_of_local_zip_entry(str(local), basename)
        return entry_sha, entry_size, path_in_zip, zip_sha, zip_size
    finally:
        try:
            local.unlink()
        except OSError:
            pass
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass


def _find_zip_in_run(s3, bucket: str, short_code: str) -> Optional[str]:
    """Find the ZIP key in scenario/<id>/run/, or None."""
    prefix = f"{SCENARIO_RUN_PREFIX}/{short_code}/run/"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            if obj["Key"].lower().endswith(".zip"):
                return obj["Key"]
    return None


def _find_trend_in_run(s3, bucket: str, short_code: str) -> Optional[str]:
    """Find a single CSV (excluding sidecar.json) in scenario/<id>/run/."""
    prefix = f"{SCENARIO_RUN_PREFIX}/{short_code}/run/"
    paginator = s3.get_paginator("list_objects_v2")
    csvs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                csvs.append(key)
    if len(csvs) == 1:
        return csvs[0]
    return None


def _build_sidecar(
    short_code: str,
    ingestion_path: str,
    expected_dv_filename: str,
    expected_sv_filename: str,
    expected_dv_path_in_zip: str,
    expected_sv_path_in_zip: str,
    dv_sha256: Optional[str],
    sv_sha256: Optional[str],
    dv_filesize_bytes: Optional[int],
    sv_filesize_bytes: Optional[int],
    zip_basename: str,
    zip_sha256: Optional[str],
    zip_filesize_bytes: Optional[int],
    trend_csv_basename: Optional[str],
    trend_csv_sha256: Optional[str],
    spreadsheet_url: str,
    spreadsheet_row_sha256: str,
    spreadsheet_file: str,
) -> Dict[str, Any]:
    return {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "short_code": short_code,
        "expected_sv_filename": expected_sv_filename,
        "expected_dv_filename": expected_dv_filename,
        "expected_sv_path_in_zip": expected_sv_path_in_zip,
        "expected_dv_path_in_zip": expected_dv_path_in_zip,
        "sv_sha256": sv_sha256,
        "dv_sha256": dv_sha256,
        "sv_filesize_bytes": sv_filesize_bytes,
        "dv_filesize_bytes": dv_filesize_bytes,
        "zip_basename": zip_basename,
        "zip_sha256": zip_sha256,
        "zip_filesize_bytes": zip_filesize_bytes,
        "trend_csv_basename": trend_csv_basename,
        "trend_csv_sha256": trend_csv_sha256,
        "convention_check": {
            "short_code_in_dv_basename": short_code.lower() in (expected_dv_filename or "").lower(),
            "short_code_in_sv_basename": short_code.lower() in (expected_sv_filename or "").lower(),
        },
        "source": {
            "spreadsheet_url": spreadsheet_url,
            "spreadsheet_row_sha256": spreadsheet_row_sha256,
            "spreadsheet_file": spreadsheet_file,
        },
        "ingestion": {
            "path": ingestion_path,
            "script": "manual_ingest.py",
            "script_version": SCRIPT_VERSION,
            "operator": _operator_tag(),
            "ingested_at_utc": _now_iso_utc(),
        },
    }


# ---------------------------------------------------------------------------
# Batch submission
# ---------------------------------------------------------------------------
def _submit_batch(
    batch_client, short_code: str, zip_bucket: str, zip_key: str,
    sidecar: Optional[Dict[str, Any]] = None,
) -> str:
    """Submit a Batch job for this scenario. Returns the jobId."""
    job_name = f"manual-{short_code}-{int(time.time())}"
    env = [
        {"name": "SCENARIO_ID", "value": short_code},
        {"name": "ZIP_BUCKET", "value": zip_bucket},
        {"name": "ZIP_KEY", "value": zip_key},
        {"name": "ABS_TOL", "value": "1e-6"},
        {"name": "REL_TOL", "value": "1e-6"},
    ]
    if sidecar:
        env.extend([
            {"name": "EXPECTED_DV_FILENAME",
             "value": sidecar.get("expected_dv_filename", "")},
            {"name": "EXPECTED_SV_FILENAME",
             "value": sidecar.get("expected_sv_filename", "")},
            {"name": "EXPECTED_DV_SHA256",
             "value": sidecar.get("dv_sha256") or ""},
            {"name": "EXPECTED_SV_SHA256",
             "value": sidecar.get("sv_sha256") or ""},
        ])

    resp = batch_client.submit_job(
        jobName=job_name,
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        ecsPropertiesOverride={
            "taskProperties": [{"containers": [{"name": "main", "environment": env}]}]
        },
    )
    return resp["jobId"]


# ---------------------------------------------------------------------------
# sidecar subcommand
# ---------------------------------------------------------------------------
def cmd_sidecar(args):
    s3 = boto3.client("s3", region_name=AWS_REGION)

    short_code = args.short_code
    dv_basename = args.dv_basename
    sv_basename = args.sv_basename

    if not dv_basename or not sv_basename:
        raise SystemExit("--dv-basename and --sv-basename are required")

    # Resolve the ZIP location and basename
    zip_key = args.zip_key or _find_zip_in_run(s3, args.bucket, short_code)
    if not zip_key:
        raise SystemExit(
            f"\nNo ZIP found at s3://{args.bucket}/{SCENARIO_RUN_PREFIX}/{short_code}/run/.\n"
            f"Either upload one first (manual_ingest.py upload), or pass --zip-key explicitly.\n"
        )
    zip_basename = args.zip_basename or Path(zip_key).name

    # Optionally compute hashes
    dv_sha = args.dv_sha256
    sv_sha = args.sv_sha256
    zip_sha = args.zip_sha256
    dv_size = None
    sv_size = None
    zip_size = None
    dv_path_in_zip = ""
    sv_path_in_zip = ""

    if args.compute_hashes:
        log.info("Computing hashes by streaming s3://%s/%s ...", args.bucket, zip_key)
        dv_sha, dv_size, dv_path_in_zip, zip_sha, zip_size = \
            _stream_sha_from_s3_zip(s3, args.bucket, zip_key, dv_basename)
        sv_sha, sv_size, sv_path_in_zip, _, _ = \
            _stream_sha_from_s3_zip(s3, args.bucket, zip_key, sv_basename)

    # Trend CSV
    trend_basename = args.trend_csv_basename
    trend_sha = args.trend_csv_sha256
    if not trend_basename:
        existing_trend = _find_trend_in_run(s3, args.bucket, short_code)
        if existing_trend:
            trend_basename = Path(existing_trend).name
            if args.compute_hashes:
                obj = s3.get_object(Bucket=args.bucket, Key=existing_trend)
                trend_sha = _sha256_of_bytes(obj["Body"].read())

    sidecar = _build_sidecar(
        short_code=short_code,
        ingestion_path=args.ingestion_path,
        expected_dv_filename=dv_basename,
        expected_sv_filename=sv_basename,
        expected_dv_path_in_zip=dv_path_in_zip,
        expected_sv_path_in_zip=sv_path_in_zip,
        dv_sha256=dv_sha, sv_sha256=sv_sha,
        dv_filesize_bytes=dv_size, sv_filesize_bytes=sv_size,
        zip_basename=zip_basename, zip_sha256=zip_sha, zip_filesize_bytes=zip_size,
        trend_csv_basename=trend_basename, trend_csv_sha256=trend_sha,
        spreadsheet_url=args.spreadsheet_url,
        spreadsheet_row_sha256=args.spreadsheet_row_sha256,
        spreadsheet_file=args.spreadsheet_file,
    )
    payload = json.dumps(sidecar, indent=2, sort_keys=True).encode("utf-8")

    sidecar_key = f"{SCENARIO_RUN_PREFIX}/{short_code}/run/sidecar.json"

    if args.dry_run:
        print(payload.decode("utf-8"))
        print(f"\nDRY RUN: would PUT to s3://{args.bucket}/{sidecar_key}")
        if args.retrigger_batch:
            print(f"DRY RUN: would submit Batch job for {short_code} from {zip_key}")
        return

    log.info("PUT s3://%s/%s ...", args.bucket, sidecar_key)
    s3.put_object(
        Bucket=args.bucket, Key=sidecar_key,
        Body=payload, ContentType="application/json",
    )
    print(f"Wrote sidecar to s3://{args.bucket}/{sidecar_key}")

    if args.retrigger_batch:
        batch = boto3.client("batch", region_name=AWS_REGION)
        job_id = _submit_batch(batch, short_code, args.bucket, zip_key, sidecar=sidecar)
        print(f"Submitted Batch job: {job_id}")
        print(f"Monitor: aws batch describe-jobs --jobs {job_id}")


# ---------------------------------------------------------------------------
# upload subcommand
# ---------------------------------------------------------------------------
def cmd_upload(args):
    """Upload ZIP (+ optional trend CSV + sidecar) to staging in the safe order.

    Order is enforced: sidecar.json -> trend CSV -> ZIP last. The ZIP PUT
    is the Lambda trigger; everything else must already be at rest when
    the trigger fires.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    short_code = args.short_code

    zip_path = args.zip_path
    trend_path = args.trend_csv_path
    dv_basename = args.dv_basename
    sv_basename = args.sv_basename

    if not zip_path or not os.path.exists(zip_path):
        raise SystemExit(f"--zip-path is required and must exist (got: {zip_path})")
    if not dv_basename or not sv_basename:
        raise SystemExit("--dv-basename and --sv-basename are required")

    zip_basename = Path(zip_path).name
    log.info("Hashing ZIP %s ...", zip_path)
    zip_sha = _sha256_of_file(zip_path)
    zip_size = os.path.getsize(zip_path)
    dv_sha, dv_size, dv_path_in_zip = _sha256_of_local_zip_entry(zip_path, dv_basename)
    sv_sha, sv_size, sv_path_in_zip = _sha256_of_local_zip_entry(zip_path, sv_basename)

    trend_basename = None
    trend_sha = None
    trend_bytes = None
    if trend_path:
        if not os.path.exists(trend_path):
            raise SystemExit(f"--trend-csv-path does not exist: {trend_path}")
        trend_basename = Path(trend_path).name
        trend_bytes = Path(trend_path).read_bytes()
        trend_sha = _sha256_of_bytes(trend_bytes)

    sidecar = _build_sidecar(
        short_code=short_code,
        ingestion_path="manual",
        expected_dv_filename=dv_basename,
        expected_sv_filename=sv_basename,
        expected_dv_path_in_zip=dv_path_in_zip,
        expected_sv_path_in_zip=sv_path_in_zip,
        dv_sha256=dv_sha, sv_sha256=sv_sha,
        dv_filesize_bytes=dv_size, sv_filesize_bytes=sv_size,
        zip_basename=zip_basename, zip_sha256=zip_sha, zip_filesize_bytes=zip_size,
        trend_csv_basename=trend_basename, trend_csv_sha256=trend_sha,
        spreadsheet_url=args.spreadsheet_url,
        spreadsheet_row_sha256=args.spreadsheet_row_sha256,
        spreadsheet_file=args.spreadsheet_file,
    )

    # Destination prefix
    dest_prefix = args.dest_prefix.rstrip("/") + f"/{short_code}/"
    sidecar_key = f"{dest_prefix}sidecar.json"
    trend_key = f"{dest_prefix}{trend_basename}" if trend_basename else None
    zip_key = f"{dest_prefix}{zip_basename}"

    if args.dry_run:
        print("DRY RUN: upload order will be:")
        print(f"  1. s3://{args.bucket}/{sidecar_key}")
        if trend_key:
            print(f"  2. s3://{args.bucket}/{trend_key}")
        print(f"  3. s3://{args.bucket}/{zip_key}  (Lambda trigger)")
        return

    log.info("PUT sidecar.json (1/%d) ...", 3 if trend_key else 2)
    s3.put_object(Bucket=args.bucket, Key=sidecar_key,
                  Body=json.dumps(sidecar, indent=2, sort_keys=True).encode("utf-8"),
                  ContentType="application/json")
    if trend_key:
        log.info("PUT trend CSV (2/3) ...")
        s3.put_object(Bucket=args.bucket, Key=trend_key,
                      Body=trend_bytes, ContentType="text/csv")
    log.info("PUT ZIP (last; this is the Lambda trigger) ...")
    s3.upload_file(zip_path, args.bucket, zip_key)

    print(f"\nUploaded {short_code} to s3://{args.bucket}/{dest_prefix}.")
    print("Uploaded in order:")
    print(f"  1. {sidecar_key}")
    if trend_key:
        print(f"  2. {trend_key}")
    print(f"  3. {zip_key}  (Lambda trigger)")
    print()
    print(
        f"If you uploaded to {STAGING_PREFIX}/, run "
        f"`python etl/ingestion/gdrive_bulk_download.py promote` to move "
        f"to {READY_PREFIX}/. If you uploaded to {READY_PREFIX}/, the "
        f"Lambda should have fired on the ZIP PUT."
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    parser = argparse.ArgumentParser(
        description="Operator helper for manual ingestion paths."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sd = sub.add_parser(
        "sidecar",
        help="Build and PUT sidecar.json to scenario/<id>/run/; optionally retrigger Batch.",
    )
    sd.add_argument("--short-code", required=True)
    sd.add_argument("--dv-basename", required=True)
    sd.add_argument("--sv-basename", required=True)
    sd.add_argument("--zip-basename", default="",
                    help="Override the ZIP basename in the sidecar. Default: derived from --zip-key or S3 lookup.")
    sd.add_argument("--zip-key", default="",
                    help="Override S3 key for the ZIP (default: discover in scenario/<id>/run/).")
    sd.add_argument("--zip-sha256", default=None)
    sd.add_argument("--dv-sha256", default=None)
    sd.add_argument("--sv-sha256", default=None)
    sd.add_argument("--trend-csv-basename", default=None)
    sd.add_argument("--trend-csv-sha256", default=None)
    sd.add_argument("--compute-hashes", action="store_true",
                    help="Stream the ZIP from S3 and compute SHA-256 hashes for DV, SV, and ZIP.")
    sd.add_argument("--retrigger-batch", action="store_true",
                    help="After PUTting sidecar, submit a Batch job directly (bypasses the Lambda).")
    sd.add_argument("--ingestion-path", default="manual",
                    choices=["manual", "automated", "backfill"],
                    help="Ingestion path label in the sidecar (default: manual).")
    sd.add_argument("--spreadsheet-url", default="",
                    help="Sidecar provenance: scenario listing spreadsheet URL.")
    sd.add_argument("--spreadsheet-row-sha256", default="",
                    help="Sidecar provenance: hash of the spreadsheet row.")
    sd.add_argument("--spreadsheet-file", default="",
                    help="Sidecar provenance: local CSV file name.")
    sd.add_argument("--bucket", default=DEFAULT_S3_BUCKET)
    sd.add_argument("--dry-run", action="store_true",
                    help="Print sidecar and skip the PUT and Batch submission.")

    up = sub.add_parser(
        "upload",
        help="Upload ZIP (+ optional trend CSV + sidecar) in safe order: sidecar -> trend -> ZIP last.",
    )
    up.add_argument("--short-code", required=True)
    up.add_argument("--zip-path", required=True, help="Local path to the ZIP file.")
    up.add_argument("--dv-basename", required=True)
    up.add_argument("--sv-basename", required=True)
    up.add_argument("--trend-csv-path", default=None,
                    help="Optional local path to the trend report CSV.")
    up.add_argument("--dest-prefix", default=STAGING_PREFIX,
                    choices=[STAGING_PREFIX, READY_PREFIX],
                    help=f"S3 prefix to upload into (default: {STAGING_PREFIX}). "
                         f"Use '{READY_PREFIX}' to bypass promote and trigger Lambda immediately.")
    up.add_argument("--spreadsheet-url", default="")
    up.add_argument("--spreadsheet-row-sha256", default="")
    up.add_argument("--spreadsheet-file", default="")
    up.add_argument("--bucket", default=DEFAULT_S3_BUCKET)
    up.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    if args.command == "sidecar":
        cmd_sidecar(args)
    elif args.command == "upload":
        cmd_upload(args)


if __name__ == "__main__":
    main()
