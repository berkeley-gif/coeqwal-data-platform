#!/usr/bin/env python3
"""
backfill_ingest_records.py - one-time helper to write `ingest_record.json`
for scenarios already in S3.

Walks `etl/ingestion/scenario_listing/model_run_file_source_working.csv`
(the same file `gdrive_bulk_download.py` reads), takes its run scope from
`--scenarios` / `--all`, locates each scenario's ZIP in
`s3://<bucket>/scenario/<id>/run/`, computes SHA-256 for the selected DV
and SV entries plus the ZIP, and PUTs an `ingest_record.json` at
`scenario/<id>/ingest_record.json`. Existing records are left alone
unless `--overwrite` is set.

This is intended for the one-time backfill of the active scenarios that
were ingested before the ingest-record contract existed. After backfill,
every scenario in S3 has a record and the Pass 2b container can run
strictly.

Usage:
  # Dry-run plan
  python etl/ingestion/tools/backfill_ingest_records.py --dry-run

  # Backfill all ready scenarios
  python etl/ingestion/tools/backfill_ingest_records.py

  # Backfill a single scenario, overwriting any existing record
  python etl/ingestion/tools/backfill_ingest_records.py --scenarios s0030 --overwrite
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3

TOOLS_DIR = Path(__file__).parent
REPO_ROOT = TOOLS_DIR.parent.parent.parent

# Make `from etl.X import Y` work when this script is invoked as
# `python etl/ingestion/tools/backfill_ingest_records.py` from the repo root.
sys.path.insert(0, str(REPO_ROOT))
from etl.common import (  # noqa: E402
    DEFAULT_S3_BUCKET,
    ingest_record_key,
    scenario_prefix,
)
from etl.ingestion.lib.config import (  # noqa: E402, F401
    INGEST_RECORD_SCHEMA_VERSION,
    SCRIPT_VERSION,
    SPREADSHEET_URL,
    WORKING_CSV_PATH,
)
from etl.ingestion.lib.csv_reader import (  # noqa: E402
    _require_working_csv,
    read_scenario_source_csv,
)
from etl.ingestion.lib.utils import (  # noqa: E402, F401
    _now_iso_utc,
    _operator_tag,
    _sha256_of_bytes,
)

# Record builder and S3 ZIP hashing live in manual_ingest (the canonical
# manual-upload tool); reuse them here so backfill writes identical records.
from etl.ingestion.tools.manual_ingest import (  # noqa: E402
    _build_ingest_record,
    _find_trend_in_run,
    _find_zip_in_run,
    _stream_sha_from_s3_zip,
)

log = logging.getLogger("backfill_ingest_records")


def _record_key_for(short_code: str) -> str:
    return ingest_record_key(scenario_prefix(short_code))


def _has_record(s3, bucket: str, short_code: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=_record_key_for(short_code))
        return True
    except s3.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def _backfill_one(
    s3, bucket: str, scenario: Dict[str, Any], overwrite: bool, dry_run: bool,
) -> Dict[str, Any]:
    """Build + PUT one ingest record. Returns a result record."""
    sc = scenario["short_code"]
    result: Dict[str, Any] = {
        "short_code": sc,
        "action": "",
        "ingest_record_key": "",
        "zip_key": "",
        "trend_csv_key": "",
        "error": "",
    }

    zip_key = _find_zip_in_run(s3, bucket, sc)
    if not zip_key:
        result["action"] = "skip"
        result["error"] = "no ZIP at scenario/<id>/run/"
        return result
    result["zip_key"] = zip_key

    if not overwrite and _has_record(s3, bucket, sc):
        result["action"] = "skip"
        result["error"] = "ingest record already exists (use --overwrite to replace)"
        return result

    dv_basename = scenario["dv_filename"]
    sv_basename = scenario["sv_filename"]
    if not dv_basename or not sv_basename:
        result["action"] = "skip"
        result["error"] = "working CSV is missing DV_Path or SV_Path"
        return result

    try:
        dv_sha, dv_size, dv_path_in_zip, zip_sha, zip_size = \
            _stream_sha_from_s3_zip(s3, bucket, zip_key, dv_basename)
        sv_sha, sv_size, sv_path_in_zip, _, _ = \
            _stream_sha_from_s3_zip(s3, bucket, zip_key, sv_basename)
    except SystemExit as e:
        # _stream_sha_from_s3_zip raises SystemExit with a clear message when
        # the basename isn't in the ZIP or is ambiguous. Capture it.
        result["action"] = "skip"
        result["error"] = str(e).strip() or "DV/SV basename not in ZIP"
        return result

    trend_basename: Optional[str] = None
    trend_sha: Optional[str] = None
    existing_trend = _find_trend_in_run(s3, bucket, sc)
    if existing_trend:
        trend_basename = Path(existing_trend).name
        result["trend_csv_key"] = existing_trend
        obj = s3.get_object(Bucket=bucket, Key=existing_trend)
        trend_sha = _sha256_of_bytes(obj["Body"].read())

    record = _build_ingest_record(
        short_code=sc,
        ingestion_path="backfill",
        expected_dv_filename=dv_basename,
        expected_sv_filename=sv_basename,
        expected_dv_path_in_zip=dv_path_in_zip,
        expected_sv_path_in_zip=sv_path_in_zip,
        dv_sha256=dv_sha, sv_sha256=sv_sha,
        dv_filesize_bytes=dv_size, sv_filesize_bytes=sv_size,
        zip_basename=Path(zip_key).name,
        zip_sha256=zip_sha, zip_filesize_bytes=zip_size,
        trend_csv_basename=trend_basename, trend_csv_sha256=trend_sha,
        spreadsheet_url=SPREADSHEET_URL,
        spreadsheet_row_sha256=scenario.get("_row_sha256", ""),
        spreadsheet_file=WORKING_CSV_PATH,
    )

    record_key = _record_key_for(sc)
    result["ingest_record_key"] = record_key
    payload = json.dumps(record, indent=2, sort_keys=True).encode("utf-8")

    if dry_run:
        result["action"] = "would_put"
        return result

    s3.put_object(
        Bucket=bucket, Key=record_key,
        Body=payload, ContentType="application/json",
    )
    result["action"] = "put"
    return result


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    parser = argparse.ArgumentParser(
        description="Backfill ingest_record.json into scenario/<id>/ "
                    "for active scenarios."
    )
    parser.add_argument("--listing-csv", default=WORKING_CSV_PATH,
                        help=f"Working CSV (default: {WORKING_CSV_PATH})")
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET,
                        help=f"S3 bucket (default: {DEFAULT_S3_BUCKET})")
    parser.add_argument("--scenarios", nargs="*",
                        help="Scenario short codes to backfill, space-separated. "
                             "Either --scenarios or --all is required.")
    parser.add_argument("--all", action="store_true",
                        help="Backfill every row in the working CSV. "
                             "Either --scenarios or --all is required.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Replace existing ingest records (default: skip if present).")
    parser.add_argument("--dry-run", action="store_true",
                        help="List what would happen without writing to S3.")
    args = parser.parse_args()

    _require_working_csv(args.listing_csv)
    scenarios: List[Dict[str, Any]] = read_scenario_source_csv(args.listing_csv)
    log.info("Loaded %d well-formed scenario row(s) from %s", len(scenarios), args.listing_csv)

    if not args.all and not args.scenarios:
        log.error(
            "Specify --scenarios <short_codes> or --all. "
            "Example: --scenarios s0042 s0043, or --all to backfill every row in the CSV."
        )
        return

    if args.scenarios:
        filter_set = {s.lower() for s in args.scenarios}
        scenarios = [s for s in scenarios if s["short_code"].lower() in filter_set]

    if not scenarios:
        log.error("No scenarios match the filter.")
        return

    s3 = boto3.client("s3")

    print(f"\nBackfill plan ({len(scenarios)} scenario(s)):")
    if args.dry_run:
        print("DRY RUN: no S3 PUTs will happen.\n")
    elif args.overwrite:
        print("Mode: OVERWRITE existing ingest records.\n")
    else:
        print("Mode: skip scenarios that already have an ingest record.\n")

    results: List[Dict[str, Any]] = []
    for sc in scenarios:
        log.info("Processing %s ...", sc["short_code"])
        r = _backfill_one(s3, args.s3_bucket, sc, args.overwrite, args.dry_run)
        results.append(r)
        action = r["action"]
        if action in ("put", "would_put"):
            log.info("[%s] %s ingest record -> %s",
                     sc["short_code"], action, r["ingest_record_key"])
        else:
            log.warning("[%s] %s: %s", sc["short_code"], action, r["error"])

    # Summary
    print("\n" + "=" * 80)
    print(f"  BACKFILL SUMMARY (bucket: {args.s3_bucket})")
    print("=" * 80)
    counts: Dict[str, int] = {}
    for r in results:
        counts[r["action"]] = counts.get(r["action"], 0) + 1
    for k in sorted(counts):
        print(f"  {k:<12}: {counts[k]}")

    skipped = [r for r in results if r["action"] == "skip"]
    if skipped:
        print("\n  Skipped scenarios:")
        for r in skipped:
            print(f"    {r['short_code']}: {r['error']}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
