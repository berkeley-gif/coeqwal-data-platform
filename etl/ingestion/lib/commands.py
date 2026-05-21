"""The three CLI subcommands for `gdrive_bulk_download.py`, in the order
they're meant to be run:

- `cmd_scan`:     list Drive contents per row without touching S3, used as
                  a Drive-side preflight before committing to a real
                  download run
- `cmd_download`: download + validate + stage to S3 (the heavy one)
- `cmd_promote`:  copy staged files from staging/ to ready/ in the safe
                  order (sidecar -> CSV -> ZIP) so the ZIP PUT triggers
                  Lambda only after its dependencies are in place

Each cmd_* takes the parsed argparse `args` and orchestrates: preflight,
load working CSV, filter scenarios, dispatch workers, write the per-run
report. The per-row work itself lives in `worker.py`. Audit report writers and
their column lists live here next to the commands that produce them.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

import boto3

from etl.common import READY_PREFIX, STAGING_PREFIX

from .config import (
    AUDIT_STATE_PATH,
    DEFAULT_OUTPUT_DIR,
    SCRIPT_VERSION,
)
from .csv_reader import (
    _parse_scenarios,
    _require_working_csv,
    read_scenario_source_csv,
)
from .preflight import _preflight
from .rclone import rclone_lsjson
from .utils import _now_iso_utc
from .worker import (
    _audit_row_template,
    _resolve_drive_access,
    process_scenario,
)

log = logging.getLogger("gdrive_bulk_download")


# ---------------------------------------------------------------------------
# scan subcommand: CSV + Drive content audit (no S3 writes)
# ---------------------------------------------------------------------------
SCAN_AUDIT_COLUMNS = [
    "scenario_id", "drive_folder_name", "drive_folder_id",
    "folder_name_match", "access_mode",
    "zip_count", "zip_names", "zip_selected", "zip_size_mb",
    "trend_csv_count", "trend_csv_names", "trend_csv_selected",
    "expected_dv_filename", "expected_sv_filename",
    "dv_root", "status",
]


def scan_scenario(scenario: Dict, rclone_remote: str) -> Dict:
    """List Drive contents for one scenario, report zip/csv counts."""
    sc = scenario["short_code"]
    folder_id = scenario["drive_folder_id"]
    folder_name = scenario["drive_folder_name"]
    dv_root = scenario["dv_root"]

    row: Dict = {
        "scenario_id": sc,
        "drive_folder_name": folder_name,
        "drive_folder_id": folder_id[:16] + "..." if len(folder_id) > 16 else folder_id,
        "folder_name_match": "",
        "access_mode": "",
        "zip_count": 0,
        "zip_names": "",
        "zip_selected": "",
        "zip_size_mb": "",
        "trend_csv_count": 0,
        "trend_csv_names": "",
        "trend_csv_selected": "",
        "expected_dv_filename": scenario["dv_filename"],
        "expected_sv_filename": scenario["sv_filename"],
        "dv_root": dv_root,
        "status": "",
    }

    if dv_root and dv_root != folder_name:
        row["folder_name_match"] = "MISMATCH"
        log.info("[%s] Folder name mismatch: GoogleDriveFolderName='%s' vs DV_Path root='%s'",
                 sc, folder_name, dv_root)
    elif dv_root:
        row["folder_name_match"] = "OK"
    else:
        row["folder_name_match"] = "NO_DV_PATH"

    fid, model_path, trend_path, access_mode = _resolve_drive_access(scenario)
    row["access_mode"] = access_mode

    if access_mode == "none":
        row["status"] = "NO_DRIVE_ACCESS"
        log.warning("[%s] No folder ID and no folder name -- cannot scan", sc)
        return row

    if access_mode == "path":
        log.info("[%s] No folder ID; using Shared Drive path: %s", sc, model_path)
    else:
        log.info("[%s] Using folder ID: %s ...", sc, folder_id[:12])

    statuses = []

    model_files = rclone_lsjson(fid, model_path, rclone_remote=rclone_remote)
    zips = [f for f in model_files if f["Name"].lower().endswith(".zip")]
    row["zip_count"] = len(zips)
    row["zip_names"] = ";".join(sorted(f["Name"] for f in zips))

    pinned_zip = scenario.get("pinned_zip", "")
    if not zips:
        statuses.append("MISSING_ZIP")
        log.warning("[%s] No ZIP files found in Model_Files/", sc)
    elif pinned_zip:
        match = [f for f in zips if f["Name"] == pinned_zip]
        if match:
            row["zip_selected"] = match[0]["Name"]
            row["zip_size_mb"] = f"{int(match[0].get('Size', 0)) / (1024 * 1024):.1f}"
        else:
            statuses.append("PINNED_ZIP_NOT_FOUND")
            log.warning("[%s] Pinned ZIP '%s' not found among: %s",
                        sc, pinned_zip, ", ".join(f["Name"] for f in zips))
    else:
        if len(zips) == 1:
            row["zip_selected"] = zips[0]["Name"]
            row["zip_size_mb"] = f"{int(zips[0].get('Size', 0)) / (1024 * 1024):.1f}"
        else:
            statuses.append("MULTIPLE_ZIPS_NO_PIN")
            log.warning("[%s] Multiple ZIPs (%d): %s; set pinned_model_run_zip",
                        sc, len(zips), ", ".join(f["Name"] for f in zips))

    trend_files = rclone_lsjson(fid, trend_path, rclone_remote=rclone_remote)
    trend_csvs = [f for f in trend_files if f["Name"].lower().endswith(".csv")]
    row["trend_csv_count"] = len(trend_csvs)
    row["trend_csv_names"] = ";".join(sorted(f["Name"] for f in trend_csvs))

    pinned_trend = scenario.get("pinned_trend", "")
    if not trend_csvs:
        statuses.append("NO_TREND_REPORT")
        log.warning("[%s] No trend report CSV found", sc)
    elif pinned_trend:
        match = [f for f in trend_csvs if f["Name"] == pinned_trend]
        if match:
            row["trend_csv_selected"] = match[0]["Name"]
        else:
            statuses.append("PINNED_TREND_NOT_FOUND")
            log.warning("[%s] Pinned trend CSV '%s' not found among: %s",
                        sc, pinned_trend, ", ".join(f["Name"] for f in trend_csvs))
    else:
        if len(trend_csvs) == 1:
            row["trend_csv_selected"] = trend_csvs[0]["Name"]
        else:
            statuses.append("MULTIPLE_TREND_REPORTS")
            log.warning("[%s] Multiple trend CSVs (%d): %s; set pinned_trend_csv",
                        sc, len(trend_csvs), ", ".join(f["Name"] for f in trend_csvs))

    if dv_root and dv_root != folder_name:
        statuses.append("FOLDER_MISMATCH")

    row["status"] = "|".join(statuses) if statuses else "OK"

    log.info("[%s] Scan done -- %s (access=%s, zips=%d, trend_csvs=%d)",
             sc, row["status"], access_mode, row["zip_count"], row["trend_csv_count"])
    return row


def write_scan_audit(rows: List[Dict], local_path: str):
    """Write scan audit CSV to disk."""
    rows.sort(key=lambda r: r.get("scenario_id", ""))
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=SCAN_AUDIT_COLUMNS,
                            extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    csv_text = buf.getvalue()

    with open(local_path, "w") as f:
        f.write(csv_text)
    log.info("Scan audit written to %s", local_path)

    print("\n" + "=" * 100)
    print("  SCAN AUDIT SUMMARY")
    print("=" * 100)
    ok = sum(1 for r in rows if r.get("status") == "OK")
    missing = sum(1 for r in rows if "MISSING" in r.get("status", ""))
    multi = sum(1 for r in rows if "MULTIPLE" in r.get("status", ""))
    mismatch = sum(1 for r in rows if "FOLDER_MISMATCH" in r.get("status", ""))
    no_access = sum(1 for r in rows if r.get("status") == "NO_DRIVE_ACCESS")
    local_only = sum(1 for r in rows if r.get("status", "").startswith("LOCAL_ONLY"))
    print(f"  Total scenarios:       {len(rows)}")
    print(f"  OK (clean):            {ok}")
    print(f"  Missing files:         {missing}")
    print(f"  Multiple (need pin):   {multi}")
    print(f"  Folder mismatches:     {mismatch}")
    print(f"  No drive access:       {no_access}")
    if local_only:
        print(f"  Local-only entries:    {local_only}")

    print()
    hdr = "  {:<8} {:<5} {:>4} {:>4} {:<8} {}"
    print(hdr.format("Scenario", "Via", "Zips", "CSVs", "Match", "Status"))
    print("  " + "-" * 90)
    for r in rows:
        print(hdr.format(
            r.get("scenario_id", ""),
            r.get("access_mode", "")[:5],
            str(r.get("zip_count", "")),
            str(r.get("trend_csv_count", "")),
            r.get("folder_name_match", ""),
            r.get("status", ""),
        ))

    attention = [r for r in rows
                 if r.get("status", "") not in ("OK", "LOCAL_ONLY")]
    if attention:
        print()
        print("  SCENARIOS REQUIRING ATTENTION:")
        print("  " + "-" * 90)
        for r in attention:
            print(f"  {r['scenario_id']}: {r['status']}")
            if r.get("zip_names"):
                print(f"    ZIPs: {r['zip_names']}")
            if r.get("trend_csv_names"):
                print(f"    Trend CSVs: {r['trend_csv_names']}")
            if r.get("folder_name_match") == "MISMATCH":
                print(f"    Folder name: {r['drive_folder_name']}")
                print(f"    DV_Path root: {r['dv_root']}")

    print("=" * 100 + "\n")


def cmd_scan(args):
    """Scan Drive contents using the working CSV.

    In order to catch missing folders, missing ZIPs, missing trend CSVs,
    folder-name mismatches, and pinned-filename-not-found cases BEFORE
    spending bandwidth on a real download run, scan walks each scenario's
    Drive folder and writes `scan_audit.csv`. It never touches S3 and
    never downloads files. Use it as a pre-flight on a freshly bootstrapped
    working CSV, or after editing rows.
    """
    rclone_remote = args.rclone_remote

    # Pre-flight (rclone only - scan never touches S3). Skipped for
    # `--local-only` because that mode bypasses Drive entirely.
    if not args.local_only:
        _preflight(rclone_remote, include_s3=False)

    _require_working_csv(args.listing_csv)
    scenarios = read_scenario_source_csv(args.listing_csv)
    log.info("Loaded %d well-formed scenario row(s) from %s",
             len(scenarios), args.listing_csv)

    if not args.all and not args.scenarios:
        log.error(
            "Specify --scenarios <short_codes> or --all. "
            "Example: --scenarios s0042 s0043, or --all to scan every row in the CSV."
        )
        return 1

    if args.scenarios:
        filter_set = _parse_scenarios(args.scenarios)
        scenarios = [s for s in scenarios if s["short_code"].lower() in filter_set]

    if not scenarios:
        log.error("No scenarios matched the filter")
        return

    if args.local_only:
        log.info("Local-only mode: writing manifest without Drive access")
        results = []
        for sc in scenarios:
            results.append({
                "scenario_id": sc["short_code"],
                "drive_folder_name": sc["drive_folder_name"],
                "drive_folder_id": sc["drive_folder_id"][:16] + "..."
                    if len(sc["drive_folder_id"]) > 16 else sc["drive_folder_id"],
                "folder_name_match": "MISMATCH" if sc["dv_root"] and sc["dv_root"] != sc["drive_folder_name"]
                    else ("OK" if sc["dv_root"] else "NO_DV_PATH"),
                "access_mode": "id" if sc["drive_folder_id"] else (
                    "path" if (sc["dv_root"] or sc["drive_folder_name"]) else "none"),
                "expected_dv_filename": sc["dv_filename"],
                "expected_sv_filename": sc["sv_filename"],
                "dv_root": sc["dv_root"],
                "zip_count": "",
                "zip_names": "",
                "zip_selected": "",
                "zip_size_mb": "",
                "trend_csv_count": "",
                "trend_csv_names": "",
                "trend_csv_selected": "",
                "status": "LOCAL_ONLY" + ("|FOLDER_MISMATCH"
                    if sc["dv_root"] and sc["dv_root"] != sc["drive_folder_name"] else "")
                    + ("|NO_FOLDER_ID" if not sc["drive_folder_id"] else ""),
            })
        output_dir = Path(getattr(args, "output_dir", None) or DEFAULT_OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        local_path = str(output_dir / "scan_audit.csv")
        write_scan_audit(results, local_path)
        return

    results: List[Dict] = []
    workers = args.workers

    if workers <= 1:
        for sc in scenarios:
            row = scan_scenario(sc, rclone_remote)
            results.append(row)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(scan_scenario, sc, rclone_remote): sc
                for sc in scenarios
            }
            for future in as_completed(futures):
                sc = futures[future]
                try:
                    row = future.result()
                    results.append(row)
                except Exception:
                    log.exception("[%s] Scan worker failed", sc["short_code"])
                    results.append({
                        "scenario_id": sc["short_code"],
                        "status": "WORKER_ERROR",
                    })

    output_dir = Path(getattr(args, "output_dir", None) or DEFAULT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = str(output_dir / "scan_audit.csv")
    write_scan_audit(results, local_path)


# ---------------------------------------------------------------------------
# download subcommand
# ---------------------------------------------------------------------------
AUDIT_COLUMNS = [
    "scenario_id", "drive_folder_id", "drive_folder_name",
    "expected_dv_filename", "expected_sv_filename",
    "ingestion_path", "access_mode",
    "zip_count", "zip_selected", "zip_size_mb", "zip_sha256",
    "dss_file_count", "classification_method",
    "sv_selected", "sv_sha256",
    "dv_selected", "dv_sha256",
    "trend_csv_count", "trend_csv_selected", "trend_csv_sha256",
    "convention_dv_ok", "convention_sv_ok",
    "s3_staging_zip_key", "s3_staging_csv_key", "s3_staging_sidecar_key",
    "validation_status", "verification_status",
    "error_code", "error_message", "notes",
]


def write_audit_report(rows: List[Dict], local_path: str,
                       s3_client, s3_bucket: str):
    """Write per-run CSV audit, upload to S3, and write the JSON state file.

    Two files come out of this function. They contain the same
    underlying per-row records, but in different shapes for different
    consumers:

    1. `audit_report.csv` (at `local_path`, default
       `etl/ingestion/audit_reports/audit_report.csv`). Flat tabular view,
       one row per scenario. Open it in a spreadsheet to eyeball a run at
       a glance. Also uploaded to `s3://<bucket>/<STAGING_PREFIX>/audit_report.csv`.

    2. `audit_state.json` (at `AUDIT_STATE_PATH`, default
       `etl/ingestion/audit_reports/audit_state.json`). Structured nested JSON,
       schema-versioned. Consumed by `etl/ingestion/tools/audit.py` to render
       `etl/ingestion/audit.md`. This is the only handoff between this
       script (which knows about local-only failures that never reached
       S3) and audit.py (which walks S3 for sidecar.json and the Batch
       container's manifest). Without this file, audit.py would have no
       record of scenarios that were skipped during ingest.

    Both files are gitignored under `etl/**/audit_reports/` and are
    regeneratable. Re-running `gdrive_bulk_download.py download` (with
    the same or a different --scenarios filter) rewrites them in place.
    Long-term history lives in S3 (sidecars + per-scenario manifests)
    and in the tracked `audit.md` that audit.py renders.
    """
    import json  # local import: keeps the module's top imports thin

    rows.sort(key=lambda r: r.get("scenario_id", ""))
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=AUDIT_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    csv_text = buf.getvalue()

    with open(local_path, "w") as f:
        f.write(csv_text)
    log.info("Audit report written to %s", local_path)

    s3_key = f"{STAGING_PREFIX}/audit_report.csv"
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key,
                         Body=csv_text.encode("utf-8"))
    log.info("Audit report uploaded to s3://%s/%s", s3_bucket, s3_key)

    state = {
        "schema_version": 1,
        "run_at_utc": _now_iso_utc(),
        "script": "gdrive_bulk_download.py",
        "script_version": SCRIPT_VERSION,
        "scenarios": rows,
    }
    AUDIT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))
    log.info("Audit state written to %s", AUDIT_STATE_PATH)

    # Console summary
    print("\n" + "=" * 100)
    print("  DOWNLOAD & VALIDATION SUMMARY")
    print("=" * 100)
    ok = sum(1 for r in rows if r.get("validation_status") == "OK")
    skipped = sum(1 for r in rows
                  if r.get("validation_status") not in ("OK", "DRY_RUN", ""))
    dry = sum(1 for r in rows if r.get("validation_status") == "DRY_RUN")
    print(f"  Total scenarios:  {len(rows)}")
    print(f"  OK:               {ok}")
    print(f"  Skipped (review): {skipped}")
    if dry:
        print(f"  Dry run:          {dry}")

    print()
    hdr = "  {:<8} {:<36} {:<36} {}"
    print(hdr.format("Scenario", "DV selected (basename)", "SV selected (basename)", "Status"))
    print("  " + "-" * 96)
    for r in rows:
        dv_name = os.path.basename(r.get("dv_selected", ""))[:36] if r.get("dv_selected") else "-"
        sv_name = os.path.basename(r.get("sv_selected", ""))[:36] if r.get("sv_selected") else "-"
        print(hdr.format(
            r.get("scenario_id", ""),
            dv_name, sv_name,
            r.get("validation_status", ""),
        ))

    attention = [r for r in rows if r.get("validation_status") not in ("OK", "DRY_RUN", "")]
    if attention:
        print()
        print("  SCENARIOS REQUIRING REVIEW (see audit.md after running etl/ingestion/tools/audit.py):")
        print("  " + "-" * 96)
        for r in attention:
            print(f"  {r.get('scenario_id', '')}: {r.get('error_code', r.get('validation_status', ''))}")
            if r.get("error_message"):
                print(f"    {r['error_message']}")

    print("=" * 100 + "\n")


def cmd_download(args):
    """Download, validate, and stage scenarios to S3."""
    rclone_remote = args.rclone_remote

    # Pre-flight: rclone installed, remote configured, S3 bucket reachable.
    # Fails fast (SystemExit) with an actionable message before we open the
    # CSV or build the per-row plan, so an unconfigured Cloud9 doesn't waste
    # an operator's time discovering the same error N times. `--dry-run` skips
    # the S3 head_bucket so a local machine iterating on the working CSV
    # without prod AWS creds can still exercise the full Drive-listing path.
    _preflight(rclone_remote, args.s3_bucket, include_s3=not args.dry_run)

    _require_working_csv(args.listing_csv)
    scenarios = read_scenario_source_csv(args.listing_csv)
    log.info("Loaded %d well-formed scenario row(s) from %s", len(scenarios), args.listing_csv)

    if not args.all and not args.scenarios:
        log.error(
            "Specify --scenarios <short_codes> or --all. "
            "Example: --scenarios s0042 s0043, or --all to process every row in the CSV."
        )
        return 1

    if args.scenarios:
        filter_set = _parse_scenarios(args.scenarios)
        scenarios = [s for s in scenarios if s["short_code"].lower() in filter_set]

    if not scenarios:
        log.error("No scenarios matched the filter")
        return

    log.info("About to process %d scenarios:", len(scenarios))
    for s in scenarios:
        fid = s["drive_folder_id"]
        log.info("  %s: %s (folder: %s)", s["short_code"],
                 s.get("drive_folder_name"),
                 fid[:12] + "..." if fid else "NONE")

    s3_client = boto3.client("s3")
    results: List[Dict] = []
    workers = args.workers

    if workers <= 1:
        for sc in scenarios:
            row = process_scenario(sc, s3_client, args.s3_bucket,
                                   args.dry_run, rclone_remote)
            results.append(row)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(process_scenario, sc, s3_client,
                            args.s3_bucket, args.dry_run, rclone_remote): sc
                for sc in scenarios
            }
            for future in as_completed(futures):
                sc = futures[future]
                try:
                    row = future.result()
                    results.append(row)
                except Exception as e:
                    log.exception("[%s] Worker failed", sc["short_code"])
                    results.append({
                        **_audit_row_template(sc),
                        "validation_status": "WORKER_ERROR",
                        "error_code": "WORKER_ERROR",
                        "error_message": str(e),
                    })

    output_dir = Path(getattr(args, "output_dir", None) or DEFAULT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    local_report = str(output_dir / "audit_report.csv")
    write_audit_report(results, local_report, s3_client, args.s3_bucket)

    # Auto-render audit.md. This snapshot reflects what just
    # got staged plus whatever was already in S3.
    if not getattr(args, "skip_audit", False):
        try:
            from etl.ingestion.tools.audit import regenerate_audit
            regenerate_audit(args.s3_bucket)
        except Exception as e:
            log.warning(
                "Audit auto-render failed (%s). Re-run manually: "
                "python etl/ingestion/tools/audit.py",
                e,
            )


# ---------------------------------------------------------------------------
# promote subcommand (timing fix: upload order)
# ---------------------------------------------------------------------------
# The ZIP is the Lambda trigger. Anything the Lambda might read must be at
# rest in ready/<id>/ BEFORE the ZIP arrives. Order is non-negotiable.
PROMOTE_ORDER_PREFIXES = ("sidecar.json",)
PROMOTE_ORDER_SUFFIXES = (".csv", ".zip")


def _sort_promote_keys(keys: List[str]) -> List[str]:
    """Sort keys for promote so sidecar -> trend CSV -> ZIP is the upload order.

    Anything else (extra docs etc.) lands after the sidecar and before the
    CSV/ZIP, in alphabetical order.
    """
    def rank(k: str) -> Tuple[int, str]:
        fn = k.rsplit("/", 1)[-1].lower()
        if fn == "sidecar.json":
            return (0, fn)
        if fn.endswith(".csv"):
            return (2, fn)
        if fn.endswith(".zip"):
            return (3, fn)
        return (1, fn)

    return sorted(keys, key=rank)


def cmd_promote(args):
    """Copy files from staging/scenario_data/<id>/ to ready/<id>/.

    Upload order is enforced: sidecar.json first, trend CSV second, ZIP last.
    The ZIP PUT under ready/ is the Lambda trigger.
    """
    s3 = boto3.client("s3")
    bucket = args.s3_bucket

    # Prefix includes the trailing slash so we only list scenario_data, not
    # any sibling directory (e.g. tier_data) under staging/.
    staging_prefix_slash = STAGING_PREFIX.rstrip("/") + "/"
    staging_depth = len(STAGING_PREFIX.split("/"))  # e.g. "staging/scenario_data" -> 2

    staging_objects: List[Dict] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=staging_prefix_slash):
        for obj in page.get("Contents", []):
            staging_objects.append(obj)

    groups: Dict[str, List[str]] = {}
    for obj in staging_objects:
        key = obj["Key"]
        parts = key.split("/")
        # Match keys shaped like <STAGING_PREFIX>/<short_code>/<file>.
        if len(parts) >= staging_depth + 2:
            sc = parts[staging_depth]
            if re.match(r"^s\d{4}$", sc):
                groups.setdefault(sc, []).append(key)

    filter_ids = None
    if args.scenarios:
        filter_ids = _parse_scenarios(args.scenarios)

    if filter_ids:
        groups = {k: v for k, v in groups.items() if k in filter_ids}
        if not groups:
            log.error("No staged files found for %s",
                      ", ".join(sorted(filter_ids)))
            return

    if not groups:
        log.error("No staged scenarios found in s3://%s/%s", bucket, staging_prefix_slash)
        return

    # Pre-sort each group's keys into the safe upload order. This is the
    # order we'll actually write to ready/.
    for sc in groups:
        groups[sc] = _sort_promote_keys(groups[sc])

    print(f"\nAbout to promote {len(groups)} scenario(s) from {staging_prefix_slash} to {READY_PREFIX}/.")
    print("Upload order per scenario: sidecar.json -> trend CSV -> ZIP last.")
    print("The ZIP PUT is the Lambda trigger.\n")
    for sc in sorted(groups):
        files = [k.split("/")[-1] for k in groups[sc]]
        print(f"  {sc}: {' -> '.join(files)}")

    if args.dry_run:
        print("\nDRY RUN: not copying anything.")
        return

    print("\nPromoting now. The ZIP PUT under ready/ triggers Lambda.\n")

    for sc in sorted(groups):
        for src_key in groups[sc]:  # already in safe order
            filename = src_key.split("/")[-1]
            dest_key = f"{READY_PREFIX}/{sc}/{filename}"
            log.info("Copying s3://%s/%s -> s3://%s/%s",
                     bucket, src_key, bucket, dest_key)
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": src_key},
                Key=dest_key,
            )
        log.info("[%s] Promoted to %s/", sc, READY_PREFIX)

    print(f"\nDone. Promoted {len(groups)} scenario(s) to {READY_PREFIX}/.")
    print("The Lambda will trigger on each ZIP upload.")
