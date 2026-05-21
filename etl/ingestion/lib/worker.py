"""Per-scenario worker: take one parsed CSV row through download, validate,
hash, sidecar build, and staging upload.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from typing import Any, Dict, Optional

from etl.common import STAGING_PREFIX

from .config import DRIVE_SCENARIO_PREFIX
from .errors import IngestionError
from .rclone import rclone_cat, rclone_copy_file, rclone_lsjson
from .utils import _sha256_of_bytes
from .zip_validation import build_sidecar, validate_and_hash_zip

log = logging.getLogger("gdrive_bulk_download")

# Thread guard so the per-scenario "Done" line at the bottom of process_scenario
# doesn't interleave between workers when --workers > 1.
_print_lock = threading.Lock()


def _resolve_drive_access(scenario: Dict) -> tuple:
    """Determine how to access a scenario's Drive folder.

    Returns (folder_id_or_empty, model_files_path, trend_path, access_mode).

    - `id`:   `drive_folder_id` came from a parseable ModelFilesLink URL.
              rclone calls pass `--drive-root-folder-id=<id>` and use paths
              relative to the folder root.
    - `path`: ModelFilesLink was missing or unparseable. Fall back to
              `GoogleDriveFolderName` or the DV_Path root as a full Drive
              path, prepended with `DRIVE_SCENARIO_PREFIX`.
    - `none`: Neither id nor name available. Caller records NO_DRIVE_ACCESS.
    """
    folder_id = scenario["drive_folder_id"]
    folder_name = scenario["drive_folder_name"]
    dv_root = scenario["dv_root"]

    if folder_id:
        return (
            folder_id,
            "Model_Files/",
            "Data_Extraction/Variables_From_trend_report_variables_v5/",
            "id",
        )

    base = dv_root or folder_name
    if not base:
        return ("", "", "", "none")

    # Prepend the Shared Drive prefix where scenario folders actually live.
    # `gdrive:` is rooted at the COEQWAL Shared Drive top level. Without the
    # prefix, rclone returns "directory not found" for the per-scenario path.
    prefix = f"{DRIVE_SCENARIO_PREFIX.rstrip('/')}/" if DRIVE_SCENARIO_PREFIX else ""

    return (
        "",
        f"{prefix}{base}/Model_Files/",
        f"{prefix}{base}/Data_Extraction/Variables_From_trend_report_variables_v5/",
        "path",
    )


def _audit_row_template(scenario: Dict[str, Any]) -> Dict[str, Any]:
    """Empty audit row, pre-populated with what we know from the CSV alone."""
    return {
        "scenario_id": scenario["short_code"],
        "drive_folder_id": scenario["drive_folder_id"],
        "drive_folder_name": scenario["drive_folder_name"],
        "expected_dv_filename": scenario["dv_filename"],
        "expected_sv_filename": scenario["sv_filename"],
        "ingestion_path": "automated",
        # `access_mode` records how `process_scenario` reached this row's
        # Drive folder: "id" (folder URL parsed cleanly), "path" (fell back
        # to GoogleDriveFolderName / DV_Path root), or "none" (could not
        # reach Drive at all -> NO_DRIVE_ACCESS). Surfaced so operators can
        # see at a glance which rows are running on the path fallback.
        "access_mode": "",
        "zip_count": 0,
        "zip_selected": "",
        "zip_size_mb": "",
        "zip_sha256": "",
        "dss_file_count": 0,
        "classification_method": "",
        "sv_selected": "",
        "sv_sha256": "",
        "dv_selected": "",
        "dv_sha256": "",
        "trend_csv_count": 0,
        "trend_csv_selected": "",
        "trend_csv_sha256": "",
        "convention_dv_ok": "",
        "convention_sv_ok": "",
        "s3_staging_zip_key": "",
        "s3_staging_csv_key": "",
        "s3_staging_sidecar_key": "",
        "validation_status": "",
        "verification_status": "",
        "error_code": "",
        "error_message": "",
        "notes": scenario.get("notes", ""),
    }


def process_scenario(
    scenario: Dict, s3_client, s3_bucket: str, dry_run: bool,
    rclone_remote: str,
) -> Dict[str, Any]:
    """Download, validate, hash, and stage one scenario. Skip-not-abort on failure."""
    sc = scenario["short_code"]
    row = _audit_row_template(scenario)

    try:
        # Resolve how to reach this scenario on Drive
        # Mirrors `scan_scenario`: prefer the parsed folder ID; otherwise fall
        # back to the WAM folder name as a path-rooted lookup; otherwise this
        # row has no Drive access at all and is recorded in the audit.
        folder_id, model_path, trend_path, access_mode = _resolve_drive_access(scenario)
        row["access_mode"] = access_mode
        if access_mode == "none":
            raise IngestionError(
                "NO_DRIVE_ACCESS",
                f"[{sc}] No Drive folder ID and no folder-name path; "
                f"set ModelFilesLink (Drive URL) or GoogleDriveFolderName in the working CSV",
            )

        # List Model_Files for ZIPs
        if access_mode == "id":
            log.info("[%s] Listing Drive folder (ID: %s) ...", sc, folder_id[:12])
        else:
            log.info("[%s] Listing Drive path: %s ...", sc, model_path)
        all_model_files = rclone_lsjson(folder_id, model_path, rclone_remote=rclone_remote)
        zips = [f for f in all_model_files if f["Name"].lower().endswith(".zip")]
        row["zip_count"] = len(zips)

        if not zips:
            raise IngestionError("MISSING_ZIP", f"[{sc}] No ZIP files in Model_Files/")

        pinned_zip = scenario.get("pinned_zip", "")
        if pinned_zip:
            match = [f for f in zips if f["Name"] == pinned_zip]
            if not match:
                raise IngestionError(
                    "PINNED_ZIP_NOT_FOUND",
                    f"[{sc}] Pinned ZIP '{pinned_zip}' not found among: "
                    f"{[f['Name'] for f in zips]}",
                )
            selected_zip = match[0]
        else:
            if len(zips) > 1:
                # Without an operator pin, we cannot pick one safely.
                names = [f["Name"] for f in zips]
                raise IngestionError(
                    "MULTIPLE_ZIPS_NO_PIN",
                    f"[{sc}] Multiple ZIPs in Model_Files/ ({names}); set pinned_model_run_zip",
                )
            selected_zip = zips[0]

        row["zip_selected"] = selected_zip["Name"]
        size_bytes = int(selected_zip.get("Size", 0))
        row["zip_size_mb"] = f"{size_bytes / (1024 * 1024):.1f}"

        # List Data_Extraction for trend CSVs
        # The trend report is optional. It is used downstream to verify the
        # extracted data. If we cannot pick a single CSV unambiguously, we
        # still stage the scenario but mark its `verification_status` as
        # `unverified_*`. The audit reports this.
        trend_files = rclone_lsjson(folder_id, trend_path, rclone_remote=rclone_remote)
        trend_csvs = [f for f in trend_files if f["Name"].lower().endswith(".csv")]
        row["trend_csv_count"] = len(trend_csvs)

        pinned_trend = scenario.get("pinned_trend", "")
        selected_csv: Optional[Dict[str, Any]] = None
        verification_status = "verified"

        if not trend_csvs:
            verification_status = "unverified_no_trend"
            log.warning(
                "[%s] No trend report CSV in trend folder. Continuing as unverified.",
                sc,
            )
        elif pinned_trend:
            match = [f for f in trend_csvs if f["Name"] == pinned_trend]
            if match:
                selected_csv = match[0]
            else:
                verification_status = "unverified_pin_missing"
                log.warning(
                    "[%s] Pinned trend CSV '%s' not found among %s. Continuing as unverified.",
                    sc, pinned_trend, [f["Name"] for f in trend_csvs],
                )
        elif len(trend_csvs) > 1:
            verification_status = "unverified_multi_trend"
            log.warning(
                "[%s] Multiple trend CSVs (%s); set pinned_trend_csv to pick one. "
                "Continuing as unverified.",
                sc, [f["Name"] for f in trend_csvs],
            )
        else:
            selected_csv = trend_csvs[0]

        row["verification_status"] = verification_status
        if selected_csv:
            row["trend_csv_selected"] = selected_csv["Name"]

        if dry_run:
            csv_label = selected_csv["Name"] if selected_csv is not None else "(no trend)"
            log.info("[%s] DRY RUN -- would download %s (%.1f MB) + %s",
                     sc, selected_zip["Name"], size_bytes / (1024 * 1024),
                     csv_label)
            row["validation_status"] = "DRY_RUN"
            return row

        # Download ZIP, validate, hash
        tmp_dir = tempfile.mkdtemp(prefix=f"gdrive_{sc}_")
        zip_filename = selected_zip["Name"]
        zip_local = os.path.join(tmp_dir, zip_filename)
        try:
            log.info("[%s] Downloading ZIP: %s (%.1f MB) ...", sc,
                     zip_filename, size_bytes / (1024 * 1024))
            ok = rclone_copy_file(folder_id, f"{model_path}{zip_filename}", tmp_dir,
                                  rclone_remote=rclone_remote)
            if not ok or not os.path.exists(zip_local):
                raise IngestionError("DOWNLOAD_FAILED", f"[{sc}] ZIP download failed")

            log.info("[%s] Validating ZIP (strict, hashing) ...", sc)
            val = validate_and_hash_zip(
                zip_local, sc, scenario["sv_filename"], scenario["dv_filename"]
            )

            row["zip_sha256"] = val["zip_sha256"]
            row["dss_file_count"] = len(val["classification"].get("sv_candidates", [])) + \
                len(val["classification"].get("dv_candidates", []))
            row["classification_method"] = val["classification"]["classification_method"]
            row["sv_selected"] = val["sv_path_in_zip"]
            row["sv_sha256"] = val["sv_sha256"]
            row["dv_selected"] = val["dv_path_in_zip"]
            row["dv_sha256"] = val["dv_sha256"]

            sv_base = scenario["sv_filename"]
            dv_base = scenario["dv_filename"]
            row["convention_sv_ok"] = sc.lower() in (sv_base or "").lower()
            row["convention_dv_ok"] = sc.lower() in (dv_base or "").lower()
            if not row["convention_dv_ok"]:
                log.warning("[%s] short_code not in DV basename '%s' (convention warn)",
                            sc, dv_base)

            # Download trend CSV (optional) and hash
            csv_name: Optional[str] = None
            csv_bytes: Optional[bytes] = None
            trend_sha: Optional[str] = None
            if selected_csv is not None:
                csv_name = selected_csv["Name"]
                log.info("[%s] Downloading trend CSV: %s ...", sc, csv_name)
                csv_bytes = rclone_cat(folder_id, f"{trend_path}{csv_name}",
                                       rclone_remote=rclone_remote)
                if not csv_bytes:
                    # Download failure for a present-but-unreadable trend CSV
                    # marks the scenario as unverified.
                    log.warning(
                        "[%s] Trend CSV download failed; continuing as unverified.", sc,
                    )
                    csv_name = None
                    csv_bytes = None
                    row["verification_status"] = "unverified_no_trend"
                else:
                    trend_sha = _sha256_of_bytes(csv_bytes)
                    row["trend_csv_sha256"] = trend_sha

            # Build sidecar.json
            sidecar = build_sidecar(
                scenario,
                zip_basename=zip_filename,
                zip_sha256=val["zip_sha256"],
                zip_filesize=val["zip_filesize_bytes"],
                sv_path_in_zip=val["sv_path_in_zip"],
                sv_sha256=val["sv_sha256"],
                sv_filesize=val["sv_filesize_bytes"],
                dv_path_in_zip=val["dv_path_in_zip"],
                dv_sha256=val["dv_sha256"],
                dv_filesize=val["dv_filesize_bytes"],
                trend_csv_basename=csv_name,
                trend_csv_sha256=trend_sha,
                access_mode=access_mode,
            )
            sidecar_bytes = json.dumps(sidecar, indent=2, sort_keys=True).encode("utf-8")

            # Upload to staging:
            # The Lambda trigger is the ready/<id>/<zip> PUT, which happens
            # during `promote`. Uploads to staging/scenario_data/<id>/ here
            # are just a holding area until the operator decides to promote.
            s3_zip_key = f"{STAGING_PREFIX}/{sc}/{zip_filename}"
            s3_sidecar_key = f"{STAGING_PREFIX}/{sc}/sidecar.json"
            s3_csv_key = f"{STAGING_PREFIX}/{sc}/{csv_name}" if csv_name else ""

            log.info("[%s] Uploading ZIP to s3://%s/%s ...", sc, s3_bucket, s3_zip_key)
            s3_client.upload_file(zip_local, s3_bucket, s3_zip_key)
            row["s3_staging_zip_key"] = s3_zip_key

            if csv_name and csv_bytes is not None:
                log.info("[%s] Uploading trend CSV to s3://%s/%s ...", sc, s3_bucket, s3_csv_key)
                s3_client.put_object(Bucket=s3_bucket, Key=s3_csv_key, Body=csv_bytes)
                row["s3_staging_csv_key"] = s3_csv_key

            log.info("[%s] Uploading sidecar.json to s3://%s/%s ...", sc, s3_bucket, s3_sidecar_key)
            s3_client.put_object(
                Bucket=s3_bucket, Key=s3_sidecar_key,
                Body=sidecar_bytes,
                ContentType="application/json",
            )
            row["s3_staging_sidecar_key"] = s3_sidecar_key

            row["validation_status"] = "OK"

        finally:
            if os.path.exists(zip_local):
                try:
                    os.remove(zip_local)
                except OSError:
                    pass
            try:
                os.rmdir(tmp_dir)
            except OSError:
                pass

    except IngestionError as e:
        row["validation_status"] = e.code
        row["error_code"] = e.code
        row["error_message"] = e.message
        log.error("[%s] %s: %s", sc, e.code, e.message)

    with _print_lock:
        status = row.get("validation_status", "")
        if status == "OK":
            log.info("[%s] Done -- OK", sc)
        elif status == "DRY_RUN":
            log.info("[%s] Done -- DRY_RUN", sc)
        else:
            log.warning("[%s] Done -- %s (recorded in audit, run continues)", sc, status)

    return row
