"""csv_reader.py - Read the working CSV and turn it into scenario dicts.

The working CSV (`etl/ingestion/scenario_listing/model_run_file_source_working.csv`)
is a copy of the WAM team's spreadsheet with extra operator columns
(`pinned_model_run_zip`, `pinned_trend_csv`, `download_status`, `notes`).

Strict mode: missing essential columns is a hard error (the CSV shape is
wrong). Per-row errors are skipped and logged. Cross-row uniqueness
violations (`short_code` and `dv_filename`) are logged and flagged on the
returned rows.
"""

from __future__ import annotations

import csv
import logging
import os
from typing import Any, Dict, List

from etl.common.scenarios import parse_scenarios as _parse_scenarios  # noqa: F401

from .config import (
    COLUMN_MAP,
    ESSENTIAL_FIELDS,
    REFERENCE_CSV_PATH,
    RE_FOLDER_ID,
    WORKING_CSV_PATH,
)
from .utils import _basename_of, _sha256_of_row

log = logging.getLogger("gdrive_bulk_download")


def _bootstrap_error_message(path: str) -> str:
    """Message printed when the working CSV is missing.
    """
    return (
        f"\nWorking CSV not found: {path}\n\n"
        f"Bootstrap from the reference copy:\n"
        f"  cp {REFERENCE_CSV_PATH} {WORKING_CSV_PATH}\n\n"
        f"Then open the working copy and fill operator columns where needed:\n"
        f"  pinned_model_run_zip, pinned_trend_csv, notes\n\n"
        f"Run scope is set on the CLI, not in the CSV: use\n"
        f"  --scenarios <s0070 s0080 ...>  or  --all\n"
    )


def _require_working_csv(path: str) -> None:
    """Hard-error if the working CSV does not exist on disk."""
    if not os.path.exists(path):
        raise SystemExit(_bootstrap_error_message(path))


def read_scenario_source_csv(csv_path: str) -> List[Dict[str, Any]]:
    """Read the working CSV. Strict mode.

    Returns the list of well-formed scenario rows. Rows with per-row errors
    are skipped and logged. Cross-row uniqueness violations are also logged.
    A missing essential column in the header is a hard error (the run cannot
    proceed because the file shape is wrong).
    """
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = list(reader)

    # ---- Check 1: essential columns present in header ------------
    missing_cols = [
        f"COLUMN_MAP['{internal}'] = '{COLUMN_MAP[internal]}'"
        for internal in ESSENTIAL_FIELDS
        if COLUMN_MAP[internal] not in header
    ]
    if missing_cols:
        raise SystemExit(
            f"\nWorking CSV is missing essential columns.\n"
            f"  CSV path: {csv_path}\n"
            f"  Missing:  {missing_cols}\n"
            f"Either restore the columns in the CSV, or update COLUMN_MAP "
            f"in etl/ingestion/lib/config.py.\n"
        )

    scenarios: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for line_no, raw in enumerate(rows, start=2):  # +1 for header, +1 for 1-index
        row_errors: List[str] = []

        # ---- Check 2: essential values non-empty -----------------
        values = {
            internal: (raw.get(COLUMN_MAP[internal]) or "").strip()
            for internal in COLUMN_MAP
        }
        for f in ESSENTIAL_FIELDS:
            if not values[f]:
                row_errors.append(f"empty_{f}")

        short_code = values["short_code"]

        # ---- Check 3: parse Drive folder ID -----------------------
        drive_folder_id = ""
        if values["drive_folder_url"]:
            m = RE_FOLDER_ID.search(values["drive_folder_url"])
            if m:
                drive_folder_id = m.group(1)
            else:
                # Some rows still have a folder name (no URL) in ModelFilesLink.
                # Treat as "no folder id" rather than a parse error. The scan
                # subcommand can still use the folder name as a path fallback.
                drive_folder_id = ""

        # ---- derive filenames from paths -------------------------
        dv_path = values["dv_path"].replace("\\", "/")
        sv_path = values["sv_path"].replace("\\", "/")
        dv_filename = _basename_of(dv_path)
        sv_filename = _basename_of(sv_path)
        if values["dv_path"] and not dv_filename:
            row_errors.append("dv_path_basename_unparseable")
        if values["sv_path"] and not sv_filename:
            row_errors.append("sv_path_basename_unparseable")

        # Optional download_status: rows we should attempt this run.
        download_status = (values.get("download_status") or "").strip().lower()

        scenario = {
            "short_code": short_code,
            "drive_folder_name": values["drive_folder_name"],
            "drive_folder_url": values["drive_folder_url"],
            "drive_folder_id": drive_folder_id,
            "dv_path": dv_path,
            "sv_path": sv_path,
            "dv_filename": dv_filename,
            "sv_filename": sv_filename,
            "dv_root": dv_path.split("/Model_Files/")[0] if "/Model_Files/" in dv_path else "",
            "pinned_zip": values["pinned_model_run_zip"],
            "pinned_trend": values["pinned_trend_csv"],
            "download_status": download_status,
            "notes": values["notes"],
            "_csv_line_no": line_no,
            "_row_sha256": _sha256_of_row({k: values[k] for k in COLUMN_MAP if values[k]}),
        }

        if row_errors:
            scenario["_row_errors"] = row_errors
            skipped.append(scenario)
        else:
            scenarios.append(scenario)

    # Cross-row uniqueness. We check short_code (must be unique) and
    # dv_filename (cross-paste detector). SV basenames are intentionally NOT
    # checked: the same SV input is often reused across scenarios on purpose.
    def _collect_dupes(field: str, rows_to_check: List[Dict[str, Any]]) -> Dict[str, List[int]]:
        index: Dict[str, List[int]] = {}
        for s in rows_to_check:
            value = s.get(field) or ""
            if not value:
                continue
            index.setdefault(value, []).append(s["_csv_line_no"])
        return {k: v for k, v in index.items() if len(v) > 1}

    dup_short_codes = _collect_dupes("short_code", scenarios + skipped)
    dup_dv = _collect_dupes("dv_filename", scenarios)

    if dup_short_codes:
        log.warning("Duplicate short_code values: %s", dup_short_codes)
    if dup_dv:
        log.warning("Duplicate DV basenames across ready rows (cross-paste risk?): %s", dup_dv)

    for s in scenarios:
        if s["short_code"] in dup_short_codes:
            s["_dup_short_code"] = True
        if s["dv_filename"] in dup_dv:
            s["_dup_dv_filename"] = True

    if skipped:
        log.warning("Skipped %d row(s) due to per-row errors:", len(skipped))
        for s in skipped:
            log.warning("  line %d (%s): %s",
                        s["_csv_line_no"], s.get("short_code", "?"), s.get("_row_errors", []))

    return scenarios
