"""
config.py - Constants for the Google Drive -> S3 ingestion pipeline.

These constants are specific to `gdrive_bulk_download.py` and the
auxiliary scripts under `etl/ingestion/tools/` (audit, manual_ingest,
backfill, etc). Anything that names an AWS resource lives in
`etl.common.aws` instead.

Every constant in this file may need to change when the
WAM team renames a spreadsheet column or reshuffles their Drive layout!
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Tuple

# COEQWAL Shared Drive layout: scenario folders live four levels deep under
# the remote root. id-mode rows bypass this entirely (the folder URL
# resolves to the deepest level directly), so this only affects path-mode
# rows that fall back to using `GoogleDriveFolderName` / DV_Path root. If a
# Drive restructure lands scenario folders elsewhere, set this to "" (or
# the new parent path) and re-run. Developers can also override per-row by
# pasting the `/folders/<id>` URL into `ModelFilesLink` for any one row.
# id-mode always wins over path-mode.
DRIVE_SCENARIO_PREFIX = "Research Teams/Water Allocation Modeling/CalSim3_Model_Runs/Scenarios"

# WAM team source spreadsheet (Google Sheets). Recorded inside each
# scenario's `ingest_record.json` under `source.spreadsheet_url` so that a
# reader of the ingest record later (months from now, in S3, with no
# context) can find the upstream sheet a scenario row came from. Also
# surfaced in operator error messages when the working CSV is missing.
SPREADSHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1pzbVx191VYXgHcZNhAqJEKNn3lN8GCZo/edit?gid=371742646#gid=371742646"
)

# Local file paths. Reference is the download from the WAM sheet.
# Working is the operator-editable copy with `pinned_*`, `download_status`,
# and `notes` columns appended. Both are tracked in git.
REFERENCE_CSV_PATH = "etl/ingestion/scenario_listing/model_run_file_source.csv"
WORKING_CSV_PATH = "etl/ingestion/scenario_listing/model_run_file_source_working.csv"

# Default directory for the per-run ingest state JSON. Gitignored. Override with --output-dir.
#
# `ingest_state.json` is the handoff record from
# `gdrive_bulk_download.py` to `tools/audit.py`. It carries one block per
# stage (`scan`, `download`), each holding the most recent run's per-row
# records. `audit.py` cross-references this with S3 evidence to render
# `audit.md`. Without it, audit.py would have no record of scenarios that
# were skipped during ingest and never reached S3.
#
# Note: `parent.parent` escapes `lib/` so the path resolves to
# `etl/ingestion/audit_reports/`, not `etl/ingestion/lib/audit_reports/`.
DEFAULT_OUTPUT_DIR = Path(__file__).parent.parent / "audit_reports"
INGEST_STATE_PATH = DEFAULT_OUTPUT_DIR / "ingest_state.json"
INGEST_STATE_SCHEMA_VERSION = 2

# Settings
EXCLUDED_SUBFOLDERS = ("archive", "discard", "old", "backup")
SCRIPT_VERSION = "2.0.0"
INGEST_RECORD_SCHEMA_VERSION = 1
RCLONE_REMOTE = "gdrive"

# `RE_` prefix marks this as a compiled regular expression (Python convention).
# Pulls a Drive folder ID out of a URL of the form
# `https://drive.google.com/drive/folders/<id>?...`.
RE_FOLDER_ID = re.compile(r"/folders/([A-Za-z0-9_-]+)")

# COLUMN_MAP: internal field name -> column name in the working CSV.
# When the spreadsheet renames a column, update the right-hand side only.
# The reader handles two standard transformations:
#   drive_folder_url -> drive_folder_id (regex-extract folder ID)
#   dv_path / sv_path -> dv_filename / sv_filename (basename)
COLUMN_MAP: Dict[str, str] = {
    # Essential columns from the WAM sheet. Script refuses to start if any
    # of these are missing from the header.
    "short_code":           "Index",
    "drive_folder_name":    "GoogleDriveFolderName",
    "drive_folder_url":     "ModelFilesLink",
    "dv_path":              "DV_Path",
    "sv_path":              "SV_Path",

    # Columns added by hand to the working CSV. All optional.
    # Pinned columns are used to disambiguate when a Drive folder has more than one ZIP or trend CSV.
    #
    # `pinned_model_run_zip`: disambiguator when a Drive folder has more than
    #   one ZIP. Set to the exact ZIP filename the script should pick.
    #
    # `pinned_trend_csv`: same idea for the trend report folder. Set to the
    #   exact CSV filename when the trend folder has more than one.
    #
    # `download_status`: informational, with two reserved values.
    #   `gdrive_bulk_download.py` never filters by it (run scope is set on
    #   the CLI via `--scenarios` or `--all`). `refresh_etl_scenarios.py`
    #   excludes a row from `ETL_SCENARIOS` when this column is `skip` or
    #   `retired`. Anything else (blank, `done`, `needs_review`, ...) is
    #   included.
    #
    # `notes`: free-text scratch for the operator. Surfaced in the audit.
    "pinned_model_run_zip": "pinned_model_run_zip",
    "pinned_trend_csv":     "pinned_trend_csv",
    "download_status":      "download_status",
    "notes":                "notes",
}

ESSENTIAL_FIELDS: Tuple[str, ...] = (
    "short_code",
    "drive_folder_name",
    "drive_folder_url",
    "dv_path",
    "sv_path",
)
