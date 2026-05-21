#!/usr/bin/env python3
"""
gdrive_bulk_download.py - automated ingestion path from the COEQWAL Water
Allocation Modeling Team's Google Drive to S3

Reads scenario rows from `etl/ingestion/scenario_listing/model_run_file_source_working.csv` (from the WAM team's spreadsheet),
downloads model run files from Drive via rclone, validates them, and stages
to `s3://<bucket>/staging/scenario_data/<id>/`. Run `promote` to move
`staging/scenario_data/<id>/*` to `ready/<id>/*` once the audit looks clean.
The ZIP PUT under `ready/` is the Lambda trigger that continues the ETL process.

Pre-flight (in `_preflight`, before any row is processed):

  - `rclone` is installed and on PATH.
  - The configured rclone remote (default `gdrive:`) is registered.
    A stale or missing config kills the run with the same fix-up message
    the per-row code would otherwise print N times.
  - The S3 bucket is reachable (creds + bucket name).

  Run by all three subcommands, with the rclone checks always on:
    - `download`        : full check (S3 included).
    - `download --dry-run`: rclone only; skips S3 head_bucket so a Mac
                            without prod AWS creds can iterate on the CSV.
    - `scan`            : rclone only.
    - `scan --local-only`: skipped entirely (touches neither Drive nor S3).

Drive access modes (in `_resolve_drive_access`, used by both `scan` and
`download`):

  - `id`:   ModelFilesLink parsed cleanly to a /folders/<id> URL. Listings
            and copies pass `--drive-root-folder-id=<id>` and use paths
            relative to the folder root (e.g. `Model_Files/`).
  - `path`: ModelFilesLink was a bare folder name or filename (28 of 100
            rows in the WAM sheet today). Falls back to using
            `GoogleDriveFolderName` or the DV_Path root as a full Drive
            path, prepended with `DRIVE_SCENARIO_PREFIX` (the parent
            directory on the Shared Drive where scenario folders live).
            No `--drive-root-folder-id` flag. To override per-row, paste
            the `/folders/<id>` URL into `ModelFilesLink` for that row;
            id-mode always wins.
  - `none`: Neither a parseable URL nor a folder name is available. The
            row is recorded with `NO_DRIVE_ACCESS` in the audit.

  The selected mode is recorded per-row in `audit_report.csv` (column
  `access_mode`) and per-scenario in `sidecar.json` (`ingestion.access_mode`).

Validation runs in two layers. Layer 1 is the spreadsheet itself (paths +
operator columns). Layer 2 is the contents of the ZIP each row points to.
A row that clears Layer 1 but trips Layer 2 is skipped and recorded in the
audit. The run continues for the other rows (skip-not-abort).

Layer 1 - spreadsheet (in `read_scenario_source_csv`):

  1. Essential columns present in the CSV header (short_code,
     drive_folder_name, drive_folder_url, dv_path, sv_path)
  2. Essential values non-empty for every `ready` row
  3. `short_code` unique across all rows
  4. `dv_filename` unique across `ready` rows (cross-paste detector)
  5. `drive_folder_url` parses to a Drive folder ID via /folders/<id>.
     If it does not, the row falls back to path-mode access; ingest only
     fails (with `NO_DRIVE_ACCESS`) when the folder name is also empty.
  6. (Warn) short_code appears in dv_filename basename

  Note: SV uniqueness is NOT checked. SV inputs are often reused across scenarios.

Layer 2 - ZIP contents (in `process_scenario`):

  7. ZIP exists in Drive folder, exactly one ZIP or pinned_model_run_zip set
  8. Expected DV basename present in ZIP, non-excluded subfolder
  9. Expected SV basename present in ZIP, non-excluded subfolder
 10. No multi-match: each expected basename matches at most one ZIP entry
 11. SHA-256 computed for selected DV, SV, ZIP, and (if present) trend CSV
 12. Trend report: optional. Missing, multiple-without-pin, or
     pinned-not-found marks the scenario as `unverified_*` in the audit.
     The scenario still gets staged.
 13. `promote` uploads in order: sidecar.json -> trend csv -> ZIP last

Outputs:
  - s3://<bucket>/staging/scenario_data/<id>/<zip>          (ZIP file)
  - s3://<bucket>/staging/scenario_data/<id>/<trend>.csv    (when present)
  - s3://<bucket>/staging/scenario_data/<id>/sidecar.json   (basenames+hashes)
  - etl/ingestion/output/audit_report.csv                   (per-row CSV view)
  - etl/ingestion/output/audit_state.json                   (per-row JSON state,
                                                             consumed by audit.py)
  - etl/ingestion/audit.md                                  (auto-rendered at
                                                             end of download)

Usage (run from repo root):
  # 1. Pre-flight: list Drive contents per row in the working CSV so the
  #    operator can confirm folders, ZIPs, and trend CSVs are reachable
  #    before committing to a download run. No S3 writes.
  python etl/ingestion/gdrive_bulk_download.py scan

  # 2. Download + validate + stage to staging/scenario_data/<id>/.
  #    Audit auto-renders at the end. Inspect etl/ingestion/audit.md.
  python etl/ingestion/gdrive_bulk_download.py download

  # 3. Promote everything staged to ready/<id>/. The ZIP PUT under ready/
  #    is what the Lambda watches, so this is the step that releases the
  #    batch into the extraction pipeline.
  python etl/ingestion/gdrive_bulk_download.py promote

  # Promote a subset (recovery, smoke test of a single scenario):
  python etl/ingestion/gdrive_bulk_download.py promote --scenarios s0020,s0021
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

# Make `from etl.common import X` work when this script is invoked as
# `python etl/ingestion/gdrive_bulk_download.py` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from etl.common import (  # noqa: E402
    AWS_REGION,
    BATCH_JOB_DEFINITION as JOB_DEFINITION,
    BATCH_QUEUE as JOB_QUEUE,
    DEFAULT_S3_BUCKET,
    READY_PREFIX,
    SCENARIO_PREFIX as SCENARIO_RUN_PREFIX,
    STAGING_PREFIX,
)

# ---------------------------------------------------------------------------
# Operator-tweakable constants (script-local, not shared with other ETL code)
# ---------------------------------------------------------------------------
# Shared constants now live in `etl/common/aws.py` and `etl/common/s3_paths.py`
# and are imported above. Everything below is specific to this script.

# COEQWAL Shared Drive layout: scenario folders live four levels deep under
# the rclone remote root. id-mode rows bypass this entirely (the folder URL
# resolves to the deepest level directly), so this only affects path-mode
# rows that fall back to using `GoogleDriveFolderName` / DV_Path root. If a
# Drive restructure lands scenario folders elsewhere, set this to "" (or
# the new parent path) and re-run. Operators can also override per-row by
# pasting the `/folders/<id>` URL into `ModelFilesLink` for any one row;
# id-mode always wins over path-mode.
DRIVE_SCENARIO_PREFIX = "Research Teams/Water Allocation Modeling/CalSim3_Model_Runs/Scenarios"

# WAM team source spreadsheet (Google Sheets). We record this URL inside
# each scenario's `sidecar.json` under `source.spreadsheet_url` so that a
# reader of the sidecar later (months from now, in S3, with no context)
# can find the upstream sheet a scenario row came from. Also surfaced in
# operator error messages when the working CSV is missing.
SPREADSHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1pzbVx191VYXgHcZNhAqJEKNn3lN8GCZo/edit?gid=371742646#gid=371742646"
)

# Local file paths. Reference is the original download from the WAM sheet.
# Working is the operator-editable copy with `pinned_*`, `download_status`,
# and `notes` columns appended. Both are tracked in git.
REFERENCE_CSV_PATH = "etl/ingestion/scenario_listing/model_run_file_source.csv"
WORKING_CSV_PATH = "etl/ingestion/scenario_listing/model_run_file_source_working.csv"

# Default output directory for audit CSVs and per-run state. Gitignored
# via the umbrella `etl/**/output/` rule. Override with
# --output-dir. The audit_state JSON written here is the handoff record
# audit.py reads to know what gdrive_bulk_download.py saw on its most
# recent run (including scenarios that were skipped and never reached S3).
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "output"
AUDIT_STATE_PATH = DEFAULT_OUTPUT_DIR / "audit_state.json"

# Behavior knobs
EXCLUDED_SUBFOLDERS = ("archive", "discard", "old", "backup")
SCRIPT_VERSION = "2.0.0"
SIDECAR_SCHEMA_VERSION = 1
RCLONE_REMOTE = "gdrive"

# `RE_` prefix marks this as a compiled regular expression (Python convention).
# Pulls a Drive folder ID out of a URL of the form
# `https://drive.google.com/drive/folders/<id>?...`.
RE_FOLDER_ID = re.compile(r"/folders/([A-Za-z0-9_-]+)")

# ---------------------------------------------------------------------------
# COLUMN_MAP: internal field name -> column name in the working CSV.
# When the spreadsheet renames a column, update the right-hand side only.
# The reader handles two standard transformations:
#   - drive_folder_url -> drive_folder_id (regex-extract folder ID)
#   - dv_path / sv_path -> dv_filename / sv_filename (basename)
# ---------------------------------------------------------------------------
COLUMN_MAP: Dict[str, str] = {
    # Essential columns from the WAM sheet. Script refuses to start if any
    # of these are missing from the header.
    "short_code":           "Index",
    "drive_folder_name":    "GoogleDriveFolderName",
    "drive_folder_url":     "ModelFilesLink",
    "dv_path":              "DV_Path",
    "sv_path":              "SV_Path",

    # Operator-managed columns added by hand to the working CSV. All optional.
    # `pinned_model_run_zip`: disambiguator when a Drive folder has more than
    #   one ZIP. Set to the exact ZIP filename the script should pick.
    # `pinned_trend_csv`: same idea for the trend report folder. Set to the
    #   exact CSV filename when the trend folder has more than one.
    # `download_status`: informational only. Recorded in the audit per
    #   row so operators can flag rows for themselves (e.g. `needs_review`,
    #   `skip`), but the script never filters by it. Run scope is set
    #   explicitly on the CLI via `--scenarios` or `--all`.
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

log = logging.getLogger("gdrive_bulk_download")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class IngestionError(Exception):
    """Per-scenario recoverable error. Captured in the audit, never aborts the run."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _norm_for_match(path: str) -> str:
    norm = path.replace("\\", "/").lstrip("./").lower()
    return f"/{norm}/"


def _in_excluded_subfolder(path: str) -> bool:
    parts = path.replace("\\", "/").lower().split("/")
    return any(part in EXCLUDED_SUBFOLDERS for part in parts)


def _basename_of(path: str) -> str:
    """Basename, treating both forward and back slashes as separators."""
    if not path:
        return ""
    return path.replace("\\", "/").rsplit("/", 1)[-1]


def _sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_of_zip_entry(zf: zipfile.ZipFile, name: str) -> Tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with zf.open(name) as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_of_row(row: Dict[str, Any]) -> str:
    """Canonical hash of a spreadsheet row's payload, for the sidecar provenance."""
    canonical = json.dumps(row, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _operator_tag() -> str:
    user = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
    try:
        host = socket.gethostname()
    except OSError:
        host = "unknown"
    return f"{user}@{host}"


# ---------------------------------------------------------------------------
# Strict spreadsheet-driven DSS classifier (Layer 2)
# ---------------------------------------------------------------------------
def classify_dss_in_zip(
    dss_paths: List[str],
    scenario_id: str,
    expected_sv_filename: str,
    expected_dv_filename: str,
) -> Dict[str, Any]:
    """Strict match on the basenames declared in the WAM spreadsheet.

    No heuristics, no fallbacks, no overrides. If either expected basename
    is empty, or the ZIP doesn't contain it, or it appears in multiple
    non-excluded paths, raise IngestionError. The caller wraps in try/except
    and records the failure in the audit.
    """
    if not expected_sv_filename:
        raise IngestionError(
            "MISSING_EXPECTED_SV",
            f"[{scenario_id}] expected_sv_filename is empty - check SV_Path in working CSV",
        )
    if not expected_dv_filename:
        raise IngestionError(
            "MISSING_EXPECTED_DV",
            f"[{scenario_id}] expected_dv_filename is empty - check DV_Path in working CSV",
        )

    expected_sv = expected_sv_filename.lower()
    expected_dv = expected_dv_filename.lower()

    sv_matches: List[str] = []
    dv_matches: List[str] = []
    skipped: List[str] = []

    for p in dss_paths:
        if _in_excluded_subfolder(p):
            skipped.append(p)
            continue
        b = os.path.basename(p).lower()
        if b == expected_sv:
            sv_matches.append(p)
        if b == expected_dv:
            dv_matches.append(p)

    available = sorted({os.path.basename(p) for p in dss_paths if not _in_excluded_subfolder(p)})

    if not sv_matches:
        raise IngestionError(
            "EXPECTED_SV_NOT_IN_ZIP",
            f"[{scenario_id}] expected SV '{expected_sv_filename}' not found in ZIP. "
            f"Available DSS basenames: {available}",
        )
    if len(sv_matches) > 1:
        raise IngestionError(
            "MULTI_MATCH_SV",
            f"[{scenario_id}] expected SV '{expected_sv_filename}' matched "
            f"{len(sv_matches)} non-excluded paths: {sv_matches}",
        )

    if not dv_matches:
        raise IngestionError(
            "EXPECTED_DV_NOT_IN_ZIP",
            f"[{scenario_id}] expected DV '{expected_dv_filename}' not found in ZIP. "
            f"Available DSS basenames: {available}",
        )
    if len(dv_matches) > 1:
        raise IngestionError(
            "MULTI_MATCH_DV",
            f"[{scenario_id}] expected DV '{expected_dv_filename}' matched "
            f"{len(dv_matches)} non-excluded paths: {dv_matches}",
        )

    return {
        "sv_selected": sv_matches[0],
        "dv_selected": dv_matches[0],
        "sv_reason": "spreadsheet (SV_Path)",
        "dv_reason": "spreadsheet (DV_Path)",
        "sv_candidates": sv_matches,
        "dv_candidates": dv_matches,
        "skipped": skipped,
        "classification_method": "spreadsheet",
    }


# ---------------------------------------------------------------------------
# Run-level error
# ---------------------------------------------------------------------------
class PreflightError(SystemExit):
    """Run-level error raised when the operator's environment isn't ready
    (rclone missing/misconfigured, OAuth token revoked, S3 bucket
    unreachable). Subclasses SystemExit so it walks cleanly out of `main()`
    without a stack trace, just like the existing bootstrap-error path.
    Distinct from `IngestionError`, which is per-row and recoverable."""


# ---------------------------------------------------------------------------
# rclone helpers
# ---------------------------------------------------------------------------
# rclone stderr substrings that indicate a config-level problem (no remote,
# bad config file, expired/revoked OAuth token). When we see these, the run
# should abort with an actionable message rather than swallow the error and
# mark every row as "MISSING_ZIP" through a folder-not-found-shaped path.
_RCLONE_CONFIG_ERROR_MARKERS = (
    "didn't find section",
    "not found in config file",
    "couldn't find section",
    "Failed to create file system",
    "couldn't decrypt",
    "401 Unauthorized",
    "invalid_grant",
    "Token has been expired or revoked",
)


def _is_rclone_config_error(stderr: str) -> bool:
    s = stderr.lower()
    return any(m.lower() in s for m in _RCLONE_CONFIG_ERROR_MARKERS)


def rclone_lsjson(folder_id: str, subpath: str = "",
                  dirs_only: bool = False,
                  rclone_remote: Optional[str] = None) -> List[Dict]:
    """List contents of a Drive folder via rclone lsjson.

    Two failure shapes:
      - Config-level error (no remote, expired token, etc.): raise
        PreflightError so the whole run aborts with the same kind of
        message as the up-front pre-flight checks. We never want a stale
        rclone config to silently mark every scenario as MISSING_ZIP.
      - Folder-not-found / per-row issue: log a warning and return [].
        The caller surfaces this through the audit (MISSING_ZIP, etc.).
    """
    remote = rclone_remote or RCLONE_REMOTE
    target = f"{remote}:{subpath}"
    cmd = ["rclone", "lsjson", target]
    if folder_id:
        cmd.append(f"--drive-root-folder-id={folder_id}")
    if dirs_only:
        cmd.append("--dirs-only")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if _is_rclone_config_error(stderr):
            raise PreflightError(
                f"\n[rclone] Config-level error talking to '{remote}:': {stderr}\n"
                f"This kills the run because the same error would repeat for every "
                f"scenario. Fix the rclone config and retry:\n"
                f"  rclone listremotes\n"
                f"  rclone config reconnect {remote}:    # if the OAuth token is stale\n"
                f"See etl/README.md (Cloud9 setup -> rclone) for the full walkthrough.\n"
            )
        log.warning("rclone lsjson failed: %s", stderr)
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        log.warning("rclone lsjson returned invalid JSON: %s",
                    result.stdout[:200])
        return []


def rclone_copy_file(folder_id: str, remote_path: str,
                     local_dest_dir: str) -> bool:
    """Download a single file from Drive to a local directory.

    When `folder_id` is non-empty, `remote_path` is interpreted as a subpath
    under that folder (Drive ID-rooted). When `folder_id` is empty,
    `remote_path` is a full path from the rclone remote root (path-mode,
    used for spreadsheet rows that have a `drive_folder_name`/`dv_root` but
    no folder URL).

    Note on output: this function is the one rclone call where we
    deliberately do NOT capture stdout/stderr. The `--progress` flag
    streams a live transfer bar (bytes/sec, ETA, percent) using VT100
    escape sequences, and the only way an operator sees it is if it goes
    straight to the parent terminal. This matches the per-step liveness
    that `etl/statistics/run_all.py` provides via its Popen+tee pattern,
    just for the 200 MB ZIP download specifically. With `--workers > 1`,
    multiple progress bars will interleave (each rclone run draws its
    own block); the per-line `[<scenario>]` log prefixes from this
    script are still legible above and below those blocks. A failed
    `rclone copy` writes its error to stderr live too; we just check
    the exit code below.
    """
    target = f"{RCLONE_REMOTE}:{remote_path}"
    cmd = ["rclone", "copy", target, local_dest_dir]
    if folder_id:
        cmd.append(f"--drive-root-folder-id={folder_id}")
    cmd.append("--progress")
    try:
        result = subprocess.run(cmd, timeout=3600)
    except subprocess.TimeoutExpired:
        log.error("rclone copy timed out after 3600s for %s", remote_path)
        return False
    if result.returncode != 0:
        log.error("rclone copy failed (exit %d) -- see rclone output above",
                  result.returncode)
        return False
    return True


def rclone_cat(folder_id: str, remote_path: str) -> Optional[bytes]:
    """Read a small file from Drive into memory.

    When `folder_id` is non-empty, `remote_path` is interpreted as a subpath
    under that folder. When `folder_id` is empty, `remote_path` is a full
    path from the rclone remote root (path-mode).
    """
    target = f"{RCLONE_REMOTE}:{remote_path}"
    cmd = ["rclone", "cat", target]
    if folder_id:
        cmd.append(f"--drive-root-folder-id={folder_id}")
    result = subprocess.run(cmd, capture_output=True, timeout=300)
    if result.returncode != 0:
        log.error("rclone cat failed: %s", result.stderr.decode().strip())
        return None
    return result.stdout


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
# Run before iterating scenarios. The goal is to fail fast with a single,
# actionable message if the operator's environment isn't ready, rather than
# discovering the same problem N times across the per-row loop.

def _preflight_rclone_installed() -> None:
    """Confirm `rclone` is on PATH."""
    try:
        result = subprocess.run(
            ["rclone", "version"], capture_output=True, text=True, timeout=10,
        )
    except FileNotFoundError:
        raise PreflightError(
            "\n[preflight] rclone is not installed (or not on PATH).\n"
            "Install it on Cloud9 with:\n"
            "  curl https://rclone.org/install.sh | sudo bash\n"
        )
    if result.returncode != 0:
        raise PreflightError(
            f"\n[preflight] `rclone version` failed with exit code {result.returncode}:\n"
            f"{(result.stderr or result.stdout).strip()}\n"
        )


def _preflight_rclone_remote(remote: str) -> None:
    """Confirm the configured rclone remote is registered."""
    try:
        result = subprocess.run(
            ["rclone", "listremotes"], capture_output=True, text=True, timeout=10,
        )
    except FileNotFoundError:
        # _preflight_rclone_installed runs first, so this is unreachable in
        # normal flow. Re-raise the same actionable message here for safety.
        raise PreflightError(
            "\n[preflight] rclone is not installed (or not on PATH).\n"
            "Install it on Cloud9 with:\n"
            "  curl https://rclone.org/install.sh | sudo bash\n"
        )
    if result.returncode != 0:
        raise PreflightError(
            f"\n[preflight] `rclone listremotes` failed with exit code {result.returncode}:\n"
            f"{(result.stderr or result.stdout).strip()}\n"
        )
    remotes = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    expected = f"{remote}:"
    if expected not in remotes:
        raise PreflightError(
            f"\n[preflight] rclone remote '{expected}' is not configured.\n"
            f"Configured remotes: {sorted(remotes) or '(none)'}\n\n"
            f"On Cloud9, copy the rclone config from a Mac that has already authenticated:\n"
            f"  # On the Mac:  cat ~/.config/rclone/rclone.conf\n"
            f"  # On Cloud9:   mkdir -p ~/.config/rclone && nano ~/.config/rclone/rclone.conf\n"
            f"See etl/README.md (Cloud9 setup -> rclone) for the full walkthrough.\n"
        )


def _preflight_s3_bucket(s3_bucket: str) -> None:
    """Confirm AWS credentials are present and the target bucket is reachable."""
    try:
        s3 = boto3.client("s3")
        s3.head_bucket(Bucket=s3_bucket)
    except Exception as e:
        raise PreflightError(
            f"\n[preflight] S3 bucket '{s3_bucket}' is not reachable: {e}\n"
            f"Check AWS credentials and the bucket name:\n"
            f"  aws sts get-caller-identity\n"
            f"  aws s3 ls s3://{s3_bucket}/\n"
        )


def _preflight(rclone_remote: str, s3_bucket: str = "",
               include_s3: bool = True) -> None:
    """Run all pre-flight checks. Raises PreflightError (a SystemExit) on failure.

    `include_s3=False` runs only the rclone checks. Used by `scan` (which
    never touches S3) and by `download --dry-run` (which lists Drive but
    never writes to S3, so it doesn't need head_bucket either - useful for
    iterating from a Mac that doesn't have AWS creds for the prod bucket).
    """
    _preflight_rclone_installed()
    _preflight_rclone_remote(rclone_remote)
    if include_s3:
        _preflight_s3_bucket(s3_bucket)
        log.info("Pre-flight checks passed (rclone=%s:, s3=%s).",
                 rclone_remote, s3_bucket)
    else:
        log.info("Pre-flight checks passed (rclone=%s:, S3 skipped).", rclone_remote)


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------
def _parse_scenarios(values) -> set:
    """Normalize a `--scenarios` argument into a set of lowercase short codes.

    Accepts whatever the operator pasted into the shell. Splits on whitespace
    and commas in any combination. Useful when copying a column straight
    from a spreadsheet (newline-separated) or a comma-separated string from
    elsewhere.

    Examples (all yield {"s0070", "s0071", "s0072"}):
      ["s0070", "s0071", "s0072"]           # nargs="*" with spaces
      ["s0070,s0071,s0072"]                 # comma-pasted into one shell token
      ["s0070\\ns0071\\ns0072"]             # newline-pasted, quoted
      ["s0070, s0071", "s0072"]             # mixed
    """
    if values is None:
        return set()
    if isinstance(values, str):
        values = [values]
    out = set()
    for v in values:
        for tok in re.split(r"[\s,]+", v.strip()):
            if tok:
                out.add(tok.lower())
    return out


# ---------------------------------------------------------------------------
# CSV reader!
# ---------------------------------------------------------------------------
def _bootstrap_error_message(path: str) -> str:
    """Build the message printed when the working CSV is missing on disk.

    The working CSV is intentionally not auto-created. The operator must
    `cp` it from the pristine reference copy so they remember the operator
    columns (`pinned_*`, `notes`) exist and may need values for the rows
    they are loading. This function builds the message the script prints
    to stderr before exiting; it never writes anything itself.
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
            f"Either restore the columns in the CSV, or update COLUMN_MAP at the top "
            f"of {__file__}.\n"
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
    # checked. The same SV input is often   reused across scenarios on purpose.
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


# ---------------------------------------------------------------------------
# ZIP validation
# ---------------------------------------------------------------------------
def validate_and_hash_zip(
    zip_path: str,
    scenario_id: str,
    expected_sv: str,
    expected_dv: str,
) -> Dict[str, Any]:
    """Open a ZIP, run the strict classifier, compute SHAs for the dss file picks.

    Returns a dict with the classification result plus zip-level metadata
    (basename, sha256, filesize) and per-file sha256/filesize for DV/SV.

    Raises IngestionError on any per-scenario failure. The caller is expected
    to catch and surface it through the audit.
    """
    try:
        zf = zipfile.ZipFile(zip_path, "r")
    except zipfile.BadZipFile as e:
        raise IngestionError("BAD_ZIP", f"[{scenario_id}] ZIP is corrupt: {e}")

    try:
        all_names = zf.namelist()
        dss_paths = [n for n in all_names if n.lower().endswith(".dss")]
        if not dss_paths:
            raise IngestionError("NO_DSS_IN_ZIP", f"[{scenario_id}] ZIP contains no .dss files")

        classification = classify_dss_in_zip(dss_paths, scenario_id, expected_sv, expected_dv)

        sv_path = classification["sv_selected"]
        dv_path = classification["dv_selected"]
        sv_sha, sv_size = _sha256_of_zip_entry(zf, sv_path)
        dv_sha, dv_size = _sha256_of_zip_entry(zf, dv_path)
    finally:
        zf.close()

    zip_sha = _sha256_of_file(zip_path)
    zip_size = os.path.getsize(zip_path)
    zip_basename = os.path.basename(zip_path)

    return {
        "classification": classification,
        "zip_basename": zip_basename,
        "zip_sha256": zip_sha,
        "zip_filesize_bytes": zip_size,
        "sv_sha256": sv_sha,
        "sv_filesize_bytes": sv_size,
        "dv_sha256": dv_sha,
        "dv_filesize_bytes": dv_size,
        "sv_path_in_zip": sv_path,
        "dv_path_in_zip": dv_path,
    }


# ---------------------------------------------------------------------------
# sidecar.json builder
# ---------------------------------------------------------------------------
def build_sidecar(
    scenario: Dict[str, Any],
    zip_basename: str,
    zip_sha256: str,
    zip_filesize: int,
    sv_path_in_zip: str,
    sv_sha256: str,
    sv_filesize: int,
    dv_path_in_zip: str,
    dv_sha256: str,
    dv_filesize: int,
    trend_csv_basename: Optional[str],
    trend_csv_sha256: Optional[str],
    access_mode: str = "id",
) -> Dict[str, Any]:
    """Build the sidecar.json payload that travels with the ZIP."""
    sc = scenario["short_code"]
    dv_base = scenario["dv_filename"]
    sv_base = scenario["sv_filename"]
    return {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "short_code": sc,
        "expected_sv_filename": sv_base,
        "expected_dv_filename": dv_base,
        "expected_sv_path_in_zip": sv_path_in_zip,
        "expected_dv_path_in_zip": dv_path_in_zip,
        "sv_sha256": sv_sha256,
        "dv_sha256": dv_sha256,
        "sv_filesize_bytes": sv_filesize,
        "dv_filesize_bytes": dv_filesize,
        "zip_basename": zip_basename,
        "zip_sha256": zip_sha256,
        "zip_filesize_bytes": zip_filesize,
        "trend_csv_basename": trend_csv_basename,
        "trend_csv_sha256": trend_csv_sha256,
        "convention_check": {
            "short_code_in_dv_basename": sc.lower() in (dv_base or "").lower(),
            "short_code_in_sv_basename": sc.lower() in (sv_base or "").lower(),
        },
        "source": {
            "spreadsheet_url": SPREADSHEET_URL,
            "spreadsheet_row_sha256": scenario.get("_row_sha256", ""),
            "spreadsheet_file": WORKING_CSV_PATH,
        },
        "ingestion": {
            "path": "automated",
            "script": "gdrive_bulk_download.py",
            "script_version": SCRIPT_VERSION,
            "operator": _operator_tag(),
            "ingested_at_utc": _now_iso_utc(),
            # How the script reached this row's Drive folder. "id" means the
            # ModelFilesLink URL parsed cleanly; "path" means we fell back to
            # GoogleDriveFolderName / DV_Path root because the URL was missing
            # or unparseable. Recorded so a future reader of the sidecar can
            # tell whether ingest used the canonical path.
            "access_mode": access_mode,
        },
    }


# ---------------------------------------------------------------------------
# Per-scenario worker
# ---------------------------------------------------------------------------
_print_lock = threading.Lock()


def _audit_row_template(scenario: Dict[str, Any]) -> Dict[str, Any]:
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
        # ----- Resolve how to reach this scenario on Drive -----------------
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

        # ----- List Model_Files for ZIPs ----------------------------------
        if access_mode == "id":
            log.info("[%s] Listing Drive folder (ID: %s) ...", sc, folder_id[:12])
        else:
            log.info("[%s] Listing Drive path: %s ...", sc, model_path)
        all_model_files = rclone_lsjson(folder_id, model_path)
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

        # ----- List Data_Extraction for trend CSVs ------------------------
        # The trend report is optional. It is used downstream to verify the
        # extracted data. If we cannot pick a
        # single CSV unambiguously, we still stage the scenario but mark its
        # `verification_status` as `unverified_*`. The audit reports this.
        trend_files = rclone_lsjson(folder_id, trend_path)
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

        # ----- Download ZIP, validate, hash -------------------------------
        tmp_dir = tempfile.mkdtemp(prefix=f"gdrive_{sc}_")
        zip_filename = selected_zip["Name"]
        zip_local = os.path.join(tmp_dir, zip_filename)
        try:
            log.info("[%s] Downloading ZIP: %s (%.1f MB) ...", sc,
                     zip_filename, size_bytes / (1024 * 1024))
            ok = rclone_copy_file(folder_id, f"{model_path}{zip_filename}", tmp_dir)
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


            # ----- Download trend CSV (optional) and hash -----------------
            csv_name: Optional[str] = None
            csv_bytes: Optional[bytes] = None
            trend_sha: Optional[str] = None
            if selected_csv is not None:
                csv_name = selected_csv["Name"]
                log.info("[%s] Downloading trend CSV: %s ...", sc, csv_name)
                csv_bytes = rclone_cat(folder_id, f"{trend_path}{csv_name}")
                if not csv_bytes:
                    # Download failure for a present-but-unreadable trend CSV
                    # Marks the scenario as unverified.
                    log.warning(
                        "[%s] Trend CSV download failed; continuing as unverified.", sc,
                    )
                    csv_name = None
                    csv_bytes = None
                    row["verification_status"] = "unverified_no_trend"
                else:
                    trend_sha = _sha256_of_bytes(csv_bytes)
                    row["trend_csv_sha256"] = trend_sha

            # ----- Build sidecar.json ------------------------------------
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

            # ----- Upload to staging --------------------------------------
            # The Lambda trigger is the ready/<id>/<zip> PUT, which happens
            # during `promote`. This uploads to staging/scenario_data/<id>/ here
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


# ---------------------------------------------------------------------------
# Audit report + audit_state.json
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

    Two on-disk artifacts come out of this function. They contain the same
    underlying per-row records, but in different shapes for different
    consumers:

    1. `audit_report.csv` (at `local_path`, default
       `etl/ingestion/output/audit_report.csv`). Flat tabular view, one
       row per scenario. Open it in a spreadsheet to eyeball a run at a
       glance. Also uploaded to `s3://<bucket>/<STAGING_PREFIX>/audit_report.csv`.

    2. `audit_state.json` (at `AUDIT_STATE_PATH`, default
       `etl/ingestion/output/audit_state.json`). Structured nested JSON,
       schema-versioned. Consumed by `etl/ingestion/audit.py` to render
       `etl/ingestion/audit.md`. This is the only handoff between this
       script (which knows about local-only failures that never reached
       S3) and audit.py (which walks S3 for sidecar/lambda/classification
       state). Without this file, audit.py would have no record of
       scenarios that were skipped during ingest.

    Both files are gitignored under `etl/**/output/` and are regeneratable.
    Re-running `gdrive_bulk_download.py download` (with the same or a
    different --scenarios filter) rewrites them in place. Long-term
    history lives in S3 (sidecars + lambda_status + classification) and
    in the tracked `audit.md` that audit.py renders.
    """
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

    # ----- Console summary -----------------------------------------------
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
        print("  SCENARIOS REQUIRING REVIEW (see audit.md after running etl/ingestion/audit.py):")
        print("  " + "-" * 96)
        for r in attention:
            print(f"  {r.get('scenario_id', '')}: {r.get('error_code', r.get('validation_status', ''))}")
            if r.get("error_message"):
                print(f"    {r['error_message']}")

    print("=" * 100 + "\n")


# ---------------------------------------------------------------------------
# download subcommand
# ---------------------------------------------------------------------------
def cmd_download(args):
    """Download, validate, and stage scenarios to S3."""
    global RCLONE_REMOTE
    RCLONE_REMOTE = args.rclone_remote

    # Pre-flight: rclone installed, remote configured, S3 bucket reachable.
    # Fails fast (SystemExit) with an actionable message before we open the
    # CSV or build the per-row plan, so an unconfigured Cloud9 doesn't waste
    # an operator's time discovering the same error N times. `--dry-run` skips
    # the S3 head_bucket so a Mac iterating on the working CSV without prod
    # AWS creds can still exercise the full Drive-listing path.
    _preflight(RCLONE_REMOTE, args.s3_bucket, include_s3=not args.dry_run)

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
                                   args.dry_run, RCLONE_REMOTE)
            results.append(row)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(process_scenario, sc, s3_client,
                            args.s3_bucket, args.dry_run, RCLONE_REMOTE): sc
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
            import sys as _sys
            _sys.path.insert(0, str(Path(__file__).parent))
            from audit import regenerate_audit  # type: ignore
            regenerate_audit(args.s3_bucket)
        except Exception as e:
            log.warning(
                "Audit auto-render failed (%s). Re-run manually: "
                "python etl/ingestion/audit.py",
                e,
            )


# ---------------------------------------------------------------------------
# promote subcommand (timing fix #1: upload order)
# ---------------------------------------------------------------------------
# The ZIP is the Lambda trigger. Anything the Lambda might read must be at
# rest in ready/<id>/ BEFORE the ZIP arrives. Order is non-negotiable.
PROMOTE_ORDER_PREFIXES = ("sidecar.json",)
PROMOTE_ORDER_SUFFIXES = (".csv", ".zip")


def _sort_promote_keys(keys: List[str]) -> List[str]:
    """Sort keys for promote so sidecar -> trend CSV -> ZIP is the upload order.

    Anything else (extra docs etc.) lands after the sidecar and before the CSV/ZIP,
    in alphabetical order.
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


def _resolve_drive_access(scenario: Dict) -> tuple:
    """Determine how to access a scenario's Drive folder.

    Returns (folder_id_or_empty, model_files_path, trend_path, access_mode).
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
    # `gdrive:` is rooted at the COEQWAL Shared Drive top level; without the
    # prefix, rclone returns "directory not found" for the per-scenario path.
    prefix = f"{DRIVE_SCENARIO_PREFIX.rstrip('/')}/" if DRIVE_SCENARIO_PREFIX else ""

    return (
        "",
        f"{prefix}{base}/Model_Files/",
        f"{prefix}{base}/Data_Extraction/Variables_From_trend_report_variables_v5/",
        "path",
    )


def scan_scenario(scenario: Dict, rclone_remote: str) -> Dict[str, Any]:
    """List Drive contents for one scenario, report zip/csv counts."""
    sc = scenario["short_code"]
    folder_id = scenario["drive_folder_id"]
    folder_name = scenario["drive_folder_name"]
    dv_root = scenario["dv_root"]

    row: Dict[str, Any] = {
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
    never downloads files.     Use it as a pre-flight on a freshly bootstrapped
    working CSV, or after editing rows.
    """
    global RCLONE_REMOTE
    RCLONE_REMOTE = args.rclone_remote

    # Pre-flight (rclone only - scan never touches S3). Skipped for
    # `--local-only` because that mode bypasses Drive entirely.
    if not args.local_only:
        _preflight(RCLONE_REMOTE, include_s3=False)

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
            row = scan_scenario(sc, RCLONE_REMOTE)
            results.append(row)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(scan_scenario, sc, RCLONE_REMOTE): sc
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
# MAIN
# ---------------------------------------------------------------------------
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    parser = argparse.ArgumentParser(
        description="Bulk download model runs from Google Drive to S3 via rclone"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    dl = sub.add_parser("download", help="Download, validate, and stage to S3")
    dl.add_argument("--listing-csv", default=WORKING_CSV_PATH,
                    help=f"Path to working CSV (default: {WORKING_CSV_PATH})")
    dl.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET,
                    help=f"S3 bucket for staging (default: {DEFAULT_S3_BUCKET})")
    dl.add_argument("--workers", type=int, default=4,
                    help="Number of concurrent download workers (default: 4)")
    dl.add_argument("--scenarios", nargs="*",
                    help="Scenario short codes to process. Whitespace or comma-separated; "
                         "newlines from a spreadsheet column paste also work. "
                         "Example: --scenarios s0042 s0043, or "
                         "--scenarios \"$(pbpaste)\" on macOS. "
                         "Either --scenarios or --all is required.")
    dl.add_argument("--rclone-remote", default="gdrive",
                    help="Name of the rclone remote (default: gdrive)")
    dl.add_argument("--all", action="store_true",
                    help="Process every row in the working CSV. "
                         "Either --scenarios or --all is required.")
    dl.add_argument("--dry-run", action="store_true",
                    help="List files without downloading")
    dl.add_argument("--output-dir", default=None,
                    help=f"Directory for audit_report.csv and audit_state.json "
                         f"(default: {DEFAULT_OUTPUT_DIR}). Auto-created if missing.")
    dl.add_argument("--skip-audit", action="store_true",
                    help="Do not auto-render audit.md at the end of the run. "
                         "Re-run `python etl/ingestion/audit.py` manually later.")

    pr = sub.add_parser("promote",
                        help=f"Copy staged files from {STAGING_PREFIX}/<id>/ to "
                             f"{READY_PREFIX}/<id>/. "
                             f"Upload order: sidecar.json -> trend csv -> ZIP last.")
    pr.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET,
                    help=f"S3 bucket (default: {DEFAULT_S3_BUCKET})")
    pr.add_argument("--scenarios", nargs="*",
                    help="Scenario short codes to promote. Whitespace or comma-separated; "
                         "newlines from a spreadsheet paste also work. "
                         "Example: --scenarios s0042 s0043. "
                         "Default: every scenario currently in staging.")
    pr.add_argument("--dry-run", action="store_true",
                    help="Print the planned copy order without copying. Use this before "
                         "a real promote to see what will fire Lambda")

    sc = sub.add_parser("scan",
                        help="Scan Drive contents using the working CSV")
    sc.add_argument("--listing-csv", default=WORKING_CSV_PATH,
                    help=f"Path to working CSV (default: {WORKING_CSV_PATH})")
    sc.add_argument("--rclone-remote", default="gdrive",
                    help="Name of the rclone remote (default: gdrive)")
    sc.add_argument("--workers", type=int, default=4,
                    help="Number of concurrent scan workers (default: 4)")
    sc.add_argument("--scenarios", nargs="*",
                    help="Scenario short codes to scan. Whitespace or comma-separated; "
                         "newlines from a spreadsheet column paste also work. "
                         "Example: --scenarios s0042 s0043, or "
                         "--scenarios \"$(pbpaste)\" on macOS. "
                         "Either --scenarios or --all is required.")
    sc.add_argument("--all", action="store_true",
                    help="Scan every row in the working CSV. "
                         "Either --scenarios or --all is required.")
    sc.add_argument("--local-only", action="store_true",
                    help="Parse CSV and write manifest without Drive access")
    sc.add_argument("--output-dir", default=None,
                    help=f"Directory for scan_audit.csv (default: "
                         f"{DEFAULT_OUTPUT_DIR}). Auto-created if missing.")

    args = parser.parse_args()
    if args.command == "download":
        cmd_download(args)
    elif args.command == "promote":
        cmd_promote(args)
    elif args.command == "scan":
        cmd_scan(args)


if __name__ == "__main__":
    main()
