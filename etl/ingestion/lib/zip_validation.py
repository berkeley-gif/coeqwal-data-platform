"""ZIP validation: make sure we pick the right DSS files, hash them, build the ingest record.

Three layers, top to bottom:

1. `classify_dss_in_zip` strict match on the basenames declared in the WAM
   spreadsheet (no heuristics, no fallbacks). Raises `IngestionError` if
   the expected SV/DV is missing, multi-match, or shadowed by an excluded
   subfolder.
2. `validate_and_hash_zip` opens the ZIP, runs the classifier, and computes
   SHA-256 plus byte counts for the selected SV/DV and the ZIP itself.
3. `build_ingest_record` assembles the JSON payload that travels with the
   ZIP to S3 and is later consulted by the Lambda and the batch container.

`process_scenario` (in `worker.py`) is the only caller that chains all three.
The tools under `etl/ingestion/tools/` (`manual_ingest`, `backfill_ingest_records`)
reuse `build_ingest_record` on their own ZIP paths.

Why we hash:

The ingest record persists in S3 alongside the ZIP and answers one question
months from now: are these bytes the same ones we audited on ingest day?
Each hash pins a different layer of that answer.

- `zip_sha256`: end-to-end integrity from Drive to Cloud9 to S3 to the
  Batch container. If the container reads a different hash than the
  ingest record declares, the ZIP was altered or corrupted in transit.
- `sv_sha256` / `dv_sha256`: per-entry fingerprints inside the ZIP.
  Catches the case where the ZIP is repacked, the filename stays the
  same, but the contents of the chosen DSS file changed.
- `trend_csv_sha256`: pins the validation reference CSV used at ingest
  time. Lets a later run prove it checked against the same trend CSV
  the operator saw, not a re-export with the same name.
- `source.spreadsheet_row_sha256` (computed upstream by `_sha256_of_row`
  in `utils.py`): pins the working-CSV row the ingest record was built
  from. If the row is later edited, the ingest record still records the
  version that produced it.
"""

from __future__ import annotations

import os
import zipfile
from typing import Any, Dict, List, Optional

from .config import (
    INGEST_RECORD_SCHEMA_VERSION,
    SCRIPT_VERSION,
    SPREADSHEET_URL,
    WORKING_CSV_PATH,
)
from .errors import IngestionError
from .utils import (
    _in_excluded_subfolder,
    _now_iso_utc,
    _operator_tag,
    _sha256_of_file,
    _sha256_of_zip_entry,
)


def classify_dss_in_zip(
    dss_paths: List[str],
    scenario_id: str,
    expected_sv_filename: str,
    expected_dv_filename: str,
) -> Dict[str, Any]:
    """Strict match on the basenames declared in the WAM spreadsheet.

    If either expected basename
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


def validate_and_hash_zip(
    zip_path: str,
    scenario_id: str,
    expected_sv: str,
    expected_dv: str,
) -> Dict[str, Any]:
    """Open a ZIP, run the strict classifier, compute SHAs for the DSS file picks.

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


def build_ingest_record(
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
    """Build the `ingest_record.json` payload that travels with the ZIP."""
    sc = scenario["short_code"]
    dv_base = scenario["dv_filename"]
    sv_base = scenario["sv_filename"]
    return {
        "schema_version": INGEST_RECORD_SCHEMA_VERSION,
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
            # ModelFilesLink URL parsed cleanly. "path" means we fell back to
            # GoogleDriveFolderName / DV_Path root because the URL was missing
            # or unparseable. Recorded so a future reader of the ingest_record
            # can tell whether ingest used the canonical path.
            "access_mode": access_mode,
        },
    }
