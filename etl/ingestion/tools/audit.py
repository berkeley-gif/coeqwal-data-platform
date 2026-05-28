#!/usr/bin/env python3
"""
audit.py - renders `etl/ingestion/audit.md` from the local ingestion state
and S3.

What this script does:
  1. Reads the `download` block of `<DEFAULT_OUTPUT_DIR>/ingest_state.json`
     (written by `gdrive_bulk_download.py download`).
  2. Walks `s3://<bucket>/scenario/*/` and reads two JSON files per
     scenario: `ingest_record.json` (what the ingestion side declared)
     and `extract_record.json` (what the Batch container produced).
  3. Cross-references the two to produce a single Markdown report at
     `etl/ingestion/audit.md`.

Report sections:
  - Run summary
  - What needs your attention (ingest skips, missing ingest record,
    extraction failures, validation failures)
  - Unverified scenarios (informational, e.g. missing trend report)
  - Active scenarios table
  - Per-scenario details (expanded for non-OK rows)

This script never modifies S3. It is safe to run anytime. It is also
called automatically at the end of `gdrive_bulk_download.py download`
(pass `--skip-audit` there to defer).

Usage:
  python etl/ingestion/tools/audit.py [--s3-bucket coeqwal-model-run] [--all]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

THIS_FILE = Path(__file__).resolve()
INGESTION_DIR = THIS_FILE.parent.parent   # etl/ingestion/
REPO_ROOT = THIS_FILE.parents[3]          # repo root

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(REPO_ROOT))
from etl.common import (  # noqa: E402
    DEFAULT_S3_BUCKET,
    extract_record_key,
    ingest_record_key,
    read_json_from_s3,
    scenario_prefix,
    scenario_run_prefix,
)
from etl.ingestion.lib.config import INGEST_STATE_PATH  # noqa: E402

# audit.md lives at etl/ingestion/audit.md. The path is not gitignored, so the developer can commit
# the file when they want the team to see the latest digest.
AUDIT_MD_PATH = INGESTION_DIR / "audit.md"

log = logging.getLogger("audit")


# ---------------------------------------------------------------------------
# S3 walker
# ---------------------------------------------------------------------------
def _list_scenario_ids(s3, bucket: str) -> List[str]:
    """List the scenario short_codes under scenario/ in the bucket."""
    paginator = s3.get_paginator("list_objects_v2")
    ids: set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix="scenario/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []) or []:
            prefix = cp.get("Prefix", "")
            # scenario/<id>/
            parts = prefix.strip("/").split("/")
            if len(parts) == 2 and parts[0] == "scenario":
                ids.add(parts[1])
    return sorted(ids)


def _collect_scenario_state(s3, bucket: str, short_code: str) -> Dict[str, Any]:
    """Pull the per-scenario JSON records: ingest (dev side) + extract
    (container side)."""
    ingest_record = read_json_from_s3(
        s3, bucket, ingest_record_key(scenario_prefix(short_code)),
    )
    extract_record = read_json_from_s3(
        s3, bucket, extract_record_key(short_code),
    )
    return {
        "short_code": short_code,
        "ingest_record": ingest_record,
        "extract_record": extract_record,
    }


# ---------------------------------------------------------------------------
# Local ingest state
# ---------------------------------------------------------------------------
def _empty_local_state() -> Dict[str, Any]:
    return {"scenarios": [], "run_at_utc": None, "script_version": None}


def _read_local_state() -> Dict[str, Any]:
    """Read the `download` block of `ingest_state.json`.

    Returns `{"scenarios": [row, ...], "run_at_utc": ..., "script_version": ...}`.
    The renderers in this module want a list of per-row records, so the
    on-disk dict-by-short_code shape is flattened on the way out.
    """
    if not INGEST_STATE_PATH.exists():
        return _empty_local_state()
    try:
        state = json.loads(INGEST_STATE_PATH.read_text())
    except json.JSONDecodeError:
        log.warning("Local ingest_state.json is not valid JSON; treating as empty")
        return _empty_local_state()
    download = state.get("download") or {}
    scenarios = (download.get("scenarios") or {})
    return {
        "scenarios": list(scenarios.values()),
        "run_at_utc": download.get("run_at_utc"),
        "script_version": download.get("script_version"),
    }


# ---------------------------------------------------------------------------
# Failure classification (actionable text per failure kind)
# ---------------------------------------------------------------------------
def _action_for_local_skip(row: Dict[str, Any]) -> str:
    """Return an actionable message for a scenario skipped during ingest."""
    code = row.get("error_code") or row.get("validation_status") or "UNKNOWN"
    sc = row.get("scenario_id", "?")
    msg = row.get("error_message", "") or ""
    if code == "MULTIPLE_ZIPS_NO_PIN":
        return (
            f"Multiple ZIPs for {sc}. Set `pinned_model_run_zip` on the {sc} row "
            f"in the working CSV, then re-run download."
        )
    if code in ("EXPECTED_DV_NOT_IN_ZIP", "EXPECTED_SV_NOT_IN_ZIP"):
        return (
            f"The basename declared in the working CSV does not match any file "
            f"inside the ZIP. Check that DV_Path / SV_Path on the {sc} row "
            f"matches a real file in Drive (or update the CSV to match the ZIP)."
        )
    if code in ("MULTI_MATCH_DV", "MULTI_MATCH_SV"):
        return (
            f"The expected basename matches multiple non-excluded paths inside "
            f"the ZIP for {sc}. Inspect the ZIP; archived copies should live in "
            f"a folder named archive/, discard/, old/, or backup/."
        )
    if code in ("MISSING_EXPECTED_DV", "MISSING_EXPECTED_SV"):
        return (
            f"DV_Path / SV_Path is empty for {sc} in the working CSV. Fill it in "
            f"from the WAM scenario listing spreadsheet."
        )
    if code in ("MISSING_ZIP", "DOWNLOAD_FAILED", "TREND_DOWNLOAD_FAILED",
                "BAD_ZIP", "NO_DSS_IN_ZIP"):
        return (
            f"Drive content issue. Verify the {sc} folder on the WAM Shared "
            f"Drive contains a valid ZIP. Then re-run download for {sc}."
        )
    if code == "NO_DRIVE_ACCESS":
        return (
            f"Cannot reach Drive for {sc}: ModelFilesLink did not parse to a "
            f"folder ID and GoogleDriveFolderName is empty. Set either column "
            f"on the {sc} row in the working CSV, then re-run download."
        )
    return f"Investigate. Message: {msg}"


def _action_for_no_ingest_record(short_code: str) -> str:
    return (
        f"A ZIP exists for {short_code} in S3 but no ingest_record.json is "
        f"alongside it. The Batch job will fail fast. Run:\n"
        f"  python etl/ingestion/tools/manual_ingest.py ingest-record \\\n"
        f"    --short-code {short_code} \\\n"
        f"    --dv-basename '<your DV filename>' \\\n"
        f"    --sv-basename '<your SV filename>' \\\n"
        f"    --compute-hashes --retrigger-batch"
    )


def _action_for_no_trend_report(short_code: str) -> str:
    return (
        f"S3 has a ZIP for {short_code} but no corresponding trend report CSV. "
        f"If you intended to upload one, "
        f"PUT it at s3://{DEFAULT_S3_BUCKET}/{scenario_run_prefix(short_code)}/<file>.csv"
    )


def _action_for_unverified(row: Dict[str, Any]) -> str:
    """Plain-English explanation for an `unverified_*` scenario."""
    sc = row.get("scenario_id", "?")
    status = row.get("verification_status", "")
    if status == "unverified_no_trend":
        return (
            f"{sc} has no trend report CSV in its Drive folder. Extraction is "
            f"still possible, but downstream verification will be skipped. "
            f"Upload a trend CSV to "
            f"Data_Extraction/Variables_From_trend_report_variables_v5/ and "
            f"re-run download for {sc} if verification is wanted."
        )
    if status == "unverified_multi_trend":
        return (
            f"{sc} has multiple trend report CSVs in its Drive folder. Set "
            f"`pinned_trend_csv` on the {sc} row of the working CSV to pick "
            f"one, then re-run download for {sc} if verification is wanted."
        )
    if status == "unverified_pin_missing":
        return (
            f"{sc} has a `pinned_trend_csv` set in the working CSV, but the "
            f"named file is not present in the trend folder. Fix the pin "
            f"or upload the file, then re-run download for {sc} if "
            f"verification is wanted."
        )
    return f"{sc} marked unverified ({status})."


def _action_for_extraction_failure(short_code: str, extract_record: Dict[str, Any]) -> str:
    """Actionable message for a scenario whose Batch run failed or partially
    failed."""
    status = extract_record.get("status", "UNKNOWN")
    targets = extract_record.get("extract_targets", "sv,dv")
    ss = extract_record.get("status_summary", {}) or {}
    parts: List[str] = []
    if status == "SUCCEEDED_PARTIAL":
        missing = []
        if not ss.get("sv_csv_written"):
            missing.append("SV")
        if not ss.get("dv_csv_written"):
            missing.append("DV")
        parts.append(
            f"{short_code}: partial extract (targets={targets}). "
            f"Missing CSV(s): {', '.join(missing) or 'unknown'}."
        )
    else:
        parts.append(f"{short_code}: Batch job ended in status {status}.")
    parts.append(
        "Inspect the CloudWatch logs for the job id below, then re-run:\n"
        f"  bash etl/ingestion/tools/retrigger_extraction.sh --go {short_code}"
    )
    job_id = extract_record.get("job_id")
    if job_id:
        parts.append(f"Batch job id: {job_id}")
    return "\n".join(parts)


def _action_for_validation_failure(short_code: str, extract_record: Dict[str, Any]) -> str:
    """Actionable message for a scenario whose extracted CSV diverged from
    the trend report reference."""
    val = extract_record.get("validation", {}) or {}
    target = val.get("target", "?")
    cells = val.get("mismatch_cells", 0)
    cols = val.get("mismatch_columns", 0)
    csv_key = val.get("mismatches_csv_key", "")
    parts = [
        f"{short_code}: validation failed against the trend report "
        f"({cells} cell(s) across {cols} column(s) diverged in {target})."
    ]
    if csv_key:
        parts.append(
            f"Per-row debug data: s3://{DEFAULT_S3_BUCKET}/{csv_key}"
        )
    parts.append(
        "Either the trend report or the extracted CSV is wrong. "
        "Compare the two and update whichever is stale, then re-extract:\n"
        f"  bash etl/ingestion/tools/retrigger_extraction.sh --go {short_code}"
    )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Per-scenario status decision
# ---------------------------------------------------------------------------
def _scenario_status(state: Dict[str, Any]) -> str:
    """One-word status for the active-scenarios table."""
    ingest_record = state.get("ingest_record")
    extract_record = state.get("extract_record")
    if not ingest_record:
        return "NO_INGEST_RECORD"
    if not extract_record:
        return "AWAITING_EXTRACTION"
    estatus = extract_record.get("status", "")
    if estatus == "FAILED":
        return "FAILED"
    if estatus == "SUCCEEDED_PARTIAL":
        return "PARTIAL"
    if (extract_record.get("validation", {}) or {}).get("result") == "failed":
        return "VALIDATION_FAILED"
    return "OK"


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------
def _short(s: Optional[str], n: int = 12) -> str:
    if not s:
        return ""
    return s if len(s) <= n else s[:n] + "..."


def _render_summary(
    local_state: Dict[str, Any],
    scenario_states: List[Dict[str, Any]],
    attention_count: int,
    cross_dupes: List[str],
    extraction_failure_count: int,
    validation_failure_count: int,
    convention_warn_count: int,
    validation_passed_count: int,
    validation_skipped_count: int,
    validation_awaiting_count: int,
) -> str:
    validation_breakdown = (
        f"{validation_passed_count} passed, "
        f"{validation_failure_count} failed, "
        f"{validation_skipped_count} skipped, "
        f"{validation_awaiting_count} awaiting extraction"
    )
    return (
        "## Run summary\n\n"
        "| metric | value |\n"
        "|---|---|\n"
        f"| Last ingest run (UTC) | {local_state.get('run_at_utc') or 'unknown'} |\n"
        f"| Active scenarios in S3 | {len(scenario_states)} |\n"
        f"| Scenarios needing developer action | {attention_count} |\n"
        f"| Cross-scenario duplicate DV basenames | {len(cross_dupes)} |\n"
        f"| Extraction failures or partials | {extraction_failure_count} |\n"
        f"| Validation failures | {validation_failure_count} |\n"
        f"| Validation breakdown | {validation_breakdown} |\n"
        f"| Convention warnings | {convention_warn_count} |\n"
    )


def _convention_warnings(
    scenario_states: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """Per-scenario list of convention failures (DV or SV basename does not
    contain the short_code). Output is a flat list of
    `{short_code, side, expected}` records, one per failing side."""
    rows: List[Dict[str, str]] = []
    for st in scenario_states:
        ingest_record = st.get("ingest_record")
        if not ingest_record:
            continue
        conv = ingest_record.get("convention_check", {}) or {}
        sc = st["short_code"]
        if not conv.get("short_code_in_dv_basename", True):
            rows.append({
                "short_code": sc,
                "side": "DV",
                "expected": ingest_record.get("expected_dv_filename") or "",
            })
        if not conv.get("short_code_in_sv_basename", True):
            rows.append({
                "short_code": sc,
                "side": "SV",
                "expected": ingest_record.get("expected_sv_filename") or "",
            })
    return rows


def _render_attention(
    local_skips: List[Dict[str, Any]],
    s3_no_ingest_record: List[str],
    extraction_failures: List[Dict[str, Any]],
    validation_failures: List[Dict[str, Any]],
    convention_warnings: List[Dict[str, str]],
) -> str:
    out: List[str] = ["## What needs your attention\n"]
    nothing = True

    if local_skips:
        nothing = False
        out.append(
            "### Scenarios skipped during ingest (local)\n\n"
            "Each row was skipped by `gdrive_bulk_download.py download` and "
            "never reached S3. Resolve, then re-run download for that scenario.\n"
        )
        for r in local_skips:
            sc = r.get("scenario_id", "?")
            code = r.get("error_code") or r.get("validation_status") or "UNKNOWN"
            out.append(f"\n#### {sc} - {code}\n")
            if r.get("error_message"):
                out.append(f"\nMessage: `{r['error_message']}`\n")
            out.append(f"\nAction:\n\n```\n{_action_for_local_skip(r)}\n```\n")

    if s3_no_ingest_record:
        nothing = False
        out.append(
            "\n### S3 scenarios missing ingest_record.json\n\n"
            "These have a ZIP at rest but no ingest record. Batch will fail "
            "fast when triggered. Add an ingest record and resubmit Batch "
            "directly:\n"
        )
        for sc in s3_no_ingest_record:
            out.append(
                f"\n#### {sc} - NO_INGEST_RECORD\n\n"
                f"```\n{_action_for_no_ingest_record(sc)}\n```\n"
            )

    if extraction_failures:
        nothing = False
        out.append(
            "\n### Batch extraction failed or partial\n\n"
            "The Batch container ran but did not produce every CSV that was "
            "requested. Re-trigger after fixing whatever caused the failure.\n"
        )
        for st in extraction_failures:
            sc = st["short_code"]
            estatus = (st.get("extract_record") or {}).get("status", "?")
            out.append(
                f"\n#### {sc} - {estatus}\n\n```\n"
                f"{_action_for_extraction_failure(sc, st['extract_record'])}\n```\n"
            )

    if validation_failures:
        nothing = False
        out.append(
            "\n### Validation failed (extracted CSV does not match trend report)\n\n"
            "Container extraction succeeded but the produced CSV diverged "
            "from the trend report reference. One of them is wrong. The per-row "
            "mismatches CSV in S3 shows which cells diverged.\n"
        )
        for st in validation_failures:
            sc = st["short_code"]
            out.append(
                f"\n#### {sc} - VALIDATION_FAILED\n\n```\n"
                f"{_action_for_validation_failure(sc, st['extract_record'])}\n```\n"
            )

    if convention_warnings:
        nothing = False
        out.append(
            "\n### Convention warnings\n\n"
            "The expected DV or SV basename does not contain the scenario's "
            "short_code. Usually a typo on the spreadsheet row, sometimes a "
            "deliberate shared filename. Confirm the expected basename is "
            "correct for the scenario and update the working CSV if not.\n\n"
        )
        out.append("| short_code | side | expected basename |\n")
        out.append("|---|---|---|\n")
        for w in convention_warnings:
            out.append(f"| {w['short_code']} | {w['side']} | `{w['expected']}` |\n")

    if nothing:
        out.append("\nNothing to do. All scenarios are in a healthy state.\n")

    return "".join(out)


def _render_unverified(
    local_unverified: List[Dict[str, Any]],
    s3_no_trend: List[str],
) -> str:
    """Informational section for scenarios that staged successfully but lack
    a usable trend report. These are not failures and do not block downstream
    extraction. They are surfaced so the developer knows verification will be
    skipped or partial for the listed short codes."""
    if not local_unverified and not s3_no_trend:
        return ""

    out: List[str] = ["## Unverified scenarios\n\n"]
    out.append(
        "Listed here because they staged without a trend report or with an "
        "ambiguous one. Extraction still runs; verification is skipped.\n"
    )

    if local_unverified:
        out.append("\n### From the most recent download run\n")
        for r in local_unverified:
            sc = r.get("scenario_id", "?")
            status = r.get("verification_status", "")
            out.append(f"\n- **{sc}** ({status}): {_action_for_unverified(r)}\n")

    if s3_no_trend:
        out.append("\n### In S3 (no `trend_csv_basename` in ingest_record)\n")
        for sc in s3_no_trend:
            out.append(f"\n- {sc}: {_action_for_no_trend_report(sc)}\n")

    return "".join(out)


def _notes_for_row(state: Dict[str, Any], status: str) -> str:
    """One-line developer-actionable pointer for the active-scenarios table.

    The status column carries the verdict. This column carries the
    where-to-look.
    """
    if status == "OK":
        return ""
    if status in ("NO_INGEST_RECORD", "FAILED", "PARTIAL", "VALIDATION_FAILED"):
        return "see attention"
    if status == "AWAITING_EXTRACTION":
        return "Batch has not produced extract_record.json yet"
    return ""


def _render_active_table(scenario_states: List[Dict[str, Any]]) -> str:
    """4-column active scenarios table.

    `status` subsumes ingest_record / extraction / validation. `notes`
    carries a one-line pointer to a deeper section. Full machine detail
    lives in the per-scenario `ingest_record.json` and
    `extract_record.json` files in S3. The Appendix section at the bottom
    of the rendered audit.md spells out their S3 keys.
    """
    lines = [
        "## Active scenarios",
        "",
        "| short_code | status | last extracted (UTC) | notes |",
        "|---|---|---|---|",
    ]
    for st in scenario_states:
        sc = st["short_code"]
        extract_record = st.get("extract_record")
        status = _scenario_status(st)
        last_extracted = (extract_record or {}).get("processed_at", "") or ""
        notes = _notes_for_row(st, status)
        lines.append(f"| {sc} | {status} | {last_extracted} | {notes} |")
    return "\n".join(lines) + "\n"


def _render_details(
    scenario_states: List[Dict[str, Any]],
    show_all: bool,
) -> str:
    """Per-scenario details, expanded for non-OK rows by default.

    Renders only the developer-actionable fields. Full machine detail
    (sha256s, schema versions, in-zip paths, sample variables) lives in
    the per-scenario `ingest_record.json` and `extract_record.json` files
    in S3. The Appendix section at the bottom of the rendered audit.md
    spells out their S3 keys and what each one contains. Pass `--all` to
    render every row.
    """
    lines = ["## Per-scenario details\n"]
    any_rendered = False

    for st in scenario_states:
        sc = st["short_code"]
        ingest_record = st.get("ingest_record")
        extract_record = st.get("extract_record")
        status = _scenario_status(st)

        if status == "OK" and not show_all:
            continue
        any_rendered = True

        lines.append(f"\n### {sc} - {status}\n")

        if not ingest_record:
            lines.append(
                "\n- Ingest record: MISSING (see 'What needs your attention' above)\n"
            )
        else:
            trend = ingest_record.get("trend_csv_basename") or "(none)"
            lines.append(
                f"\n- Ingestion path: {ingest_record.get('ingestion', {}).get('path', '?')}"
                f"\n- Trend CSV: `{trend}`"
            )

        if extract_record:
            val = extract_record.get("validation", {}) or {}
            job_id = extract_record.get("job_id") or ""
            lines.append(
                f"\n- Batch status: {extract_record.get('status', '?')}"
                f"\n- Batch job id: {job_id or '(none)'}"
            )
            if val.get("result") == "failed":
                lines.append(
                    f"\n- Validation: failed "
                    f"({val.get('mismatch_cells', 0)} cell(s) across "
                    f"{val.get('mismatch_columns', 0)} column(s) in "
                    f"{val.get('target', '?')})"
                )
                csv_key = val.get("mismatches_csv_key")
                if csv_key:
                    lines.append(
                        f"\n- Mismatches CSV: s3://{DEFAULT_S3_BUCKET}/{csv_key}"
                    )

        lines.append("\n")

    if not any_rendered:
        lines.append("\nNo scenarios needed expanded detail. Pass `--all` to see every row.\n")

    return "".join(lines)


def _render_appendix(bucket: str) -> str:
    """Pointer at the per-scenario JSON records that hold the full machine
    detail this report intentionally omits."""
    return (
        "\n## Appendix: what's in the JSON records\n\n"
        "This report intentionally drops machine-only fields (sha256 prefixes, "
        "schema versions, in-zip paths, sample B-parts, "
        "developer/script provenance). Those live in two per-scenario "
        "JSON records in S3:\n\n"
        f"- `s3://{bucket}/scenario/<id>/ingest_record.json` - ingestion-side "
        "contract: declared SV/DV basenames + SHA-256, ZIP hash, trend-CSV "
        "hash, convention check, spreadsheet provenance.\n"
        f"- `s3://{bucket}/scenario/<id>/extract_record.json` - Batch container "
        "output: detected DSS paths, written CSV keys, validation summary, "
        "sample variable B-parts, job id.\n\n"
        "Fetch one with `aws s3 cp s3://<bucket>/scenario/<id>/<record>.json -`.\n"
    )


def _render(
    local_state: Dict[str, Any],
    scenario_states: List[Dict[str, Any]],
    show_all: bool,
) -> Tuple[str, Dict[str, int]]:
    """Build the full markdown plus a small dict of headline counts.

    The counts are the same numbers that go into the `## Run summary`
    table in `audit.md`. They are surfaced so the caller
    (`regenerate_audit`) can print a one-line console summary without
    re-walking `scenario_states`.
    """
    local_skips: List[Dict[str, Any]] = []
    local_unverified: List[Dict[str, Any]] = []
    for row in local_state.get("scenarios", []) or []:
        status = row.get("validation_status") or ""
        verification = row.get("verification_status") or ""
        if status not in ("OK", "DRY_RUN", ""):
            local_skips.append(row)
        elif verification.startswith("unverified_"):
            local_unverified.append(row)

    s3_no_ingest_record = [
        st["short_code"] for st in scenario_states if not st.get("ingest_record")
    ]
    s3_no_trend = [
        st["short_code"] for st in scenario_states
        if st.get("ingest_record") and not st["ingest_record"].get("trend_csv_basename")
    ]

    extraction_failures = [
        st for st in scenario_states
        if st.get("ingest_record") and st.get("extract_record")
        and st["extract_record"].get("status") in ("FAILED", "SUCCEEDED_PARTIAL")
    ]
    validation_failures = [
        st for st in scenario_states
        if st.get("ingest_record") and st.get("extract_record")
        and (st["extract_record"].get("validation", {}) or {}).get("result") == "failed"
    ]
    # Counts for the validation breakdown line. Each
    # filter is independent. `passed` is scenarios where validate_csvs.py
    # ran cleanly. `skipped` covers the explicit skip results from
    # `batch_entrypoint.sh` (no SV/DV target, validator script missing,
    # reference CSV download failed). `awaiting` is scenarios that have
    # an ingest record but no extract record yet, meaning Batch has not
    # written its outcome.
    validation_passed = [
        st for st in scenario_states
        if st.get("ingest_record") and st.get("extract_record")
        and (st["extract_record"].get("validation", {}) or {}).get("result") == "passed"
    ]
    validation_skipped = [
        st for st in scenario_states
        if st.get("ingest_record") and st.get("extract_record")
        and (
            ((st["extract_record"].get("validation", {}) or {}).get("result") or "").startswith("skipped_")
            or (st["extract_record"].get("validation", {}) or {}).get("result") == "download_failed"
        )
    ]
    validation_awaiting = [
        st for st in scenario_states
        if st.get("ingest_record") and not st.get("extract_record")
    ]

    # Cross-scenario DV duplicates (sourced from the ingest records).
    dv_basename_to_scenarios: Dict[str, List[str]] = {}
    for st in scenario_states:
        sc = st["short_code"]
        ingest_record = st.get("ingest_record")
        if not ingest_record:
            continue
        dv = ingest_record.get("expected_dv_filename")
        if dv:
            dv_basename_to_scenarios.setdefault(dv, []).append(sc)
    cross_dupes = [
        f"{dv} ({', '.join(sorted(scs))})"
        for dv, scs in sorted(dv_basename_to_scenarios.items())
        if len(scs) > 1
    ]

    convention_warnings = _convention_warnings(scenario_states)
    convention_warn_count = len(convention_warnings)

    attention_count = (
        len(local_skips)
        + len(s3_no_ingest_record)
        + len(extraction_failures)
        + len(validation_failures)
        + convention_warn_count
    )

    parts: List[str] = []
    parts.append("# COEQWAL ETL audit\n")
    parts.append(
        "\n_Regenerated by `python etl/ingestion/tools/audit.py`. Open this for the "
        "state of the system. Open the logs (paths under each failure) "
        "when you have a question about a specific run._\n\n"
    )
    parts.append(_render_summary(
        local_state, scenario_states, attention_count, cross_dupes,
        len(extraction_failures), len(validation_failures), convention_warn_count,
        len(validation_passed), len(validation_skipped), len(validation_awaiting),
    ))
    parts.append("\n")
    parts.append(_render_attention(
        local_skips, s3_no_ingest_record, extraction_failures, validation_failures,
        convention_warnings,
    ))
    parts.append("\n")
    unverified_block = _render_unverified(local_unverified, s3_no_trend)
    if unverified_block:
        parts.append(unverified_block)
        parts.append("\n")
    if cross_dupes:
        parts.append("## Cross-scenario duplicate DV basenames\n\n")
        parts.append("Two or more active scenarios extracted the same DV file. "
                     "Investigate, this usually indicates a spreadsheet cross-paste.\n\n")
        for entry in cross_dupes:
            parts.append(f"- {entry}\n")
        parts.append("\n")
    parts.append(_render_active_table(scenario_states))
    parts.append("\n")
    parts.append(_render_details(scenario_states, show_all))
    parts.append(_render_appendix(DEFAULT_S3_BUCKET))

    summary: Dict[str, int] = {
        "total": len(scenario_states),
        "attention_count": attention_count,
        "validation_failure_count": len(validation_failures),
        "extraction_failure_count": len(extraction_failures),
        "convention_warn_count": convention_warn_count,
        "validation_passed_count": len(validation_passed),
        "validation_skipped_count": len(validation_skipped),
        "validation_awaiting_count": len(validation_awaiting),
    }
    return "".join(parts), summary


# ---------------------------------------------------------------------------
# Top-level API (imported by gdrive_bulk_download.py for auto-render)
# ---------------------------------------------------------------------------
def regenerate_audit(
    s3_bucket: str,
    audit_md_path: Path = AUDIT_MD_PATH,
    show_all: bool = False,
    dry_run: bool = False,
) -> Optional[str]:
    """Read `ingest_state.json::download` plus S3 evidence and render audit.md.

    Returns the rendered markdown string when `dry_run` is True, else
    `None`. The standalone `main()` and the auto-render hook in
    `gdrive_bulk_download.py cmd_download` both call this.
    """
    s3 = boto3.client("s3")
    local_state = _read_local_state()

    log.debug("Listing scenarios in s3://%s/scenario/ ...", s3_bucket)
    scenario_ids = _list_scenario_ids(s3, s3_bucket)
    log.info("Refreshing audit (collecting state for %d scenarios) ...", len(scenario_ids))

    scenario_states: List[Dict[str, Any]] = []
    for sc in scenario_ids:
        log.debug("  collecting state for %s", sc)
        scenario_states.append(_collect_scenario_state(s3, s3_bucket, sc))

    markdown, summary = _render(local_state, scenario_states, show_all)

    if dry_run:
        return markdown

    audit_md_path.parent.mkdir(parents=True, exist_ok=True)
    audit_md_path.write_text(markdown)
    log.info("Wrote audit to %s", audit_md_path)

    print(f"\nAudit written to {audit_md_path}. Review and commit it manually when ready.")
    print(
        f"Summary: {summary['total']} active scenarios in S3, "
        f"{summary['attention_count']} need developer action "
        f"(extraction failures: {summary['extraction_failure_count']}, "
        f"validation failures: {summary['validation_failure_count']}, "
        f"convention warnings: {summary['convention_warn_count']})."
    )
    print(
        f"Validation: {summary['validation_passed_count']} passed, "
        f"{summary['validation_failure_count']} failed, "
        f"{summary['validation_skipped_count']} skipped, "
        f"{summary['validation_awaiting_count']} awaiting extraction."
    )
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    parser = argparse.ArgumentParser(
        description="Regenerate etl/ingestion/audit.md from S3 state + local ingestion state."
    )
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET,
                        help=f"S3 bucket (default: {DEFAULT_S3_BUCKET})")
    parser.add_argument("--all", action="store_true",
                        help="Expand per-scenario details for every scenario, not just non-OK rows.")
    parser.add_argument("--out", default=str(AUDIT_MD_PATH),
                        help=f"Output markdown file (default: {AUDIT_MD_PATH})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the rendered markdown to stdout instead of writing to disk.")
    args = parser.parse_args()

    markdown = regenerate_audit(
        s3_bucket=args.s3_bucket,
        audit_md_path=Path(args.out),
        show_all=args.all,
        dry_run=args.dry_run,
    )
    if args.dry_run and markdown is not None:
        print(markdown)


if __name__ == "__main__":
    main()
