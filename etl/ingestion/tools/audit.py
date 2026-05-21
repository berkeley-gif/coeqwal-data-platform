#!/usr/bin/env python3
"""
audit.py - renders `etl/ingestion/audit.md` from the local ingestion state
and S3.

What this script does:
  1. Reads `<DEFAULT_OUTPUT_DIR>/audit_state.json` (the per-run record
     written by `gdrive_bulk_download.py download`).
  2. Walks `s3://<bucket>/scenario/*/` and reads two JSON files per
     scenario: `run/sidecar.json` (what was uploaded) and
     `<id>_manifest.json` (what the Batch container did).
  3. Cross-references the two to produce a single Markdown report at
     `etl/ingestion/audit.md`.

Report sections:
  - Run summary
  - What needs your attention (ingest skips, no-sidecar, extraction
    failures, validation failures)
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3

THIS_FILE = Path(__file__).resolve()
INGESTION_DIR = THIS_FILE.parent.parent   # etl/ingestion/
REPO_ROOT = THIS_FILE.parents[3]          # repo root

# When this script is invoked directly (`python etl/ingestion/tools/audit.py`),
# Python does not know where to find the `etl` package, so the imports
# below would fail with ModuleNotFoundError. Adding the repo root to
# `sys.path` tells Python where the `etl/` folder lives.
sys.path.insert(0, str(REPO_ROOT))
from etl.common import DEFAULT_S3_BUCKET, read_json_from_s3  # noqa: E402
from etl.ingestion.lib.config import AUDIT_STATE_PATH  # noqa: E402

# audit.md stays at etl/ingestion/audit.md (tracked in git, referenced by
# the README) regardless of where this script lives.
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
    """Pull the per-scenario JSON files: sidecar (dev side) + manifest
    (container side)."""
    sidecar = read_json_from_s3(
        s3, bucket, f"scenario/{short_code}/run/sidecar.json",
    )
    manifest = read_json_from_s3(
        s3, bucket, f"scenario/{short_code}/{short_code}_manifest.json",
    )
    return {
        "short_code": short_code,
        "sidecar": sidecar,
        "manifest": manifest,
    }


# ---------------------------------------------------------------------------
# Local ingest state
# ---------------------------------------------------------------------------
def _read_local_state() -> Dict[str, Any]:
    """Read the local per-run state from gdrive_bulk_download.py."""
    if not AUDIT_STATE_PATH.exists():
        return {"scenarios": [], "run_at_utc": None, "script_version": None}
    try:
        return json.loads(AUDIT_STATE_PATH.read_text())
    except json.JSONDecodeError:
        log.warning("Local audit_state.json is not valid JSON; treating as empty")
        return {"scenarios": [], "run_at_utc": None, "script_version": None}


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


def _action_for_no_sidecar(short_code: str) -> str:
    return (
        f"A ZIP exists for {short_code} in S3 but no sidecar.json is alongside "
        f"it. The Batch job will fail fast. Run:\n"
        f"  python etl/ingestion/tools/manual_ingest.py sidecar \\\n"
        f"    --short-code {short_code} \\\n"
        f"    --dv-basename '<your DV filename>' \\\n"
        f"    --sv-basename '<your SV filename>' \\\n"
        f"    --compute-hashes --retrigger-batch"
    )


def _action_for_no_trend_report(short_code: str) -> str:
    return (
        f"S3 has a ZIP for {short_code} but no corresponding trend report CSV. "
        f"If you intended to upload one, "
        f"PUT it at s3://{DEFAULT_S3_BUCKET}/scenario/{short_code}/run/<file>.csv"
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


def _action_for_extraction_failure(short_code: str, manifest: Dict[str, Any]) -> str:
    """Actionable message for a scenario whose Batch run failed or partially
    failed."""
    status = manifest.get("status", "UNKNOWN")
    targets = manifest.get("extract_targets", "sv,calsim")
    ss = manifest.get("status_summary", {}) or {}
    parts: List[str] = []
    if status == "SUCCEEDED_PARTIAL":
        missing = []
        if not ss.get("sv_csv_written"):
            missing.append("SV")
        if not ss.get("calsim_csv_written"):
            missing.append("CalSim")
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
    job_id = manifest.get("job_id")
    if job_id:
        parts.append(f"Batch job id: {job_id}")
    return "\n".join(parts)


def _action_for_validation_failure(short_code: str, manifest: Dict[str, Any]) -> str:
    """Actionable message for a scenario whose extracted CSV diverged from
    the trend report reference."""
    val = manifest.get("validation", {}) or {}
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
    sidecar = state.get("sidecar")
    manifest = state.get("manifest")
    if not sidecar:
        return "NO_SIDECAR"
    if not manifest:
        return "AWAITING_EXTRACTION"
    mstatus = manifest.get("status", "")
    if mstatus == "FAILED":
        return "FAILED"
    if mstatus == "SUCCEEDED_PARTIAL":
        return "PARTIAL"
    if (manifest.get("validation", {}) or {}).get("result") == "failed":
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
) -> str:
    return (
        "## Run summary\n\n"
        "| metric | value |\n"
        "|---|---|\n"
        f"| Regenerated (UTC) | {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} |\n"
        f"| Last ingest run (UTC) | {local_state.get('run_at_utc') or 'unknown'} |\n"
        f"| Active scenarios in S3 | {len(scenario_states)} |\n"
        f"| Scenarios needing operator action | {attention_count} |\n"
        f"| Cross-scenario duplicate DV basenames | {len(cross_dupes)} |\n"
        f"| Extraction failures or partials | {extraction_failure_count} |\n"
        f"| Validation failures | {validation_failure_count} |\n"
        f"| Convention warnings | {convention_warn_count} |\n"
    )


def _render_attention(
    local_skips: List[Dict[str, Any]],
    s3_no_sidecar: List[str],
    extraction_failures: List[Dict[str, Any]],
    validation_failures: List[Dict[str, Any]],
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

    if s3_no_sidecar:
        nothing = False
        out.append(
            "\n### S3 scenarios missing sidecar.json\n\n"
            "These have a ZIP at rest but no sidecar. Batch will fail fast "
            "when triggered. Add a sidecar and resubmit Batch directly:\n"
        )
        for sc in s3_no_sidecar:
            out.append(f"\n#### {sc} - NO_SIDECAR\n\n```\n{_action_for_no_sidecar(sc)}\n```\n")

    if extraction_failures:
        nothing = False
        out.append(
            "\n### Batch extraction failed or partial\n\n"
            "The Batch container ran but did not produce every CSV that was "
            "requested. Re-trigger after fixing whatever caused the failure.\n"
        )
        for st in extraction_failures:
            sc = st["short_code"]
            mstatus = (st.get("manifest") or {}).get("status", "?")
            out.append(f"\n#### {sc} - {mstatus}\n\n```\n"
                       f"{_action_for_extraction_failure(sc, st['manifest'])}\n```\n")

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
            out.append(f"\n#### {sc} - VALIDATION_FAILED\n\n```\n"
                       f"{_action_for_validation_failure(sc, st['manifest'])}\n```\n")

    if nothing:
        out.append("\nNothing to do. All scenarios are in a healthy state.\n")

    return "".join(out)


def _render_unverified(
    local_unverified: List[Dict[str, Any]],
    s3_no_trend: List[str],
) -> str:
    """Informational section for scenarios that staged successfully but lack
    a usable trend report. These are not failures and do not block downstream
    extraction. They are surfaced so the operator knows verification will be
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
        out.append("\n### In S3 (no `trend_csv_basename` in sidecar)\n")
        for sc in s3_no_trend:
            out.append(f"\n- {sc}: {_action_for_no_trend_report(sc)}\n")

    return "".join(out)


def _render_active_table(scenario_states: List[Dict[str, Any]]) -> str:
    lines = [
        "## Active scenarios",
        "",
        "| short_code | path | sidecar | trend | convention | extraction | validation | mismatches | last extracted (UTC) | status |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for st in scenario_states:
        sc = st["short_code"]
        sidecar = st.get("sidecar")
        manifest = st.get("manifest")

        path = (sidecar or {}).get("ingestion", {}).get("path", "?") if sidecar else "?"
        sidecar_cell = "yes" if sidecar else "MISSING"
        trend_cell = "yes" if (sidecar or {}).get("trend_csv_basename") else "no"

        if sidecar:
            conv = sidecar.get("convention_check", {}) or {}
            conv_parts = []
            if not conv.get("short_code_in_dv_basename", True):
                conv_parts.append("dv warn")
            if not conv.get("short_code_in_sv_basename", True):
                conv_parts.append("sv warn")
            conv_cell = ", ".join(conv_parts) if conv_parts else "OK"
        else:
            conv_cell = "?"

        if manifest:
            extraction_cell = manifest.get("status", "?")
            validation = manifest.get("validation", {}) or {}
            val_result = validation.get("result", "")
            validation_cell = val_result if val_result else "-"
            cells = validation.get("mismatch_cells", 0) or 0
            mismatch_cell = str(cells) if cells else "-"
            last_extracted = manifest.get("processed_at", "")
        else:
            extraction_cell = "-"
            validation_cell = "-"
            mismatch_cell = "-"
            last_extracted = ""

        status = _scenario_status(st)

        lines.append(
            f"| {sc} | {path} | {sidecar_cell} | {trend_cell} | {conv_cell} | "
            f"{extraction_cell} | {validation_cell} | {mismatch_cell} | "
            f"{last_extracted} | {status} |"
        )
    return "\n".join(lines) + "\n"


def _render_details(
    scenario_states: List[Dict[str, Any]],
    show_all: bool,
) -> str:
    """Per-scenario details, expanded for non-OK or convention-warn rows by default."""
    lines = ["## Per-scenario details\n"]
    any_rendered = False

    for st in scenario_states:
        sc = st["short_code"]
        sidecar = st.get("sidecar")
        manifest = st.get("manifest")
        status = _scenario_status(st)

        expand = show_all or status != "OK"
        if not expand and sidecar:
            conv = sidecar.get("convention_check", {}) or {}
            if (not conv.get("short_code_in_dv_basename", True)) or \
                    (not conv.get("short_code_in_sv_basename", True)):
                expand = True

        if not expand:
            continue
        any_rendered = True

        lines.append(f"\n### {sc}\n")
        if sidecar:
            lines.append(
                f"\n- Ingestion path: {sidecar.get('ingestion', {}).get('path', '?')}"
                f"\n- Sidecar present: yes (schema_version={sidecar.get('schema_version')})"
                f"\n- Expected DV: `{sidecar.get('expected_dv_filename')}` "
                f"(sha256={_short(sidecar.get('dv_sha256'))})"
                f"\n- Expected SV: `{sidecar.get('expected_sv_filename')}` "
                f"(sha256={_short(sidecar.get('sv_sha256'))})"
                f"\n- ZIP basename: `{sidecar.get('zip_basename')}` "
                f"(sha256={_short(sidecar.get('zip_sha256'))})"
                f"\n- Trend CSV: `{sidecar.get('trend_csv_basename') or '(none)'}`"
            )
        else:
            lines.append("\n- Sidecar present: NO\n- Action: see 'What needs your attention' above")

        if manifest:
            ss = manifest.get("status_summary", {}) or {}
            val = manifest.get("validation", {}) or {}
            lines.append(
                f"\n- Batch status: {manifest.get('status', '?')}"
                f"\n- Extract targets: {manifest.get('extract_targets', 'sv,calsim')}"
                f"\n- Processed at (UTC): {manifest.get('processed_at', '?')}"
                f"\n- Batch job id: {manifest.get('job_id', '?')}"
                f"\n- SV CSV written: {ss.get('sv_csv_written')}"
                f"\n- CalSim CSV written: {ss.get('calsim_csv_written')}"
                f"\n- Validation: {val.get('result', '-')} "
                f"(target={val.get('target', '-')}, "
                f"mismatch_cells={val.get('mismatch_cells', 0)})"
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


def _render(
    local_state: Dict[str, Any],
    scenario_states: List[Dict[str, Any]],
    show_all: bool,
) -> str:
    """Build the full markdown."""
    local_skips: List[Dict[str, Any]] = []
    local_unverified: List[Dict[str, Any]] = []
    for row in local_state.get("scenarios", []) or []:
        status = row.get("validation_status") or ""
        verification = row.get("verification_status") or ""
        if status not in ("OK", "DRY_RUN", ""):
            local_skips.append(row)
        elif verification.startswith("unverified_"):
            local_unverified.append(row)

    s3_no_sidecar = [st["short_code"] for st in scenario_states if not st.get("sidecar")]
    s3_no_trend = [st["short_code"] for st in scenario_states
                   if st.get("sidecar") and not st["sidecar"].get("trend_csv_basename")]

    extraction_failures = [
        st for st in scenario_states
        if st.get("sidecar") and st.get("manifest")
        and st["manifest"].get("status") in ("FAILED", "SUCCEEDED_PARTIAL")
    ]
    validation_failures = [
        st for st in scenario_states
        if st.get("sidecar") and st.get("manifest")
        and (st["manifest"].get("validation", {}) or {}).get("result") == "failed"
    ]

    # Cross-scenario DV duplicates (sourced from the sidecars).
    dv_basename_to_scenarios: Dict[str, List[str]] = {}
    for st in scenario_states:
        sc = st["short_code"]
        sidecar = st.get("sidecar")
        if not sidecar:
            continue
        dv = sidecar.get("expected_dv_filename")
        if dv:
            dv_basename_to_scenarios.setdefault(dv, []).append(sc)
    cross_dupes = [
        f"{dv} ({', '.join(sorted(scs))})"
        for dv, scs in sorted(dv_basename_to_scenarios.items())
        if len(scs) > 1
    ]

    convention_warn_count = 0
    for st in scenario_states:
        sidecar = st.get("sidecar")
        if not sidecar:
            continue
        conv = sidecar.get("convention_check", {}) or {}
        if not conv.get("short_code_in_dv_basename", True):
            convention_warn_count += 1
        if not conv.get("short_code_in_sv_basename", True):
            convention_warn_count += 1

    attention_count = (
        len(local_skips)
        + len(s3_no_sidecar)
        + len(extraction_failures)
        + len(validation_failures)
    )

    parts: List[str] = []
    parts.append("# COEQWAL ETL audit\n")
    parts.append(
        "\n_Regenerated by `python etl/ingestion/tools/audit.py`. Open this for the "
        "state of the system. Open the logs (paths under each failure) only "
        "when you have a question about a specific run._\n\n"
    )
    parts.append(_render_summary(
        local_state, scenario_states, attention_count, cross_dupes,
        len(extraction_failures), len(validation_failures), convention_warn_count,
    ))
    parts.append("\n")
    parts.append(_render_attention(
        local_skips, s3_no_sidecar, extraction_failures, validation_failures,
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
    return "".join(parts)


# ---------------------------------------------------------------------------
# Top-level API (imported by gdrive_bulk_download.py for auto-render)
# ---------------------------------------------------------------------------
def regenerate_audit(
    s3_bucket: str,
    audit_md_path: Path = AUDIT_MD_PATH,
    show_all: bool = False,
    dry_run: bool = False,
) -> Optional[str]:
    """Read audit_state.json plus S3 evidence and render audit.md.

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

    markdown = _render(local_state, scenario_states, show_all)

    if dry_run:
        return markdown

    audit_md_path.parent.mkdir(parents=True, exist_ok=True)
    audit_md_path.write_text(markdown)
    log.info("Wrote audit to %s", audit_md_path)
    print(f"\nAudit written to {audit_md_path}. Review and commit it manually when ready.")
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
