#!/usr/bin/env python3
"""
Orchestrator: scenario ingestion -> Batch DSS extraction -> statistics -> verify.

Stages (continue-on-error; exits non-zero if any scenario fails any stage):
  scan -> download -> promote -> wait-batch -> statistics -> verify

Subprocesses existing tools so their logs stream live (same terminal).

Usage:
  python etl/run_full_pipeline.py --scenarios s0107 s0108 \\
      [--workers 4] [--dry-run]

  python etl/run_full_pipeline.py --all [--workers 4]

Resume after partial failure:
  python etl/run_full_pipeline.py --resume \\
      --report-dir etl/ingestion/output/pipeline_runs/<timestamp> \\
      --start-stage batch

Requires Cloud9 / IAM: S3 read on coeqwal-model-run, batch:ListJobs, batch:DescribeJobs,
plus DATABASE_URL for statistics and verify (unless orchestrator --dry-run).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import boto3

# Repo layout: etl/run_full_pipeline.py -> repo root is parent.parent
REPO_ROOT = Path(__file__).resolve().parent.parent
INGEST_SCRIPT = REPO_ROOT / "etl" / "ingestion" / "gdrive_bulk_download.py"
STATS_SCRIPT = REPO_ROOT / "etl" / "statistics" / "run_all.py"
VERIFY_SCRIPT = REPO_ROOT / "etl" / "statistics" / "verify_all_sections.py"
SCAN_AUDIT_CSV = REPO_ROOT / "etl" / "ingestion" / "output" / "scan_audit.csv"
AUDIT_REPORT_CSV = REPO_ROOT / "etl" / "ingestion" / "output" / "audit_report.csv"

DEFAULT_LISTING_CSV = (
    "etl/ingestion/scenario_listing/model_run_file_source_working.csv"
)

INGEST_OUTPUT_PREFIX = Path("etl/ingestion/output")

# Shared constants from etl/common/. Make `from etl.common import X` work
# when this script is invoked as `python etl/run_full_pipeline.py`.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from etl.common import (  # noqa: E402
    S3_BUCKET as DEFAULT_S3_BUCKET,
    BATCH_QUEUE as DEFAULT_BATCH_QUEUE,
)

# Import manifest reader from ingestion helpers (avoid duplicating S3 JSON IO).
_INGEST_DIR = str(REPO_ROOT / "etl" / "ingestion")
if _INGEST_DIR not in sys.path:
    sys.path.insert(0, _INGEST_DIR)
try:
    from check_extraction_results import read_json_from_s3  # noqa: E402
except ImportError:
    read_json_from_s3 = None  # type: ignore

log = logging.getLogger("run_full_pipeline")

STAGES_ORDER = ["scan", "download", "promote", "batch", "stats", "verify"]

TERMINAL_BATCH = frozenset({"SUCCEEDED", "FAILED"})
ACTIVE_BATCH = frozenset(
    {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}
)

def utc_stamp_dir() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_scenarios(values: Optional[List[str]]) -> List[str]:
    if not values:
        return []
    out: List[str] = []
    for v in values:
        for tok in re.split(r"[\s,]+", v.strip()):
            if tok:
                out.append(tok.lower())
    return sorted(set(out))


def parse_skip(skip_raw: Optional[str]) -> Set[str]:
    if not skip_raw:
        return set()
    return {x.strip().lower() for x in skip_raw.split(",") if x.strip()}


def validate_stages(skip: Set[str], start: str) -> None:
    bad = skip - set(STAGES_ORDER)
    if bad:
        raise SystemExit(f"Unknown --skip-stage value(s): {bad}")
    if start not in STAGES_ORDER:
        raise SystemExit(f"--start-stage must be one of {STAGES_ORDER}")


def stage_index(stage: str) -> int:
    return STAGES_ORDER.index(stage)


def should_run_stage(stage: str, start: str, skip: Set[str]) -> bool:
    if stage in skip:
        return False
    return stage_index(stage) >= stage_index(start)


def tee_run(
    cmd: List[str],
    log_path: Path,
    cwd: Path,
    env: Optional[Dict[str, str]] = None,
) -> int:
    """Run subprocess; stream merged stdout/stderr to terminal and log_path."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env or os.environ.copy(),
    )
    assert proc.stdout is not None

    def reader():
        with open(log_path, "w", encoding="utf-8") as lf:
            for line in proc.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                lf.write(line)

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    proc.wait()
    t.join(timeout=5)
    return proc.returncode


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def rows_by_scenario(rows: List[Dict[str, str]], key: str = "scenario_id") -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        sid = (r.get(key) or "").strip().lower()
        if sid:
            out[sid] = r
    return out


def scan_ok(status: str) -> bool:
    return status.strip() == "OK"


def download_ok(row: Dict[str, str]) -> bool:
    return (row.get("validation_status") or "").strip() == "OK"


def manifest_extraction_ok(manifest: Dict[str, Any]) -> Tuple[bool, str]:
    st = (manifest.get("status") or "").strip().upper()
    if st in ("SUCCEEDED", "SUCCEEDED_PARTIAL"):
        return True, st
    return False, st or "UNKNOWN"


def discover_batch_job(
    batch_client,
    queue: str,
    scenario_id: str,
    promote_started_ms: int,
    clock_skew_ms: int = 10_000,
) -> Optional[Dict[str, Any]]:
    """Most recent Batch job named etl-<scenario>-* created near promote_started_ms."""
    prefix = f"etl-{scenario_id.lower()}-"
    best: Optional[Dict[str, Any]] = None
    best_created = 0
    floor_ms = max(0, promote_started_ms - clock_skew_ms)

    for status in sorted(ACTIVE_BATCH | TERMINAL_BATCH):
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"jobQueue": queue, "jobStatus": status}
            if token:
                kwargs["nextToken"] = token
            resp = batch_client.list_jobs(**kwargs)
            for j in resp.get("jobSummaryList") or []:
                name = j.get("jobName") or ""
                if not name.startswith(prefix):
                    continue
                created = int(j.get("createdAt") or 0)
                if created < floor_ms:
                    continue
                if created >= best_created:
                    best_created = created
                    best = j
            token = resp.get("nextToken")
            if not token:
                break
    return best


def head_object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def load_state(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def init_per_scenario(scenario_ids: List[str]) -> Dict[str, Dict[str, str]]:
    blank = {s: "not_run" for s in STAGES_ORDER}
    return {sid: dict(blank) for sid in scenario_ids}


def banner(title: str) -> None:
    log.info("%s", "=" * 60)
    log.info("%s", title)
    log.info("%s", "=" * 60)


def write_summary_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    cols = ["scenario_id"] + STAGES_ORDER + ["notes"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def print_console_table(per_summary: List[Dict[str, Any]]) -> None:
    cols = ["scenario_id"] + STAGES_ORDER
    widths = {c: len(c) for c in cols}
    for r in per_summary:
        for c in cols:
            widths[c] = max(widths[c], len(str(r.get(c, ""))))
    hdr = "  ".join(c.ljust(widths[c]) for c in cols)
    print("\n" + "=" * len(hdr))
    print("PIPELINE SUMMARY")
    print("=" * len(hdr))
    print(hdr)
    print("-" * len(hdr))
    for r in per_summary:
        print("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))
        notes = r.get("notes")
        if notes:
            print(f"  notes: {notes}")
    print("=" * len(hdr) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Full pipeline: ingest -> Batch -> statistics -> verify",
    )
    parser.add_argument("--scenarios", nargs="*", help="Scenario short codes")
    parser.add_argument("--all", action="store_true", help="All rows from listing CSV scan")
    parser.add_argument("--workers", type=int, default=4, help="Ingest worker threads")
    parser.add_argument("--listing-csv", default=DEFAULT_LISTING_CSV, help="Working CSV path")
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET)
    parser.add_argument("--batch-queue", default=DEFAULT_BATCH_QUEUE)
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Directory for logs + pipeline_state.json (default: timestamp under ingestion/output/pipeline_runs)",
    )
    parser.add_argument(
        "--start-stage",
        default="scan",
        choices=STAGES_ORDER,
        help="First stage to run (use with --resume)",
    )
    parser.add_argument(
        "--skip-stage",
        default=None,
        help="Comma-separated stages to skip (scan,download,...)",
    )
    parser.add_argument("--batch-timeout", type=int, default=7200)
    parser.add_argument("--batch-poll-interval", type=int, default=60)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Passed through to ingestion download/promote dry-run behavior where supported",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue using existing --report-dir (loads pipeline_state.json)",
    )
    args = parser.parse_args()

    if args.resume and not args.report_dir:
        parser.error("--resume requires explicit --report-dir")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    skip = parse_skip(args.skip_stage)
    validate_stages(skip, args.start_stage)

    if args.start_stage != "scan" and not args.resume:
        parser.error(
            "--start-stage after scan requires --resume with an existing --report-dir "
            "that contains pipeline_state.json"
        )
    if args.all and "scan" in skip:
        parser.error("--all requires the scan stage (remove scan from --skip-stage)")

    if not args.scenarios and not args.all:
        parser.error("Specify --scenarios or --all")

    listing_rel = args.listing_csv
    cwd = REPO_ROOT

    report_dir = Path(args.report_dir) if args.report_dir else (
        REPO_ROOT / INGEST_OUTPUT_PREFIX / "pipeline_runs" / utc_stamp_dir()
    )
    report_dir = report_dir.resolve()
    state_path = report_dir / "pipeline_state.json"
    summary_json = report_dir / "pipeline_summary.json"
    summary_csv = report_dir / "pipeline_summary.csv"

    scenario_ids: List[str]
    state: Dict[str, Any]
    promote_started_ms: int = 0

    if args.resume:
        if not state_path.exists():
            raise SystemExit(f"--resume requires pipeline_state.json at {state_path}")
        state = load_state(state_path)
        scenario_ids = list(state.get("scenario_ids") or [])
        promote_started_ms = int(state.get("promote_started_at_ms") or 0)
        per_scenario: Dict[str, Dict[str, str]] = state.get("per_scenario") or {}
        job_ids: Dict[str, str] = dict(state.get("batch_job_ids") or {})
        log.info("Resumed from %s (%d scenarios)", state_path, len(scenario_ids))
    else:
        report_dir.mkdir(parents=True, exist_ok=True)
        scenario_ids = parse_scenarios(args.scenarios)
        if args.all:
            scenario_ids = []  # filled after scan
        elif not scenario_ids:
            parser.error("--scenarios requires at least one id unless --all")
        per_scenario = {}
        job_ids = {}
        state = {
            "version": 1,
            "scenario_ids": scenario_ids,
            "listing_csv": listing_rel,
            "s3_bucket": args.s3_bucket,
            "batch_queue": args.batch_queue,
            "promote_started_at_ms": None,
            "promote_started_at_iso": None,
            "per_scenario": {},
            "batch_job_ids": {},
        }

    def persist() -> None:
        state["scenario_ids"] = scenario_ids
        state["per_scenario"] = per_scenario
        state["batch_job_ids"] = job_ids
        state["promote_started_at_ms"] = promote_started_ms or state.get(
            "promote_started_at_ms"
        )
        save_state(state_path, state)

    ingest_base_cmd = [
        sys.executable,
        str(INGEST_SCRIPT),
        "--listing-csv",
        listing_rel,
        "--s3-bucket",
        args.s3_bucket,
        "--rclone-remote",
        "gdrive",
    ]

    t_pipeline0 = time.time()

    # ----- SCAN -----
    if should_run_stage("scan", args.start_stage, skip):
        banner(f"STAGE 1/{len(STAGES_ORDER)}: scan ({args.all and 'ALL' or len(scenario_ids)} scenarios)")
        t0 = time.time()
        cmd = ingest_base_cmd + ["scan", "--workers", str(args.workers)]
        if args.all:
            cmd.append("--all")
        else:
            cmd.extend(["--scenarios", *scenario_ids])
        rc = tee_run(cmd, report_dir / "scan.log", cwd=cwd)
        log.info("scan finished in %.1fs (exit %d)", time.time() - t0, rc)

        scan_rows = read_csv_rows(SCAN_AUDIT_CSV)
        by_sid = rows_by_scenario(scan_rows)

        if args.all:
            scenario_ids = sorted(by_sid.keys())
            if not scenario_ids:
                raise SystemExit("scan produced no rows in scan_audit.csv")
            state.setdefault("version", 1)
            log.info("--all: discovered %d scenario(s) from scan audit", len(scenario_ids))

        per_scenario = init_per_scenario(scenario_ids)

        for sid in scenario_ids:
            row = by_sid.get(sid)
            if row is None:
                per_scenario.setdefault(sid, {s: "not_run" for s in STAGES_ORDER})
                per_scenario[sid]["scan"] = "failed"
                continue
            per_scenario.setdefault(sid, {s: "not_run" for s in STAGES_ORDER})
            per_scenario[sid]["scan"] = "ok" if scan_ok(row.get("status", "")) else "failed"
            if per_scenario[sid]["scan"] == "failed":
                note = row.get("status", "")
                per_scenario[sid]["notes"] = (per_scenario[sid].get("notes") or "") + f" scan:{note};"

        persist()
    else:
        if args.resume:
            per_scenario = dict(state.get("per_scenario") or {})
            if not per_scenario:
                per_scenario = init_per_scenario(scenario_ids)
            log.info(
                "Skipping scan (--resume --start-stage %s)",
                args.start_stage,
            )
        elif "scan" in skip:
            per_scenario = init_per_scenario(scenario_ids)
            for sid in scenario_ids:
                per_scenario[sid]["scan"] = "skipped"
        else:
            raise RuntimeError("scan stage skipped unexpectedly")

    ok_after_scan = [s for s in scenario_ids if per_scenario.get(s, {}).get("scan") == "ok"]

    # ----- DOWNLOAD -----
    if should_run_stage("download", args.start_stage, skip):
        banner(f"STAGE 2/{len(STAGES_ORDER)}: download ({len(ok_after_scan)} scenarios)")
        if not ok_after_scan:
            log.warning("No scenarios passed scan; skipping download subprocess")
        else:
            t0 = time.time()
            cmd = ingest_base_cmd + [
                "download",
                "--workers",
                str(args.workers),
                "--scenarios",
                *ok_after_scan,
            ]
            if args.dry_run:
                cmd.append("--dry-run")
            rc = tee_run(cmd, report_dir / "download.log", cwd=cwd)
            log.info("download finished in %.1fs (exit %d)", time.time() - t0, rc)

        audit_rows = read_csv_rows(AUDIT_REPORT_CSV)
        by_aud = rows_by_scenario(audit_rows)

        for sid in scenario_ids:
            if per_scenario.get(sid, {}).get("scan") != "ok":
                per_scenario[sid]["download"] = "skipped"
                continue
            row = by_aud.get(sid)
            if row is None:
                per_scenario[sid]["download"] = "failed"
                continue
            per_scenario[sid]["download"] = (
                "ok" if download_ok(row) else "failed"
            )
            if per_scenario[sid]["download"] != "ok":
                per_scenario[sid]["notes"] = (
                    (per_scenario[sid].get("notes") or "")
                    + f" download:{row.get('validation_status')}/{row.get('error_code')};"
                )

        persist()

    ok_after_download = [
        s
        for s in scenario_ids
        if per_scenario.get(s, {}).get("scan") == "ok"
        and per_scenario.get(s, {}).get("download") == "ok"
    ]

    # ----- PROMOTE -----
    if should_run_stage("promote", args.start_stage, skip):
        banner(f"STAGE 3/{len(STAGES_ORDER)}: promote ({len(ok_after_download)} scenarios)")
        promote_started_ms = int(time.time() * 1000)
        promote_started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        state["promote_started_at_ms"] = promote_started_ms
        state["promote_started_at_iso"] = promote_started_iso

        if not ok_after_download:
            log.warning("No scenarios passed download; skipping promote subprocess")
        else:
            t0 = time.time()
            cmd = ingest_base_cmd + ["promote", "--scenarios", *ok_after_download]
            if args.dry_run:
                cmd.append("--dry-run")
            rc = tee_run(cmd, report_dir / "promote.log", cwd=cwd)
            log.info("promote finished in %.1fs (exit %d)", time.time() - t0, rc)

        audit_rows = read_csv_rows(AUDIT_REPORT_CSV)
        by_aud = rows_by_scenario(audit_rows)
        s3c = boto3.client("s3")

        for sid in scenario_ids:
            if per_scenario.get(sid, {}).get("download") != "ok":
                per_scenario[sid]["promote"] = "skipped"
                continue
            row = by_aud.get(sid, {})
            zip_name = (row.get("zip_selected") or "").strip()
            if args.dry_run:
                per_scenario[sid]["promote"] = "ok"
                continue
            if not zip_name:
                per_scenario[sid]["promote"] = "failed"
                per_scenario[sid]["notes"] = (
                    (per_scenario[sid].get("notes") or "") + " promote:no_zip_selected;"
                )
                continue
            ready_key = f"ready/{sid}/{zip_name}"
            if head_object_exists(s3c, args.s3_bucket, ready_key):
                per_scenario[sid]["promote"] = "ok"
            else:
                per_scenario[sid]["promote"] = "failed"
                per_scenario[sid]["notes"] = (
                    (per_scenario[sid].get("notes") or "")
                    + f" promote:missing s3://{args.s3_bucket}/{ready_key};"
                )

        persist()
    elif args.resume:
        promote_started_ms = int(state.get("promote_started_at_ms") or 0)

    ok_after_promote = [
        s
        for s in scenario_ids
        if per_scenario.get(s, {}).get("promote") == "ok"
        and not args.dry_run
    ]

    # ----- WAIT BATCH -----
    if should_run_stage("batch", args.start_stage, skip):
        if read_json_from_s3 is None:
            raise SystemExit("check_extraction_results.read_json_from_s3 unavailable")

        banner(f"STAGE 4/{len(STAGES_ORDER)}: wait-for-Batch ({len(ok_after_promote)} scenarios)")
        if args.dry_run:
            log.warning("Skipping Batch wait (--dry-run ingest)")
            for sid in scenario_ids:
                if per_scenario.get(sid, {}).get("promote") == "ok":
                    per_scenario[sid]["batch"] = "skipped"
            persist()
        elif not ok_after_promote:
            log.warning("No promoted scenarios; skipping Batch wait")
            for sid in scenario_ids:
                per_scenario.setdefault(sid, {st: "not_run" for st in STAGES_ORDER})
                if per_scenario[sid].get("promote") == "ok":
                    per_scenario[sid]["batch"] = "skipped"
        else:
            if promote_started_ms <= 0:
                raise SystemExit(
                    "promote_started_at_ms missing; cannot discover Batch jobs. "
                    "Re-run promote stage or pass a fresh report-dir."
                )

            batch_client = boto3.client("batch", region_name="us-west-2")
            s3_client = boto3.client("s3", region_name="us-west-2")

            deadline = time.time() + args.batch_timeout
            pending_discovery = set(ok_after_promote)
            batch_done: Set[str] = set()

            poll_i = 0
            while time.time() < deadline:
                poll_i += 1
                for sid in list(pending_discovery):
                    job = discover_batch_job(
                        batch_client,
                        args.batch_queue,
                        sid,
                        promote_started_ms,
                    )
                    if job and job.get("jobId"):
                        job_ids[sid] = job["jobId"]
                        pending_discovery.discard(sid)
                        log.info(
                            "[%s] discovered Batch job %s (%s)",
                            sid,
                            job["jobId"],
                            job.get("jobName"),
                        )

                rev_map = {jid: s for s, jid in job_ids.items()}
                describe_ids = [
                    job_ids[s]
                    for s in ok_after_promote
                    if s not in batch_done and s in job_ids
                ]
                if describe_ids:
                    for i in range(0, len(describe_ids), 100):
                        chunk = describe_ids[i : i + 100]
                        desc = batch_client.describe_jobs(jobs=chunk)
                        for job in desc.get("jobs") or []:
                            jid = job["jobId"]
                            sid_j = rev_map.get(jid)
                            if not sid_j or sid_j in batch_done:
                                continue
                            per_scenario.setdefault(
                                sid_j, {stg: "not_run" for stg in STAGES_ORDER}
                            )
                            st = job.get("status") or ""
                            reason = job.get("statusReason") or ""

                            if st == "FAILED":
                                per_scenario[sid_j]["batch"] = "failed"
                                per_scenario[sid_j]["notes"] = (
                                    (per_scenario[sid_j].get("notes") or "")
                                    + f" batch_job:{reason};"
                                )
                                batch_done.add(sid_j)
                                continue

                            if st == "SUCCEEDED":
                                manifest_key = (
                                    f"scenario/{sid_j}/{sid_j}_manifest.json"
                                )
                                manifest = read_json_from_s3(
                                    s3_client, args.s3_bucket, manifest_key
                                )
                                ok_m, mst = (
                                    manifest_extraction_ok(manifest)
                                    if manifest
                                    else (False, "NO_MANIFEST")
                                )
                                if ok_m:
                                    per_scenario[sid_j]["batch"] = "ok"
                                else:
                                    per_scenario[sid_j]["batch"] = "failed"
                                    per_scenario[sid_j]["notes"] = (
                                        (per_scenario[sid_j].get("notes") or "")
                                        + f" batch_manifest:{mst};"
                                    )
                                batch_done.add(sid_j)

                done_ok = sum(
                    1
                    for s in ok_after_promote
                    if per_scenario.get(s, {}).get("batch") == "ok"
                )
                done_fail = sum(
                    1
                    for s in ok_after_promote
                    if per_scenario.get(s, {}).get("batch") == "failed"
                )

                line_parts = []
                for sid in sorted(ok_after_promote):
                    if sid in pending_discovery:
                        line_parts.append(f"{sid}=discovering")
                    elif sid not in job_ids:
                        line_parts.append(f"{sid}=no_job")
                    elif sid in batch_done:
                        line_parts.append(f"{sid}={per_scenario[sid].get('batch')}")
                    else:
                        line_parts.append(f"{sid}=running")
                log.info(
                    "Batch poll #%d (%d/%d done ok=%d fail=%d): %s",
                    poll_i,
                    len(batch_done),
                    len(ok_after_promote),
                    done_ok,
                    done_fail,
                    " ".join(line_parts[:40])
                    + (" …" if len(line_parts) > 40 else ""),
                )

                if len(batch_done) == len(ok_after_promote):
                    break

                time.sleep(args.batch_poll_interval)

            # Timeout / undiscovered handling
            for sid in ok_after_promote:
                per_scenario.setdefault(sid, {stg: "not_run" for stg in STAGES_ORDER})
                if sid in batch_done:
                    continue
                if sid in pending_discovery:
                    per_scenario[sid]["batch"] = "failed"
                    per_scenario[sid]["notes"] = (
                        (per_scenario[sid].get("notes") or "")
                        + " batch:job_never_discovered;"
                    )
                else:
                    per_scenario[sid]["batch"] = "failed"
                    per_scenario[sid]["notes"] = (
                        (per_scenario[sid].get("notes") or "") + " batch:timeout;"
                    )

            for sid in scenario_ids:
                if sid not in ok_after_promote:
                    per_scenario.setdefault(sid, {stg: "not_run" for stg in STAGES_ORDER})
                    if per_scenario[sid].get("batch") == "not_run":
                        per_scenario[sid]["batch"] = "skipped"

        persist()

    ok_after_batch = [
        s
        for s in scenario_ids
        if per_scenario.get(s, {}).get("batch") == "ok"
    ]

    # ----- STATS -----
    if should_run_stage("stats", args.start_stage, skip):
        banner(f"STAGE 5/{len(STAGES_ORDER)}: statistics ({len(ok_after_batch)} scenarios)")

        for sid in scenario_ids:
            if sid not in ok_after_batch:
                per_scenario.setdefault(sid, {stg: "not_run" for stg in STAGES_ORDER})
                per_scenario[sid]["stats"] = "skipped"
                continue

            per_scenario.setdefault(sid, {stg: "not_run" for stg in STAGES_ORDER})
            t0 = time.time()
            cmd = [
                sys.executable,
                str(STATS_SCRIPT),
                "--scenario",
                sid,
                "--continue-on-error",
            ]
            if args.dry_run:
                cmd.append("--dry-run")
            rc = tee_run(cmd, report_dir / f"stats_{sid}.log", cwd=cwd)
            per_scenario[sid]["stats"] = "ok" if rc == 0 else "failed"
            log.info("[%s] statistics exit %d (%.1fs)", sid, rc, time.time() - t0)
            if rc != 0:
                per_scenario[sid]["notes"] = (
                    (per_scenario[sid].get("notes") or "") + " stats:nonzero_exit;"
                )

        persist()

    ok_after_stats = [
        s
        for s in scenario_ids
        if per_scenario.get(s, {}).get("stats") == "ok"
    ]

    # ----- VERIFY -----
    if should_run_stage("verify", args.start_stage, skip):
        banner(f"STAGE 6/{len(STAGES_ORDER)}: verify ({len(ok_after_stats)} scenarios)")
        verify_root = report_dir / "verify"
        verify_root.mkdir(parents=True, exist_ok=True)

        for sid in scenario_ids:
            if sid not in ok_after_stats:
                per_scenario.setdefault(sid, {stg: "not_run" for stg in STAGES_ORDER})
                per_scenario[sid]["verify"] = "skipped"
                continue

            per_scenario.setdefault(sid, {stg: "not_run" for stg in STAGES_ORDER})
            t0 = time.time()
            cmd = [
                sys.executable,
                str(VERIFY_SCRIPT),
                "--scenario",
                sid,
                "--report-dir",
                str(verify_root),
            ]
            # Respect DATABASE-less csv-only only when orchestrator dry-run?
            if args.dry_run:
                cmd.append("--csv-only")
            rc = tee_run(cmd, report_dir / f"verify_{sid}.log", cwd=cwd)
            per_scenario[sid]["verify"] = "ok" if rc == 0 else "failed"
            log.info("[%s] verify exit %d (%.1fs)", sid, rc, time.time() - t0)
            if rc != 0:
                per_scenario[sid]["notes"] = (
                    (per_scenario[sid].get("notes") or "") + " verify:nonzero_exit;"
                )

        persist()

    # ----- SUMMARY -----
    summary_rows: List[Dict[str, Any]] = []
    any_fail = False
    for sid in scenario_ids:
        ps = per_scenario.get(sid, {})
        row: Dict[str, Any] = {"scenario_id": sid, "notes": ps.get("notes", "").strip()}
        for st in STAGES_ORDER:
            v = ps.get(st, "not_run")
            row[st] = v
            if v == "failed":
                any_fail = True
        summary_rows.append(row)

    summary_json.write_text(
        json.dumps(
            {
                "scenario_ids": scenario_ids,
                "rows": summary_rows,
                "elapsed_s": round(time.time() - t_pipeline0, 1),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    write_summary_csv(summary_csv, summary_rows)
    print_console_table(summary_rows)

    log.info(
        "Pipeline finished in %.1fs; summary -> %s",
        time.time() - t_pipeline0,
        summary_csv,
    )

    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
