#!/usr/bin/env python3
"""
status.py - ETL system status: one-screen snapshot of ingestion, Batch, statistics,
tiers, verification, and connectivity. Read-only, no flags.

Run from the repo root on Cloud9 or locally:

    python etl/status.py

Exit 0 if every connectivity check passes, 1 if any connectivity check fails.
Stale or missing local artifacts (no recent ingest run, no stats audit, etc.)
are reported but do not affect the exit code. The intent is "is the system
reachable" rather than "is the system idle".
"""

from __future__ import annotations

import csv
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(REPO_ROOT))
from etl.common import S3_BUCKET, BATCH_QUEUE  # noqa: E402
from etl.ingestion.lib.config import (  # noqa: E402
    INGEST_STATE_PATH,
    WORKING_CSV_PATH,
)

PIPELINE_RUNS_DIR = REPO_ROOT / "etl" / "ingestion" / "audit_reports" / "pipeline_runs"
STATS_AUDIT_DIR = REPO_ROOT / "etl" / "statistics" / "audit_reports"
VERIFICATION_DIR = REPO_ROOT / "audits" / "verification_reports"

TIER_LOADER_PATH = REPO_ROOT / "etl" / "tier_data" / "scripts" / "load_all_tier_results.py"

# ANSI color helpers. Disabled when stdout is not a tty.
_USE_COLOR = sys.stdout.isatty()


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def green(text: str) -> str:
    return _c("32", text)


def yellow(text: str) -> str:
    return _c("33", text)


def red(text: str) -> str:
    return _c("31", text)


def bold(text: str) -> str:
    return _c("1", text)


_FAIL_COUNT = 0


def section(title: str) -> None:
    print(f"\n{bold(title)}")


def line(label: str, value: str) -> None:
    print(f"  {label:<24} {value}")


def info(label: str, value: str) -> None:
    line(label, value)


def ok(label: str, value: str = "OK") -> None:
    line(label, green(value))


def warn(label: str, value: str) -> None:
    line(label, yellow(value))


def fail(label: str, value: str) -> None:
    global _FAIL_COUNT
    _FAIL_COUNT += 1
    line(label, red(value))


def _ago(then: datetime) -> str:
    """Render a UTC timestamp plus a human-readable lag from now."""
    now = datetime.now(timezone.utc)
    delta = now - then
    secs = int(delta.total_seconds())
    if secs < 60:
        rel = f"{secs}s ago"
    elif secs < 3600:
        rel = f"{secs // 60}m ago"
    elif secs < 86400:
        rel = f"{secs // 3600}h ago"
    else:
        rel = f"{secs // 86400}d ago"
    return f"{then.strftime('%Y-%m-%d %H:%M UTC')} ({rel})"


def _parse_iso(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _latest(pattern_dir: Path, glob: str) -> Optional[Path]:
    if not pattern_dir.exists():
        return None
    matches = sorted(pattern_dir.glob(glob), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


def section_ingestion() -> None:
    section("Ingestion")

    working_csv = REPO_ROOT / WORKING_CSV_PATH
    if working_csv.exists():
        with open(working_csv, newline="") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # header
                active = sum(1 for _ in reader)
            except StopIteration:
                active = 0
        info("Working CSV", f"{WORKING_CSV_PATH} ({active} rows)")
    else:
        warn("Working CSV", f"missing: {WORKING_CSV_PATH}")

    if INGEST_STATE_PATH.exists():
        import json
        try:
            state = json.loads(INGEST_STATE_PATH.read_text())
            download = state.get("download") or {}
            scen_count = len(download.get("scenarios") or download)
            mtime = datetime.fromtimestamp(INGEST_STATE_PATH.stat().st_mtime, timezone.utc)
            info("Last download run", f"{_ago(mtime)} ({scen_count} scenarios in state)")
        except Exception as exc:
            warn("Last download run", f"unreadable: {exc}")
    else:
        info("Last download run", "no ingest_state.json (never run, or fresh checkout)")

    latest_pipeline = None
    if PIPELINE_RUNS_DIR.exists():
        runs = sorted([p for p in PIPELINE_RUNS_DIR.iterdir() if p.is_dir()],
                      key=lambda p: p.stat().st_mtime, reverse=True)
        latest_pipeline = runs[0] if runs else None
    if latest_pipeline:
        mtime = datetime.fromtimestamp(latest_pipeline.stat().st_mtime, timezone.utc)
        info("Last pipeline run", f"{_ago(mtime)}  -> {latest_pipeline.relative_to(REPO_ROOT)}")
    else:
        info("Last pipeline run", "no orchestrator runs yet")


def section_batch() -> None:
    section("Batch (AWS)")
    try:
        import boto3
        client = boto3.client("batch")
        active = 0
        for status in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"):
            resp = client.list_jobs(jobQueue=BATCH_QUEUE, jobStatus=status)
            active += len(resp.get("jobSummaryList", []))
        ok("Active jobs", f"{active} in {BATCH_QUEUE}")

        succeeded = client.list_jobs(jobQueue=BATCH_QUEUE, jobStatus="SUCCEEDED")
        failed = client.list_jobs(jobQueue=BATCH_QUEUE, jobStatus="FAILED")
        cutoff_ms = (datetime.now(timezone.utc).timestamp() - 86400) * 1000
        n_succ = sum(1 for j in succeeded.get("jobSummaryList", []) if (j.get("stoppedAt") or 0) >= cutoff_ms)
        n_fail = sum(1 for j in failed.get("jobSummaryList", []) if (j.get("stoppedAt") or 0) >= cutoff_ms)
        info("Last 24h", f"{n_succ} SUCCEEDED, {n_fail} FAILED")
    except Exception as exc:
        warn("Batch query", f"skipped: {type(exc).__name__}: {exc}")


def section_statistics() -> None:
    section("Statistics")
    latest = _latest(STATS_AUDIT_DIR, "stats_audit_*.csv")
    if latest:
        mtime = datetime.fromtimestamp(latest.stat().st_mtime, timezone.utc)
        try:
            with open(latest, newline="") as f:
                rows = sum(1 for _ in csv.reader(f)) - 1
        except Exception:
            rows = -1
        rows_str = f"{rows} rows" if rows >= 0 else "row count unavailable"
        info("Last stats audit", f"{_ago(mtime)}  -> {latest.relative_to(REPO_ROOT)} ({rows_str})")
    else:
        info("Last stats audit", "no stats_audit_*.csv yet")


def section_tiers() -> None:
    section("Tiers")
    if TIER_LOADER_PATH.exists():
        text = TIER_LOADER_PATH.read_text()
        for line_ in text.splitlines():
            if line_.startswith("TIER_VERSION_ID"):
                info("Loader tier_version_id", line_.split("=", 1)[1].strip())
                break
        else:
            warn("Loader tier_version_id", "constant not found in load_all_tier_results.py")
    else:
        warn("Loader tier_version_id", "load_all_tier_results.py missing")
    info("Last load timestamp", "query the DB to confirm (audit_log on tier_location_result)")


def section_verification() -> None:
    section("Verification")
    if VERIFICATION_DIR.exists():
        reports = sorted(VERIFICATION_DIR.glob("*.json"),
                         key=lambda p: p.stat().st_mtime, reverse=True)
        if reports:
            latest = reports[0]
            mtime = datetime.fromtimestamp(latest.stat().st_mtime, timezone.utc)
            info("Last report", f"{_ago(mtime)}  -> {latest.relative_to(REPO_ROOT)}")
            info("Reports on disk", f"{len(reports)} in {VERIFICATION_DIR.relative_to(REPO_ROOT)}")
        else:
            info("Last report", f"empty: {VERIFICATION_DIR.relative_to(REPO_ROOT)}")
    else:
        info("Last report", f"no directory: {VERIFICATION_DIR.relative_to(REPO_ROOT)}")


def _ping_rds() -> Tuple[bool, str]:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return False, "DATABASE_URL not set"
    try:
        import psycopg2
    except ImportError:
        return False, "psycopg2 not installed (pip install -r requirements.txt)"
    try:
        with psycopg2.connect(db_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, "1 row returned"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _ping_aws() -> Tuple[bool, str]:
    try:
        import boto3
        sts = boto3.client("sts")
        ident = sts.get_caller_identity()
        return True, ident.get("Arn", "(no Arn)")
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _ping_s3() -> Tuple[bool, str]:
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_bucket(Bucket=S3_BUCKET)
        return True, f"s3://{S3_BUCKET} reachable"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _ping_rclone() -> Tuple[bool, str]:
    if not shutil.which("rclone"):
        return False, "rclone not installed"
    try:
        result = subprocess.run(
            ["rclone", "lsd", "gdrive:", "--max-depth", "1"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            return True, "gdrive: listable"
        return False, (result.stderr or result.stdout).strip().splitlines()[-1] if (result.stderr or result.stdout) else "lsd failed"
    except subprocess.TimeoutExpired:
        return False, "timed out after 15s"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def section_connectivity() -> None:
    section("Connectivity")
    for label, fn in (
        ("AWS sts", _ping_aws),
        ("S3", _ping_s3),
        ("RDS", _ping_rds),
        ("rclone gdrive", _ping_rclone),
    ):
        ok_, msg = fn()
        if ok_:
            ok(label, msg)
        else:
            fail(label, msg)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> int:
    if any(a in sys.argv[1:] for a in ("-h", "--help")):
        print(__doc__.strip() if __doc__ else "etl/status.py: read-only ETL system snapshot")
        return 0

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(bold(f"ETL System Status ({now})"))
    print("=" * 48)

    section_ingestion()
    section_batch()
    section_statistics()
    section_tiers()
    section_verification()
    section_connectivity()

    print()
    if _FAIL_COUNT == 0:
        print(green("All connectivity checks passed."))
        return 0
    print(red(f"{_FAIL_COUNT} connectivity check(s) failed."))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
