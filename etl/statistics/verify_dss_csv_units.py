#!/usr/bin/env python3
"""
Independent DSS-vs-CSV unit verification.

Downloads the original DSS file (from the staging ZIP in S3) and the
extracted CSV for each scenario, then compares the unit metadata from
the DSS against the unit declared in the CSV header row.

This is the ground-truth check: no rules or expectations — just
"what did the DSS say?" vs "what ended up in the CSV?".

Requires pydsstools (available on Cloud9 / the Batch container).

Usage:
    python verify_dss_csv_units.py                         # all scenarios
    python verify_dss_csv_units.py --scenario s0025        # one scenario
    python verify_dss_csv_units.py --workers 4             # parallel
    python verify_dss_csv_units.py --output report.csv     # save CSV report
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import zipfile
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("verify_units")

BUCKET = "coeqwal-model-run"

# Same unit-sanitization as dss_to_csv.py
_UNIT_STRIP_RE = re.compile(r"[{}\[\]()]+")


def _sanitize_unit(raw: str) -> str:
    return _UNIT_STRIP_RE.sub("", raw).strip().upper()


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _csv_key(scenario_id: str) -> str:
    return f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv"


def _find_run_zip(s3, scenario_id: str) -> Optional[str]:
    """Find the ZIP key under scenario/{scenario_id}/run/ in S3."""
    prefix = f"scenario/{scenario_id}/run/"
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    zips = [obj["Key"] for obj in resp.get("Contents", [])
            if obj["Key"].lower().endswith(".zip")]
    if not zips:
        return None
    # If multiple, pick the most recently modified
    if len(zips) == 1:
        return zips[0]
    # list_objects_v2 returns LastModified; re-fetch with detail
    best = max(
        resp["Contents"],
        key=lambda o: o.get("LastModified", ""),
    )
    return best["Key"] if best["Key"].lower().endswith(".zip") else zips[0]


# ---------------------------------------------------------------------------
# DSS classification (mirrors classify_dss.py logic)
# ---------------------------------------------------------------------------

_GW_BASENAMES = ("cvgroundwaterbudget.dss", "cvgroundwaterout.dss")
_EXCLUDED = ("archive", "discard", "old", "backup")


def _classify_dss_paths(paths: List[str]) -> Optional[str]:
    """Pick the CalSim output DSS from a list of paths inside the ZIP."""
    cal_candidates = []
    for p in paths:
        parts = p.replace("\\", "/").lower().split("/")
        if any(part in _EXCLUDED for part in parts):
            continue
        basename = parts[-1] if parts else ""
        if basename in _GW_BASENAMES:
            continue
        slug = "/" + "/".join(parts) + "/"
        if "/dss/output/" in slug:
            cal_candidates.append(p)

    if not cal_candidates:
        for p in paths:
            parts = p.replace("\\", "/").lower().split("/")
            if any(part in _EXCLUDED for part in parts):
                continue
            basename = parts[-1] if parts else ""
            if basename in _GW_BASENAMES:
                continue
            if "_dv" in basename or any(tok in basename for tok in ("out", "output", "results")):
                cal_candidates.append(p)

    if not cal_candidates:
        return None

    for p in cal_candidates:
        if "_dv" in os.path.basename(p).lower():
            return p
    for p in cal_candidates:
        b = os.path.basename(p).lower()
        if any(tok in b for tok in ("out", "output", "results")):
            return p
    return cal_candidates[0] if len(cal_candidates) == 1 else None


# ---------------------------------------------------------------------------
# Core verification
# ---------------------------------------------------------------------------

def _read_dss_units(dss_path: str) -> Dict[str, Dict[str, str]]:
    """Open DSS and return {b_part: {c_part: unit}} for all pathnames.

    Import pydsstools lazily so the module can be imported on any platform
    (it only fails at runtime if the DSS isn't available).
    """
    from pydsstools.heclib.dss import HecDss  # noqa: PLC0415

    result: Dict[str, Dict[str, str]] = {}
    dss = HecDss.Open(dss_path)
    try:
        pathnames = dss.getPathnameList("/*/*/*/*/*/*/")
        for pathname in pathnames:
            parts = pathname.split("/")
            if len(parts) < 7:
                continue
            b, c = parts[2], parts[3]
            try:
                data = dss.read_ts(pathname)
                unit = _sanitize_unit(getattr(data, "units", ""))
            except Exception:
                unit = "?READ_ERROR?"
            result.setdefault(b, {})[c] = unit
    finally:
        dss.close()
    return result


def _read_csv_units(s3, scenario_id: str) -> Dict[str, Dict[str, str]]:
    """Read CSV header from S3 and return {b_part: {c_part: unit}}."""
    key = _csv_key(scenario_id)
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    hdr = pd.read_csv(io.BytesIO(raw), header=None, nrows=7, low_memory=False)

    b_row = [str(v) for v in hdr.iloc[1].tolist()]
    c_row = [str(v) for v in hdr.iloc[2].tolist()]
    units_row = [str(u).strip().upper() for u in hdr.iloc[6].tolist()]

    result: Dict[str, Dict[str, str]] = {}
    for b, c, unit in zip(b_row, c_row, units_row):
        if b == "b" or b == "DateTime":
            continue
        result.setdefault(b, {})[c] = unit
    return result


def verify_scenario(
    scenario_id: str,
) -> Tuple[str, int, int, List[Dict]]:
    """Verify one scenario. Returns (scenario_id, total_checked, mismatches_count, details).

    Each detail dict: {b_part, c_part, dss_unit, csv_unit}.
    """
    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp(prefix=f"verify_{scenario_id}_")

    try:
        # 1. Find and download the model run ZIP
        zip_key = _find_run_zip(s3, scenario_id)
        if not zip_key:
            log.warning("%s: no ZIP found in s3://%s/scenario/%s/run/",
                        scenario_id, BUCKET, scenario_id)
            return scenario_id, 0, 0, []

        log.info("%s: downloading %s ...", scenario_id, zip_key)
        zip_local = os.path.join(tmp_dir, "input.zip")
        s3.download_file(BUCKET, zip_key, zip_local)

        # 2. Extract and find the CalSim output DSS
        with zipfile.ZipFile(zip_local, "r") as zf:
            dss_names = [n for n in zf.namelist() if n.lower().endswith(".dss")]
            dv_path = _classify_dss_paths(dss_names)
            if not dv_path:
                log.warning("%s: could not identify CalSim output DSS in ZIP", scenario_id)
                return scenario_id, 0, 0, []
            zf.extractall(tmp_dir)

        dss_local = os.path.join(tmp_dir, dv_path)
        if not os.path.isfile(dss_local):
            log.error("%s: extracted DSS not found at %s", scenario_id, dss_local)
            return scenario_id, 0, 0, []

        log.info("%s: opening DSS (%s) ...", scenario_id, dv_path)
        dss_units = _read_dss_units(dss_local)

        # 3. Read CSV header
        log.info("%s: reading CSV header ...", scenario_id)
        csv_units = _read_csv_units(s3, scenario_id)

        # 4. Compare
        mismatches = []
        checked = 0
        for b_part, c_map in dss_units.items():
            csv_c_map = csv_units.get(b_part, {})
            for c_part, dss_unit in c_map.items():
                csv_unit = csv_c_map.get(c_part)
                if csv_unit is None:
                    continue
                checked += 1
                if dss_unit != csv_unit:
                    mismatches.append({
                        "scenario": scenario_id,
                        "b_part": b_part,
                        "c_part": c_part,
                        "dss_unit": dss_unit,
                        "csv_unit": csv_unit,
                    })

        if mismatches:
            log.warning("%s: %d mismatch(es) out of %d checked",
                        scenario_id, len(mismatches), checked)
        else:
            log.info("%s: all %d series match", scenario_id, checked)

        return scenario_id, checked, len(mismatches), mismatches

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Independent DSS-vs-CSV unit verification"
    )
    parser.add_argument(
        "--scenario", "-s",
        help="Verify a single scenario (default: all known scenarios)",
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=1,
        help="Parallel workers (default: 1 — DSS reads are CPU-bound, keep low)",
    )
    parser.add_argument(
        "--output", "-o", default="verify_dss_csv_units_report.csv",
        help="Output CSV path for mismatches (default: verify_dss_csv_units_report.csv)",
    )
    args = parser.parse_args()

    if args.scenario:
        scenarios = [args.scenario]
    else:
        try:
            from scenarios import SCENARIOS
            scenarios = list(SCENARIOS)
        except ImportError:
            log.error("Cannot import SCENARIOS — use --scenario or ensure scenarios.py is importable")
            sys.exit(1)

    workers = max(1, args.workers)
    log.info("Verifying %d scenario(s) with %d worker(s)", len(scenarios), workers)

    t0 = time.time()
    all_mismatches: List[Dict] = []
    summary: List[Tuple[str, int, int]] = []

    if workers == 1:
        for sid in scenarios:
            sid, checked, n_bad, details = verify_scenario(sid)
            summary.append((sid, checked, n_bad))
            all_mismatches.extend(details)
    else:
        from concurrent.futures import ProcessPoolExecutor, as_completed
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(verify_scenario, sid): sid for sid in scenarios}
            for fut in as_completed(futs):
                try:
                    sid, checked, n_bad, details = fut.result()
                    summary.append((sid, checked, n_bad))
                    all_mismatches.extend(details)
                except Exception as e:
                    sid = futs[fut]
                    log.error("%s: verification failed — %s", sid, e)
                    summary.append((sid, 0, -1))

    elapsed = time.time() - t0

    # ---- Summary ----
    print(f"\n{'=' * 70}")
    print("  DSS-vs-CSV UNIT VERIFICATION")
    print(f"{'=' * 70}")
    print(f"  Scenarios checked: {len(summary)}")
    total_checked = sum(c for _, c, _ in summary)
    total_mismatches = sum(m for _, _, m in summary if m >= 0)
    print(f"  Total series compared: {total_checked}")
    print(f"  Total mismatches: {total_mismatches}")
    print(f"  Elapsed: {elapsed:.1f}s")
    print()

    clean = [(s, c) for s, c, m in summary if m == 0 and c > 0]
    bad = [(s, c, m) for s, c, m in summary if m > 0]
    skipped = [(s,) for s, c, m in summary if c == 0 and m == 0]
    errored = [(s,) for s, c, m in summary if m < 0]

    if bad:
        print("  MISMATCHES:")
        print(f"  {'─' * 66}")
        for s, c, m in sorted(bad):
            print(f"    {s}: {m} mismatch(es) / {c} checked")
        print()
        print("  MISMATCH DETAILS:")
        print(f"  {'─' * 66}")
        for row in sorted(all_mismatches, key=lambda r: (r["scenario"], r["b_part"])):
            print(f"    {row['scenario']}  {row['b_part']} (C={row['c_part']}): "
                  f"DSS={row['dss_unit']!r}  CSV={row['csv_unit']!r}")
        print()

    if clean:
        print(f"  CLEAN ({len(clean)} scenarios — all units match):")
        print(f"  {'─' * 66}")
        for s, c in sorted(clean):
            print(f"    {s}: {c} series verified")
        print()

    if skipped:
        print(f"  SKIPPED ({len(skipped)} scenarios — no ZIP or CSV found):")
        for (s,) in sorted(skipped):
            print(f"    {s}")
        print()

    if errored:
        print(f"  ERRORS ({len(errored)} scenarios):")
        for (s,) in sorted(errored):
            print(f"    {s}")
        print()

    print(f"{'=' * 70}\n")

    # ---- Write CSV ----
    if all_mismatches:
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "scenario", "b_part", "c_part", "dss_unit", "csv_unit",
            ])
            writer.writeheader()
            for row in sorted(all_mismatches, key=lambda r: (r["scenario"], r["b_part"])):
                writer.writerow(row)
        log.info("Mismatch report written to %s", args.output)
    else:
        log.info("No mismatches found — no report file written.")

    if total_mismatches > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
