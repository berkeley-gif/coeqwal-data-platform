#!/usr/bin/env python3
"""Print a one-screen summary of the most recent ingest stage(s).

Reads `ingest_state.json`, which carries one block per `gdrive_bulk_download.py`
subcommand (`scan`, `download`). Pass `--stage` to pick which block(s) to
print; default is `download`. `--stage all` prints both, scan first.
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from etl.ingestion.lib.config import INGEST_STATE_PATH  # noqa: E402


STAGES = ("scan", "download")


def _scenarios_as_list(block: Dict[str, Any]) -> List[Dict[str, Any]]:
    scenarios = (block.get("scenarios") or {})
    if isinstance(scenarios, dict):
        return [scenarios[k] for k in sorted(scenarios.keys())]
    # Pre-v2 layout (legacy) stored a list. Keep working until the file
    # is rewritten by the next run.
    return list(scenarios)


def _print_scan(block: Dict[str, Any]) -> None:
    scenarios = _scenarios_as_list(block)
    print(f"Last scan: {block.get('run_at_utc', '?')}")
    print(f"Script:    {block.get('script', '?')} v{block.get('script_version', '?')}")
    print(f"Scenarios: {len(scenarios)}")
    print()
    print(f"  {'ID':<8} {'Access':<6} {'Zips':>4} {'CSVs':>4} {'Match':<8} {'Status'}")
    print("  " + "-" * 70)
    for s in scenarios:
        sid = s.get("scenario_id", "?")
        access = (s.get("access_mode") or "")[:6]
        zips = str(s.get("zip_count", ""))
        csvs = str(s.get("trend_csv_count", ""))
        match = s.get("folder_name_match") or ""
        status = s.get("status") or ""
        print(f"  {sid:<8} {access:<6} {zips:>4} {csvs:>4} {match:<8} {status}")
    print()


def _print_download(block: Dict[str, Any]) -> None:
    scenarios = _scenarios_as_list(block)
    print(f"Last download: {block.get('run_at_utc', '?')}")
    print(f"Script:        {block.get('script', '?')} v{block.get('script_version', '?')}")
    print(f"Scenarios:     {len(scenarios)}")
    print()
    print(f"  {'ID':<8} {'Status':<22} {'ZIP sha256':<14} {'DV sha256':<14} {'SV sha256':<14} {'Trend sha256':<14}")
    print("  " + "-" * 90)
    for s in scenarios:
        sid = s.get("scenario_id", "?")
        status = s.get("validation_status") or s.get("verification_status") or "?"
        zip_sha = (s.get("zip_sha256") or "")[:12]
        dv_sha = (s.get("dv_sha256") or "")[:12]
        sv_sha = (s.get("sv_sha256") or "")[:12]
        trend_sha = (s.get("trend_csv_sha256") or "")[:12]
        print(f"  {sid:<8} {status:<22} {zip_sha:<14} {dv_sha:<14} {sv_sha:<14} {trend_sha:<14}")
    print()


_PRINTERS = {"scan": _print_scan, "download": _print_download}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Show the most recent ingest stage(s) recorded in "
            "ingest_state.json. Pick a stage with --stage."
        )
    )
    parser.add_argument(
        "--stage",
        choices=("scan", "download", "all"),
        default="download",
        help="Which stage block to print (default: download)",
    )
    args = parser.parse_args()

    if not INGEST_STATE_PATH.exists():
        print(
            f"No ingest state at {INGEST_STATE_PATH}. "
            f"Run `gdrive_bulk_download.py scan/download ...` first."
        )
        return 1

    try:
        state = json.loads(INGEST_STATE_PATH.read_text())
    except json.JSONDecodeError:
        print(f"ingest_state.json at {INGEST_STATE_PATH} is not valid JSON.")
        return 1

    stages = STAGES if args.stage == "all" else (args.stage,)
    printed = False
    for stage in stages:
        block = state.get(stage)
        if not block:
            print(f"No {stage} block in ingest_state.json yet. "
                  f"Run `gdrive_bulk_download.py {stage} ...` to populate it.")
            print()
            continue
        if printed and len(stages) > 1:
            print("=" * 100)
        _PRINTERS[stage](block)
        printed = True

    return 0 if printed else 1


if __name__ == "__main__":
    sys.exit(main())
