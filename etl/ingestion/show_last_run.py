#!/usr/bin/env python3
"""Print a one-screen summary of the scenarios processed in the last
gdrive_bulk_download.py download run. Reads audit_state.json."""
import json
import sys
from pathlib import Path

STATE_PATH = Path(__file__).parent / "output" / "audit_state.json"


def main() -> int:
    if not STATE_PATH.exists():
        print(f"No audit state at {STATE_PATH}. Run `gdrive_bulk_download.py download ...` first.")
        return 1

    state = json.loads(STATE_PATH.read_text())
    scenarios = state.get("scenarios", [])

    print(f"Last run:  {state.get('run_at_utc', '?')}")
    print(f"Script:    {state.get('script', '?')} v{state.get('script_version', '?')}")
    print(f"Scenarios: {len(scenarios)}")
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
