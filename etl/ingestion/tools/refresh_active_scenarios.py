#!/usr/bin/env python3
"""
refresh_active_scenarios.py - rewrite the active-scenarios block at the
top of etl/README.md from the API.

Calls GET https://api.coeqwal.org/api/scenarios, extracts every short_code
where `is_active` is true, sorts them, and rewrites the block in
etl/README.md between the marker comments:

  <!-- ACTIVE_SCENARIOS:BEGIN -->
  ...
  <!-- ACTIVE_SCENARIOS:END -->

The list is rendered inline, comma-separated. If the markers are missing,
the script errors out rather than guessing where to insert. The script
never edits anything else in the README and never commits to git.

Usage:
  python etl/ingestion/tools/refresh_active_scenarios.py
  python etl/ingestion/tools/refresh_active_scenarios.py --api-url https://api.coeqwal.org
  python etl/ingestion/tools/refresh_active_scenarios.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import List

DEFAULT_API_URL = "https://api.coeqwal.org"
# Path is `etl/README.md` relative to this script in `etl/ingestion/tools/`.
README_PATH = Path(__file__).resolve().parents[2] / "README.md"

BEGIN_MARKER = "<!-- ACTIVE_SCENARIOS:BEGIN -->"
END_MARKER = "<!-- ACTIVE_SCENARIOS:END -->"

log = logging.getLogger("refresh_active_scenarios")


def _fetch_scenarios(api_url: str) -> List[dict]:
    """GET <api>/api/scenarios and return the parsed JSON list."""
    url = api_url.rstrip("/") + "/api/scenarios"
    log.info("GET %s ...", url)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
    except urllib.error.URLError as e:
        raise SystemExit(f"\nFailed to reach API at {url}: {e}\n")
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        raise SystemExit(f"\nAPI returned non-JSON: {e}\n")
    if not isinstance(data, list):
        raise SystemExit(f"\nExpected a list from {url}, got {type(data).__name__}\n")
    return data


def _build_block(short_codes: List[str], api_url: str) -> str:
    """Render the new ACTIVE_SCENARIOS block."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    inline = ", ".join(short_codes) if short_codes else "(none)"
    return (
        f"{BEGIN_MARKER}\n"
        f"\n"
        f"**Active scenarios ({len(short_codes)})**: {inline}\n"
        f"\n"
        f"_Last refreshed {now} from `{api_url}/api/scenarios`. "
        f"Regenerate with `python etl/ingestion/tools/refresh_active_scenarios.py`._\n"
        f"\n"
        f"{END_MARKER}"
    )


def _replace_block(content: str, new_block: str) -> str:
    """Replace the existing ACTIVE_SCENARIOS block with new_block."""
    begin_idx = content.find(BEGIN_MARKER)
    end_idx = content.find(END_MARKER)
    if begin_idx == -1 or end_idx == -1:
        raise SystemExit(
            f"\nMarkers not found in {README_PATH}.\n"
            f"Expected both '{BEGIN_MARKER}' and '{END_MARKER}'.\n"
            f"Add an empty block to the README and re-run this script.\n"
        )
    if end_idx < begin_idx:
        raise SystemExit(
            f"\nMarker order is wrong in {README_PATH} (END before BEGIN). Fix manually.\n"
        )
    end_full = end_idx + len(END_MARKER)
    return content[:begin_idx] + new_block + content[end_full:]


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    parser = argparse.ArgumentParser(
        description="Refresh the active-scenarios block in etl/README.md from the live API."
    )
    parser.add_argument("--api-url", default=DEFAULT_API_URL,
                        help=f"API base URL (default: {DEFAULT_API_URL})")
    parser.add_argument("--readme", default=str(README_PATH),
                        help=f"Path to the README to edit (default: {README_PATH})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the new block and the patched README to stdout, don't write.")
    args = parser.parse_args()

    scenarios = _fetch_scenarios(args.api_url)
    active = sorted(
        s["short_code"]
        for s in scenarios
        if isinstance(s, dict) and s.get("is_active") and s.get("short_code")
    )
    log.info("Fetched %d scenarios; %d are active", len(scenarios), len(active))

    new_block = _build_block(active, args.api_url)

    readme = Path(args.readme)
    if not readme.exists():
        raise SystemExit(f"\nREADME not found at {readme}\n")
    current = readme.read_text()
    new_content = _replace_block(current, new_block)

    if args.dry_run:
        print("--- new block ---")
        print(new_block)
        if current == new_content:
            print("\n(no change required)")
        else:
            print("\n(README would change)")
        return

    if current == new_content:
        log.info("No change required; README is already up to date.")
        return

    readme.write_text(new_content)
    log.info("Updated %s with %d active scenarios", readme, len(active))


if __name__ == "__main__":
    main()
