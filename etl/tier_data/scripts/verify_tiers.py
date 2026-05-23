#!/usr/bin/env python3
"""
verify_tiers.py - Verify tier data: compare staging CSVs against live API responses.

For each tier outcome and scenario, this script:
  1. Reads the staging CSV and computes expected tier_result values
     (using the same logic as load_all_tier_results.py)
  2. Fetches the corresponding data from the API
  3. Reports mismatches

Usage:
    python verify_tiers.py
    python verify_tiers.py --api-url https://api.coeqwal.org/api
    python verify_tiers.py --scenario s0020
    python verify_tiers.py --tier CWS_DEL
    python verify_tiers.py --scenarios-override s0070,s0072
"""

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.request import urlopen, Request
from urllib.error import URLError

import pandas as pd

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from etl.common import (  # noqa: E402
    assess_coverage,
    format_coverage_warnings,
    get_db_connection,
    resolve_active_scenarios,
)
from etl.common.active_scenarios import ACTIVE_SCENARIOS  # noqa: E402
from etl.tier_data.staging_inventory import (  # noqa: E402
    parse_res_stor_column as _res_stor_location_id,
)

# Rebound inside main() when --scenarios-override is passed
ALLOWED_SCENARIOS: frozenset[str] = ACTIVE_SCENARIOS

STAGING_DIR = Path(__file__).parent.parent / "staging"

API_URL_DEFAULT = "https://api.coeqwal.org/api"


def api_get(base_url: str, path: str):
    url = f"{base_url}{path}"
    req = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except URLError as e:
        print(f"  API ERROR: {url} -> {e}")
        return None


def normalize_scenario_id(raw) -> str:
    s = str(raw).strip()
    if s.startswith("s"):
        return s
    return f"s{int(s):04d}"


# ---------------------------------------------------------------------------
# CSV parsers.mirror load_all_tier_results.py logic
# ---------------------------------------------------------------------------


def parse_cws_del() -> dict:
    path = STAGING_DIR / "CWS_DEL.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    results = {}
    for _, row in df.iterrows():
        sid = normalize_scenario_id(row.get("scenario_id", row.iloc[0]))
        if sid not in ALLOWED_SCENARIOS:
            continue
        counts = Counter()
        locations = {}
        for col in df.columns[1:]:
            val = row[col]
            if pd.isna(val) or str(val).strip().upper() == "NA":
                continue
            tier = int(float(val))
            counts[tier] += 1
            locations[col] = tier
        total = sum(counts.values())
        results[sid] = {
            "tier_1": counts.get(1, 0),
            "tier_2": counts.get(2, 0),
            "tier_3": counts.get(3, 0),
            "tier_4": counts.get(4, 0),
            "total": total,
            "locations": locations,
        }
    return results


def parse_ag_rev() -> dict:
    path = STAGING_DIR / "AG_REV.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    results = {}

    is_long_format = "region" in df.columns and "tier" in df.columns

    if is_long_format:
        for sid, group in df.groupby("scenario"):
            sid = normalize_scenario_id(sid)
            if sid not in ALLOWED_SCENARIOS:
                continue
            counts = Counter()
            locations = {}
            for _, row in group.iterrows():
                tier = int(row["tier"])
                region = str(row["region"])
                counts[tier] += 1
                locations[region] = tier
            total = sum(counts.values())
            results[sid] = {
                "tier_1": counts.get(1, 0),
                "tier_2": counts.get(2, 0),
                "tier_3": counts.get(3, 0),
                "tier_4": counts.get(4, 0),
                "total": total,
                "locations": locations,
            }
    else:
        scenario_col = df.columns[0]
        du_columns = [c for c in df.columns[1:] if c]
        for _, row in df.iterrows():
            sid = normalize_scenario_id(row[scenario_col])
            if sid not in ALLOWED_SCENARIOS:
                continue
            counts = Counter()
            locations = {}
            for du_id in du_columns:
                val = row[du_id]
                if pd.isna(val) or str(val).strip().upper() == "NA":
                    continue
                tier = int(float(val))
                counts[tier] += 1
                locations[str(du_id)] = tier
            total = sum(counts.values())
            results[sid] = {
                "tier_1": counts.get(1, 0),
                "tier_2": counts.get(2, 0),
                "tier_3": counts.get(3, 0),
                "tier_4": counts.get(4, 0),
                "total": total,
                "locations": locations,
            }
    return results


def _discover_env_flows_files() -> list:
    """Mirror load_all_tier_results._discover_env_flows_files ordering."""
    priority = {"historical": 0, "cc50": 1, "cc95": 2}

    def sort_key(p):
        name_lower = p.stem.lower()
        for tag, order in priority.items():
            if tag in name_lower:
                return order
        return 99

    files = []
    legacy = STAGING_DIR / "ENV_FLOWS.csv"
    if legacy.exists():
        files.append(legacy)
    files.extend(sorted(STAGING_DIR.glob("ENV_FLOWS_*.csv"), key=sort_key))
    return files


def _load_one_env_flows_frame(path) -> "pd.DataFrame":
    """
    Return a DataFrame with index=stations, columns=scenario IDs.

    Auto-detects orientation: if the first column's values look like scenario
    IDs (s0xxx) we transpose; otherwise we strip any "(tag)" suffix from
    column headers to recover clean scenario IDs.
    """
    df = pd.read_csv(path, index_col=0)
    first_vals = [str(v) for v in df.index[:5]]
    rows_are_scenarios = bool(first_vals) and all(
        v.startswith("s0") for v in first_vals
    )
    if rows_are_scenarios:
        return df.T

    rename = {}
    for col in df.columns:
        base = col.split("(")[0].strip()
        if base and base not in rename.values():
            rename[col] = base
    return df.rename(columns=rename)


def parse_env_flows() -> dict:
    files = _discover_env_flows_files()
    if not files:
        return {}

    # Later files (cc50, cc95) overwrite earlier ones (historical) for
    # overlapping scenarios, matching load_all_tier_results.py semantics.
    results = {}
    for path in files:
        df = _load_one_env_flows_frame(path)
        for col in df.columns:
            sid = normalize_scenario_id(str(col).strip())
            if sid not in ALLOWED_SCENARIOS:
                continue
            counts = Counter()
            locations = {}
            for station, val in df[col].items():
                if pd.isna(val):
                    continue
                tier = int(float(val))
                counts[tier] += 1
                locations[str(station)] = tier
            total = sum(counts.values())
            results[sid] = {
                "tier_1": counts.get(1, 0),
                "tier_2": counts.get(2, 0),
                "tier_3": counts.get(3, 0),
                "tier_4": counts.get(4, 0),
                "total": total,
                "locations": locations,
            }
    return results


def parse_res_stor() -> dict:
    path = STAGING_DIR / "RES_STOR.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    results = {}
    for _, row in df.iterrows():
        sid = normalize_scenario_id(row["Scenario"])
        if sid not in ALLOWED_SCENARIOS:
            continue
        counts = Counter()
        locations = {}
        for col in df.columns:
            if col == "Scenario":
                continue
            loc_id = _res_stor_location_id(col)
            val = row[col]
            if pd.isna(val):
                continue
            tier = int(float(val))
            counts[tier] += 1
            locations[loc_id] = tier
        total = sum(counts.values())
        results[sid] = {
            "tier_1": counts.get(1, 0),
            "tier_2": counts.get(2, 0),
            "tier_3": counts.get(3, 0),
            "tier_4": counts.get(4, 0),
            "total": total,
            "locations": locations,
        }
    return results


def parse_gw_stor() -> dict:
    path = STAGING_DIR / "GW_STOR.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    results = {}
    for _, row in df.iterrows():
        sid = normalize_scenario_id(row["scenario"])
        if sid not in ALLOWED_SCENARIOS:
            continue
        counts = Counter()
        for col in df.columns[1:]:
            val = row[col]
            if pd.isna(val):
                continue
            tier = int(val)
            if tier == 0:
                tier = 1
            counts[tier] += 1
        total = sum(counts.values())
        results[sid] = {
            "tier_1": counts.get(1, 0),
            "tier_2": counts.get(2, 0),
            "tier_3": counts.get(3, 0),
            "tier_4": counts.get(4, 0),
            "total": total,
        }
    return results


def parse_single_value(filename: str, scenario_col: str, tier_col: str) -> dict:
    path = STAGING_DIR / filename
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    results = {}
    for _, row in df.iterrows():
        sid = normalize_scenario_id(row[scenario_col])
        if sid not in ALLOWED_SCENARIOS:
            continue
        results[sid] = {"level": int(row[tier_col])}
    return results


def _parse_tier_range(raw) -> int:
    """Mirror load_all_tier_results._parse_tier_range: 'Tier 4' or 4 -> 4."""
    if pd.isna(raw):
        raise ValueError("Tier_range is NaN")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return int(raw)
    s = str(raw).strip()
    if s.isdigit():
        return int(s)
    parts = s.split()
    if len(parts) == 2 and parts[0].lower() == "tier" and parts[1].isdigit():
        return int(parts[1])
    raise ValueError(f"Cannot parse Tier_range: {raw!r}")


def parse_wrc_salmon_ab() -> dict:
    """
    WRC_SALMON_AB is a single-value tier (one integer level per scenario),
    stored wide with columns: scenario, Hydroclimate, Tier_range, tier_score_cont.
    """
    path = STAGING_DIR / "WRC_SALMON_AB.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if "scenario" not in df.columns or "Tier_range" not in df.columns:
        print(
            f"  WRC_SALMON_AB: unexpected columns {list(df.columns)}, skipping"
        )
        return {}

    results = {}
    parse_errors = []
    for _, row in df.iterrows():
        sid = normalize_scenario_id(row["scenario"])
        if sid not in ALLOWED_SCENARIOS:
            continue
        try:
            level = _parse_tier_range(row["Tier_range"])
        except ValueError as exc:
            parse_errors.append(f"{sid}: {exc}")
            continue
        results[sid] = {"level": level}

    if parse_errors:
        print(f"  WRC_SALMON_AB parse errors: {'; '.join(parse_errors[:5])}")

    missing = sorted(ALLOWED_SCENARIOS - set(results.keys()))
    if missing:
        print(
            f"  WRC_SALMON_AB: no salmon data in CSV for "
            f"{len(missing)} allowed scenarios (skipping): "
            f"{', '.join(missing[:10])}{'...' if len(missing) > 10 else ''}"
        )
    return results


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_multi_value(tier_code: str, expected: dict, api_data: dict, report: list):
    for sid, exp in sorted(expected.items()):
        api_scenario = api_data.get(sid)
        if not api_scenario:
            report.append(f"  MISSING {tier_code} {sid}: not in API")
            continue
        tier_info = api_scenario.get("tiers", {}).get(tier_code)
        if not tier_info:
            report.append(f"  MISSING {tier_code} {sid}: tier not in API response")
            continue

        api_values = {}
        for item in tier_info.get("data", []):
            tier_num = int(item["tier"].replace("tier", ""))
            api_values[tier_num] = item["value"]
        api_total = tier_info.get("total")

        mismatches = []
        for t in range(1, 5):
            exp_val = exp.get(f"tier_{t}", 0)
            api_val = api_values.get(t, 0)
            if exp_val != api_val:
                mismatches.append(f"tier_{t}: CSV={exp_val} API={api_val}")

        if exp["total"] != api_total:
            mismatches.append(f"total: CSV={exp['total']} API={api_total}")

        if mismatches:
            report.append(f"  MISMATCH {tier_code} {sid}: {', '.join(mismatches)}")
        else:
            report.append(
                f"  OK {tier_code} {sid}: {exp['total']} locations, tiers match"
            )


def verify_single_value(tier_code: str, expected: dict, api_data: dict, report: list):
    for sid, exp in sorted(expected.items()):
        api_scenario = api_data.get(sid)
        if not api_scenario:
            report.append(f"  MISSING {tier_code} {sid}: not in API")
            continue
        tier_info = api_scenario.get("tiers", {}).get(tier_code)
        if not tier_info:
            report.append(f"  MISSING {tier_code} {sid}: tier not in API response")
            continue

        api_level = tier_info.get("level")
        if exp["level"] != api_level:
            report.append(
                f"  MISMATCH {tier_code} {sid}: CSV={exp['level']} API={api_level}"
            )
        else:
            report.append(f"  OK {tier_code} {sid}: level {exp['level']}")


def render_scorecard(
    per_tier: List[Dict[str, object]],
    json_path: Optional[Path],
    file_obj=sys.stdout,
) -> None:
    """One-screen PASS / FAIL summary, one row per tier code.

    Each row reports the total number of staging-vs-API comparisons, the
    count of mismatches plus missing values, and up to three scenario
    examples for failing tiers.
    """
    if not per_tier:
        print("No tier checks ran.", file=file_obj)
        return

    print("\nverify_tiers.py", file=file_obj)
    print("===============", file=file_obj)

    overall_pass = 0
    overall_total = 0
    for entry in per_tier:
        tier_code = entry["tier_code"]
        n_ok = int(entry["ok"])
        n_mis = int(entry["mismatch"])
        n_missing = int(entry["missing"])
        n_total = n_ok + n_mis + n_missing
        n_bad = n_mis + n_missing
        overall_total += 1
        if n_bad == 0:
            overall_pass += 1
            print(
                f"PASS {tier_code:<14} ({n_total} checks, 0 mismatches)",
                file=file_obj,
            )
        else:
            examples = ", ".join(entry["bad_examples"])  # type: ignore[arg-type]
            ellipsis = ", ..." if n_bad > 3 else ""
            print(
                f"FAIL {tier_code:<14} ({n_total} checks, {n_bad} issues: {examples}{ellipsis})",
                file=file_obj,
            )

    n_fail_sections = overall_total - overall_pass
    print(file=file_obj)
    if n_fail_sections == 0:
        print(
            f"Overall: {overall_pass}/{overall_total} tiers PASS.",
            file=file_obj,
        )
    else:
        print(
            f"Overall: {overall_pass}/{overall_total} tiers PASS, {n_fail_sections} FAIL.",
            file=file_obj,
        )
    if json_path:
        print(f"Detail: {json_path}", file=file_obj)


def _bucket(text: str) -> str:
    """Classify a single report line into ok / mismatch / missing."""
    if "MISMATCH" in text:
        return "mismatch"
    if "MISSING" in text:
        return "missing"
    if " OK " in f" {text} " or text.lstrip().startswith("OK"):
        return "ok"
    return "ok"  # default to ok for "  OK <tier>" style lines


def main():
    parser = argparse.ArgumentParser(description="Verify tier data against API")
    parser.add_argument("--api-url", default=API_URL_DEFAULT)
    scenario_group = parser.add_mutually_exclusive_group()
    scenario_group.add_argument(
        "--scenario", help="Verify only this scenario (narrows within ACTIVE_SCENARIOS)",
    )
    scenario_group.add_argument(
        "--scenarios-override", nargs="*", default=[],
        help="Per-invocation replacement for ACTIVE_SCENARIOS. Comma/whitespace/newline "
             "separated. Use to pre-flight a scenario before flipping is_active=1. "
             "Logs a WARNING when active.",
    )
    parser.add_argument("--tier", help="Verify only this tier code")
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Directory for JSON report (default: <repo>/audits/verification_reports)",
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--no-json",
        action="store_true",
        help="Skip the JSON file write. Use for ad-hoc local runs.",
    )
    output_group.add_argument(
        "--json-stdout",
        action="store_true",
        help="Dump combined JSON to stdout instead of the scorecard. Useful for CI or piping to jq.",
    )
    args = parser.parse_args()

    global ALLOWED_SCENARIOS  # noqa: PLW0603
    ALLOWED_SCENARIOS = resolve_active_scenarios(args.scenarios_override)

    # Pull the active tier_location catalog and run a coverage scan so the
    # verifier emits the same one-line attribute/geometry WARNINGs the
    # loader does. Skipped silently when DATABASE_URL is unset; the
    # CSV-vs-API comparison below still runs in that case.
    conn = get_db_connection(required=False)
    if conn is not None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tier_short_code, location_type, location_id "
                    "FROM tier_location WHERE is_active = TRUE"
                )
                catalog_rows = cur.fetchall()
            reports = assess_coverage(conn, catalog_rows)
        finally:
            conn.close()
        tier_count = len({row[0] for row in catalog_rows})
        print(
            f"tier_location catalog: {len(catalog_rows)} active rows across "
            f"{tier_count} tiers"
        )
        for line in format_coverage_warnings(catalog_rows, reports):
            print(line)
    else:
        print("WARNING: DATABASE_URL not set; skipping tier_location coverage scan.")

    repo_root = Path(__file__).resolve().parent.parent.parent.parent
    default_report_dir = repo_root / "audits" / "verification_reports"
    explicit_report_dir = Path(args.report_dir) if args.report_dir else default_report_dir

    print(f"API: {args.api_url}")
    print(f"Staging: {STAGING_DIR}")
    print()

    # Fetch all scenario tier data from API
    print("Fetching tier data from API...")
    api_data: Dict[str, object] = {}
    scenarios = sorted(ALLOWED_SCENARIOS)
    if args.scenario:
        scenarios = [args.scenario]

    for sid in scenarios:
        resp = api_get(args.api_url, f"/tiers/scenarios/{sid}/tiers")
        if resp:
            api_data[sid] = resp
    print(f"  Got data for {len(api_data)}/{len(scenarios)} scenarios")
    print()

    # Parse staging CSVs and verify
    tier_checks = [
        ("CWS_DEL", parse_cws_del, "multi"),
        ("AG_REV", parse_ag_rev, "multi"),
        ("ENV_FLOWS", parse_env_flows, "multi"),
        ("RES_STOR", parse_res_stor, "multi"),
        ("GW_STOR", parse_gw_stor, "multi"),
        (
            "DELTA_ECO",
            lambda: parse_single_value("DELTA_ECO.csv", "Scenario", "TierValue"),
            "single",
        ),
        (
            "FW_DELTA_USES",
            lambda: parse_single_value(
                "FW_DELTA_USES.csv", "ScenarioID", "Salinity_Tier"
            ),
            "single",
        ),
        (
            "FW_EXP",
            lambda: parse_single_value(
                "FW_EXP.csv", "Scenario", "Salinity_Export_Tier"
            ),
            "single",
        ),
        ("WRC_SALMON_AB", parse_wrc_salmon_ab, "single"),
    ]

    per_tier: List[Dict[str, object]] = []

    for tier_code, parser_fn, tier_type in tier_checks:
        if args.tier and args.tier != tier_code:
            continue

        print(f"{'=' * 60}")
        print(f"TIER: {tier_code} ({tier_type})")
        print(f"{'=' * 60}")

        expected = parser_fn()
        if args.scenario:
            expected = {k: v for k, v in expected.items() if k == args.scenario}

        if not expected:
            print("  No staging data found")
            print()
            per_tier.append({
                "tier_code": tier_code,
                "tier_type": tier_type,
                "ok": 0,
                "mismatch": 0,
                "missing": 0,
                "lines": [],
                "bad_examples": [],
            })
            continue

        report: List[str] = []
        if tier_type == "multi":
            verify_multi_value(tier_code, expected, api_data, report)
        else:
            verify_single_value(tier_code, expected, api_data, report)

        n_ok = 0
        n_mismatch = 0
        n_missing = 0
        bad_examples: List[str] = []
        for ln in report:
            print(ln)
            bucket = _bucket(ln)
            if bucket == "ok":
                n_ok += 1
            elif bucket == "mismatch":
                n_mismatch += 1
                if len(bad_examples) < 3:
                    bad_examples.append(ln.strip().split()[1] if len(ln.strip().split()) > 1 else "?")
            elif bucket == "missing":
                n_missing += 1
                if len(bad_examples) < 3:
                    bad_examples.append(ln.strip().split()[1] if len(ln.strip().split()) > 1 else "?")
        per_tier.append({
            "tier_code": tier_code,
            "tier_type": tier_type,
            "ok": n_ok,
            "mismatch": n_mismatch,
            "missing": n_missing,
            "lines": report,
            "bad_examples": bad_examples,
        })
        print()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_url": args.api_url,
        "scenarios": scenarios,
        "tier_filter": args.tier,
        "per_tier": per_tier,
    }

    json_path: Optional[Path] = None
    if not args.no_json and not args.json_stdout:
        explicit_report_dir.mkdir(parents=True, exist_ok=True)
        json_path = explicit_report_dir / f"tiers_{timestamp}.json"
        with open(json_path, "w") as f:
            json.dump(report_payload, f, indent=2, default=str)

    if args.json_stdout:
        json.dump(report_payload, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
    else:
        render_scorecard(per_tier, json_path)

    total_mismatch = sum(int(e["mismatch"]) for e in per_tier)
    total_missing = sum(int(e["missing"]) for e in per_tier)
    if total_mismatch + total_missing > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
