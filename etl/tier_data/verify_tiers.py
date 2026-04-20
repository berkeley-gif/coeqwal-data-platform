#!/usr/bin/env python3
"""
Verify tier data: compare staging CSVs against live API responses.

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
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

import pandas as pd

STAGING_DIR = Path(__file__).parent / "staging"

API_URL_DEFAULT = "https://api.coeqwal.org/api"

ALLOWED_SCENARIOS = {
    "s0011",
    "s0020",
    "s0021",
    "s0023",
    "s0024",
    "s0025",
    "s0026",
    "s0027",
    "s0028",
    "s0030",
    "s0031",
    "s0032",
    "s0033",
    "s0035",
    "s0036",
    "s0037",
    "s0039",
    "s0040",
    "s0041",
    "s0042",
    "s0044",
    "s0045",
    "s0046",
    "s0065",
}

RESERVOIR_LOCATIONS = {
    "S_SHSTA_Storage_Tier": ("SHSTA", "Shasta"),
    "S_TRNTY_Storage_Tier": ("TRNTY", "Trinity"),
    "S_FOLSM_Storage_Tier": ("FOLSM", "Folsom"),
    "S_OROVL_Storage_Tier": ("OROVL", "Oroville"),
    "S_MLRTN_Storage_Tier": ("MLRTN", "Millerton"),
    "S_MELON_Storage_Tier": ("MELON", "New Melones"),
    "S_PEDRO_Storage_Tier": ("PEDRO", "New Don Pedro"),
    "S_MCLRE_Storage_Tier": ("MCLRE", "McClure"),
    "S_SLUIS_CVP_Storage_Tier": ("SLUIS_CVP", "San Luis CVP"),
    "S_SLUIS_SWP_Storage_Tier": ("SLUIS_SWP", "San Luis SWP"),
}


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
        for col, (loc_id, _) in RESERVOIR_LOCATIONS.items():
            if col not in df.columns:
                continue
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


def main():
    parser = argparse.ArgumentParser(description="Verify tier data against API")
    parser.add_argument("--api-url", default=API_URL_DEFAULT)
    parser.add_argument("--scenario", help="Verify only this scenario")
    parser.add_argument("--tier", help="Verify only this tier code")
    args = parser.parse_args()

    print(f"API: {args.api_url}")
    print(f"Staging: {STAGING_DIR}")
    print()

    # Fetch all scenario tier data from API
    print("Fetching tier data from API...")
    api_data = {}
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

    total_ok = 0
    total_mismatch = 0
    total_missing = 0

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
            continue

        report = []
        if tier_type == "multi":
            verify_multi_value(tier_code, expected, api_data, report)
        else:
            verify_single_value(tier_code, expected, api_data, report)

        for line in report:
            print(line)
            if "OK" in line:
                total_ok += 1
            elif "MISMATCH" in line:
                total_mismatch += 1
            elif "MISSING" in line:
                total_missing += 1
        print()

    # WRC_SALMON_AB.hardcoded tier 4, s0065 excluded
    if not args.tier or args.tier == "WRC_SALMON_AB":
        print(f"{'=' * 60}")
        print("TIER: WRC_SALMON_AB (single, hardcoded)")
        print(f"{'=' * 60}")
        check_scenarios = sorted(ALLOWED_SCENARIOS - {"s0065"})
        if args.scenario:
            check_scenarios = [args.scenario] if args.scenario != "s0065" else []
        for sid in check_scenarios:
            api_scenario = api_data.get(sid)
            if not api_scenario:
                print(f"  MISSING WRC_SALMON_AB {sid}: not in API")
                total_missing += 1
                continue
            tier_info = api_scenario.get("tiers", {}).get("WRC_SALMON_AB")
            if not tier_info:
                print(f"  MISSING WRC_SALMON_AB {sid}: tier not in API response")
                total_missing += 1
                continue
            api_level = tier_info.get("level")
            if api_level != 4:
                print(f"  MISMATCH WRC_SALMON_AB {sid}: expected=4 API={api_level}")
                total_mismatch += 1
            else:
                print(f"  OK WRC_SALMON_AB {sid}: level 4")
                total_ok += 1
        print()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  OK:       {total_ok}")
    print(f"  MISMATCH: {total_mismatch}")
    print(f"  MISSING:  {total_missing}")
    if total_mismatch == 0 and total_missing == 0:
        print("\n  ALL CHECKS PASSED")
    else:
        print(f"\n  {total_mismatch + total_missing} ISSUE(S) FOUND")
        sys.exit(1)


if __name__ == "__main__":
    main()
