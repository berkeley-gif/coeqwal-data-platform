#!/usr/bin/env python3
"""
Diagnose which target DV/SV variables are present or absent in each scenario.

Usage:
    python etl/statistics/env_flows/diagnose_dv_columns.py
    python etl/statistics/env_flows/diagnose_dv_columns.py --scenarios s0011,s0023,s0039
"""

import argparse
import csv
import sys
from pathlib import Path

import boto3
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
CHANNEL_CSV = PROJECT_ROOT / "database/seed_tables/04_calsim_data/channel_entity.csv"
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from etl.common import S3_BUCKET  # noqa: E402

ALL_SCENARIOS = [
    "s0011",
    "s0020",
    "s0021",
    "s0023",
    "s0024",
    "s0025",
    "s0026",
    "s0027",
    "s0028",
    "s0029",
    "s0030",
    "s0031",
    "s0032",
    "s0033",
    "s0039",
    "s0040",
    "s0041",
    "s0042",
    "s0044",
]

EXPECTED_DV_COLS = 79  # 59 channel flows + 20 MIF
EXPECTED_SV_COLS = 28  # 11 UNIMP + 17 EFLOWS


def load_target_ids():
    dv_ids, sv_ids = set(), set()
    with open(CHANNEL_CSV, newline="") as f:
        for row in csv.DictReader(f):
            cls = row.get("channel_class", "").strip()
            if not cls:
                continue
            arc = row["network_arc_id"].strip()
            dv_ids.add(arc)
            has_mif = row.get("has_mif", "").lower() in ("true", "t", "1")
            has_ef = row.get("has_eflows", "").lower() in ("true", "t", "1")
            unimp = row.get("unimp_sv_variable", "").strip()
            if has_mif:
                dv_ids.add(f"{arc}_MIF")
            if unimp:
                sv_ids.add(unimp)
            if has_ef:
                reach = arc[2:] if arc.startswith("C_") else arc
                sv_ids.add(f"EFLOWS_{reach}")
    return dv_ids, sv_ids


def get_header_vars(s3, bucket, key, row_index=1):
    """Return the list of Part B variable names from a CalSim CSV header."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        hdr = pd.read_csv(resp["Body"], header=None, nrows=7, low_memory=False)
        return [str(v) for v in hdr.iloc[row_index].tolist()]
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        print(f"  ERROR reading {key}: {exc}")
        return None


def diagnose_scenario(s3, scenario_id, dv_ids, sv_ids):
    print(f"\n{'=' * 60}")
    print(f"Scenario: {scenario_id}")
    print(f"{'=' * 60}")

    # ── DV──────
    dv_keys = [
        f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv",
        f"scenario/{scenario_id}/csv/{scenario_id}_DV.csv",
    ]
    dv_vars = None
    for key in dv_keys:
        dv_vars = get_header_vars(s3, S3_BUCKET, key)
        if dv_vars:
            print(f"DV file: {key}")
            break

    if dv_vars is None:
        print("  DV file NOT FOUND")
    else:
        found_dv = set(dv_vars) & dv_ids
        missing_dv = dv_ids - set(dv_vars)
        print(f"DV columns found: {len(found_dv)} / {len(dv_ids)} expected")
        if missing_dv:
            missing_channels = sorted(v for v in missing_dv if "_MIF" not in v)
            missing_mif = sorted(v for v in missing_dv if "_MIF" in v)
            if missing_channels:
                print(
                    f"  MISSING channel flows ({len(missing_channels)}): {missing_channels}"
                )
            if missing_mif:
                print(f"  MISSING MIF vars    ({len(missing_mif)}): {missing_mif}")
        else:
            print("  All target DV variables present ✓")

    # ── SV──────
    sv_key = f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_sv_input.csv"
    sv_vars = get_header_vars(s3, S3_BUCKET, sv_key)

    if sv_vars is None:
        print("SV file NOT FOUND")
    else:
        found_sv = set(sv_vars) & sv_ids
        missing_sv = sv_ids - set(sv_vars)
        print(f"SV columns found: {len(found_sv)} / {len(sv_ids)} expected")
        if missing_sv:
            missing_unimp = sorted(v for v in missing_sv if v.startswith("UNIMP_"))
            missing_eflows = sorted(v for v in missing_sv if v.startswith("EFLOWS_"))
            if missing_unimp:
                print(f"  MISSING UNIMP vars  ({len(missing_unimp)}): {missing_unimp}")
            if missing_eflows:
                print(
                    f"  MISSING EFLOWS vars ({len(missing_eflows)}): {missing_eflows}"
                )
        else:
            print("  All target SV variables present ✓")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenarios", help="Comma-separated scenario IDs (default: all)"
    )
    args = parser.parse_args()

    scenarios = (
        [s.strip() for s in args.scenarios.split(",")]
        if args.scenarios
        else ALL_SCENARIOS
    )

    dv_ids, sv_ids = load_target_ids()
    print(f"Target: {len(dv_ids)} DV variables, {len(sv_ids)} SV variables")

    s3 = boto3.client("s3")
    for scen in scenarios:
        diagnose_scenario(s3, scen, dv_ids, sv_ids)

    print(f"\n{'=' * 60}")
    print("Diagnosis complete.")


if __name__ == "__main__":
    main()
