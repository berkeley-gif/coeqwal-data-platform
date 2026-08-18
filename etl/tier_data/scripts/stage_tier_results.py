#!/usr/bin/env python3
"""
Preprocess team-delivered tier CSVs into the canonical flat filenames that
load_all_tier_results.py expects in etl/tier_data/staging/.

The data team drops raw tier result CSVs into etl/tier_data/staging/tier_results/
organized by subdirectory (CWS_DEL/, AG_REV/, ENV_FLOWS/, RES_STOR/,
GW_STOR/, DELTA_ECO/, FW_DELTA_USES/, FW_EXP/, WRC_SALMON_AB/). The loader, on
the other hand, expects nine canonical flat filenames directly under staging/
(CWS_DEL.csv, AG_REV.csv, ENV_FLOWS.csv, RES_STOR.csv,
GW_STOR.csv, DELTA_ECO.csv, FW_DELTA_USES.csv, FW_EXP.csv, WRC_SALMON_AB.csv).

This script bridges the two: it reads from staging/tier_results/** and writes
the canonical flat files into staging/, concatenating split files where
necessary. It is idempotent: re-run it after any new team drop.

Usage:
    # Write canonical flat files (default)
    python stage_tier_results.py

    # Preview what would be written without touching disk
    python stage_tier_results.py --dry-run

    # Use alternate source/destination directories
    python stage_tier_results.py --source-dir /path/to/tier_results --out-dir /path/to/staging
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd


SCRIPT_DIR = Path(__file__).parent
STAGING_BASE = SCRIPT_DIR.parent
DEFAULT_SOURCE_DIR = STAGING_BASE / "staging" / "tier_results"
DEFAULT_OUT_DIR = STAGING_BASE / "staging"

# Column-0 header aliases accepted on input for ENV_FLOWS upstream CSVs.
# All are treated as the scenario column.
ENV_FLOWS_SCENARIO_COL_ALIASES: tuple = ("Scenario", "Station")


def _find_single(source_dir: Path, *candidates: str) -> Optional[Path]:
    """Return the first existing path among candidates, else None."""
    for name in candidates:
        p = source_dir / name
        if p.exists():
            return p
    return None


def _find_glob(source_dir: Path, pattern: str) -> List[Path]:
    """Sorted list of matches for a glob pattern."""
    return sorted(source_dir.glob(pattern))


def _concat_csvs(paths: Iterable[Path]) -> pd.DataFrame:
    """Concatenate CSVs row-wise, preserving column order of the first file."""
    frames = [pd.read_csv(p) for p in paths]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=0, ignore_index=True)


def _write(df: pd.DataFrame, out_path: Path, dry_run: bool, label: str) -> None:
    """Emit a CSV with consistent quoting, or log the intended write in dry-run."""
    if dry_run:
        print(f"  [dry-run] would write {out_path.name}  ({len(df)} rows, {len(df.columns)} cols)  -- {label}")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"  wrote {out_path.name:<28} ({len(df)} rows, {len(df.columns)} cols)  -- {label}")


def stage_cws_del(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """CWS_DEL/20260603_all_scenarios_through_ecearth_tiers_continuous.csv -> CWS_DEL.csv"""
    src = _find_single(source_dir / "CWS_DEL", "20260813_all_scenarios_through_ecearth_tiers_continuous.csv")
    if src is None:
        matches = _find_glob(source_dir / "CWS_DEL", "*.csv")
        if not matches:
            print("  CWS_DEL: no source under community/, skipped")
            return False
        src = matches[-1]
        print(f"  CWS_DEL: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "CWS_DEL.csv", dry_run, f"from CWS_DEL/{src.name}")
    return True


def stage_ag_rev(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """AG_REV/continuous_tiers.csv -> AG_REV.csv"""
    src = _find_single(source_dir / "AG_REV", "continuous_tiers.csv")
    if src is None:
        matches = _find_glob(source_dir / "AG_REV", "*.csv")
        if not matches:
            print("  AG_REV: no source under AG_REV/, skipped")
            return False
        src = matches[-1]
        print(f"  AG_REV: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "AG_REV.csv", dry_run, f"from AG_REV/{src.name}")
    return True


def stage_env_flows(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """ENV_FLOWS/Continuous_Tier_Table_June18(1).csv -> ENV_FLOWS.csv"""
    src = _find_single(source_dir / "ENV_FLOWS", "eFlows_Continuous_Tier_Table.csv")
    if src is None:
        matches = _find_glob(source_dir / "ENV_FLOWS", "*.csv")
        if not matches:
            print("  ENV_FLOWS: no source under ENV_FLOWS/, skipped")
            return False
        src = matches[-1]
        print(f"  ENV_FLOWS: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "ENV_FLOWS.csv", dry_run, f"from ENV_FLOWS/{src.name}")
    return True


def stage_res_stor(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """RES_STOR/Continuous_ReservoirStorage_Tiers_Hist_CC50_CC95_TAI_ECV.csv -> RES_STOR.csv"""
    src = _find_single(source_dir / "RES_STOR", "Continuous_ReservoirStorage_Tiers_Hist_CC50_CC95_TAI_ECV.csv")
    if src is None:
        matches = _find_glob(source_dir / "RES_STOR", "*.csv")
        if not matches:
            print("  RES_STOR: no source under RES_STOR/, skipped")
            return False
        src = matches[-1]
        print(f"  RES_STOR: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "RES_STOR.csv", dry_run, f"from RES_STOR/{src.name}")
    return True


def stage_gw_stor(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """GW_STOR/Continuous_GroundWater_Tiers_Hist_CC50_CC95_TAI_ECV.csv -> GW_STOR.csv"""
    src = _find_single(source_dir / "GW_STOR", "Continuous_GroundWater_Tiers_Revised_08132026.csv")
    if src is None:
        matches = _find_glob(source_dir / "GW_STOR", "*.csv")
        if not matches:
            print("  GW_STOR: no source under GW_STOR/, skipped")
            return False
        src = matches[-1]
        print(f"  GW_STOR: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "GW_STOR.csv", dry_run, f"from GW_STOR/{src.name}")
    return True


def stage_delta_eco(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """DELTA_ECO/TierOutcomes_Continuous.csv -> DELTA_ECO.csv."""
    src = _find_single(source_dir / "DELTA_ECO", "TierOutcomes_Continuous.csv")
    if src is None:
        matches = _find_glob(source_dir / "DELTA_ECO", "*.csv")
        if not matches:
            print("  DELTA_ECO: no source under DELTA_ECO/, skipped")
            return False
        src = matches[-1]
        print(f"  DELTA_ECO: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "DELTA_ECO.csv", dry_run, f"from DELTA_ECO/{src.name}")
    return True


def stage_fw_delta_uses(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """FW_DELTA_USES/Continuous_InDeltaSalinity_Tiers_Hist_CC50_CC95_TAI_ECV.csv -> FW_DELTA_USES.csv"""
    src = _find_single(source_dir / "FW_DELTA_USES", "Continuous_InDeltaSalinity_Tiers_Revised_08112026.csv")
    if src is None:
        matches = _find_glob(source_dir / "FW_DELTA_USES", "*.csv")
        if not matches:
            print("  FW_DELTA_USES: no source under FW_DELTA_USES/, skipped")
            return False
        src = matches[-1]
        print(f"  FW_DELTA_USES: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "FW_DELTA_USES.csv", dry_run, f"from FW_DELTA_USES/{src.name}")
    return True


def stage_fw_exp(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """FW_EXP/Continuous_ExportSalinity_Tiers_Hist_CC50_CC95_TAI_ECV.csv -> FW_EXP.csv"""
    src = _find_single(source_dir / "FW_EXP", "Continuous_ExportSalinity_Tiers_Hist_CC50_CC95_TAI_ECV.csv")
    if src is None:
        matches = _find_glob(source_dir / "FW_EXP", "*.csv")
        if not matches:
            print("  FW_EXP: no source under FW_EXP/, skipped")
            return False
        src = matches[-1]
        print(f"  FW_EXP: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "FW_EXP.csv", dry_run, f"from FW_EXP/{src.name}")
    return True


def stage_salmon(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """
    WRC_SALMON_AB/TIERS_WRLCM_01A_TIERS_best_year_MASTER_capacity_percent_Th_6_18_2026-08-10_2026-08-10_ALL_TierDataOnly_Modified_By_EL.csv -> WRC_SALMON_AB.csv

    Schema: scenario, Tier_range, tier_score_cont.
    Keep: scenario, tier_score_cont.
    """
    src = _find_single(source_dir / "WRC_SALMON_AB", "TIERS_WRLCM_01A_TIERS_best_year_MASTER_capacity_percent_Th_6_18_2026-08-10_2026-08-10_ALL_TierDataOnly_Modified_By_EL.csv")
    if src is None:
        matches = _find_glob(source_dir / "WRC_SALMON_AB", "*.csv")
        if not matches:
            print("  WRC_SALMON_AB: no source under WRC_SALMON_AB/, skipped")
            return False
        src = matches[-1]
        print(f"  WRC_SALMON_AB: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    keep = [c for c in ("scenario", "tier_score_cont") if c in df.columns]
    df = df[keep]
    _write(df, out_dir / "WRC_SALMON_AB.csv", dry_run, f"from WRC_SALMON_AB/{src.name}")
    return True


STAGE_STEPS: List[tuple] = [
    ("CWS_DEL",       stage_cws_del),
    ("AG_REV",        stage_ag_rev),
    ("ENV_FLOWS",     stage_env_flows),
    ("RES_STOR",      stage_res_stor),
    ("GW_STOR",       stage_gw_stor),
    ("DELTA_ECO",     stage_delta_eco),
    ("FW_DELTA_USES", stage_fw_delta_uses),
    ("FW_EXP",        stage_fw_exp),
    ("WRC_SALMON_AB", stage_salmon),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize team-delivered tier CSVs into canonical flat files for load_all_tier_results.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help=f"Directory containing team-delivered CSV subdirs (default: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Directory to write canonical flat files (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without modifying any files",
    )

    args = parser.parse_args()

    source_dir: Path = args.source_dir.resolve()
    out_dir: Path = args.out_dir.resolve()

    if not source_dir.is_dir():
        print(f"Error: source directory not found: {source_dir}", file=sys.stderr)
        return 1

    print("=" * 60)
    print("STAGE TIER RESULTS")
    print(f"Source  : {source_dir}")
    print(f"Output  : {out_dir}")
    if args.dry_run:
        print("Mode    : DRY RUN")
    print("=" * 60)

    ok_count = 0
    skipped: List[str] = []
    for name, fn in STAGE_STEPS:
        print(f"\n[{name}]")
        ok = fn(source_dir, out_dir, args.dry_run)
        if ok:
            ok_count += 1
        else:
            skipped.append(name)

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Staged : {ok_count} / {len(STAGE_STEPS)}")
    if skipped:
        print(f"Skipped: {', '.join(skipped)}")
    if not args.dry_run:
        print("\nNext: python etl/tier_data/scripts/load_all_tier_results.py --dry-run")
    return 0 if not skipped else 2


if __name__ == "__main__":
    sys.exit(main())
