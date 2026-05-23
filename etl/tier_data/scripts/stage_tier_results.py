#!/usr/bin/env python3
"""
Preprocess team-delivered tier CSVs into the canonical flat filenames that
load_all_tier_results.py expects in etl/tier_data/staging/.

The data team drops raw tier result CSVs into etl/tier_data/staging/tier_results/
organized by subdirectory (ag/, community/, eflows/, delta_ecology/, salmon/)
or as flat Hist+CC50+CC95 files at the root of tier_results/. The loader, on
the other hand, expects nine canonical flat filenames directly under staging/
(CWS_DEL.csv, AG_REV.csv, ENV_FLOWS_{historical,cc50,cc95}.csv, RES_STOR.csv,
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
    """community/20260413_all_scenarios_through_cc95_tiers_categorical.csv -> CWS_DEL.csv"""
    src = _find_single(source_dir / "community", "20260413_all_scenarios_through_cc95_tiers_categorical.csv")
    if src is None:
        matches = _find_glob(source_dir / "community", "*.csv")
        if not matches:
            print("  CWS_DEL: no source under community/, skipped")
            return False
        src = matches[-1]
        print(f"  CWS_DEL: no exact match, using {src.name}")

    df = pd.read_csv(src)
    if df.columns[0] != "scenario":
        df = df.rename(columns={df.columns[0]: "scenario"})
    _write(df, out_dir / "CWS_DEL.csv", dry_run, f"from community/{src.name}")
    return True


def stage_ag_rev(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """ag/{historical,cc50,cc95}.csv -> AG_REV.csv (concatenated, wide format)."""
    ag_dir = source_dir / "ag"
    parts: List[Path] = []
    for tag in ("historical", "cc50", "cc95"):
        p = ag_dir / f"{tag}.csv"
        if p.exists():
            parts.append(p)
    if not parts:
        print("  AG_REV: no source files under ag/, skipped")
        return False

    df = _concat_csvs(parts)
    df = df.drop_duplicates(subset=[df.columns[0]], keep="last")
    _write(df, out_dir / "AG_REV.csv", dry_run,
           f"from ag/{{{','.join(p.stem for p in parts)}}}.csv")
    return True


def stage_env_flows(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """
    eflows/*.csv -> ENV_FLOWS_{historical,cc50,cc95}.csv.

    The loader's _discover_env_flows_files globs ENV_FLOWS_*.csv and sorts
    historical -> cc50 -> cc95 so that later files overwrite earlier ones
    for overlapping scenarios. We preserve the three-file split here.

    Column-0 header on input may be either `Scenario` or `Station` (the
    eFLOW's team's historical convention). Both are accepted.

    `load_all_tier_results.py:_load_one_env_flows_file` auto-detects
    row/column orientation.
    """
    eflows_dir = source_dir / "eflows"

    wrote_any = False
    for tag, patterns in (
        ("historical", ("Historical_Scenarios_Eflows_*.csv",)),
        ("cc50",       ("Climate_Scenarios_50_Eflows_*.csv",)),
        ("cc95",       ("Climate_Scenarios_95_Eflows_*.csv",)),
    ):
        src = None
        for pat in patterns:
            matches = _find_glob(eflows_dir, pat)
            if matches:
                src = matches[-1]
                break
        if src is None:
            print(f"  ENV_FLOWS_{tag}: no source in eflows/, skipped")
            continue
        df = pd.read_csv(src)
        if len(df.columns) == 0:
            print(f"  ENV_FLOWS_{tag}: source {src.name} has no columns, skipped")
            continue
        header = df.columns[0]
        if header not in ENV_FLOWS_SCENARIO_COL_ALIASES:
            print(
                f"  ENV_FLOWS_{tag}: WARNING column-0 header {header!r} in "
                f"{src.name} is not one of "
                f"{list(ENV_FLOWS_SCENARIO_COL_ALIASES)}; treating it as the "
                "scenario column and normalizing to 'Scenario'"
            )
        if header != "Scenario":
            df = df.rename(columns={header: "Scenario"})
        _write(df, out_dir / f"ENV_FLOWS_{tag}.csv", dry_run, f"from eflows/{src.name}")
        wrote_any = True
    return wrote_any


def stage_res_stor(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """ReservoirStorage_Tiers_Hist_CC50_CC95.csv -> RES_STOR.csv (identity copy)."""
    src = _find_single(source_dir, "ReservoirStorage_Tiers_Hist_CC50_CC95.csv")
    if src is None:
        print("  RES_STOR: no source, skipped")
        return False
    df = pd.read_csv(src)
    _write(df, out_dir / "RES_STOR.csv", dry_run, f"from {src.name}")
    return True


def stage_gw_stor(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """GroundWater_Tiers_Hist_CC50_CC95.csv -> GW_STOR.csv (identity copy)."""
    src = _find_single(source_dir, "GroundWater_Tiers_Hist_CC50_CC95.csv")
    if src is None:
        print("  GW_STOR: no source, skipped")
        return False
    df = pd.read_csv(src)
    _write(df, out_dir / "GW_STOR.csv", dry_run, f"from {src.name}")
    return True


def stage_delta_eco(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """
    delta_ecology/TierOutcomes_{Historical,CC50,CC95}.csv -> DELTA_ECO.csv.

    Concatenate; keep only Scenario and TierValue (loader ignores the other
    stats columns Mean/SD/TierScore, so dropping them keeps the flat file tidy).
    """
    de_dir = source_dir / "delta_ecology"
    parts: List[Path] = []
    for tag in ("Historical", "CC50", "CC95"):
        p = de_dir / f"TierOutcomes_{tag}.csv"
        if p.exists():
            parts.append(p)
    if not parts:
        print("  DELTA_ECO: no source files under delta_ecology/, skipped")
        return False

    df = _concat_csvs(parts)
    keep = [c for c in ("Scenario", "TierValue") if c in df.columns]
    if keep != ["Scenario", "TierValue"]:
        print(f"  DELTA_ECO: expected Scenario+TierValue, got {list(df.columns)}")
        return False
    df = df[keep].drop_duplicates(subset=["Scenario"], keep="last")
    _write(df, out_dir / "DELTA_ECO.csv", dry_run,
           f"from delta_ecology/TierOutcomes_{{{','.join(p.stem.split('_')[-1] for p in parts)}}}.csv")
    return True


def stage_fw_delta_uses(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """InDeltaSalinity_Tiers_Hist_CC50_CC95.csv -> FW_DELTA_USES.csv (identity)."""
    src = _find_single(source_dir, "InDeltaSalinity_Tiers_Hist_CC50_CC95.csv")
    if src is None:
        print("  FW_DELTA_USES: no source, skipped")
        return False
    df = pd.read_csv(src)
    _write(df, out_dir / "FW_DELTA_USES.csv", dry_run, f"from {src.name}")
    return True


def stage_fw_exp(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """ExportSalinity_Tiers_Hist_CC50_CC95.csv -> FW_EXP.csv (identity)."""
    src = _find_single(source_dir, "ExportSalinity_Tiers_Hist_CC50_CC95.csv")
    if src is None:
        print("  FW_EXP: no source, skipped")
        return False
    df = pd.read_csv(src)
    _write(df, out_dir / "FW_EXP.csv", dry_run, f"from {src.name}")
    return True


def stage_salmon(source_dir: Path, out_dir: Path, dry_run: bool) -> bool:
    """
    salmon/TIERS_WRLCM_01_BestYearSummary_*.csv -> WRC_SALMON_AB.csv.

    Schema: scenario, Hydroclimate, Tier_range, tier_score_cont.
    load_all_tier_results.py parses Tier_range (e.g. "Tier 4") into an integer
    tier_level; tier_score_cont is passed through but not currently stored.
    """
    matches = _find_glob(source_dir / "salmon", "TIERS_WRLCM_01_BestYearSummary_*.csv")
    if not matches:
        matches = _find_glob(source_dir / "salmon", "*.csv")
    if not matches:
        print("  WRC_SALMON_AB: no source under salmon/, skipped")
        return False
    src = matches[-1]
    df = pd.read_csv(src)
    _write(df, out_dir / "WRC_SALMON_AB.csv", dry_run, f"from salmon/{src.name}")
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
        help=f"Directory containing team-delivered CSVs (default: {DEFAULT_SOURCE_DIR})",
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
