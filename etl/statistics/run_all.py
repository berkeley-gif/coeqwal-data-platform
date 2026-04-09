#!/usr/bin/env python3
"""
Consolidated statistics ETL runner.

Runs all statistics calculations for a scenario in the correct order:
1. Reservoir statistics (storage, percentiles, spill, period summary)
2. Urban demand unit (DU) statistics (delivery, shortage)
3. M&I contractor statistics (delivery, shortage)
4. CWS aggregate statistics (SWP, CVP, MWD totals)
5. Agricultural (AG) statistics (delivery, shortage, aggregates)
6. Wildlife Refuge statistics (delivery, shortage, reliability)
7. Environmental River Flow statistics (% unimpaired, % functional flow, alteration index)

Usage:
    # Run all statistics for a scenario
    python run_all.py --scenario s0029

    # Dry run (calculate but don't write to DB)
    python run_all.py --scenario s0029 --dry-run

    # Run only specific modules
    python run_all.py --scenario s0029 --only reservoirs,du_urban

    # Run all scenarios
    python run_all.py --all-scenarios

    # Run all scenarios + sensitivity analysis as a post-processing step
    python run_all.py --all-scenarios --with-sensitivity

    # Use local CSV instead of S3
    python run_all.py --scenario s0029 --csv-path /path/to/csv
"""

import argparse
import csv
import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("run_all")

# Script directory
SCRIPT_DIR = Path(__file__).parent

# Available ETL modules and their entry points
ETL_MODULES = {
    "reservoirs": {
        "path": SCRIPT_DIR / "main.py",
        "name": "Reservoir Statistics",
        "tables": [
            "reservoir_monthly_percentile",
            "reservoir_storage_monthly",
            "reservoir_spill_monthly",
            "reservoir_period_summary",
        ],
    },
    "du_urban": {
        "path": SCRIPT_DIR / "du_urban" / "main.py",
        "name": "Urban Demand Unit Statistics",
        "tables": ["du_delivery_monthly", "du_shortage_monthly", "du_period_summary"],
        "csv_arg": "--output-csv",
    },
    "mi": {
        "path": SCRIPT_DIR / "mi" / "main.py",
        "name": "M&I Contractor Statistics",
        "tables": [
            "mi_delivery_monthly",
            "mi_shortage_monthly",
            "mi_contractor_period_summary",
        ],
    },
    "cws_aggregate": {
        "path": SCRIPT_DIR / "cws_aggregate" / "main.py",
        "name": "CWS Aggregate Statistics",
        "tables": ["cws_aggregate_monthly", "cws_aggregate_period_summary"],
    },
    "ag": {
        "path": SCRIPT_DIR / "ag" / "main.py",
        "name": "Agricultural Statistics",
        "tables": [
            "ag_du_demand_monthly",
            "ag_du_sw_delivery_monthly",
            "ag_du_gw_pumping_monthly",
            "ag_du_shortage_monthly",
            "ag_du_period_summary",
            "ag_aggregate_monthly",
            "ag_aggregate_period_summary",
        ],
        "csv_arg": "--dv-path",
    },
    "refuge": {
        "path": SCRIPT_DIR / "refuge" / "main.py",
        "name": "Wildlife Refuge Statistics",
        "tables": [
            "refuge_du_delivery_monthly",
            "refuge_du_shortage_monthly",
            "refuge_du_period_summary",
        ],
        "csv_arg": "--dv-path",
    },
    "env_flows": {
        "path": SCRIPT_DIR / "env_flows" / "main.py",
        "name": "Environmental River Flow Statistics",
        "tables": [
            "env_flow_channel_monthly",
            "env_flow_channel_seasonal",
            "env_flow_channel_period_summary",
        ],
        "csv_arg": "--dv-path",
    },
    "delta": {
        "path": SCRIPT_DIR / "delta" / "main.py",
        "name": "Delta Statistics (Outflow, X2, Salinity)",
        "tables": ["delta_monthly", "delta_period_summary"],
    },
}

from scenarios import SCENARIOS  # noqa: E402

# Track failures in real time for the running tally
_failure_count = 0
_failure_log: List[str] = []

BUCKET = "coeqwal-model-run"


def preflight_check_duplicates(scenario_id: str, csv_path: Optional[str] = None) -> int:
    """Check a scenario CSV for duplicate B-part column names.

    Reads only the 7-row header.  Logs a warning for each duplicate
    found and returns the number of duplicated variable names.
    """
    import io
    from collections import Counter

    import pandas as pd

    try:
        if csv_path:
            hdr = pd.read_csv(csv_path, header=None, nrows=7, low_memory=False)
        else:
            import boto3
            s3 = boto3.client("s3")
            key = f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv"
            raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            hdr = pd.read_csv(io.BytesIO(raw), header=None, nrows=7, low_memory=False)

        b_row = [str(v) for v in hdr.iloc[1].tolist()]
        total_cols = len(b_row) - 1  # exclude DateTime
        dupes = {name: cnt for name, cnt in Counter(b_row).items() if cnt > 1}

        if dupes:
            log.warning(
                "PRE-FLIGHT: %s — %d duplicate B-part(s) in %d columns "
                "(C-part-aware dedup will resolve):",
                scenario_id,
                len(dupes),
                total_cols,
            )
            for name, cnt in sorted(dupes.items()):
                indices = [i for i, v in enumerate(b_row) if v == name]
                c_parts = [str(hdr.iloc[2, i]) for i in indices]
                log.warning(
                    "  %s (%dx): C-parts = %s",
                    name,
                    cnt,
                    ", ".join(c_parts),
                )
        else:
            log.info(
                "PRE-FLIGHT: %s — %d columns, no duplicates",
                scenario_id,
                total_cols,
            )
        return len(dupes)

    except Exception as e:
        log.warning("PRE-FLIGHT: could not check %s for duplicates: %s", scenario_id, e)
        return 0


def _alert_failure(
    module_name: str,
    scenario_id: str,
    elapsed: float,
    exception: Optional[Exception] = None,
):
    """Print a loud, immediate alert to stderr when a module fails.

    This ensures the operator sees failures in real time rather than
    discovering them only in the final scorecard.
    """
    global _failure_count
    _failure_count += 1
    module_label = ETL_MODULES.get(module_name, {}).get("name", module_name)
    msg = f"{scenario_id} / {module_label}"
    if exception:
        msg += f" — {exception}"
    _failure_log.append(msg)

    banner = (
        f"\n{'!' * 60}\n"
        f"  FAILURE #{_failure_count}: {module_label}\n"
        f"  Scenario: {scenario_id}  |  Elapsed: {elapsed:.1f}s\n"
    )
    if exception:
        banner += f"  Exception: {exception}\n"
    banner += f"  Total failures so far: {_failure_count}\n{'!' * 60}\n"
    sys.stderr.write(banner)
    sys.stderr.flush()


def run_module(
    module_name: str,
    scenario_id: str,
    dry_run: bool = False,
    csv_path: Optional[str] = None,
) -> Tuple[bool, float]:
    """Run a single ETL module for a scenario.

    Returns (success, elapsed_seconds).
    """
    module = ETL_MODULES.get(module_name)
    if not module:
        log.error(f"Unknown module: {module_name}")
        return False, 0.0

    script_path = module["path"]
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return False, 0.0

    log.info(f"{'=' * 60}")
    log.info(f"Running: {module['name']} for {scenario_id}")
    log.info(f"Tables: {', '.join(module['tables'])}")
    log.info(f"{'=' * 60}")

    cmd = [sys.executable, str(script_path), "--scenario", scenario_id]

    if dry_run:
        cmd.append("--dry-run")

    if csv_path:
        abs_csv_path = str(Path(csv_path).resolve())
        csv_arg_name = module.get("csv_arg", "--csv-path")
        cmd.extend([csv_arg_name, abs_csv_path])

    t0 = time.time()
    stderr_lines: List[str] = []
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=script_path.parent,
            env=os.environ.copy(),
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        for line in proc.stderr:
            sys.stderr.write(line)
            sys.stderr.flush()
            stderr_lines.append(line)

        proc.wait()
        elapsed = time.time() - t0

        if proc.returncode != 0:
            tail = "".join(stderr_lines[-15:]).strip()
            log.error(f"Module {module_name} failed (exit {proc.returncode})")
            exc = RuntimeError(tail) if tail else None
            _alert_failure(module_name, scenario_id, elapsed, exception=exc)
            return False, elapsed

        log.info(f"✅ {module['name']} completed successfully ({elapsed:.1f}s)")
        return True, elapsed

    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"Error running {module_name}: {e}")
        _alert_failure(module_name, scenario_id, elapsed, exception=e)
        return False, elapsed


def cleanup_temp_files(scenario_id: str):
    """
    Clean up temporary files to free memory on Cloud9.

    CalSim CSV files are large and can exhaust memory if not cleaned up
    between scenarios.
    """
    import glob
    import shutil

    # Clean up /tmp/s0* files (downloaded CSVs)
    tmp_pattern = f"/tmp/{scenario_id}*"
    tmp_files = glob.glob(tmp_pattern)
    if tmp_files:
        for f in tmp_files:
            try:
                if os.path.isdir(f):
                    shutil.rmtree(f)
                else:
                    os.remove(f)
            except Exception as e:
                log.warning(f"Could not remove {f}: {e}")
        log.info(f"Cleaned up {len(tmp_files)} temp files matching {tmp_pattern}")

    # NOTE: Do NOT glob "/tmp/s0*" here -- with parallel workers,
    # that would delete temp files belonging to other in-flight scenarios.


def run_all_modules(
    scenario_id: str,
    modules: Optional[List[str]] = None,
    dry_run: bool = False,
    csv_path: Optional[str] = None,
    continue_on_error: bool = False,
) -> dict:
    """Run all (or specified) ETL modules for a scenario.

    Returns dict of module_name → {"status": str, "elapsed_s": float}.
    """
    if modules is None:
        modules = list(ETL_MODULES.keys())

    results = {}

    log.info(f"\n{'#' * 60}")
    log.info(f"# PROCESSING SCENARIO: {scenario_id}")
    log.info(f"# Modules: {', '.join(modules)}")
    log.info(f"# Dry run: {dry_run}")
    log.info(f"{'#' * 60}\n")

    n_dupes = preflight_check_duplicates(scenario_id, csv_path)

    for module_name in modules:
        success, elapsed = run_module(module_name, scenario_id, dry_run, csv_path)
        results[module_name] = {
            "status": "success" if success else "failed",
            "elapsed_s": elapsed,
        }

        if not success and not continue_on_error:
            log.error(f"Stopping due to failure in {module_name}")
            break

    cleanup_temp_files(scenario_id)

    log.info(f"\n{'=' * 60}")
    log.info(f"SUMMARY for {scenario_id}:")
    if n_dupes > 0:
        log.info(f"  ⚠️  {n_dupes} duplicate B-part(s) detected (resolved via C-part preference)")
    for module_name, info in results.items():
        icon = "✅" if info["status"] == "success" else "❌"
        log.info(
            f"  {icon} {ETL_MODULES[module_name]['name']}: "
            f"{info['status']} ({info['elapsed_s']:.1f}s)"
        )
    if _failure_count > 0:
        log.info(f"  ⚠️  Running failure tally: {_failure_count} total failures so far")
    log.info(f"{'=' * 60}\n")

    results["_meta"] = {"duplicate_b_parts": n_dupes}
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Run all statistics ETL modules for a scenario",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all.py --scenario s0029
  python run_all.py --scenario s0029 --dry-run
  python run_all.py --scenario s0029 --only reservoirs,du_urban
  python run_all.py --all-scenarios
  python run_all.py --all-scenarios --batch-size 10
  python run_all.py --all-scenarios --batch-size 10 --start-from s0027
  python run_all.py --all-scenarios --batch-size 15 --continue-on-error
        """,
    )

    parser.add_argument("--scenario", "-s", help="Scenario ID to process (e.g., s0029)")
    parser.add_argument(
        "--all-scenarios", action="store_true", help="Process all known scenarios"
    )
    parser.add_argument(
        "--only",
        help=f"Comma-separated list of modules to run. Available: {', '.join(ETL_MODULES.keys())}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calculate statistics but do not write to database",
    )
    parser.add_argument(
        "--csv-path", help="Local CSV file path (instead of loading from S3)"
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing remaining modules/scenarios even if one fails. "
        "Default behavior is fail-fast: abort the entire run on the first error.",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=1,
        help="Number of scenarios to process in parallel (default: 1). "
        "Each worker downloads ~300MB CSV, so set based on available RAM. "
        "Recommended: 4 for 8GB+ RAM.",
    )
    parser.add_argument(
        "--with-sensitivity",
        action="store_true",
        help="Run cross-scenario sensitivity analysis after per-scenario modules. "
        "Requires all (or most) per-scenario statistics to be in the DB already.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Process scenarios in batches of this size, pausing between batches. "
        "0 (default) = no batching (process all at once). "
        "Recommended: 10-15 to prevent long-running AWS token issues.",
    )
    parser.add_argument(
        "--start-from",
        help="Skip scenarios before this one (inclusive). "
        "Useful for resuming after a partial run. Example: --start-from s0027",
    )
    parser.add_argument(
        "--list-modules", action="store_true", help="List available modules and exit"
    )

    args = parser.parse_args()

    # List modules and exit
    if args.list_modules:
        print("\nAvailable ETL modules:")
        print("-" * 60)
        for name, info in ETL_MODULES.items():
            print(f"\n{name}:")
            print(f"  Name: {info['name']}")
            print(f"  Script: {info['path']}")
            print(f"  Tables: {', '.join(info['tables'])}")
        print(f"\nKnown scenarios: {', '.join(SCENARIOS)}")
        return

    # Validate arguments
    if not args.scenario and not args.all_scenarios:
        parser.error("Specify --scenario or --all-scenarios")

    # Parse modules
    modules = None
    if args.only:
        modules = [m.strip() for m in args.only.split(",")]
        invalid = [m for m in modules if m not in ETL_MODULES]
        if invalid:
            parser.error(
                f"Unknown modules: {', '.join(invalid)}. Available: {', '.join(ETL_MODULES.keys())}"
            )

    # Check DATABASE_URL
    if not args.dry_run and not os.getenv("DATABASE_URL"):
        parser.error("DATABASE_URL environment variable required (or use --dry-run)")

    # Determine scenarios
    scenarios = SCENARIOS if args.all_scenarios else [args.scenario]

    if args.start_from:
        if args.start_from not in scenarios:
            parser.error(
                f"--start-from {args.start_from} not found in scenario list. "
                f"Available: {', '.join(scenarios[:5])}..."
            )
        idx = scenarios.index(args.start_from)
        skipped = scenarios[:idx]
        scenarios = scenarios[idx:]
        log.info(
            f"--start-from {args.start_from}: skipping {len(skipped)} scenario(s), "
            f"{len(scenarios)} remaining"
        )

    workers = max(1, args.workers)
    batch_size = max(0, args.batch_size)

    # Split scenarios into batches (0 = single batch of all)
    if batch_size > 0:
        batches = [
            scenarios[i : i + batch_size] for i in range(0, len(scenarios), batch_size)
        ]
        log.info(
            f"Processing {len(scenarios)} scenarios in "
            f"{len(batches)} batch(es) of up to {batch_size}"
        )
    else:
        batches = [scenarios]

    # Process scenarios
    all_results = {}
    effective_modules = modules or list(ETL_MODULES.keys())
    start_time = time.time()

    aborted = False
    global_idx = 0

    for batch_num, batch in enumerate(batches, 1):
        if aborted:
            break

        if len(batches) > 1:
            log.info(f"\n{'*' * 60}")
            log.info(
                f"  BATCH {batch_num}/{len(batches)}: "
                f"{batch[0]} .. {batch[-1]}  ({len(batch)} scenarios)"
            )
            log.info(f"{'*' * 60}\n")

        if workers == 1 or len(batch) == 1:
            for scenario_id in batch:
                global_idx += 1
                results = run_all_modules(
                    scenario_id,
                    modules=modules,
                    dry_run=args.dry_run,
                    csv_path=args.csv_path,
                    continue_on_error=args.continue_on_error,
                )
                all_results[scenario_id] = results
                log.info(f"[{global_idx}/{len(scenarios)}] {scenario_id} finished")

                scenario_had_failure = any(
                    (r["status"] if isinstance(r, dict) else r) == "failed"
                    for k, r in results.items() if k != "_meta"
                )
                if scenario_had_failure and not args.continue_on_error:
                    remaining = len(scenarios) - global_idx
                    log.error(
                        f"ABORTING: {scenario_id} had failures and "
                        f"--continue-on-error was not set. "
                        f"{remaining} scenario(s) skipped."
                    )
                    aborted = True
                    break
        else:
            log.info(
                f"Running batch of {len(batch)} scenarios with {workers} parallel workers"
            )

            def _process_scenario(scenario_id: str) -> tuple:
                results = run_all_modules(
                    scenario_id,
                    modules=modules,
                    dry_run=args.dry_run,
                    csv_path=args.csv_path,
                    continue_on_error=args.continue_on_error,
                )
                return scenario_id, results

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_process_scenario, sid): sid for sid in batch
                }
                for future in as_completed(futures):
                    sid = futures[future]
                    try:
                        scenario_id, results = future.result()
                        all_results[scenario_id] = results
                        global_idx += 1
                        log.info(
                            f"[{global_idx}/{len(scenarios)}] {scenario_id} finished"
                        )

                        scenario_had_failure = any(
                            (r["status"] if isinstance(r, dict) else r) == "failed"
                            for k, r in results.items() if k != "_meta"
                        )
                        if scenario_had_failure and not args.continue_on_error:
                            log.error(
                                f"ABORTING: {scenario_id} had failures. "
                                f"Cancelling remaining scenarios."
                            )
                            for f in futures:
                                f.cancel()
                            aborted = True
                            break
                    except Exception as e:
                        log.error(f"{sid} raised an exception: {e}")
                        all_results[sid] = {
                            m: {"status": "failed", "elapsed_s": 0.0}
                            for m in effective_modules
                        }
                        global_idx += 1
                        if not args.continue_on_error:
                            log.error("ABORTING due to unhandled exception.")
                            for f in futures:
                                f.cancel()
                            aborted = True
                            break

        if len(batches) > 1 and not aborted and batch_num < len(batches):
            batch_elapsed = time.time() - start_time
            log.info(
                f"Batch {batch_num} complete. "
                f"Elapsed so far: {batch_elapsed / 60:.1f} min. "
                f"{len(scenarios) - global_idx} scenario(s) remaining."
            )

    elapsed = time.time() - start_time
    log.info(f"Total wall-clock time: {elapsed / 60:.1f} minutes")

    if aborted:
        log.error(
            "Run was ABORTED due to errors. Use --continue-on-error to "
            "force processing all scenarios despite failures."
        )

    # Print comprehensive scorecard at the end
    has_failures = print_scorecard(all_results, scenarios, effective_modules)

    # Write structured audit CSV
    audit_path = f"stats_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    write_audit_csv(
        all_results,
        scenarios,
        effective_modules,
        elapsed,
        args.dry_run,
        audit_path,
    )

    # Post-processing: cross-scenario sensitivity analysis
    if args.with_sensitivity:
        log.info("\n" + "=" * 60)
        log.info("Running cross-scenario sensitivity analysis ...")
        log.info("=" * 60)
        sensitivity_script = SCRIPT_DIR / "sensitivity" / "calculate_sensitivity.py"
        sens_cmd = [sys.executable, str(sensitivity_script)]
        if args.dry_run:
            sens_cmd.append("--dry-run")
        try:
            result = subprocess.run(sens_cmd, env=os.environ.copy())
            if result.returncode != 0:
                log.error("Sensitivity analysis failed")
                has_failures = True
            else:
                log.info("Sensitivity analysis completed successfully")
        except Exception as e:
            log.error(f"Error running sensitivity analysis: {e}")
            has_failures = True

    # DB row-count verification (skip for dry runs)
    if not args.dry_run:
        verify_db_row_counts()

    if has_failures:
        sys.exit(1)


def print_scorecard(
    all_results: dict, scenarios: List[str], modules: List[str]
) -> bool:
    """
    Print a comprehensive scorecard showing results for all scenarios and modules.

    This is displayed at the very end so it's visible after logs scroll away.
    Returns True if any tasks failed.
    """
    # Build the scorecard
    print("\n")
    print("=" * 80)
    print("=" * 80)
    print("                         ETL PROCESSING SCORECARD")
    print("=" * 80)
    print("=" * 80)
    print()

    # Module abbreviations for compact display
    module_abbrev = {
        "reservoirs": "RES",
        "du_urban": "DU",
        "mi": "M&I",
        "cws_aggregate": "CWS",
        "ag": "AG",
        "refuge": "REF",
        "env_flows": "EF",
        "delta": "DLT",
        "sensitivity": "SENS",
    }

    # Legend
    print("Legend: ✅ = Success, ❌ = Failed, ⏭️ = Skipped, ⚪ = Not Run")
    print()

    # Header row
    header = "Scenario    │"
    for mod in modules:
        abbrev = module_abbrev.get(mod, mod[:4].upper())
        header += f" {abbrev:^5} │"
    header += " Status"
    print(header)
    print("─" * len(header))

    # Data rows
    total_success = 0
    total_failed = 0
    total_skipped = 0
    scenario_status = {}

    scenarios_with_dupes = []

    for scenario_id in scenarios:
        results = all_results.get(scenario_id, {})
        meta = results.get("_meta", {})
        n_dupes = meta.get("duplicate_b_parts", 0)
        row = f"{scenario_id:^11} │"

        scenario_successes = 0
        scenario_failures = 0
        scenario_skipped = 0

        for mod in modules:
            raw = results.get(mod)
            status = raw["status"] if isinstance(raw, dict) else (raw or "not_run")
            if status == "success":
                row += "  ✅   │"
                scenario_successes += 1
                total_success += 1
            elif status == "failed":
                row += "  ❌   │"
                scenario_failures += 1
                total_failed += 1
            elif status == "skipped":
                row += "  ⏭️   │"
                scenario_skipped += 1
                total_skipped += 1
            else:
                row += "  ⚪   │"

        if scenario_failures > 0:
            row += " ❌ FAILED"
            scenario_status[scenario_id] = "failed"
        elif scenario_skipped == len(modules):
            row += " ⚪ NOT RUN"
            scenario_status[scenario_id] = "not_run"
        elif scenario_successes == len(modules):
            row += " ✅ COMPLETE"
            scenario_status[scenario_id] = "complete"
        else:
            row += " ⚠️ PARTIAL"
            scenario_status[scenario_id] = "partial"

        if n_dupes > 0:
            row += f"  [DUP:{n_dupes}]"
            scenarios_with_dupes.append(scenario_id)

        print(row)

    print("─" * len(header))
    print()

    # Summary statistics
    total_scenarios = len(scenarios)
    complete_scenarios = sum(1 for s in scenario_status.values() if s == "complete")
    failed_scenarios = sum(1 for s in scenario_status.values() if s == "failed")
    partial_scenarios = sum(1 for s in scenario_status.values() if s == "partial")

    print("SUMMARY")
    print("─" * 40)
    print(f"  Scenarios:  {complete_scenarios}/{total_scenarios} complete")
    if failed_scenarios > 0:
        print(f"              {failed_scenarios} failed")
    if partial_scenarios > 0:
        print(f"              {partial_scenarios} partial")
    print()
    print(f"  Tasks:      {total_success} succeeded")
    if total_failed > 0:
        print(f"              {total_failed} failed")
    if total_skipped > 0:
        print(f"              {total_skipped} skipped")
    print()
    print(f"  Duplicates: {len(scenarios_with_dupes)} scenario(s) had duplicate B-parts")
    if scenarios_with_dupes:
        print(f"              {', '.join(scenarios_with_dupes)}")
    print()

    # List of failures for easy reference
    if total_failed > 0:
        print("FAILURES (need attention)")
        print("─" * 40)
        for scenario_id in scenarios:
            results = all_results.get(scenario_id, {})
            for mod, raw in results.items():
                if mod == "_meta":
                    continue
                st = raw["status"] if isinstance(raw, dict) else raw
                if st == "failed":
                    print(
                        f"  • {scenario_id} / {ETL_MODULES.get(mod, {}).get('name', mod)}"
                    )
        print()

    # Final status
    print("=" * 80)
    if total_failed == 0 and total_success > 0:
        print("                    🎉 ALL TASKS COMPLETED SUCCESSFULLY! 🎉")
    elif total_failed > 0:
        print(f"                    ⚠️  {total_failed} TASK(S) FAILED - REVIEW ABOVE ⚠️")
    else:
        print("                    ⚪ NO TASKS WERE RUN")
    print("=" * 80)
    print()

    return total_failed > 0


def write_audit_csv(
    all_results: dict,
    scenarios: List[str],
    modules: List[str],
    elapsed_total: float,
    dry_run: bool,
    output_path: str = "stats_audit.csv",
):
    """Write a structured audit CSV summarising every scenario × module."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for scenario_id in scenarios:
        results = all_results.get(scenario_id, {})
        meta = results.get("_meta", {})
        n_dupes = meta.get("duplicate_b_parts", 0)
        for mod in modules:
            raw = results.get(mod)
            if isinstance(raw, dict):
                status = raw["status"]
                elapsed = raw.get("elapsed_s", 0.0)
            elif raw:
                status = raw
                elapsed = 0.0
            else:
                status = "not_run"
                elapsed = 0.0
            rows.append(
                {
                    "timestamp": ts,
                    "scenario": scenario_id,
                    "module": mod,
                    "status": status,
                    "elapsed_s": f"{elapsed:.1f}",
                    "dry_run": str(dry_run),
                    "duplicate_b_parts": n_dupes,
                }
            )

    fieldnames = [
        "timestamp", "scenario", "module", "status", "elapsed_s",
        "dry_run", "duplicate_b_parts",
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    total = len(rows)
    ok = sum(1 for r in rows if r["status"] == "success")
    fail = sum(1 for r in rows if r["status"] == "failed")
    log.info(
        f"Audit CSV written to {output_path}  "
        f"({total} tasks: {ok} ok, {fail} failed, "
        f"{elapsed_total / 60:.1f} min total)"
    )


DB_ROW_COUNT_TABLES = [
    "reservoir_storage_monthly",
    "reservoir_period_summary",
    "ag_du_demand_monthly",
    "ag_du_period_summary",
    "du_delivery_monthly",
    "du_period_summary",
    "mi_delivery_monthly",
    "mi_contractor_period_summary",
    "cws_aggregate_monthly",
    "cws_aggregate_period_summary",
    "refuge_du_delivery_monthly",
    "refuge_du_period_summary",
    "env_flow_channel_monthly",
    "env_flow_channel_period_summary",
    "delta_monthly",
    "delta_period_summary",
    "sensitivity_climate",
    "sensitivity_operational",
]


def verify_db_row_counts(db_url: Optional[str] = None):
    """Print row counts for all statistics tables as a quick sanity check."""
    url = db_url or os.getenv("DATABASE_URL")
    if not url:
        log.info("Skipping DB verification (no DATABASE_URL)")
        return

    try:
        import psycopg2
    except ImportError:
        log.warning("psycopg2 not installed; skipping DB verification")
        return

    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
    except Exception as e:
        log.warning(f"Could not connect for verification: {e}")
        return

    print("\n" + "=" * 60)
    print("  DATABASE ROW COUNTS (verification)")
    print("=" * 60)
    print(f"  {'Table':<45} {'Rows':>10}")
    print("  " + "─" * 56)

    for table in DB_ROW_COUNT_TABLES:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
            count = cur.fetchone()[0]
            flag = "" if count > 0 else "  ⚠️ EMPTY"
            print(f"  {table:<45} {count:>10,}{flag}")
        except Exception:
            conn.rollback()
            print(f"  {table:<45} {'(missing)':>10}")

    # Scenario coverage: how many distinct scenarios per key table
    coverage_tables = [
        ("reservoir_storage_monthly", "scenario_short_code"),
        ("ag_du_demand_monthly", "scenario_short_code"),
        ("delta_monthly", "scenario_short_code"),
    ]
    print()
    print(f"  {'Table':<45} {'Scenarios':>10}")
    print("  " + "─" * 56)
    for table, col in coverage_tables:
        try:
            cur.execute(f"SELECT COUNT(DISTINCT {col}) FROM {table}")  # noqa: S608
            count = cur.fetchone()[0]
            print(f"  {table:<45} {count:>10}")
        except Exception:
            conn.rollback()
            print(f"  {table:<45} {'(n/a)':>10}")

    print("=" * 60)
    print()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
