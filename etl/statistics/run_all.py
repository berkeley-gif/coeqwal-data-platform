#!/usr/bin/env python3
"""
Consolidated statistics ETL runner.

Runs all statistics calculations for a scenario in the correct order:
1. Reservoir statistics (storage, percentiles, spill, period summary)
2. Urban demand unit (DU) statistics (delivery, shortage)
3. M&I contractor statistics (delivery, shortage)
4. CWS aggregate statistics (SWP, CVP, MWD totals)
5. Agricultural (AG) statistics (delivery, shortage, aggregates)

Usage:
    # Run all statistics for a scenario
    python run_all.py --scenario s0029
    
    # Dry run (calculate but don't write to DB)
    python run_all.py --scenario s0029 --dry-run
    
    # Run only specific modules
    python run_all.py --scenario s0029 --only reservoirs,du_urban
    
    # Run all scenarios
    python run_all.py --all-scenarios
    
    # Use local CSV instead of S3
    python run_all.py --scenario s0029 --csv-path /path/to/csv
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

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
    'reservoirs': {
        'path': SCRIPT_DIR / 'main.py',
        'name': 'Reservoir Statistics',
        'tables': ['reservoir_monthly_percentile', 'reservoir_storage_monthly',
                   'reservoir_spill_monthly', 'reservoir_period_summary'],
    },
    'du_urban': {
        'path': SCRIPT_DIR / 'du_urban' / 'main.py',
        'name': 'Urban Demand Unit Statistics',
        'tables': ['du_delivery_monthly', 'du_shortage_monthly', 'du_period_summary'],
    },
    'mi': {
        'path': SCRIPT_DIR / 'mi' / 'main.py',
        'name': 'M&I Contractor Statistics',
        'tables': ['mi_delivery_monthly', 'mi_shortage_monthly', 'mi_contractor_period_summary'],
    },
    'cws_aggregate': {
        'path': SCRIPT_DIR / 'cws_aggregate' / 'main.py',
        'name': 'CWS Aggregate Statistics',
        'tables': ['cws_aggregate_monthly', 'cws_aggregate_period_summary'],
    },
    'ag': {
        'path': SCRIPT_DIR / 'ag' / 'main.py',
        'name': 'Agricultural Statistics',
        'tables': ['ag_du_delivery_monthly', 'ag_du_shortage_monthly', 'ag_du_period_summary',
                   'ag_aggregate_monthly', 'ag_aggregate_period_summary'],
    },
}

# All known scenarios (22 scenarios for full run)
SCENARIOS = [
    's0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0026', 's0027',
    's0028', 's0029', 's0030', 's0031', 's0032', 's0033', 's0039', 's0040',
    's0041', 's0042', 's0044', 's0045', 's0046', 's0065'
]


def run_module(
    module_name: str,
    scenario_id: str,
    dry_run: bool = False,
    csv_path: Optional[str] = None,
) -> bool:
    """
    Run a single ETL module for a scenario.
    
    Returns True if successful, False otherwise.
    """
    module = ETL_MODULES.get(module_name)
    if not module:
        log.error(f"Unknown module: {module_name}")
        return False
    
    script_path = module['path']
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return False
    
    log.info(f"{'='*60}")
    log.info(f"Running: {module['name']} for {scenario_id}")
    log.info(f"Tables: {', '.join(module['tables'])}")
    log.info(f"{'='*60}")
    
    # Build command
    cmd = [sys.executable, str(script_path), '--scenario', scenario_id]
    
    if dry_run:
        cmd.append('--dry-run')
    
    if csv_path:
        # Convert to absolute path for submodules running in different directories
        abs_csv_path = str(Path(csv_path).resolve())
        cmd.extend(['--csv-path', abs_csv_path])
    
    # Run the script
    try:
        result = subprocess.run(
            cmd,
            cwd=script_path.parent,
            env=os.environ.copy(),
            capture_output=False,  # Stream output to console
        )
        
        if result.returncode != 0:
            log.error(f"Module {module_name} failed with return code {result.returncode}")
            return False
        
        log.info(f"✅ {module['name']} completed successfully")
        return True
        
    except Exception as e:
        log.error(f"Error running {module_name}: {e}")
        return False


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
    
    # Also clean up any /tmp/s0* pattern (catches all scenario temp files)
    all_scenario_tmp = glob.glob("/tmp/s0*")
    if all_scenario_tmp:
        for f in all_scenario_tmp:
            try:
                if os.path.isdir(f):
                    shutil.rmtree(f)
                else:
                    os.remove(f)
            except Exception as e:
                log.warning(f"Could not remove {f}: {e}")
        log.info(f"Cleaned up {len(all_scenario_tmp)} additional temp files")


def run_all_modules(
    scenario_id: str,
    modules: Optional[List[str]] = None,
    dry_run: bool = False,
    csv_path: Optional[str] = None,
    continue_on_error: bool = False,
) -> dict:
    """
    Run all (or specified) ETL modules for a scenario.
    
    Returns dict with results for each module.
    """
    if modules is None:
        modules = list(ETL_MODULES.keys())
    
    results = {}
    
    log.info(f"\n{'#'*60}")
    log.info(f"# PROCESSING SCENARIO: {scenario_id}")
    log.info(f"# Modules: {', '.join(modules)}")
    log.info(f"# Dry run: {dry_run}")
    log.info(f"{'#'*60}\n")
    
    for module_name in modules:
        success = run_module(module_name, scenario_id, dry_run, csv_path)
        results[module_name] = 'success' if success else 'failed'
        
        if not success and not continue_on_error:
            log.error(f"Stopping due to failure in {module_name}")
            break
    
    # Clean up temp files after processing each scenario to free memory
    cleanup_temp_files(scenario_id)
    
    # Summary
    log.info(f"\n{'='*60}")
    log.info(f"SUMMARY for {scenario_id}:")
    for module_name, status in results.items():
        icon = '✅' if status == 'success' else '❌'
        log.info(f"  {icon} {ETL_MODULES[module_name]['name']}: {status}")
    log.info(f"{'='*60}\n")
    
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
        """
    )
    
    parser.add_argument(
        '--scenario', '-s',
        help='Scenario ID to process (e.g., s0029)'
    )
    parser.add_argument(
        '--all-scenarios',
        action='store_true',
        help='Process all known scenarios'
    )
    parser.add_argument(
        '--only',
        help=f'Comma-separated list of modules to run. Available: {", ".join(ETL_MODULES.keys())}'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Calculate statistics but do not write to database'
    )
    parser.add_argument(
        '--csv-path',
        help='Local CSV file path (instead of loading from S3)'
    )
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue processing other modules even if one fails'
    )
    parser.add_argument(
        '--list-modules',
        action='store_true',
        help='List available modules and exit'
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
        modules = [m.strip() for m in args.only.split(',')]
        invalid = [m for m in modules if m not in ETL_MODULES]
        if invalid:
            parser.error(f"Unknown modules: {', '.join(invalid)}. Available: {', '.join(ETL_MODULES.keys())}")
    
    # Check DATABASE_URL
    if not args.dry_run and not os.getenv('DATABASE_URL'):
        parser.error("DATABASE_URL environment variable required (or use --dry-run)")
    
    # Determine scenarios
    scenarios = SCENARIOS if args.all_scenarios else [args.scenario]
    
    # Process each scenario
    all_results = {}
    for scenario_id in scenarios:
        results = run_all_modules(
            scenario_id,
            modules=modules,
            dry_run=args.dry_run,
            csv_path=args.csv_path,
            continue_on_error=args.continue_on_error,
        )
        all_results[scenario_id] = results
    
    # Print comprehensive scorecard at the end
    print_scorecard(all_results, scenarios, modules or list(ETL_MODULES.keys()))


def print_scorecard(all_results: dict, scenarios: List[str], modules: List[str]):
    """
    Print a comprehensive scorecard showing results for all scenarios and modules.
    
    This is displayed at the very end so it's visible after logs scroll away.
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
        'reservoirs': 'RES',
        'du_urban': 'DU',
        'mi': 'M&I',
        'cws_aggregate': 'CWS',
        'ag': 'AG',
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
    
    for scenario_id in scenarios:
        results = all_results.get(scenario_id, {})
        row = f"{scenario_id:^11} │"
        
        scenario_successes = 0
        scenario_failures = 0
        scenario_skipped = 0
        
        for mod in modules:
            status = results.get(mod, 'not_run')
            if status == 'success':
                row += "  ✅   │"
                scenario_successes += 1
                total_success += 1
            elif status == 'failed':
                row += "  ❌   │"
                scenario_failures += 1
                total_failed += 1
            elif status == 'skipped':
                row += "  ⏭️   │"
                scenario_skipped += 1
                total_skipped += 1
            else:
                row += "  ⚪   │"
        
        # Scenario overall status
        if scenario_failures > 0:
            row += " ❌ FAILED"
            scenario_status[scenario_id] = 'failed'
        elif scenario_skipped == len(modules):
            row += " ⚪ NOT RUN"
            scenario_status[scenario_id] = 'not_run'
        elif scenario_successes == len(modules):
            row += " ✅ COMPLETE"
            scenario_status[scenario_id] = 'complete'
        else:
            row += " ⚠️ PARTIAL"
            scenario_status[scenario_id] = 'partial'
        
        print(row)
    
    print("─" * len(header))
    print()
    
    # Summary statistics
    total_scenarios = len(scenarios)
    complete_scenarios = sum(1 for s in scenario_status.values() if s == 'complete')
    failed_scenarios = sum(1 for s in scenario_status.values() if s == 'failed')
    partial_scenarios = sum(1 for s in scenario_status.values() if s == 'partial')
    
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
    
    # List of failures for easy reference
    if total_failed > 0:
        print("FAILURES (need attention)")
        print("─" * 40)
        for scenario_id in scenarios:
            results = all_results.get(scenario_id, {})
            failures = [mod for mod, status in results.items() if status == 'failed']
            if failures:
                for mod in failures:
                    print(f"  • {scenario_id} / {ETL_MODULES[mod]['name']}")
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


if __name__ == '__main__':
    main()
