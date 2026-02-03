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

# All known scenarios
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027', 's0029']


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
    
    # Final summary
    if len(scenarios) > 1:
        log.info("\n" + "#" * 60)
        log.info("# FINAL SUMMARY - ALL SCENARIOS")
        log.info("#" * 60)
        for scenario_id, results in all_results.items():
            successes = sum(1 for s in results.values() if s == 'success')
            total = len(results)
            icon = '✅' if successes == total else '⚠️' if successes > 0 else '❌'
            log.info(f"  {icon} {scenario_id}: {successes}/{total} modules succeeded")


if __name__ == '__main__':
    main()
