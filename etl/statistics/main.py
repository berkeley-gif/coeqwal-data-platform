#!/usr/bin/env python3
"""
Main entry point for reservoir statistics ETL.

Designed for automated pipelines:
- AWS Lambda triggered by S3 ObjectCreated events
- Direct database writes via psycopg2
- CLI for manual runs and testing

Usage:
    # CLI
    python main.py --scenario s0020
    python main.py --s3-key scenario/s0020/csv/s0020_coeqwal_calsim_output.csv

    # Lambda handler
    from main import lambda_handler
"""

import argparse
import json
import logging
import os
import re
from typing import Any, Dict, Optional

# Import calculation modules
from reservoirs.reservoir_metrics import RESERVOIR_THRESHOLDS
from reservoirs.calculate_reservoir_statistics import (
    calculate_all_statistics,
    load_reservoir_entities,
    SCENARIOS,
)
from reservoirs.calculate_reservoir_percentiles import (
    calculate_all_reservoir_percentiles,
)

# Optional: psycopg2 for direct database writes
try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("statistics_etl")

# Environment config
DATABASE_URL = os.getenv('DATABASE_URL')
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')


def extract_scenario_from_s3_key(s3_key: str) -> Optional[str]:
    """
    Extract scenario ID from S3 object key.

    Expected patterns:
    - scenario/s0020/csv/s0020_coeqwal_calsim_output.csv
    - scenario/s0020/csv/s0020_DV.csv
    """
    match = re.search(r'scenario/(s\d{4})/', s3_key)
    if match:
        return match.group(1)
    return None


def get_db_connection():
    """Get database connection from DATABASE_URL."""
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 required for database writes. Install with: pip install psycopg2-binary")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(DATABASE_URL)


def write_percentiles_to_db(scenario_id: str, results: Dict[str, Any]) -> int:
    """
    Write percentile data directly to reservoir_monthly_percentile table.

    Includes both percent-of-capacity and TAF values.
    Returns number of rows written.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    rows = []
    for short_code, res_data in results['reservoirs'].items():
        entity_id = res_data['id']
        capacity_taf = res_data.get('capacity_taf')

        for water_month, stats in res_data['monthly_percentiles'].items():
            rows.append((
                scenario_id,
                entity_id,
                int(water_month),
                # Percent of capacity values
                stats.get('q0'),
                stats.get('q10'),
                stats.get('q30'),
                stats.get('q50'),
                stats.get('q70'),
                stats.get('q90'),
                stats.get('q100'),
                stats.get('mean'),
                # TAF values
                stats.get('q0_taf'),
                stats.get('q10_taf'),
                stats.get('q30_taf'),
                stats.get('q50_taf'),
                stats.get('q70_taf'),
                stats.get('q90_taf'),
                stats.get('q100_taf'),
                stats.get('mean_taf'),
                capacity_taf,
            ))

    if rows:
        execute_values(
            cursor,
            """
            INSERT INTO reservoir_monthly_percentile (
                scenario_short_code, reservoir_entity_id, water_month,
                q0, q10, q30, q50, q70, q90, q100, mean_value,
                q0_taf, q10_taf, q30_taf, q50_taf, q70_taf, q90_taf, q100_taf, mean_taf,
                capacity_taf
            ) VALUES %s
            ON CONFLICT (scenario_short_code, reservoir_entity_id, water_month)
            DO UPDATE SET
                q0 = EXCLUDED.q0, q10 = EXCLUDED.q10, q30 = EXCLUDED.q30,
                q50 = EXCLUDED.q50, q70 = EXCLUDED.q70, q90 = EXCLUDED.q90,
                q100 = EXCLUDED.q100, mean_value = EXCLUDED.mean_value,
                q0_taf = EXCLUDED.q0_taf, q10_taf = EXCLUDED.q10_taf, q30_taf = EXCLUDED.q30_taf,
                q50_taf = EXCLUDED.q50_taf, q70_taf = EXCLUDED.q70_taf, q90_taf = EXCLUDED.q90_taf,
                q100_taf = EXCLUDED.q100_taf, mean_taf = EXCLUDED.mean_taf,
                capacity_taf = EXCLUDED.capacity_taf,
                updated_at = NOW()
            """,
            rows
        )
        conn.commit()

    cursor.close()
    conn.close()

    log.info(f"Wrote {len(rows)} percentile rows for {scenario_id}")
    return len(rows)


def write_statistics_to_db(
    scenario_id: str,
    storage_monthly: list,
    spill_monthly: list,
    period_summary: list
) -> Dict[str, int]:
    """
    Write statistics directly to database tables.

    Returns dict with counts of rows written to each table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    counts = {}

    # Storage monthly
    if storage_monthly:
        storage_rows = [
            (
                row['scenario_short_code'],
                row['reservoir_entity_id'],
                row['water_month'],
                row['storage_avg_taf'],
                row['storage_cv'],
                row['storage_pct_capacity'],
                # Percentiles as % of capacity
                row['q0'], row['q10'], row['q30'], row['q50'],
                row['q70'], row['q90'], row['q100'],
                # Percentiles in TAF (volume)
                row.get('q0_taf'), row.get('q10_taf'), row.get('q30_taf'),
                row.get('q50_taf'), row.get('q70_taf'), row.get('q90_taf'),
                row.get('q100_taf'),
                row['capacity_taf'],
                row['sample_count'],
            )
            for row in storage_monthly
        ]
        execute_values(
            cursor,
            """
            INSERT INTO reservoir_storage_monthly (
                scenario_short_code, reservoir_entity_id, water_month,
                storage_avg_taf, storage_cv, storage_pct_capacity,
                q0, q10, q30, q50, q70, q90, q100,
                q0_taf, q10_taf, q30_taf, q50_taf, q70_taf, q90_taf, q100_taf,
                capacity_taf, sample_count
            ) VALUES %s
            ON CONFLICT (scenario_short_code, reservoir_entity_id, water_month)
            DO UPDATE SET
                storage_avg_taf = EXCLUDED.storage_avg_taf,
                storage_cv = EXCLUDED.storage_cv,
                storage_pct_capacity = EXCLUDED.storage_pct_capacity,
                q0 = EXCLUDED.q0, q10 = EXCLUDED.q10, q30 = EXCLUDED.q30,
                q50 = EXCLUDED.q50, q70 = EXCLUDED.q70, q90 = EXCLUDED.q90,
                q100 = EXCLUDED.q100,
                q0_taf = EXCLUDED.q0_taf, q10_taf = EXCLUDED.q10_taf,
                q30_taf = EXCLUDED.q30_taf, q50_taf = EXCLUDED.q50_taf,
                q70_taf = EXCLUDED.q70_taf, q90_taf = EXCLUDED.q90_taf,
                q100_taf = EXCLUDED.q100_taf,
                capacity_taf = EXCLUDED.capacity_taf,
                sample_count = EXCLUDED.sample_count,
                updated_at = NOW()
            """,
            storage_rows
        )
        counts['storage_monthly'] = len(storage_rows)

    # Spill monthly
    if spill_monthly:
        spill_rows = [
            (
                row['scenario_short_code'],
                row['reservoir_entity_id'],
                row['water_month'],
                row['spill_months_count'],
                row['total_months'],
                row['spill_frequency_pct'],
                row['spill_avg_cfs'],
                row['spill_max_cfs'],
                row['spill_q50'],
                row['spill_q90'],
                row['spill_q100'],
                row.get('storage_at_spill_avg_pct'),
            )
            for row in spill_monthly
        ]
        execute_values(
            cursor,
            """
            INSERT INTO reservoir_spill_monthly (
                scenario_short_code, reservoir_entity_id, water_month,
                spill_months_count, total_months, spill_frequency_pct,
                spill_avg_cfs, spill_max_cfs,
                spill_q50, spill_q90, spill_q100,
                storage_at_spill_avg_pct
            ) VALUES %s
            ON CONFLICT (scenario_short_code, reservoir_entity_id, water_month)
            DO UPDATE SET
                spill_months_count = EXCLUDED.spill_months_count,
                total_months = EXCLUDED.total_months,
                spill_frequency_pct = EXCLUDED.spill_frequency_pct,
                spill_avg_cfs = EXCLUDED.spill_avg_cfs,
                spill_max_cfs = EXCLUDED.spill_max_cfs,
                spill_q50 = EXCLUDED.spill_q50,
                spill_q90 = EXCLUDED.spill_q90,
                spill_q100 = EXCLUDED.spill_q100,
                storage_at_spill_avg_pct = EXCLUDED.storage_at_spill_avg_pct,
                updated_at = NOW()
            """,
            spill_rows
        )
        counts['spill_monthly'] = len(spill_rows)

    # Period summary (includes probability metrics)
    if period_summary:
        summary_rows = [
            (
                row['scenario_short_code'],
                row['reservoir_entity_id'],
                row['simulation_start_year'],
                row['simulation_end_year'],
                row['total_years'],
                row.get('storage_exc_p5'),
                row.get('storage_exc_p10'),
                row.get('storage_exc_p25'),
                row.get('storage_exc_p50'),
                row.get('storage_exc_p75'),
                row.get('storage_exc_p90'),
                row.get('storage_exc_p95'),
                row['dead_pool_taf'],
                row['dead_pool_pct'],
                row.get('spill_threshold_pct'),
                row['spill_years_count'],
                row['spill_frequency_pct'],
                row['spill_mean_cfs'],
                row['spill_peak_cfs'],
                row['annual_spill_avg_taf'],
                row['annual_spill_cv'],
                row['annual_spill_max_taf'],
                row['annual_max_spill_q50'],
                row['annual_max_spill_q90'],
                row['annual_max_spill_q100'],
                # Probability metrics
                row.get('flood_pool_prob_all'),
                row.get('flood_pool_prob_september'),
                row.get('flood_pool_prob_april'),
                row.get('dead_pool_prob_all'),
                row.get('dead_pool_prob_september'),
                row.get('storage_cv_all'),
                row.get('storage_cv_april'),
                row.get('storage_cv_september'),
                row.get('annual_avg_taf'),
                row.get('april_avg_taf'),
                row.get('september_avg_taf'),
                row['capacity_taf'],
            )
            for row in period_summary
        ]
        execute_values(
            cursor,
            """
            INSERT INTO reservoir_period_summary (
                scenario_short_code, reservoir_entity_id,
                simulation_start_year, simulation_end_year, total_years,
                storage_exc_p5, storage_exc_p10, storage_exc_p25, storage_exc_p50,
                storage_exc_p75, storage_exc_p90, storage_exc_p95,
                dead_pool_taf, dead_pool_pct, spill_threshold_pct,
                spill_years_count, spill_frequency_pct,
                spill_mean_cfs, spill_peak_cfs,
                annual_spill_avg_taf, annual_spill_cv, annual_spill_max_taf,
                annual_max_spill_q50, annual_max_spill_q90, annual_max_spill_q100,
                flood_pool_prob_all, flood_pool_prob_september, flood_pool_prob_april,
                dead_pool_prob_all, dead_pool_prob_september,
                storage_cv_all, storage_cv_april, storage_cv_september,
                annual_avg_taf, april_avg_taf, september_avg_taf,
                capacity_taf
            ) VALUES %s
            ON CONFLICT (scenario_short_code, reservoir_entity_id)
            DO UPDATE SET
                simulation_start_year = EXCLUDED.simulation_start_year,
                simulation_end_year = EXCLUDED.simulation_end_year,
                total_years = EXCLUDED.total_years,
                storage_exc_p5 = EXCLUDED.storage_exc_p5,
                storage_exc_p10 = EXCLUDED.storage_exc_p10,
                storage_exc_p25 = EXCLUDED.storage_exc_p25,
                storage_exc_p50 = EXCLUDED.storage_exc_p50,
                storage_exc_p75 = EXCLUDED.storage_exc_p75,
                storage_exc_p90 = EXCLUDED.storage_exc_p90,
                storage_exc_p95 = EXCLUDED.storage_exc_p95,
                dead_pool_taf = EXCLUDED.dead_pool_taf,
                dead_pool_pct = EXCLUDED.dead_pool_pct,
                spill_threshold_pct = EXCLUDED.spill_threshold_pct,
                spill_years_count = EXCLUDED.spill_years_count,
                spill_frequency_pct = EXCLUDED.spill_frequency_pct,
                spill_mean_cfs = EXCLUDED.spill_mean_cfs,
                spill_peak_cfs = EXCLUDED.spill_peak_cfs,
                annual_spill_avg_taf = EXCLUDED.annual_spill_avg_taf,
                annual_spill_cv = EXCLUDED.annual_spill_cv,
                annual_spill_max_taf = EXCLUDED.annual_spill_max_taf,
                annual_max_spill_q50 = EXCLUDED.annual_max_spill_q50,
                annual_max_spill_q90 = EXCLUDED.annual_max_spill_q90,
                annual_max_spill_q100 = EXCLUDED.annual_max_spill_q100,
                flood_pool_prob_all = EXCLUDED.flood_pool_prob_all,
                flood_pool_prob_september = EXCLUDED.flood_pool_prob_september,
                flood_pool_prob_april = EXCLUDED.flood_pool_prob_april,
                dead_pool_prob_all = EXCLUDED.dead_pool_prob_all,
                dead_pool_prob_september = EXCLUDED.dead_pool_prob_september,
                storage_cv_all = EXCLUDED.storage_cv_all,
                storage_cv_april = EXCLUDED.storage_cv_april,
                storage_cv_september = EXCLUDED.storage_cv_september,
                annual_avg_taf = EXCLUDED.annual_avg_taf,
                april_avg_taf = EXCLUDED.april_avg_taf,
                september_avg_taf = EXCLUDED.september_avg_taf,
                capacity_taf = EXCLUDED.capacity_taf,
                updated_at = NOW()
            """,
            summary_rows
        )
        counts['period_summary'] = len(summary_rows)

    conn.commit()
    cursor.close()
    conn.close()

    log.info(f"Wrote statistics for {scenario_id}: {counts}")
    return counts


def process_scenario(
    scenario_id: str,
    write_to_db: bool = True,
    csv_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Process a single scenario: calculate all statistics and optionally write to DB.

    Args:
        scenario_id: e.g., 's0020'
        write_to_db: If True, write directly to database. If False, return data only.
        csv_path: Optional local CSV file path (uses S3 if not provided)

    Returns:
        Dict with processing results and counts.
    """
    log.info(f"Processing scenario: {scenario_id}")
    if csv_path:
        log.info(f"Using local CSV: {csv_path}")

    # Load reservoir metadata
    reservoirs = load_reservoir_entities()

    result = {
        'scenario_id': scenario_id,
        'status': 'success',
        'counts': {},
    }

    try:
        # Calculate percentiles (for UI charts)
        log.info("Calculating percentiles...")
        percentile_results = calculate_all_reservoir_percentiles(scenario_id, reservoirs, csv_path)

        if write_to_db:
            result['counts']['percentiles'] = write_percentiles_to_db(scenario_id, percentile_results)
        else:
            result['percentile_data'] = percentile_results

        # Calculate comprehensive statistics
        log.info("Calculating statistics...")
        storage_monthly, spill_monthly, period_summary = calculate_all_statistics(
            scenario_id, reservoirs, csv_path
        )

        if write_to_db:
            stats_counts = write_statistics_to_db(
                scenario_id, storage_monthly, spill_monthly, period_summary
            )
            result['counts'].update(stats_counts)
        else:
            result['storage_monthly'] = storage_monthly
            result['spill_monthly'] = spill_monthly
            result['period_summary'] = period_summary

    except Exception as e:
        log.error(f"Error processing {scenario_id}: {e}")
        result['status'] = 'error'
        result['error'] = str(e)
        raise

    log.info(f"Completed {scenario_id}: {result['counts']}")
    return result


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for S3-triggered ETL.

    Triggered by S3 ObjectCreated events when a new scenario CSV is uploaded.

    Event structure:
    {
        "Records": [{
            "s3": {
                "bucket": {"name": "coeqwal-model-run"},
                "object": {"key": "scenario/s0020/csv/s0020_coeqwal_calsim_output.csv"}
            }
        }]
    }
    """
    log.info(f"Lambda triggered with event: {json.dumps(event)}")

    results = []

    for record in event.get('Records', []):
        s3_info = record.get('s3', {})
        bucket = s3_info.get('bucket', {}).get('name')
        key = s3_info.get('object', {}).get('key')

        if not key:
            log.warning("No S3 key in record, skipping")
            continue

        # Extract scenario ID from key
        scenario_id = extract_scenario_from_s3_key(key)
        if not scenario_id:
            log.warning(f"Could not extract scenario ID from key: {key}")
            continue

        log.info(f"Processing scenario {scenario_id} from s3://{bucket}/{key}")

        try:
            result = process_scenario(scenario_id, write_to_db=True)
            results.append(result)
        except Exception as e:
            results.append({
                'scenario_id': scenario_id,
                'status': 'error',
                'error': str(e),
            })

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processed {len(results)} scenarios',
            'results': results,
        })
    }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Reservoir statistics ETL - calculates and loads statistics to database'
    )
    parser.add_argument(
        '--scenario', '-s',
        help='Scenario ID to process (e.g., s0020)'
    )
    parser.add_argument(
        '--s3-key',
        help='S3 object key (extracts scenario ID from path)'
    )
    parser.add_argument(
        '--all-scenarios',
        action='store_true',
        help='Process all known scenarios'
    )
    parser.add_argument(
        '--csv-path',
        help='Local CSV file path (instead of loading from S3)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Calculate but do not write to database'
    )
    parser.add_argument(
        '--output-json',
        action='store_true',
        help='Output results as JSON (implies --dry-run)'
    )

    args = parser.parse_args()

    # Determine scenarios to process
    scenarios = []
    if args.s3_key:
        scenario_id = extract_scenario_from_s3_key(args.s3_key)
        if scenario_id:
            scenarios.append(scenario_id)
        else:
            parser.error(f"Could not extract scenario ID from: {args.s3_key}")
    elif args.scenario:
        scenarios.append(args.scenario)
    elif args.all_scenarios:
        scenarios = SCENARIOS
    else:
        parser.error("Specify --scenario, --s3-key, or --all-scenarios")

    write_to_db = not (args.dry_run or args.output_json)

    if write_to_db and not DATABASE_URL:
        parser.error("DATABASE_URL environment variable required for database writes")

    all_results = []
    for scenario_id in scenarios:
        try:
            result = process_scenario(
                scenario_id,
                write_to_db=write_to_db,
                csv_path=args.csv_path
            )
            all_results.append(result)
        except Exception as e:
            log.error(f"Failed to process {scenario_id}: {e}")
            if len(scenarios) == 1:
                raise

    if args.output_json:
        print(json.dumps(all_results, indent=2, default=str))

    log.info(f"Completed processing {len(all_results)} scenarios")


if __name__ == '__main__':
    main()
