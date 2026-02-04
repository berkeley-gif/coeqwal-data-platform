#!/usr/bin/env python3
"""
Calculate delivery and demand statistics for urban demand units.

Version 2: Uses database variable mappings and loads demand CSV.

Approach:
1. Load variable mappings from du_urban_variable table
2. Load main CalSim output CSV (deliveries, shortages)
3. Load demand CSV (UD_*, DEM_D_*_PMI demands)
4. Calculate statistics including pct_demand_met
5. Insert into database tables

Usage:
    python calculate_du_statistics_v2.py --scenario s0020
    python calculate_du_statistics_v2.py --scenario s0020 --local
    python calculate_du_statistics_v2.py --all-scenarios
"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Optional imports
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    import psycopg2
    from psycopg2.extras import execute_values, RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("du_statistics_v2")

# Configuration
SCENARIOS = ['s0011', 's0020', 's0021', 's0024', 's0025', 's0027', 's0029']
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Local paths
LOCAL_OUTPUT_DIR = PROJECT_ROOT / "etl/pipelines"
LOCAL_DEMAND_DIR = PROJECT_ROOT / "etl/demands"

# Percentiles
PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# Unit conversion: CFS (cubic feet per second) to TAF (thousand acre-feet)
# TAF = CFS * seconds_per_day * days / (43560 sq ft per acre) / 1000
# Simplified: CFS * days * 86400 / 43560 / 1000 = CFS * days * 0.001983471
CFS_TO_TAF_PER_DAY = 0.001983471


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def get_variable_mappings(conn) -> Dict[str, Dict]:
    """
    Load variable mappings from du_urban_variable table.
    
    Returns:
        Dict mapping du_id to {delivery_variable, demand_variable, shortage_variable, ...}
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT du_id, delivery_variable, demand_variable, shortage_variable, 
               variable_type, requires_sum
        FROM du_urban_variable
        WHERE is_active = TRUE
    """)
    rows = cur.fetchall()
    cur.close()
    
    mappings = {row['du_id']: dict(row) for row in rows}
    log.info(f"Loaded {len(mappings)} variable mappings from database")
    return mappings


def get_delivery_arcs(conn) -> Dict[str, List[str]]:
    """
    Load delivery arcs for multi-arc DUs from du_urban_delivery_arc table.
    
    Returns:
        Dict mapping du_id to list of arc variable names
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT du_id, delivery_arc
        FROM du_urban_delivery_arc
        WHERE is_active = TRUE
        ORDER BY du_id, arc_order
    """)
    rows = cur.fetchall()
    cur.close()
    
    arcs = {}
    for row in rows:
        du_id = row['du_id']
        if du_id not in arcs:
            arcs[du_id] = []
        arcs[du_id].append(row['delivery_arc'])
    
    log.info(f"Loaded delivery arcs for {len(arcs)} multi-arc DUs")
    return arcs


# =============================================================================
# DATA LOADING
# =============================================================================

def load_csv_with_dss_headers(file_path: str) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load CSV with DSS-style multi-row headers.
    
    Row structure:
        Row 0 (A): Source (CALSIM, MANUAL-ADD, CALCULATED)
        Row 1 (B): Variable name
        Row 2 (C): Kind
        Row 3 (E): Time step
        Row 4 (F): Level
        Row 5 (type): Type
        Row 6 (units): Units
        Row 7+: Data
    
    Returns:
        Tuple of (DataFrame with data, list of variable names from row B)
    """
    log.info(f"Loading CSV: {file_path}")
    
    # Read header rows to get variable names (row B = row 1)
    header_df = pd.read_csv(file_path, header=None, nrows=7)
    var_names = header_df.iloc[1].tolist()  # Row B has variable names
    
    # Read data portion (skip 7 header rows)
    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    
    # Set simple column names (avoid multi-index issues)
    data_df.columns = range(len(data_df.columns))
    
    # Create a mapping from variable name to column index
    var_to_idx = {}
    for idx, var in enumerate(var_names):
        if var not in var_to_idx:  # First occurrence wins
            var_to_idx[var] = idx
    
    # Create column names, handling duplicates by appending index
    col_names = []
    seen = {}
    for idx, var in enumerate(var_names):
        if var in seen:
            col_names.append(f"{var}_{seen[var]}")
            seen[var] += 1
        else:
            col_names.append(var)
            seen[var] = 1
    
    data_df.columns = col_names
    
    # First column is date
    date_col = col_names[0]
    data_df['DateTime'] = pd.to_datetime(data_df[date_col], errors='coerce')
    
    log.info(f"Loaded {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df, var_names


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """Add water year, water month, and days in month columns."""
    df = df.copy()
    
    if 'DateTime' not in df.columns or df['DateTime'].isna().all():
        raise ValueError("DateTime column not found or all null")
    
    df['CalendarMonth'] = df['DateTime'].dt.month
    df['CalendarYear'] = df['DateTime'].dt.year
    df['DaysInMonth'] = df['DateTime'].dt.daysinmonth
    
    # Water month: Oct(10)->1, Nov(11)->2, ..., Sep(9)->12
    df['WaterMonth'] = ((df['CalendarMonth'] - 10) % 12) + 1
    
    # Water year: Oct-Dec belong to next water year
    df['WaterYear'] = df['CalendarYear']
    df.loc[df['CalendarMonth'] >= 10, 'WaterYear'] += 1
    
    log.info(f"Date range: {df['DateTime'].min()} to {df['DateTime'].max()}")
    log.info(f"Water years: {df['WaterYear'].min()} to {df['WaterYear'].max()}")
    
    return df


def load_scenario_data(
    scenario_id: str,
    use_local: bool = False,
    output_csv_path: Optional[str] = None,
    demand_csv_path: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load main CalSim output and demand CSV for a scenario.
    
    Args:
        scenario_id: Scenario ID (e.g., 's0020')
        use_local: Use local files instead of S3
        output_csv_path: Override path for main output CSV
        demand_csv_path: Override path for demand CSV
    
    Returns:
        Tuple of (output_df, demand_df)
    """
    if use_local:
        # Local file paths
        if output_csv_path is None:
            output_csv_path = str(LOCAL_OUTPUT_DIR / f"{scenario_id}_coeqwal_calsim_output.csv")
        if demand_csv_path is None:
            demand_csv_path = str(LOCAL_DEMAND_DIR / f"{scenario_id}_demand.csv")
    else:
        # S3 paths (download to temp)
        if not HAS_BOTO3:
            raise ImportError("boto3 required for S3 access")
        
        s3 = boto3.client('s3')
        
        # Download main output
        output_key = f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv"
        output_csv_path = f"/tmp/{scenario_id}_output.csv"
        log.info(f"Downloading s3://{S3_BUCKET}/{output_key}")
        s3.download_file(S3_BUCKET, output_key, output_csv_path)
        
        # Download demand CSV
        demand_key = f"reference/{scenario_id}_demand.csv"
        demand_csv_path = f"/tmp/{scenario_id}_demand.csv"
        log.info(f"Downloading s3://{S3_BUCKET}/{demand_key}")
        s3.download_file(S3_BUCKET, demand_key, demand_csv_path)
    
    # Load CSVs
    output_df, output_cols = load_csv_with_dss_headers(output_csv_path)
    demand_df, demand_cols = load_csv_with_dss_headers(demand_csv_path)
    
    # Add water year/month
    output_df = add_water_year_month(output_df)
    demand_df = add_water_year_month(demand_df)
    
    return output_df, demand_df


# =============================================================================
# STATISTICS CALCULATION
# =============================================================================

def get_column_value(df: pd.DataFrame, column_name: str) -> pd.Series:
    """Get column values, handling missing columns gracefully."""
    if column_name is None or column_name == 'NOT_FOUND':
        return pd.Series([np.nan] * len(df), index=df.index, dtype=float)
    
    if column_name not in df.columns:
        log.debug(f"Column not found: {column_name}")
        return pd.Series([np.nan] * len(df), index=df.index, dtype=float)
    
    col_data = df[column_name]
    
    # Handle case where column lookup returns DataFrame (duplicate columns)
    if isinstance(col_data, pd.DataFrame):
        col_data = col_data.iloc[:, 0]  # Take first column
    
    return pd.to_numeric(col_data, errors='coerce')


def calculate_du_statistics(
    output_df: pd.DataFrame,
    demand_df: pd.DataFrame,
    mappings: Dict[str, Dict],
    delivery_arcs: Dict[str, List[str]],
    scenario_id: str
) -> Tuple[List[Dict], List[Dict]]:
    """
    Calculate delivery and demand statistics for all DUs.
    
    Returns:
        Tuple of (delivery_monthly_rows, period_summary_rows)
    """
    delivery_monthly_rows = []
    period_summary_rows = []
    
    processed = 0
    skipped = 0
    
    for du_id, mapping in mappings.items():
        delivery_var = mapping.get('delivery_variable')
        demand_var = mapping.get('demand_variable')
        shortage_var = mapping.get('shortage_variable')
        requires_sum = mapping.get('requires_sum', False)
        
        # Get delivery values (in CFS)
        if requires_sum and du_id in delivery_arcs:
            # Sum multiple delivery arcs
            arc_series = [get_column_value(output_df, arc) for arc in delivery_arcs[du_id]]
            delivery_cfs = sum(arc_series)
        else:
            delivery_cfs = get_column_value(output_df, delivery_var)
        
        # Convert delivery from CFS to TAF (monthly)
        # TAF = CFS * days_in_month * CFS_TO_TAF_PER_DAY
        delivery = delivery_cfs * output_df['DaysInMonth'] * CFS_TO_TAF_PER_DAY
        
        # Get demand values (already in TAF from demand CSV)
        demand = get_column_value(demand_df, demand_var)
        
        # Get shortage values (in CFS, convert to TAF)
        shortage_cfs = get_column_value(output_df, shortage_var)
        shortage = shortage_cfs * output_df['DaysInMonth'] * CFS_TO_TAF_PER_DAY
        
        # Skip if no data
        if delivery.isna().all() and demand.isna().all():
            log.debug(f"Skipping {du_id}: no delivery or demand data")
            skipped += 1
            continue
        
        processed += 1
        
        # Calculate monthly statistics
        for wm in range(1, 13):
            wm_mask = output_df['WaterMonth'] == wm
            
            del_month = delivery[wm_mask].dropna()
            dem_month = demand[wm_mask].dropna() if demand.notna().any() else pd.Series(dtype=float)
            
            if del_month.empty and dem_month.empty:
                continue
            
            # Match existing table schema: scenario_short_code, not scenario_id
            row = {
                'scenario_short_code': scenario_id,
                'du_id': du_id,
                'water_month': wm,
                'sample_count': len(del_month) if not del_month.empty else 0,
            }
            
            # Delivery statistics (match existing column names: delivery_avg_taf, q0, q10, etc.)
            if not del_month.empty:
                row['delivery_avg_taf'] = round(float(del_month.mean()), 2)
                row['delivery_cv'] = round(float(del_month.std() / del_month.mean()), 4) if del_month.mean() > 0 else 0
                # Percentiles use q0, q10, etc. not delivery_q0
                for p in PERCENTILES:
                    row[f'q{p}'] = round(float(np.percentile(del_month, p)), 2)
                # Exceedance percentiles
                for p in EXCEEDANCE_PERCENTILES:
                    row[f'exc_p{p}'] = round(float(np.percentile(del_month, p)), 2)
            
            delivery_monthly_rows.append(row)
        
        # Calculate period summary
        water_years = sorted(output_df['WaterYear'].unique())
        
        # Annual delivery (convert CFS to TAF: CFS * days * 0.001984)
        annual_delivery = output_df.groupby('WaterYear').apply(
            lambda g: delivery[g.index].sum(), include_groups=False
        )
        
        # Annual demand
        if demand.notna().any():
            annual_demand = demand_df.groupby('WaterYear').apply(
                lambda g: demand[g.index].sum(), include_groups=False
            )
        else:
            annual_demand = pd.Series([np.nan] * len(water_years), index=water_years)
        
        # Annual shortage
        if shortage.notna().any():
            annual_shortage = output_df.groupby('WaterYear').apply(
                lambda g: shortage[g.index].sum(), include_groups=False
            )
        else:
            annual_shortage = pd.Series([np.nan] * len(water_years), index=water_years)
        
        # Match existing table schema
        summary = {
            'scenario_short_code': scenario_id,
            'du_id': du_id,
            'simulation_start_year': int(water_years[0]),
            'simulation_end_year': int(water_years[-1]),
            'total_years': len(water_years),
        }
        
        # Annual delivery stats
        ad = annual_delivery.dropna()
        if not ad.empty:
            summary['annual_delivery_avg_taf'] = round(float(ad.mean()), 2)
            summary['annual_delivery_cv'] = round(float(ad.std() / ad.mean()), 4) if ad.mean() > 0 else 0
            for p in EXCEEDANCE_PERCENTILES:
                summary[f'delivery_exc_p{p}'] = round(float(np.percentile(ad, p)), 2)
        
        # Annual demand stats
        adm = annual_demand.dropna()
        if not adm.empty:
            summary['annual_demand_avg_taf'] = round(float(adm.mean()), 2)
        
        # Annual shortage stats
        ash = annual_shortage.dropna()
        if not ash.empty:
            summary['annual_shortage_avg_taf'] = round(float(ash.mean()), 2)
            shortage_years = (ash > 0).sum()
            summary['shortage_years_count'] = int(shortage_years)
            summary['shortage_frequency_pct'] = round(shortage_years / len(ash) * 100, 2)
            summary['reliability_pct'] = round(100 - summary['shortage_frequency_pct'], 2)
        
        # Percent demand met (annual)
        if not ad.empty and not adm.empty:
            # Align by water year
            common_years = ad.index.intersection(adm.index)
            if len(common_years) > 0:
                pct_met = (ad[common_years] / adm[common_years]) * 100
                pct_met = np.clip(pct_met, 0, 100)
                pct_met = pct_met.dropna()
                if len(pct_met) > 0:
                    summary['avg_pct_demand_met'] = round(float(pct_met.mean()), 2)
        
        period_summary_rows.append(summary)
    
    log.info(f"Processed {processed} DUs, skipped {skipped}")
    log.info(f"Generated {len(delivery_monthly_rows)} monthly rows, {len(period_summary_rows)} summary rows")
    
    return delivery_monthly_rows, period_summary_rows


# =============================================================================
# DATABASE INSERT
# =============================================================================

def save_to_database(
    conn,
    delivery_monthly_rows: List[Dict],
    period_summary_rows: List[Dict],
    scenario_id: str
):
    """Save results to database tables."""
    cur = conn.cursor()
    
    # Delete existing data for this scenario (uses scenario_short_code)
    cur.execute("DELETE FROM du_delivery_monthly WHERE scenario_short_code = %s", (scenario_id,))
    cur.execute("DELETE FROM du_period_summary WHERE scenario_short_code = %s", (scenario_id,))
    log.info(f"Cleared existing data for scenario {scenario_id}")
    
    # Helper to convert numpy types
    def convert_val(val):
        if val is None:
            return None
        if isinstance(val, (np.integer, np.int64, np.int32)):
            return int(val)
        if isinstance(val, (np.floating, np.float64, np.float32)):
            return float(val)
        return val
    
    # Insert delivery monthly
    if delivery_monthly_rows:
        # Columns matching existing schema
        monthly_cols = [
            'scenario_short_code', 'du_id', 'water_month',
            'delivery_avg_taf', 'delivery_cv',
            'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
            'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
            'sample_count'
        ]
        
        values = [
            tuple(convert_val(row.get(col)) for col in monthly_cols)
            for row in delivery_monthly_rows
        ]
        
        insert_sql = f"""
            INSERT INTO du_delivery_monthly ({', '.join(monthly_cols)})
            VALUES %s
            ON CONFLICT (scenario_short_code, du_id, water_month) 
            DO UPDATE SET
                delivery_avg_taf = EXCLUDED.delivery_avg_taf,
                delivery_cv = EXCLUDED.delivery_cv,
                q0 = EXCLUDED.q0, q10 = EXCLUDED.q10, q30 = EXCLUDED.q30,
                q50 = EXCLUDED.q50, q70 = EXCLUDED.q70, q90 = EXCLUDED.q90, q100 = EXCLUDED.q100,
                exc_p5 = EXCLUDED.exc_p5, exc_p10 = EXCLUDED.exc_p10, exc_p25 = EXCLUDED.exc_p25,
                exc_p50 = EXCLUDED.exc_p50, exc_p75 = EXCLUDED.exc_p75, exc_p90 = EXCLUDED.exc_p90,
                exc_p95 = EXCLUDED.exc_p95,
                sample_count = EXCLUDED.sample_count,
                updated_at = NOW()
        """
        execute_values(cur, insert_sql, values)
        log.info(f"Inserted {len(values)} delivery monthly rows")
    
    # Insert period summary
    if period_summary_rows:
        # Columns matching existing schema
        summary_cols = [
            'scenario_short_code', 'du_id',
            'simulation_start_year', 'simulation_end_year', 'total_years',
            'annual_delivery_avg_taf', 'annual_delivery_cv',
            'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25',
            'delivery_exc_p50', 'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
            'annual_shortage_avg_taf', 'shortage_years_count', 'shortage_frequency_pct',
            'reliability_pct', 'avg_pct_demand_met', 'annual_demand_avg_taf'
        ]
        
        values = [
            tuple(convert_val(row.get(col)) for col in summary_cols)
            for row in period_summary_rows
        ]
        
        insert_sql = f"""
            INSERT INTO du_period_summary ({', '.join(summary_cols)})
            VALUES %s
            ON CONFLICT (scenario_short_code, du_id)
            DO UPDATE SET
                simulation_start_year = EXCLUDED.simulation_start_year,
                simulation_end_year = EXCLUDED.simulation_end_year,
                total_years = EXCLUDED.total_years,
                annual_delivery_avg_taf = EXCLUDED.annual_delivery_avg_taf,
                annual_delivery_cv = EXCLUDED.annual_delivery_cv,
                delivery_exc_p5 = EXCLUDED.delivery_exc_p5,
                delivery_exc_p10 = EXCLUDED.delivery_exc_p10,
                delivery_exc_p25 = EXCLUDED.delivery_exc_p25,
                delivery_exc_p50 = EXCLUDED.delivery_exc_p50,
                delivery_exc_p75 = EXCLUDED.delivery_exc_p75,
                delivery_exc_p90 = EXCLUDED.delivery_exc_p90,
                delivery_exc_p95 = EXCLUDED.delivery_exc_p95,
                annual_shortage_avg_taf = EXCLUDED.annual_shortage_avg_taf,
                shortage_years_count = EXCLUDED.shortage_years_count,
                shortage_frequency_pct = EXCLUDED.shortage_frequency_pct,
                reliability_pct = EXCLUDED.reliability_pct,
                avg_pct_demand_met = EXCLUDED.avg_pct_demand_met,
                annual_demand_avg_taf = EXCLUDED.annual_demand_avg_taf,
                updated_at = NOW()
        """
        execute_values(cur, insert_sql, values)
        log.info(f"Inserted {len(values)} period summary rows")
    
    conn.commit()
    cur.close()


# =============================================================================
# MAIN
# =============================================================================

def process_scenario(
    scenario_id: str,
    conn,
    use_local: bool = False,
    output_csv_path: Optional[str] = None,
    demand_csv_path: Optional[str] = None,
    dry_run: bool = False
) -> Tuple[List[Dict], List[Dict]]:
    """Process a single scenario."""
    log.info(f"Processing scenario: {scenario_id}")
    
    # Load variable mappings from database
    mappings = get_variable_mappings(conn)
    delivery_arcs = get_delivery_arcs(conn)
    
    # Load scenario data
    output_df, demand_df = load_scenario_data(
        scenario_id,
        use_local=use_local,
        output_csv_path=output_csv_path,
        demand_csv_path=demand_csv_path
    )
    
    # Calculate statistics
    delivery_monthly, period_summary = calculate_du_statistics(
        output_df, demand_df, mappings, delivery_arcs, scenario_id
    )
    
    if not dry_run:
        save_to_database(conn, delivery_monthly, period_summary, scenario_id)
    
    return delivery_monthly, period_summary


def get_mock_mappings() -> Dict[str, Dict]:
    """Return mock variable mappings for testing without database."""
    # Sample mappings for testing
    return {
        '02_PU': {'delivery_variable': 'DL_02_PU', 'demand_variable': 'UD_02_PU', 'shortage_variable': 'SHRTG_02_PU', 'requires_sum': False},
        '26N_NU1': {'delivery_variable': 'DL_26N_NU1', 'demand_variable': 'UD_26N_NU1', 'shortage_variable': 'SHRTG_26N_NU1', 'requires_sum': False},
        'FRFLD': {'delivery_variable': 'D_WTPNBR_FRFLD', 'demand_variable': 'UD_FRFLD', 'shortage_variable': None, 'requires_sum': True},
        'SBA029': {'delivery_variable': 'D_SBA029_ACWD_PMI', 'demand_variable': 'DEM_D_SBA029_ACWD_PMI', 'shortage_variable': 'SHORT_D_SBA029_ACWD_PMI', 'requires_sum': False},
        'MWD': {'delivery_variable': 'DEL_SWP_MWD', 'demand_variable': 'TABLEA_CONTRACT_MWD', 'shortage_variable': 'SHORT_SWP_MWD', 'requires_sum': False},
    }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate urban demand unit statistics (v2 with demand data)'
    )
    parser.add_argument('--scenario', '-s', help='Scenario ID (e.g., s0020)')
    parser.add_argument('--all-scenarios', action='store_true', help='Process all scenarios')
    parser.add_argument('--local', action='store_true', help='Use local files instead of S3')
    parser.add_argument('--output-csv', help='Override main output CSV path')
    parser.add_argument('--demand-csv', help='Override demand CSV path')
    parser.add_argument('--dry-run', action='store_true', help='Calculate but do not save')
    parser.add_argument('--output-json', action='store_true', help='Output results as JSON')
    parser.add_argument('--mock-mappings', action='store_true', help='Use mock mappings for testing (no database required)')
    
    args = parser.parse_args()
    
    if not args.scenario and not args.all_scenarios:
        parser.error("Either --scenario or --all-scenarios required")
    
    scenarios = SCENARIOS if args.all_scenarios else [args.scenario]
    
    all_monthly = []
    all_summary = []
    
    if args.mock_mappings:
        # Use mock mappings for testing
        log.info("Using mock variable mappings (no database)")
        mappings = get_mock_mappings()
        delivery_arcs = {
            'FRFLD': ['D_WTPNBR_FRFLD', 'D_WTPWMN_FRFLD'],
        }
        
        for scenario_id in scenarios:
            try:
                # Load data
                output_df, demand_df = load_scenario_data(
                    scenario_id,
                    use_local=args.local,
                    output_csv_path=args.output_csv,
                    demand_csv_path=args.demand_csv
                )
                
                # Calculate statistics
                monthly, summary = calculate_du_statistics(
                    output_df, demand_df, mappings, delivery_arcs, scenario_id
                )
                
                all_monthly.extend(monthly)
                all_summary.extend(summary)
                
            except Exception as e:
                log.error(f"Error processing {scenario_id}: {e}")
                import traceback
                traceback.print_exc()
                if not args.all_scenarios:
                    raise
    else:
        # Connect to database
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL environment variable required (or use --mock-mappings for testing)")
        
        if not HAS_PSYCOPG2:
            raise ImportError("psycopg2 required. Install with: pip install psycopg2-binary")
        
        conn = psycopg2.connect(database_url)
        
        for scenario_id in scenarios:
            try:
                monthly, summary = process_scenario(
                    scenario_id,
                    conn,
                    use_local=args.local,
                    output_csv_path=args.output_csv,
                    demand_csv_path=args.demand_csv,
                    dry_run=args.dry_run
                )
                all_monthly.extend(monthly)
                all_summary.extend(summary)
            except Exception as e:
                log.error(f"Error processing {scenario_id}: {e}")
                if not args.all_scenarios:
                    raise
        
        conn.close()
    
    if args.output_json:
        output = {
            'delivery_monthly': all_monthly,
            'period_summary': all_summary,
        }
        print(json.dumps(output, indent=2, default=str))
    
    log.info(f"Complete. Total: {len(all_monthly)} monthly, {len(all_summary)} summary rows")


if __name__ == '__main__':
    main()
