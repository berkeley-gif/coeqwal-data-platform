#!/usr/bin/env python3
"""
Calculate delivery and shortage statistics for urban demand units.

This module processes the 71 canonical CWS demand units using:
- Variable mappings from du_urban_variable table
- Multi-arc delivery from du_urban_delivery_arc table
- Group memberships from du_urban_group_member for categorization
- Entity attributes from du_urban_entity for metadata

Extraction categories (from du_urban_group):
- var_wba: WBA-style units with DL_* delivery (40 units)
- var_gw_only: Groundwater-only units with GP_* (3 units)
- var_swp_contractor: SWP contractor units with D_*_PMI (11 units)
- var_named_locality: Named localities with D_* arcs (15 units)
- var_missing: No CalSim variables found (2 units)

Usage:
    python calculate_du_statistics.py --scenario s0020
    python calculate_du_statistics.py --scenario s0020 --csv-path /path/to/calsim_output.csv
    python calculate_du_statistics.py --all-scenarios
    python calculate_du_statistics.py --scenario s0020 --group var_wba  # Process only WBA units
"""

import argparse
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Optional: boto3 for S3 access
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

# Optional: psycopg2 for database access
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
log = logging.getLogger("du_statistics")

# Known scenarios
SCENARIOS = ['s0011', 's0020', 's0021', 's0023', 's0024', 's0025', 's0027', 's0029']

# S3 bucket configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'coeqwal-model-run')

# Percentiles for statistics
DELIVERY_PERCENTILES = [0, 10, 30, 50, 70, 90, 100]
EXCEEDANCE_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# Minimum threshold for counting a year as having a "shortage" (in TAF)
# This filters out floating-point precision artifacts from CalSim's linear programming solver.
SHORTAGE_THRESHOLD_TAF = 0.1


# =============================================================================
# DATABASE FUNCTIONS - Load variable mappings and entity data
# =============================================================================

def get_db_connection():
    """Get database connection from DATABASE_URL."""
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 is required for database access")
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    return psycopg2.connect(database_url)


def load_du_variable_mappings(conn, group_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load demand unit variable mappings from database.
    
    Joins du_urban_variable with du_urban_entity for attributes,
    and includes group memberships for categorization.
    
    Args:
        conn: Database connection
        group_filter: Optional group short_code to filter (e.g., 'var_wba')
    
    Returns:
        List of dicts with du_id, variables, attributes, and groups
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Base query joining variable mappings with entity attributes
    query = """
        SELECT 
            v.du_id,
            v.delivery_variable,
            v.shortage_variable,
            v.variable_type,
            v.requires_sum,
            v.notes as variable_notes,
            -- Entity attributes
            e.wba_id,
            e.hydrologic_region,
            e.cs3_type,
            e.community_agency,
            e.total_acres,
            e.gw,
            e.sw,
            e.primary_contractor_short_code,
            -- Group memberships (aggregated)
            (
                SELECT array_agg(g.short_code ORDER BY g.id)
                FROM du_urban_group_member gm
                JOIN du_urban_group g ON gm.du_urban_group_id = g.id
                WHERE gm.du_id = v.du_id
            ) as groups
        FROM du_urban_variable v
        LEFT JOIN du_urban_entity e ON v.du_id = e.du_id
        WHERE v.is_active = TRUE
    """
    
    # Add group filter if specified
    if group_filter:
        query += """
            AND EXISTS (
                SELECT 1 FROM du_urban_group_member gm
                JOIN du_urban_group g ON gm.du_urban_group_id = g.id
                WHERE gm.du_id = v.du_id AND g.short_code = %s
            )
        """
        cur.execute(query + " ORDER BY v.du_id", (group_filter,))
    else:
        cur.execute(query + " ORDER BY v.du_id")
    
    results = [dict(row) for row in cur.fetchall()]
    cur.close()
    
    log.info(f"Loaded {len(results)} demand unit variable mappings" + 
             (f" (filtered by {group_filter})" if group_filter else ""))
    
    return results


def load_du_delivery_arcs(conn) -> Dict[str, List[str]]:
    """
    Load multi-arc delivery mappings for units requiring summation.
    
    Returns:
        Dict mapping du_id to list of delivery arc variable names
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT du_id, array_agg(delivery_arc ORDER BY arc_order) as arcs
        FROM du_urban_delivery_arc
        WHERE is_active = TRUE
        GROUP BY du_id
    """)
    
    results = {row['du_id']: row['arcs'] for row in cur.fetchall()}
    cur.close()
    
    log.info(f"Loaded delivery arcs for {len(results)} multi-arc units")
    
    return results


def load_group_summary(conn) -> Dict[str, Dict[str, Any]]:
    """
    Load summary of all groups with member counts.
    
    Returns:
        Dict mapping group short_code to {label, member_count, du_ids}
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT 
            g.short_code,
            g.label,
            g.description,
            COUNT(gm.id) as member_count,
            array_agg(gm.du_id ORDER BY gm.display_order) as du_ids
        FROM du_urban_group g
        LEFT JOIN du_urban_group_member gm ON g.id = gm.du_urban_group_id
        GROUP BY g.id, g.short_code, g.label, g.description
        ORDER BY g.display_order
    """)
    
    results = {row['short_code']: dict(row) for row in cur.fetchall()}
    cur.close()
    
    return results


# =============================================================================
# CALSIM DATA LOADING
# =============================================================================

def load_calsim_csv_from_s3(scenario_id: str) -> pd.DataFrame:
    """Load CalSim output CSV from S3 bucket."""
    if not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 access. Install with: pip install boto3")

    s3 = boto3.client('s3')

    possible_keys = [
        f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv",
        f"scenario/{scenario_id}/csv/{scenario_id}_DV.csv",
    ]

    for key in possible_keys:
        try:
            log.info(f"Trying S3 key: s3://{S3_BUCKET}/{key}")
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_csv(response['Body'], header=None, nrows=8)

            col_names = df.iloc[1].tolist()

            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data_df = pd.read_csv(response['Body'], header=None, skiprows=7, low_memory=False)
            data_df.columns = col_names

            log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
            return data_df

        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            log.warning(f"Error loading {key}: {e}")
            continue

    raise FileNotFoundError(f"Could not find CalSim output for {scenario_id} in S3")


def load_calsim_csv_from_file(file_path: str) -> pd.DataFrame:
    """Load CalSim output CSV from local file."""
    log.info(f"Loading from file: {file_path}")

    header_df = pd.read_csv(file_path, header=None, nrows=8)
    col_names = header_df.iloc[1].tolist()

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    data_df.columns = col_names

    log.info(f"Loaded: {data_df.shape[0]} rows, {data_df.shape[1]} columns")
    return data_df


def add_water_year_month(df: pd.DataFrame) -> pd.DataFrame:
    """Add water year and water month columns."""
    df = df.copy()

    first_col = df.columns[0]
    date_values = df[first_col]

    try:
        df['DateTime'] = pd.to_datetime(date_values, errors='coerce')

        if df['DateTime'].notna().sum() > 0:
            df['CalendarMonth'] = df['DateTime'].dt.month
            df['CalendarYear'] = df['DateTime'].dt.year
            df['WaterMonth'] = ((df['CalendarMonth'] - 10) % 12) + 1
            df['WaterYear'] = df['CalendarYear']
            df.loc[df['CalendarMonth'] >= 10, 'WaterYear'] += 1

            log.info(f"Detected monthly data: {df['DateTime'].min()} to {df['DateTime'].max()}")
            return df
    except Exception as e:
        log.debug(f"Could not parse as datetime: {e}")

    date_numeric = pd.to_numeric(date_values, errors='coerce')
    if date_numeric.notna().all() and (date_numeric >= 1900).all() and (date_numeric <= 2100).all():
        df['WaterYear'] = date_numeric.astype(int)
        df['WaterMonth'] = 0
        log.info(f"Detected annual data: years {df['WaterYear'].min()}-{df['WaterYear'].max()}")
        return df

    raise ValueError(f"Could not parse date column '{first_col}'")


# =============================================================================
# STATISTICS CALCULATION
# =============================================================================

def extract_delivery_data(
    df: pd.DataFrame,
    du_mapping: Dict[str, Any],
    delivery_arcs: Dict[str, List[str]]
) -> pd.Series:
    """
    Extract delivery data for a demand unit based on its variable mapping.
    
    Handles:
    - Direct variable lookup (DL_*, D_*_PMI, GP_*, etc.)
    - Multi-arc summation for units with requires_sum=True
    """
    du_id = du_mapping['du_id']
    delivery_var = du_mapping['delivery_variable']
    requires_sum = du_mapping['requires_sum']
    
    # Skip units with no variable
    if delivery_var == 'NOT_FOUND':
        return pd.Series(dtype=float)
    
    # Multi-arc summation
    if requires_sum and du_id in delivery_arcs:
        arcs = delivery_arcs[du_id]
        available_arcs = [arc for arc in arcs if arc in df.columns]
        
        if not available_arcs:
            log.debug(f"{du_id}: No delivery arcs found in data")
            return pd.Series(dtype=float)
        
        if len(available_arcs) < len(arcs):
            log.warning(f"{du_id}: Only {len(available_arcs)}/{len(arcs)} arcs found")
        
        return df[available_arcs].sum(axis=1)
    
    # Direct variable lookup
    if delivery_var in df.columns:
        result = df[delivery_var]
        # Handle duplicate column names (returns DataFrame instead of Series)
        if isinstance(result, pd.DataFrame):
            log.warning(f"{du_id}: Duplicate columns for '{delivery_var}', using first")
            result = result.iloc[:, 0]
        return result
    
    log.debug(f"{du_id}: Delivery variable '{delivery_var}' not in data")
    return pd.Series(dtype=float)


def extract_shortage_data(
    df: pd.DataFrame,
    du_mapping: Dict[str, Any]
) -> pd.Series:
    """Extract shortage data for a demand unit."""
    du_id = du_mapping['du_id']
    shortage_var = du_mapping['shortage_variable']
    
    if not shortage_var:
        return pd.Series(dtype=float)
    
    if shortage_var in df.columns:
        result = df[shortage_var]
        # Handle duplicate column names (returns DataFrame instead of Series)
        if isinstance(result, pd.DataFrame):
            log.warning(f"{du_id}: Duplicate columns for '{shortage_var}', using first")
            result = result.iloc[:, 0]
        return result
    
    log.debug(f"{du_id}: Shortage variable '{shortage_var}' not in data")
    return pd.Series(dtype=float)


def calculate_delivery_monthly(
    df: pd.DataFrame,
    du_mapping: Dict[str, Any],
    delivery_arcs: Dict[str, List[str]]
) -> List[Dict[str, Any]]:
    """Calculate monthly delivery statistics for a demand unit."""
    du_id = du_mapping['du_id']
    
    delivery_data = extract_delivery_data(df, du_mapping, delivery_arcs)
    if delivery_data.empty:
        return []
    
    df_work = df.copy()
    df_work['total_delivery'] = delivery_data
    
    results = []
    is_annual = (df_work['WaterMonth'] == 0).all()
    
    if is_annual:
        data = df_work['total_delivery'].dropna()
        if data.empty:
            return []
        
        row = {
            'du_id': du_id,
            'water_month': 0,
            'delivery_avg_taf': round(float(data.mean()), 2),
            'delivery_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'sample_count': len(data),
        }
        
        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)
        
        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            row[f'exc_p{p}'] = round(float(np.percentile(data, 100 - p)), 2)
        
        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_work[df_work['WaterMonth'] == wm]['total_delivery'].dropna()
            if month_data.empty:
                continue
            
            row = {
                'du_id': du_id,
                'water_month': wm,
                'delivery_avg_taf': round(float(month_data.mean()), 2),
                'delivery_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'sample_count': len(month_data),
            }
            
            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)
            
            # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
            for p in EXCEEDANCE_PERCENTILES:
                row[f'exc_p{p}'] = round(float(np.percentile(month_data, 100 - p)), 2)
            
            results.append(row)
    
    return results


def calculate_shortage_monthly(
    df: pd.DataFrame,
    du_mapping: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Calculate monthly shortage statistics for a demand unit."""
    du_id = du_mapping['du_id']
    
    shortage_data = extract_shortage_data(df, du_mapping)
    if shortage_data.empty:
        return []
    
    df_work = df.copy()
    df_work['total_shortage'] = shortage_data
    
    results = []
    is_annual = (df_work['WaterMonth'] == 0).all()
    
    if is_annual:
        data = df_work['total_shortage'].dropna()
        if data.empty:
            return []
        
        shortage_count = (data > SHORTAGE_THRESHOLD_TAF).sum()
        
        row = {
            'du_id': du_id,
            'water_month': 0,
            'shortage_avg_taf': round(float(data.mean()), 2),
            'shortage_cv': round(float(data.std() / data.mean()), 4) if data.mean() > 0 else 0,
            'shortage_frequency_pct': round((shortage_count / len(data)) * 100, 2),
            'sample_count': len(data),
        }
        
        for p in DELIVERY_PERCENTILES:
            row[f'q{p}'] = round(float(np.percentile(data, p)), 2)
        
        results.append(row)
    else:
        for wm in range(1, 13):
            month_data = df_work[df_work['WaterMonth'] == wm]['total_shortage'].dropna()
            if month_data.empty:
                continue
            
            shortage_count = (month_data > SHORTAGE_THRESHOLD_TAF).sum()
            
            row = {
                'du_id': du_id,
                'water_month': wm,
                'shortage_avg_taf': round(float(month_data.mean()), 2),
                'shortage_cv': round(float(month_data.std() / month_data.mean()), 4) if month_data.mean() > 0 else 0,
                'shortage_frequency_pct': round((shortage_count / len(month_data)) * 100, 2),
                'sample_count': len(month_data),
            }
            
            for p in DELIVERY_PERCENTILES:
                row[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)
            
            results.append(row)
    
    return results


def calculate_period_summary(
    df: pd.DataFrame,
    du_mapping: Dict[str, Any],
    delivery_arcs: Dict[str, List[str]]
) -> Optional[Dict[str, Any]]:
    """Calculate period-of-record summary for a demand unit."""
    du_id = du_mapping['du_id']
    
    delivery_data = extract_delivery_data(df, du_mapping, delivery_arcs)
    if delivery_data.empty:
        return None
    
    df_work = df.copy()
    df_work['total_delivery'] = delivery_data
    
    water_years = sorted(df_work['WaterYear'].unique())
    
    result = {
        'du_id': du_id,
        'simulation_start_year': int(water_years[0]),
        'simulation_end_year': int(water_years[-1]),
        'total_years': len(water_years),
    }
    
    # Annual delivery statistics
    annual_delivery = df_work.groupby('WaterYear')['total_delivery'].sum()
    result['annual_delivery_avg_taf'] = round(float(annual_delivery.mean()), 2)
    if annual_delivery.mean() > 0:
        result['annual_delivery_cv'] = round(float(annual_delivery.std() / annual_delivery.mean()), 4)
    else:
        result['annual_delivery_cv'] = 0
    
    # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
    for p in EXCEEDANCE_PERCENTILES:
        result[f'delivery_exc_p{p}'] = round(float(np.percentile(annual_delivery, 100 - p)), 2)
    
    # Shortage statistics
    shortage_data = extract_shortage_data(df, du_mapping)
    if not shortage_data.empty:
        df_work['total_shortage'] = shortage_data
        annual_shortage = df_work.groupby('WaterYear')['total_shortage'].sum()
        shortage_years = (annual_shortage > SHORTAGE_THRESHOLD_TAF).sum()
        
        result['annual_shortage_avg_taf'] = round(float(annual_shortage.mean()), 2)
        result['shortage_years_count'] = int(shortage_years)
        result['shortage_frequency_pct'] = round((shortage_years / len(water_years)) * 100, 2)
        
        # Exceedance percentiles: exc_pX = value exceeded X% of time = (100-X)th percentile
        for p in EXCEEDANCE_PERCENTILES:
            result[f'shortage_exc_p{p}'] = round(float(np.percentile(annual_shortage, 100 - p)), 2)
        
        # Reliability = 1 - (shortage / delivery)
        if result['annual_delivery_avg_taf'] > 0:
            result['reliability_pct'] = round(
                (1 - result['annual_shortage_avg_taf'] / result['annual_delivery_avg_taf']) * 100, 2
            )
        else:
            result['reliability_pct'] = None
    else:
        result['annual_shortage_avg_taf'] = None
        result['shortage_years_count'] = None
        result['shortage_frequency_pct'] = None
        result['reliability_pct'] = None
        for p in EXCEEDANCE_PERCENTILES:
            result[f'shortage_exc_p{p}'] = None
    
    return result


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def calculate_all_du_statistics(
    scenario_id: str,
    csv_path: Optional[str] = None,
    group_filter: Optional[str] = None
) -> Tuple[List[Dict], List[Dict], List[Dict], Dict[str, Any]]:
    """
    Calculate all statistics for urban demand units for a scenario.
    
    Args:
        scenario_id: Scenario short code (e.g., 's0020')
        csv_path: Optional local CSV path (otherwise loads from S3)
        group_filter: Optional group to filter (e.g., 'var_wba')
    
    Returns:
        Tuple of (delivery_monthly_rows, shortage_monthly_rows, period_summary_rows, processing_summary)
    """
    log.info(f"Processing scenario: {scenario_id}")
    
    # Get database connection and load mappings
    conn = get_db_connection()
    du_mappings = load_du_variable_mappings(conn, group_filter)
    delivery_arcs = load_du_delivery_arcs(conn)
    conn.close()
    
    # Load CalSim output
    if csv_path:
        df = load_calsim_csv_from_file(csv_path)
    else:
        df = load_calsim_csv_from_s3(scenario_id)
    
    # Add water year/month
    df = add_water_year_month(df)
    
    available_columns = set(df.columns)
    log.info(f"Available columns: {len(available_columns)}")
    
    delivery_monthly_rows = []
    shortage_monthly_rows = []
    period_summary_rows = []
    
    # Track processing by category
    category_stats = {
        'var_wba': {'processed': 0, 'with_delivery': 0, 'with_shortage': 0},
        'var_gw_only': {'processed': 0, 'with_delivery': 0, 'with_shortage': 0},
        'var_swp_contractor': {'processed': 0, 'with_delivery': 0, 'with_shortage': 0},
        'var_named_locality': {'processed': 0, 'with_delivery': 0, 'with_shortage': 0},
        'var_missing': {'processed': 0, 'with_delivery': 0, 'with_shortage': 0},
    }
    
    for mapping in du_mappings:
        du_id = mapping['du_id']
        groups = mapping.get('groups') or []
        
        # Identify the extraction category
        category = None
        for cat in ['var_wba', 'var_gw_only', 'var_swp_contractor', 'var_named_locality', 'var_missing']:
            if cat in groups:
                category = cat
                break
        
        if category:
            category_stats[category]['processed'] += 1
        
        # Skip missing variable units
        if mapping['delivery_variable'] == 'NOT_FOUND':
            log.debug(f"{du_id}: Skipping - no CalSim variable")
            continue
        
        # Calculate delivery monthly
        delivery_rows = calculate_delivery_monthly(df, mapping, delivery_arcs)
        if delivery_rows:
            for row in delivery_rows:
                row['scenario_short_code'] = scenario_id
            delivery_monthly_rows.extend(delivery_rows)
            if category:
                category_stats[category]['with_delivery'] += 1
        
        # Calculate shortage monthly
        shortage_rows = calculate_shortage_monthly(df, mapping)
        if shortage_rows:
            for row in shortage_rows:
                row['scenario_short_code'] = scenario_id
            shortage_monthly_rows.extend(shortage_rows)
            if category:
                category_stats[category]['with_shortage'] += 1
        
        # Calculate period summary
        summary = calculate_period_summary(df, mapping, delivery_arcs)
        if summary:
            summary['scenario_short_code'] = scenario_id
            period_summary_rows.append(summary)
    
    # Build processing summary
    processing_summary = {
        'scenario_id': scenario_id,
        'total_mappings': len(du_mappings),
        'delivery_monthly_rows': len(delivery_monthly_rows),
        'shortage_monthly_rows': len(shortage_monthly_rows),
        'period_summary_rows': len(period_summary_rows),
        'category_stats': category_stats,
    }
    
    log.info(f"Processed {len(du_mappings)} demand units")
    for cat, stats in category_stats.items():
        if stats['processed'] > 0:
            log.info(f"  {cat}: {stats['processed']} units, "
                    f"{stats['with_delivery']} with delivery, "
                    f"{stats['with_shortage']} with shortage")
    
    return delivery_monthly_rows, shortage_monthly_rows, period_summary_rows, processing_summary


def save_to_database(
    delivery_monthly_rows: List[Dict],
    shortage_monthly_rows: List[Dict],
    period_summary_rows: List[Dict],
    scenario_ids: List[str]
):
    """Save calculated statistics to database."""
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 required for database save")
    
    def convert_numpy(val):
        """Convert numpy types to Python native types."""
        if val is None:
            return None
        if isinstance(val, (np.integer, np.int64, np.int32)):
            return int(val)
        if isinstance(val, (np.floating, np.float64, np.float32)):
            return float(val)
        return val
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Delete existing data for these scenarios
        for scenario_id in scenario_ids:
            cur.execute("DELETE FROM du_delivery_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM du_shortage_monthly WHERE scenario_short_code = %s", (scenario_id,))
            cur.execute("DELETE FROM du_period_summary WHERE scenario_short_code = %s", (scenario_id,))
            log.info(f"Cleared existing data for scenario {scenario_id}")
        
        # Insert delivery monthly rows
        if delivery_monthly_rows:
            monthly_cols = [
                'scenario_short_code', 'du_id', 'water_month',
                'delivery_avg_taf', 'delivery_cv',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'exc_p5', 'exc_p10', 'exc_p25', 'exc_p50', 'exc_p75', 'exc_p90', 'exc_p95',
                'sample_count'
            ]
            monthly_values = [
                tuple(convert_numpy(row.get(col)) for col in monthly_cols)
                for row in delivery_monthly_rows
            ]
            insert_sql = f"""
                INSERT INTO du_delivery_monthly ({', '.join(monthly_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, monthly_values)
            log.info(f"Inserted {len(monthly_values)} delivery monthly rows")
        
        # Insert shortage monthly rows
        if shortage_monthly_rows:
            shortage_cols = [
                'scenario_short_code', 'du_id', 'water_month',
                'shortage_avg_taf', 'shortage_cv', 'shortage_frequency_pct',
                'q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100',
                'sample_count'
            ]
            shortage_values = [
                tuple(convert_numpy(row.get(col)) for col in shortage_cols)
                for row in shortage_monthly_rows
            ]
            insert_sql = f"""
                INSERT INTO du_shortage_monthly ({', '.join(shortage_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, shortage_values)
            log.info(f"Inserted {len(shortage_values)} shortage monthly rows")
        
        # Insert period summary rows
        if period_summary_rows:
            summary_cols = [
                'scenario_short_code', 'du_id',
                'simulation_start_year', 'simulation_end_year', 'total_years',
                'annual_delivery_avg_taf', 'annual_delivery_cv',
                'delivery_exc_p5', 'delivery_exc_p10', 'delivery_exc_p25',
                'delivery_exc_p50', 'delivery_exc_p75', 'delivery_exc_p90', 'delivery_exc_p95',
                'annual_shortage_avg_taf', 'shortage_years_count', 'shortage_frequency_pct',
                'shortage_exc_p5', 'shortage_exc_p10', 'shortage_exc_p25',
                'shortage_exc_p50', 'shortage_exc_p75', 'shortage_exc_p90', 'shortage_exc_p95',
                'reliability_pct'
            ]
            summary_values = [
                tuple(convert_numpy(row.get(col)) for col in summary_cols)
                for row in period_summary_rows
            ]
            insert_sql = f"""
                INSERT INTO du_period_summary ({', '.join(summary_cols)})
                VALUES %s
            """
            execute_values(cur, insert_sql, summary_values)
            log.info(f"Inserted {len(summary_values)} period summary rows")
        
        conn.commit()
        log.info("Database save complete")
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate delivery and shortage statistics for urban demand units'
    )
    parser.add_argument(
        '--scenario', '-s',
        help='Scenario ID (e.g., s0020)'
    )
    parser.add_argument(
        '--all-scenarios',
        action='store_true',
        help='Process all known scenarios'
    )
    parser.add_argument(
        '--csv-path',
        help='Local CalSim output CSV file path (instead of S3)'
    )
    parser.add_argument(
        '--group', '-g',
        help='Filter to specific group (e.g., var_wba, var_gw_only)'
    )
    parser.add_argument(
        '--output-json',
        action='store_true',
        help='Output results as JSON'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Calculate but do not save to database'
    )
    parser.add_argument(
        '--list-groups',
        action='store_true',
        help='List available groups and exit'
    )
    
    args = parser.parse_args()
    
    # List groups mode
    if args.list_groups:
        conn = get_db_connection()
        groups = load_group_summary(conn)
        conn.close()
        
        print("\nAvailable groups:")
        print("-" * 60)
        for code, info in groups.items():
            print(f"  {code:20} {info['label']:25} ({info['member_count']} members)")
        return
    
    if not args.scenario and not args.all_scenarios:
        parser.error("Either --scenario or --all-scenarios is required")
    
    scenarios_to_process = SCENARIOS if args.all_scenarios else [args.scenario]
    
    all_delivery_monthly = []
    all_shortage_monthly = []
    all_period_summary = []
    all_summaries = []
    
    for scenario_id in scenarios_to_process:
        try:
            delivery_monthly, shortage_monthly, period_summary, summary = calculate_all_du_statistics(
                scenario_id,
                csv_path=args.csv_path,
                group_filter=args.group
            )
            
            all_delivery_monthly.extend(delivery_monthly)
            all_shortage_monthly.extend(shortage_monthly)
            all_period_summary.extend(period_summary)
            all_summaries.append(summary)
            
        except Exception as e:
            log.error(f"Error processing {scenario_id}: {e}")
            if not args.all_scenarios:
                raise
    
    if args.dry_run:
        log.info("Dry run complete. Statistics calculated but not saved.")
        log.info(f"Total: {len(all_delivery_monthly)} delivery monthly, "
                 f"{len(all_shortage_monthly)} shortage monthly, "
                 f"{len(all_period_summary)} period summary rows")
        return
    
    if args.output_json:
        output = {
            'delivery_monthly': all_delivery_monthly,
            'shortage_monthly': all_shortage_monthly,
            'period_summary': all_period_summary,
            'processing_summaries': all_summaries,
        }
        print(json.dumps(output, indent=2))
        return
    
    # Save to database
    scenario_ids = list(set(row['scenario_short_code'] for row in all_delivery_monthly))
    save_to_database(all_delivery_monthly, all_shortage_monthly, all_period_summary, scenario_ids)
    
    log.info("=" * 60)
    log.info("PROCESSING COMPLETE")
    log.info("=" * 60)
    log.info(f"Delivery monthly: {len(all_delivery_monthly)} rows")
    log.info(f"Shortage monthly: {len(all_shortage_monthly)} rows")
    log.info(f"Period summary: {len(all_period_summary)} rows")


if __name__ == '__main__':
    main()
