#!/usr/bin/env python3
"""
Cross-scenario sensitivity analysis.

Computes two types of sensitivity from pre-computed per-scenario statistics:

1. **Climate sensitivity** — Within each hydroclimate sibling group
   (scenarios sharing identical operations), measure how each metric
   changes from historical → cc50 → cc95.  Stored per
   (sibling_group, module, entity, metric, water_month).

2. **Operational sensitivity** — Within each hydroclimate level
   (e.g. all historical-hydrology scenarios), measure how each metric
   varies across the different operational configurations.  Stored per
   (hydroclimate_id, module, entity, metric, water_month).

Results are written to ``sensitivity_climate`` and
``sensitivity_operational`` (created by migration 54).

This script reads from the database (not S3) and must run AFTER all
per-scenario statistics modules have completed.

Usage:
    python calculate_sensitivity.py
    python calculate_sensitivity.py --dry-run
    python calculate_sensitivity.py --only reservoir,ag
"""

import argparse
import logging
import math
import os
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import psycopg2
    from psycopg2.extras import execute_values

    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


def _py_native(val):
    """Convert numpy scalars to Python native types for psycopg2."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    return val


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("sensitivity")

# ────────────────────────────────────────────────────────────────────────
# Data-source definitions
# ────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MetricSource:
    table: str
    module: str
    entity_col: str
    value_col: str
    metric_name: str
    unit: str
    is_period: bool  # True → water_month stored as 0


MONTHLY_SOURCES: List[MetricSource] = [
    # Reservoirs
    MetricSource(
        "reservoir_storage_monthly",
        "reservoir",
        "reservoir_entity_id",
        "storage_avg_taf",
        "storage_avg",
        "TAF",
        False,
    ),
    # AG
    MetricSource(
        "ag_du_demand_monthly",
        "ag",
        "du_id",
        "demand_avg_taf",
        "demand_avg",
        "TAF",
        False,
    ),
    MetricSource(
        "ag_du_sw_delivery_monthly",
        "ag",
        "du_id",
        "sw_delivery_avg_taf",
        "sw_delivery_avg",
        "TAF",
        False,
    ),
    MetricSource(
        "ag_du_gw_pumping_monthly",
        "ag",
        "du_id",
        "gw_pumping_avg_taf",
        "gw_pumping_avg",
        "TAF",
        False,
    ),
    MetricSource(
        "ag_du_shortage_monthly",
        "ag",
        "du_id",
        "shortage_avg_taf",
        "shortage_avg",
        "TAF",
        False,
    ),
    # DU Urban
    MetricSource(
        "du_delivery_monthly",
        "du_urban",
        "du_id",
        "delivery_avg_taf",
        "delivery_avg",
        "TAF",
        False,
    ),
    MetricSource(
        "du_shortage_monthly",
        "du_urban",
        "du_id",
        "shortage_avg_taf",
        "shortage_avg",
        "TAF",
        False,
    ),
    # Refuge
    MetricSource(
        "refuge_du_delivery_monthly",
        "refuge",
        "du_id",
        "delivery_avg_taf",
        "delivery_avg",
        "TAF",
        False,
    ),
    MetricSource(
        "refuge_du_shortage_monthly",
        "refuge",
        "du_id",
        "shortage_avg_taf",
        "shortage_avg",
        "TAF",
        False,
    ),
    # Env Flows
    MetricSource(
        "env_flow_channel_monthly",
        "env_flows",
        "network_arc_id",
        "flow_avg_cfs",
        "flow_avg_cfs",
        "CFS",
        False,
    ),
    MetricSource(
        "env_flow_channel_monthly",
        "env_flows",
        "network_arc_id",
        "flow_avg_taf",
        "flow_avg_taf",
        "TAF",
        False,
    ),
    MetricSource(
        "env_flow_channel_monthly",
        "env_flows",
        "network_arc_id",
        "pct_unimpaired_avg",
        "pct_unimpaired",
        "PCT",
        False,
    ),
    # MI
    MetricSource(
        "mi_delivery_monthly",
        "mi",
        "mi_contractor_code",
        "delivery_avg_taf",
        "delivery_avg",
        "TAF",
        False,
    ),
    MetricSource(
        "mi_shortage_monthly",
        "mi",
        "mi_contractor_code",
        "shortage_avg_taf",
        "shortage_avg",
        "TAF",
        False,
    ),
    # CWS Aggregate
    MetricSource(
        "cws_aggregate_monthly",
        "cws_aggregate",
        "cws_aggregate_id",
        "delivery_avg_taf",
        "delivery_avg",
        "TAF",
        False,
    ),
    MetricSource(
        "cws_aggregate_monthly",
        "cws_aggregate",
        "cws_aggregate_id",
        "shortage_avg_taf",
        "shortage_avg",
        "TAF",
        False,
    ),
]

PERIOD_SOURCES: List[MetricSource] = [
    # AG period
    MetricSource(
        "ag_du_period_summary",
        "ag",
        "du_id",
        "annual_demand_avg_taf",
        "annual_demand_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "ag_du_period_summary",
        "ag",
        "du_id",
        "annual_sw_delivery_avg_taf",
        "annual_sw_delivery_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "ag_du_period_summary",
        "ag",
        "du_id",
        "annual_shortage_avg_taf",
        "annual_shortage_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "ag_du_period_summary",
        "ag",
        "du_id",
        "reliability_pct",
        "reliability",
        "PCT",
        True,
    ),
    # DU Urban period
    MetricSource(
        "du_period_summary",
        "du_urban",
        "du_id",
        "annual_delivery_avg_taf",
        "annual_delivery_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "du_period_summary",
        "du_urban",
        "du_id",
        "annual_shortage_avg_taf",
        "annual_shortage_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "du_period_summary",
        "du_urban",
        "du_id",
        "annual_demand_avg_taf",
        "annual_demand_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "du_period_summary",
        "du_urban",
        "du_id",
        "reliability_pct",
        "reliability",
        "PCT",
        True,
    ),
    # Refuge period
    MetricSource(
        "refuge_du_period_summary",
        "refuge",
        "du_id",
        "annual_delivery_avg_taf",
        "annual_delivery_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "refuge_du_period_summary",
        "refuge",
        "du_id",
        "annual_shortage_avg_taf",
        "annual_shortage_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "refuge_du_period_summary",
        "refuge",
        "du_id",
        "reliability_pct_95",
        "reliability",
        "PCT",
        True,
    ),
    # Env Flows period
    MetricSource(
        "env_flow_channel_period_summary",
        "env_flows",
        "network_arc_id",
        "avg_pct_unimpaired",
        "annual_pct_unimpaired",
        "PCT",
        True,
    ),
    MetricSource(
        "env_flow_channel_period_summary",
        "env_flows",
        "network_arc_id",
        "avg_pct_ff",
        "annual_pct_ff",
        "PCT",
        True,
    ),
    # MI period
    MetricSource(
        "mi_contractor_period_summary",
        "mi",
        "mi_contractor_code",
        "annual_delivery_avg_taf",
        "annual_delivery_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "mi_contractor_period_summary",
        "mi",
        "mi_contractor_code",
        "annual_shortage_avg_taf",
        "annual_shortage_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "mi_contractor_period_summary",
        "mi",
        "mi_contractor_code",
        "annual_demand_avg_taf",
        "annual_demand_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "mi_contractor_period_summary",
        "mi",
        "mi_contractor_code",
        "reliability_pct",
        "reliability",
        "PCT",
        True,
    ),
    # CWS Aggregate period
    MetricSource(
        "cws_aggregate_period_summary",
        "cws_aggregate",
        "cws_aggregate_id",
        "annual_delivery_avg_taf",
        "annual_delivery_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "cws_aggregate_period_summary",
        "cws_aggregate",
        "cws_aggregate_id",
        "annual_shortage_avg_taf",
        "annual_shortage_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "cws_aggregate_period_summary",
        "cws_aggregate",
        "cws_aggregate_id",
        "annual_demand_avg_taf",
        "annual_demand_avg",
        "TAF",
        True,
    ),
    MetricSource(
        "cws_aggregate_period_summary",
        "cws_aggregate",
        "cws_aggregate_id",
        "reliability_pct",
        "reliability",
        "PCT",
        True,
    ),
]

ALL_MODULES = sorted({s.module for s in MONTHLY_SOURCES + PERIOD_SOURCES})

# ────────────────────────────────────────────────────────────────────────
# Helper: safe percent change
# ────────────────────────────────────────────────────────────────────────


def _pct_change(new_val, ref_val):
    """Percent change from ref_val to new_val.  Returns None when undefined."""
    if ref_val is None or new_val is None:
        return None
    if isinstance(ref_val, float) and (math.isnan(ref_val) or math.isnan(new_val)):
        return None
    if abs(ref_val) < 1e-12:
        return None
    return (new_val - ref_val) / abs(ref_val) * 100.0


# ────────────────────────────────────────────────────────────────────────
# Core: load scenario metadata
# ────────────────────────────────────────────────────────────────────────


def load_scenario_metadata(conn) -> pd.DataFrame:
    """Return DataFrame with scenario → hydroclimate mapping."""
    query = """
        SELECT s.short_code,
               s.hydroclimate_id,
               s.hydroclimate_sibling,
               h.short_code AS hydroclimate_code
        FROM scenario s
        JOIN hydroclimate h ON s.hydroclimate_id = h.id
        WHERE s.is_active = TRUE
          AND s.hydroclimate_id IS NOT NULL
          AND s.hydroclimate_sibling IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    log.info(
        f"Loaded metadata for {len(df)} scenarios across "
        f"{df['hydroclimate_id'].nunique()} hydroclimate levels, "
        f"{df['hydroclimate_sibling'].nunique()} sibling groups"
    )
    return df


def identify_reference_hydroclimate(meta: pd.DataFrame) -> int:
    """Find the 'historical' hydroclimate_id (baseline for % change).

    The sibling group leader's short_code == hydroclimate_sibling.
    That leader is always the historical-hydrology scenario.
    """
    leaders = meta[meta["short_code"] == meta["hydroclimate_sibling"]]
    if leaders.empty:
        ref_id = int(meta["hydroclimate_id"].min())
        log.warning(f"No sibling-group leaders found; using hydroclimate_id={ref_id}")
        return ref_id
    ref_id = int(leaders["hydroclimate_id"].mode().iloc[0])
    log.info(f"Reference (historical) hydroclimate_id = {ref_id}")
    return ref_id


# ────────────────────────────────────────────────────────────────────────
# Core: pull metric values for all scenarios
# ────────────────────────────────────────────────────────────────────────


def fetch_metric_data(conn, src: MetricSource) -> Optional[pd.DataFrame]:
    """Query a single metric from a statistics table.

    Returns a DataFrame with columns:
        scenario_short_code, entity_id, water_month, value
    or None if the table/column does not exist.
    """
    month_expr = "water_month" if not src.is_period else "0 AS water_month"
    query = f"""
        SELECT scenario_short_code,
               {src.entity_col}::TEXT AS entity_id,
               {month_expr},
               {src.value_col} AS value
        FROM {src.table}
    """
    try:
        df = pd.read_sql(query, conn)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df
    except Exception as e:
        log.warning(f"Skipping {src.table}.{src.value_col}: {e}")
        conn.rollback()
        return None


def fetch_delta_monthly(conn) -> Optional[pd.DataFrame]:
    """Delta monthly is structured differently: variable_code is the entity,
    and the unit column varies per row.

    Returns list of (DataFrame, metric_name, unit) per variable.
    """
    query = """
        SELECT scenario_short_code,
               variable_code AS entity_id,
               water_month,
               avg AS value,
               unit
        FROM delta_monthly
    """
    try:
        df = pd.read_sql(query, conn)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df
    except Exception as e:
        log.warning(f"Skipping delta_monthly: {e}")
        conn.rollback()
        return None


# ────────────────────────────────────────────────────────────────────────
# Core: compute climate sensitivity
# ────────────────────────────────────────────────────────────────────────


def compute_climate_sensitivity(
    data: pd.DataFrame,
    meta: pd.DataFrame,
    ref_hydro_id: int,
    module: str,
    metric_name: str,
    unit: str,
) -> List[Tuple]:
    """Compute climate sensitivity rows for one metric.

    For each sibling group, find the historical/cc50/cc95 values and
    compute absolute + percent change.
    """
    merged = data.merge(
        meta[["short_code", "hydroclimate_id", "hydroclimate_sibling"]],
        left_on="scenario_short_code",
        right_on="short_code",
        how="inner",
    )
    if merged.empty:
        return []

    hydro_ids = sorted(merged["hydroclimate_id"].unique())
    non_ref_ids = [h for h in hydro_ids if h != ref_hydro_id]

    cc50_id = None
    cc95_id = None
    for h in non_ref_ids:
        codes = merged.loc[merged["hydroclimate_id"] == h, "short_code"].str.lower()
        sample = codes.iloc[0] if len(codes) > 0 else ""
        if "cc50" in sample or "cc50" in str(h):
            cc50_id = h
        elif "cc95" in sample or "cc95" in str(h):
            cc95_id = h

    if cc50_id is None and len(non_ref_ids) >= 1:
        cc50_id = non_ref_ids[0]
    if cc95_id is None and len(non_ref_ids) >= 2:
        cc95_id = non_ref_ids[1]

    rows = []
    group_cols = ["hydroclimate_sibling", "entity_id", "water_month"]
    for key, grp in merged.groupby(group_cols):
        sibling, entity, wm = key

        hist_rows = grp[grp["hydroclimate_id"] == ref_hydro_id]
        hist_val = hist_rows["value"].mean() if not hist_rows.empty else None

        cc50_val = None
        if cc50_id is not None:
            cc50_rows = grp[grp["hydroclimate_id"] == cc50_id]
            cc50_val = cc50_rows["value"].mean() if not cc50_rows.empty else None

        cc95_val = None
        if cc95_id is not None:
            cc95_rows = grp[grp["hydroclimate_id"] == cc95_id]
            cc95_val = cc95_rows["value"].mean() if not cc95_rows.empty else None

        cc50_abs = (
            (cc50_val - hist_val)
            if (cc50_val is not None and hist_val is not None)
            else None
        )
        cc95_abs = (
            (cc95_val - hist_val)
            if (cc95_val is not None and hist_val is not None)
            else None
        )
        cc50_pct = _pct_change(cc50_val, hist_val)
        cc95_pct = _pct_change(cc95_val, hist_val)

        rows.append(
            (
                sibling,
                module,
                str(entity),
                metric_name,
                int(wm),
                unit,
                _py_native(hist_val),
                _py_native(cc50_val),
                _py_native(cc95_val),
                _py_native(cc50_abs),
                _py_native(cc95_abs),
                _py_native(cc50_pct),
                _py_native(cc95_pct),
            )
        )

    return rows


# ────────────────────────────────────────────────────────────────────────
# Core: compute operational sensitivity
# ────────────────────────────────────────────────────────────────────────


def compute_operational_sensitivity(
    data: pd.DataFrame,
    meta: pd.DataFrame,
    module: str,
    metric_name: str,
    unit: str,
) -> List[Tuple]:
    """Compute operational sensitivity rows for one metric.

    For each hydroclimate level, compute spread statistics across all
    operational configurations.
    """
    merged = data.merge(
        meta[["short_code", "hydroclimate_id"]],
        left_on="scenario_short_code",
        right_on="short_code",
        how="inner",
    )
    if merged.empty:
        return []

    rows = []
    group_cols = ["hydroclimate_id", "entity_id", "water_month"]
    for key, grp in merged.groupby(group_cols):
        hydro_id, entity, wm = key
        vals = grp["value"].dropna()
        n = len(vals)
        if n < 2:
            continue

        mn = float(vals.min())
        mx = float(vals.max())
        mean = float(vals.mean())
        std = float(vals.std())
        rng = mx - mn
        pct_rng = (rng / abs(mean) * 100.0) if abs(mean) > 1e-12 else None

        rows.append(
            (
                int(hydro_id),
                module,
                str(entity),
                metric_name,
                int(wm),
                unit,
                n,
                mn,
                mx,
                mean,
                std,
                rng,
                pct_rng,
            )
        )

    return rows


# ────────────────────────────────────────────────────────────────────────
# Core: write results
# ────────────────────────────────────────────────────────────────────────

CLIMATE_INSERT = """
    INSERT INTO sensitivity_climate
        (sibling_group, module, entity_id, metric_name, water_month, unit,
         hist_value, cc50_value, cc95_value,
         cc50_abs_change, cc95_abs_change, cc50_pct_change, cc95_pct_change,
         updated_at)
    VALUES %s
    ON CONFLICT (sibling_group, module, entity_id, metric_name, water_month)
    DO UPDATE SET
        unit             = EXCLUDED.unit,
        hist_value       = EXCLUDED.hist_value,
        cc50_value       = EXCLUDED.cc50_value,
        cc95_value       = EXCLUDED.cc95_value,
        cc50_abs_change  = EXCLUDED.cc50_abs_change,
        cc95_abs_change  = EXCLUDED.cc95_abs_change,
        cc50_pct_change  = EXCLUDED.cc50_pct_change,
        cc95_pct_change  = EXCLUDED.cc95_pct_change,
        updated_at       = NOW()
"""

OPERATIONAL_INSERT = """
    INSERT INTO sensitivity_operational
        (hydroclimate_id, module, entity_id, metric_name, water_month, unit,
         scenario_count, min_value, max_value, mean_value, std_value,
         range_value, pct_range,
         updated_at)
    VALUES %s
    ON CONFLICT (hydroclimate_id, module, entity_id, metric_name, water_month)
    DO UPDATE SET
        unit            = EXCLUDED.unit,
        scenario_count  = EXCLUDED.scenario_count,
        min_value       = EXCLUDED.min_value,
        max_value       = EXCLUDED.max_value,
        mean_value      = EXCLUDED.mean_value,
        std_value       = EXCLUDED.std_value,
        range_value     = EXCLUDED.range_value,
        pct_range       = EXCLUDED.pct_range,
        updated_at      = NOW()
"""


def write_climate_rows(conn, rows: List[Tuple], dry_run: bool):
    if not rows:
        return
    if dry_run:
        log.info(f"  [dry-run] Would write {len(rows)} climate sensitivity rows")
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            CLIMATE_INSERT,
            rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
            page_size=2000,
        )
    conn.commit()
    log.info(f"  Wrote {len(rows)} climate sensitivity rows")


def write_operational_rows(conn, rows: List[Tuple], dry_run: bool):
    if not rows:
        return
    if dry_run:
        log.info(f"  [dry-run] Would write {len(rows)} operational sensitivity rows")
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            OPERATIONAL_INSERT,
            rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
            page_size=2000,
        )
    conn.commit()
    log.info(f"  Wrote {len(rows)} operational sensitivity rows")


# ────────────────────────────────────────────────────────────────────────
# Main orchestration
# ────────────────────────────────────────────────────────────────────────


def process_source(
    conn,
    src: MetricSource,
    meta: pd.DataFrame,
    ref_hydro_id: int,
    dry_run: bool,
) -> Tuple[int, int]:
    """Process one MetricSource.  Returns (climate_rows, ops_rows) counts."""
    data = fetch_metric_data(conn, src)
    if data is None or data.empty:
        return 0, 0

    log.info(f"  {src.table}.{src.value_col} → {len(data)} rows from DB")

    climate_rows = compute_climate_sensitivity(
        data, meta, ref_hydro_id, src.module, src.metric_name, src.unit
    )
    ops_rows = compute_operational_sensitivity(
        data, meta, src.module, src.metric_name, src.unit
    )

    write_climate_rows(conn, climate_rows, dry_run)
    write_operational_rows(conn, ops_rows, dry_run)

    return len(climate_rows), len(ops_rows)


def process_delta(
    conn,
    meta: pd.DataFrame,
    ref_hydro_id: int,
    dry_run: bool,
) -> Tuple[int, int]:
    """Process the delta_monthly table (variable_code × unit structure)."""
    df = fetch_delta_monthly(conn)
    if df is None or df.empty:
        return 0, 0

    total_climate = 0
    total_ops = 0

    for (var_code, unit_val), var_df in df.groupby(["entity_id", "unit"]):
        unit_str = str(unit_val).strip() if unit_val else "UNKNOWN"
        metric_name = f"avg_{var_code}"

        climate_rows = compute_climate_sensitivity(
            var_df, meta, ref_hydro_id, "delta", metric_name, unit_str
        )
        ops_rows = compute_operational_sensitivity(
            var_df, meta, "delta", metric_name, unit_str
        )

        write_climate_rows(conn, climate_rows, dry_run)
        write_operational_rows(conn, ops_rows, dry_run)

        total_climate += len(climate_rows)
        total_ops += len(ops_rows)

    return total_climate, total_ops


def run_sensitivity(
    db_url: str,
    modules: Optional[List[str]] = None,
    dry_run: bool = False,
):
    """Main entry point."""
    if not HAS_PSYCOPG2:
        log.error("psycopg2 is required.  pip install psycopg2-binary")
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    try:
        meta = load_scenario_metadata(conn)
        if meta.empty:
            log.error("No scenario metadata found. Are scenarios loaded in the DB?")
            return

        ref_hydro_id = identify_reference_hydroclimate(meta)

        total_climate = 0
        total_ops = 0

        # Standard monthly + period sources
        all_sources = MONTHLY_SOURCES + PERIOD_SOURCES
        if modules:
            all_sources = [s for s in all_sources if s.module in modules]

        for src in all_sources:
            log.info(
                f"Processing {src.module}/{src.metric_name} "
                f"({'period' if src.is_period else 'monthly'}) ..."
            )
            c, o = process_source(conn, src, meta, ref_hydro_id, dry_run)
            total_climate += c
            total_ops += o

        # Delta (special handling)
        if modules is None or "delta" in modules:
            log.info("Processing delta (variable_code × unit) ...")
            c, o = process_delta(conn, meta, ref_hydro_id, dry_run)
            total_climate += c
            total_ops += o

        log.info("=" * 60)
        log.info("SENSITIVITY ANALYSIS COMPLETE")
        log.info(f"  Climate sensitivity rows:     {total_climate:,}")
        log.info(f"  Operational sensitivity rows:  {total_ops:,}")
        log.info(f"  Dry run: {dry_run}")
        log.info("=" * 60)

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Cross-scenario sensitivity analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute but do not write to database",
    )
    parser.add_argument(
        "--only",
        help=f"Comma-separated modules to include. Available: {', '.join(ALL_MODULES)}",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection string (default: $DATABASE_URL)",
    )
    args = parser.parse_args()

    if not args.db_url and not args.dry_run:
        parser.error("DATABASE_URL not set and --dry-run not specified")

    modules = None
    if args.only:
        modules = [m.strip() for m in args.only.split(",")]
        invalid = [m for m in modules if m not in ALL_MODULES + ["delta"]]
        if invalid:
            parser.error(f"Unknown modules: {', '.join(invalid)}")

    run_sensitivity(
        db_url=args.db_url or "",
        modules=modules,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
