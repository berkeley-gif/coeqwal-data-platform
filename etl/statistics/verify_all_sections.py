#!/usr/bin/env python3
"""
verify_all_sections.py - Comprehensive data verification for the COEQWAL data explorer.

Computes expected values from CalSim DV and SV reference CSVs, queries the
database for actual ETL output, and produces a JSON verification report with
automated PASS/FAIL per metric.

Usage:
    python verify_all_sections.py --scenario s0020
    python verify_all_sections.py --scenario s0020 --csv-only
    python verify_all_sections.py --all-scenarios --report-dir audits/verification_reports

Requires:
    - Reference CSVs: DV (calsim output) and SV (sv input) in etl/reference/ or --ref-dir
    - DATABASE_URL env var for DB comparison (skip with --csv-only)
    - psycopg2 (pip install psycopg2-binary)

Data sources after ETL refactoring:
    - DV: delivery (DN_*, D_*), shortage (SHRTG_*, SHORT_*), AG demand (AW_*),
           AG GW pumping (GP_*), reservoir storage (S_*),
           env flows (C_*), CWS aggregates (DEL_*, SHORT_*),
           Delta outflow (NDO), X2 (X2_PRV_KM), salinity (EM/JP/RS/CO_EC_MONTH, *EC_MAX14DAY)
    - SV: urban demand (UD_*)
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Optional: only RealDictCursor is referenced directly. The connection itself
# is opened via etl.common.db.get_db_connection.
try:
    from psycopg2.extras import RealDictCursor
except ImportError:
    RealDictCursor = None  # type: ignore[assignment]

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Constants

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from units import CFS_TO_TAF_PER_DAY  # noqa: E402
from etl.common import get_db_connection  # noqa: E402
from etl.common.etl_scenarios import ETL_SCENARIOS as ALL_SCENARIOS  # noqa: E402

SHORTAGE_THRESHOLD_TAF = 0.1

ABS_TOL = 0.5
REL_TOL = 0.01

SCENARIO_RUN_IDS = {
    "s0020": "s0020_DCRadjBL_2020LU_wTUCP",
    "s0028": "s0028_CVgwLimit_SGMALU_wTUCP",
}

# ---------------------------------------------------------------------------
# Roadmap: the per-section variable lists below are hand-curated. The
# long-term goal is to derive them from `domain_family_map` or the seed
# CSVs (`channel_entity.csv`, `du_urban_entity.csv`,
# `du_agriculture_entity.csv`, `du_refuge_entity.csv`) so a new aggregate
# variable added to the ETL is automatically covered here instead of
# silently under-covered. Until then, treat this block as the verifier's
# scope contract: when the ETL adds or removes a variable, update the
# matching list here so the JSON report stays meaningful.
# ---------------------------------------------------------------------------

# Variable lists from COEQWAL_V3/notebooks/variable_groupings.csv
RESERVOIR_VARS = {
    "SHSTA": ("S_SHSTA", 4552.0),
    "OROVL": ("S_OROVL", 3424.8),
    "FOLSM": ("S_FOLSM", 967.0),
    "TRNTY": ("S_TRNTY", 2448.0),
    "MELON": ("S_MELON", 2420.0),
    "MLRTN": ("S_MLRTN", 524.0),
    "SLUIS_CVP": ("S_SLUIS_CVP", 966.0),
    "SLUIS_SWP": ("S_SLUIS_SWP", 1062.0),
}

FLOW_VARS = [
    "C_SAC041",
    "C_SAC085",
    "C_FTR003",
    "C_SAC257",
    "C_KSWCK",
    "C_AMR004",
    "C_SJR070",
    "C_STS017",
    "C_TUO003",
    "C_SJR115",
    "C_MCD005",
]

CWS_AGGREGATE_VARS = [
    ("DEL_SWP_PMI", "SWP M&I Total"),
    ("DEL_CVP_PMI_N", "CVP M&I NOD"),
    ("DEL_CVP_PMI_S", "CVP M&I SOD"),
    ("DEL_SWP_PMI_S", "SWP M&I SOD"),
]

CWS_SHORTAGE_VARS = [
    ("SHORT_SWP_TOTA", "SWP Shortage Total"),
    ("SHORT_CVP_TOT_N", "CVP Shortage NOD"),
    ("SHORT_CVP_TOT_S", "CVP Shortage SOD"),
]

AG_AGGREGATE_VARS = [
    ("DEL_SWP_PAG", "SWP AG Total"),
    ("DEL_SWP_PAG_N", "SWP AG NOD"),
    ("DEL_SWP_PAG_S", "SWP AG SOD"),
    ("DEL_CVP_PAG_N", "CVP AG NOD"),
    ("DEL_CVP_PAG_S", "CVP AG SOD"),
]

AG_AGGREGATE_NEW_VARS = [
    ("cvp_psc_n", "DEL_CVP_PSC_N", "CVP Settlement NOD"),
    ("cvp_pex_s", "DEL_CVP_PEX_S", "CVP Exchange SOD"),
]

AG_COMPUTED_AGGREGATES = {
    "nod_ag": {
        "delivery_components": ["DEL_CVP_PAG_N", "DEL_SWP_PAG_N", "DEL_CVP_PSC_N"],
        "shortage_components": [
            "SHORT_CVP_PAG_N",
            "SHORT_SWP_PAG_N",
            "SHORT_CVP_PSC_N",
        ],
    },
    "sod_ag": {
        "delivery_components": ["DEL_CVP_PAG_S", "DEL_SWP_PAG_S", "DEL_CVP_PEX_S"],
        "shortage_components": [
            "SHORT_CVP_PAG_S",
            "SHORT_SWP_PAG_S",
            "SHORT_CVP_PEX_S",
        ],
    },
}

DELTA_OUTFLOW_VARS = [("ndo", "NDO", "CFS")]
DELTA_X2_VARS = [("x2", "X2_PRV_KM", "KM")]
DELTA_SALINITY_VARS = [
    ("em_ec", "EM_EC_MONTH", "UMHOS/CM"),
    ("jp_ec", "JP_EC_MONTH", "UMHOS/CM"),
    ("rs_ec", "RS_EC_MONTH", "UMHOS/CM"),
    ("co_ec", "CO_EC_MONTH", "UMHOS/CM"),
    ("banks_ec", "BANKSEC_MAX14DAY", "UMHOS/CM"),
    ("tracy_ec", "TRACYEC_MAX14DAY", "UMHOS/CM"),
]

SAMPLE_CWS_DUS = ["02_PU", "26S_PU1", "71_PU1", "GDPUD_NU", "MWD", "CCWD"]

SAMPLE_AG_DUS = ["02_PA", "08N_PA", "61_PA1", "71_PA1", "02_NA", "64_PA1"]


# Data Classes───


@dataclass
class Check:
    metric: str
    section: str
    entity: str
    expected: Optional[float]
    actual: Optional[float] = None
    abs_tol: float = ABS_TOL
    rel_tol: float = REL_TOL

    @property
    def status(self) -> str:
        if self.expected is None:
            return "skip"
        if self.actual is None:
            return "no_db"
        if np.isnan(self.expected) and np.isnan(self.actual):
            return "pass"
        if np.isnan(self.expected) or np.isnan(self.actual):
            return "fail"
        if np.isclose(self.expected, self.actual, atol=self.abs_tol, rtol=self.rel_tol):
            return "pass"
        return "fail"


@dataclass
class Report:
    scenario_id: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    checks: List[Check] = field(default_factory=list)
    csv_files_used: Dict[str, str] = field(default_factory=dict)
    db_connected: bool = False

    @property
    def summary(self) -> Dict[str, int]:
        statuses = [c.status for c in self.checks]
        return {
            "total": len(statuses),
            "pass": statuses.count("pass"),
            "fail": statuses.count("fail"),
            "skip": statuses.count("skip"),
            "no_db": statuses.count("no_db"),
        }

    def add(
        self,
        metric: str,
        section: str,
        entity: str,
        expected: Optional[float],
        actual: Optional[float] = None,
        abs_tol: float = ABS_TOL,
        rel_tol: float = REL_TOL,
    ):
        self.checks.append(
            Check(
                metric=metric,
                section=section,
                entity=entity,
                expected=expected,
                actual=actual,
                abs_tol=abs_tol,
                rel_tol=rel_tol,
            )
        )

    def to_dict(self) -> dict:
        checks = []
        for c in self.checks:
            d = asdict(c)
            d["status"] = c.status
            checks.append(d)
        return {
            "scenario_id": self.scenario_id,
            "timestamp": self.timestamp,
            "db_connected": self.db_connected,
            "csv_files_used": self.csv_files_used,
            "summary": self.summary,
            "checks": checks,
        }

    def print_summary(self):
        s = self.summary
        total = s["total"]
        print(f"\n{'=' * 70}")
        print(f"VERIFICATION SUMMARY for {self.scenario_id}")
        print(f"{'=' * 70}")
        print(f"  Total checks:  {total}")
        print(f"  PASS:          {s['pass']}")
        print(f"  FAIL:          {s['fail']}")
        print(f"  Skipped:       {s['skip']}")
        print(f"  No DB data:    {s['no_db']}")
        if total > 0:
            pct = s["pass"] / max(total - s["skip"], 1) * 100
            print(f"  Pass rate:     {pct:.1f}%")
        failures = [c for c in self.checks if c.status == "fail"]
        if failures:
            print(f"\nFAILED CHECKS ({len(failures)}):")
            for c in failures[:20]:
                diff = abs(c.expected - c.actual) if c.actual is not None else None
                print(
                    f"  [{c.section}] {c.entity} / {c.metric}: "
                    f"expected={c.expected:.4f}, actual={c.actual:.4f}, "
                    f"diff={diff:.4f}"
                )
            if len(failures) > 20:
                print(f"  ... and {len(failures) - 20} more")


# CSV parsing


def parse_calsim_csv(file_path: str) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Parse a CalSim DSS-export CSV (DV, SV, DELIVERIES, or DEMANDS).
    Returns (data_df, units_series).
    """
    header_df = pd.read_csv(file_path, header=None, nrows=7, low_memory=False)
    var_names = header_df.iloc[1].tolist()
    units_row = header_df.iloc[6].tolist() if len(header_df) >= 7 else []

    col_names = []
    seen: Dict[str, int] = {}
    col_units = []
    for idx, var in enumerate(var_names):
        unit = units_row[idx] if idx < len(units_row) else "UNKNOWN"
        if var in seen:
            col_names.append(f"{var}_{seen[var]}")
            seen[var] += 1
        else:
            col_names.append(str(var))
            seen[var] = 1
        col_units.append(str(unit).strip())

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    if len(data_df.columns) > len(col_names):
        col_names.extend(
            [f"_extra_{i}" for i in range(len(data_df.columns) - len(col_names))]
        )
    elif len(data_df.columns) < len(col_names):
        col_names = col_names[: len(data_df.columns)]
        col_units = col_units[: len(data_df.columns)]

    data_df.columns = col_names
    first_col = col_names[0]
    data_df["DateTime"] = pd.to_datetime(data_df[first_col], errors="coerce")
    data_df = data_df.dropna(subset=["DateTime"])

    period_date = data_df["DateTime"].where(
        data_df["DateTime"].dt.day != 1,
        data_df["DateTime"] - pd.Timedelta(days=1),
    )
    data_df["CalendarMonth"] = period_date.dt.month
    data_df["CalendarYear"] = period_date.dt.year
    data_df["WaterYear"] = data_df["CalendarYear"].where(
        data_df["CalendarMonth"] < 10,
        data_df["CalendarYear"] + 1,
    )
    data_df["DaysInMonth"] = period_date.dt.days_in_month
    units_series = pd.Series(col_units, index=col_names)
    return data_df, units_series


def get_column_taf(
    df: pd.DataFrame, units: pd.Series, col_name: str
) -> Optional[pd.Series]:
    if col_name not in df.columns:
        return None
    raw = pd.to_numeric(df[col_name], errors="coerce")
    unit = str(units.get(col_name, "UNKNOWN")).upper()
    if unit == "TAF":
        return raw
    elif unit == "CFS":
        return raw * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    return raw


def get_column_cfs(
    df: pd.DataFrame, units: pd.Series, col_name: str
) -> Optional[pd.Series]:
    if col_name not in df.columns:
        return None
    return pd.to_numeric(df[col_name], errors="coerce")


def annual_avg_taf(series: pd.Series, water_years: pd.Series) -> Optional[float]:
    if series is None or series.dropna().empty:
        return None
    annual = series.groupby(water_years).sum()
    return round(float(annual.mean()), 4)


def monthly_avg(series: pd.Series, months: pd.Series, month: int) -> Optional[float]:
    if series is None:
        return None
    mask = months == month
    vals = series[mask].dropna()
    if vals.empty:
        return None
    return round(float(vals.mean()), 4)


# DB Helpers─────


def connect_db() -> Optional[object]:
    url = os.environ.get("DATABASE_URL")
    if not url:
        log.warning("DATABASE_URL not set; skipping DB verification")
        return None
    try:
        conn = get_db_connection(db_url=url)
        log.info("Connected to database")
        return conn
    except Exception as e:
        log.error(f"DB connection failed: {e}")
        return None


def db_query(conn, sql: str, params: tuple = ()) -> List[dict]:
    if conn is None:
        return []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


# Section: Reservoirs 


def verify_reservoirs(
    report: Report, dv_df: Optional[pd.DataFrame], dv_units: Optional[pd.Series], conn
) -> None:
    section = "reservoirs"
    log.info(f"Verifying {section}...")

    for short_code, (calsim_var, capacity) in RESERVOIR_VARS.items():
        # Expected values from CSV
        exp_apr = None
        exp_sep = None
        exp_ann = None
        exp_apr_pct = None
        exp_sep_pct = None

        if dv_df is not None and calsim_var in dv_df.columns:
            raw = pd.to_numeric(dv_df[calsim_var], errors="coerce")
            exp_apr = monthly_avg(raw, dv_df["CalendarMonth"], 4)
            exp_sep = monthly_avg(raw, dv_df["CalendarMonth"], 9)
            exp_ann = round(float(raw.mean()), 4) if not raw.dropna().empty else None
            if exp_apr is not None and capacity > 0:
                exp_apr_pct = round(exp_apr / capacity * 100, 4)
            if exp_sep is not None and capacity > 0:
                exp_sep_pct = round(exp_sep / capacity * 100, 4)

        # DB actual values
        act_apr = None
        act_sep = None
        act_ann = None
        act_apr_pct = None
        act_sep_pct = None

        if conn:
            rows = db_query(
                conn,
                """
                SELECT rps.april_avg_taf, rps.september_avg_taf,
                       rps.annual_avg_taf, rps.capacity_taf
                FROM reservoir_period_summary rps
                JOIN reservoir_entity re ON rps.reservoir_entity_id = re.id
                WHERE rps.scenario_short_code = %s AND re.short_code = %s
            """,
                (report.scenario_id, short_code),
            )
            if rows:
                r = rows[0]
                act_apr = _safe_round(r.get("april_avg_taf"))
                act_sep = _safe_round(r.get("september_avg_taf"))
                act_ann = _safe_round(r.get("annual_avg_taf"))
                cap = r.get("capacity_taf")
                if act_apr is not None and cap and cap > 0:
                    act_apr_pct = round(act_apr / float(cap) * 100, 4)
                if act_sep is not None and cap and cap > 0:
                    act_sep_pct = round(act_sep / float(cap) * 100, 4)

        report.add("april_avg_taf", section, short_code, exp_apr, act_apr)
        report.add("september_avg_taf", section, short_code, exp_sep, act_sep)
        report.add("annual_avg_taf", section, short_code, exp_ann, act_ann)
        report.add("april_pct_capacity", section, short_code, exp_apr_pct, act_apr_pct)
        report.add(
            "september_pct_capacity", section, short_code, exp_sep_pct, act_sep_pct
        )

    # Spill frequency
    if conn:
        for short_code, (calsim_var, _cap) in RESERVOIR_VARS.items():
            rows = db_query(
                conn,
                """
                SELECT rps.spill_frequency_pct
                FROM reservoir_period_summary rps
                JOIN reservoir_entity re ON rps.reservoir_entity_id = re.id
                WHERE rps.scenario_short_code = %s AND re.short_code = %s
            """,
                (report.scenario_id, short_code),
            )
            act_spill = (
                _safe_round(rows[0].get("spill_frequency_pct")) if rows else None
            )
            report.add("spill_frequency_pct", section, short_code, None, act_spill)


# Section: CWS aggregates


def verify_cws_aggregates(
    report: Report, dv_df: Optional[pd.DataFrame], dv_units: Optional[pd.Series], conn
) -> None:
    section = "cws_aggregate"
    log.info(f"Verifying {section}...")

    for var, label in CWS_AGGREGATE_VARS:
        exp_ann_taf = None
        if dv_df is not None and var in dv_df.columns:
            taf_series = get_column_taf(dv_df, dv_units, var)
            exp_ann_taf = annual_avg_taf(taf_series, dv_df["WaterYear"])

        act_ann_taf = None
        act_reliability = None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT p.annual_delivery_avg_taf, p.reliability_pct
                FROM cws_aggregate_period_summary p
                JOIN cws_aggregate_entity e ON p.cws_aggregate_id = e.id
                WHERE p.scenario_short_code = %s AND e.short_code = %s
            """,
                (report.scenario_id, var),
            )
            if rows:
                act_ann_taf = _safe_round(rows[0].get("annual_delivery_avg_taf"))
                act_reliability = _safe_round(rows[0].get("reliability_pct"))

        report.add("annual_delivery_avg_taf", section, var, exp_ann_taf, act_ann_taf)
        if act_reliability is not None:
            report.add("reliability_pct", section, var, None, act_reliability)

    for var, label in CWS_SHORTAGE_VARS:
        exp_short_taf = None
        if dv_df is not None and var in dv_df.columns:
            taf_series = get_column_taf(dv_df, dv_units, var)
            exp_short_taf = annual_avg_taf(taf_series, dv_df["WaterYear"])
        report.add("annual_shortage_avg_taf", section, var, exp_short_taf, None)


# Section: M&I contractors 


def verify_mi_contractors(report: Report, conn) -> None:
    section = "mi_contractors"
    log.info(f"Verifying {section}...")
    if not conn:
        return

    rows = db_query(
        conn,
        """
        SELECT mc.short_code, p.annual_delivery_avg_taf,
               p.annual_shortage_avg_taf, p.reliability_pct,
               p.avg_pct_demand_met, p.annual_demand_avg_taf
        FROM mi_contractor_period_summary p
        JOIN mi_contractor mc ON p.mi_contractor_code = mc.short_code
        WHERE p.scenario_short_code = %s
        ORDER BY mc.short_code
    """,
        (report.scenario_id,),
    )

    for r in rows:
        code = r["short_code"]
        report.add(
            "annual_delivery_avg_taf",
            section,
            code,
            None,
            _safe_round(r.get("annual_delivery_avg_taf")),
        )
        report.add(
            "annual_shortage_avg_taf",
            section,
            code,
            None,
            _safe_round(r.get("annual_shortage_avg_taf")),
        )
        report.add(
            "reliability_pct",
            section,
            code,
            None,
            _safe_round(r.get("reliability_pct")),
        )
        report.add(
            "avg_pct_demand_met",
            section,
            code,
            None,
            _safe_round(r.get("avg_pct_demand_met")),
        )

    if not rows:
        report.add("data_present", section, "all", 1.0, 0.0)


# Section: CWS demand units


def verify_cws_du(
    report: Report,
    dv_df: Optional[pd.DataFrame],
    dv_units: Optional[pd.Series],
    sv_df: Optional[pd.DataFrame],
    sv_units: Optional[pd.Series],
    conn,
) -> None:
    """Verify CWS DU delivery (DN_* from DV) and demand (UD_* from SV)."""
    section = "cws_du"
    log.info(f"Verifying {section}...")

    for du in SAMPLE_CWS_DUS:
        del_col = f"DN_{du}"
        dem_col = f"UD_{du}"

        exp_del_taf = None
        exp_dem_taf = None
        if dv_df is not None:
            del_taf = get_column_taf(dv_df, dv_units, del_col)
            exp_del_taf = annual_avg_taf(del_taf, dv_df["WaterYear"])
        if sv_df is not None:
            dem_taf = get_column_taf(sv_df, sv_units, dem_col)
            exp_dem_taf = annual_avg_taf(dem_taf, sv_df["WaterYear"])

        act_del_taf = None
        act_dem_taf = None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT p.annual_delivery_avg_taf, p.annual_demand_avg_taf
                FROM du_period_summary p
                WHERE p.scenario_short_code = %s AND p.du_id = %s
            """,
                (report.scenario_id, du),
            )
            if rows:
                act_del_taf = _safe_round(rows[0].get("annual_delivery_avg_taf"))
                act_dem_taf = _safe_round(rows[0].get("annual_demand_avg_taf"))

        report.add("annual_delivery_avg_taf", section, du, exp_del_taf, act_del_taf)
        report.add("annual_demand_avg_taf", section, du, exp_dem_taf, act_dem_taf)


# Section: AG demand units 


def verify_ag(
    report: Report, dv_df: Optional[pd.DataFrame], dv_units: Optional[pd.Series], conn
) -> None:
    """Verify AG demand (AW_*), delivery (DN_*), GW pumping (GP_*) — all from DV."""
    section = "ag"
    log.info(f"Verifying {section}...")

    for du in SAMPLE_AG_DUS:
        del_col = f"DN_{du}"
        dem_col = f"AW_{du}"
        gp_col = f"GP_{du}"

        exp_del_taf = None
        exp_dem_taf = None
        exp_gp_taf = None

        if dv_df is not None:
            del_taf = get_column_taf(dv_df, dv_units, del_col)
            exp_del_taf = annual_avg_taf(del_taf, dv_df["WaterYear"])
            gp_taf = get_column_taf(dv_df, dv_units, gp_col)
            exp_gp_taf = annual_avg_taf(gp_taf, dv_df["WaterYear"])
            dem_taf = get_column_taf(dv_df, dv_units, dem_col)
            exp_dem_taf = annual_avg_taf(dem_taf, dv_df["WaterYear"])

        act_del_taf = None
        act_gp_taf = None
        act_dem_taf = None
        act_reliability = None

        if conn:
            rows = db_query(
                conn,
                """
                SELECT p.annual_sw_delivery_avg_taf,
                       p.annual_gw_pumping_avg_taf,
                       p.annual_demand_avg_taf,
                       p.reliability_pct
                FROM ag_du_period_summary p
                WHERE p.scenario_short_code = %s AND p.du_id = %s
            """,
                (report.scenario_id, du),
            )
            if rows:
                r = rows[0]
                act_del_taf = _safe_round(r.get("annual_sw_delivery_avg_taf"))
                act_gp_taf = _safe_round(r.get("annual_gw_pumping_avg_taf"))
                act_dem_taf = _safe_round(r.get("annual_demand_avg_taf"))
                act_reliability = _safe_round(r.get("reliability_pct"))

        report.add("annual_sw_delivery_avg_taf", section, du, exp_del_taf, act_del_taf)
        report.add("annual_gw_pumping_avg_taf", section, du, exp_gp_taf, act_gp_taf)
        report.add("annual_demand_avg_taf", section, du, exp_dem_taf, act_dem_taf)
        if act_reliability is not None:
            report.add("reliability_pct", section, du, None, act_reliability)

    # AG aggregate delivery — expected from DV (original aggregates keyed by DV variable)
    for var, label in AG_AGGREGATE_VARS:
        exp_ann = None
        if dv_df is not None:
            taf_series = get_column_taf(dv_df, dv_units, var)
            exp_ann = annual_avg_taf(taf_series, dv_df["WaterYear"])

        act_ann = None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT p.annual_delivery_avg_taf
                FROM ag_aggregate_period_summary p
                JOIN ag_aggregate_entity e ON p.aggregate_code = e.short_code
                WHERE p.scenario_short_code = %s AND e.short_code = %s
            """,
                (report.scenario_id, var),
            )
            if rows:
                act_ann = _safe_round(rows[0].get("annual_delivery_avg_taf"))
        report.add("annual_delivery_avg_taf", "ag_aggregate", var, exp_ann, act_ann)

    # New direct AG aggregates (cvp_psc_n, cvp_pex_s)
    for short_code, dv_var, label in AG_AGGREGATE_NEW_VARS:
        exp_ann = None
        if dv_df is not None:
            taf_series = get_column_taf(dv_df, dv_units, dv_var)
            exp_ann = annual_avg_taf(taf_series, dv_df["WaterYear"])

        act_ann = None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT p.annual_delivery_avg_taf
                FROM ag_aggregate_period_summary p
                JOIN ag_aggregate_entity e ON p.aggregate_code = e.short_code
                WHERE p.scenario_short_code = %s AND e.short_code = %s
            """,
                (report.scenario_id, short_code),
            )
            if rows:
                act_ann = _safe_round(rows[0].get("annual_delivery_avg_taf"))
        report.add(
            "annual_delivery_avg_taf", "ag_aggregate", short_code, exp_ann, act_ann
        )

    # Computed AG aggregates (nod_ag, sod_ag) — sum of components
    for agg_code, components in AG_COMPUTED_AGGREGATES.items():
        exp_ann = None
        if dv_df is not None:
            total_annual = None
            for del_var in components["delivery_components"]:
                comp_series = get_column_taf(dv_df, dv_units, del_var)
                if comp_series is not None:
                    comp_annual = comp_series.groupby(dv_df["WaterYear"]).sum()
                    if total_annual is None:
                        total_annual = comp_annual
                    else:
                        total_annual = total_annual + comp_annual
            if total_annual is not None:
                exp_ann = round(float(total_annual.mean()), 4)

        act_ann = None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT p.annual_delivery_avg_taf
                FROM ag_aggregate_period_summary p
                JOIN ag_aggregate_entity e ON p.aggregate_code = e.short_code
                WHERE p.scenario_short_code = %s AND e.short_code = %s
            """,
                (report.scenario_id, agg_code),
            )
            if rows:
                act_ann = _safe_round(rows[0].get("annual_delivery_avg_taf"))
        report.add(
            "annual_delivery_avg_taf", "ag_aggregate", agg_code, exp_ann, act_ann
        )


# Section: Env Flows 


def verify_env_flows(
    report: Report, dv_df: Optional[pd.DataFrame], dv_units: Optional[pd.Series], conn
) -> None:
    section = "env_flows"
    log.info(f"Verifying {section}...")

    for var in FLOW_VARS:
        exp_avg_cfs = None
        _exp_ann_taf = None

        if dv_df is not None and var in dv_df.columns:
            raw = pd.to_numeric(dv_df[var], errors="coerce")
            exp_avg_cfs = (
                round(float(raw.mean()), 4) if not raw.dropna().empty else None
            )
            taf_series = get_column_taf(dv_df, dv_units, var)
            _exp_ann_taf = annual_avg_taf(taf_series, dv_df["WaterYear"])

        act_avg_cfs = None
        act_pearson_r = None
        act_pct_unimp = None
        act_pct_ff = None

        # network_arc_id stores the channel code string ("C_SAC041") directly,
        # so match the full variable against it. There is no join to network_arc.
        if conn:
            monthly_rows = db_query(
                conn,
                """
                SELECT AVG(m.flow_avg_cfs) as overall_avg_cfs
                FROM env_flow_channel_monthly m
                WHERE m.scenario_short_code = %s AND m.network_arc_id = %s
            """,
                (report.scenario_id, var),
            )
            if monthly_rows and monthly_rows[0].get("overall_avg_cfs") is not None:
                act_avg_cfs = _safe_round(monthly_rows[0]["overall_avg_cfs"])

            period_rows = db_query(
                conn,
                """
                SELECT p.pearson_r, p.avg_pct_unimpaired, p.avg_pct_ff
                FROM env_flow_channel_period_summary p
                WHERE p.scenario_short_code = %s AND p.network_arc_id = %s
            """,
                (report.scenario_id, var),
            )
            if period_rows:
                r = period_rows[0]
                act_pearson_r = _safe_round(r.get("pearson_r"))
                act_pct_unimp = _safe_round(r.get("avg_pct_unimpaired"))
                act_pct_ff = _safe_round(r.get("avg_pct_ff"))

        report.add("avg_cfs", section, var, exp_avg_cfs, act_avg_cfs)
        if act_pearson_r is not None:
            report.add("pearson_r", section, var, None, act_pearson_r)
        if act_pct_unimp is not None:
            report.add("avg_pct_unimpaired", section, var, None, act_pct_unimp)
        if act_pct_ff is not None:
            report.add("avg_pct_ff", section, var, None, act_pct_ff)


# Section: Refuge


def verify_refuge(report: Report, conn) -> None:
    section = "refuge"
    log.info(f"Verifying {section}...")
    if not conn:
        return

    rows = db_query(
        conn,
        """
        SELECT p.du_id, p.annual_delivery_avg_taf,
               p.annual_shortage_avg_taf, p.reliability_pct_95,
               p.annual_shortage_pct_avg
        FROM refuge_du_period_summary p
        WHERE p.scenario_short_code = %s
        ORDER BY p.du_id
    """,
        (report.scenario_id,),
    )

    for r in rows:
        du = r["du_id"]
        report.add(
            "annual_delivery_avg_taf",
            section,
            du,
            None,
            _safe_round(r.get("annual_delivery_avg_taf")),
        )
        report.add(
            "annual_shortage_avg_taf",
            section,
            du,
            None,
            _safe_round(r.get("annual_shortage_avg_taf")),
        )
        report.add(
            "reliability_pct_95",
            section,
            du,
            None,
            _safe_round(r.get("reliability_pct_95")),
        )

    if not rows:
        report.add("data_present", section, "all", 1.0, 0.0)


# Section: Delta


def verify_delta(
    report: Report, dv_df: Optional[pd.DataFrame], dv_units: Optional[pd.Series], conn
) -> None:
    """Verify Delta outflow (NDO), X2 position, and salinity from DV CSV vs DB."""
    section = "delta"
    log.info(f"Verifying {section}...")

    # --- NDO outflow: annual avg TAF ---
    for var_code, calsim_var, native_unit in DELTA_OUTFLOW_VARS:
        exp_ann_taf = None
        if dv_df is not None and calsim_var in dv_df.columns:
            taf_series = get_column_taf(dv_df, dv_units, calsim_var)
            exp_ann_taf = annual_avg_taf(taf_series, dv_df["WaterYear"])

        act_ann_taf = None
        act_avg_cfs = None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT summary_data
                FROM delta_period_summary
                WHERE scenario_short_code = %s AND variable_code = %s
            """,
                (report.scenario_id, var_code),
            )
            if rows:
                sd = rows[0].get("summary_data", {})
                act_ann_taf = _safe_round(sd.get("annual_avg_taf"))
                act_avg_cfs = _safe_round(sd.get("avg_cfs"))

        report.add("annual_avg_taf", section, var_code, exp_ann_taf, act_ann_taf)
        if act_avg_cfs is not None:
            exp_avg_cfs = None
            if dv_df is not None and calsim_var in dv_df.columns:
                raw = pd.to_numeric(dv_df[calsim_var], errors="coerce")
                exp_avg_cfs = (
                    round(float(raw.mean()), 4) if not raw.dropna().empty else None
                )
            report.add("avg_cfs", section, var_code, exp_avg_cfs, act_avg_cfs)

    # --- X2: period average in KM ---
    for var_code, calsim_var, native_unit in DELTA_X2_VARS:
        exp_avg = None
        if dv_df is not None and calsim_var in dv_df.columns:
            raw = pd.to_numeric(dv_df[calsim_var], errors="coerce")
            exp_avg = round(float(raw.mean()), 4) if not raw.dropna().empty else None

        act_avg = None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT summary_data
                FROM delta_period_summary
                WHERE scenario_short_code = %s AND variable_code = %s
            """,
                (report.scenario_id, var_code),
            )
            if rows:
                sd = rows[0].get("summary_data", {})
                act_avg = _safe_round(sd.get("avg_km"))

        report.add("avg_km", section, var_code, exp_avg, act_avg)

        # April and September X2
        if dv_df is not None and calsim_var in dv_df.columns:
            raw = pd.to_numeric(dv_df[calsim_var], errors="coerce")
            exp_apr = monthly_avg(raw, dv_df["CalendarMonth"], 4)
            exp_sep = monthly_avg(raw, dv_df["CalendarMonth"], 9)
        else:
            exp_apr, exp_sep = None, None

        act_apr, act_sep = None, None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT summary_data
                FROM delta_period_summary
                WHERE scenario_short_code = %s AND variable_code = %s
            """,
                (report.scenario_id, var_code),
            )
            if rows:
                sd = rows[0].get("summary_data", {})
                act_apr = _safe_round(sd.get("april_avg_km"))
                act_sep = _safe_round(sd.get("sept_avg_km"))

        report.add("april_avg_km", section, var_code, exp_apr, act_apr)
        report.add("sept_avg_km", section, var_code, exp_sep, act_sep)

    # --- Salinity: period average EC ---
    for var_code, calsim_var, native_unit in DELTA_SALINITY_VARS:
        exp_avg = None
        if dv_df is not None and calsim_var in dv_df.columns:
            raw = pd.to_numeric(dv_df[calsim_var], errors="coerce")
            exp_avg = round(float(raw.mean()), 4) if not raw.dropna().empty else None

        act_avg = None
        if conn:
            rows = db_query(
                conn,
                """
                SELECT summary_data
                FROM delta_period_summary
                WHERE scenario_short_code = %s AND variable_code = %s
            """,
                (report.scenario_id, var_code),
            )
            if rows:
                sd = rows[0].get("summary_data", {})
                act_avg = _safe_round(sd.get("avg_ec"))

        report.add("avg_ec", section, var_code, exp_avg, act_avg)

    # --- Delta monthly: spot-check NDO month 1 ---
    if conn:
        rows = db_query(
            conn,
            """
            SELECT variable_code, water_month, avg, avg_cfs, sample_count
            FROM delta_monthly
            WHERE scenario_short_code = %s
            ORDER BY variable_code, water_month
        """,
            (report.scenario_id,),
        )
        row_count = len(rows)
        expected_rows = 8 * 12  # 8 variables x 12 months
        report.add(
            "monthly_row_count",
            section,
            "all",
            float(expected_rows),
            float(row_count),
            abs_tol=12,
            rel_tol=0.15,
        )


# Section: Tiers

TIER_CODES = [
    "CWS_DEL",
    "AG_REV",
    "ENV_FLOWS",
    "RES_STOR",
    "GW_STOR",
    "DELTA_ECO",
    "FW_DELTA_USES",
    "FW_EXP",
    "WRC_SALMON_AB",
]


def verify_tiers(report: Report, conn, tier_staging_dir: Optional[Path]) -> None:
    section = "tiers"
    log.info(f"Verifying {section}...")
    if not conn:
        return

    for tier_code in TIER_CODES:
        rows = db_query(
            conn,
            """
            SELECT tr.single_tier_level,
                   tr.tier_1_value, tr.tier_2_value,
                   tr.tier_3_value, tr.tier_4_value,
                   tr.total_value
            FROM tier_result tr
            WHERE tr.scenario_short_code = %s
              AND tr.tier_short_code = %s
              AND tr.is_active = TRUE
        """,
            (report.scenario_id, tier_code),
        )

        if rows:
            r = rows[0]
            single = r.get("single_tier_level")
            total = r.get("total_value")
            if single is not None:
                report.add("single_tier_level", section, tier_code, None, float(single))
            elif total is not None:
                report.add(
                    "total_location_count", section, tier_code, None, float(total)
                )
            else:
                report.add("data_present", section, tier_code, 1.0, 0.0)
        else:
            report.add("data_present", section, tier_code, 1.0, 0.0)

    # Verify tier location counts against staging CSVs if available
    if tier_staging_dir and tier_staging_dir.exists():
        _verify_tier_staging(report, conn, tier_staging_dir)


def _verify_tier_staging(report: Report, conn, staging_dir: Path) -> None:
    staging_files = {
        "CWS_DEL": "CWS_DEL.csv",
        "AG_REV": "AG_REV.csv",
        "ENV_FLOWS": "ENV_FLOWS.csv",
        "RES_STOR": "RES_STOR.csv",
        "GW_STOR": "GW_STOR.csv",
        "DELTA_ECO": "DELTA_ECO.csv",
        "FW_DELTA_USES": "FW_DELTA_USES.csv",
        "FW_EXP": "FW_EXP.csv",
    }

    for tier_code, filename in staging_files.items():
        csv_path = staging_dir / filename
        if not csv_path.exists():
            continue

        db_rows = db_query(
            conn,
            """
            SELECT COUNT(*) as loc_count
            FROM tier_location_result
            WHERE scenario_short_code = %s
              AND tier_short_code = %s
              AND is_active = TRUE
        """,
            (report.scenario_id, tier_code),
        )

        db_count = db_rows[0]["loc_count"] if db_rows else 0

        try:
            csv_df = pd.read_csv(str(csv_path))
            scenario_id = report.scenario_id
            if tier_code in ("CWS_DEL",):
                csv_count = (
                    csv_df[csv_df.iloc[:, 0] == scenario_id].shape[0]
                    if scenario_id in csv_df.iloc[:, 0].values
                    else 0
                )
                if csv_count == 0:
                    csv_row = csv_df[csv_df.iloc[:, 0] == scenario_id]
                    if not csv_row.empty:
                        csv_count = csv_row.iloc[0, 1:].notna().sum()
            else:
                csv_count = None
        except Exception:
            csv_count = None

        if csv_count is not None and csv_count > 0:
            report.add(
                "tier_location_count",
                "tier_staging",
                tier_code,
                float(csv_count),
                float(db_count),
            )
        elif db_count > 0:
            report.add(
                "tier_location_count", "tier_staging", tier_code, None, float(db_count)
            )


# Section: Unit conversion validation


def verify_unit_conversion(
    report: Report, dv_df: Optional[pd.DataFrame], dv_units: Optional[pd.Series]
) -> None:
    """Verify CFS-to-TAF conversion factor by checking balance identities.

    For AG DUs: AW (demand) = DN (delivery) + GP (GW pumping).
    All are in CFS in DV. If our conversion is consistent, annual TAF totals
    should satisfy this identity within tolerance.
    """
    section = "unit_conversion"
    log.info(f"Verifying {section}...")
    if dv_df is None:
        return

    test_dus = ["02_NA", "08N_PA", "61_PA1"]
    for du in test_dus:
        aw_col = f"AW_{du}"
        dn_col = f"DN_{du}"
        gp_col = f"GP_{du}"

        if aw_col not in dv_df.columns or dn_col not in dv_df.columns:
            continue

        aw_taf = get_column_taf(dv_df, dv_units, aw_col)
        dn_taf = get_column_taf(dv_df, dv_units, dn_col)

        if gp_col in dv_df.columns:
            gp_taf = get_column_taf(dv_df, dv_units, gp_col)
        else:
            gp_taf = aw_taf - dn_taf

        annual_aw = aw_taf.groupby(dv_df["WaterYear"]).sum()
        annual_sum = (dn_taf + gp_taf).groupby(dv_df["WaterYear"]).sum()

        exp = round(float(annual_aw.mean()), 4)
        act = round(float(annual_sum.mean()), 4)
        report.add(
            "ag_balance_aw_eq_dn_plus_gp",
            section,
            du,
            exp,
            act,
            abs_tol=5.0,
            rel_tol=0.05,
        )


# Helpers


def _safe_round(val, decimals=4) -> Optional[float]:
    if val is None:
        return None
    return round(float(val), decimals)


def find_file(base_dir: Path, run_id: str, suffix: str) -> Optional[Path]:
    """Search for a reference CSV by run_id and suffix pattern.

    Tries full run_id first, then falls back to just the scenario prefix (e.g. s0020).
    """
    for f in sorted(base_dir.glob(f"{run_id}*{suffix}*")):
        if f.suffix == ".csv":
            return f
    scenario_prefix = run_id.split("_")[0] if "_" in run_id else run_id
    if scenario_prefix != run_id:
        for f in sorted(base_dir.glob(f"{scenario_prefix}*{suffix}*")):
            if f.suffix == ".csv":
                return f
    return None


# Mainy mainy main main


def run_scenario(
    scenario_id: str,
    ref_dir: Path,
    report_dir: Optional[Path],
    csv_only: bool,
    tier_staging_dir: Optional[Path],
) -> Report:
    report = Report(scenario_id=scenario_id)

    run_id = SCENARIO_RUN_IDS.get(scenario_id, scenario_id)
    log.info(f"Scenario: {scenario_id} (run_id: {run_id})")
    log.info(f"Reference dir: {ref_dir}")

    dv_path = find_file(ref_dir, run_id, "calsim_output")
    sv_path = find_file(ref_dir, run_id, "sv_input")

    if dv_path is None:
        dv_path = find_file(ref_dir, run_id, "DV")
    if sv_path is None:
        sv_path = find_file(ref_dir, run_id, "SV")

    dv_df, dv_units = (None, None)
    sv_df, sv_units = (None, None)

    if dv_path:
        log.info(f"  DV: {dv_path.name}")
        report.csv_files_used["dv"] = str(dv_path)
        dv_df, dv_units = parse_calsim_csv(str(dv_path))
    else:
        log.warning("  DV: NOT FOUND")

    if sv_path:
        log.info(f"  SV: {sv_path.name}")
        report.csv_files_used["sv"] = str(sv_path)
        sv_df, sv_units = parse_calsim_csv(str(sv_path))
    else:
        log.warning("  SV: NOT FOUND")

    conn = None
    if not csv_only:
        conn = connect_db()
        report.db_connected = conn is not None

    try:
        verify_unit_conversion(report, dv_df, dv_units)
        verify_reservoirs(report, dv_df, dv_units, conn)
        verify_cws_aggregates(report, dv_df, dv_units, conn)
        verify_cws_du(report, dv_df, dv_units, sv_df, sv_units, conn)
        verify_ag(report, dv_df, dv_units, conn)
        verify_mi_contractors(report, conn)
        verify_env_flows(report, dv_df, dv_units, conn)
        verify_refuge(report, conn)
        verify_delta(report, dv_df, dv_units, conn)
        verify_tiers(report, conn, tier_staging_dir)
    finally:
        if conn:
            conn.close()

    report.print_summary()

    if report_dir:
        report_dir.mkdir(parents=True, exist_ok=True)
        out_path = report_dir / f"{scenario_id}_layer2.json"
        with open(out_path, "w") as f:
            json.dump(report.to_dict(), f, indent=2, default=str)
        log.info(f"Report written to {out_path}")

    return report


def render_scorecard(
    reports: List[Report],
    json_paths: List[Path],
    file_obj=sys.stdout,
) -> None:
    """One-screen PASS / FAIL summary aggregated by section across scenarios.

    Each row is a verifier section (reservoirs, cws_aggregate, ag, ...). Counts
    sum across every scenario in this run. Up to three failure examples are
    listed per failing section as `scenario/entity`. `Detail: <path>` at the
    bottom points at the last JSON report written (if any).
    """
    if not reports:
        print("No reports to render.", file=file_obj)
        return

    scenarios = [r.scenario_id for r in reports]
    if len(scenarios) > 1:
        title = f"verify_all_sections.py {scenarios[0]}..{scenarios[-1]} ({len(scenarios)} scenarios)"
    else:
        title = f"verify_all_sections.py {scenarios[0]}"
    print(f"\n{title}", file=file_obj)
    print("=" * len(title), file=file_obj)

    by_section: Dict[str, Dict[str, object]] = {}
    for r in reports:
        for c in r.checks:
            entry = by_section.setdefault(
                c.section, {"total": 0, "fail": 0, "fail_examples": []}
            )
            entry["total"] = int(entry["total"]) + 1  # type: ignore[arg-type]
            if c.status == "fail":
                entry["fail"] = int(entry["fail"]) + 1  # type: ignore[arg-type]
                examples: List[str] = entry["fail_examples"]  # type: ignore[assignment]
                if len(examples) < 3:
                    examples.append(f"{r.scenario_id}/{c.entity}")

    overall_pass = 0
    overall_total = 0
    for sec in sorted(by_section.keys()):
        e = by_section[sec]
        total = int(e["total"])
        n_fail = int(e["fail"])
        overall_total += 1
        if n_fail == 0:
            overall_pass += 1
            print(f"PASS {sec:<18} ({total} checks, 0 mismatches)", file=file_obj)
        else:
            examples = ", ".join(e["fail_examples"])  # type: ignore[arg-type]
            ellipsis = ", ..." if n_fail > 3 else ""
            print(
                f"FAIL {sec:<18} ({total} checks, {n_fail} mismatches: {examples}{ellipsis})",
                file=file_obj,
            )

    n_fail_sections = overall_total - overall_pass
    print(file=file_obj)
    if n_fail_sections == 0:
        print(f"Overall: {overall_pass}/{overall_total} sections PASS.", file=file_obj)
    else:
        print(
            f"Overall: {overall_pass}/{overall_total} sections PASS, {n_fail_sections} FAIL.",
            file=file_obj,
        )
    if json_paths:
        print(f"Detail: {json_paths[-1]}", file=file_obj)


def main():
    parser = argparse.ArgumentParser(
        description="Verify data explorer values against reference CSVs and database"
    )
    parser.add_argument("--scenario", default=None, help="Scenario ID (e.g., s0020)")
    parser.add_argument(
        "--all-scenarios", action="store_true", help="Run for all active scenarios"
    )
    parser.add_argument(
        "--ref-dir", default=None, help="Path to reference CSV directory"
    )
    parser.add_argument(
        "--report-dir", default=None, help="Path for JSON output reports"
    )
    parser.add_argument(
        "--csv-only", action="store_true", help="Skip DB comparison (CSV-only mode)"
    )
    parser.add_argument(
        "--tier-staging-dir", default=None, help="Path to tier staging CSVs"
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--no-json",
        action="store_true",
        help="Skip the per-scenario JSON file write. Use for ad-hoc local runs.",
    )
    output_group.add_argument(
        "--json-stdout",
        action="store_true",
        help="Dump combined JSON to stdout instead of the scorecard. Useful for CI or piping to jq.",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    repo_root = script_dir.parent.parent
    ref_dir = (
        Path(args.ref_dir)
        if args.ref_dir
        else repo_root / "audits" / "notebooks_reference"
    )
    default_report_dir = repo_root / "audits" / "verification_reports"
    explicit_report_dir = Path(args.report_dir) if args.report_dir else default_report_dir
    # Honor --no-json / --json-stdout by suppressing the per-scenario file write.
    report_dir_for_run: Optional[Path] = (
        None if (args.no_json or args.json_stdout) else explicit_report_dir
    )
    tier_staging_dir = (
        Path(args.tier_staging_dir)
        if args.tier_staging_dir
        else repo_root / "etl" / "tier_data" / "staging"
    )

    scenarios = []
    if args.all_scenarios:
        scenarios = ALL_SCENARIOS
    elif args.scenario:
        scenarios = [args.scenario]
    else:
        scenarios = ["s0020"]

    all_reports: List[Report] = []
    for sid in scenarios:
        log.info(f"\n{'=' * 70}")
        log.info(f"  SCENARIO: {sid}")
        log.info(f"{'=' * 70}")
        r = run_scenario(sid, ref_dir, report_dir_for_run, args.csv_only, tier_staging_dir)
        all_reports.append(r)

    if args.json_stdout:
        json.dump(
            {"scenarios": [r.to_dict() for r in all_reports]},
            sys.stdout,
            indent=2,
            default=str,
        )
        sys.stdout.write("\n")
    else:
        json_paths: List[Path] = []
        if report_dir_for_run:
            for r in all_reports:
                json_paths.append(report_dir_for_run / f"{r.scenario_id}_layer2.json")
        render_scorecard(all_reports, json_paths)

    total_fail = sum(r.summary["fail"] for r in all_reports)
    sys.exit(1 if total_fail > 0 else 0)


if __name__ == "__main__":
    main()
