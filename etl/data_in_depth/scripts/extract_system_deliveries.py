"""extract_system_deliveries.py - annual water-year CVP/SWP delivery & Delta
export totals -> data_in_depth_value.

Pulls 25 explicit trend-report variables (CVP/SWP delivery totals broken out
by NOD/SOD/AG/M&I/Refuges, Delta export totals, and a handful of Southern San
Joaquin Valley export paths), each ALREADY a pre-aggregated/pre-computed raw
CalSim variable (e.g. DEL_CVP_TOTAL, DEL_CVP_TOT_N_WAMER for NOD, DEL_CVP_TOT_S_WLOSS
for SOD) - unlike every other data_in_depth domain, there is NO subject_member
aggregation here: NOD/SOD/Total triplets are each their OWN independent raw
variable in the source, not something this ETL sums from parts. Per user
decision (2026-08-12), every variable is its own flat METRIC subject
(subject_kind='metric', no location) - short_code = the variable name itself,
uppercased, 1:1 with source_variable

Sums the monthly TAF across each **water year (Oct-Sep)** to a single annual
volume per variable, matching extract_river_flow.py's convention (these are
monthly CalSim series, not already-annual like the CWS/ag/groundwater source
files) - per explicit user decision (2026-08-12), confirmed over an
April/September point-sample alternative.

- period = 'annual', unit = TAF (one row per scenario/subject/water_year).
- Water year Y = Oct (Y-1) .. Sep (Y). Only years with all 12 months are summed.
- No percent-of-capacity, no aggregates (each row already IS a total/leaf).
- Everything derived (exceedance, box, mean, CV) is computed live by the API.

Output -> etl/data_in_depth/output/system_deliveries_values.sql (gitignored). Apply:
    psql "$DATABASE_URL" -f etl/data_in_depth/output/system_deliveries_values.sql
Prereqs: create_data_in_depth_value_table.sql +
         seed_data_in_depth_system_deliveries_subjects.sql applied.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd

try:
    from .trend_report import TrendReport, DEFAULT_PATH
    from .trend_report_parser import load_values
except ImportError:  # pragma: no cover
    from trend_report import TrendReport, DEFAULT_PATH
    from trend_report_parser import load_values

log = logging.getLogger("extract_system_deliveries")

# variable (as it appears in the trend-report catalog, uppercased) -> label,
# verbatim from the user's spec (2026-08-12). MUST match
# seed_data_in_depth_system_deliveries_subjects.sql's short_code/label pairs
# byte-for-byte (25 entries, cross-checked - see extractor's --dry-run output).
SYSTEM_DELIVERY_VARS: Dict[str, str] = {
    "DEL_CVP_TOT_N_WAMER": "NOD Central Valley Project deliveries (AG + M&I + Wildlife Refuges)",
    "DEL_CVP_TOT_S_WLOSS": "SOD Central Valley Project deliveries (AG + M&I + Wildlife Refuges)",
    "DEL_CVP_TOTAL": "Total Central Valley Project deliveries (AG + M&I + Wildlife Refuges)",
    "DEL_CVP_PAG_NOD": "NOD Central Valley Project deliveries AG",
    "DEL_CVP_PAG_SOD": "SOD Central Valley Project deliveries AG",
    "DEL_CVP_PAG_TOTAL": "Total Central Valley Project deliveries AG",
    "DEL_CVP_PMI_TOTAL": "Total Central Valley Project deliveries M&I",
    "DEL_CVP_PMI_N_WAMER": "NOD Central Valley Project deliveries M&I",
    "DEL_CVP_PMI_S": "SOD Central Valley Project deliveries M&I",
    "DEL_CVP_PRF_TOTAL": "Central Valley Project deliveries Wildlife Refuges",
    "C_CVP_TOTAL_EXPORTS": "Central Valley Project Delta Exports",
    "DEL_SWP_TOT_N": "NOD State Water Project deliveries (AG + M&I)",
    "DEL_SWP_TOT_S": "SOD State Water Project deliveries (AG + M&I)",
    "DEL_SWP_TOTAL": "Total State Water Project deliveries (AG + M&I)",
    "DEL_SWP_PAG_NOD": "NOD State Water Project deliveries AG",
    "DEL_SWP_PAG_S": "SOD State Water Project deliveries AG",
    "DEL_SWP_PAG_TOTAL": "Total State Water Project deliveries AG",
    "DEL_SWP_PMI": "Total State Water Project deliveries M&I",
    "DEL_SWP_PMI_N": "NOD State Water Project deliveries M&I",
    "DEL_SWP_PMI_S": "SOD State Water Project deliveries M&I",
    "D_MLRTN_FRK000": "Southern San Joaquin Valley exports (Friant Division)",
    "D_CAA238_CVPCV": "Southern San Joaquin Valley exports (Cross Valley Canal)",
    "SWP_TA_KERNAG": "Southern San Joaquin Valley Exports (Kern County Water Agency)",
    "C_CAA003_SWP": "State Water Project Delta Exports",
    "C_CVPSWP_TOTAL_EXPORTS": "total Delta Exports (CVP + SWP)",
}
SUBJECT_KIND = "metric"
PERIOD = "annual"
UNIT_TAF = "TAF"
MONTHS_PER_WY = 12
VALUE_TABLE = "data_in_depth_value"
DEFAULT_OUT_DIR = Path("etl/data_in_depth/output")

# c_part to exclude when a (variable, scenario) has more than one TAF column -
# see the module docstring's "Duplicate-column gotcha". Only affects
# DEL_CVP_TOTAL/DEL_CVP_PAG_TOTAL/DEL_CVP_PRF_TOTAL; harmless no-op elsewhere.
EXCLUDE_C_PART = "DELIVERY-CALC"


def build_series(tr: TrendReport, scenarios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Annual (water-year) TAF sums per (scenario, subject/variable, water_year)."""
    variables: List[str] = list(SYSTEM_DELIVERY_VARS)

    # Filter the catalog directly (not via TrendReport.select/to_long's
    # `unit=` path, which skips the prefer_unit dedup and would choke on the
    # DELIVERY-CVP/DELIVERY-CALC duplicate columns - see module docstring).
    cat = tr.catalog
    sub = cat[(cat["variable"].isin(variables)) & (cat["unit"] == UNIT_TAF)]
    sub = sub[sub["c_part"] != EXCLUDE_C_PART]
    if scenarios is not None:
        sub = sub[sub["scenario"].isin(set(scenarios))]

    dupes = sub.duplicated(subset=["variable", "scenario"])
    if dupes.any():
        raise ValueError(
            f"Unexpected duplicate (variable, scenario) columns after c_part filter: "
            f"{sub.loc[dupes, ['variable', 'scenario', 'c_part']].to_dict('records')}"
        )

    found = set(sub["variable"].unique())
    missing = [v for v in variables if v not in found]
    if missing:
        log.warning("system-delivery variables with no TAF data: %s", missing)

    wide = load_values(tr.path, catalog=tr.catalog, col_positions=sub["col_pos"])
    long = (
        wide.stack(["variable", "scenario", "unit"], future_stack=True)
        .rename("value").reset_index().dropna(subset=["value"])
    )
    if long.empty:
        raise ValueError("No system-deliveries TAF rows found")

    df = long.copy()
    # Water year (Oct-Sep): months Oct/Nov/Dec belong to the next calendar year's WY.
    df["water_year"] = df["date"].dt.year + (df["date"].dt.month >= 10).astype(int)
    df["subject_code"] = df["variable"]        # 1:1 - the variable itself is the subject
    df["source_variable"] = df["variable"]

    g = (df.groupby(["scenario", "subject_code", "source_variable", "water_year"])["value"]
            .agg(value="sum", n="count").reset_index())
    incomplete = int((g["n"] != MONTHS_PER_WY).sum())
    if incomplete:
        log.warning("dropping %d partial water-years (< %d months)", incomplete, MONTHS_PER_WY)
    g = g[g["n"] == MONTHS_PER_WY].copy()

    g["subject_kind"] = SUBJECT_KIND
    g["period"] = PERIOD
    g["unit_code"] = UNIT_TAF
    return g[["scenario", "subject_kind", "subject_code", "source_variable",
              "period", "water_year", "value", "unit_code"]].reset_index(drop=True)


def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def generate_value_sql(records: pd.DataFrame, batch_size: int = 5000) -> str:
    """Batched INSERT ... SELECT FROM (VALUES ...) upserts; ids resolved by join."""
    lines = [
        f"-- System deliveries (annual water-year TAF) values for {VALUE_TABLE}",
        f"-- {len(records)} rows | generated by etl/data_in_depth/scripts/extract_system_deliveries.py",
        "\\set ON_ERROR_STOP on", "BEGIN;", "",
    ]
    recs = list(records.itertuples(index=False))
    for i in range(0, len(recs), batch_size):
        chunk = recs[i:i + batch_size]
        vals = ",\n".join(
            f"  ({_q(r.scenario)},{_q(r.subject_kind)},{_q(r.subject_code)},{_q(r.source_variable)},"
            f"{_q(r.period)},{int(r.water_year)},{r.value:.4f},{_q(r.unit_code)})"
            for r in chunk
        )
        lines.append(f"INSERT INTO {VALUE_TABLE}")
        lines.append("  (scenario_short_code, data_in_depth_subject_id, source_variable, period, water_year, value, unit_id)")
        lines.append("SELECT v.scenario, s.id, v.source_variable, v.period, v.water_year, v.value, u.id")
        lines.append(f"FROM (VALUES\n{vals}\n) AS v(scenario, subject_kind, subject_code, source_variable, period, water_year, value, unit_code)")
        lines.append("JOIN data_in_depth_subject s ON s.subject_kind = v.subject_kind AND s.short_code = v.subject_code")
        lines.append("JOIN unit u ON u.short_code = v.unit_code")
        lines.append("ON CONFLICT (scenario_short_code, data_in_depth_subject_id, source_variable, period, water_year, unit_id)")
        lines.append("DO UPDATE SET value = EXCLUDED.value, updated_at = NOW(), updated_by = coeqwal_current_operator();")
        lines.append("")
    lines += ["COMMIT;", ""]
    return "\n".join(lines)


def _main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Extract annual water-year system-deliveries into data_in_depth_value SQL.")
    p.add_argument("--path", default=DEFAULT_PATH)
    p.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    p.add_argument("--scenarios", nargs="*", help="limit scenarios (default: all 115)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true", help="report summary; write nothing")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    tr = TrendReport.load(args.path)
    df = build_series(tr, scenarios=args.scenarios)

    log.info("value rows=%d | scenarios=%d | variables=%d | water_years=%d-%d",
             len(df), df["scenario"].nunique(), df["subject_code"].nunique(),
             int(df["water_year"].min()), int(df["water_year"].max()))

    if args.dry_run:
        log.info("dry-run: nothing written")
        print(df.head(6).to_string(index=False))
        return 0

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "system_deliveries_values.sql").write_text(generate_value_sql(df, args.batch_size))
    log.info("wrote system_deliveries_values.sql (%d rows)", len(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
