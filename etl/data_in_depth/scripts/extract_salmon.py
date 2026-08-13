"""extract_salmon.py - annual WRLCM adult-females rolling average -> data_in_depth_value.

Source: data/raw/salmon/TIERS_WRLCM.csv (single file, all scenarios/years),
columns used: scenario_w_Rs, year_cal, metric_avg_roll. Other columns
(scenario, scenario_hist_ref, scenario_hist_ref_w_Rs, reintroduction,
Hydroclimate, Tier_range, tier_score_cont) are NOT extracted - out of scope
per the single requested variable (metric_avg_roll); Hydroclimate is a fixed
per-scenario attribute (not a grain dimension - verified 1 value/scenario).


`-R` scenarios (e.g. `s0020-R`) are NOT YET in the master `scenario` table
(`data_in_depth_value.scenario_short_code` is a formal FK -> scenario
(short_code)), so per explicit user decision this extractor DROPS every row
whose `scenario_w_Rs` ends in `-R` for now - only baseline scenarios are
extracted. Once `-R` scenarios are added to the master scenario list, this
filter can simply be removed (no other code changes needed - `scenario_w_Rs`
is already the column being emitted). See open_issues.md #13.

Subject: WRLCM_ADULT_FEMALES (subject_kind='metric', no location - see
seed_data_in_depth_salmon_subjects.sql). Unit: NOF_3YR_AVG (new - "natural-
origin adult females, 3-year rolling average"; stored as given, no scaling
assumption). period='annual'. water_year = year_cal (calendar year, NOT
water-year Oct-Sep like the trend-report domains - this source's own year
index). No aggregates (single subject, nothing to sum).


Output -> etl/data_in_depth/output/salmon_values.sql (gitignored). Apply:
    psql "$DATABASE_URL" -f etl/data_in_depth/output/salmon_values.sql
Prereqs: create_data_in_depth_value_table.sql (adds unit NOF_3YR_AVG)
         + seed_data_in_depth_salmon_subjects.sql.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

try:
    from .trend_report import TrendReport
except ImportError:  # pragma: no cover
    from trend_report import TrendReport

log = logging.getLogger("extract_salmon")

SALMON_FILE = "data/raw/salmon/TIERS_WRLCM.csv"
PERIOD = "annual"
UNIT = "NOF_3YR_AVG"
SUBJECT_KIND = "metric"
SUBJECT_CODE = "WRLCM_ADULT_FEMALES"
SOURCE_VARIABLE = "METRIC_AVG_ROLL"
EXCLUDED_SCENARIOS = {"s0002"}
REINTRO_SUFFIX = "-R"
VALUE_TABLE = "data_in_depth_value"
DEFAULT_OUT = Path("etl/data_in_depth/output/salmon_values.sql")

COLS = {
    "scenario_w_Rs": "scenario",
    "year_cal": "water_year",
    "metric_avg_roll": "value",
}


def build_records(salmon_file: str = SALMON_FILE, scenarios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Long records: (scenario, subject_kind, subject_code, source_variable, period, water_year, value, unit_code).

    `scenario` here is `scenario_w_Rs` from the source (see module docstring) -
    reintroduction rows (suffix `-R`) are dropped since those scenarios aren't
    yet in the master scenario table.
    """
    data = pd.read_csv(salmon_file, usecols=list(COLS)).rename(columns=COLS)

    reintro_mask = data["scenario"].str.endswith(REINTRO_SUFFIX)
    n_reintro = int(reintro_mask.sum())
    if n_reintro:
        log.info("dropping %d reintroduction rows (scenario_w_Rs ending in %r) - "
                  "not yet in the master scenario table", n_reintro, REINTRO_SUFFIX)
        data = data[~reintro_mask]

    data = data[~data["scenario"].isin(EXCLUDED_SCENARIOS)]

    # Keep only the standard 115-scenario set (per TrendReport's catalog).
    # This source carries 4 non-standard scenarios (s0036/s0076/s0096/s0122)
    # not in that set - drop them (see module docstring; open_issues.md #4).
    standard_scenarios = set(TrendReport.load().scenarios)
    raw_scenarios = set(data["scenario"].unique())
    outliers = sorted(raw_scenarios - standard_scenarios)
    if outliers:
        log.warning("dropping %d non-standard scenario(s) not in the standard 115-scenario set: %s",
                    len(outliers), outliers)
        data = data[data["scenario"].isin(standard_scenarios)]

    if scenarios:
        data = data[data["scenario"].isin(set(scenarios))]

    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    n0 = len(data)
    data = data[data["value"].notna()]
    if n0 != len(data):
        log.warning("dropped %d rows with non-numeric/missing metric_avg_roll", n0 - len(data))

    records = pd.DataFrame({
        "scenario": data["scenario"].values,
        "subject_kind": SUBJECT_KIND,
        "subject_code": SUBJECT_CODE,
        "source_variable": SOURCE_VARIABLE,
        "period": PERIOD,
        "water_year": data["water_year"].astype(int).values,
        "value": data["value"].values,
        "unit_code": UNIT,
    })
    return records


def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def generate_value_sql(records: pd.DataFrame, batch_size: int = 5000) -> str:
    lines = [
        f"-- Salmon (WRLCM) annual adult-females rolling-average values for {VALUE_TABLE}",
        f"-- {len(records)} rows | generated by etl/data_in_depth/scripts/extract_salmon.py",
        "\\set ON_ERROR_STOP on", "BEGIN;", "",
    ]
    recs = list(records.itertuples(index=False))
    for i in range(0, len(recs), batch_size):
        chunk = recs[i:i + batch_size]
        vals = ",\n".join(
            f"  ({_q(r.scenario)},{_q(r.subject_kind)},{_q(r.subject_code)},{_q(r.source_variable)},"
            f"{_q(r.period)},{int(r.water_year)},{r.value:.6f},{_q(r.unit_code)})"
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
    p = argparse.ArgumentParser(description="Extract annual WRLCM salmon adult-females rolling average into data_in_depth_value SQL.")
    p.add_argument("--salmon-file", default=SALMON_FILE)
    p.add_argument("--out", default=str(DEFAULT_OUT))
    p.add_argument("--scenarios", nargs="*", help="limit scenarios (default: all in the standard 115-scenario set)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true", help="report summary; write nothing")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    recs = build_records(args.salmon_file, scenarios=args.scenarios)
    log.info("rows=%d | scenarios=%d | source_variable=%s | years=%d-%d",
             len(recs), recs["scenario"].nunique(), SOURCE_VARIABLE,
             int(recs["water_year"].min()), int(recs["water_year"].max()))

    if args.dry_run:
        log.info("dry-run: nothing written")
        print(recs.head(6).to_string(index=False))
        return 0

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(generate_value_sql(recs, args.batch_size))
    log.info("wrote %s (%d rows)", out, len(recs))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
