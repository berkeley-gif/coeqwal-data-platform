"""extract_reservoir_storage.py - April & September reservoir storage -> data_in_depth.

Pulls `S_<code>` storage for the 8 tier reservoirs from the trend-report export
(via TrendReport; s0002 already excluded -> 115 scenarios), samples April and
September of each water year, and emits raw per-year rows for
`data_in_depth_value`, each stored in TWO units: volume (TAF) and
percent-of-capacity (PCT_CAP).

Only RAW per-year values are written. Everything population-dependent
(exceedance percentiles, mean, CV, box-plot quantiles) is computed LIVE by the
API at query time so it stays correct under WYT filtering — nothing derived is
stored. (PCT_CAP is a per-row transform, not a population statistic, so it is
safe to store.)

Aggregates NOD_Reservoirs (Trinity+Shasta+Oroville+Folsom) and SOD_Reservoirs
(San Luis CVP+SWP+New Melones+Millerton) are summed across members at ETL time;
their percent-of-capacity uses the summed member capacities.

Output -> etl/data_in_depth/output/reservoir_storage_values.sql (gitignored).
Apply with:
    psql "$DATABASE_URL" -f etl/data_in_depth/output/reservoir_storage_values.sql
Prereqs: create_data_in_depth_subject_table.sql, create_data_in_depth_value_table.sql,
and seed_data_in_depth_subjects.sql applied.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd

try:
    from .trend_report import TrendReport, DEFAULT_PATH
except ImportError:  # pragma: no cover
    from trend_report import TrendReport, DEFAULT_PATH

log = logging.getLogger("extract_reservoir_storage")

# Authoritative storage capacities (TAF), keyed by reservoir short_code.
CAPACITY_TAF: Dict[str, float] = {
    "TRNTY": 2447.7, "SHSTA": 4552.0, "OROVL": 3424.8, "FOLSM": 967.0,
    "SLUIS_CVP": 972.0, "SLUIS_SWP": 1067.0, "MELON": 2420.0, "MLRTN": 524.0,
}
RESERVOIRS: List[str] = list(CAPACITY_TAF)
# Aggregate short_codes are domain-qualified (…_Reservoirs) because NOD/SOD
# recur across segments; the subject_code must match the seeded subject.
AGGREGATES: Dict[str, List[str]] = {
    "NOD_Reservoirs": ["TRNTY", "SHSTA", "OROVL", "FOLSM"],
    "SOD_Reservoirs": ["SLUIS_CVP", "SLUIS_SWP", "MELON", "MLRTN"],
}
PERIODS: Dict[str, int] = {"april": 4, "sept": 9}   # period label -> calendar month

UNIT_TAF, UNIT_PCT = "TAF", "PCT_CAP"
VALUE_TABLE = "data_in_depth_value"
DEFAULT_OUT_DIR = Path("etl/data_in_depth/output")


# --- extraction ------------------------------------------------------------
def _entity_volumes(tr: TrendReport, scenarios: Optional[Sequence[str]]) -> pd.DataFrame:
    """Long frame of April/Sept storage per (scenario, reservoir, period, year)."""
    svars = [f"S_{c}" for c in RESERVOIRS]
    long = tr.to_long(variables=svars, scenarios=scenarios, unit=UNIT_TAF, dropna=True)
    if long.empty:
        raise ValueError("No S_<reservoir> storage rows found")
    df = long[long["date"].dt.month.isin(PERIODS.values())].copy()
    month_to_period = {m: p for p, m in PERIODS.items()}
    df["period"] = df["date"].dt.month.map(month_to_period)
    df["water_year"] = df["date"].dt.year
    df["subject_kind"] = "entity"
    df["subject_code"] = df["variable"].str[2:]            # strip 'S_'
    df["source_variable"] = df["variable"]
    df["volume_taf"] = df["value"].astype(float)
    df["capacity_taf"] = df["subject_code"].map(CAPACITY_TAF)
    return df[["scenario", "subject_kind", "subject_code", "source_variable",
              "period", "water_year", "volume_taf", "capacity_taf"]]


def _aggregate_volumes(ent: pd.DataFrame) -> pd.DataFrame:
    """Sum member reservoirs into NOD/SOD (only years where all members present)."""
    out: List[pd.DataFrame] = []
    for agg, members in AGGREGATES.items():
        sub = ent[ent["subject_code"].isin(members)]
        g = (sub.groupby(["scenario", "period", "water_year"])["volume_taf"]
                .agg(total="sum", n="count").reset_index())
        g = g[g["n"] == len(members)].copy()               # require all members
        g["subject_kind"] = "aggregate"
        g["subject_code"] = agg
        g["source_variable"] = f"S_{agg}"                  # derived sum, documented
        g["volume_taf"] = g["total"]
        g["capacity_taf"] = float(sum(CAPACITY_TAF[m] for m in members))
        out.append(g[["scenario", "subject_kind", "subject_code", "source_variable",
                      "period", "water_year", "volume_taf", "capacity_taf"]])
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def build_series(tr: TrendReport, scenarios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Full per-year frame (entities + aggregates) with volume and pct-of-capacity."""
    ent = _entity_volumes(tr, scenarios)
    agg = _aggregate_volumes(ent)
    df = pd.concat([ent, agg], ignore_index=True)
    df["pct_capacity"] = df["volume_taf"] / df["capacity_taf"] * 100.0
    return df.reset_index(drop=True)


def value_records(df: pd.DataFrame) -> pd.DataFrame:
    """Two rows per year: (TAF, volume) and (PCT_CAP, pct-of-capacity)."""
    base = ["scenario", "subject_kind", "subject_code", "source_variable", "period", "water_year"]
    taf = df[base].copy(); taf["value"] = df["volume_taf"]; taf["unit_code"] = UNIT_TAF
    pct = df[base].copy(); pct["value"] = df["pct_capacity"]; pct["unit_code"] = UNIT_PCT
    return pd.concat([taf, pct], ignore_index=True)


# --- SQL generation --------------------------------------------------------
def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def generate_value_sql(records: pd.DataFrame, batch_size: int = 5000) -> str:
    """Batched INSERT ... SELECT FROM (VALUES ...) upserts; ids resolved by join."""
    lines = [
        f"-- Reservoir storage (April & September) values for {VALUE_TABLE}",
        f"-- {len(records)} rows | generated by etl/data_in_depth/scripts/extract_reservoir_storage.py",
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
    p = argparse.ArgumentParser(description="Extract April/September reservoir storage into data_in_depth_value SQL.")
    p.add_argument("--path", default=DEFAULT_PATH)
    p.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    p.add_argument("--scenarios", nargs="*", help="limit scenarios (default: all 115)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true", help="report summary; write nothing")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    tr = TrendReport.load(args.path)
    df = build_series(tr, scenarios=args.scenarios)
    vrecs = value_records(df)

    log.info("series rows=%d | value rows=%d | scenarios=%d | subjects=%d | years=%d-%d",
             len(df), len(vrecs), df["scenario"].nunique(),
             df["subject_code"].nunique(), int(df["water_year"].min()), int(df["water_year"].max()))
    log.info("subjects: %s", sorted(df["subject_code"].unique()))

    if args.dry_run:
        log.info("dry-run: nothing written")
        print(df.head(6).to_string(index=False))
        return 0

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "reservoir_storage_values.sql").write_text(generate_value_sql(vrecs, args.batch_size))
    log.info("wrote reservoir_storage_values.sql (%d rows)", len(vrecs))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
