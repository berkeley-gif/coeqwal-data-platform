"""extract_cws.py - annual CWS delivery/percent-demand-met/welfare -> data_in_depth_value.

Two independent sources feed this extractor:

1. data/raw/cws/<scenario>_demand_met_by_year.csv (one file per scenario,
   already annual), columns: scenario_id, dwuc, year, annual_delivery,
   annual_demand, percent_demand_met.

   Emits two measures per (scenario, dwuc, year) ENTITY:
     * CWS_DELIVERY        : annual_delivery, unit TAF
     * CWS_PCT_DEMAND_MET  : percent_demand_met, taken directly from the
                             source (NOT derived here), unit PCT_DEMAND_MET

   NOD_CWS/SOD_CWS AGGREGATES get both measures: CWS_DELIVERY (summed) and
   CWS_PCT_DEMAND_MET (demand-weighted, sum(delivery)/sum(demand)*100, capped
   at 100 to match the entity-row convention - see the note in
   build_records() and etl/data_in_depth/open_issues.md #5).

   annual_demand is read from source but not persisted as its own series. A
   dedicated static per-entity demand table (demand doesn't vary by
   scenario) is a TODO; see etl/data_in_depth/open_issues.md.

2. data/raw/cws/DUs_allscs_welfare_outcomes.xlsx (single file, all
   scenarios/years/DUs), columns used: Scenario, Demand Unit, Water Year,
   Shortage Total, Shortage Percent, welfare_loss_allscs_capped_1pctQ_USD.

   Emits three measures per (scenario, Demand Unit, year), ENTITY and
   NOD_CWS/SOD_CWS AGGREGATE (aggregation added 2026-08-06 once a NOD/SOD
   mapping existed for this source's DUs - see WELFARE_AGG_MAP):
     * CWS_WELFARE_LOSS   : welfare_loss_allscs_capped_1pctQ_USD, unit USD
                            (aggregate: summed across members)
     * CWS_SHORTAGE_TOTAL : Shortage Total, unit TAF
                            (aggregate: summed across members)
     * CWS_SHORTAGE_PCT   : Shortage Percent, unit PCT_SHORTAGE (NaN
                             backfilled to 0.0 when Shortage Total is exactly
                             0 - confirmed exact 1:1 correspondence)
                            (aggregate: demand-weighted
                             sum(shortage_total)/sum(supply_total+shortage_total)*100,
                             never exceeds 100 since both terms are
                             non-negative - no capping needed, unlike
                             pct_demand_met)
   Water years 2022-2030 are dropped (single-scenario artifact, not part of
   the normal multi-scenario timeseries). See cws_extract_decisions.md for
   the full rationale on every decision in this source.

Subjects (both sources) are CWS demand units (location_type='demand_unit');
rows land via a join on data_in_depth_subject.short_code, so every dwuc/DU
code must be seeded (see seed_data_in_depth_cws_subjects.sql) or its rows are
dropped.

Output -> etl/data_in_depth/output/cws_values.sql (gitignored). Apply:
    psql "$DATABASE_URL" -f etl/data_in_depth/output/cws_values.sql
Prereqs: create_data_in_depth_value_table.sql + seed_data_in_depth_cws_subjects.sql.
Reading the .xlsx source requires openpyxl (pip install openpyxl) - not a
hard repo dependency, same convention as etl/tier_data's optional xlsx path.
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

log = logging.getLogger("extract_cws")

CWS_DIR = "data/raw/cws"
PERIOD = "annual"
UNIT_DELIVERY = "TAF"
UNIT_PCT_MET = "PCT_DEMAND_MET"
SRC_DELIVERY = "CWS_DELIVERY"
SRC_PCT_MET = "CWS_PCT_DEMAND_MET"
EXCLUDED_SCENARIOS = {"s0002"}
VALUE_TABLE = "data_in_depth_value"
DEFAULT_OUT = Path("etl/data_in_depth/output/cws_values.sql")

# --- welfare-outcomes source (added 2026-08-05; see cws_extract_decisions.md) ---
WELFARE_FILE = "data/raw/cws/DUs_allscs_welfare_outcomes.xlsx"
WELFARE_MAX_WATER_YEAR = 2021  # 2022-2030 exist only for a single scenario - drop
UNIT_SHORTAGE_PCT = "PCT_SHORTAGE"
UNIT_WELFARE = "USD"
SRC_WELFARE_LOSS = "CWS_WELFARE_LOSS"
SRC_SHORTAGE_TOTAL = "CWS_SHORTAGE_TOTAL"
SRC_SHORTAGE_PCT = "CWS_SHORTAGE_PCT"
WELFARE_COLS = {
    "Scenario": "scenario",
    "Demand Unit": "du",
    "Water Year": "water_year",
    "Shortage Total": "shortage_total",
    "Shortage Percent": "shortage_pct",
    "Supply Total": "supply_total",  # not persisted - only used to weight the shortage_pct aggregate
    "welfare_loss_allscs_capped_1pctQ_USD": "welfare_loss",
}
WELFARE_MEASURES = (
    ("welfare_loss", SRC_WELFARE_LOSS, UNIT_WELFARE),
    ("shortage_total", SRC_SHORTAGE_TOTAL, UNIT_DELIVERY),
    ("shortage_pct", SRC_SHORTAGE_PCT, UNIT_SHORTAGE_PCT),
)

# dwuc -> NOD/SOD grouping for the NOD_CWS / SOD_CWS aggregates.
CWS_AGG_MAP = {
    "02_PU": "NOD", "02_SU": "NOD", "03_PU1": "NOD", "03_PU2": "NOD", "03_SU": "NOD",
    "11_NU1": "NOD", "12_NU1": "NOD", "13_NU1": "NOD", "16_PU": "NOD", "20_NU1": "NOD",
    "21_PU": "NOD", "24_NU1": "NOD", "24_NU2": "NOD", "24_NU3": "NOD", "25_PU": "NOD",
    "26N_NU1": "NOD", "26N_NU2": "NOD", "26N_NU3": "NOD", "26N_PU1": "NOD", "26N_PU2": "NOD",
    "26N_PU3": "NOD", "26S_NU1": "NOD", "26S_PU1": "NOD", "26S_PU2": "NOD", "26S_PU4": "NOD",
    "26S_PU5": "NOD", "26S_PU6": "NOD", "50_PU": "NOD", "60N_NU2": "NOD", "60S_NU1": "SOD",
    "61_NU2": "SOD", "90_PU": "SOD", "ACFC": "SOD", "AMADR": "NOD", "AMCYN": "NOD",
    "ANTOC": "SOD", "BNCIA": "NOD", "CCWD": "SOD", "CSB038": "SOD", "CSB103": "SOD",
    "CSPSO": "NOD", "EBMUD": "SOD", "ELDID_NU1": "NOD", "ELDID_NU2": "NOD", "ELDID_NU3": "NOD",
    "ESB324": "SOD", "ESB347": "SOD", "ESB355": "SOD", "ESB414": "SOD", "ESB420": "SOD",
    "FRFLD": "SOD", "GDPUD_NU": "NOD", "GRSVL": "NOD", "JLIND": "NOD", "KCWA": "SOD",
    "MHILL_NU": "NOD", "MWD": "SOD", "NAPA": "NOD", "NAPA2": "NOD", "PCWA3": "NOD",
    "PLMAS": "NOD", "SBA029": "SOD", "SBA036": "SOD", "SBCWD": "SOD", "SCVWD": "SOD",
    "SUISN": "NOD", "SVWRD": "SOD", "TLMNE": "NOD", "TVAFB": "NOD", "UNION": "NOD",
    "UPANG": "NOD", "VLLJO": "NOD", "WLDWD": "NOD", "WSB032": "SOD",
}
AGG_CODE = {"NOD": "NOD_CWS", "SOD": "SOD_CWS"}

# DU -> NOD/SOD grouping for the welfare-outcomes source's NOD_CWS/SOD_CWS
# aggregates (welfare_loss/shortage_total/shortage_pct). Covers all 63 DUs in
# the welfare file: the 32 that overlap CWS_AGG_MAP use the SAME assignment
# (verified identical there), plus 31 welfare-only codes (29 newly-seeded DUs
# + 62_NU/72_PU, which exist in the original 77 but were never assigned
# NOD/SOD in CWS_AGG_MAP) - mapping supplied by the user 2026-08-06, 51 NOD /
# 12 SOD, validated against seed_data_in_depth_cws_subjects.sql.
WELFARE_AGG_MAP = {
    "02_PU": "NOD", "02_SU": "NOD", "03_PU1": "NOD", "03_PU2": "NOD", "03_SU": "NOD",
    "11_NU1": "NOD", "12_NU1": "NOD", "13_NU1": "NOD", "16_PU": "NOD", "20_NU1": "NOD",
    "21_PU": "NOD", "24_NU1": "NOD", "24_NU2": "NOD", "24_NU3": "NOD", "25_PU": "NOD",
    "26N_NU1": "NOD", "26N_NU2": "NOD", "26N_NU3": "NOD", "26N_PU1": "NOD", "26N_PU2": "NOD",
    "26N_PU3": "NOD", "26S_NU1": "NOD", "26S_PU1": "NOD", "26S_PU2": "NOD", "26S_PU4": "NOD",
    "26S_PU5": "NOD", "26S_PU6": "NOD", "50_PU": "NOD", "60N_NU2": "NOD", "60S_NU1": "SOD",
    "61_NU2": "SOD", "90_PU": "SOD",
    "02_NU": "NOD", "03_PU3": "NOD", "04_NU1": "NOD", "04_NU2": "NOD", "05_NU": "NOD",
    "06_NU": "NOD", "07N_NU": "NOD", "07S_NU": "NOD", "08N_NU": "NOD", "08S_NU": "NOD",
    "10_NU1": "NOD", "11_NU2": "NOD", "13_NU2": "NOD", "15N_NU": "NOD", "15S_NU": "NOD",
    "17S_NU": "NOD", "20_NU2": "NOD", "25_NU": "NOD", "26N_NU4": "NOD", "26N_NU5": "NOD",
    "26S_NU2": "NOD", "26S_NU3": "NOD",
    "60N_NU1": "SOD", "61_NU1": "SOD", "61_NU3": "SOD", "62_NU": "SOD", "63_NU": "SOD",
    "64_NU": "SOD", "71_NU": "SOD", "72_NU": "SOD", "72_PU": "SOD",
}


def build_records(cws_dir: str = CWS_DIR, scenarios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Long records: (scenario, subject_code, source_variable, period, water_year, value, unit_code)."""
    frames = []
    for f in sorted(glob.glob(os.path.join(cws_dir, "*.csv"))):
        df = pd.read_csv(f)
        df.columns = [c.strip().lower() for c in df.columns]
        frames.append(df)
    if not frames:
        raise ValueError(f"no CSVs found in {cws_dir}")
    data = pd.concat(frames, ignore_index=True)

    # Source-data gap: some files carry rows with scenario_id='NA' (pandas -> NaN)
    # and null delivery/demand for a few contractors (ACFC, CSB038, ESB355, KCWA,
    # WSB032) in ~9 scenarios. Drop them (can't insert nan / no scenario).
    n0 = len(data)
    data = data[data["scenario_id"].notna()]
    dropped_scen = n0 - len(data)

    data = data[~data["scenario_id"].isin(EXCLUDED_SCENARIOS)]
    if scenarios:
        data = data[data["scenario_id"].isin(set(scenarios))]

    data["annual_delivery"] = pd.to_numeric(data["annual_delivery"], errors="coerce")
    data["annual_demand"] = pd.to_numeric(data["annual_demand"], errors="coerce")
    data["percent_demand_met"] = pd.to_numeric(data["percent_demand_met"], errors="coerce")
    n1 = len(data)
    data = data[data["annual_delivery"].notna() & data["annual_demand"].notna()
                & data["percent_demand_met"].notna()]
    dropped_val = n1 - len(data)
    if dropped_scen or dropped_val:
        log.warning("dropped %d NA-scenario rows and %d null-value rows (source data gaps)",
                    dropped_scen, dropped_val)

    base = pd.DataFrame({
        "scenario": data["scenario_id"].values,
        "subject_kind": "entity",
        "subject_code": data["dwuc"].values,
        "period": PERIOD,
        "water_year": data["year"].astype(int).values,
    })
    dely = base.copy()
    dely["source_variable"] = SRC_DELIVERY
    dely["unit_code"] = UNIT_DELIVERY
    dely["value"] = data["annual_delivery"].values
    pctm = base.copy()
    pctm["source_variable"] = SRC_PCT_MET
    pctm["unit_code"] = UNIT_PCT_MET
    pctm["value"] = data["percent_demand_met"].values
    cols = ["scenario", "subject_kind", "subject_code", "source_variable",
            "period", "water_year", "value", "unit_code"]
    ent = pd.concat([dely[cols], pctm[cols]], ignore_index=True)

    unmapped = set(data["dwuc"]) - set(CWS_AGG_MAP)
    if unmapped:
        log.warning("dwuc not in NOD/SOD map (excluded from aggregates): %s", sorted(unmapped))

    # NOD_CWS / SOD_CWS delivery = sum of member deliveries per scenario-year.
    # Percent-demand-met = demand-weighted sum(delivery)/sum(demand)*100,
    # capped at 100 to match the entity-row convention (source data caps
    # percent_demand_met at 100 even though raw delivery can exceed demand for
    # individual contractors - uncapped this hit 163% for NOD_CWS/s0011/1921).
    # Decided 2026-07-24 - see open_issues.md #5.
    #
    # Members whose source rows were dropped (NA data) are simply excluded from
    # both sums -> in the ~9 scenarios where a SOD contractor (ACFC/CSB038/
    # ESB355/KCWA/WSB032) has no data, SOD_CWS delivery and percent-demand-met
    # are both computed from the remaining members only (no flag; consistent
    # with how delivery already behaved) - see open_issues.md #4.
    a = data.copy()
    a["agg"] = a["dwuc"].map(CWS_AGG_MAP)
    a = a[a["agg"].notna()]
    grp = a.groupby(["scenario_id", "agg", "year"], as_index=False)[
        ["annual_delivery", "annual_demand"]].sum()

    agg_dely = pd.DataFrame({
        "scenario": grp["scenario_id"].values,
        "subject_kind": "aggregate",
        "subject_code": grp["agg"].map(AGG_CODE).values,
        "source_variable": SRC_DELIVERY,
        "period": PERIOD,
        "water_year": grp["year"].astype(int).values,
        "value": grp["annual_delivery"].values,
        "unit_code": UNIT_DELIVERY,
    })
    agg_pctm = pd.DataFrame({
        "scenario": grp["scenario_id"].values,
        "subject_kind": "aggregate",
        "subject_code": grp["agg"].map(AGG_CODE).values,
        "source_variable": SRC_PCT_MET,
        "period": PERIOD,
        "water_year": grp["year"].astype(int).values,
        "value": (grp["annual_delivery"] / grp["annual_demand"] * 100).clip(upper=100).values,
        "unit_code": UNIT_PCT_MET,
    })
    return pd.concat([ent, agg_dely[cols], agg_pctm[cols]], ignore_index=True)


def build_welfare_records(welfare_file: str = WELFARE_FILE,
                           scenarios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Long records from the welfare-outcomes source: welfare_loss, shortage_total,
    shortage_pct - both ENTITY and NOD_CWS/SOD_CWS AGGREGATE rows (added
    2026-08-06; see cws_extract_decisions.md and WELFARE_AGG_MAP above).

    welfare_loss/shortage_total are extensive (dollars/TAF) - summed across
    members, unambiguous. shortage_pct is demand-weighted:
    sum(shortage_total) / sum(supply_total + shortage_total) * 100 - unlike
    pct_demand_met this can never exceed 100 (shortage and supply are both
    non-negative), so no capping is needed.
    """
    try:
        import openpyxl  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "reading the CWS welfare-outcomes .xlsx requires openpyxl: pip install openpyxl"
        ) from e

    data = pd.read_excel(welfare_file, usecols=list(WELFARE_COLS)).rename(columns=WELFARE_COLS)

    # 2022-2030 exist only for a single scenario (s0133), one row per DU (not
    # per-scenario) - a source artifact, not part of the normal timeseries.
    data = data[data["water_year"] <= WELFARE_MAX_WATER_YEAR]

    data = data[~data["scenario"].isin(EXCLUDED_SCENARIOS)]
    if scenarios:
        data = data[data["scenario"].isin(set(scenarios))]

    for col, _, _ in WELFARE_MEASURES:
        data[col] = pd.to_numeric(data[col], errors="coerce")
    data["supply_total"] = pd.to_numeric(data["supply_total"], errors="coerce")

    # Shortage Percent is blank (not 0) whenever Shortage Total is exactly 0 -
    # confirmed exact 1:1 correspondence in the core range. Backfill to 0.0
    # (physically correct: no shortage = 0% shortage) rather than drop, so
    # exceedance/statistics aren't thinned for DUs with perfect delivery.
    zero_shortage = data["shortage_total"] == 0
    data.loc[zero_shortage & data["shortage_pct"].isna(), "shortage_pct"] = 0.0
    unexplained_null = data["shortage_pct"].isna() & ~zero_shortage
    if unexplained_null.any():
        log.warning("%d shortage_pct rows are null without shortage_total==0 (unexpected, left null)",
                    int(unexplained_null.sum()))

    base = pd.DataFrame({
        "scenario": data["scenario"].values,
        "subject_kind": "entity",
        "subject_code": data["du"].values,
        "period": PERIOD,
        "water_year": data["water_year"].astype(int).values,
    })
    cols = ["scenario", "subject_kind", "subject_code", "source_variable",
            "period", "water_year", "value", "unit_code"]
    frames = []
    for col, src, unit in WELFARE_MEASURES:
        f = base.copy()
        f["source_variable"] = src
        f["value"] = data[col].values
        f["unit_code"] = unit
        frames.append(f[cols])
    ent = pd.concat(frames, ignore_index=True)
    ent = ent[ent["value"].notna()]

    unmapped = set(data["du"]) - set(WELFARE_AGG_MAP)
    if unmapped:
        log.warning("welfare DU not in NOD/SOD map (excluded from aggregates): %s", sorted(unmapped))

    # NOD_CWS / SOD_CWS welfare_loss/shortage_total = sum of member values per
    # scenario-year. shortage_pct = demand-weighted
    # sum(shortage_total)/sum(supply_total+shortage_total)*100 - can't exceed
    # 100 since both terms are non-negative, so no capping needed (unlike
    # pct_demand_met). See module/function docstrings.
    a = data.copy()
    a["agg"] = a["du"].map(WELFARE_AGG_MAP)
    a = a[a["agg"].notna()]
    grp = a.groupby(["scenario", "agg", "water_year"], as_index=False)[
        ["welfare_loss", "shortage_total", "supply_total"]].sum()

    agg_frames = []
    for col, src, unit in (("welfare_loss", SRC_WELFARE_LOSS, UNIT_WELFARE),
                            ("shortage_total", SRC_SHORTAGE_TOTAL, UNIT_DELIVERY)):
        f = pd.DataFrame({
            "scenario": grp["scenario"].values,
            "subject_kind": "aggregate",
            "subject_code": grp["agg"].map(AGG_CODE).values,
            "source_variable": src,
            "period": PERIOD,
            "water_year": grp["water_year"].astype(int).values,
            "value": grp[col].values,
            "unit_code": unit,
        })
        agg_frames.append(f[cols])
    agg_pct = pd.DataFrame({
        "scenario": grp["scenario"].values,
        "subject_kind": "aggregate",
        "subject_code": grp["agg"].map(AGG_CODE).values,
        "source_variable": SRC_SHORTAGE_PCT,
        "period": PERIOD,
        "water_year": grp["water_year"].astype(int).values,
        "value": (grp["shortage_total"] / (grp["supply_total"] + grp["shortage_total"]) * 100).values,
        "unit_code": UNIT_SHORTAGE_PCT,
    })
    agg_frames.append(agg_pct[cols])

    records = pd.concat([ent, *agg_frames], ignore_index=True)
    return records[records["value"].notna()]


def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def generate_value_sql(records: pd.DataFrame, batch_size: int = 5000) -> str:
    lines = [
        f"-- CWS annual delivery/percent-demand-met/welfare-outcome values for {VALUE_TABLE}",
        f"-- {len(records)} rows | generated by etl/data_in_depth/scripts/extract_cws.py",
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
    p = argparse.ArgumentParser(description="Extract annual CWS delivery/percent-demand-met/welfare-outcome into data_in_depth_value SQL.")
    p.add_argument("--cws-dir", default=CWS_DIR)
    p.add_argument("--welfare-file", default=WELFARE_FILE)
    p.add_argument("--skip-welfare", action="store_true", help="omit the welfare-outcomes source")
    p.add_argument("--out", default=str(DEFAULT_OUT))
    p.add_argument("--scenarios", nargs="*", help="limit scenarios (default: all)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true", help="report summary; write nothing")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    recs = build_records(args.cws_dir, scenarios=args.scenarios)
    if not args.skip_welfare:
        welfare_recs = build_welfare_records(args.welfare_file, scenarios=args.scenarios)
        recs = pd.concat([recs, welfare_recs], ignore_index=True)
    log.info("rows=%d | scenarios=%d | dwuc=%d | measures=%s | years=%d-%d",
             len(recs), recs["scenario"].nunique(), recs["subject_code"].nunique(),
             sorted(recs["source_variable"].unique()),
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
