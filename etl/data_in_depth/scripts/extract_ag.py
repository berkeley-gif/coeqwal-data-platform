"""extract_ag.py - annual ag demand-unit net diversion/GW pumping/shortage/revenue -> data_in_depth_value.

Source: data/raw/ag/revenuesc_shortage_by_demand_unit.csv (single file, all
scenarios/years/demand-units), columns: Year, Demand Unit, Scenario,
Net Diversion (AF), Groundwater Pumping Restrictred (AF) [sic, source typo],
Shortage (AF), Annual Revenue ($). Already annual (one row per DU/scenario/
water_year) - no monthly aggregation needed. The three volume columns are in
AF; converted to TAF here (data_in_depth convention) by dividing by 1000.
Revenue is NOT a volume - kept in USD, no conversion.

Emits FOUR measures per (scenario, demand unit, year) ENTITY, straight from
the source columns (no derived shortage calc - Shortage (AF) is already a
source column, not delivery-minus-demand):
  * AG_NET_DIVERSION : Net Diversion (AF) / 1000, unit TAF
  * AG_GW_PUMPING    : Groundwater Pumping Restrictred (AF) / 1000, unit TAF
  * AG_SHORTAGE      : Shortage (AF) / 1000, unit TAF
  * AG_REVENUE       : Annual Revenue ($), unit USD (added 2026-07-29 - initially
                        scoped out as "not a water-volume measure", but the
                        generic value table doesn't require a volume, so it
                        fits fine alongside PCT_CAP/PCT_DEMAND_MET/ft)

NOD_Agriculture/SOD_Agriculture AGGREGATES get all four measures, each summed
across members - unlike CWS's percent-demand-met, these are all extensive
(volumes + dollars) so summing is unambiguous (no capping/weighting question).

Subjects are ag demand units (location_type='ag_demand_unit'); rows land via a
join on data_in_depth_subject.short_code, so every Demand Unit code in the
data must be seeded (see seed_data_in_depth_ag_subjects.sql) or its rows are
dropped.

Output -> etl/data_in_depth/output/ag_values.sql (gitignored). Apply:
    psql "$DATABASE_URL" -f etl/data_in_depth/output/ag_values.sql
Prereqs: create_data_in_depth_value_table.sql + seed_data_in_depth_ag_subjects.sql
         + alter_data_in_depth_subject_add_ag_demand_unit.sql.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

log = logging.getLogger("extract_ag")

AG_FILE = "data/raw/ag/revenuesc_shortage_by_demand_unit.csv"
PERIOD = "annual"
UNIT_VOLUME = "TAF"
UNIT_REVENUE = "USD"
AF_TO_TAF = 1000.0
SRC_NET_DIVERSION = "AG_NET_DIVERSION"
SRC_GW_PUMPING = "AG_GW_PUMPING"
SRC_SHORTAGE = "AG_SHORTAGE"
SRC_REVENUE = "AG_REVENUE"
EXCLUDED_SCENARIOS = {"s0002"}
VALUE_TABLE = "data_in_depth_value"
DEFAULT_OUT = Path("etl/data_in_depth/output/ag_values.sql")

COLS = {
    "Year": "water_year",
    "Demand Unit": "du",
    "Scenario": "scenario",
    "Net Diversion (AF)": "net_diversion",
    "Groundwater Pumping Restrictred (AF)": "gw_pumping",
    "Shortage (AF)": "shortage",
    "Annual Revenue ($)": "revenue",
}

# Demand unit -> NOD/SOD grouping for the NOD_Agriculture / SOD_Agriculture
# aggregates (73 NOD, 59 SOD; validated against seed_data_in_depth_ag_subjects.sql).
AG_AGG_MAP = {
    "02_NA": "NOD", "02_PA": "NOD", "02_SA": "NOD", "03_NA": "NOD", "03_PA": "NOD",
    "03_SA": "NOD", "04_NA": "NOD", "04_PA1": "NOD", "04_PA2": "NOD", "05_NA": "NOD",
    "06_NA": "NOD", "06_PA": "NOD", "07N_NA": "NOD", "07N_PA": "NOD", "07S_NA": "NOD",
    "07S_PA": "NOD", "08N_NA": "NOD", "08N_PA": "NOD", "08N_SA1": "NOD", "08N_SA2": "NOD",
    "08S_NA1": "NOD", "08S_NA2": "NOD", "08S_PA": "NOD", "08S_SA1": "NOD", "08S_SA2": "NOD",
    "08S_SA3": "NOD", "09_NA": "NOD", "09_SA1": "NOD", "09_SA2": "NOD", "10_NA": "NOD",
    "11_NA": "NOD", "11_SA1": "NOD", "11_SA2": "NOD", "11_SA3": "NOD", "11_SA4": "NOD",
    "12_NA": "NOD", "12_SA": "NOD", "13_NA": "NOD", "14_NA": "NOD", "15N_NA1": "NOD",
    "15N_NA2": "NOD", "15N_SA": "NOD", "15S_NA1": "NOD", "15S_NA2": "NOD", "15S_SA": "NOD",
    "16_NA1": "NOD", "16_NA2": "NOD", "16_PA": "NOD", "16_SA": "NOD", "17N_NA": "NOD",
    "17S_NA": "NOD", "17S_SA": "NOD", "18_NA": "NOD", "18_SA": "NOD", "19_SA": "NOD",
    "20_NA1": "NOD", "20_NA2": "NOD", "20_PA": "NOD", "21_NA": "NOD", "21_PA": "NOD",
    "21_SA": "NOD", "22_NA": "NOD", "22_SA1": "NOD", "22_SA2": "NOD", "23_NA": "NOD",
    "24_NA1": "NOD", "24_NA2": "NOD", "24_NA3": "NOD", "25_NA": "NOD", "25_PA1": "NOD",
    "25_PA2": "NOD", "26N_NA": "NOD", "26S_NA": "NOD",
    "50_PA1": "SOD", "50_PA2": "SOD", "60N_NA1": "SOD", "60N_NA2": "SOD", "60N_NA3": "SOD",
    "60N_NA4": "SOD", "60N_NA5": "SOD", "60S_NA1": "SOD", "60S_NA2": "SOD", "60S_PA1": "SOD",
    "60S_PA2": "SOD", "61_NA1": "SOD", "61_NA2": "SOD", "61_NA3": "SOD", "61_NA4": "SOD",
    "61_NA5": "SOD", "61_NA6": "SOD", "61_PA1": "SOD", "61_PA2": "SOD", "61_PA3": "SOD",
    "62_NA1": "SOD", "62_NA2": "SOD", "62_NA3": "SOD", "62_NA4": "SOD", "62_NA5": "SOD",
    "62_NA6": "SOD", "63_NA1": "SOD", "63_NA2": "SOD", "63_NA3": "SOD", "63_NA4": "SOD",
    "64_NA1": "SOD", "64_NA2": "SOD", "64_PA1": "SOD", "64_PA2": "SOD", "64_PA3": "SOD",
    "64_XA": "SOD", "71_NA1": "SOD", "71_NA2": "SOD", "71_PA1": "SOD", "71_PA2": "SOD",
    "71_PA3": "SOD", "71_PA4": "SOD", "71_PA5": "SOD", "71_PA6": "SOD", "71_PA7": "SOD",
    "71_PA8": "SOD", "72_NA1": "SOD", "72_NA2": "SOD", "72_PA": "SOD", "72_XA1": "SOD",
    "72_XA2": "SOD", "72_XA3": "SOD", "73_NA": "SOD", "73_PA1": "SOD", "73_PA2": "SOD",
    "73_PA3": "SOD", "73_XA": "SOD", "90_PA1": "SOD", "90_PA2": "SOD",
}
AGG_CODE = {"NOD": "NOD_Agriculture", "SOD": "SOD_Agriculture"}
# (source column, source_variable, unit)
MEASURES = (
    ("net_diversion", SRC_NET_DIVERSION, UNIT_VOLUME),
    ("gw_pumping", SRC_GW_PUMPING, UNIT_VOLUME),
    ("shortage", SRC_SHORTAGE, UNIT_VOLUME),
    ("revenue", SRC_REVENUE, UNIT_REVENUE),
)


def build_records(ag_file: str = AG_FILE, scenarios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Long records: (scenario, subject_kind, subject_code, source_variable, period, water_year, value, unit_code)."""
    data = pd.read_csv(ag_file, usecols=list(COLS)).rename(columns=COLS)

    data = data[~data["scenario"].isin(EXCLUDED_SCENARIOS)]
    if scenarios:
        data = data[data["scenario"].isin(set(scenarios))]

    for col, _, unit in MEASURES:
        data[col] = pd.to_numeric(data[col], errors="coerce")
        if unit == UNIT_VOLUME:
            data[col] = data[col] / AF_TO_TAF

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
    for col, src, unit in MEASURES:
        f = base.copy()
        f["source_variable"] = src
        f["value"] = data[col].values
        f["unit_code"] = unit
        frames.append(f[cols])
    ent = pd.concat(frames, ignore_index=True)

    unmapped = set(data["du"]) - set(AG_AGG_MAP)
    if unmapped:
        log.warning("demand unit not in NOD/SOD map (excluded from aggregates): %s", sorted(unmapped))

    # NOD_Agriculture / SOD_Agriculture = sum of member values per scenario-year,
    # one sum per measure. All four measures are extensive (volumes + dollars),
    # so summing is unambiguous (unlike CWS's percent-demand-met).
    a = data.copy()
    a["agg"] = a["du"].map(AG_AGG_MAP)
    a = a[a["agg"].notna()]
    grp = a.groupby(["scenario", "agg", "water_year"], as_index=False)[
        [c for c, _, _ in MEASURES]].sum()

    agg_frames = []
    for col, src, unit in MEASURES:
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
    return pd.concat([ent, *agg_frames], ignore_index=True)


def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def generate_value_sql(records: pd.DataFrame, batch_size: int = 5000) -> str:
    lines = [
        f"-- Ag annual net diversion / GW pumping / shortage values for {VALUE_TABLE}",
        f"-- {len(records)} rows | generated by etl/data_in_depth/scripts/extract_ag.py",
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
    p = argparse.ArgumentParser(description="Extract annual ag net diversion/GW pumping/shortage into data_in_depth_value SQL.")
    p.add_argument("--ag-file", default=AG_FILE)
    p.add_argument("--out", default=str(DEFAULT_OUT))
    p.add_argument("--scenarios", nargs="*", help="limit scenarios (default: all)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true", help="report summary; write nothing")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    recs = build_records(args.ag_file, scenarios=args.scenarios)
    log.info("rows=%d | scenarios=%d | demand_units=%d | measures=%s | years=%d-%d",
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
