"""extract_groundwater.py - annual groundwater storage volume & level -> data_in_depth_value.

Source: two WIDE annual CSVs, one row per water_year (1921-2021), one column
per (entity, scenario):
  * data/raw/ground_water/GroundWater_Volumes_Annual.csv  (42 WBAs x 115 scenarios,
    no s0002 column)
  * data/raw/ground_water/GroundWater_Levels_Annual.csv   (42 WBAs x 116 scenarios,
    INCLUDES s0002 - excluded here like every other extract)
Column format: "<entity>_<scenario>", e.g. "WBA2_s0011". Verified the two files
carry the exact same 42-entity set (WBA2-WBA26S, WBA50, WBA60N/S, WBA61-64,
WBA71-73, WBA90, DETAW); bare "WBA" is NOT a column in either file and is
deliberately not seeded (see seed_data_in_depth_groundwater_subjects.sql).

Emits two measures per (scenario, WBA, year) ENTITY:
  * GW_STOR   : volume, source values assumed AF -> converted to TAF (/1000,
                same data_in_depth convention as ag/CWS). ASSUMPTION, not
                confirmed against a units metadata file - see open_issues.md.
  * GW_LEVEL  : level, unit ft, taken AS-IS from the source (no conversion -
                these are water-table elevations, not a volume).

NOD_GroundwaterStorage/SOD_GroundwaterStorage AGGREGATES get GW_STOR ONLY
(summed across members, unambiguous - a volume). GW_LEVEL is NOT aggregated:
summing water-table elevations across WBAs is physically meaningless, and an
unweighted mean would imply an area/capacity weighting this data doesn't
support. Decided 2026-07-28.

Subjects are groundwater WBAs (location_type='wba'); rows land via a join on
data_in_depth_subject.short_code, so every WBA code in the data must be seeded
(see seed_data_in_depth_groundwater_subjects.sql) or its rows are dropped.

Output -> etl/data_in_depth/output/groundwater_values.sql (gitignored). Apply:
    psql "$DATABASE_URL" -f etl/data_in_depth/output/groundwater_values.sql
Prereqs: create_data_in_depth_value_table.sql (adds unit 'ft')
         + seed_data_in_depth_groundwater_subjects.sql.
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

log = logging.getLogger("extract_groundwater")

VOLUMES_FILE = "data/raw/ground_water/GroundWater_Volumes_Annual.csv"
LEVELS_FILE = "data/raw/ground_water/GroundWater_Levels_Annual.csv"
PERIOD = "annual"
UNIT_VOLUME = "TAF"
UNIT_LEVEL = "ft"
AF_TO_TAF = 1000.0
SRC_VOLUME = "GW_STOR"
SRC_LEVEL = "GW_LEVEL"
EXCLUDED_SCENARIOS = {"s0002"}
VALUE_TABLE = "data_in_depth_value"
DEFAULT_OUT = Path("etl/data_in_depth/output/groundwater_values.sql")

COL_RE = re.compile(r"^(?P<entity>.+)_(?P<scenario>s\d{4})$")

# WBA -> NOD/SOD grouping for the NOD_GroundwaterStorage / SOD_GroundwaterStorage
# aggregates (30 NOD, 12 SOD; validated against seed_data_in_depth_groundwater_subjects.sql).
GW_AGG_MAP = {
    "WBA2": "NOD", "WBA3": "NOD", "WBA4": "NOD", "WBA5": "NOD", "WBA6": "NOD",
    "WBA7N": "NOD", "WBA7S": "NOD", "WBA8N": "NOD", "WBA8S": "NOD", "WBA9": "NOD",
    "WBA10": "NOD", "WBA11": "NOD", "WBA12": "NOD", "WBA13": "NOD", "WBA14": "NOD",
    "WBA15N": "NOD", "WBA15S": "NOD", "WBA16": "NOD", "WBA17N": "NOD", "WBA17S": "NOD",
    "WBA18": "NOD", "WBA19": "NOD", "WBA20": "NOD", "WBA21": "NOD", "WBA22": "NOD",
    "WBA23": "NOD", "WBA24": "NOD", "WBA25": "NOD", "WBA26N": "NOD", "WBA26S": "NOD",
    "WBA60N": "SOD", "DETAW": "SOD", "WBA50": "SOD", "WBA60S": "SOD", "WBA61": "SOD",
    "WBA62": "SOD", "WBA63": "SOD", "WBA64": "SOD", "WBA71": "SOD", "WBA72": "SOD",
    "WBA73": "SOD", "WBA90": "SOD",
}
AGG_CODE = {"NOD": "NOD_GroundwaterStorage", "SOD": "SOD_GroundwaterStorage"}


def _melt_wide(path: str, value_name: str) -> pd.DataFrame:
    """Wide (water_year x "<entity>_<scenario>") -> long (scenario, entity, water_year, value)."""
    df = pd.read_csv(path)
    df = df.rename(columns={df.columns[0]: "water_year"})
    long = df.melt(id_vars="water_year", var_name="col", value_name=value_name)
    parsed = long["col"].str.extract(COL_RE)
    long["entity"] = parsed["entity"]
    long["scenario"] = parsed["scenario"]
    bad = long["entity"].isna() | long["scenario"].isna()
    if bad.any():
        raise ValueError(f"{path}: {bad.sum()} column(s) didn't match '<entity>_sNNNN': "
                          f"{sorted(long.loc[bad, 'col'].unique())}")
    return long[["scenario", "entity", "water_year", value_name]]


def build_records(volumes_file: str = VOLUMES_FILE, levels_file: str = LEVELS_FILE,
                   scenarios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Long records: (scenario, subject_kind, subject_code, source_variable, period, water_year, value, unit_code)."""
    vol = _melt_wide(volumes_file, "volume_af")
    lvl = _melt_wide(levels_file, "level_ft")

    vol = vol[~vol["scenario"].isin(EXCLUDED_SCENARIOS)]
    lvl = lvl[~lvl["scenario"].isin(EXCLUDED_SCENARIOS)]
    if scenarios:
        vol = vol[vol["scenario"].isin(set(scenarios))]
        lvl = lvl[lvl["scenario"].isin(set(scenarios))]

    vol["volume_taf"] = pd.to_numeric(vol["volume_af"], errors="coerce") / AF_TO_TAF
    lvl["level_ft"] = pd.to_numeric(lvl["level_ft"], errors="coerce")

    cols = ["scenario", "subject_kind", "subject_code", "source_variable",
            "period", "water_year", "value", "unit_code"]

    vol_ent = pd.DataFrame({
        "scenario": vol["scenario"].values,
        "subject_kind": "entity",
        "subject_code": vol["entity"].values,
        "source_variable": SRC_VOLUME,
        "period": PERIOD,
        "water_year": vol["water_year"].astype(int).values,
        "value": vol["volume_taf"].values,
        "unit_code": UNIT_VOLUME,
    })
    lvl_ent = pd.DataFrame({
        "scenario": lvl["scenario"].values,
        "subject_kind": "entity",
        "subject_code": lvl["entity"].values,
        "source_variable": SRC_LEVEL,
        "period": PERIOD,
        "water_year": lvl["water_year"].astype(int).values,
        "value": lvl["level_ft"].values,
        "unit_code": UNIT_LEVEL,
    })
    ent = pd.concat([vol_ent[cols], lvl_ent[cols]], ignore_index=True)

    unmapped = set(vol["entity"]) - set(GW_AGG_MAP)
    if unmapped:
        log.warning("WBA not in NOD/SOD map (excluded from aggregates): %s", sorted(unmapped))

    # NOD_GroundwaterStorage / SOD_GroundwaterStorage = sum of member GW_STOR
    # volumes per scenario-year. GW_LEVEL is deliberately NOT aggregated - see
    # module docstring.
    a = vol.copy()
    a["agg"] = a["entity"].map(GW_AGG_MAP)
    a = a[a["agg"].notna()]
    grp = a.groupby(["scenario", "agg", "water_year"], as_index=False)["volume_taf"].sum()

    agg = pd.DataFrame({
        "scenario": grp["scenario"].values,
        "subject_kind": "aggregate",
        "subject_code": grp["agg"].map(AGG_CODE).values,
        "source_variable": SRC_VOLUME,
        "period": PERIOD,
        "water_year": grp["water_year"].astype(int).values,
        "value": grp["volume_taf"].values,
        "unit_code": UNIT_VOLUME,
    })
    return pd.concat([ent, agg[cols]], ignore_index=True)


def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def generate_value_sql(records: pd.DataFrame, batch_size: int = 5000) -> str:
    lines = [
        f"-- Groundwater annual storage volume & level values for {VALUE_TABLE}",
        f"-- {len(records)} rows | generated by etl/data_in_depth/scripts/extract_groundwater.py",
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
    p = argparse.ArgumentParser(description="Extract annual groundwater storage volume/level into data_in_depth_value SQL.")
    p.add_argument("--volumes-file", default=VOLUMES_FILE)
    p.add_argument("--levels-file", default=LEVELS_FILE)
    p.add_argument("--out", default=str(DEFAULT_OUT))
    p.add_argument("--scenarios", nargs="*", help="limit scenarios (default: all)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true", help="report summary; write nothing")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    recs = build_records(args.volumes_file, args.levels_file, scenarios=args.scenarios)
    log.info("rows=%d | scenarios=%d | wbas=%d | measures=%s | years=%d-%d",
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
