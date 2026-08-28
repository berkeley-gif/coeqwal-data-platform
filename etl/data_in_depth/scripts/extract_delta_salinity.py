"""extract_delta_salinity.py - April & September Delta X2 -> data_in_depth_value.

Pulls `X2_PRV` (Delta 2-psu isohaline position, km) for every scenario from
its own dedicated DSS-export CSV, samples the April and September value of
each water year, and emits raw per-year rows for `data_in_depth_value`.

**REVISED (2026-08-28):** no longer reads from the shared `trend_report.py` /
`TrendReport` catalog (`trend_report_variables_v5.csv`). X2 now has its own
standalone source file, `data/raw/delta_salinity/X2_PRV.csv` - same DSS-export
CSV shape (`A/B/C/E/F/Type/Units` header rows, then one row per month, one
column per scenario), just scoped to this single variable, so this extractor
parses it directly instead of going through the shared multi-variable
catalog. Verified the new file's scenario set is IDENTICAL to the old trend-
report catalog's: 116 columns = the same 115-scenario set + `s0002`, nothing
added or missing.

The source's own variable name changed too: the old trend-report catalog's
b-part was `X2_PRV_KM_sNNNN`; this file's is `X2_PRV_sNNNN` (no `_KM`). Per
explicit user decision the DB-side identity changes to match: `source_variable`
is now `X2_PRV` (was `X2_PRV_KM`). The `X2` SUBJECT is unchanged - only the
source_variable identity moved. Because that's part of `data_in_depth_value`'s
unique key, existing `X2_PRV_KM`-tagged rows will NOT be upserted over by this
change - they'll sit alongside the new `X2_PRV` rows as orphans. Clean them up
explicitly (`DELETE FROM data_in_depth_value WHERE source_variable =
'X2_PRV_KM'`) once the new extract has been applied. The API's
`DELTA_SOURCE_VARS` (`data_in_depth_endpoints.py`) was updated to `["X2_PRV"]`
to match.

- Subject: X2 (subject_kind='metric', no location).
- period = 'april' / 'sept'; unit = 'km'; one row per (scenario, period, water_year).
- water_year = calendar year of the sampled month (April/Sep of year Y are in WY Y).
- No aggregates. Everything derived (exceedance, box, mean, CV) is computed live
  by the API.

Unit note: the source labels the unit 'KM', but the DB `unit` lookup
short_code is lowercase 'km'. We read the 'KM' column but EMIT 'km' so the
generated SQL's `JOIN unit ON short_code` resolves.

REINTRODUCTION (-R) SCENARIOS: s0020-R and friends (see reintro_scenarios.py)
are duplicated verbatim from their base scenario's rows (X2 is a CalSim-only
variable, unaffected by the WRLCM reintroduction toggle) - see
:mod:`reintro_scenarios` and open_issues.md #14.

Output -> etl/data_in_depth/output/delta_salinity_values.sql (gitignored). Apply:
    psql "$DATABASE_URL" -f etl/data_in_depth/output/delta_salinity_values.sql
Prereqs: create_data_in_depth_value_table.sql + seed_data_in_depth_delta_subjects.sql applied.
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
from pathlib import Path
from typing import List, Optional, Sequence

import pandas as pd

try:
    from .reintro_scenarios import duplicate_reintro_scenarios
except ImportError:  # pragma: no cover
    from reintro_scenarios import duplicate_reintro_scenarios

log = logging.getLogger("extract_delta_salinity")

DELTA_FILE = "data/raw/delta_salinity/X2_PRV.csv"
SOURCE_VARIABLE = "X2_PRV"      # source file b-part prefix AND the stored source_variable (renamed 2026-08-28, was X2_PRV_KM)
SUBJECT_CODE = "X2"
SUBJECT_KIND = "metric"
PERIODS = {"april": 4, "sept": 9}   # period label -> calendar month
UNIT_SOURCE = "KM"               # unit as labelled in the source file's header
UNIT_DB = "km"                   # unit short_code in the DB lookup (lowercase)
EXCLUDED_SCENARIOS = {"s0002"}
HEADER_ROWS = 7                  # A, B, C, E, F, Type, Units - data starts on row 8
VALUE_TABLE = "data_in_depth_value"
DEFAULT_OUT_DIR = Path("etl/data_in_depth/output")

_SCEN_COL_RE = re.compile(rf"^{re.escape(SOURCE_VARIABLE)}_(s\d{{4}})$")


def _read_header(path: str) -> List[str]:
    """Read the DSS-export header block; return the ordered scenario list.

    Also validates the Units row is uniformly the expected unit - there's no
    TrendReport-style `unit=` filter doing this for us anymore, so it's
    checked explicitly here instead.
    """
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = {row[0]: row[1:] for row in (next(reader) for _ in range(HEADER_ROWS))}

    units = set(header["Units"])
    if units != {UNIT_SOURCE}:
        raise ValueError(f"{path}: expected Units row to be all {UNIT_SOURCE!r}, got {units}")

    scenarios = []
    for col in header["B"]:
        m = _SCEN_COL_RE.match(col)
        if not m:
            raise ValueError(f"{path}: unexpected column header {col!r} (expected {SOURCE_VARIABLE}_sNNNN)")
        scenarios.append(m.group(1))
    return scenarios


def build_series(delta_file: str = DELTA_FILE, scenarios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """April/September X2 (km) per (scenario, period, water_year)."""
    scen_cols = _read_header(delta_file)
    wide = pd.read_csv(delta_file, skiprows=HEADER_ROWS, header=None)
    wide.columns = ["date"] + scen_cols
    wide["date"] = pd.to_datetime(wide["date"])

    long = wide.melt(id_vars="date", var_name="scenario", value_name="value")
    long = long[~long["scenario"].isin(EXCLUDED_SCENARIOS)]
    if scenarios:
        long = long[long["scenario"].isin(set(scenarios))]

    long = long[long["date"].dt.month.isin(PERIODS.values())].copy()
    month_to_period = {m: p for p, m in PERIODS.items()}
    long["period"] = long["date"].dt.month.map(month_to_period)
    long["water_year"] = long["date"].dt.year
    long["subject_kind"] = SUBJECT_KIND
    long["subject_code"] = SUBJECT_CODE
    long["source_variable"] = SOURCE_VARIABLE
    long["value"] = pd.to_numeric(long["value"], errors="coerce").astype(float)
    long["unit_code"] = UNIT_DB
    return long[["scenario", "subject_kind", "subject_code", "source_variable",
                 "period", "water_year", "value", "unit_code"]].reset_index(drop=True)


def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def generate_value_sql(records: pd.DataFrame, batch_size: int = 5000) -> str:
    """Batched INSERT ... SELECT FROM (VALUES ...) upserts; ids resolved by join."""
    lines = [
        f"-- Delta salinity X2 (April & September, km) values for {VALUE_TABLE}",
        f"-- {len(records)} rows | generated by etl/data_in_depth/scripts/extract_delta_salinity.py",
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
    p = argparse.ArgumentParser(description="Extract April/September Delta X2 into data_in_depth_value SQL.")
    p.add_argument("--delta-file", default=DELTA_FILE)
    p.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    p.add_argument("--scenarios", nargs="*", help="limit scenarios (default: all 115, s0002 excluded)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true", help="report summary; write nothing")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    df = build_series(args.delta_file, scenarios=args.scenarios)
    df = duplicate_reintro_scenarios(df)

    log.info("value rows=%d | scenarios=%d | periods=%s | water_years=%d-%d",
             len(df), df["scenario"].nunique(), sorted(df["period"].unique()),
             int(df["water_year"].min()), int(df["water_year"].max()))

    if args.dry_run:
        log.info("dry-run: nothing written")
        print(df.head(6).to_string(index=False))
        return 0

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "delta_salinity_values.sql").write_text(generate_value_sql(df, args.batch_size))
    log.info("wrote delta_salinity_values.sql (%d rows)", len(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
