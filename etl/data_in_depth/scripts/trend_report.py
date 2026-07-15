"""trend_report.py - Accessor facade over the CalSim trend-report DSS export.

Wraps :mod:`trend_report_parser` in a ``TrendReport`` object so callers can
discover and pull subsets without touching the file layout. The intended flow:

    from trend_report import TrendReport

    tr = TrendReport.load("data/raw/trend_report_variables_v5.csv")   # catalog only (fast)
    tr.scenarios            # -> ['s0002', 's0011', ...]
    tr.variables            # -> ['AWOANN_64_XADV', 'C_AMR004', 'S_SHSTA', ...]

    # pull an explicit <200-variable subset (TAF preferred where available):
    reservoirs = tr.select(variables=my_reservoir_vars, scenarios=["s0002"])
    ts = tr.series("S_SHSTA", "s0002")           # a single time series
    tidy = tr.to_long(variables=my_reservoir_vars)   # [date, variable, scenario, unit, value]

Design: a *wide* DataFrame (DatetimeIndex x (variable, scenario, unit)
MultiIndex) backed by a small *catalog* used to decide which columns to read.
Subsets are defined by explicit variable lists (no prefix classification);
``select``/``load_values`` read only the needed columns via ``usecols``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Union

import pandas as pd

try:  # works both as a package import and a same-dir script
    from .trend_report_parser import build_catalog, load_values, resolve_columns, variables_from_file
except ImportError:  # pragma: no cover
    from trend_report_parser import build_catalog, load_values, resolve_columns, variables_from_file

log = logging.getLogger("trend_report")

DEFAULT_PATH = "data/raw/trend_report_variables_v5.csv"

# s0002 is the first scenario in the file and is omitted from ALL analysis
# extracts (it's excluded by project decision). The file has 116 scenarios;
# after exclusion every extraction sees 115. Pass exclude_scenarios=None to
# TrendReport.load to see the raw set.
EXCLUDED_SCENARIOS = frozenset({"s0002"})


class TrendReport:
    """Catalog + on-demand wide values for the trend-report export.

    By default s0002 is dropped from the catalog at load, so `scenarios`,
    `select`, `series`, and `to_long` all operate on the 115-scenario set.
    """

    def __init__(self, path: Union[str, Path], catalog: pd.DataFrame):
        self.path = Path(path)
        self.catalog = catalog

    # --- construction ------------------------------------------------------
    @classmethod
    def load(
        cls,
        path: Union[str, Path] = DEFAULT_PATH,
        exclude_scenarios: Optional[Iterable[str]] = EXCLUDED_SCENARIOS,
    ) -> "TrendReport":
        """Build the column catalog only (reads 7 rows; instant even at 566 MB).

        `exclude_scenarios` is dropped from the catalog so every downstream
        selection ignores it (default: {'s0002'} -> 115 scenarios). Pass None
        to keep the full 116-scenario set.
        """
        catalog = build_catalog(path)
        if exclude_scenarios:
            catalog = (
                catalog[~catalog["scenario"].isin(set(exclude_scenarios))]
                .reset_index(drop=True)
            )
        return cls(path, catalog)

    # --- discovery ---------------------------------------------------------
    @property
    def variables(self) -> List[str]:
        return sorted(self.catalog["variable"].unique())

    @property
    def scenarios(self) -> List[str]:
        return sorted(v for v in self.catalog["scenario"].dropna().unique())

    @property
    def units(self) -> List[str]:
        return sorted(self.catalog["unit"].unique())

    def find(self, pattern: str) -> List[str]:
        """Case-insensitive substring/regex search over variable names."""
        mask = self.catalog["variable"].str.contains(pattern, case=False, regex=True)
        return sorted(self.catalog.loc[mask, "variable"].unique())

    # --- selection ---------------------------------------------------------
    def select(
        self,
        variables: Optional[Iterable[str]] = None,
        scenarios: Optional[Iterable[str]] = None,
        unit: Optional[str] = None,
        prefer_unit: Optional[str] = "TAF",
        dtype: str = "float32",
    ) -> pd.DataFrame:
        """Return a wide DataFrame for the requested subset.

        variables   : explicit list (recommended; subsets are <200 vars).
        scenarios   : explicit list (None = all loaded, i.e. 115 after s0002 exclusion).
        unit        : force a single unit (e.g. "CFS"); overrides prefer_unit.
        prefer_unit : when both units exist for a (variable, scenario), keep
                      this one (default TAF). None keeps every unit.
        Reads only the matching columns from disk.
        """
        sub = resolve_columns(
            self.catalog, variables=variables, scenarios=scenarios,
            unit=unit, prefer_unit=prefer_unit,
        )
        if sub.empty:
            return pd.DataFrame()
        return load_values(self.path, catalog=self.catalog,
                           col_positions=sub["col_pos"], dtype=dtype)

    def series(
        self,
        variable: str,
        scenario: str,
        unit: Optional[str] = None,
        prefer_unit: Optional[str] = "TAF",
    ) -> pd.Series:
        """Return a single (variable, scenario) time series as a Series."""
        df = self.select(variables=[variable], scenarios=[scenario],
                         unit=unit, prefer_unit=prefer_unit)
        if df.empty:
            raise KeyError(f"No column for variable={variable!r} scenario={scenario!r} unit={unit!r}")
        s = df.iloc[:, 0]
        s.name = df.columns[0]  # (variable, scenario, unit) tuple
        return s

    def to_long(
        self,
        variables: Optional[Iterable[str]] = None,
        scenarios: Optional[Iterable[str]] = None,
        unit: Optional[str] = None,
        prefer_unit: Optional[str] = "TAF",
        dropna: bool = True,
    ) -> pd.DataFrame:
        """Tidy [date, variable, scenario, unit, value] frame for a subset.

        Materialize the long form only for a subset - the whole file long is
        ~55M+ rows.
        """
        wide = self.select(variables=variables, scenarios=scenarios,
                          unit=unit, prefer_unit=prefer_unit)
        if wide.empty:
            return pd.DataFrame(columns=["date", "variable", "scenario", "unit", "value"])
        long = (
            wide.stack(["variable", "scenario", "unit"], future_stack=True)
            .rename("value")
            .reset_index()
        )
        if dropna:
            long = long.dropna(subset=["value"])
        return long


# --- CLI: quick inspection -------------------------------------------------
def _main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Inspect / subset the CalSim trend-report export.")
    p.add_argument("path", nargs="?", default=DEFAULT_PATH, help=f"CSV path (default: {DEFAULT_PATH})")
    p.add_argument("--summary", action="store_true", help="print column/variable/scenario/unit counts")
    p.add_argument("--list-variables", action="store_true")
    p.add_argument("--list-scenarios", action="store_true")
    p.add_argument("--grep", help="filter --list-variables by pattern")
    p.add_argument("--dump", action="store_true", help="load a subset and write it out")
    p.add_argument("--variables", nargs="*", help="explicit variable list for --dump")
    p.add_argument("--variables-file", help="newline-delimited variable list file for --dump")
    p.add_argument("--scenarios", nargs="*", help="scenario list for --dump (default: all)")
    p.add_argument("--unit", help="force a single unit for --dump")
    p.add_argument("--prefer-unit", default="TAF", help="preferred unit when both exist (default TAF)")
    p.add_argument("--out", help="output path for --dump (.parquet or .csv)")
    p.add_argument("--long", action="store_true", help="--dump in tidy long form")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    tr = TrendReport.load(args.path)

    if args.summary or not (args.list_variables or args.list_scenarios or args.dump):
        c = tr.catalog
        print(f"file:       {args.path}")
        print(f"columns:    {len(c)}")
        print(f"variables:  {c['variable'].nunique()}")
        print(f"scenarios:  {c['scenario'].nunique()}")
        print(f"units:      {', '.join(tr.units)}")
        dupes = c['b_part'].duplicated().sum()
        print(f"b-parts with a unit-dup column: {dupes}")

    if args.list_scenarios:
        print("\n".join(tr.scenarios))
    if args.list_variables:
        vs = tr.find(args.grep) if args.grep else tr.variables
        print("\n".join(vs))

    if args.dump:
        variables = list(args.variables or [])
        if args.variables_file:
            variables += variables_from_file(args.variables_file)
        if not variables:
            p.error("--dump requires --variables and/or --variables-file")
        if args.long:
            data = tr.to_long(variables=variables, scenarios=args.scenarios,
                              unit=args.unit, prefer_unit=args.prefer_unit)
        else:
            data = tr.select(variables=variables, scenarios=args.scenarios,
                             unit=args.unit, prefer_unit=args.prefer_unit)
        log.info("subset shape: %s", data.shape)
        if args.out:
            out = Path(args.out)
            if out.suffix == ".parquet":
                data.to_parquet(out)
            else:
                data.to_csv(out)
            log.info("wrote %s", out)
        else:
            print(data.head())
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
