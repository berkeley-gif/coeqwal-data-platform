"""trend_report_parser.py - Low-level parser for the CalSim trend-report DSS export.

The source file (``data/raw/trend_report_variables_v5.csv``, ~566 MB, 46,176
columns) is a DSS-pathname dump. Its first 7 rows are the DSS pathname parts,
not a normal header:

    row 0 (idx 0)  A-part   -> "CALSIM"                     (dropped)
    row 1 (idx 1)  B-part   -> "<VARIABLE>_<scenario>"      (the column key)
    row 2 (idx 2)  C-part   -> data type e.g. "FLOW"        (kept as metadata)
    row 3 (idx 3)  D-part   -> interval "1MON"              (dropped)
    row 4 (idx 4)  E-part   -> "L2020A"                     (dropped)
    row 5 (idx 5)  F-part   -> "PER-AVER"                   (dropped)
    row 6 (idx 6)  Units    -> "TAF" / "CFS" / "KM" / ...   (kept; disambiguator)
    row 7+ (idx 7) data     -> col 0 is the date, rest are values

Column 0 of every row is a label column (``A``/``B``/.../``Units``/date).

Each B-part encodes two dimensions: ``AWOANN_64_XADV_s0002`` -> variable
``AWOANN_64_XADV`` + scenario ``s0002``. Flow variables appear TWICE with the
same B-part, differing only by Units (CFS vs TAF), so the unique column key is
``(variable, scenario, unit)``.

This module does two things and nothing else (no DB, no domain logic):
  * ``build_catalog(path)``  - reads only the 7 preamble rows -> a small
    metadata DataFrame, one row per data column (the discovery index).
  * ``load_values(path, ...)`` - reads the data rows, optionally only the
    columns you ask for (``usecols``), into a wide DataFrame with a
    ``(variable, scenario, unit)`` MultiIndex and a DatetimeIndex.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Union

import pandas as pd

# --- file layout constants -------------------------------------------------
LABEL_COL = 0            # column 0 holds A/B/.../Units then the date
B_ROW = 1                # B-part row (variable_scenario headers)
C_ROW = 2                # C-part row (data type)
UNIT_ROW = 6             # Units row
DATA_START_ROW = 7       # first row of actual data (0-based)

_SCENARIO_RE = re.compile(r"^(?P<variable>.+)_(?P<scenario>s\d+)$")
_UNIT_DEDUP_RE = re.compile(r"\.\d+$")   # strip pandas-style "CFS.1" -> "CFS"


def parse_b_part(b_part: str) -> tuple[str, Optional[str]]:
    """Split a B-part into ``(variable, scenario)``.

    ``AWOANN_64_XADV_s0002`` -> ``("AWOANN_64_XADV", "s0002")``. The variable
    itself contains underscores, so we anchor on the trailing ``_s<digits>``.
    Returns ``(b_part, None)`` if there is no scenario suffix.
    """
    m = _SCENARIO_RE.match(b_part.strip())
    if not m:
        return b_part.strip(), None
    return m.group("variable"), m.group("scenario")


def normalize_unit(unit: str) -> str:
    """Canonicalize a raw Units cell.

    ``CFS.1`` -> ``CFS`` (dedup artifact), ``UMHOS/CM`` -> ``UMHOS_CM``.
    """
    u = (unit or "").strip()
    u = _UNIT_DEDUP_RE.sub("", u)
    return u.upper().replace("/", "_")


def build_catalog(path: Union[str, Path]) -> pd.DataFrame:
    """Parse the 7 preamble rows into a column catalog (fast; ignores file size).

    Returns one row per data column with:
      col_pos   - integer column index in the file (>=1); used for ``usecols``
      b_part    - raw B-part
      variable  - B-part minus the scenario suffix
      scenario  - the ``s<digits>`` token (or None)
      unit      - normalized Units value
      unit_raw  - Units value as written in the file
      c_part    - DSS C-part (data type), retained for validation/reference
    """
    path = Path(path)
    with path.open(newline="") as f:
        reader = csv.reader(f)
        preamble = [next(reader) for _ in range(DATA_START_ROW)]

    b_parts = preamble[B_ROW]
    c_parts = preamble[C_ROW]
    units = preamble[UNIT_ROW]

    rows = []
    for pos in range(1, len(b_parts)):          # skip label col 0
        b = b_parts[pos]
        variable, scenario = parse_b_part(b)
        raw_unit = units[pos] if pos < len(units) else ""
        rows.append(
            {
                "col_pos": pos,
                "b_part": b,
                "variable": variable,
                "scenario": scenario,
                "unit": normalize_unit(raw_unit),
                "unit_raw": raw_unit,
                "c_part": c_parts[pos] if pos < len(c_parts) else "",
            }
        )
    return pd.DataFrame(rows)


def load_values(
    path: Union[str, Path],
    catalog: Optional[pd.DataFrame] = None,
    col_positions: Optional[Sequence[int]] = None,
    dtype: str = "float32",
) -> pd.DataFrame:
    """Load data rows into a wide DataFrame with a (variable, scenario, unit) MultiIndex.

    Parameters
    ----------
    catalog
        Result of ``build_catalog``; built on demand if omitted.
    col_positions
        File column indices to load (from ``catalog['col_pos']``). If None,
        loads ALL data columns (~46k cols, ~200 MB at float32 - prefer a
        subset). The date column (0) is always included.
    dtype
        Value dtype; ``float32`` halves memory vs the pandas default.

    The index is a DatetimeIndex named ``date``; columns are a MultiIndex
    ``(variable, scenario, unit)``.
    """
    path = Path(path)
    if catalog is None:
        catalog = build_catalog(path)

    if col_positions is None:
        value_positions = list(catalog["col_pos"])
    else:
        value_positions = sorted(set(int(p) for p in col_positions))

    usecols = [LABEL_COL] + value_positions
    # read_csv preserves file order for usecols regardless of listing order,
    # so the loaded value columns come back in ascending position order.
    df = pd.read_csv(
        path,
        skiprows=DATA_START_ROW,
        header=None,
        usecols=usecols,
        index_col=LABEL_COL,
        dtype={p: dtype for p in value_positions},
        low_memory=False,
    )
    df.index = pd.to_datetime(df.index)
    df.index.name = "date"

    cat_by_pos = catalog.set_index("col_pos")
    ordered = cat_by_pos.loc[value_positions]
    df.columns = pd.MultiIndex.from_arrays(
        [ordered["variable"].to_numpy(), ordered["scenario"].to_numpy(), ordered["unit"].to_numpy()],
        names=["variable", "scenario", "unit"],
    )
    return df


def variables_from_file(path: Union[str, Path]) -> List[str]:
    """Read a newline-delimited variable list (blank lines and ``#`` comments ignored).

    Convenience for maintaining the <200-variable subset lists (reservoirs,
    river_flows, ...) as plain text files.
    """
    out: List[str] = []
    for line in Path(path).read_text().splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def resolve_columns(
    catalog: pd.DataFrame,
    variables: Optional[Iterable[str]] = None,
    scenarios: Optional[Iterable[str]] = None,
    unit: Optional[str] = None,
    prefer_unit: Optional[str] = "TAF",
    unit_preference: Sequence[str] = ("TAF", "CFS", "KM", "UMHOS_CM", "NONE"),
) -> pd.DataFrame:
    """Filter the catalog to the columns implied by a selection.

    variables / scenarios : keep only these (None = all).
    unit                  : keep only this exact unit (overrides prefer_unit).
    prefer_unit           : when a (variable, scenario) has multiple units and
                            no explicit ``unit`` is given, keep one row using
                            ``unit_preference`` (default: TAF first). Set to
                            None to keep every unit.
    Returns the filtered/collapsed catalog subset (still keyed by col_pos).
    """
    sub = catalog
    if variables is not None:
        sub = sub[sub["variable"].isin(set(variables))]
    if scenarios is not None:
        sub = sub[sub["scenario"].isin(set(scenarios))]
    if unit is not None:
        return sub[sub["unit"] == normalize_unit(unit)].copy()
    if prefer_unit is None:
        return sub.copy()

    # collapse to one preferred unit per (variable, scenario)
    order = list(dict.fromkeys([normalize_unit(prefer_unit), *unit_preference]))
    rank = {u: i for i, u in enumerate(order)}
    sub = sub.copy()
    sub["_rank"] = sub["unit"].map(lambda u: rank.get(u, len(rank)))
    keep_idx = (
        sub.sort_values("_rank")
        .groupby(["variable", "scenario"], sort=False)
        .head(1)
        .index
    )
    return sub.loc[keep_idx].drop(columns="_rank")
