"""data_in_depth_stats.py - pure statistics for data_in_depth series.

No FastAPI / asyncpg / numpy dependencies — stdlib only — so it is independently
unit-testable and adds nothing to the API image. The endpoint fetches raw rows
(SQL does the filter/join) and hands them here; every derived value is computed
LIVE over whatever population the request selected, so it stays correct under
WYT filtering.

Methods (pinned):
- quantiles: linear interpolation, type-7 (matches numpy `linear` and Postgres
  `PERCENTILE_CONT`).
- box whiskers: Tukey 1.5*IQR; points beyond the fences are outliers.
- CV: sample stdev (ddof=1) / mean; None when n < 2 or mean == 0.
- exceedance: Weibull plotting position P = m/(n+1)*100, DESCENDING rank
  (m=1 = largest value = lowest exceedance %); ties broken by water_year asc.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

INCLUDE_ALL: Tuple[str, ...] = ("values", "exceedance", "box", "statistics")

_ROUND = 4


def _r(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), _ROUND)


def quantile(sorted_vals: Sequence[float], q: float) -> float:
    """Type-7 linear-interpolation quantile. `sorted_vals` ascending, q in [0,1]."""
    n = len(sorted_vals)
    if n == 0:
        raise ValueError("quantile of empty sequence")
    if n == 1:
        return float(sorted_vals[0])
    h = (n - 1) * q
    lo = int(math.floor(h))
    hi = min(lo + 1, n - 1)
    return float(sorted_vals[lo]) + (h - lo) * (float(sorted_vals[hi]) - float(sorted_vals[lo]))


def summary_stats(values: Sequence[float]) -> Dict[str, Any]:
    """n, mean, cv (sample). cv is None when n < 2 or mean == 0."""
    n = len(values)
    if n == 0:
        return {"n": 0, "mean": None, "cv": None}
    mean = statistics.fmean(values)
    cv = None
    if n >= 2 and mean != 0:
        cv = statistics.stdev(values) / mean          # stdev is sample (ddof=1)
    return {"n": n, "mean": _r(mean), "cv": _r(cv)}


def box_stats(values: Sequence[float]) -> Optional[Dict[str, Any]]:
    """Tukey box: quartiles, IQR, 1.5*IQR whiskers, and outlier points."""
    n = len(values)
    if n == 0:
        return None
    s = sorted(float(v) for v in values)
    q1, med, q3 = quantile(s, 0.25), quantile(s, 0.50), quantile(s, 0.75)
    iqr = q3 - q1
    lo_fence, hi_fence = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    inside = [v for v in s if lo_fence <= v <= hi_fence]
    outliers = [v for v in s if v < lo_fence or v > hi_fence]
    return {
        "min": _r(s[0]), "q1": _r(q1), "median": _r(med), "q3": _r(q3), "max": _r(s[-1]),
        "iqr": _r(iqr),
        "whisker_low": _r(inside[0] if inside else s[0]),
        "whisker_high": _r(inside[-1] if inside else s[-1]),
        "outliers": [_r(v) for v in outliers],
    }


def exceedance_curve(pairs: Sequence[Tuple[int, float]]) -> List[Dict[str, Any]]:
    """Weibull descending exceedance: (water_year, value) -> [{water_year,value,percentile}]."""
    ordered = sorted(pairs, key=lambda wv: (-wv[1], wv[0]))   # value desc, year asc
    n = len(ordered)
    return [
        {"water_year": wy, "value": _r(v), "percentile": _r((m + 1) / (n + 1) * 100.0)}
        for m, (wy, v) in enumerate(ordered)
    ]


def series(pairs: Sequence[Tuple[int, float]]) -> List[Dict[str, Any]]:
    """Raw per-year values ordered by water_year (box plots + client-side use)."""
    return [{"water_year": wy, "value": _r(v)} for wy, v in sorted(pairs)]


def _facets(pairs: Sequence[Tuple[int, float]], include: Sequence[str]) -> Dict[str, Any]:
    inc = set(include)
    vals = [v for _, v in pairs]
    out: Dict[str, Any] = {}
    if "values" in inc:
        out["values"] = series(pairs)
    if "exceedance" in inc:
        out["exceedance"] = exceedance_curve(pairs)
    if "box" in inc:
        out["box"] = box_stats(vals)
    if "statistics" in inc:
        out["statistics"] = summary_stats(vals)
    return out


def compute_series(
    rows: Iterable[Mapping[str, Any]],
    include: Sequence[str] = INCLUDE_ALL,
    wyt_filter: Optional[Sequence[int]] = None,
    subject_key: str = "subjects",
) -> Dict[str, Any]:
    """Group raw rows and compute per-scenario, per-subject, per-period, per-unit.

    Each row needs: scenario_short_code, subject_code, subject_kind, subject_label,
    period, unit, water_year, value. Compute is per single scenario (no pooling
    across scenarios) even when many scenarios are requested. `subject_key` names
    the per-scenario array in the output (e.g. "reservoirs", "rivers").
    """
    # scenario -> subject_code -> {kind,label, periods: {period: {unit: [(wy,val)]}}}
    grouped: Dict[str, Dict[str, Dict[str, Any]]] = {}
    years: Dict[str, set] = {}
    for r in rows:
        sc = r["scenario_short_code"]
        code = r["subject_code"]
        wy = int(r["water_year"])
        val = float(r["value"])
        subj = grouped.setdefault(sc, {}).setdefault(
            code, {"kind": r["subject_kind"], "label": r["subject_label"], "periods": {}}
        )
        subj["periods"].setdefault(r["period"], {}).setdefault(r["unit"], []).append((wy, val))
        years.setdefault(sc, set()).add(wy)

    scenarios_out: List[Dict[str, Any]] = []
    for sc in sorted(grouped):
        subjects = []
        for code in sorted(grouped[sc]):
            subj = grouped[sc][code]
            periods_out = {
                period: {unit: _facets(pairs, include) for unit, pairs in units.items()}
                for period, units in subj["periods"].items()
            }
            subjects.append(
                {"subject": code, "kind": subj["kind"], "label": subj["label"], "periods": periods_out}
            )
        scenarios_out.append(
            {"scenario": sc, "n_years": len(years.get(sc, ())), subject_key: subjects}
        )

    return {
        "wyt_filter": list(wyt_filter) if wyt_filter else None,
        "scenarios": scenarios_out,
    }


def compute_reservoir_storage(
    rows: Iterable[Mapping[str, Any]],
    include: Sequence[str] = INCLUDE_ALL,
    wyt_filter: Optional[Sequence[int]] = None,
) -> Dict[str, Any]:
    """Backward-compatible wrapper: subject array keyed "reservoirs"."""
    return compute_series(rows, include=include, wyt_filter=wyt_filter, subject_key="reservoirs")
