"""Unit tests for the pure data_in_depth stats module (no DB, no FastAPI).

Run from api/coeqwal-api:
    python -m pytest tests/test_data_in_depth_stats.py -q
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from routes.data_in_depth_stats import (  # noqa: E402
    box_stats,
    compute_reservoir_storage,
    exceedance_curve,
    quantile,
    summary_stats,
)


# --- quantile (type-7 linear) ---------------------------------------------
def test_quantile_matches_type7():
    s = [10, 20, 30, 40]
    assert quantile(s, 0.0) == 10
    assert quantile(s, 1.0) == 40
    assert quantile(s, 0.5) == 25            # (20+30)/2 interpolated
    assert round(quantile(s, 0.25), 4) == 17.5


def test_quantile_single():
    assert quantile([42], 0.5) == 42


# --- summary stats ---------------------------------------------------------
def test_summary_mean_cv():
    r = summary_stats([2, 4, 4, 4, 5, 5, 7, 9])
    assert r["n"] == 8
    assert r["mean"] == 5.0
    # sample stdev = 2.138..., cv = 0.4277
    assert abs(r["cv"] - 0.4277) < 1e-3


def test_summary_cv_none_when_n_lt_2():
    assert summary_stats([100.0])["cv"] is None
    assert summary_stats([])["cv"] is None
    assert summary_stats([])["n"] == 0


def test_summary_cv_none_when_mean_zero():
    assert summary_stats([-1.0, 1.0])["cv"] is None   # mean 0


# --- box (Tukey) -----------------------------------------------------------
def test_box_outliers_and_whiskers():
    vals = [10, 11, 12, 13, 14, 15, 100]          # 100 is a high outlier
    b = box_stats(vals)
    assert b["min"] == 10 and b["max"] == 100
    assert 100.0 in b["outliers"]
    assert b["whisker_high"] == 15                # highest non-outlier
    assert b["whisker_low"] == 10


def test_box_no_outliers():
    b = box_stats([1, 2, 3, 4, 5])
    assert b["outliers"] == []
    assert b["whisker_low"] == 1 and b["whisker_high"] == 5


def test_box_empty_is_none():
    assert box_stats([]) is None


# --- exceedance (Weibull descending) --------------------------------------
def test_exceedance_descending_and_plotting_position():
    pairs = [(1922, 10.0), (1923, 30.0), (1924, 20.0)]
    curve = exceedance_curve(pairs)
    # largest value first, lowest exceedance %
    assert [c["value"] for c in curve] == [30.0, 20.0, 10.0]
    assert curve[0]["percentile"] == round(1 / 4 * 100, 4)      # 25.0
    assert curve[-1]["percentile"] == round(3 / 4 * 100, 4)     # 75.0


def test_exceedance_ties_broken_by_water_year():
    pairs = [(1925, 50.0), (1922, 50.0), (1930, 50.0)]
    curve = exceedance_curve(pairs)
    assert [c["water_year"] for c in curve] == [1922, 1925, 1930]


# --- compute_reservoir_storage (grouping + include + n_years) --------------
def _rows():
    rows = []
    for sc in ("s0011", "s0020"):
        for wy, v in [(1922, 100.0), (1923, 200.0), (1924, 300.0)]:
            rows.append({
                "scenario_short_code": sc, "subject_code": "SHSTA",
                "subject_kind": "entity", "subject_label": "Shasta",
                "period": "sept", "unit": "TAF", "water_year": wy, "value": v,
            })
    return rows


def test_compute_nests_per_scenario():
    out = compute_reservoir_storage(_rows())
    assert [s["scenario"] for s in out["scenarios"]] == ["s0011", "s0020"]
    s0 = out["scenarios"][0]
    assert s0["n_years"] == 3
    unit = s0["reservoirs"][0]["periods"]["sept"]["TAF"]
    assert {"values", "exceedance", "box", "statistics"} <= set(unit)
    assert unit["statistics"]["mean"] == 200.0


def test_compute_include_filters_facets():
    out = compute_reservoir_storage(_rows(), include=["statistics"])
    unit = out["scenarios"][0]["reservoirs"][0]["periods"]["sept"]["TAF"]
    assert set(unit) == {"statistics"}


def test_compute_wyt_filter_echoed():
    out = compute_reservoir_storage(_rows(), wyt_filter=[1, 2])
    assert out["wyt_filter"] == [1, 2]
    out2 = compute_reservoir_storage(_rows())
    assert out2["wyt_filter"] is None
