#!/usr/bin/env python3
"""
test_ensure_unique_axes.py - Behavioral spec for the duplicate-axis guard
used by load_all_tier_results.py.

The guard exists because pd.read_csv silently produces a DataFrame with
a duplicated index or column axis when the source CSV has duplicate
labels. Downstream code calling df.loc[row, col] then gets a Series
back instead of a scalar, and the next pd.isna() call blows up with the
unhelpful "truth value of a Series is ambiguous" error. The guard
distinguishes identical duplicates (drop with NOTICE) from conflicting
duplicates (raise ValueError naming the offending labels).

Run:
    python etl/tier_data/scripts/test_ensure_unique_axes.py

Exits 0 on success, 1 on failure. No pytest dependency.

The helper is inlined here so the test can run without importing the
loader (which pulls in etl.common and other project state). Keep the
inlined copy byte-identical to the version in load_all_tier_results.py
so this test catches behavioral drift.
"""

import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pandas as pd


def _ensure_unique_axes(df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    """Detect duplicate row or column labels in a tier staging frame.

    Identical duplicates are dropped with a `NOTICE`. Conflicting
    duplicates raise `ValueError` naming the labels so the upstream CSV
    can be fixed.
    """
    def _conflicts(frame: pd.DataFrame) -> bool:
        ref = frame.iloc[0].fillna('__NA__')
        return not frame.fillna('__NA__').eq(ref).all().all()

    if df.index.has_duplicates:
        dup_labels = sorted({str(v) for v in df.index[df.index.duplicated()]})
        conflicts = [d for d in dup_labels if _conflicts(df.loc[[d]])]
        if conflicts:
            raise ValueError(
                f"{csv_path.name}: duplicate row labels with conflicting "
                f"values: {conflicts}. Resolve in the upstream CSV "
                f"(one row per scenario)."
            )
        print(
            f"  NOTICE: {csv_path.name} has duplicate but identical rows "
            f"for {dup_labels}; keeping first occurrence."
        )
        df = df[~df.index.duplicated(keep='first')]

    if df.columns.has_duplicates:
        dup_labels = sorted({str(v) for v in df.columns[df.columns.duplicated()]})
        conflicts = [d for d in dup_labels if _conflicts(df.loc[:, [d]].T)]
        if conflicts:
            raise ValueError(
                f"{csv_path.name}: duplicate column labels with conflicting "
                f"values: {conflicts}. Resolve in the upstream CSV "
                f"(one column per location)."
            )
        print(
            f"  NOTICE: {csv_path.name} has duplicate but identical columns "
            f"for {dup_labels}; keeping first occurrence."
        )
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

    return df


# ---------------------------------------------------------------------------
# Tiny assertion helpers (no pytest dependency)
# ---------------------------------------------------------------------------

PASSED = 0
FAILED: list[str] = []


def case(label: str):
    def decorator(fn):
        global PASSED
        try:
            fn()
            PASSED += 1
            print(f"  PASS: {label}")
        except AssertionError as e:
            FAILED.append(f"{label}: {e}")
            print(f"  FAIL: {label}: {e}")
        except Exception as e:
            FAILED.append(f"{label}: unexpected {type(e).__name__}: {e}")
            print(f"  FAIL: {label}: unexpected {type(e).__name__}: {e}")
        return fn
    return decorator


def _capture(fn, *args, **kwargs):
    """Run fn, returning (return_value_or_exception, captured_stdout)."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            result = fn(*args, **kwargs)
        except Exception as e:
            return e, buf.getvalue()
    return result, buf.getvalue()


FAKE_PATH = Path("FAKE_TIER.csv")


# ---------------------------------------------------------------------------
# Test cases - exercise every branch of _ensure_unique_axes
# ---------------------------------------------------------------------------

print("Running _ensure_unique_axes behavioral spec...")
print()


@case("no duplicates -> no-op")
def _():
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}, index=['x', 'y'])
    result, out = _capture(_ensure_unique_axes, df, FAKE_PATH)
    assert isinstance(result, pd.DataFrame), f"expected DataFrame, got {type(result)}"
    assert result.shape == (2, 2), f"shape changed: {result.shape}"
    assert "NOTICE" not in out, f"unexpected NOTICE on clean frame: {out!r}"


@case("identical duplicate rows -> drop with NOTICE")
def _():
    # Two 's0046' rows with identical values, like the real ENV_FLOWS bug.
    df = pd.DataFrame(
        [[1, 2], [9, 9], [1, 2]],
        index=['s0046', 's0050', 's0046'],
        columns=['A', 'B'],
    )
    result, out = _capture(_ensure_unique_axes, df, FAKE_PATH)
    assert isinstance(result, pd.DataFrame), f"expected DataFrame, got {result!r}"
    assert list(result.index) == ['s0046', 's0050'], f"index: {list(result.index)}"
    assert "NOTICE" in out and "s0046" in out, f"missing NOTICE: {out!r}"


@case("conflicting duplicate rows -> raise ValueError naming label")
def _():
    df = pd.DataFrame(
        [[1, 2], [9, 9], [3, 4]],
        index=['s0046', 's0050', 's0046'],
        columns=['A', 'B'],
    )
    result, _out = _capture(_ensure_unique_axes, df, FAKE_PATH)
    assert isinstance(result, ValueError), f"expected ValueError, got {result!r}"
    assert "s0046" in str(result), f"error did not name s0046: {result}"
    assert "FAKE_TIER.csv" in str(result), f"error did not name CSV: {result}"


@case("identical duplicate columns -> drop with NOTICE")
def _():
    df = pd.DataFrame([[1, 2, 1], [3, 4, 3]], columns=['SAC289', 'X', 'SAC289'])
    result, out = _capture(_ensure_unique_axes, df, FAKE_PATH)
    assert isinstance(result, pd.DataFrame), f"expected DataFrame, got {result!r}"
    assert list(result.columns) == ['SAC289', 'X'], f"columns: {list(result.columns)}"
    assert "NOTICE" in out and "SAC289" in out, f"missing NOTICE: {out!r}"


@case("conflicting duplicate columns -> raise ValueError naming label")
def _():
    df = pd.DataFrame([[1, 2, 99], [3, 4, 99]], columns=['SAC289', 'X', 'SAC289'])
    result, _out = _capture(_ensure_unique_axes, df, FAKE_PATH)
    assert isinstance(result, ValueError), f"expected ValueError, got {result!r}"
    assert "SAC289" in str(result), f"error did not name SAC289: {result}"


@case("NaN-vs-NaN counts as identical, not conflict")
def _():
    import numpy as np
    df = pd.DataFrame(
        [[1, np.nan], [9, 9], [1, np.nan]],
        index=['s0046', 's0050', 's0046'],
        columns=['A', 'B'],
    )
    result, out = _capture(_ensure_unique_axes, df, FAKE_PATH)
    assert isinstance(result, pd.DataFrame), f"NaN-vs-NaN treated as conflict: {result!r}"
    assert "NOTICE" in out, f"missing NOTICE: {out!r}"


@case("both row and column dupes resolved in one pass")
def _():
    df = pd.DataFrame(
        [[1, 2, 1], [9, 9, 9], [1, 2, 1]],
        index=['s0046', 's0050', 's0046'],
        columns=['SAC289', 'X', 'SAC289'],
    )
    result, out = _capture(_ensure_unique_axes, df, FAKE_PATH)
    assert isinstance(result, pd.DataFrame), f"expected DataFrame, got {result!r}"
    assert result.shape == (2, 2), f"shape: {result.shape}"
    assert out.count("NOTICE") == 2, f"expected 2 NOTICEs, got {out.count('NOTICE')}"


@case("multiple distinct duplicate labels reported together")
def _():
    df = pd.DataFrame(
        [[1, 2], [3, 4], [1, 2], [5, 6], [3, 4]],
        index=['s0046', 's0050', 's0046', 's0060', 's0050'],
        columns=['A', 'B'],
    )
    result, out = _capture(_ensure_unique_axes, df, FAKE_PATH)
    assert isinstance(result, pd.DataFrame), f"expected DataFrame, got {result!r}"
    assert list(result.index) == ['s0046', 's0050', 's0060'], f"index: {list(result.index)}"
    assert "s0046" in out and "s0050" in out, f"missing both labels: {out!r}"


# ---------------------------------------------------------------------------

print()
print(f"Results: {PASSED} passed, {len(FAILED)} failed")
if FAILED:
    print()
    print("Failures:")
    for f in FAILED:
        print(f"  - {f}")
    sys.exit(1)
print("All cases passed.")
sys.exit(0)
