"""Null-safe casting helpers.

Use these instead of the truthiness pattern `float(row["x"]) if row["x"] else 0.0`,
which collapses both NULL and a legitimate measured 0 into the fallback.
These helpers preserve real zero values and pass NULL through as `None`,
which serializes to JSON `null` so the client can distinguish "no data" from
"the measured value is zero".
"""

from typing import Any, Optional


def safe_float(val: Any) -> Optional[float]:
    """Return `float(val)` for any non-None input, or `None` if `val is None`.

    Preserves measured zero. Use for any numeric column that may be NULL in
    the database or unset upstream.
    """
    if val is None:
        return None
    return float(val)


def safe_int(val: Any) -> Optional[int]:
    """Return `int(val)` for any non-None input, or `None` if `val is None`.

    Use for any integer column that may be NULL in the database.
    """
    if val is None:
        return None
    return int(val)


def safe_str(val: Any) -> Optional[str]:
    """Return `str(val)` for any non-None input, or `None` if `val is None`.

    Use for description / label columns that may legitimately be NULL.
    """
    if val is None:
        return None
    return str(val)
