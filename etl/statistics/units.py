"""
Shared unit-conversion constants, CSV-loading helpers, and data-integrity
safeguard functions for the COEQWAL statistics ETL.

All modules should import from here instead of defining their own copies.
"""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)

# CFS to TAF per calendar day: (86400 s/day) / (43560 ft²/acre × 1000 ac/kac)
# Usage: TAF = CFS × DaysInMonth × CFS_TO_TAF_PER_DAY
# Matches the V3 notebook factor of 0.0019834714
CFS_TO_TAF_PER_DAY = 86_400 / 43_560_000  # ≈ 0.00198347107438

# MWD Table A contract demand (from COEQWAL_V3 DataExtraction.py line 914)
MWD_TABLE_A_ANNUAL_TAF = 1911.5

# ─── Data integrity safeguard thresholds ────────────────────────────
# Percentage warning threshold (does NOT clamp — only logs a warning)
PCT_WARNING_THRESHOLD = 200.0

# Maximum plausible monthly TAF value for a single DU or contractor.
# Values above this after conversion strongly suggest a double-conversion
# or a missed CFS→TAF step.
MONTHLY_TAF_SANITY_LIMIT = 2000.0

# Minimum mean (TAF) below which CV = std/mean is meaningless.
# When the mean is in LP-noise territory, CV explodes to millions.
CV_MIN_MEAN_TAF = 0.01

# Database stores CVs in NUMERIC(6,4) → max 99.9999.
# Any real-world CV above ~10 is already extreme; 99 is a safety cap.
MAX_CV = 99.0


def compute_cv(series: "pd.Series") -> float:
    """Coefficient of variation (std / |mean|), guarded and capped.

    Returns 0.0 when |mean| <= CV_MIN_MEAN_TAF, caps at MAX_CV.
    """
    mean_val = float(series.mean())
    if abs(mean_val) <= CV_MIN_MEAN_TAF:
        return 0.0
    cv = round(float(series.std() / abs(mean_val)), 4)
    return min(cv, MAX_CV)


def safe_pct(
    numerator: float,
    denominator: float,
    label: str = "",
    logger: Optional[logging.Logger] = None,
) -> float:
    """Compute a percentage with an optional plausibility warning.

    Returns ``(numerator / denominator) * 100``.  If the result exceeds
    ``PCT_WARNING_THRESHOLD`` a warning is logged (but the value is
    **not** clamped — it is returned as-is so the issue is visible in the
    data for investigation).
    """
    if denominator <= 0:
        return 0.0
    pct = (numerator / denominator) * 100
    if pct > PCT_WARNING_THRESHOLD and logger:
        logger.warning(
            "Suspicious percentage: %s = %.1f%% "
            "(num=%.2f, den=%.2f). Possible unit mismatch.",
            label,
            pct,
            numerator,
            denominator,
        )
    return pct


def validate_water_balance(
    df: pd.DataFrame,
    du_ids: List[str],
    logger: logging.Logger,
    tolerance: float = 1.01,
) -> int:
    """Check GP vs AW for agricultural DU × month after conversion.

    The actual CalSim 3 water balance (from WRESL constraints-Deliveries)
    is::

        AW + RP = DN + GP + RU + SHORTAGE

    where RP = Riparian/misc ET = AW × RPF (typically 5–15% of AW).
    GP is bounded by ``GPmax × AW × (1 + RPF − RUF)``, so GP > AW is
    expected whenever the DU has non-zero riparian losses.  GP/AW ratios
    of 1.0–1.15× are normal.

    This check flags DUs where GP exceeds AW by more than *tolerance*
    (default 1%).  Values well above ~1.15× may indicate a data issue.

    **This check applies only to agricultural DUs.**  Refuge DUs have
    different water accounting — exclude them before calling.

    Returns the total number of rows exceeding the tolerance.
    """
    violations = 0
    for du_id in du_ids:
        aw_col, gp_col = f"AW_{du_id}", f"GP_{du_id}"
        if aw_col not in df.columns or gp_col not in df.columns:
            continue
        aw = pd.to_numeric(df[aw_col], errors="coerce")
        gp = pd.to_numeric(df[gp_col], errors="coerce")
        mask = (gp > aw * tolerance) & (aw > 0)
        n = int(mask.sum())
        if n > 0:
            max_ratio = float((gp[mask] / aw[mask]).max())
            logger.warning(
                "GP > AW×%.2f for %s in %d rows (max GP/AW = %.1fx). "
                "Expected range is 1.0–1.15× due to riparian losses (RP).",
                tolerance,
                du_id,
                n,
                max_ratio,
            )
            violations += n
    if violations == 0:
        logger.info(
            "Water balance check passed: GP within expected range for all DU-months"
        )
    else:
        logger.warning(
            "Water balance: %d rows with GP > AW×%.2f", violations, tolerance
        )
    return violations


def check_post_conversion_magnitude(
    df: pd.DataFrame,
    columns: List[str],
    limit: float = MONTHLY_TAF_SANITY_LIMIT,
    logger: Optional[logging.Logger] = None,
) -> int:
    """Spot-check converted TAF values for implausible magnitudes.

    Returns the number of columns whose maximum exceeds *limit*.
    """
    flagged = 0
    for col in columns:
        vals = pd.to_numeric(df[col], errors="coerce")
        max_val = float(vals.max()) if not vals.dropna().empty else 0.0
        if max_val > limit:
            flagged += 1
            if logger:
                logger.warning(
                    "Suspicious magnitude after conversion: %s max = %.1f TAF/month "
                    "(limit: %.0f). Possible double conversion.",
                    col,
                    max_val,
                    limit,
                )
    return flagged


# ─────────────────────────────────────────────────────────────────────
# DSS-format CSV header helpers
#
# CalSim CSVs exported from DSS have a 7-row header:
#   Row 0 (A): Source       (CALSIM, MANUAL-ADD, CALCULATED)
#   Row 1 (B): Variable     (e.g. DN_06_NA, S_SHSTA)
#   Row 2 (C): Kind         (FLOW, STORAGE, …)
#   Row 3 (E): Time step    (1MON)
#   Row 4 (F): Level        (L2020A, …)
#   Row 5     : Record type  (PER-AVER, PER-CUM, INST-VAL)
#   Row 6     : Units        (CFS, TAF, NONE, …)
#
# The V3 pipeline may add TAF duplicates for CFS columns via
# ``convert_all_cfs_to_taf``, meaning a single variable name can
# appear twice (once CFS, once TAF).  The helpers below handle this.
# ─────────────────────────────────────────────────────────────────────


def parse_dss_csv_header(
    file_or_body,
) -> Tuple[List[str], List[str]]:
    """Read the 7-row DSS-export header.

    Returns (var_names, units_row) — both aligned by column index.
    """
    hdr = pd.read_csv(file_or_body, header=None, nrows=7, low_memory=False)
    var_names = [str(v) for v in hdr.iloc[1].tolist()]
    units_row = [str(u).strip().upper() for u in hdr.iloc[6].tolist()]
    return var_names, units_row


def load_dss_csv(
    file_path: str,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Load a DSS-export CSV.

    Returns (data_df, units_map) where *units_map* maps each column
    name to its declared unit (e.g. ``"CFS"``, ``"TAF"``).
    """
    var_names, units_row = parse_dss_csv_header(file_path)
    units_map = dict(zip(var_names, units_row))

    data_df = pd.read_csv(file_path, header=None, skiprows=7, low_memory=False)
    data_df.columns = var_names

    return data_df, units_map
