#!/usr/bin/env python3
"""
Reservoir metrics calculations aligned with COEQWAL research notebooks.

This module provides calculation functions for:
- Flood pool probability (probability storage >= flood level)
- Dead pool probability (probability storage <= dead pool level)
- Coefficient of variation (CV = std / mean)
- Annual average (mean of annual means)
- Monthly average (mean for a specific month across all years)

Reference: coeqwal/notebooks/coeqwalpackage/metrics.py

Usage:
    from etl.statistics.reservoir_metrics import (
        calculate_flood_pool_probability,
        calculate_dead_pool_probability,
        calculate_cv,
        calculate_annual_average,
        calculate_monthly_average,
    )
"""

from typing import Dict, List, Optional, Union
import numpy as np
import pandas as pd


def calculate_flood_pool_probability(
    storage_values: pd.Series,
    flood_threshold: Union[float, pd.Series],
    months: Optional[List[int]] = None,
    date_index: Optional[pd.DatetimeIndex] = None,
) -> Dict[str, float]:
    """
    Calculate probability that storage hits or exceeds flood pool level.

    Aligns with notebook's `frequency_hitting_level()` function (metrics.py:617-655).
    Flood pool is hit when: storage >= flood_threshold

    Args:
        storage_values: Series of storage values (TAF)
        flood_threshold: Either a constant (TAF) or a Series of variable thresholds
                        (e.g., from S_SHSTALEVEL5DV column)
        months: Optional list of calendar months to filter (1-12)
        date_index: DatetimeIndex for month filtering (required if months specified)

    Returns:
        Dict with keys:
        - 'probability': 0.0 to 1.0
        - 'hit_count': Number of months hitting flood pool
        - 'total_count': Total months analyzed

    Example:
        >>> storage = df['S_SHSTA']
        >>> # With constant threshold
        >>> result = calculate_flood_pool_probability(storage, flood_threshold=4200)
        >>> # With variable threshold from model output
        >>> flood_level = df['S_SHSTALEVEL5DV']
        >>> result = calculate_flood_pool_probability(storage, flood_level)
    """
    storage = storage_values.dropna()

    if months is not None:
        if date_index is None:
            raise ValueError("date_index required when months filter is specified")
        # Handle both DatetimeIndex and Series
        if hasattr(date_index, 'dt'):
            month_mask = date_index.dt.month.isin(months)
        else:
            month_mask = date_index.month.isin(months)
        storage = storage.loc[month_mask]
        if isinstance(flood_threshold, pd.Series):
            flood_threshold = flood_threshold.loc[month_mask]

    if len(storage) == 0:
        return {'probability': 0.0, 'hit_count': 0, 'total_count': 0}

    # Calculate difference: storage - threshold
    # Flood pool hit when storage >= threshold (difference >= 0)
    if isinstance(flood_threshold, pd.Series):
        flood_threshold = flood_threshold.reindex(storage.index)
        difference = storage.values - flood_threshold.values
    else:
        difference = storage.values - flood_threshold

    # Add small epsilon to make >= comparison (following notebook pattern)
    difference = difference + 0.000001

    # Count where difference >= 0 (storage >= threshold)
    hit_count = int((difference >= 0).sum())
    total_count = len(storage)

    return {
        'probability': hit_count / total_count if total_count > 0 else 0.0,
        'hit_count': hit_count,
        'total_count': total_count,
    }


def calculate_dead_pool_probability(
    storage_values: pd.Series,
    dead_pool_threshold: Union[float, pd.Series],
    months: Optional[List[int]] = None,
    date_index: Optional[pd.DatetimeIndex] = None,
) -> Dict[str, float]:
    """
    Calculate probability that storage drops to or below dead pool level.

    Aligns with notebook's `frequency_hitting_level()` with floodzone=False.
    Dead pool is hit when: storage <= dead_pool_threshold

    Args:
        storage_values: Series of storage values (TAF)
        dead_pool_threshold: Either a constant (TAF) or a Series of variable thresholds
                            (e.g., from S_SHSTALEVEL1DV column)
        months: Optional list of calendar months to filter (1-12)
        date_index: DatetimeIndex or Series for month filtering (required if months specified)

    Returns:
        Dict with keys:
        - 'probability': 0.0 to 1.0
        - 'hit_count': Number of months hitting dead pool
        - 'total_count': Total months analyzed

    Example:
        >>> storage = df['S_SHSTA']
        >>> result = calculate_dead_pool_probability(storage, dead_pool_threshold=115)
    """
    storage = storage_values.dropna()

    if months is not None:
        if date_index is None:
            raise ValueError("date_index required when months filter is specified")
        # Handle both DatetimeIndex and Series
        if hasattr(date_index, 'dt'):
            month_mask = date_index.dt.month.isin(months)
        else:
            month_mask = date_index.month.isin(months)
        storage = storage.loc[month_mask]
        if isinstance(dead_pool_threshold, pd.Series):
            dead_pool_threshold = dead_pool_threshold.loc[month_mask]

    if len(storage) == 0:
        return {'probability': 0.0, 'hit_count': 0, 'total_count': 0}

    # Calculate difference: storage - threshold
    # Dead pool hit when storage <= threshold (difference <= 0)
    if isinstance(dead_pool_threshold, pd.Series):
        dead_pool_threshold = dead_pool_threshold.reindex(storage.index)
        difference = storage.values - dead_pool_threshold.values
    else:
        difference = storage.values - dead_pool_threshold

    # Count where difference <= 0 (storage <= threshold)
    hit_count = int((difference <= 0).sum())
    total_count = len(storage)

    return {
        'probability': hit_count / total_count if total_count > 0 else 0.0,
        'hit_count': hit_count,
        'total_count': total_count,
    }


def calculate_cv(
    values: pd.Series,
    months: Optional[List[int]] = None,
    date_index: Optional[pd.DatetimeIndex] = None,
) -> float:
    """
    Calculate coefficient of variation (CV = std / mean).

    Aligns with notebook's `compute_cv()` function (metrics.py:383-393).
    Higher CV indicates more variability / less predictability.

    Args:
        values: Series of values
        months: Optional list of calendar months to filter (1-12)
        date_index: DatetimeIndex or Series for month filtering (required if months specified)

    Returns:
        CV value (float). Returns 0 if mean is 0.

    Example:
        >>> cv = calculate_cv(df['S_SHSTA'], months=[4], date_index=df['DateTime'])
        >>> print(f"April storage CV: {cv:.4f}")
    """
    data = values.dropna()

    if months is not None:
        if date_index is None:
            raise ValueError("date_index required when months filter is specified")
        # Handle both DatetimeIndex and Series
        if hasattr(date_index, 'dt'):
            month_mask = date_index.dt.month.isin(months)
        else:
            month_mask = date_index.month.isin(months)
        data = data.loc[month_mask]

    if len(data) == 0:
        return 0.0

    mean = float(data.mean())
    if mean == 0:
        return 0.0

    std = float(data.std())
    return std / mean


def calculate_annual_average(
    values: pd.Series,
    water_years: pd.Series,
    months: Optional[List[int]] = None,
    date_index: Optional[pd.DatetimeIndex] = None,
) -> float:
    """
    Calculate annual average (mean of annual means).

    Aligns with notebook's `ann_avg()` function (metrics.py:526-534).
    This calculates the mean for each water year, then takes the mean of those.

    Args:
        values: Series of values
        water_years: Series with water year for each row (Oct-Dec = next year)
        months: Optional list of calendar months to filter (1-12)
        date_index: DatetimeIndex or Series for month filtering (required if months specified)

    Returns:
        Annual average value

    Example:
        >>> ann_avg = calculate_annual_average(
        ...     df['S_SHSTA'],
        ...     df['WaterYear'],
        ...     months=[9, 10, 11],  # Fall only
        ...     date_index=df['DateTime']
        ... )
    """
    data = pd.DataFrame({'value': values, 'WaterYear': water_years}).dropna()

    if months is not None:
        if date_index is None:
            raise ValueError("date_index required when months filter is specified")
        # Handle both DatetimeIndex and Series
        if hasattr(date_index, 'dt'):
            month_mask = date_index.dt.month.isin(months)
        else:
            month_mask = date_index.month.isin(months)
        data = data.loc[month_mask]

    if len(data) == 0:
        return 0.0

    # Calculate mean for each water year
    annual_means = data.groupby('WaterYear')['value'].mean()

    # Return mean of annual means
    return float(annual_means.mean())


def calculate_monthly_average(
    values: pd.Series,
    date_index: pd.DatetimeIndex,
    month: int,
) -> float:
    """
    Calculate average for a specific calendar month across all years.

    Aligns with notebook's `mnth_avg()` function (metrics.py:545-554).

    Args:
        values: Series of values
        date_index: DatetimeIndex or Series for filtering
        month: Calendar month number (1=January, 9=September, etc.)

    Returns:
        Monthly average value

    Example:
        >>> sep_avg = calculate_monthly_average(df['S_SHSTA'], df['DateTime'], month=9)
        >>> apr_avg = calculate_monthly_average(df['S_SHSTA'], df['DateTime'], month=4)
    """
    # Handle both DatetimeIndex and Series
    if hasattr(date_index, 'dt'):
        month_mask = date_index.dt.month == month
    else:
        month_mask = date_index.month == month
    month_data = values.loc[month_mask].dropna()

    if len(month_data) == 0:
        return 0.0

    return float(month_data.mean())


def calculate_monthly_percentiles(
    values: pd.Series,
    date_index: pd.DatetimeIndex,
    percentiles: List[int] = None,
) -> Dict[int, Dict[str, float]]:
    """
    Calculate percentile statistics for each calendar month.

    This is the same approach used in calculate_reservoir_percentiles.py,
    validated against the notebook's approach.

    Args:
        values: Series of values
        date_index: DatetimeIndex or Series for month extraction
        percentiles: List of percentiles to calculate (default: [0, 10, 30, 50, 70, 90, 100])

    Returns:
        Dict of month (1-12) -> Dict of percentile values

    Example:
        >>> stats = calculate_monthly_percentiles(df['S_SHSTA'], df['DateTime'])
        >>> september_median = stats[9]['q50']
    """
    if percentiles is None:
        percentiles = [0, 10, 30, 50, 70, 90, 100]

    results = {}

    for month in range(1, 13):
        # Handle both DatetimeIndex and Series
        if hasattr(date_index, 'dt'):
            month_mask = date_index.dt.month == month
        else:
            month_mask = date_index.month == month
        month_data = values.loc[month_mask].dropna()

        if len(month_data) == 0:
            results[month] = {f'q{p}': 0.0 for p in percentiles}
            results[month]['mean'] = 0.0
            continue

        stats = {}
        for p in percentiles:
            stats[f'q{p}'] = round(float(np.percentile(month_data, p)), 2)

        stats['mean'] = round(float(month_data.mean()), 2)
        results[month] = stats

    return results


def calculate_water_month_percentiles(
    values: pd.Series,
    water_months: pd.Series,
    percentiles: List[int] = None,
) -> Dict[int, Dict[str, float]]:
    """
    Calculate percentile statistics for each water month.

    Water months: Oct=1, Nov=2, ..., Sep=12

    Args:
        values: Series of values
        water_months: Series with water month for each row
        percentiles: List of percentiles to calculate (default: [0, 10, 30, 50, 70, 90, 100])

    Returns:
        Dict of water_month (1-12) -> Dict of percentile values
    """
    if percentiles is None:
        percentiles = [0, 10, 30, 50, 70, 90, 100]

    data = pd.DataFrame({'value': values, 'WaterMonth': water_months}).dropna()
    results = {}

    for wm in range(1, 13):
        wm_data = data[data['WaterMonth'] == wm]['value']

        if len(wm_data) == 0:
            results[wm] = {f'q{p}': 0.0 for p in percentiles}
            results[wm]['mean'] = 0.0
            continue

        stats = {}
        for p in percentiles:
            stats[f'q{p}'] = round(float(np.percentile(wm_data, p)), 2)

        stats['mean'] = round(float(wm_data.mean()), 2)
        results[wm] = stats

    return results


# =============================================================================
# Reservoir-specific threshold constants
# =============================================================================

# Complete thresholds for all 92 reservoirs in reservoir_entity.csv
#
# Threshold sources:
#   - Variable: S_*LEVEL*DV columns from CalSim model output
#   - Constant: Known fixed values from CalSim documentation
#   - None: Uses fallback (dead_pool_taf from reservoir_entity.csv)
#
# Level naming convention in CalSim:
#   - LEVEL1: Dead pool / inactive storage (minimum operating level)
#   - LEVEL2-4: Intermediate operating rules (varies by reservoir)
#   - LEVEL5: Flood control level (top of conservation storage)
#
# Note: For reservoirs with None as flood_var, no flood pool probability
# will be calculated (returns NULL). For None as dead_var, the ETL will
# use dead_pool_taf from reservoir_entity.csv as the threshold.
#
RESERVOIR_THRESHOLDS = {
    # Format: 'SHORT_CODE': {
    #     'flood_var': 'S_*LEVEL*DV' variable name OR float constant (TAF) OR None,
    #     'dead_var': 'S_*LEVEL*DV' variable name OR float constant (TAF) OR None,
    # }
    #
    # ===================== MAJOR CVP RESERVOIRS =====================
    'SHSTA': {  # Shasta (4552 TAF) - largest CVP reservoir
        'flood_var': 'S_SHSTALEVEL5DV',
        'dead_var': 'S_SHSTALEVEL1DV',
    },
    'TRNTY': {  # Trinity (2448 TAF)
        'flood_var': 'S_TRNTYLEVEL5DV',
        'dead_var': 'S_TRNTYLEVEL1DV',
    },
    'FOLSM': {  # Folsom (975 TAF)
        'flood_var': 'S_FOLSMLEVEL5DV',
        'dead_var': 'S_FOLSMLEVEL1DV',
    },
    'MELON': {  # New Melones (2400 TAF)
        'flood_var': 'S_MELONLEVEL4DV',  # Uses Level 4
        'dead_var': 80.0,
    },
    'MLRTN': {  # Millerton (520 TAF)
        'flood_var': 524.0,
        'dead_var': 135.0,
    },
    'SLUIS_CVP': {  # San Luis CVP share (1062 TAF)
        'flood_var': 'S_SLUIS_CVPLEVEL5DV',
        'dead_var': 'S_SLUIS_CVPLEVEL1DV',
    },
    #
    # ===================== MAJOR SWP RESERVOIRS =====================
    'OROVL': {  # Oroville (3537 TAF) - largest SWP reservoir
        'flood_var': 'S_OROVLLEVEL5DV',
        'dead_var': 'S_OROVLLEVEL1DV',
    },
    'SLUIS_SWP': {  # San Luis SWP share (979 TAF)
        'flood_var': 'S_SLUIS_SWPLEVEL5DV',
        'dead_var': 'S_SLUIS_SWPLEVEL1DV',
    },
    'SLUIS': {  # Combined San Luis (2041 TAF)
        'flood_var': 'S_SLUISLEVEL5DV',
        'dead_var': 'S_SLUISLEVEL1DV',
    },
    #
    # ===================== CVP/LOCAL FLOOD CONTROL =====================
    'BLKBT': {  # Black Butte (136 TAF)
        'flood_var': 'S_BLKBTLEVEL5DV',
        'dead_var': None,  # Uses entity dead_pool_taf
    },
    'NHGAN': {  # New Hogan (317 TAF)
        'flood_var': 'S_NHGANLEVEL5DV',
        'dead_var': 'S_NHGANLEVEL1DV',
    },
    'ENGLB': {  # Englebright (70 TAF)
        'flood_var': 'S_ENGLBLEVEL5DV',
        'dead_var': None,
    },
    'HNSLY': {  # Hensley (90 TAF)
        'flood_var': 'S_HNSLYLEVEL5DV',
        'dead_var': 'S_HNSLYLEVEL1DV',
    },
    'ESTMN': {  # Eastman (5.9 TAF)
        'flood_var': 'S_ESTMNLEVEL5DV',
        'dead_var': 'S_ESTMNLEVEL1DV',
    },
    'LGRSV': {  # Little Grass Valley (93 TAF)
        'flood_var': 'S_LGRSVLEVEL5DV',
        'dead_var': None,
    },
    'INDVL': {  # Indian Valley (286 TAF)
        'flood_var': 'S_INDVLLEVEL5DV',
        'dead_var': None,
    },
    'SLYCK': {  # Sly Creek (65 TAF)
        'flood_var': 'S_SLYCKLEVEL5DV',
        'dead_var': None,
    },
    #
    # ===================== YUBA/BEAR SYSTEM =====================
    'NBLDB': {  # New Bullards Bar (966 TAF)
        'flood_var': 'S_NBLDBLEVEL5DV',
        'dead_var': 'S_NBLDBLEVEL1DV',
    },
    'CMPFW': {  # Camp Far West (103 TAF)
        'flood_var': 'S_CMPFWLEVEL5DV',
        'dead_var': None,
    },
    'RLLNS': {  # Rollins (66 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'CMBIE': {  # Combie (10 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'JKSMD': {  # Jackson Meadows (66.6 TAF)
        'flood_var': 'S_JKSMDLEVEL5DV',
        'dead_var': None,
    },
    #
    # ===================== AMERICAN RIVER SYSTEM =====================
    'HHOLE': {  # Hell Hole (207 TAF)
        'flood_var': 'S_HHOLELEVEL5DV',
        'dead_var': None,
    },
    'FRMDW': {  # French Meadows (136 TAF)
        'flood_var': 'S_FRMDWLEVEL5DV',
        'dead_var': None,
    },
    'LOONL': {  # Loon Lake (73.8 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'UNVLY': {  # Union Valley (277 TAF)
        'flood_var': 'S_UNVLYLEVEL5DV',
        'dead_var': None,
    },
    'ICEHS': {  # Ice House (45.8 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'NTOMA': {  # Natoma (8.9 TAF) - afterbay
        'flood_var': None,
        'dead_var': None,
    },
    'GERLE': {  # Gerle (1.3 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'STMPY': {  # Stumpy Meadows (20 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'JNKSN': {  # Jenkinson/Sly Park (40.6 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== TRINITY SYSTEM =====================
    'WKYTN': {  # Whiskeytown (241 TAF)
        'flood_var': 'S_WKYTNLEVEL5DV',
        'dead_var': 'S_WKYTNLEVEL1DV',
    },
    'LWSTN': {  # Lewiston (14.7 TAF) - regulating
        'flood_var': None,
        'dead_var': None,
    },
    'KSWCK': {  # Keswick (23.8 TAF) - regulating
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== FEATHER RIVER SYSTEM =====================
    'ALMNR': {  # Almanor (1143 TAF)
        'flood_var': 'S_ALMNRLEVEL5DV',
        'dead_var': None,
    },
    'BTVLY': {  # Butt Valley (49.8 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'BUCKS': {  # Bucks (101 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'ANTLP': {  # Antelope (179 TAF)
        'flood_var': 'S_ANTLPLEVEL5DV',
        'dead_var': None,
    },
    'FRMAN': {  # Frenchman (53.6 TAF)
        'flood_var': 'S_FRMANLEVEL5DV',
        'dead_var': None,
    },
    'DAVIS': {  # Lake Davis (1 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'RVPHB': {  # Round Valley/Philbrook (40 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'THRMA': {  # Thermalito Afterbay (61 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'THRMF': {  # Thermalito Forebay (73.5 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'MTMDW': {  # Mountain Meadows (24 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== MOKELUMNE SYSTEM =====================
    'CMCHE': {  # Camanche (417 TAF)
        'flood_var': 'S_CMCHELEVEL5DV',
        'dead_var': None,
    },
    'PARDE': {  # Pardee (197 TAF)
        'flood_var': 'S_PARDELEVEL5DV',
        'dead_var': None,
    },
    'SLTSP': {  # Salt Springs (141 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'RNCHO': {  # Rancho Murieta (5 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== STANISLAUS SYSTEM =====================
    'TULOC': {  # Tulloch (68.4 TAF)
        'flood_var': 'S_TULOCLEVEL5DV',
        'dead_var': None,
    },
    'BEARD': {  # Beardsley (97.7 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'DONLL': {  # Donnell (64.4 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'RELIE': {  # Relief (15.6 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'PCRST': {  # Pinecrest (4.3 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'LYONS': {  # Lyons (6 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'SPICE': {  # New Spicer Meadows (4 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'ALPNE': {  # Alpine (4.1 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== TUOLUMNE SYSTEM =====================
    'PEDRO': {  # Don Pedro (2030 TAF)
        'flood_var': 'S_PEDROLEVEL5DV',
        'dead_var': 'S_PEDROLEVEL1DV',
    },
    'HTCHY': {  # Hetch Hetchy (360 TAF)
        'flood_var': 'S_HTCHYLEVEL5DV',
        'dead_var': None,
    },
    'LLOYD': {  # Cherry/Lloyd (220 TAF)
        'flood_var': 'S_LLOYDLEVEL5DV',
        'dead_var': None,
    },
    'ELENR': {  # Eleanor (27 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'TRLCK': {  # Turlock (68.4 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'MDSTO': {  # Modesto (28 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'WDWRD': {  # Woodward (29.1 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== MERCED SYSTEM =====================
    'MCLRE': {  # McClure/Exchequer (1025 TAF)
        'flood_var': 'S_MCLRELEVEL5DV',
        'dead_var': 'S_MCLRELEVEL1DV',
    },
    #
    # ===================== SAN JOAQUIN SYSTEM =====================
    # Note: Millerton (MLRTN) listed above in Major CVP
    #
    # ===================== CALAVERAS SYSTEM =====================
    # Note: New Hogan (NHGAN) listed above in Flood Control
    'AMADR': {  # Lake Amador (7 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== CACHE/PUTAH CREEK =====================
    'BRYSA': {  # Lake Berryessa (1602 TAF)
        'flood_var': 'S_BRYSALEVEL5DV',
        'dead_var': None,
    },
    'CLRLK': {  # Clear Lake (131 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'EPARK': {  # East Park (51 TAF)
        'flood_var': 'S_EPARKLEVEL5DV',
        'dead_var': None,
    },
    'SGRGE': {  # Stony Gorge (50.1 TAF)
        'flood_var': 'S_SGRGELEVEL5DV',
        'dead_var': None,
    },
    #
    # ===================== SOUTH YUBA / TAHOE FEEDER =====================
    'SPLDG': {  # Spaulding (74.8 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'SCOTF': {  # Scott Flat (26 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'BOWMN': {  # Bowman (68.3 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'FRDYC': {  # Fordyce (49.4 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'FRNCH': {  # French/Faucherie/Sawmill (13.8 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'LCBRF': {  # Lindsey/Culbertson/Blue/Rucker/Feely (11.5 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'LKVLY': {  # Lake Valley (8.9 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'MERLC': {  # Merle Collins (1.3 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'SILVR': {  # Silver (31 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== COSUMNES / EL DORADO =====================
    'CAPLS': {  # Caples (21.6 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'ECHOL': {  # Echo (74.1 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'ALOHA': {  # Aloha (5.1 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== BEAR RIVER (LOWER) =====================
    'UBEAR': {  # Upper Bear (6.8 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'LBEAR': {  # Lower Bear (49.6 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== EBMUD =====================
    'EBMUD': {  # EBMUD Terminal (200 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'EBTML': {  # EBMUD Terminal alt (200 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== OTHER / MISC =====================
    'BLUMD': {  # Blue/Twin Meadow (9 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    'LOSVQ': {  # Los Vaqueros (100 TAF)
        'flood_var': None,
        'dead_var': None,
    },
    #
    # ===================== SOUTHERN CALIFORNIA (SWP) =====================
    'CSTIC': {  # Castaic (325 TAF)
        'flood_var': 'S_CSTICLEVEL5DV',
        'dead_var': None,
    },
    'PYRMD': {  # Pyramid (173 TAF)
        'flood_var': 'S_PYRMDLEVEL5DV',
        'dead_var': None,
    },
    'SVRWD': {  # Silverwood (73 TAF)
        'flood_var': 'S_SVWRDLEVEL5DV',
        'dead_var': None,
    },
    'PRRIS': {  # Perris (131 TAF)
        'flood_var': 'S_PRRISLEVEL5DV',
        'dead_var': None,
    },
    'DELVE': {  # Del Valle (77 TAF)
        'flood_var': 'S_DELVELEVEL5DV',
        'dead_var': None,
    },
}


def get_flood_threshold(
    reservoir_code: str,
    df: Optional[pd.DataFrame] = None,
) -> Union[float, pd.Series, None]:
    """
    Get flood pool threshold for a reservoir.

    Returns variable threshold series from DataFrame if available,
    otherwise returns constant from RESERVOIR_THRESHOLDS.

    Args:
        reservoir_code: Reservoir short code (e.g., 'SHSTA')
        df: DataFrame containing model output (for variable thresholds)

    Returns:
        Float constant or Series of variable thresholds, or None if not found
    """
    if reservoir_code not in RESERVOIR_THRESHOLDS:
        return None

    threshold = RESERVOIR_THRESHOLDS[reservoir_code].get('flood_var')

    if threshold is None:
        return None

    if isinstance(threshold, (int, float)):
        return float(threshold)

    # Variable threshold - try to get from DataFrame
    if df is not None and threshold in df.columns:
        return df[threshold]

    return None


def get_dead_pool_threshold(
    reservoir_code: str,
    df: Optional[pd.DataFrame] = None,
    entity_dead_pool_taf: Optional[float] = None,
) -> Union[float, pd.Series, None]:
    """
    Get dead pool threshold for a reservoir.

    Returns variable threshold series from DataFrame if available,
    constant from RESERVOIR_THRESHOLDS, or entity dead_pool_taf as fallback.

    Args:
        reservoir_code: Reservoir short code (e.g., 'SHSTA')
        df: DataFrame containing model output (for variable thresholds)
        entity_dead_pool_taf: Dead pool from reservoir_entity.csv as fallback

    Returns:
        Float constant or Series of variable thresholds, or None if not found
    """
    if reservoir_code in RESERVOIR_THRESHOLDS:
        threshold = RESERVOIR_THRESHOLDS[reservoir_code].get('dead_var')

        if threshold is not None:
            if isinstance(threshold, (int, float)):
                return float(threshold)

            # Variable threshold - try to get from DataFrame
            if df is not None and threshold in df.columns:
                return df[threshold]

    # Fallback to entity dead pool
    if entity_dead_pool_taf is not None:
        return float(entity_dead_pool_taf)

    return None


# =============================================================================
# Convenience functions for batch processing
# =============================================================================

def calculate_all_reservoir_probabilities(
    df: pd.DataFrame,
    reservoir_code: str,
    capacity_taf: float,
    dead_pool_taf: float,
    date_column: str = 'DateTime',
) -> Dict[str, Dict[str, float]]:
    """
    Calculate all probability metrics for a single reservoir.

    Args:
        df: DataFrame with storage data and DateTime
        reservoir_code: Short code (e.g., 'SHSTA')
        capacity_taf: Reservoir capacity (TAF)
        dead_pool_taf: Dead pool level from entity (TAF)
        date_column: Name of datetime column

    Returns:
        Dict with 'flood_pool' and 'dead_pool' probability results,
        plus 'all_year', 'september', and 'april' variants.
    """
    storage_col = f'S_{reservoir_code}'

    if storage_col not in df.columns:
        return {}

    storage = df[storage_col]
    date_idx = df[date_column]

    # Get thresholds
    flood_threshold = get_flood_threshold(reservoir_code, df)
    dead_threshold = get_dead_pool_threshold(reservoir_code, df, dead_pool_taf)

    results = {}

    # All-year probabilities
    if flood_threshold is not None:
        results['flood_pool_all'] = calculate_flood_pool_probability(
            storage, flood_threshold
        )

        # September flood probability (end of water year)
        results['flood_pool_september'] = calculate_flood_pool_probability(
            storage, flood_threshold, months=[9], date_index=date_idx
        )

    if dead_threshold is not None:
        results['dead_pool_all'] = calculate_dead_pool_probability(
            storage, dead_threshold
        )

        # September dead pool probability
        results['dead_pool_september'] = calculate_dead_pool_probability(
            storage, dead_threshold, months=[9], date_index=date_idx
        )

    # CV calculations
    results['cv_all'] = calculate_cv(storage)
    results['cv_april'] = calculate_cv(storage, months=[4], date_index=date_idx)
    results['cv_september'] = calculate_cv(storage, months=[9], date_index=date_idx)

    return results


# =============================================================================
# Verification and comparison utilities
# =============================================================================

def verify_metric(
    expected: float,
    actual: float,
    tolerance: float = 0.001,
) -> Dict[str, any]:
    """
    Verify a calculated metric against an expected value.

    Args:
        expected: Expected value (e.g., from notebook output)
        actual: Calculated value from our implementation
        tolerance: Relative tolerance for comparison (default 0.1%)

    Returns:
        Dict with verification results
    """
    if expected == 0:
        passed = abs(actual) < tolerance
        percent_error = 0 if passed else float('inf')
    else:
        percent_error = abs((actual - expected) / expected)
        passed = percent_error < tolerance

    return {
        'expected': expected,
        'actual': actual,
        'difference': actual - expected,
        'percent_error': percent_error * 100,  # Convert to percentage
        'passed': passed,
    }


def compare_monthly_percentiles(
    calculated: Dict[int, Dict[str, float]],
    expected: Dict[int, Dict[str, float]],
    tolerance: float = 0.001,
) -> Dict[str, any]:
    """
    Compare calculated monthly percentiles against expected values.

    Args:
        calculated: Dict of water_month -> {q0, q10, ..., q100, mean}
        expected: Same structure with expected values
        tolerance: Relative tolerance for comparison

    Returns:
        Dict with comparison results by month and percentile
    """
    results = {
        'all_passed': True,
        'total_comparisons': 0,
        'passed_comparisons': 0,
        'failed_comparisons': [],
        'by_month': {},
    }

    for month in range(1, 13):
        if month not in calculated or month not in expected:
            continue

        month_results = {}
        for key in ['q0', 'q10', 'q30', 'q50', 'q70', 'q90', 'q100', 'mean']:
            if key not in calculated[month] or key not in expected[month]:
                continue

            verification = verify_metric(
                expected[month][key],
                calculated[month][key],
                tolerance
            )

            results['total_comparisons'] += 1
            if verification['passed']:
                results['passed_comparisons'] += 1
            else:
                results['all_passed'] = False
                results['failed_comparisons'].append({
                    'month': month,
                    'metric': key,
                    **verification
                })

            month_results[key] = verification

        results['by_month'][month] = month_results

    return results


def format_verification_report(
    reservoir_code: str,
    scenario_id: str,
    verifications: Dict[str, any],
) -> str:
    """
    Format verification results as a human-readable report.

    Args:
        reservoir_code: Reservoir short code
        scenario_id: Scenario identifier
        verifications: Dict of metric_name -> verification result

    Returns:
        Formatted string report
    """
    lines = [
        f"Verification Report: {reservoir_code} ({scenario_id})",
        "=" * 60,
        "",
    ]

    passed_count = 0
    failed_count = 0

    for metric_name, result in verifications.items():
        if isinstance(result, dict) and 'passed' in result:
            status = "PASS" if result['passed'] else "FAIL"
            if result['passed']:
                passed_count += 1
            else:
                failed_count += 1

            lines.append(f"{metric_name}:")
            lines.append(f"  Expected: {result['expected']:.4f}")
            lines.append(f"  Actual:   {result['actual']:.4f}")
            lines.append(f"  Diff:     {result['difference']:.4f} ({result['percent_error']:.2f}%)")
            lines.append(f"  Status:   {status}")
            lines.append("")

    lines.append("-" * 60)
    lines.append(f"Total: {passed_count} passed, {failed_count} failed")

    return "\n".join(lines)


# =============================================================================
# Summary statistics for reporting
# =============================================================================

def summarize_probability_metrics(
    period_summary: Dict[str, any],
) -> Dict[str, str]:
    """
    Create human-readable summary of probability metrics.

    Args:
        period_summary: Dict from calculate_period_summary()

    Returns:
        Dict of metric descriptions
    """
    summary = {}

    # Flood pool risk assessment
    flood_prob = period_summary.get('flood_pool_prob_all')
    if flood_prob is not None:
        if flood_prob < 0.01:
            summary['flood_risk'] = f"Very low flood risk ({flood_prob:.1%})"
        elif flood_prob < 0.05:
            summary['flood_risk'] = f"Low flood risk ({flood_prob:.1%})"
        elif flood_prob < 0.10:
            summary['flood_risk'] = f"Moderate flood risk ({flood_prob:.1%})"
        else:
            summary['flood_risk'] = f"Elevated flood risk ({flood_prob:.1%})"
    else:
        summary['flood_risk'] = "Flood risk not calculated (no threshold)"

    # Dead pool risk assessment
    dead_prob = period_summary.get('dead_pool_prob_all')
    if dead_prob is not None:
        if dead_prob < 0.001:
            summary['drought_risk'] = f"Very low drought risk ({dead_prob:.1%})"
        elif dead_prob < 0.01:
            summary['drought_risk'] = f"Low drought risk ({dead_prob:.1%})"
        elif dead_prob < 0.05:
            summary['drought_risk'] = f"Moderate drought risk ({dead_prob:.1%})"
        else:
            summary['drought_risk'] = f"Elevated drought risk ({dead_prob:.1%})"
    else:
        summary['drought_risk'] = "Drought risk not calculated (no threshold)"

    # Storage variability assessment
    cv = period_summary.get('storage_cv_all')
    if cv is not None:
        if cv < 0.15:
            summary['variability'] = f"Low variability (CV={cv:.3f})"
        elif cv < 0.30:
            summary['variability'] = f"Moderate variability (CV={cv:.3f})"
        else:
            summary['variability'] = f"High variability (CV={cv:.3f})"

    return summary


def list_available_thresholds() -> Dict[str, Dict[str, str]]:
    """
    List all reservoirs with known threshold configurations.

    Returns:
        Dict of reservoir_code -> threshold info for documentation/display
    """
    info = {}
    for code, thresholds in RESERVOIR_THRESHOLDS.items():
        flood_var = thresholds.get('flood_var')
        dead_var = thresholds.get('dead_var')

        info[code] = {
            'flood_threshold': (
                f"{flood_var:.1f} TAF (constant)"
                if isinstance(flood_var, (int, float))
                else f"{flood_var} (model output)"
            ) if flood_var else "Not defined",
            'dead_threshold': (
                f"{dead_var:.1f} TAF (constant)"
                if isinstance(dead_var, (int, float))
                else f"{dead_var} (model output)"
            ) if dead_var else "Not defined",
        }

    return info
