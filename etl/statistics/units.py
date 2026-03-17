"""
Shared unit-conversion constants for the COEQWAL statistics ETL.

All modules should import from here instead of defining their own copies.
"""

# CFS to TAF per calendar day: (86400 s/day) / (43560 ft²/acre × 1000 ac/kac)
# Usage: TAF = CFS × DaysInMonth × CFS_TO_TAF_PER_DAY
CFS_TO_TAF_PER_DAY = 86_400 / 43_560_000  # ≈ 0.00198347107438

# MWD Table A contract demand (from COEQWAL_V3 DataExtraction.py line 914)
MWD_TABLE_A_ANNUAL_TAF = 1911.5
