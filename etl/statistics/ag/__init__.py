"""
Agricultural (AG) Statistics Module

Calculates delivery and shortage statistics for agricultural demand units.

Data sources from CalSim output:
- AW_{DU_ID} - Applied Water (delivery) for all regions
- GW_SHORT_{DU_ID} - Groundwater shortage for SJR/Tulare regions only
- DEL_SWP_PAG, DEL_CVP_PAG_N, etc. - Aggregate deliveries

Statistics tables populated:
- ag_du_delivery_monthly - Monthly delivery statistics by demand unit
- ag_du_shortage_monthly - Monthly shortage statistics (SJR/Tulare only)
- ag_du_period_summary - Period-of-record summary by demand unit
- ag_aggregate_monthly - Monthly statistics for project aggregates
- ag_aggregate_period_summary - Period summary for project aggregates
"""

from .calculate_ag_statistics import (
    calculate_all_ag_statistics,
    calculate_du_delivery_monthly,
    calculate_du_shortage_monthly,
    calculate_du_period_summary,
    calculate_aggregate_monthly,
    calculate_aggregate_period_summary,
    load_ag_demand_units,
)

__all__ = [
    "calculate_all_ag_statistics",
    "calculate_du_delivery_monthly",
    "calculate_du_shortage_monthly",
    "calculate_du_period_summary",
    "calculate_aggregate_monthly",
    "calculate_aggregate_period_summary",
    "load_ag_demand_units",
]
