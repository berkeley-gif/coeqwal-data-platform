"""Reservoir statistics module.

Calculates monthly percentiles, storage timeseries, spill timeseries, and
period-of-record summaries for CalSim reservoir nodes. Output tables:
`reservoir_monthly_percentile`, `reservoir_storage_monthly`,
`reservoir_spill_monthly`, `reservoir_period_summary`.

Public entry point is `module.run`. Calculation helpers live in
`calculate_reservoir_statistics.py` and `calculate_reservoir_percentiles.py`.
The math library `reservoir_metrics.py` mirrors the `coeqwal/notebooks`
repo `coeqwalpackage/metrics.py`.
"""
