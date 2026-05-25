"""COEQWAL statistics package.

- `run_all.py`           main CLI: dispatches stats modules per scenario

- `lib/`                 library modules imported by `run_all.py` and by each
                         module's `module.py`

- `<module>/`            one subdir per stats module (ag, delta, cws_aggregate,
                         du_urban, env_flows, mi, refuge, reservoirs, etc.). Holds
                         the per-module `calculate_*.py` and the per-module CLI
                         `main.py`. Modules that expose `module.py` implement the
                         shared `run()` contract from `lib/protocol.py`

- `verify_*.py`          verification scripts

- `audit_reports/`       per-run `stats_audit_<timestamp>.csv` outputs. Gitignored
"""
