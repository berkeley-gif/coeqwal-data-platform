#!/usr/bin/env python3
"""
Entry point for the env_flows ETL module.

Called by run_all.py with:
    python main.py --scenario s0020

Passes --csv-path as --dv-path if provided (consistent with run_all.py convention).
For env flows, both --dv-path and --sv-path can be set independently via the
calculate_env_flow_statistics.py CLI for manual runs.
"""

import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).parent / "calculate_env_flow_statistics.py"


def main() -> None:
    # Forward all arguments unchanged to the main calculation script
    cmd = [sys.executable, str(SCRIPT)] + sys.argv[1:]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
