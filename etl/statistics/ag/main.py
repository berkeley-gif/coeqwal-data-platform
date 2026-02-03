#!/usr/bin/env python3
"""
CLI entry point for agricultural statistics calculation.

Usage:
    python main.py --scenario s0020
    python main.py --scenario s0020 --csv-path /path/to/calsim_output.csv --dry-run
    python main.py --all-scenarios
"""

from calculate_ag_statistics import main

if __name__ == "__main__":
    main()
