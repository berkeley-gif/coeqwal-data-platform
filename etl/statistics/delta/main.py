#!/usr/bin/env python3
"""
CLI entry point for Delta statistics calculation.

Usage:
    python main.py --scenario s0020
    python main.py --scenario s0020 --csv-path /path/to/calsim_output.csv --dry-run
"""

from calculate_delta_statistics import main

if __name__ == "__main__":
    main()
