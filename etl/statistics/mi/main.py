#!/usr/bin/env python3
"""
CLI entry point for M&I contractor statistics calculation.

Usage:
    python main.py --scenario s0020
    python main.py --scenario s0020 --csv-path /path/to/deliveries.csv --dry-run
    python main.py --all-scenarios
"""

from calculate_mi_statistics import main

if __name__ == "__main__":
    main()
