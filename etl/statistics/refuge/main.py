#!/usr/bin/env python3
"""
CLI entry point for wildlife refuge statistics calculation.

Usage:
    python main.py --scenario s0020
    python main.py --scenario s0020 --dry-run
    python main.py --all-scenarios
    python main.py --scenario s0020 --sv-path /path/to/sv.csv --dv-path /path/to/dv.csv --dry-run
"""

from calculate_refuge_statistics import main

if __name__ == "__main__":
    main()
