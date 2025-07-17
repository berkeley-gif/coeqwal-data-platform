#!/usr/bin/env python
"""
COEQWAL DSS -> CSV converter
- Works in AWS Batch container environment
- Structured logging to stdout (CloudWatch friendly)
- Raises errors so Batch can retry
"""
from __future__ import annotations

import os
import sys
import re
import json
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from pydsstools.heclib.dss import HecDss


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
LOG_LEVEL = os.getenv("COEQWAL_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("dss_to_csv")


class DSSProcessor:
    """Enhanced DSS to CSV processor that handles multiple DSS file types."""

    def __init__(
        self,
        dss_type: str = "auto",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "monthly",
        missing_value: float = -901,
        timestamp_adjustment: str = "end_of_month",
    ):
        self.dss_type = dss_type
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.frequency = frequency
        self.missing_value = missing_value
        self.timestamp_adjustment = timestamp_adjustment

        # Default processing configs by DSS type
        self.processing_configs = {
            "calsim_output": {
                "default_start_date": "1921-10-31",
                "default_end_date": "2021-09-30",
                "series_key_format": "{b}_{c}",
                "column_name_format": "{b}_{c}_{f}",
                "header_mapping": ["a", "b", "c", "e", "f", "type", "units"],
                "timestamp_adjustment": "end_of_month",
            },
            "sv_input": {
                "default_start_date": None,  # Use full date range
                "default_end_date": None,
                "series_key_format": "{a}_{b}_{c}",
                "column_name_format": "{a}_{b}_{c}",
                "header_mapping": ["a", "b", "c", "d", "e", "f", "units"],
                "timestamp_adjustment": "none",
            },
        }

    def detect_dss_type(self, dss_file_path: str) -> str:
        """Auto-detect DSS file type based on pathnames."""
        log.info("Auto-detecting DSS file type...")
        dss = HecDss.Open(dss_file_path)
        try:
            pathnames = dss.getPathnameList("/*/*/*/*/*/*/")
            if not pathnames:
                return "unknown"

            sample_pathnames = pathnames[: min(10, len(pathnames))]

            calsim_output_indicators = 0
            sv_input_indicators = 0

            for pathname in sample_pathnames:
                parts = pathname.split("/")
                if len(parts) >= 7:
                    a, b, c, d, e, f = parts[1:7]

                    # CalSim output patterns
                    if re.match(r"^[SCDI]$", b):  # Common CalSim variable types
                        calsim_output_indicators += 1
                    if re.match(r"^(TAF|CFS|AF)$", f):  # Common units
                        calsim_output_indicators += 1
                    if re.match(r"^[A-Z0-9_]+$", c):  # CalSim entity names
                        calsim_output_indicators += 1

                    # SV input patterns (approximate; tune for real data)
                    if re.match(r"^SV", a):
                        sv_input_indicators += 1
                    if re.match(r"^(INITIAL|INPUT|STATE)$", d):
                        sv_input_indicators += 1

            detected_type = (
                "calsim_output"
                if calsim_output_indicators > sv_input_indicators
                else "sv_input"
            )
            log.info("Detected DSS type: %s", detected_type)
            return detected_type
        finally:
            dss.close()

    def process_dss_file(self, dss_file_path: str, output_csv_path: str) -> Dict[str, int]:
        """Main processing method. Returns metrics dict."""
        log.info("Starting DSS->CSV conversion")
        log.info("Input DSS: %s", dss_file_path)
        log.info("Output CSV: %s", output_csv_path)

        if not os.path.exists(dss_file_path):
            msg = f"DSS file does not exist: {dss_file_path}"
            log.error(msg)
            raise FileNotFoundError(msg)

        # Auto-detect DSS type if needed
        if self.dss_type == "auto":
            self.dss_type = self.detect_dss_type(dss_file_path)

        # Config
        config = self.processing_configs.get(
            self.dss_type, self.processing_configs["calsim_output"]
        )
        log.info("Processing as: %s", self.dss_type)

        # Default date range
        if not self.start_date and config["default_start_date"]:
            self.start_date = pd.Timestamp(config["default_start_date"])
        if not self.end_date and config["default_end_date"]:
            self.end_date = pd.Timestamp(config["default_end_date"])

        log.debug("Opening DSS file...")
        dss = HecDss.Open(dss_file_path)

        try:
            log.debug("Getting pathname list...")
            available_pathnames = dss.getPathnameList("/*/*/*/*/*/*/")
            log.info("Found %d pathnames", len(available_pathnames))

            all_datetimes: Set[pd.Timestamp] = set()
            time_series_groups: Dict[str, Dict] = {}

            log.info("Processing time series data...")
            for i, pathname in enumerate(available_pathnames):
                if i % 100 == 0:
                    log.debug("Processed %d/%d pathnames", i, len(available_pathnames))

                try:
                    data = dss.read_ts(pathname)
                    parts = pathname.split("/")
                    if len(parts) < 7:
                        continue
                    a, b, c, d, e, f = parts[1:7]

                    series_key = config["series_key_format"].format(
                        a=a, b=b, c=c, d=d, e=e, f=f
                    )

                    if series_key not in time_series_groups:
                        time_series_groups[series_key] = {
                            "data": {},
                            "a": a,
                            "b": b,
                            "c": c,
                            "d": d,
                            "e": e,
                            "f": f,
                            "units": getattr(data, "units", ""),
                            "type": getattr(data, "type", ""),
                        }

                    values = getattr(data, "values", [])
                    pytimes = getattr(data, "pytimes", [])
                    if len(values) != len(pytimes):
                        log.warning("Mismatched lengths for %s", pathname)

                    # Replace missing codes
                    values = np.where(values == self.missing_value, np.nan, values)

                    # Timestamps
                    adj = config["timestamp_adjustment"]
                    for dt_, value in zip(pytimes, values):
                        ts_dt = self._adjust_timestamp(dt_, adj)
                        time_series_groups[series_key]["data"][ts_dt] = value
                        all_datetimes.add(ts_dt)

                except Exception as e:  # noqa: BLE001
                    log.warning("Error processing '%s': %s", pathname, e)

            log.info("Processed %d time series", len(time_series_groups))
            if not time_series_groups:
                raise RuntimeError("No time series data read from DSS.")

            log.debug("Creating output DataFrame...")
            combined_df = self._create_output_dataframe(
                time_series_groups, all_datetimes, config
            )

            if self.start_date or self.end_date:
                log.debug("Applying date filters...")
                combined_df = self._filter_by_date(combined_df)

            log.debug("Saving to CSV...")
            self._save_to_csv(combined_df, output_csv_path)

        finally:
            dss.close()

        log.info("Conversion completed successfully")

        metrics = {
            "pathnames": len(available_pathnames),
            "series": len(time_series_groups),
            "datetimes": len(all_datetimes),
            "csv": output_csv_path,
            "dss_type": self.dss_type,
        }
        return metrics

    def _adjust_timestamp(self, dt: datetime, adjustment: str) -> pd.Timestamp:
        ts_dt = pd.Timestamp(dt).normalize()

        if adjustment == "end_of_month":
            # If already month-end keep; else roll forward to month-end
            if ts_dt.is_month_end:
                return ts_dt
            return ts_dt + MonthEnd(0)
        elif adjustment == "start_of_month":
            return ts_dt.replace(day=1)
        else:  # 'none'
            return ts_dt

    def _create_output_dataframe(
        self, time_series_groups: Dict, all_datetimes: Set, config: Dict
    ) -> pd.DataFrame:
        sorted_datetimes = sorted(list(all_datetimes))
        sorted_keys = sorted(time_series_groups.keys(), key=lambda x: time_series_groups[x]["b"])

        dfs_to_concat = [pd.DataFrame({"DateTime": sorted_datetimes})]

        for series_key in sorted_keys:
            info = time_series_groups[series_key]
            column_name = config["column_name_format"].format(
                a=info["a"],
                b=info["b"],
                c=info["c"],
                d=info["d"],
                e=info["e"],
                f=info["f"],
            )
            series_data = [info["data"].get(dt, np.nan) for dt in sorted_datetimes]
            dfs_to_concat.append(pd.DataFrame({column_name: series_data}))

        combined = pd.concat(dfs_to_concat, axis=1)

        if combined.shape[1] > 1:
            combined = combined[combined.iloc[:, 1:].notna().any(axis=1)]

        header_mapping = config["header_mapping"]
        header_data = {"DateTime": header_mapping}

        for series_key in sorted_keys:
            info = time_series_groups[series_key]
            column_name = config["column_name_format"].format(
                a=info["a"],
                b=info["b"],
                c=info["c"],
                d=info["d"],
                e=info["e"],
                f=info["f"],
            )
            header_values = []
            for field in header_mapping:
                if field == "type":
                    header_values.append(info.get("type", ""))
                elif field == "units":
                    header_values.append(info["units"])
                else:
                    header_values.append(info.get(field, ""))
            header_data[column_name] = header_values

        header_df = pd.DataFrame(header_data)
        final_df = pd.concat([header_df, combined], ignore_index=True)
        return final_df

    def _filter_by_date(self, df: pd.DataFrame) -> pd.DataFrame:
        header_rows = len(self.processing_configs[self.dss_type]["header_mapping"])
        header_df = df.iloc[:header_rows]
        data_df = df.iloc[header_rows:].copy()

        data_df.iloc[:, 0] = pd.to_datetime(data_df.iloc[:, 0])

        if self.start_date:
            data_df = data_df[data_df.iloc[:, 0] >= self.start_date]
        if self.end_date:
            data_df = data_df[data_df.iloc[:, 0] <= self.end_date]

        return pd.concat([header_df, data_df], ignore_index=True)

    def _save_to_csv(self, df: pd.DataFrame, output_csv_path: str) -> None:
        output_dir = os.path.dirname(output_csv_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output_csv_path, index=False, header=False, na_rep="NaN")
        log.info("Exported CSV: %s", output_csv_path)


def export_all_paths_to_csv(dss_file_path, output_csv_path):
    processor = DSSProcessor(dss_type="calsim_output")
    return processor.process_dss_file(dss_file_path, output_csv_path)


def show_dss_info(dss_file_path: str):
    log.info("DSS File Information: %s", dss_file_path)
    if not os.path.exists(dss_file_path):
        log.error("DSS file does not exist: %s", dss_file_path)
        return
    dss = HecDss.Open(dss_file_path)
    try:
        pathnames = dss.getPathnameList("/*/*/*/*/*/*/")
        log.info("Total pathnames: %d", len(pathnames))
        for i, pathname in enumerate(pathnames[:10]):
            log.info("Sample %02d: %s", i + 1, pathname)
    finally:
        dss.close()


def _default_csv_name(dss_path: str) -> str:
    base = os.path.basename(dss_path)
    root, _ = os.path.splitext(base)
    return root + ".csv"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enhanced DSS to CSV converter supporting multiple DSS file types"
    )
    parser.add_argument("--dss", type=str, required=True, help="Path to input DSS file")
    parser.add_argument("--csv", type=str, help="Path to output CSV file")
    parser.add_argument(
        "--type",
        choices=["calsim_output", "sv_input", "auto"],
        default="auto",
        help="Type of DSS file (default: auto-detect)",
    )
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--frequency",
        choices=["monthly", "daily", "annual"],
        default="monthly",
        help="Data frequency (post-processing; not yet implemented)",
    )
    parser.add_argument(
        "--missing-value",
        type=float,
        default=-901,
        help="Value to treat as missing data (default: -901)",
    )
    parser.add_argument(
        "--timestamp-adjustment",
        choices=["end_of_month", "start_of_month", "none"],
        default="end_of_month",
        help="Timestamp adjustment (default: end_of_month)",
    )
    parser.add_argument(
        "--info", action="store_true", help="Show information about the DSS file and exit"
    )

    args = parser.parse_args()

    if args.info:
        show_dss_info(args.dss)
        sys.exit(0)

    csv_path = args.csv or _default_csv_name(args.dss)

    processor = DSSProcessor(
        dss_type=args.type,
        start_date=args.start_date,
        end_date=args.end_date,
        frequency=args.frequency,
        missing_value=args.missing_value,
        timestamp_adjustment=args.timestamp_adjustment,
    )

    metrics = processor.process_dss_file(args.dss, csv_path)
    # Emit metrics JSON line (easy to parse from CloudWatch)
    log.info("METRICS %s", json.dumps(metrics))