#!/usr/bin/env python3
"""
gdrive_bulk_download.py - CLI entry point for the Google Drive -> S3 ingestion pipeline.

Three subcommands, meant to be run in this order. Each operates on rows
of the working CSV (`etl/ingestion/scenario_listing/model_run_file_source_working.csv`):

  scan      List Drive contents per row. Read-only: no downloads, no S3
            writes. Use it to catch CSV problems before spending bandwidth.
  download  For each row, pull the ZIP, validate against the expected
            SV/DV basenames, hash everything, build the ingest record,
            and upload to `staging/scenario_data/<id>/`. Auto-renders
            `audit.md` at the end. Skip-not-abort: per-row errors land
            in the audit, the run continues.
  promote   Copy each scenario's staged files to `ready/<id>/` in the
            safe order (ingest_record.json -> trend csv -> ZIP last).
            The ZIP PUT under `ready/` is the Lambda trigger that
            releases extraction.

Usage (run from repo root):

  python etl/ingestion/gdrive_bulk_download.py scan --all
  python etl/ingestion/gdrive_bulk_download.py download --all
  python etl/ingestion/gdrive_bulk_download.py promote
  python etl/ingestion/gdrive_bulk_download.py download --scenarios s0042 s0043

Each subcommand has its own `--help` with the full flag list.

This file is a thin argparse shim. The actual
implementation modules live in `lib/`. Auxiliary CLIs (manual upload, recovery,
post-extraction verification) live in `tools/`. See `tools/README.md`.

Outputs:

  s3://<bucket>/staging/scenario_data/<id>/<zip,trend.csv,ingest_record.json>
  etl/ingestion/audit_reports/ingest_state.json  per-run scan + download record
  etl/ingestion/audit.md                         auto-rendered at end of download
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from etl.common import (  # noqa: E402
    READY_PREFIX,
    S3_BUCKET as DEFAULT_S3_BUCKET,
    STAGING_PREFIX,
)
from etl.ingestion.lib.commands import (  # noqa: E402
    cmd_download,
    cmd_promote,
    cmd_scan,
)
from etl.ingestion.lib.config import (  # noqa: E402, F401 (re-exported for back-compat)
    DEFAULT_OUTPUT_DIR,
    INGEST_RECORD_SCHEMA_VERSION,
    INGEST_STATE_PATH,
    SCRIPT_VERSION,
    SPREADSHEET_URL,
    WORKING_CSV_PATH,
)
from etl.ingestion.lib.csv_reader import (  # noqa: E402, F401 (re-exported for back-compat)
    _require_working_csv,
    read_scenario_source_csv,
)
from etl.ingestion.lib.utils import (  # noqa: E402, F401 (re-exported for back-compat)
    _now_iso_utc,
    _operator_tag,
)


# Shared argparse flag builders
#
# These helpers exist so flag declarations are defined only once.


def _add_listing_csv(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--listing-csv", default=WORKING_CSV_PATH,
                        help=f"Path to working CSV (default: {WORKING_CSV_PATH})")


def _add_rclone_remote(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--rclone-remote", default="gdrive",
                        help="Name of the rclone remote (default: gdrive)")


def _add_workers(parser: argparse.ArgumentParser, kind: str) -> None:
    parser.add_argument("--workers", type=int, default=4,
                        help=f"Number of concurrent {kind} workers (default: 4)")


def _add_scenarios_filter(parser: argparse.ArgumentParser, verb: str) -> None:
    """Add the `--scenarios` / `--all` pair used by `download` and `scan`.

    `promote` has its own slightly different `--scenarios` (no `--all`
    counterpart, different default-behavior wording) and is declared
    inline rather than going through this helper.
    """
    parser.add_argument("--scenarios", nargs="*",
                        help=f"Scenario short codes to {verb}. "
                             "Whitespace or comma-separated. Newlines from a "
                             "spreadsheet column paste also work. "
                             "Example: --scenarios s0042 s0043. "
                             "Either --scenarios or --all is required.")
    parser.add_argument("--all", action="store_true",
                        help=f"{verb.capitalize()} every row in the working CSV. "
                             "Either --scenarios or --all is required.")


def _add_output_dir(parser: argparse.ArgumentParser, output_files: str) -> None:
    parser.add_argument("--output-dir", default=None,
                        help=f"Directory for {output_files} (default: "
                             f"{DEFAULT_OUTPUT_DIR}). Auto-created if missing.")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    parser = argparse.ArgumentParser(
        description="Bulk download model runs from Google Drive to S3 via rclone"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sc = sub.add_parser("scan",
                        help="Scan Drive contents using the working CSV")
    _add_listing_csv(sc)
    _add_rclone_remote(sc)
    _add_workers(sc, "scan")
    _add_scenarios_filter(sc, "scan")
    sc.add_argument("--local-only", action="store_true",
                    help="Parse CSV and write manifest without Drive access")
    _add_output_dir(sc, "ingest_state.json")

    dl = sub.add_parser("download", help="Download, validate, and stage to S3")
    _add_listing_csv(dl)
    dl.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET,
                    help=f"S3 bucket for staging (default: {DEFAULT_S3_BUCKET})")
    _add_workers(dl, "download")
    _add_scenarios_filter(dl, "process")
    _add_rclone_remote(dl)
    dl.add_argument("--dry-run", action="store_true",
                    help="List files without downloading")
    _add_output_dir(dl, "ingest_state.json")
    dl.add_argument("--skip-audit", action="store_true",
                    help="Do not auto-render audit.md at the end of the run. "
                         "Re-run `python etl/ingestion/tools/audit.py` manually later.")

    pr = sub.add_parser("promote",
                        help=f"Copy staged files from {STAGING_PREFIX}/<id>/ to "
                             f"{READY_PREFIX}/<id>/. "
                             f"Upload order: ingest_record.json -> trend csv -> ZIP last.")
    pr.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET,
                    help=f"S3 bucket (default: {DEFAULT_S3_BUCKET})")
    pr.add_argument("--scenarios", nargs="*",
                    help="Scenario short codes to promote. Whitespace or comma-separated; "
                         "newlines from a spreadsheet paste also work. "
                         "Example: --scenarios s0042 s0043. "
                         "Default: every scenario currently in staging.")
    pr.add_argument("--dry-run", action="store_true",
                    help="Print the planned copy order without copying. Use this before "
                         "a real promote to see what will fire Lambda")

    args = parser.parse_args()
    if args.command == "scan":
        cmd_scan(args)
    elif args.command == "download":
        cmd_download(args)
    elif args.command == "promote":
        cmd_promote(args)


if __name__ == "__main__":
    main()
