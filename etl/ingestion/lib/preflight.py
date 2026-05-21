"""Pre-flight orchestrator for the ingestion CLI.

Environment-readiness checks run once at the top of a subcommand, before
any row is touched. Fail the whole run in seconds with one actionable
message instead of failing N times in the per-row loop.

Four checks, in order:
  1. rclone is on PATH
  2. rclone remote (default `gdrive:`) is registered locally
  3. rclone OAuth token actually works (light Drive API call)
  4. S3 bucket is reachable (AWS creds + bucket name)

Which subcommand runs which:
  download             1, 2, 3, 4   full check
  download --dry-run   1, 2, 3      no S3 writes, so no AWS creds needed
  scan                 1, 2, 3      never touches S3
  scan --local-only    none         skips Drive too
  promote              none         boto3 surfaces AWS errors on first call
"""

from __future__ import annotations

import logging

import boto3

from .errors import PreflightError
from .rclone import (
    _preflight_rclone_auth,
    _preflight_rclone_installed,
    _preflight_rclone_remote,
)

log = logging.getLogger("gdrive_bulk_download")


def _preflight_s3_bucket(s3_bucket: str) -> None:
    """Confirm AWS credentials are present and the target bucket is reachable."""
    try:
        s3 = boto3.client("s3")
        s3.head_bucket(Bucket=s3_bucket)
    except Exception as e:
        raise PreflightError(
            f"\n[preflight] S3 bucket '{s3_bucket}' is not reachable: {e}\n"
            f"Check AWS credentials and the bucket name:\n"
            f"  aws sts get-caller-identity\n"
            f"  aws s3 ls s3://{s3_bucket}/\n"
        )


def _preflight(rclone_remote: str, s3_bucket: str = "",
               include_s3: bool = True) -> None:
    """Run all pre-flight checks. Raises PreflightError (a SystemExit) on failure.

    `include_s3=False` runs only the rclone-side checks (installed,
    remote registered, OAuth valid). Used by `scan` (which never touches
    S3) and by `download --dry-run` (which lists Drive but never writes
    to S3, so it doesn't need AWS creds; useful for iterating from a
    machine that doesn't have prod AWS creds).
    """
    _preflight_rclone_installed()
    _preflight_rclone_remote(rclone_remote)
    _preflight_rclone_auth(rclone_remote)
    if include_s3:
        _preflight_s3_bucket(s3_bucket)
        log.info("Pre-flight checks passed (rclone=%s:, s3=%s).",
                 rclone_remote, s3_bucket)
    else:
        log.info("Pre-flight checks passed (rclone=%s:, S3 skipped).", rclone_remote)
