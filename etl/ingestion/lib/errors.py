"""Exception types for the ingestion pipeline.

Two kinds of failures:

- `IngestionError`: per-scenario, recoverable. Captured in the audit row for
  the scenario. The run continues with the remaining rows (skip-not-abort).
- `PreflightError`: run-level, fatal. Subclasses `SystemExit` so it walks
  cleanly out of `main()` without a stack trace, matching the bootstrap
  error path. Raised when the operator's environment is not ready (rclone
  missing/misconfigured, OAuth token revoked, S3 bucket unreachable).
"""

from __future__ import annotations


class IngestionError(Exception):
    """Per-scenario recoverable error. Captured in the audit, never aborts the run."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class PreflightError(SystemExit):
    """Run-level error raised when the operator's environment isn't ready.

    Subclasses SystemExit so it walks cleanly out of `main()` without a
    stack trace, just like the existing bootstrap-error path. Distinct
    from `IngestionError`, which is per-row and recoverable.
    """
