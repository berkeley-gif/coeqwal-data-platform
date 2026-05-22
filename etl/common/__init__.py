"""Shared constants and helpers for the COEQWAL ETL: AWS resource names,
S3 key layout, database connection helpers.

Import everything from this package directly:

    from etl.common import S3_BUCKET, calsim_output_csv_key, get_db_connection

The sys.path preamble (and why every script has one)
----------------------------------------------------

Scripts under `etl/` are designed to be invoked directly as
`python etl/path/to/script.py` from the repo root, on Cloud9, in a local
venv, in the Batch container, or anywhere else. To make `etl.common`
importable in that context, each script starts with:

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[N]))
    from etl.common import S3_BUCKET  # noqa: E402

`N` is the depth from the script to the repo root. 2 for
`etl/ingestion/foo.py`, 3 for `etl/statistics/ag/foo.py`.

This is the intentional design, not a workaround. The alternative was to
declare `etl` as an installable package and add `-e .` to
`requirements.txt`. We picked the path manipulation because:

1. It is self-documenting at the call site. A reader opening any script
   sees the `sys.path.insert(...)` line and immediately understands the
   script's relationship to the rest of the codebase. The mechanism is
   local information, not an invisible side effect of a package install.
2. It matches how the production runtimes load this code. The Batch
   container and Lambda zip ship the code with their own packaging. They
   do not run `pip install -e .`. Cloud9 dev environments do not either.
   Keeping the dev pattern identical to the prod pattern reduces
   "works on my machine" surface area.
3. There is no setup-state to drift. Every run computes the repo root
   fresh from `__file__`. No re-install required after `git pull`. No
   stale `.pth` file when a Cloud9 environment is rebuilt or a venv is
   moved.

The `# noqa: E402` on the `from etl.common import ...` line acknowledges
the trade. It is not an apology.
"""

from etl.common.aws import (
    AWS_REGION,
    BATCH_JOB_DEFINITION,
    BATCH_QUEUE,
    CLOUDWATCH_LAMBDA_LOG_GROUP,
    DEFAULT_S3_BUCKET,
    ECR_IMAGE_TAG,
    ECR_REPOSITORY,
    JOB_DEFINITION,
    JOB_QUEUE,
    LAMBDA_NAME,
    S3_BUCKET,
    read_json_from_s3,
)
from etl.common.db import (
    DATABASE_URL_ENV,
    DatabaseUrlMissing,
    get_database_url,
    get_db_connection,
)
from etl.common.s3_paths import (
    EXTRACT_RECORD_BASENAME,
    INGEST_RECORD_BASENAME,
    READY_PREFIX,
    REFERENCE_PREFIX,
    SCENARIO_PREFIX,
    STAGING_PREFIX,
    calsim_output_csv_key,
    extract_record_key,
    ingest_record_key,
    ready_prefix,
    s3_url,
    scenario_csv_prefix,
    scenario_prefix,
    scenario_run_prefix,
    scenario_validation_prefix,
    scenario_verify_prefix,
    scenario_zip_key,
    staging_prefix,
    sv_input_csv_key,
)

__all__ = [
    # aws
    "AWS_REGION",
    "BATCH_JOB_DEFINITION",
    "BATCH_QUEUE",
    "CLOUDWATCH_LAMBDA_LOG_GROUP",
    "ECR_IMAGE_TAG",
    "ECR_REPOSITORY",
    "LAMBDA_NAME",
    "S3_BUCKET",
    # aws legacy aliases (kept for backwards compatibility)
    "DEFAULT_S3_BUCKET",
    "JOB_DEFINITION",
    "JOB_QUEUE",
    # aws helpers
    "read_json_from_s3",
    # db
    "DATABASE_URL_ENV",
    "DatabaseUrlMissing",
    "get_database_url",
    "get_db_connection",
    # s3_paths constants
    "EXTRACT_RECORD_BASENAME",
    "INGEST_RECORD_BASENAME",
    "READY_PREFIX",
    "REFERENCE_PREFIX",
    "SCENARIO_PREFIX",
    "STAGING_PREFIX",
    # s3_paths builders
    "calsim_output_csv_key",
    "extract_record_key",
    "ingest_record_key",
    "ready_prefix",
    "s3_url",
    "scenario_csv_prefix",
    "scenario_prefix",
    "scenario_run_prefix",
    "scenario_validation_prefix",
    "scenario_verify_prefix",
    "scenario_zip_key",
    "staging_prefix",
    "sv_input_csv_key",
]
