"""Shared constants and helpers for the COEQWAL ETL like AWS resource names, S3 key layout, database connection helpers.

Import everything from this package directly:

    from etl.common import S3_BUCKET, calsim_output_csv_key, get_db_connection

Scripts invoked as `python etl/foo/bar.py` from the repo root need this
preamble first so the `etl` package is importable:

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[N]))

`N` is the depth from the script to the repo root (2 for
`etl/ingestion/foo.py`, 3 for `etl/statistics/ag/foo.py`).
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
