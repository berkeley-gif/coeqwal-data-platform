"""AWS resource constants and small AWS helpers shared across the COEQWAL ETL.

Anything that names a real AWS resource (bucket, queue, job definition,
region, image tag, IAM role) lives here, alongside thin helpers that wrap
boto3 calls used from more than one script.

Override at runtime via environment variables where appropriate (see each
constant). Hardcoded defaults match what is deployed today (May 21, 2026).
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

S3_BUCKET = os.getenv("COEQWAL_S3_BUCKET") or os.getenv("S3_BUCKET", "coeqwal-model-run")
"""Primary S3 bucket holding scenario ZIPs, extracted CSVs, sidecars, and
manifests. Override with `COEQWAL_S3_BUCKET` (preferred) or `S3_BUCKET`
(legacy, still honored for backwards compatibility with statistics scripts)."""

AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
"""AWS region. boto3 picks this up too via standard AWS_REGION, repeated
here so callers can pass it explicitly when needed."""

BATCH_QUEUE = os.getenv("COEQWAL_BATCH_QUEUE", "coeqwal-dss-queue")
"""AWS Batch job queue that the Lambda submits DSS-to-CSV extraction
jobs to. Fargate Spot compute environment underneath."""

BATCH_JOB_DEFINITION = os.getenv("COEQWAL_BATCH_JOBDEF", "coeqwal-dss-jobdef")
"""AWS Batch job definition (bare name without revision number).
Batch automatically resolves to whichever revision is currently active, so
bumping container memory does NOT require a code change.
Create a new revision in the AWS console. Only edit this constant if you rename
the job definition itself."""

LAMBDA_NAME = "coeqwalEtlTrigger"
"""Name of the Lambda function that fires on `ready/*.zip` PUT events."""

ECR_REPOSITORY = "coeqwal-etl"
"""ECR repository name for the Batch container image."""

ECR_IMAGE_TAG = "coeqwal-etl:latest"
"""Image tag the Batch job definition points at. Built and pushed by the
GitHub Actions workflow on every push to main that touches batch-container/."""

CLOUDWATCH_LAMBDA_LOG_GROUP = f"/aws/lambda/{LAMBDA_NAME}"
"""CloudWatch log group for the trigger Lambda."""


# Legacy aliases. Older scripts imported these names. New code should use
# the unprefixed names above. Kept here so the migration to etl.common does
# not accidentallybreak any caller.
DEFAULT_S3_BUCKET = S3_BUCKET
JOB_QUEUE = BATCH_QUEUE
JOB_DEFINITION = BATCH_JOB_DEFINITION


def read_json_from_s3(s3_client: Any, bucket: str, key: str) -> Optional[dict]:
    """Fetch and parse a JSON object from S3.

    Returns the parsed dict, or `None` when the key does not exist or the
    body is not valid JSON. Errors other than missing-key (network, IAM)
    propagate to the caller.

    Used by `audit.py`, `run_full_pipeline.py`, and any other reader that
    needs the contents of one of the small per-scenario JSONs (sidecar,
    manifest, etc) without having to repeat the boto3 + decode boilerplate.
    """
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        return None
    except s3_client.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    try:
        return json.loads(obj["Body"].read())
    except json.JSONDecodeError:
        return None
