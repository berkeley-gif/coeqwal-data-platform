"""S3 path builders for the COEQWAL model-run bucket

Bucket layout (under `etl.common.aws.S3_BUCKET`):

    staging/scenario_data/<short_code>/   model run ZIP, trend report csv, ingest_record.json
    ready/<short_code>/                   promote target, ZIP PUT triggers Lambda
    scenario/<short_code>/ingest_record.json    ingestion-side contract (basenames, hashes, provenance)
    scenario/<short_code>/extract_record.json   Batch container's per-run record (validation counts inlined)
    scenario/<short_code>/run/            ZIP after Lambda moves it (kept here for the presign-download API)
    scenario/<short_code>/verify/         trend report CSV
    scenario/<short_code>/csv/            extracted CSVs
    scenario/<short_code>/validation/     <id>_validation_mismatches.csv (per-row debug, only when mismatches found)
"""

from __future__ import annotations

STAGING_PREFIX = "staging/scenario_data"
READY_PREFIX = "ready"
SCENARIO_PREFIX = "scenario"
REFERENCE_PREFIX = "reference"

INGEST_RECORD_BASENAME = "ingest_record.json"
EXTRACT_RECORD_BASENAME = "extract_record.json"


def staging_prefix(short_code: str) -> str:
    """S3 prefix where ingestion stages a scenario before promote."""
    return f"{STAGING_PREFIX}/{short_code}"


def ready_prefix(short_code: str) -> str:
    """S3 prefix where promote copies files. ZIP PUT here triggers Lambda."""
    return f"{READY_PREFIX}/{short_code}"


def scenario_prefix(short_code: str) -> str:
    """S3 prefix for everything a single scenario owns post-Lambda."""
    return f"{SCENARIO_PREFIX}/{short_code}"


def scenario_run_prefix(short_code: str) -> str:
    """S3 prefix where the original ZIP lives after Lambda moves it.
    """
    return f"{SCENARIO_PREFIX}/{short_code}/run"


def scenario_verify_prefix(short_code: str) -> str:
    """S3 prefix where Lambda places the trend reference CSV."""
    return f"{SCENARIO_PREFIX}/{short_code}/verify"


def scenario_csv_prefix(short_code: str) -> str:
    """S3 prefix where the Batch container writes extracted CSVs."""
    return f"{SCENARIO_PREFIX}/{short_code}/csv"


def scenario_validation_prefix(short_code: str) -> str:
    """S3 prefix where the Batch container writes per-scenario validation
    reports comparing extracted CSVs against the trend reference."""
    return f"{SCENARIO_PREFIX}/{short_code}/validation"


def ingest_record_key(prefix: str) -> str:
    """`ingest_record.json` lives at `<prefix>/ingest_record.json`.

    Used at three points in the lifecycle, all with the same shape:
    `staging/scenario_data/<id>/`, `ready/<id>/`, and `scenario/<id>/`.
    The contents are the ingestion-side contract: declared basenames,
    SHA-256 hashes, sizes, provenance. Consumers: Batch container (for
    validation), `tools/audit.py`.
    """
    return f"{prefix}/{INGEST_RECORD_BASENAME}"


def extract_record_key(short_code: str) -> str:
    """S3 key for the Batch container's extraction record (per scenario)."""
    return f"{SCENARIO_PREFIX}/{short_code}/{EXTRACT_RECORD_BASENAME}"


def calsim_output_csv_key(short_code: str) -> str:
    """S3 key for the extracted CalSim (DV) output CSV."""
    return f"{scenario_csv_prefix(short_code)}/{short_code}_coeqwal_calsim_output.csv"


def sv_input_csv_key(short_code: str) -> str:
    """S3 key for the extracted SV (state-variable) input CSV."""
    return f"{scenario_csv_prefix(short_code)}/{short_code}_coeqwal_sv_input.csv"


def scenario_zip_key(short_code: str, zip_basename: str) -> str:
    """S3 key for a scenario's ZIP after Lambda moves it under scenario/."""
    return f"{scenario_run_prefix(short_code)}/{zip_basename}"


def s3_url(bucket: str, key: str) -> str:
    """Render an s3:// URL for logging and audit output."""
    return f"s3://{bucket}/{key}"
