"""S3 path builders for the COEQWAL model-run bucket

Bucket layout (under `etl.common.aws.S3_BUCKET`):

    staging/scenario_data/<short_code>/   model run ZIP, trend report csv, sidecar.json
    ready/<short_code>/                   promote target, ZIP PUT triggers Lambda
    scenario/<short_code>/run/            ZIP, sidecar.json (plus lambda_status.json after Pass 2b)
    scenario/<short_code>/verify/         trend report CSV
    scenario/<short_code>/csv/            extracted CSVs + .units.json sidecars
    scenario/<short_code>/<id>_manifest.json    Batch container's per-run record (validation counts inlined)
    scenario/<short_code>/validation/     <id>_validation_mismatches.csv (per-row debug, only when mismatches found)
"""

from __future__ import annotations

STAGING_PREFIX = "staging/scenario_data"
READY_PREFIX = "ready"
SCENARIO_PREFIX = "scenario"
REFERENCE_PREFIX = "reference"


def staging_prefix(short_code: str) -> str:
    """S3 prefix where ingestion stages a scenario before promote."""
    return f"{STAGING_PREFIX}/{short_code}"


def ready_prefix(short_code: str) -> str:
    """S3 prefix where promote copies files. ZIP PUT here triggers Lambda."""
    return f"{READY_PREFIX}/{short_code}"


def scenario_run_prefix(short_code: str) -> str:
    """S3 prefix where Lambda moves ZIP + sidecar after firing."""
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


def scenario_manifest_key(short_code: str) -> str:
    """S3 key for the per-scenario manifest the Batch container writes."""
    return f"{SCENARIO_PREFIX}/{short_code}/{short_code}_manifest.json"


def calsim_output_csv_key(short_code: str) -> str:
    """S3 key for the extracted CalSim (DV) output CSV."""
    return f"{scenario_csv_prefix(short_code)}/{short_code}_coeqwal_calsim_output.csv"


def sv_input_csv_key(short_code: str) -> str:
    """S3 key for the extracted SV (state-variable) input CSV."""
    return f"{scenario_csv_prefix(short_code)}/{short_code}_coeqwal_sv_input.csv"


def scenario_zip_key(short_code: str, zip_basename: str) -> str:
    """S3 key for a scenario's ZIP after Lambda moves it under scenario/."""
    return f"{scenario_run_prefix(short_code)}/{zip_basename}"


def sidecar_key(prefix: str) -> str:
    """`sidecar.json` is always at `<prefix>/sidecar.json`. Use with any of
    the `*_prefix` builders above."""
    return f"{prefix}/sidecar.json"


def s3_url(bucket: str, key: str) -> str:
    """Render an s3:// URL for logging and audit output."""
    return f"s3://{bucket}/{key}"
