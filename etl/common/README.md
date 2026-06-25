# `etl/common/`

Shared constants and helpers for the COEQWAL ETL.

A single source of truth for AWS resource names, S3 key layout, database connection helpers, the active/ETL scenario sets, and the tier-location entity registry. Everything is re-exported from the package root, so a script imports straight from `etl.common`.

## What lives here

**AWS, S3, and database primitives**

| File | Purpose |
|------|---------|
| [`aws.py`](aws.py) | AWS resource constants (`S3_BUCKET`, `BATCH_QUEUE`, `BATCH_JOB_DEFINITION`, `AWS_REGION`, `LAMBDA_NAME`, `CLOUDWATCH_LAMBDA_LOG_GROUP`, `ECR_REPOSITORY`, `ECR_IMAGE_TAG`) plus the `read_json_from_s3(s3_client, bucket, key)` helper used by `audit.py` and `run_full_pipeline.py`. Override at runtime via env vars (`COEQWAL_S3_BUCKET`, `COEQWAL_BATCH_QUEUE`, `COEQWAL_BATCH_JOBDEF`). |
| [`s3_paths.py`](s3_paths.py) | The model-run bucket layout in one place: prefix constants (`STAGING_PREFIX`, `READY_PREFIX`, `SCENARIO_PREFIX`, `REFERENCE_PREFIX`) and key builders (`staging_prefix`, `ready_prefix`, `scenario_prefix`, `scenario_run_prefix`, `scenario_csv_prefix`, `scenario_verify_prefix`, `scenario_validation_prefix`, `scenario_zip_key`, `calsim_output_csv_key`, `sv_input_csv_key`, `ingest_record_key`, `extract_record_key`, `s3_url`). The module docstring diagrams the full bucket layout. |
| [`db.py`](db.py) | `get_db_connection(required=True)` and `get_database_url(required=True)`, reading `DATABASE_URL` with actionable errors. Raises `DatabaseUrlMissing` (a `RuntimeError` + `ValueError`) when required but unset. |

**Scenario sets and parsing**

| File | Purpose |
|------|---------|
| [`active_scenarios.py`](active_scenarios.py) | `ACTIVE_SCENARIOS` frozenset: the curated/public scenarios live on the website (`is_active` in the DB). Auto-generated, do not hand-edit. Regenerate with [`etl/ingestion/tools/refresh_active_scenarios.py`](../ingestion/tools/refresh_active_scenarios.py); change membership with [`set_scenario_active.py`](../ingestion/tools/set_scenario_active.py). Use for anything that must match what the website serves (tier uploads, tier verification). |
| [`etl_scenarios.py`](etl_scenarios.py) | `ETL_SCENARIOS` frozenset: the scenarios the ETL is meant to process (everything not `retired`/`skip` in the model-run source CSV). Auto-generated, do not hand-edit. Regenerate with [`etl/ingestion/tools/refresh_etl_scenarios.py`](../ingestion/tools/refresh_etl_scenarios.py). Use for anything running against raw ETL output or per-scenario statistics (`run_all.py --all-scenarios`, `verify_all_sections.py --all-scenarios`). |
| [`scenarios.py`](scenarios.py) | `parse_scenarios(...)` normalizes a `--scenarios` CLI argument (comma / whitespace / pasted spreadsheet column) into a set of short codes. `resolve_active_scenarios(...)` is the per-run override hatch for consumers that gate on `ACTIVE_SCENARIOS`. |

**Tier-location resolution**

| File | Purpose |
|------|---------|
| [`tier_location_entities.py`](tier_location_entities.py) | Registry mapping each tier `location_type` to where its display name lives, where its geometry lives, and the join key (`LOCATION_ENTITY_MAP`, the `AttributeResolver` / `GeometryResolver` types, coverage helpers). The single source of truth shared by the tier-data scripts, the DU geometry loader, and the tier API route. See [`etl/tier_data/README.md`](../tier_data/README.md). |

## How to import from a script under `etl/`

Scripts are typically invoked as `python etl/foo/bar.py` from the repo root. To make the `etl` package importable in that mode, add a three-line preamble, then import everything directly from `etl.common`:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[N]))  # N = depth to repo root

from etl.common import S3_BUCKET, calsim_output_csv_key, get_db_connection  # noqa: E402
```

For a script at `etl/ingestion/foo.py`, `N=2`. For `etl/statistics/ag/foo.py`, `N=3`.

The preamble is intentionally explicit (no hidden magic) so a new developer reading the script can see why it works. It also matches how the production runtimes load this code: the Batch container and Lambda zip ship the code with their own packaging and never run `pip install -e .`, so the dev pattern stays identical to prod. The full rationale lives in the [`etl/common/__init__.py`](__init__.py) docstring.

## Why constants live here and not in each script

One edit in `etl/common/aws.py`, or one env-var override at runtime, changes the bucket or Batch target everywhere. If the S3 key layout moves, fix `etl/common/s3_paths.py` and every consumer follows. The alternative, a literal `S3_BUCKET = "coeqwal-model-run"` in each script, means a search-and-replace across dozens of files and a silent drift whenever one is missed.

## Backward-compatible aliases

`aws.py` also exports the legacy names `DEFAULT_S3_BUCKET`, `JOB_QUEUE`, and `JOB_DEFINITION` as aliases of `S3_BUCKET`, `BATCH_QUEUE`, and `BATCH_JOB_DEFINITION`. They exist so older scripts keep working. New code should prefer the unprefixed names.
