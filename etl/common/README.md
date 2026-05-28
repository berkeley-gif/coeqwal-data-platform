# `etl/common/`

Shared constants and helpers for the COEQWAL ETL.

A single source of truth for things that were copy-pasted across 20+
scripts before: AWS resource names, S3 key layout, database connection
helpers, and (in later phases) shared logging and IO helpers.

## What lives here

| File | Purpose |
|------|---------|
| [`aws.py`](aws.py) | AWS resource constants (`S3_BUCKET`, `BATCH_QUEUE`, `BATCH_JOB_DEFINITION`, `AWS_REGION`, `LAMBDA_NAME`, `ECR_*`) plus the `read_json_from_s3(s3_client, bucket, key)` helper used by `audit.py` and `run_full_pipeline.py`. Override at runtime via env vars (`COEQWAL_S3_BUCKET`, `COEQWAL_BATCH_QUEUE`, `COEQWAL_BATCH_JOBDEF`). |
| [`s3_paths.py`](s3_paths.py) | Builders for S3 keys: `staging_prefix(short_code)`, `ready_prefix`, `scenario_prefix`, `scenario_run_prefix`, `scenario_csv_prefix`, `calsim_output_csv_key`, `sv_input_csv_key`, `ingest_record_key`, `extract_record_key`, `s3_url`. |
| [`db.py`](db.py) | `get_db_connection(required=True)` reading `DATABASE_URL` with actionable errors. Raises `DatabaseUrlMissing` when required but unset. |

Future additions (later phases):

| File | Purpose |
|------|---------|
| `logging.py` | Shared `get_logger(name, scenario=None)` with timestamps, file + console handlers, banner/footer helpers (Phase 0.5) |

## How to import from a script under `etl/`

Scripts are typically invoked as `python etl/foo/bar.py` from the repo
root. To make the `etl` package importable in that mode, add a
three-line preamble, then import everything directly from `etl.common`:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[N]))  # N = depth to repo root

from etl.common import S3_BUCKET, calsim_output_csv_key, get_db_connection
```

For a script at `etl/ingestion/foo.py`, `N=2`. For `etl/statistics/ag/foo.py`, `N=3`.

The preamble is intentionally explicit (no hidden magic) so a new
developer reading the script can see why it works. After the preamble,
one import line pulls whatever symbols you need from `etl.common`.

## Why constants live here and not in each script

Before: `S3_BUCKET = "coeqwal-model-run"` was written 20+ times across
the ETL. Renaming the bucket or moving to a staging account meant a
search-and-replace across dozens of files, and any script that missed
the memo silently kept writing to the old place.

Now: one edit in `etl/common/aws.py`, or one env var override at
runtime. The same applies to the S3 key layout - if `staging/` ever
moves, fix `etl/common/s3_paths.py`.

## Backward compatibility

`etl/common/aws.py` re-exports legacy names (`DEFAULT_S3_BUCKET`,
`JOB_QUEUE`, `JOB_DEFINITION`) so scripts that imported those names
from `etl/ingestion/gdrive_bulk_download.py` keep working during the
migration. New code should prefer the unprefixed names.
