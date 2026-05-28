# ingestion/tools

Auxiliary CLIs for the ingestion pipeline. The pipeline itself runs from
`gdrive_bulk_download.py` one level up in [`etl/ingestion/`](../). The
pipeline overview and the manual-upload contract live in
[`etl/README.md`](../../README.md). This file just sorts the tools here
by use case.

## When to reach for what

### After ingestion

- **`audit.py`** -- regenerates `etl/ingestion/audit.md`, the one-file
  status report for the whole pipeline. Reads `ingest_record.json`
  (ingestion side) and `extract_record.json` (Batch container side) for
  every active scenario in S3, plus the `download` block of
  `ingest_state.json` from the most recent local run. Auto-runs at the
  end of `gdrive_bulk_download.py download`. Re-run by hand after Batch
  finishes.

  ```
  python etl/ingestion/tools/audit.py
  ```

- **`show_last_run.py`** -- prints a quick summary of the most recent
  ingest stage(s). Default is the last `download`. Pass
  `--stage scan` or `--stage all` to print the scan block, or both
  blocks back-to-back.

  ```
  python etl/ingestion/tools/show_last_run.py
  python etl/ingestion/tools/show_last_run.py --stage scan
  python etl/ingestion/tools/show_last_run.py --stage all
  ```

### Recovery: re-trigger extraction

Two tools, two code paths. Both re-run Batch on a ZIP that is already in
S3 at `scenario/<id>/run/`.

- **`retrigger_extraction.sh`** -- the default. Copies the ZIP from
  `scenario/<id>/run/` back to `ready/`. The S3 PUT fires the Lambda,
  which dispatches Batch through the production path. Use this unless
  you need an override.

  ```
  bash etl/ingestion/tools/retrigger_extraction.sh --go s0020
  ```

- **`reextract_all_scenarios.py`** -- the surgical version. Calls
  `batch.submit_job()` directly, bypassing the Lambda. Use when you
  need one of the override knobs:

  | flag | what it does |
  |---|---|
  | `--validate` | Have the container compare its output against a known-good CSV at `scenario/<id>/verify/`. Regression testing after a code change. |
  | `--memory` / `--vcpus` | Bump the container's resource request for one run. Use when a very-large DSS OOMs at the default. Permanent fix is bumping the job definition. |
  | `--sv-only` / `--dv-only` | Re-extract one side only. Skips the other side at the container, saving Batch minutes. Mutually exclusive. |

  ```
  python etl/ingestion/tools/reextract_all_scenarios.py --help
  ```

### Alternative entry path (not from Drive)

- **`manual_ingest.py`** -- upload a ZIP. Builds the ingest record for you.
  Subcommands: `upload` (ZIP + ingest record + optional trend CSV in safe
  order) and `ingest-record` (write an ingest record for an existing ZIP
  and optionally retrigger Batch).

  ```
  python etl/ingestion/tools/manual_ingest.py --help
  ```

### Very occassionally

- **`refresh_active_scenarios.py`** -- rewrites the active-scenarios
  block at the top of `etl/README.md` from the live API. Run when that
  block is visibly stale.

  ```
  python etl/ingestion/tools/refresh_active_scenarios.py
  ```

For the one-time backfill of `ingest_record.json` for scenarios that
landed in S3 before the contract existed, see
[`etl/archive/oneshot_scripts/backfill_ingest_records.py`](../../archive/oneshot_scripts/backfill_ingest_records.py).
