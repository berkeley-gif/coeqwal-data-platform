# audits/

Full-database snapshots and verification reports.

## What is tracked

Only `monthly_*/` directories and this README. Everything else under
`audits/` is gitignored.

- `monthly_<ts>/` - schema, content, and result-table snapshots from
  [`database/audit/run_monthly_audit.py`](../database/audit/run_monthly_audit.py).
  Latest: `monthly_20260524_143951/` (generated 2026-05-24 on Cloud9,
  downloaded locally).

## What is gitignored (and why)

- `audits/verification_reports/` - per-scenario JSON output from
  `verify_all_sections.py`, `verify_api.py`, and `verify_tiers.py`.
  Regenerable from a DB connection + the reference CSVs.
- `audits/validation_mismatches/` - local mirrors of S3 records
  (`<scenario>_extract_record.json`, `<scenario>_validation_mismatches.csv`).
  The S3 copies are the source of truth, so the local mirror is a
  convenience.
- `audits/*.tar.gz` and `audits/monthly_*/*.tar.gz` - tarballs that
  duplicate the unzipped snapshot contents.

The policy lives in [`.gitignore`](../.gitignore) (the `/audits/*`
block). It uses default-block plus whitelist so a new subdirectory
under `audits/` is gitignored by default. See
[`etl/verification/README.md` section 15](../etl/verification/README.md#15-git-tracking-policy)
for the full rationale and how this relates to ETL verification layers.

## Regenerate

The first two commands are model-run pipeline verification (Layers 2 and
3 in [`etl/verification/README.md`](../etl/verification/README.md)).
The third belongs to the tier-data pipeline, see
[`etl/tier_data/README.md`](../etl/tier_data/README.md). They are
listed together because all three write to
`audits/verification_reports/`, not because they share a runbook.

```bash
# Full monthly snapshot (schema + content + result-table samples)
python database/audit/run_monthly_audit.py

# Model-run pipeline verification for one scenario.
# Layer 2 (experimental, under development): spot check on hand-curated
# entities. Requires the DV + SV CSVs to be in etl/reference/ first.
# See etl/verification/README.md Layer 2 for scope and maintenance tax.
python etl/statistics/verify_all_sections.py --scenario s0020
# Layer 3: API responses vs direct DB queries
python etl/statistics/verify_api.py --scenario s0020

# Tier-data pipeline verification (Layer 3-tier)
python etl/tier_data/scripts/verify_tiers.py --scenario s0020
```
