# Database audit tools

This audit runs and does real, useful work (see [What it still does well](#what-it-still-does-well)). The code has served its purpose well, but currently it needs too much hand-editing and inside care for direct handoff. It's good code, serves a good purpose, and has been very helpful, and it does run, but it will need work to keep it in order. Specifically:

- **The list of tables to export is hand-maintained in Python and drifts silently.** Two lists at the top of `run_monthly_audit.py` (`LAYERS`, `RESULTS_TABLES`) decide what gets exported and sampled. Nothing checks them against the live schema, so a newly added table is quietly left out with no warning.
- **Section 1b (Schema vs. ERD) no longer runs.** It opens a renamed file and parses an old ERD format, so it silently skips.
- **Expected row counts are hardcoded** (`EXPECTED_COUNTS`), so ordinary data changes surface as false failures, which trains readers to ignore the audit.
- **Layer 09 (TIER) is missing from the export**, a concrete instance of the drift above.

The bones of this audit are healthy: the inventory, integrity, health, and cost sections read the live database directly, and it already reconciles the `domain_family_map` registry against `pg_tables`. This will continue to work. The weaknesses above are the curated layer on top. Each is detailed with a suggested fix in [Improving the audit](#improving-the-audit), roughly ordered by value over effort. A new maintainer could start there.

## What it still does well

Even with the rough edges above, a single run gives you a lot, and most of it reads the live database directly, so it stays accurate even where the curated lists lag:

- **A complete table inventory** with columns, row counts, and sizes for every table in the database (section 1a, straight from `pg_tables`).
- **Data-integrity checks**: NULL audit fields, orphaned rows, invalid values, and `coeqwal_developer` group membership (2a).
- **ETL coverage**: per-scenario row counts across all results tables, so you can see which scenarios are loaded (2b).
- **Database health and cost**: cache hit ratio, connections, dead tuples, bloat, table sizes, and unused indexes (3 and 4).
- **A registry cross-check**: `domain_family_map` against the live tables, flagging tables that are unregistered or registered but absent.
- **Downloadable snapshots**: full CSV exports of the reference layers it does cover, head/tail samples of the results tables, and a `schema_snapshot.json` you can diff over time.

If you need an inventory, an integrity pass, or a health and cost check, it is worth running. The known gaps mainly affect the completeness of the reference-layer content export (Layer 09) and the ERD comparison (1b).


## Monthly audit

The primary audit tool. One command produces a comprehensive report covering content, verification, health, and cost.

### Running the audit

From Cloud9, with `DATABASE_URL` set:

```bash
cd ~/environment/coeqwal-backend
python database/audit/run_monthly_audit.py
```

### What it produces

A timestamped folder under `audits/`:

```
audits/monthly_YYYYMMDD_HHMMSS/
├── report.md                       The Markdown report you read
├── schema_snapshot.json            Full schema snapshot
├── tables_summary.csv              Per-table row counts + audit field status
├── layer_exports/                  Full CSV exports for layers 00-08
│   ├── 00_versioning/
│   │   ├── developer.csv
│   │   ├── role_memberships.csv    pg_auth_members (e.g. coeqwal_developer)
│   │   ├── version_family.csv
│   │   └── ...
│   ├── 01_lookup/
│   └── ...through 08_theme/
└── results_samples/                First 10 + last 10 rows for layers 10+
    ├── reservoir_storage_monthly_head.csv
    ├── reservoir_storage_monthly_tail.csv
    └── ...
```

### Report sections

| # | Section | What it checks |
|---|---------|----------------|
| 1a | Table inventory | Every table: name, layer, columns, rows, size |
| 1b | Schema vs. ERD | Tables/columns in DB but not ERD, and vice versa. **Currently skipped** (see note below) |
| 1c | Row counts vs. expected | Layers 00-08 counts against known targets |
| 1d | Reference data downloads | Full CSV export of layers 00-08 |
| 1e | Results data samples | Head/tail CSV samples of layers 10+ |
| 2a | Data integrity | NULL audit fields, orphaned rows, invalid values, `coeqwal_developer` group membership |
| 2b | ETL coverage | Per-scenario row counts across all results tables |
| 2c | ETL accuracy summary | Reads existing ETL statistics and API verification reports (`verify_all_sections.py` / `verify_api.py` output, see [The `audits/` directory](#the-audits-directory)) |
| 3a-d | Database health | Cache hit ratio, connections, dead tuples, bloat |
| 4a-c | Database cost | Table sizes, unused indexes, total storage |
| 5 | Audit summary | PASS/FAIL for each check, with details on failures |

Notes:

- Section 1b does not run today. `verify_erd_against_audit.py` hardcodes an old ERD filename (`COEQWAL_SCENARIOS_DB_ERD.md`, now `ERD.md`) and expects the old tree-format ERD, so the comparison is silently skipped. See [Other audit tools](#other-audit-tools) and [`../SCHEMA_BACKLOG.md`](../SCHEMA_BACKLOG.md) § 10.
- "Layers 00-08" in 1c and 1d is the current export scope. Layer 09 (TIER) is not yet covered, see [Known gap, Layer 09](#known-gap-layer-09-tier).

### Skipping sections

```bash
# Skip health checks (e.g. when focused only on data content)
python database/audit/run_monthly_audit.py --skip health

# Skip multiple sections
python database/audit/run_monthly_audit.py --skip health --skip cost

# Valid sections: content | verification | health | cost
```

### Prerequisites

```bash
pip install psycopg2-binary pandas
```

### How it works internally

The script is self-contained. All schema snapshot, layer export, and sampling logic is built in. The only sibling import is `verify_erd_against_audit.py` for ERD comparison (same `database/audit/` directory). No Lambda, no `sys.path` hacks, no AWS dependencies.

Database connections are opened as `readonly=True`. The script never writes to the database.

---

## The `audits/` directory

Snapshots and verification reports live under `audits/`. Most of it is gitignored, only the monthly snapshots are committed.

### What is tracked

Only `monthly_*/` directories are committed. Everything else under `audits/` is gitignored.

- `monthly_<ts>/` holds the schema, content, and result-table snapshots from `run_monthly_audit.py`. Latest committed: `monthly_20260524_143951/` (generated 2026-05-24 on Cloud9, downloaded locally).

### What is gitignored (and why)

- `audits/verification_reports/` is per-scenario JSON from `verify_all_sections.py`, `verify_api.py`, and `verify_tiers.py`. Regenerable from a DB connection plus the reference CSVs.
- `audits/validation_mismatches/` are local mirrors of S3 records (`<scenario>_extract_record.json`, `<scenario>_validation_mismatches.csv`). The S3 copies are the source of truth, so the local mirror is a convenience.
- `audits/*.tar.gz` and `audits/monthly_*/*.tar.gz` are tarballs that duplicate the unzipped snapshot contents.

The policy lives in [`.gitignore`](../../.gitignore) (the `/audits/*` block). It uses default-block plus whitelist, so a new subdirectory under `audits/` is gitignored by default. See [`etl/verification/README.md` ("Receipts: the paper trail")](../../etl/verification/README.md#receipts-the-paper-trail) for the full rationale and how it relates to ETL verification layers.

### Regenerating the contents

The `monthly_*/` snapshot comes from `run_monthly_audit.py` (see [Running the audit](#running-the-audit)). The gitignored `verification_reports/` come from the three verification scripts, which all write there (grouped for that reason, not because they share a runbook): `verify_all_sections.py` and `verify_api.py` are model-run verification (Layers 2 and 3 in [`etl/verification/README.md`](../../etl/verification/README.md)), and `verify_tiers.py` belongs to the tier-data pipeline ([`etl/tier_data/README.md`](../../etl/tier_data/README.md)).

```bash
python etl/statistics/verify_all_sections.py --scenario s0020 
python etl/statistics/verify_api.py --scenario s0020            # API vs DB
python etl/tier_data/scripts/verify_tiers.py --scenario s0020 
```

---

## Other audit tools

These standalone tools can also be used independently.

### `verify_erd_against_audit.py`

Compares ERD documentation against a database audit snapshot. Imported by the monthly audit for section 1b, but **has drifted**. It hardcodes the old ERD filename and its parser expects the old tree-format ERD while `database/schema/ERD.md` is now Markdown tables, so the comparison is silently skipped. See [`../SCHEMA_BACKLOG.md`](../SCHEMA_BACKLOG.md) § 10 before relying on it.

```bash
python database/audit/verify_erd_against_audit.py \
    database/schema/ERD.md \
    audits/latest.json

# JSON output for scripting
python database/audit/verify_erd_against_audit.py \
    database/schema/ERD.md \
    audits/latest.json --json
```

### `generate_erd_from_audit.py`

Generates a draft ERD from an audit snapshot. Use this when the ERD needs updating:

```bash
python database/audit/generate_erd_from_audit.py \
    audits/latest.json \
    database/schema/GENERATED_ERD.md
```

### `export_layer_tables.py`

Standalone CSV export of layers 00-08 (the monthly audit has its own built-in export, but this script is useful for quick one-off checks):

```bash
python database/scripts/export_layer_tables.py
python database/scripts/export_layer_tables.py --layer 06
```

### `run_audit.sh`

Quick schema-only snapshot (no health/cost/content checks). Produces `audits/audit_*.json` and `audits/tables_summary_*.csv`:

```bash
bash database/run_audit.sh
```

---

## When to run what

| Situation | Tool |
|-----------|------|
| Monthly check-up | `python database/audit/run_monthly_audit.py` |
| Quick schema snapshot | `bash database/run_audit.sh` |
| After editing seed data | `python database/scripts/export_layer_tables.py --layer NN` |
| After running ETL | `python etl/statistics/verify_all_sections.py --scenario {id}` |
| After deploying API changes | `python etl/statistics/verify_api.py --scenario {id}` |
| After running the tier-data pipeline | `python etl/tier_data/scripts/verify_tiers.py --scenario {id}` |
| Check ERD against the DB | Read § 1b of the monthly audit report. The standalone `verify_erd_against_audit.py` has drifted (see below) |

---

## Maintaining the audit

The audit's table coverage is hand-maintained in two lists at the top of `run_monthly_audit.py`, and they drift as the schema grows. Section 1a (table inventory) still counts every table from the live snapshot, so a missing layer only shows up as a gap in the content export and result samples, not in the headline counts. That makes the drift easy to miss.

- **`LAYERS`:** the reference layers exported in full to `layer_exports/` (sections 1c and 1d). Today it runs `00_versioning` through `08_theme`.
- **`RESULTS_TABLES`:** the result tables sampled head/tail into `results_samples/` (section 1e).

### Known gap, Layer 09 (TIER)

Layer 09 falls between the two lists.

- `LAYERS` stops at `08_theme`, so there is no `09_tier` bucket. The Layer 09 tables are never exported as reference data.
- `tier_definition` (the Layer 09 rubric, a small reference table) is sampled as a result in `RESULTS_TABLES` instead of exported in full like the other reference layers.
- `tier_location` (Layer 09 location-to-tier membership, around 280 rows) is in neither list, so it is absent from the content export entirely.

Suggested fix when someone touches the audit next:

- Add a `09_tier` entry to `LAYERS` with `tier_definition` and `tier_location`, so the rubric exports in full and `tier_location` stops being invisible.
- Drop `tier_definition` from `RESULTS_TABLES` (keep `tier_result` and `tier_location_result`, which are the Layer 10 results).
- Update the `layers 00-08` / `layers 10+` wording in this file and in the report so it reflects the real coverage.

### Standing rule

When a new layer or reference table is added to the schema, update `LAYERS` and `RESULTS_TABLES` to match, then refresh the `00-09` / `10+` wording in the docs. The audit will silently skip anything that is not in one of the two lists. Until the coverage self-check (see [Improving the audit](#improving-the-audit) item 1) exists, this is a manual discipline.

---

## Improving the audit

For the next developer. Roughly ordered by value over effort. None are blocking, the audit currently runs.

### 1. Make table coverage self-checking

**Symptom:** `LAYERS` and `RESULTS_TABLES` are hand-maintained and drift silently. A new table appears in the 1a inventory but is never exported, sampled, or count-checked, and nothing flags it.

**Where:** `LAYERS` and `RESULTS_TABLES` at the top of `run_monthly_audit.py`, consumed in the 1c/1d/1e export loops.

**Approach:** the script already does this exact reconciliation for the registry, computing `all_db_tables - mapped_tables` for `domain_family_map`. Apply the same idea to the export lists. Enumerate tables from the snapshot, subtract `LAYERS` and `RESULTS_TABLES` (and a known-skip set), and emit any leftovers as a section-1 finding, ideally a FAIL. That turns a silent omission into a printed to-do list, so the audit tells the next dev what to add instead of hiding it.

**Effort:** S.

### 2. Fix or retire Section 1b (Schema vs. ERD)

**Symptom:** 1b is dead. It hardcodes `COEQWAL_SCENARIOS_DB_ERD.md` (the file is now `ERD.md`), and `verify_erd_against_audit.py` parses a tree-format ERD while `ERD.md` is now prose plus Markdown tables. Even after fixing the path, the parser would find no tables.

**Where:** the 1b block in `run_monthly_audit.py` (the `erd_path = ...COEQWAL_SCENARIOS_DB_ERD.md` line) and `verify_erd_against_audit.py` (`parse_erd_tables`).

**Options:**

- Retire the hand-diff and invert the flow: generate the ERD from the snapshot with `generate_erd_from_audit.py` and treat the snapshot as the source of truth. Recommended, since the hand-maintained ERD is what drifted.
- Or rewrite `parse_erd_tables` to read the Markdown-table `ERD.md`.
- Short term, make the skip loud (print "1b DISABLED, see Improving the audit") instead of a quiet "file not found".

Tracked in [`../SCHEMA_BACKLOG.md`](../SCHEMA_BACKLOG.md) § 10.

**Effort:** S to make the skip honest, M to rewrite or invert.

### 3. Soften the hardcoded expected counts

**Symptom:** `EXPECTED_COUNTS` holds fixed integers, so legitimate reference-data changes report as failures, which trains readers to ignore the audit.

**Where:** `EXPECTED_COUNTS` near the top of `run_monthly_audit.py`, used in the 1c check.

**Approach:** compare against the previous snapshot's counts (a delta report) and/or use minimum thresholds or ranges for tables that legitimately grow. Reserve exact equality for genuinely fixed tables.

**Effort:** M.

### 4. Close the Layer 09 gap and check `env_flow_season`

The Layer 09 fix is detailed in [Known gap, Layer 09](#known-gap-layer-09-tier). While in the lists, also resolve `env_flow_season`: it sits in `RESULTS_TABLES` but the ERD lists it under Layer 01 (lookup). Decide its home and move it.

**Effort:** S.

### 5. (Optional, structural) Move layer membership into the database

`domain_family_map` maps each table to a version family, not to a layer number, so the layer grouping cannot be generated from it today. If the hand-maintained `LAYERS` keeps causing drift, add a `layer` column to the registry (or a small layer lookup) so the grouping lives in the database and the Python list can be derived. Do this only if items 1 and 4 do not settle the churn.

**Effort:** L.
