# Database audit tools

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
| 1b | Schema vs. ERD | Tables/columns in DB but not ERD, and vice versa |
| 1c | Row counts vs. expected | Layers 00-08 counts against known targets |
| 1d | Reference data downloads | Full CSV export of layers 00-08 |
| 1e | Results data samples | Head/tail CSV samples of layers 10+ |
| 2a | Data integrity | NULL audit fields, orphaned rows, invalid values, `coeqwal_developer` group membership |
| 2b | ETL coverage | Per-scenario row counts across all results tables |
| 2c | ETL accuracy summary | Reads existing Layer 2/3 verification reports |
| 3a-d | Database health | Cache hit ratio, connections, dead tuples, bloat |
| 4a-c | Database cost | Table sizes, unused indexes, total storage |
| 5 | Audit summary | PASS/FAIL for each check, with details on failures |

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

When a new layer or reference table is added to the schema, update `LAYERS` and `RESULTS_TABLES` to match, then refresh the `00-08` / `10+` wording in the docs. The audit will silently skip anything that is not in one of the two lists.
