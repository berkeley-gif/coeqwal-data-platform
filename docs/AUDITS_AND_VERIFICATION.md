# Audits and verification index

Where verification and audit artifacts live, and how they relate to the
[verification doc wrap plan](../.cursor/plans/finish_etl_+_verification_plans_1bd6e5c0.plan.md)
(Section 4).

---

## Monthly database audit (schema + content snapshot)

| | |
|---|---|
| **Script** | [`database/audit/run_monthly_audit.py`](../database/audit/run_monthly_audit.py) |
| **When** | Manual. Run before major data changes. |
| **Output** | `audits/monthly_YYYYMMDD_HHMMSS/` |
| **Latest** | `audits/monthly_20260524_143951/` (May 24 2026) |

Contents:
- `report.md` - row counts, ERD diff, index sizes, audit-field checks
- `tables_summary.csv` - per-table inventory
- `schema_snapshot.json` - full schema
- `layer_exports/` - full CSV export of reference / entity / lookup tables
- `results_samples/` - head/tail samples of statistics result tables

**Use for:** grounding documentation, cross-checking seed CSVs against live
RDS, ERD verification.

```bash
cd ~/environment/coeqwal-backend
python database/audit/run_monthly_audit.py
```

---

## ETL verification layers

Documented in [`etl/verification/README.md`](../etl/verification/README.md)
and [`docs/VERIFICATION.md`](VERIFICATION.md).

| Layer | Script | Output (under `audits/`) |
|---|---|---|
| 1 - DSS extraction | `etl/batch-container/` (inside Batch job) | `validation_mismatches/{scenario}_extract_record.json` |
| 1b - DSS unit check | same | manifest fields |
| 2 - Statistics vs CSV | `etl/statistics/verify_all_sections.py` | `verification_reports/{scenario}_layer2.json` |
| 3 - API vs DB | `etl/statistics/verify_api.py` | `verification_reports/{scenario}_layer3.json` |
| Tier staging | `etl/tier_data/scripts/verify_tiers.py` | (stdout / exit code) |

---

## Tier location geometry audit

| | |
|---|---|
| **Script** | [`etl/tier_data/scripts/audit_tier_location_geometry.py`](../etl/tier_data/scripts/audit_tier_location_geometry.py) |
| **When** | Before / after tier load, when entity tables change |
| **Checks** | `tier_location` ids resolve to entity attributes and polygons |

---

## gw/sw classification reconciliation (deferred)

| | |
|---|---|
| **Script** | [`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../etl/tier_data/scripts/reconcile_gw_sw_sources.py) |
| **Walkthrough** | [`docs/gw_sw_reconciliation.md`](gw_sw_reconciliation.md) |
| **When** | Informational until value reconciliation resumes |
| **CSV output** | `python etl/tier_data/scripts/reconcile_gw_sw_sources.py --csv-out` (no path = writes under `data/raw/.../urban_gw_sw_audit.csv`). Do not use `/tmp` on Cloud9. |
| **Type migration** | [`database/scripts/sql/57_du_urban_gw_sw_boolean.sql`](../database/scripts/sql/57_du_urban_gw_sw_boolean.sql) |

---

## Database geometry

| | |
|---|---|
| **Doc** | [`docs/database_geometry_pattern.md`](database_geometry_pattern.md) |
| **DU polygons** | Open decision. Do not load until policy chosen. |

---

## Git tracking policy

- `audits/` is **tracked** (except `audits/*.tar.gz`, which duplicate the
  unzipped directories).
- `data/raw/pdf_tables_from_CalSim_report/` is **tracked** (whitelisted in
  `.gitignore`).
- Reference xlsx files under `etl/tier_data/reference/` and
  `etl/statistics/reference/` are **tracked**.

---

## Planned streamlining (Section 4 of finish plan)

- Final link audit on [`docs/VERIFICATION.md`](VERIFICATION.md)
- Replace "operator" with "developer" where appropriate
- Optional: unified `verify_release.py` scorecard orchestrator

The monthly audit is the **database state baseline**. Layer 2/3 JSON files
are the **pipeline correctness** checks. Use both when validating a change.
