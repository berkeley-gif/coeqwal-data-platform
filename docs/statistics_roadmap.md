# Statistics and M&I data roadmap

Deferred work for the statistics / model-run pipeline. Not in scope for
the tier-location data quality batch (Section 1).

---

## CVP contractor load (unfinished)

**Current state (verified May 2026 audit):** all 30 `mi_contractor` rows
came from `swp_contractor_perdel_A.wresl` with `project = SWP`. Zero CVP
rows.

**Schema expectation:** [`database/scripts/sql/12_mi_statistics/03_create_mi_contractor_entity_tables.sql`](../database/scripts/sql/12_mi_statistics/03_create_mi_contractor_entity_tables.sql)
comments reference CVP source files (`nodcvpcontract.table`, etc.).

**Work:**
1. Locate CVP contractor source tables / WRESL files.
2. Load CVP rows into `mi_contractor` and `mi_contractor_delivery_arc`.
3. Re-run M&I statistics ETL and Layer 2 verification for a sample scenario.

---

## `cvp_total` aggregate row (decision pending)

**Current state:** `cws_aggregate_entity` has 6 rows. SWP has `swp_total`
plus NOD/SOD splits. CVP has `cvp_nod` and `cvp_sod` only. No `cvp_total`.

**Question for data team:** should a CVP-wide total row exist (mirroring
`swp_total`), or is NOD+SOD sufficient?

**If yes:** add seed row, delivery variables, and ETL path in
[`etl/statistics/cws_aggregate/`](../etl/statistics/cws_aggregate/).

---

## Master crosswalk vs `du_urban_variable`

**File:** [`etl/statistics/reference/Master crosswalk SW DUs M&I.xlsx`](../etl/statistics/reference/Master%20crosswalk%20SW%20DUs%20M&I.xlsx)

86 rows mapping `du_id` to CalSim `UD_*` demand and `DN_*` delivery variables.

**DB table:** `du_urban_variable` (90 rows in May 2026 audit).

**Work (statistics batch):**
1. Cross-reference xlsx ids against `du_urban_variable`.
2. Identify new, matching, and conflicting rows.
3. Update `du_urban_variable` and re-run urban DU statistics verification.

---

## `gw` / `sw` BOOLEAN migration

**Current:** `du_urban_entity.gw` and `.sw` are `VARCHAR(5)` with `'0'`/`'1'`.

**Target:** `BOOLEAN NULL` with reader audit across ETL and API.

Tracked in Section 1 Phase 1.4a of the finish plan.

---

## Reference data sources for gw/sw

| Source | Location | Role |
|---|---|---|
| Seed CSV | `database/seed_tables/04_calsim_data/du_urban_entity.csv` | Current committed reference |
| CalSim report PDF | `data/raw/pdf_tables_from_CalSim_report/urban_du.pdf` | Upstream source for urban gw/sw |
| Ag PDF extracts | `data/raw/csv_from_CalSim_report_pdf/du+diversion/*.csv` | Upstream for ag gw/sw |
| M&I team xlsx | `etl/tier_data/reference/Final_M&Idemandunits_withlatlongs.xlsx` | Team refresh, may override seed |

Reconciliation script:
[`etl/tier_data/scripts/reconcile_gw_sw_sources.py`](../etl/tier_data/scripts/reconcile_gw_sw_sources.py)

---

## Urban gw/sw reconciliation (in progress)

**Walkthrough:** [`docs/gw_sw_reconciliation.md`](gw_sw_reconciliation.md)

**Status (May 2026):**
- Urban seed vs M&I xlsx: 88/120 agree, **32 disagree** (semantic, not format)
- Ag SAC Table 3-3 vs seed: 82/82 agree
- Ag SJR Table 3-6 vs seed: 62/62 agree
- Urban PDF flat extract: **14 du_ids** only (need full Table 3-7, ~123 ids)
- 3 disagreements resolvable now where xlsx and PDF OR agree (`02_PU`, `24_NU1`, `62_NU`)

**Remaining:**
1. Complete `urban_du_calsim_report.csv` from `urban_du.pdf` (9 pages)
2. Case-by-case decisions for other 29 disagreements
3. Update `du_urban_entity.csv` seed
4. Then `gw`/`sw` BOOLEAN migration (Phase 1.4a)

Ag PDF tables 3-4 and 3-5 have no gw/sw columns (diversion arcs only).
Do not compare them to seed gw/sw.

---

## Connection-helper unification (completed May 2026)

All 11 database-opening call sites under `etl/statistics/` now route through
[`etl.common.db.get_db_connection`](../etl/common/db.py): the 8 calculation
modules (reservoirs, delta, refuge, env_flows, sensitivity, du_urban,
cws_aggregate, ag, mi), the orchestrator `run_all.py`, and the two verifier
scripts (`verify_all_sections.py`, `verify_api.py`).

The helper emits an INFO log line on each connect that names the URL source
(env var vs explicit override) and a credential-safe `user@host:port/dbname`
target summary. Passwords are never read by the logger.

Inline `psycopg2.connect()` calls in the statistics tree are gone.

---

## Per-scenario atomic transactions in stats writers

**Current state:** every writer opens a connection, runs DELETE plus INSERT,
commits, and closes within seconds. Most writers commit per scenario but rely
on garbage collection for cleanup on error. Refuge and env_flows are the only
modules with deterministic try/finally cleanup today.

**Goal:** wrap each writer's DELETE plus INSERT in `with conn:` and
`with conn.cursor() as cur:` so a mid-scenario failure rolls back
deterministically and the connection is released on every path.

**Why deferred:** requires live RDS verification of rollback behavior per
module. The SQL is battle-tested but the rollback semantics have to be
exercised against a real database.

---

## Caller-injectable connection for testability

**Goal:** add an optional `conn` parameter to each module's `run()` function
(and underlying writers where applicable) so tests can pass a mock or fake
connection without monkey-patching the helper.

**Important constraint.** If implemented, the `conn` parameter is for unit
tests only. It must NOT be used to share one real connection across all 8
modules during a scenario run. Each module's calculation phase takes minutes,
sometimes 30 or more. RDS sitting behind an NLB with a ~350-second idle
timeout will drop a TCP connection while a module is busy calculating, and
the next write call will crash.

The current pattern (one short-lived connection per write, lasting seconds)
is correct and must be preserved. The orchestrator must keep passing
`conn=None` so each module opens its own connection at the moment it writes.

---

## Verification streamlining

**Current state (verified May 2026):** verification surface area is now
consolidated into [`etl/verification/README.md`](../etl/verification/README.md)
(the prior `docs/VERIFICATION.md` and `docs/AUDITS_AND_VERIFICATION.md`
were folded in). `verify_metrics.py` was deleted as redundant (its
reservoir-only coverage is now part of `verify_all_sections.py`). What
remains are the developer-driven layers (Layer 2, Layer 3, Layer 3-tier)
which today each take a separate command and write reports under
[`audits/verification_reports/`](../audits/verification_reports/).

The work below makes the developer-driven layers cheaper to run, easier
to read, and less surprising. Items are roughly ordered by value over
effort.

### V1. Single `verify_release.py` scorecard orchestrator (highest value)

**Goal:** one command that runs Layer 2 + Layer 3 + Layer 3-tier for a
scenario (or `--all-scenarios`) and prints a single PASS / FAIL
scorecard with per-layer drill-down. Today these are three commands and
the `etl/verification/README.md` paste block is the closest equivalent.

**Sketch:**
```bash
python etl/verification/verify_release.py --scenario s0042
python etl/verification/verify_release.py --all-scenarios --report-dir audits/verification_reports
```

Behaviour:
- Run each layer in sequence (fail-fast configurable with `--continue-on-failure`).
- Collect per-layer JSON into a `release_{scenario}_{ts}.json` aggregate.
- Print a one-screen scorecard with per-section pass counts (Reservoirs:
  PASS 8/8, CWS: PASS 6/6, AG: FAIL 130/131, ...) and the path to the
  per-layer detail.
- Exit code: 0 if all PASS, 1 otherwise.

The orchestrator's `--verify` preset is the closest equivalent today
but it only runs Layer 2.

### V2. Standardize verification report naming and paths

**Current:** `verify_all_sections.py` writes
`{scenario}_layer2.json`, `verify_api.py` writes
`{scenario}_layer3.json`, `verify_tiers.py` writes `tiers_{ts}.json`
(no per-scenario stamping).

**Goal:** consistent `{scenario}_{layer}.json` for all three (and
`release_{scenario}_{ts}.json` from V1). Drop the per-run timestamp
default for tier verification so the latest run is always at a known
path. Keep a `--timestamped` opt-in for developers who need to retain
prior runs.

### V3. Default `--scenario` to the active set

**Current:** the verifiers default to processing nothing, so every
invocation needs `--scenario sXXX` or `--all-scenarios`. The
`--all-scenarios` ceremony is the most common case.

**Goal:** drop the requirement. If no scenario flag is passed, default
to `ACTIVE_SCENARIOS` and print a banner naming the resolved set.
Preserve `--scenario` and `--scenarios-override` for the targeted
cases. Makes the paste-block in `etl/verification/README.md` shorter
and reduces the per-PR ceremony for developers running a quick check.

### V4. Auto-detect pre-flight scenarios (drop `--scenarios-override`)

**Current:** between the statistics load and the `set_scenario_active`
step, a new scenario is in the DB but not in `ACTIVE_SCENARIOS`.
Verifying it requires `--scenarios-override s0070`, which is awkward
and not memorable.

**Goal:** add a `--include-inactive` flag that pulls the union of
`ACTIVE_SCENARIOS` and any scenarios that have data in the
result tables but are not yet active. Emits the same `WARNING` banner
as `--scenarios-override` so the resolved set is visible in any
pipeline log. Keep `--scenarios-override` as the explicit per-invocation
escape hatch.

### V5. Reference-directory clarity

**Current:** three reference homes coexist with overlapping-but-distinct
purposes, which makes the verifiers and `--csv-only` invocations
confusing:

- [`etl/reference/`](../etl/reference/) holds the CalSim scenario CSVs
  (DV `s0020_coeqwal_calsim_output.csv`, SV `s0020_coeqwal_sv_input.csv`,
  trend-report CSVs). This is the verifiers' default (`--ref-dir`
  default in `verify_all_sections.py`).
- [`etl/statistics/reference/`](../etl/statistics/reference/) holds the
  `Master crosswalk SW DUs M&I.xlsx` (the CalSim-variable to DU-label
  map intended for `compare_master_crosswalk.py` [NOT YET IMPLEMENTED];
  see `docs/TEAM_RUNBOOK.md` thread A5). Different role; not used by
  the verifiers.
- `s3://coeqwal-model-run/reference/` is the cloud mirror of
  `etl/reference/`.

The two local homes do not conflict, but they look like they do (both
named `reference/`), so the `--csv-only` path on
`verify_all_sections.py` is not obvious about which directory it loaded
from.

**Goal:** keep the role split, name it explicitly, and make the
verifiers log the resolved path at INFO:

- Rename one of the local directories so the role is obvious from the
  path (for example `etl/reference/` -> `etl/reference_calsim_csvs/`
  and `etl/statistics/reference/` -> `etl/statistics/crosswalks/`).
- Have `verify_all_sections.py` and `verify_api.py` log the resolved
  `--ref-dir` at INFO on startup so the loaded source is unambiguous.
- Document the S3 sync command and the role split in
  [`etl/verification/README.md`](../etl/verification/README.md).

### V6. CI integration for Layer 3 (API vs DB)

**Current:** all developer-driven layers run on Cloud9 only. A failing
verification today blocks a release only when a developer notices.

**Goal:** a GitHub Action workflow that runs `verify_api.py
--all-scenarios` against the staging DB on every PR that touches
`api/coeqwal-api/**` or `etl/statistics/**`, posts the scorecard as a
PR comment, and gates merge on PASS. Same approach for `verify_tiers.py`
when `etl/tier_data/scripts/**` changes.

**Constraint:** requires a CI-accessible read-only DB role and a stable
test bucket. Layer 2 is heavier (recomputes from CSVs) and may be too
slow for per-PR; start with Layer 3.

### V7. Layer 4 smoke test (`/verification` page renders)

**Current:** the public status page at `/verification` is a
human-readable surface, not a tested one. A backend change that breaks
the JSON shape of the verification endpoint will not fail any test
today; it will fail the page silently.

**Goal:** a Playwright smoke test in the website repo that loads
`/verification`, asserts the per-scenario PASS / FAIL grid renders for
at least one scenario, and that the drill-down links resolve. Run in
CI on every website PR.

### V8. Unit tests on the verifiers themselves

**Current:** `verify_all_sections.py` and `verify_api.py` are
~1500 lines combined of comparison logic with no unit tests. A bug in
the verifier (wrong tolerance, off-by-one in iteration, swallowed
exception) could silently pass bad data.

**Goal:** introduce a `tests/verification/` directory with fixture CSVs
+ fixture DB rows + fixture API responses, and assert the verifiers'
PASS / FAIL decisions match expectations for at least:
- baseline correctness (synthetic match across all sections)
- known-mismatch detection (synthetic 0.1% drift, must FAIL)
- NaN handling on both sides (both NaN: PASS; one NaN: FAIL)
- per-section coverage (every section in `etl/verification/README.md`
  has at least one test)

**Why deferred:** depends on a fixture-DB pattern that the wider ETL
test suite does not have today. Could lean on the caller-injectable
connection from "Caller-injectable connection for testability" above.

### Sequencing

V1, V2, V3 are local refactors with no external dependencies — they
can ship in any order behind a single PR each. V4 depends on V3. V5
is independent. V6 and V7 require new CI plumbing and stable
credentials. V8 depends on the caller-injectable connection roadmap
item.
