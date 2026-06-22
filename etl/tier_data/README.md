# ETL: Tier Outcome Results

Loads tier outcome data for all active scenarios into the `tier_result` and `tier_location_result` database tables.

This README has two distinct workflows. Pick the one you need:

| If the tier data team sent you... | Go to |
|---|---|
| **new tier-result values** (integral tier 1-4 numbers per scenario) | [How to load new tier data](#how-to-load-new-tier-data) (the section below) |
| **a changed tier-LOCATION list** (added/dropped a location_id from a tier) | [Updating tier locations when a tier team sends new data](#updating-tier-locations-when-a-tier-team-sends-new-data) (further down) |

---

For lookups while you work, see the [Reference](#reference) section: what gets written, what each tier is, and the staging CSV formats.

---

## How to load new tier data

**1. Drop new CSVs into `etl/tier_data/staging/`.**
   Filenames are fixed: `CWS_DEL.csv`, `AG_REV.csv`, `ENV_FLOWS.csv`, `RES_STOR.csv`, `GW_STOR.csv`, `DELTA_ECO.csv`, `FW_DELTA_USES.csv`, `FW_EXP.csv`, `WRC_SALMON_AB.csv`. Format reference: [Staging CSV format](#staging-csv-format) below.

   If the team sends pre-staging drops (multiple files per tier, per-climate splits, etc.) under `staging/tier_results/`, normalize them with:
   ```bash
   python etl/tier_data/scripts/stage_tier_results.py
   ```

**2. Refresh the active-scenario allowlist if needed.**
   ```bash
   python etl/ingestion/tools/refresh_active_scenarios.py
   ```
   That regenerates `etl/common/active_scenarios.py` from the live API (`/api/scenarios`, `is_active=true`). If any scenarios are being retired, add their short codes to `DEACTIVATED_SCENARIOS` in [`scripts/load_all_tier_results.py`](scripts/load_all_tier_results.py).

**3. Commit and push from Mac.** The staging CSVs are git-tracked.

**4. On Cloud9: `git pull`.**

**5. Dry run, rectify counts:**
   ```bash
   python etl/tier_data/scripts/load_all_tier_results.py --dry-run
   ```
   Expected counts per scenario:

   | Tier | Location rows / scenario |
   |------|--------------------------|
   | `CWS_DEL` | ~76 (varies, NAs skipped) |
   | `AG_REV` | ~132 |
   | `ENV_FLOWS` | 17 |
   | `RES_STOR` | 8 |
   | `GW_STOR` | 42 |
   | `DELTA_ECO` | 1 |
   | `FW_DELTA_USES` | 2 (but 1 tier value reported) |
   | `FW_EXP` | 2 (but 1 tier value reported) |
   | `WRC_SALMON_AB` | 1 (`s0065` excluded by the data team) |

**6. Generate the SQL:**
   ```bash
   python etl/tier_data/scripts/load_all_tier_results.py --output-sql all_tiers.sql
   ```
   Writes `etl/tier_data/output/all_tiers.sql` (the whole `output/` tree is gitignored via `etl/**/output/`, debatable whether this is a good idea).

**7. Apply it:**
   ```bash
   psql $DATABASE_URL -f etl/tier_data/output/all_tiers.sql
   ```
   The SQL ends with two verification queries (one per table) showing row counts grouped by `tier_short_code`. Active scenario counts should match `ALLOWED_SCENARIOS`. Use `$DATABASE_URL` (your personal role) so audit attribution is to you, not on the shared `postgres` account.

**8. Validate uniqueness and idempotency.**
   ```bash
   # Should return zero rows. The DB constraint already guarantees
   # this, but the explicit check catches any future schema drift.
   psql $DATABASE_URL <<'SQL'
     SELECT scenario_short_code, tier_short_code, location_id,
            tier_version_id, COUNT(*) AS dupe_count
     FROM tier_location_result
     GROUP BY scenario_short_code, tier_short_code, location_id, tier_version_id
     HAVING COUNT(*) > 1;

     SELECT scenario_short_code, tier_short_code, tier_version_id,
            COUNT(*) AS dupe_count
     FROM tier_result
     GROUP BY scenario_short_code, tier_short_code, tier_version_id
     HAVING COUNT(*) > 1;
   SQL
   ```

   Optional cross-check against the live API (uses tier_result + entity joins via the same code path as the public API). Useful before flipping a new scenario's `is_active`:
   ```bash
   python etl/tier_data/scripts/verify_tiers.py
   ```

> **No seed CSV step.** `tier_result` and `tier_location_result` are project data, not reference data, so they are not mirrored into `database/seed_tables/10_tier/`. The staging CSVs in `staging/` plus this loader are the source of truth.

> **Test a new scenario before publishing it.** The scripts only process scenarios in the active list ([`etl/common/active_scenarios.py`](../common/active_scenarios.py)), so a scenario that is not yet public gets skipped. To trial one anyway, pass `--scenarios-override sXXX` to [`scripts/load_all_tier_results.py`](scripts/load_all_tier_results.py) (with `--dry-run`) and [`scripts/verify_tiers.py`](scripts/verify_tiers.py). The override applies to that one run only and is never saved. When the scenario checks out, publish it (set `is_active=1`) with [`etl/ingestion/tools/set_scenario_active.py`](../ingestion/tools/set_scenario_active.py).

### Partial loads

To load only a subset of tiers (e.g. after a single-tier data update):
```bash
python etl/tier_data/scripts/load_all_tier_results.py \
    --only ENV_FLOWS,RES_STOR --output-sql partial.sql
psql $DATABASE_URL -f etl/tier_data/output/partial.sql
```
Other tier rows are left alone (the UPSERT only touches the tiers in the generated SQL).

### Direct DB apply (skip the SQL file)

If you set `$DATABASE_URL`, the loader can apply directly without writing a file. Use this only for one-off backfills, not for routine loads - the file path keeps a reviewable record of what hit the DB:
```bash
DATABASE_URL=$DATABASE_URL python etl/tier_data/scripts/load_all_tier_results.py
```

### Notes

- `tier_location_result` has no `is_active` column. Retired-scenario rows stay in the table. `tier_result` does have an `is_active` column. The API hides them by filtering on `tier_result.is_active`.
- `DETAW` is a shared `location_id` across `GW_STOR` and `DELTA_ECO` by design. It's the CalSim id for the Legal Delta, which is both a WBA (for groundwater accounting) and the polygon used by `DELTA_ECO`. The composite uniqueness key keeps these rows distinct because the `tier_short_code` differs. API routes that take a tier in the path return only that tier's `DETAW` row. Client code keying by `location_id` across tiers should use `(tier_short_code, location_id)`.
- Generated SQL lands in `etl/tier_data/output/` by default and that whole tree is gitignored. Only the script and staging CSVs are tracked.

---

## Updating tier locations when a tier team sends new data

The tier teams' staging CSVs are the source of truth for tier-location membership. The `tier_location` database table is a narrow catalog (`tier_short_code`, `location_type`, `location_id`, `display_order`, `is_active`). Display names and geometry are resolved at query time by joining `location_id` to the entity tables documented in [`etl/common/tier_location_entities.py`](../common/tier_location_entities.py). There is no seed CSV for `tier_location`.

The workflow:

1. Stage the new tier CSVs as usual.

2. Show the diff between staging and the live catalog:

   ```bash
   python etl/tier_data/scripts/diff_tier_locations.py
   ```

   Optional `--tier RES_STOR` scopes the diff to one tier. The output lists ids the tier team added (`in CSV, not in DB`) and ids that are no longer in staging (`in DB, not in CSV`).

3. Optional but recommended: audit geometry and attribute coverage for the new ids before promoting them. This walks the entity tables named in [`etl/common/tier_location_entities.py`](../common/tier_location_entities.py) (`network` and `network_gis`, `du_urban_entity`, `du_agriculture_entity`, `reservoir_entity` and `reservoir`, `wba`, `compliance_station`) and reports any `location_id` that the catalog would carry but the entity table cannot resolve. Note that each tier resolves against its own table, so `AG_REV` ids check `du_agriculture_entity` while `CWS_DEL` ids check `du_urban_entity`, and `RES_STOR` attributes come from `reservoir_entity` with geometry from `reservoir`.

   ```bash
   python etl/tier_data/scripts/audit_tier_location_geometry.py
   ```

   Re-run after each gap-fill until the scorecard reports 100% attribute and geometry coverage.

4. Dry-run the sync to see exactly which rows would change:

   ```bash
   python etl/tier_data/scripts/sync_tier_locations_from_staging.py --dry-run
   ```

   The plan reports inserts, reactivations (rows that returned to staging after being soft-deleted), display-order updates, and deactivations. The script refuses to write rows whose `location_id` does not resolve in the entity table. Pass `--allow-unresolved` only during an active gap-fill.

5. Apply:

   ```bash
   python etl/tier_data/scripts/sync_tier_locations_from_staging.py
   ```

   The script runs in one transaction. Rows that left staging are soft-deleted (`is_active = FALSE`) so historical `tier_location_result` rows still have a catalog row to point at. Re-adding a row to staging flips `is_active` back to TRUE on the next sync.

6. Re-run `python etl/tier_data/scripts/diff_tier_locations.py`. Gaps should be gone.

### Coverage alerts in the scripts

Every script that touches `tier_location` now runs a coverage scan and prints a one-line WARNING per tier with missing attribute or geometry data, for example:

```
WARNING: tier_location coverage gap in RES_STOR: 1 missing attribute [ORO]; 1 missing geometry [ORO]. Run `python etl/tier_data/scripts/audit_tier_location_geometry.py --tier RES_STOR` for details.
```

| Script | What it does with the alert |
|---|---|
| [`scripts/sync_tier_locations_from_staging.py`](scripts/sync_tier_locations_from_staging.py) | Prints a coverage scorecard (attribute X/Y, geometry A/B) and the WARNING block in its plan. Geometry gaps warn only. Blocks on attribute gaps, exiting with code 2 unless `--allow-unresolved` is passed. |
| [`scripts/diff_tier_locations.py`](scripts/diff_tier_locations.py) | Prints a coverage scorecard across the staging and catalog ids and the WARNING block. Read-only. Never exits non-zero. |
| [`scripts/load_all_tier_results.py`](scripts/load_all_tier_results.py) | Prints the WARNING block on startup against active catalog rows. Falls back to `location_id` for any name that fails to resolve. Continues regardless. |
| [`scripts/verify_tiers.py`](scripts/verify_tiers.py) | Prints the WARNING block on startup after the RES_STOR catalog fetch. Leaves pass/fail logic unchanged. Continues regardless. |
| [`scripts/audit_tier_location_geometry.py`](scripts/audit_tier_location_geometry.py) | Prints the full per-id scorecard and the ERD-vs-live drift pass. Writes JSON with `--json`. Exits non-zero on any gap so CI can branch on it. |

Geometry gaps are warn-only in every script. Attribute (unresolved-id) gaps behave differently. `sync_tier_locations_from_staging.py` blocks on them and exits with code 2 unless `--allow-unresolved` is passed, while `diff`, `load`, and `verify` only warn. The audit script exits non-zero on any gap, attribute or geometry. Reach for the audit script when you need the full per-id detail or want CI to fail on regressions.

---

## Reference

### What gets written

The loader writes to two tables, both UPSERT (`ON CONFLICT ... DO UPDATE`):

| Table | One row per | Unique key (DB-enforced) |
|---|---|---|
| `tier_result` | scenario × tier | `(scenario_short_code, tier_short_code, tier_version_id)` |
| `tier_location_result` | scenario × tier × location | `(scenario_short_code, tier_short_code, location_id, tier_version_id)` |

### Tier outcomes

| Short code | Name | Type | Locations |
|------------|------|------|-----------|
| `CWS_DEL` | Community water system deliveries | multi-value | Demand units |
| `AG_REV` | Agricultural revenue | multi-value | Demand units |
| `ENV_FLOWS` | Environmental flows | multi-value | 17 stream reaches |
| `RES_STOR` | Reservoir storage | multi-value | 8 reservoirs, note that San Louis is split into CSV and SWP |
| `GW_STOR` | Groundwater storage | multi-value | 42 locations: 41 water budget areas + `DETAW` (Delta) |
| `DELTA_ECO` | Delta ecology | single-value | DETAW (Delta) |
| `FW_DELTA_USES` | Freshwater for in-Delta uses | single-value | Emmaton, Jersey Point |
| `FW_EXP` | Freshwater for Delta exports | single-value | Banks, Jones pumping plants |
| `WRC_SALMON_AB` | Salmon abundance | single-value | Sacramento at Keswick |

### Staging CSV format

Each tier has a CSV file in `staging/` named by its short code. The formats differ by tier:

| File | Column layout |
|------|--------------|
| `CWS_DEL.csv` | `scenario_id`, then one column per demand unit short code. Values = tier 1-4 or NA |
| `AG_REV.csv` | Wide: `scenario_id`, then one column per region. Values = tier 1-4. Long `scenario, region, tier` format is also auto-detected for backwards compatibility |
| `ENV_FLOWS.csv` | `Scenario`, then one column per station short code (e.g. `AMR004`, `SAC289`). Values = tier 1-4. Upstream eflows drops use `Station` as the column-0 header; `scripts/stage_tier_results.py` rewrites it to `Scenario` on copy so all tier staging CSVs follow the same scenarios-as-rows, locations-as-columns convention |
| `RES_STOR.csv` | `Scenario`, then one column per reservoir (e.g. `S_SHSTA_Storage_Tier`). Values = tier 1-4 |
| `GW_STOR.csv` | `scenario`, then one column per WBA (e.g. `WBA2`, `WBA7N`) plus `DETAW`. Values = tier 0-4 |
| `DELTA_ECO.csv` | `Scenario` (numeric, e.g. `11` for `s0011`), `TierValue` |
| `FW_DELTA_USES.csv` | `ScenarioID`, `Salinity_Tier` |
| `FW_EXP.csv` | `Scenario`, `Salinity_Export_Tier` |
| `WRC_SALMON_AB.csv` | `scenario`, `Tier_range` (string like `"Tier 4"`). `s0065` is excluded by the data team |

NA cells in any CSV are skipped (no location row generated for that slot).

---

## Known limitations and roadmap

These are open items that the tier pipeline does not yet handle.

### NOD/SOD regional outcomes are not produced by this pipeline

The frontend shows ten North-of-Delta and South-of-Delta regional outcomes (`NOD_*` and `SOD_*`), but the tier API does not serve them. They come from a static dataset bundled with the website, sourced from the Water Allocation Modeling Team's water data dashboard. The tier teams do not currently report NOD/SOD aggregates.

Much of the raw material already lives in the database. The entity tables that tier locations resolve to (`network` and `network_node`, `reservoir_entity`, the demand-unit tables, and `wba`) record a hydrologic region. NOD maps to the SAC hydrologic region, and SOD maps to SJR or TLB. Compliance and pumping stations are neither, because they sit in the Delta. A future roadmap item is to derive the NOD/SOD split from these hydrologic-region foreign keys rather than maintaining a separate static file. This is the way the database has been created to work: to house data attributes and be able to filter and aggregate on them for current reporting and future analysis.

### Tier scale is moving from discrete to continuous

The pipeline and the database currently assume a discrete 1-4 tier scale. A planned migration moves to a continuous scale. Code and documentation that hardcode the 1-4 range (validation bounds, score normalization, and frontend color and axis mapping) will need to be revisited as part of that migration.
