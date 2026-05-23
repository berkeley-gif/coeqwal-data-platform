# ETL.Tier Outcome Results

Loads tier outcome data for all active scenarios into the `tier_result` and
`tier_location_result` database tables.

---

## How to load new tier data

**1. Drop new CSVs into `etl/tier_data/staging/`.** Filenames are fixed: `CWS_DEL.csv`, `AG_REV.csv`, `ENV_FLOWS.csv`, `RES_STOR.csv`, `GW_STOR.csv`, `DELTA_ECO.csv`, `FW_DELTA_USES.csv`, `FW_EXP.csv`, `WRC_SALMON_AB.csv`.

**2. If the active scenario list changed**, run `python etl/ingestion/tools/refresh_active_scenarios.py` to regenerate `etl/common/active_scenarios.py` (the canonical `ACTIVE_SCENARIOS` set this script imports). The refresh script reads `is_active=true` rows from the live API. Add any short codes that need to be deactivated to `DEACTIVATED_SCENARIOS` in `scripts/load_all_tier_results.py`.

**3. Commit and push from Cloud9.** The staging CSVs are git-tracked on purpose.

**4. On Cloud9: `git pull`.**

**5. Dry run to check counts:**

```bash
python etl/tier_data/scripts/load_all_tier_results.py --dry-run
```

**6. Generate the SQL:**

```bash
python etl/tier_data/scripts/load_all_tier_results.py --output-sql all_tiers.sql
```

That writes `etl/tier_data/output/all_tiers.sql`.

**7. Apply it:**

```bash
psql $DATABASE_URL -f etl/tier_data/output/all_tiers.sql
```

Read the two verification tables it prints at the end. Active scenario counts should match what's in `ALLOWED_SCENARIOS`.

**8. Export back into seed CSVs** so the DB can be rebuilt from scratch. The two `\COPY` blocks below; copy the outputs to `database/seed_tables/10_tier/`.

> **Output files.** `load_all_tier_results.py --output-sql <name>` writes the
> generated UPSERT script to `etl/tier_data/output/<name>` (gitignored). Bare
> filenames are auto-routed there. Paths with `/` are respected. See
> [`etl/README.md`](../README.md#output-files-audits-generated-sql) for the
> full output catalog.

> **Pre-flight a new scenario before flipping `is_active=1`.** Both
> [`scripts/load_all_tier_results.py`](scripts/load_all_tier_results.py) and
> [`scripts/verify_tiers.py`](scripts/verify_tiers.py) accept `--scenarios-override sXXX,sYYY`
> as a per-invocation replacement for `ACTIVE_SCENARIOS`. Use it to dry-run a
> tier load (`--scenarios-override sXXX --dry-run`) or verify tier coverage
> against the live API for a scenario that is not yet public. The override is
> never persisted. To change `ACTIVE_SCENARIOS` itself, use
> [`etl/ingestion/tools/set_scenario_active.py`](../ingestion/tools/set_scenario_active.py).
> Each run emits a `WARNING` line naming the resolved override set.

---

## Updating tier locations when a tier team sends new data

The tier teams' staging CSVs are the source of truth for tier-location
membership. The `tier_location` database table is a narrow catalog
(`tier_short_code`, `location_type`, `location_id`, `display_order`,
`is_active`); display names and geometry are resolved at query time by
joining `location_id` to the entity tables documented in
[`etl/common/tier_location_entities.py`](../common/tier_location_entities.py).
There is no seed CSV for `tier_location`.

The workflow:

1. Stage the new tier CSVs as usual.

2. Show the diff between staging and the live catalog:

   ```bash
   python etl/tier_data/scripts/diff_tier_locations.py
   ```

   Optional `--tier RES_STOR` scopes the diff to one tier. The output
   lists ids the tier team added (`in CSV, not in DB`) and ids that are
   no longer in staging (`in DB, not in CSV`).

3. Optional but recommended: audit geometry and attribute coverage for
   the new ids before promoting them. This walks the entity tables
   (`network`, `network_gis`, `reservoir`, `wba`, `compliance_station`,
   `du_urban_entity`) and reports any `location_id` that the catalog
   would carry but the entity table cannot resolve.

   ```bash
   python etl/tier_data/scripts/audit_tier_location_geometry.py
   ```

   Re-run after each gap-fill until the scorecard reports 100% attribute
   and geometry coverage.

4. Dry-run the sync to see exactly which rows would change:

   ```bash
   python etl/tier_data/scripts/sync_tier_locations_from_staging.py --dry-run
   ```

   The plan reports inserts, reactivations (rows that returned to
   staging after being soft-deleted), display-order updates, and
   deactivations. The script refuses to write rows whose `location_id`
   does not resolve in the entity table; pass `--allow-unresolved` only
   during an active gap-fill.

5. Apply:

   ```bash
   python etl/tier_data/scripts/sync_tier_locations_from_staging.py
   ```

   The script runs in one transaction. Rows that left staging are
   soft-deleted (`is_active = FALSE`) so historical
   `tier_location_result` rows still have a catalog row to point at.
   Re-adding a row to staging flips `is_active` back to TRUE on the
   next sync.

6. Re-run `python etl/tier_data/scripts/diff_tier_locations.py`. Gaps should be
   gone.

### Coverage alerts in the daily scripts

Every script that touches `tier_location` now runs a coverage scan and
prints a one-line WARNING per tier with missing attribute or geometry
data:

```
WARNING: tier_location coverage gap in RES_STOR: 1 missing attribute [ORO]; 1 missing geometry [ORO]. Run `python etl/tier_data/scripts/audit_tier_location_geometry.py --tier RES_STOR` for details.
```

| Script | What it does with the alert |
|---|---|
| [`scripts/sync_tier_locations_from_staging.py`](scripts/sync_tier_locations_from_staging.py) | Prints per-tier `coverage: attribute X/Y, geometry A/B` in the plan, then the WARNING block. Attribute gaps still block sync (use `--allow-unresolved` during gap-fill). Geometry gaps warn only. |
| [`scripts/diff_tier_locations.py`](scripts/diff_tier_locations.py) | Appends a coverage scorecard across the union of staging and catalog ids, then the WARNING block. Read-only, never exits non-zero. |
| [`scripts/load_all_tier_results.py`](scripts/load_all_tier_results.py) | Emits the WARNING block on startup against active catalog rows. Loader continues regardless; the loader falls back to `location_id` for any name that fails to resolve. |
| [`scripts/verify_tiers.py`](scripts/verify_tiers.py) | Emits the WARNING block on startup, immediately after the RES_STOR catalog fetch. Verifier pass/fail logic is unchanged. |
| [`scripts/audit_tier_location_geometry.py`](scripts/audit_tier_location_geometry.py) | The dedicated tool. Full per-id scorecard plus the ERD-vs-live drift pass. Exits non-zero on any gap so CI / wrappers can branch on it. JSON dump via `--json`. |

The four daily scripts only ever warn; only the audit script changes its
exit code on gaps. Reach for the audit script when you need the full
per-id detail or want CI to fail on regressions.

---

## Tier outcomes

| Short code | Name | Type | Locations |
|------------|------|------|-----------|
| `CWS_DEL` | Community water system deliveries | multi-value | Demand units |
| `AG_REV` | Agricultural revenue | multi-value | Demand units (regions) |
| `ENV_FLOWS` | Environmental flows | multi-value | 17 stream reaches |
| `RES_STOR` | Reservoir storage | multi-value | 8 reservoirs |
| `GW_STOR` | Groundwater storage | multi-value | 42 locations: 41 water budget areas + `DETAW` (Delta) |
| `DELTA_ECO` | Delta ecology | single-value | DETAW (Delta) |
| `FW_DELTA_USES` | Freshwater for in-Delta uses | single-value | Emmaton, Jersey Point |
| `FW_EXP` | Freshwater for Delta exports | single-value | Banks, Jones pumping plants |
| `WRC_SALMON_AB` | Salmon abundance | single-value | Sacramento at Keswick (s0065 excluded by data team) |

---

## Staging CSV format

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

## Workflow: loading new tier data

### 1. Update staging CSVs locally

Replace the files in `staging/` with the new data. Keep the filenames exactly as
shown above. The staging directory is tracked in git so files can be pushed and pulled.

### 2. Refresh the scenario allowlist if needed

Run `python etl/ingestion/tools/refresh_active_scenarios.py` to regenerate `etl/common/active_scenarios.py` from the live API (`/api/scenarios`, `is_active=true` rows). That is the canonical source for `ALLOWED_SCENARIOS` in this script. If any scenarios are being retired, add them to `DEACTIVATED_SCENARIOS` in `scripts/load_all_tier_results.py`.

### 3. Push from Cloud9

Save `scripts/load_all_tier_results.py` and the updated CSVs. They will be committed and
pushed automatically on save (via your git workflow).

### 4. Pull on Cloud9

```bash
cd ~/environment/coeqwal-backend
git pull
```

### 5. Dry run, verify counts

```bash
python etl/tier_data/scripts/load_all_tier_results.py --dry-run
```

Expected counts per scenario:

| Tier | Location rows / scenario |
|------|--------------------------|
| CWS_DEL | ~76 (varies, NAs skipped) |
| AG_REV | ~132 |
| ENV_FLOWS | 17 |
| RES_STOR | 8 |
| GW_STOR | 42 |
| DELTA_ECO | 1 |
| FW_DELTA_USES | 2 (but 1 tier value reported) |
| FW_EXP | 2 (but 1 tier value reported) |
| WRC_SALMON_AB | 1 (s0065 excluded) |

### 6. Generate SQL

```bash
python etl/tier_data/scripts/load_all_tier_results.py --output-sql all_tiers.sql
```

The bare filename is auto-routed into `etl/tier_data/output/all_tiers.sql`
(gitignored). Pass an absolute or relative path containing `/` to write
elsewhere.

### 7. Apply to the database

```bash
psql $DATABASE_URL -f etl/tier_data/output/all_tiers.sql
```

Check the two verification tables printed at the end:

- `tier_result`: each tier should show `(active scenarios)` = count of non-retired
  scenarios, and `(total scenarios)` = active + any retired ones.
- `tier_location_result`: row counts should match `location rows / scenario` x number
  of active scenarios (plus any legacy rows from retired scenarios, which are harmless).

### 8. Update seed CSVs

After a successful load, export the full tables back to replace the seed files
so the database can be rebuilt from scratch:

```sql
\COPY (
  SELECT scenario_short_code, tier_short_code,
         tier_1_value, tier_2_value, tier_3_value, tier_4_value,
         norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4,
         total_value, single_tier_level
  FROM tier_result
  WHERE tier_version_id = 8
  ORDER BY scenario_short_code, tier_short_code
) TO '/tmp/tier_result.csv' CSV HEADER;

\COPY (
  SELECT scenario_short_code, tier_short_code, location_type, location_id,
         location_name, tier_level, tier_value, display_order
  FROM tier_location_result
  WHERE tier_version_id = 8
  ORDER BY scenario_short_code, tier_short_code, display_order
) TO '/tmp/tier_location_result.csv' CSV HEADER;
```

Copy `/tmp/tier_result.csv` and `/tmp/tier_location_result.csv` back to
`database/seed_tables/10_tier/`.

---

## Notes

- `TIER_VERSION_ID = 8` is hardcoded throughout. Do not change without data team sign-off.
- Both UPSERTs are safe to re-run. They use `ON CONFLICT DO UPDATE`.
- `tier_location_result` has no `is_active` column. Retired scenario rows remain in
  the table but are never surfaced because the API filters on `tier_result.is_active`.
- `DETAW` is a shared `location_id` across `GW_STOR` and `DELTA_ECO` by design. It is the
  CalSim id for the Legal Delta (from the DWR Delta Evapotranspiration of Applied Water
  model), which geographically coincides with the Legal Delta polygon used by
  `DELTA_ECO`. The `tier_location_result` uniqueness constraint is
  `(scenario_short_code, tier_short_code, location_id, tier_version_id)`, so the same
  `location_id` under different `tier_short_code` values is not a collision. API routes
  that take a tier short code in the path (e.g. `/api/tier-map/{scenario}/{tier}/locations`)
  return only that tier's `DETAW` row. Any client code that keys rows by `location_id`
  across tiers should use the composite key `(tier_short_code, location_id)`.
- Generated SQL lands in `etl/tier_data/output/` by default and that whole
  directory is gitignored (see `etl/**/output/` in `.gitignore`). Only the
  script and staging CSVs are tracked.
- To load only specific tiers (e.g. after a partial data update):
  ```bash
  python etl/tier_data/scripts/load_all_tier_results.py --only ENV_FLOWS,RES_STOR --output-sql partial.sql
  psql $DATABASE_URL -f etl/tier_data/output/partial.sql
  ```
