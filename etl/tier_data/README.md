# ETL.Tier Outcome Results

Loads tier outcome data for all active scenarios into the `tier_result` and
`tier_location_result` database tables.

---

## How to load new tier data

**1. Drop new CSVs into `etl/tier_data/staging/`.** Filenames are fixed: `CWS_DEL.csv`, `AG_REV.csv`, `ENV_FLOWS.csv`, `RES_STOR.csv`, `GW_STOR.csv`, `DELTA_ECO.csv`, `FW_DELTA_USES.csv`, `FW_EXP.csv`, `WRC_SALMON_AB.csv`.

**2. If the active scenario list changed**, open `etl/tier_data/load_all_tier_results.py` and edit `ALLOWED_SCENARIOS` at the top. Move retired ones into `DEACTIVATED_SCENARIOS`.

**3. Commit and push from your laptop.** The staging CSVs are git-tracked on purpose.

**4. On Cloud9: `git pull`.**

**5. Dry run to check counts:**

```bash
cd etl/tier_data
python load_all_tier_results.py --dry-run
```

**6. Generate the SQL:**

```bash
python load_all_tier_results.py --output-sql all_tiers.sql
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
| `ENV_FLOWS.csv` | First col = station short code (row index). Remaining cols = scenario codes. Values = tier 1-4 |
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

### 2. Update the scenario allowlist if needed

Open `load_all_tier_results.py` and update `ALLOWED_SCENARIOS` at the top of the file.
If any scenarios are being retired, add them to `DEACTIVATED_SCENARIOS`.

### 3. Push from your local machine

Save `load_all_tier_results.py` and the updated CSVs. They will be committed and
pushed automatically on save (via your git workflow).

### 4. Pull on Cloud9

```bash
cd ~/environment/coeqwal-backend
git pull
```

### 5. Dry run, verify counts

```bash
cd etl/tier_data
python load_all_tier_results.py --dry-run
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
python load_all_tier_results.py --output-sql all_tiers.sql
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
  python load_all_tier_results.py --only ENV_FLOWS,RES_STOR --output-sql partial.sql
  psql $DATABASE_URL -f etl/tier_data/output/partial.sql
  ```
