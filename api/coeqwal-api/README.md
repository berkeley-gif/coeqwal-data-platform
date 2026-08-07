# COEQWAL API

FastAPI backend for the COEQWAL project.

## Deployment

The API deploys via **GitHub Actions → ECR → ECS Fargate**. When you push changes to `main` on GitHub, the CI pipeline auto-builds a new Docker image and deploys it to ECS when files under `api/` change. No manual steps needed under this CI.

Note that we have `ruff` linting configured. Run `ruff check .` before pushing to avoid failed builds. See "Linting" below.

If the auto-deploy gets stuck for whatever reason (it hasn't to date), see "Manual Deployment (Troubleshooting)" further down for how to rebuild and push the Docker image from Cloud9.

## Quick start

- **Base URL**: `https://api.coeqwal.org/api`
- **Interactive docs**: `https://api.coeqwal.org/docs`
- **Health check**: `https://api.coeqwal.org/api/health`

## Smoke tests

After a deploy, paste this into a shell to walk the main scenario, tier, and statistics endpoint families. It uses scenario `s0020` (override via `SMOKE_SCENARIO`) and targets prod (override via `COEQWAL_API_URL`). Each line prints `PASS 200 <path>` or `FAIL <code> <path>`, and the script exits non-zero if anything missed.

```bash
BASE="${COEQWAL_API_URL:-https://api.coeqwal.org}"
SCENARIO="${SMOKE_SCENARIO:-s0020}"
FAILED=0

while IFS= read -r path; do
  [ -z "$path" ] && continue
  code=$(curl -sS --max-time 60 -o /dev/null -w '%{http_code}' "$BASE$path")
  if [ "$code" = "200" ]; then
    printf 'PASS %3s %s\n' "$code" "$path"
  else
    printf 'FAIL %3s %s\n' "$code" "$path"
    FAILED=$((FAILED+1))
  fi
done <<EOF
/api/health
/api/scenarios
/api/scenarios/$SCENARIO
/api/tiers/definitions
/api/tiers/list
/api/tiers/scenarios/$SCENARIO/tiers
/api/statistics/reservoirs
/api/statistics/reservoir-groups
/api/statistics/scenarios/$SCENARIO/reservoir-percentiles
/api/statistics/scenarios/$SCENARIO/spill-monthly
/api/statistics/mi-contractors
/api/statistics/scenarios/$SCENARIO/mi-contractors/monthly
/api/statistics/scenarios/$SCENARIO/mi-contractors/period-summary
/api/statistics/demand-units
/api/statistics/scenarios/$SCENARIO/demand-units/monthly
/api/statistics/scenarios/$SCENARIO/demand-units/period-summary
/api/statistics/ag-demand-units
/api/statistics/scenarios/$SCENARIO/ag-demand-units/monthly?du_id=64_PA1
/api/statistics/scenarios/$SCENARIO/ag-demand-units/period-summary
/api/statistics/ag-aggregates
/api/statistics/scenarios/$SCENARIO/ag-aggregates/monthly
/api/statistics/scenarios/$SCENARIO/ag-aggregates/period-summary
/api/statistics/cws-aggregates
/api/statistics/scenarios/$SCENARIO/cws-aggregates/monthly
/api/statistics/scenarios/$SCENARIO/cws-aggregates/period-summary
/api/statistics/refuge-demand-units
/api/statistics/scenarios/$SCENARIO/refuge-demand-units/monthly
/api/statistics/scenarios/$SCENARIO/refuge-demand-units/period-summary
/api/statistics/channels
/api/statistics/scenarios/$SCENARIO/delta/monthly
/api/statistics/batch?scenarios=$SCENARIO
EOF

echo
if [ "$FAILED" = "0" ]; then
  echo "All endpoint families OK."
else
  echo "$FAILED endpoint(s) failed."
  exit 1
fi
```

### Targeted shape checks

For regressions specific to the recent streamline, pipe individual endpoints through `jq`:

```bash
BASE="${COEQWAL_API_URL:-https://api.coeqwal.org}"

# AG DU monthly now returns the four merged metric bands in one payload.
# Among the keys, expect monthly_demand, monthly_sw_delivery, monthly_gw_pumping,
# and monthly_shortage alongside the entry metadata (label, agency, region, etc).
curl -sS "$BASE/api/statistics/scenarios/s0020/ag-demand-units/monthly?du_id=64_PA1" \
  | jq '.demand_units["64_PA1"] | keys'

# Env-flow channels list envelope uses `count`, not `total`.
curl -sS "$BASE/api/statistics/channels" | jq 'keys'
```

## How `coeqwal-website` consumes this API

The website **should not call these endpoints with `fetch()`** directly. All data access should flow through the `@repo/data` package, which lives in the separate `coeqwal-website` repo at `packages/data` (locally: [`../../../COEQWAL_repo/coeqwal-website/packages/data`](../../../COEQWAL_repo/coeqwal-website/packages/data)). The paths below are relative to that package's `src/`:

- `src/coeqwal/api.ts` holds endpoint URL builders.
- `src/coeqwal/fetchers.ts` wraps every endpoint in a typed `apiFetcher<T>` call.
- `src/coeqwal/hooks/*` exposes SWR hooks the website uses to read from the API. Most endpoints have a dedicated hook. A few have several hooks for different filter shapes (e.g. `useReservoirPercentiles`, `useAllReservoirPercentiles`, and `useGroupedReservoirPercentiles` all hit `/reservoir-percentiles` with different query params), and `useBatchStatistics` makes a single request to `/statistics/batch` that returns storage, CWS, ag, and env-flow data for multiple scenarios at once. Hook files are organized by domain (`useMiContractorStatistics.ts`, `useUrbanDemandUnitStatistics.ts`, `useAgStatistics.ts`, `useRefugeStatistics.ts`, etc).
- `src/coeqwal/types.ts` holds the request/response type definitions.
- `src/cache/keys.ts` holds the matching cache-key constants.

When you change an endpoint here, the matching website changes are: for a path or query-param change, the URL builder in `api.ts` and its cache key in `cache/keys.ts`. For a payload-shape change, the response type in `types.ts` and the typed fetcher in `fetchers.ts`. A hook, and occasionally one component, may also need updating. Components read this API's data through hooks, never raw endpoint URLs. Outside callers (notebooks, third-party tools, ad-hoc scripts) can consume these endpoints directly (because they don't have a website to weigh down).

## Local development

Cloud9 is the supported environment; `$DATABASE_URL` is already set there and points at production RDS.

```bash
cd api/coeqwal-api

# First time (or when requirements.txt changes): build/refresh the venv
source ../../venv/bin/activate      # Cloud9 venv lives at repo-root/venv
pip install -r requirements.txt     # fastapi, uvicorn, asyncpg, ...

# DATABASE_URL is inherited from the Cloud9 shell
uvicorn main:app --reload --port 8000

open http://localhost:8000/docs
```

### Building the dev container

Work is faster with the venv + `uvicorn` flow above, but you can directly access the container when you need to test the image itself rather than the app code. It runs the same multi-stage Dockerfile CI uses, but via the `dev` target's friendlier defaults (root user, default asyncio loop, no access log). The `DATABASE_URL` value here is a placeholder.

```bash
docker build --target dev -t coeqwal-api:dev api/coeqwal-api

docker run --rm -p 8000:8000 \
  -e DATABASE_URL="postgresql://coeqwal:coeqwal@host.docker.internal:5432/coeqwal_scenario" \
  coeqwal-api:dev
```

The `prod` target of the same Dockerfile is what CI pushes to ECR (see `.github/workflows/api.yml`).

## Linting

We use [Ruff](https://docs.astral.sh/ruff/) to catch errors before deployment.

```bash
# Check for errors
ruff check .

# Auto-fix simple issues
ruff check . --fix
```

## API Endpoints & Filtering

The endpoint catalog lives in the interactive docs, generated from the live FastAPI app:

- **Swagger UI**: `https://api.coeqwal.org/docs`
- **Machine-readable reference**: `GET https://api.coeqwal.org/api` lists the endpoint families plus a `data_summary` of key row counts (scenarios, tier indicators, network nodes/arcs)

Every endpoint's query parameters are documented in Swagger (`/docs`). Two kinds of filtering are worth understanding here: **group** filters (curated named sets that Swagger can't fully convey) and ordinary attribute filters. For the full list of endpoints, use the docs above or the `## Smoke tests` block earlier in this file.

### Filtering by group: CVP / SWP divisions

The goal is to slice scenario outputs in different emergent ways, for example, SWP/CVP or NOD/SOD via `?group=<short_code>` filter. Group membership lives in the database as a `*_group` / `*_group_member` table pair per entity, so the sets change with data, not code.

**Reservoirs are the worked example.** Reservoir statistics accept either specific reservoirs or a named group:

| Parameter | Type | Description |
|-----------|------|-------------|
| `reservoirs` | string | Comma-separated short_codes (e.g., `SHSTA,OROVL,FOLSM`) |
| `group` | string | Named group: `major`, `cvp`, or `swp` |

```bash
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/reservoir-percentiles?group=cvp"
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/spill-monthly?group=swp"
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/reservoir-percentiles?reservoirs=SHSTA,OROVL,FOLSM"
```

Current membership snapshot:

| Group | Members |
|-------|---------|
| `major` | SHSTA, TRNTY, OROVL, FOLSM, MELON, MLRTN, SLUIS_CVP, SLUIS_SWP |
| `cvp` | SHSTA, TRNTY, FOLSM, MELON, MLRTN, SLUIS_CVP |
| `swp` | OROVL, SLUIS_SWP |

**Demand units are next, and already plumbed.** The urban demand-unit endpoints accept the same `?group=` filter, which looks up `du_urban_group` by `short_code`.

A group has two parts:

- a **definition** - one row in `du_urban_group` that names the group (its `short_code`, e.g. `cvp_served`)
- its **membership** - rows in `du_urban_group_member` listing which demand units belong to that group

Today `du_urban_group` defines several groups - `tier`, `cvp_served`, `swp_served`, `nod` (north of Delta), `sod` (south of Delta) - but only `tier` has its membership filled in. The division groups are named but empty. So a call like `?group=cvp_served` succeeds and returns an empty result, not an error, until someone adds those membership rows. `tier` is the only group the app needs right now; the NOD/SOD and CVP/SWP division groups can be populated later, if and when that slicing is actually required.

This `*_group` pattern is a candidate way forward for the newly-required NOD/SOD slicing across the three demand-unit types (urban, ag, refuge). The data-model work and open decisions (backfilling membership, adding `du_agriculture_group` / `du_refuge_group`, whether to consolidate with the `cws_list` registry) are tracked in [the database roadmap](../../database/README.md#demand-unit-group-membership-data-explorer-filters). The reservoirs example points the way.

### Other filters

Unlike groups, these filters match a column that already exists on each row, so there is nothing to curate or populate - they work. Every endpoint's parameters are in Swagger (`/docs`). Besides `group`, the attribute filters are:

| Endpoint family | Filters |
|-----------------|---------|
| ag | `region`, `cs3_type`, `du_id`, `provider`, `aggregate` |
| refuge | `region`, `cs3_type`, `du_id`, `water_month` |
| demand units | `du_id` |
| M&I contractors | `contractor` |
| env flow (`channels`) | `watershed`, `channel_class`, `has_mif`, `has_eflows` |
| cws | `aggregate` |
| delta | `category` |
| tiers | `scenarios`, `codes` |
| batch | `scenarios`, `types` |

Discovery endpoints:

```bash
# List all reservoirs with statistics data
curl "https://api.coeqwal.org/api/statistics/reservoirs"

# List reservoir groups and their members
curl "https://api.coeqwal.org/api/statistics/reservoir-groups"

# List scenarios
curl "https://api.coeqwal.org/api/scenarios"
```

### Data-in-depth

Six **separate, specific** endpoints under `/api/data-in-depth`, all reading the
generic `data_in_depth_*` tables. They are intentionally not merged into one
generalized endpoint; each has its own route, defaults, and scoping.

| Endpoint | Subjects | Periods | Units / measures | Value |
|----------|----------|---------|------------------|-------|
| `GET /reservoir-storage` | 8 tier reservoirs + `NOD_Reservoirs`/`SOD_Reservoirs` aggregates | `april`, `sept` | units: `volume` (TAF), `pct_capacity` | end-of-month storage |
| `GET /river-flows` | 17 river-flow channel nodes | `annual` | units: `volume` (TAF) | water-year (Oct–Sep) sum of monthly TAF |
| `GET /delta-salinity` | `X2` (Delta 2-psu isohaline position) | `april`, `sept` | units: `km` | month value |
| `GET /cws` | 106 CWS demand units + `NOD_CWS`/`SOD_CWS` aggregates | `annual` | measures: `delivery` (TAF), `pct_demand_met` (PCT_DEMAND_MET), `welfare_loss` (USD), `shortage_total` (TAF), `shortage_pct` (PCT_SHORTAGE) | annual delivery, percent of demand met & welfare-outcome measures |
| `GET /ag` | 132 ag demand units + `NOD_Agriculture`/`SOD_Agriculture` aggregates | `annual` | measures: `net_diversion`, `gw_pumping`, `shortage` (TAF), `revenue` (USD) | annual net diversion, groundwater pumping, shortage & revenue |
| `GET /groundwater-storage` | 42 groundwater WBAs + `NOD_GroundwaterStorage`/`SOD_GroundwaterStorage` aggregates | `annual` | measures: `volume` (TAF), `level` (ft) | annual groundwater storage volume & water-table level |

**Shared design (all six):**

- **Hybrid compute.** SQL filters/joins/fetches the raw per-year values; the API
  computes every derived value **live** (exceedance percentile, box-plot
  quantiles, mean, CV). Nothing derived is stored, so results stay correct when a
  WYT filter changes the population of years.
- **Multiple scenarios per request**, but each scenario is computed
  independently (no cross-scenario pooling).
- **Common parameters:** `scenarios` (required, CSV), `subjects` (default all),
  `include` (CSV of facets, combinable — `values`, `exceedance`, `box`,
  `statistics`; default all), and `wyt` (CSV of water-year-types `1`–`5`; default
  all years → **no join**). Per-endpoint `periods` and the innermost split follow
  the table above: reservoir/river/delta split by **`units`**; CWS, ag, and
  groundwater split by **`measures`** (CWS: `delivery` TAF / `pct_demand_met`
  PCT_DEMAND_MET / `welfare_loss` USD / `shortage_total` TAF / `shortage_pct`
  PCT_SHORTAGE — different units across a mix of two source files, so it
  groups by measure rather than unit; ag: `net_diversion` / `gw_pumping` /
  `shortage` (TAF) / `revenue` (USD) — revenue isn't a volume, but the value
  table doesn't require one, so it's grouped alongside the other three as a
  fourth measure; groundwater: `volume` TAF / `level` ft — different units AND
  different physical meaning, volume is extensive, level is an intensive
  water-table elevation).
- **WYT filtering semantics.** Because percentiles, box quantiles, mean, and CV
  are population-dependent, a `wyt` filter joins `scenario_water_year_type` and
  **recomputes** them over the matching years. Each scenario reports `n_years`;
  when a scenario has `n < 2` matching years `cv` is `null`, and `n = 0` yields an
  empty series (not an error). Per-row values (volume, percent-of-capacity, km,
  delivery, percent-of-demand-met, groundwater level) are just filtered, not
  recomputed.
  Entity `pct_demand_met` is read directly from the source data (not derived
  from delivery/demand). `NOD_CWS`/`SOD_CWS` aggregate `pct_demand_met` is
  demand-weighted (`sum(delivery)/sum(demand)*100`), capped at 100 to match
  the entity-row convention — see `etl/data_in_depth/open_issues.md` #5. In
  the ~9 scenarios with a missing SOD contractor (#4), both the aggregate
  delivery and percent-demand-met are computed from whichever members have
  data that scenario-year (silently partial, not flagged).
  `welfare_loss`/`shortage_total`/`shortage_pct` (from the separate
  welfare-outcomes source — see `etl/data_in_depth/cws_extract_decisions.md`)
  are available at `NOD_CWS`/`SOD_CWS` too: `welfare_loss`/`shortage_total`
  are summed across members (extensive), and aggregate `shortage_pct` is
  demand-weighted (`sum(shortage_total)/sum(supply_total+shortage_total)*100`)
  — unlike `pct_demand_met` this can never exceed 100 (both terms are
  non-negative), so no capping is needed. `welfare_loss`'s extreme skew
  (open_issues.md #9) also means its `exceedance` facet is unconditionally
  suppressed for now (silently
  omitted, not an error) — a future version will compute welfare_loss
  percentiles over **all** water years always, ignoring `wyt`, rather than
  recomputing per filtered population like every other series here; until
  that's built, requesting `include=exceedance` for `welfare_loss` just gets
  no `exceedance` key back. `values`/`box`/`statistics` are unaffected, and
  this is scoped to `welfare_loss` only — see open_issues.md #10.
  `NOD_GroundwaterStorage`/`SOD_GroundwaterStorage` aggregates only have the
  `volume` measure — summing water-table elevations across WBAs is physically
  meaningless, so `level` is never aggregated (permanent, not pending; see
  `etl/data_in_depth/open_issues.md` #8); requesting `measures=level` for those
  two subjects returns an empty series.

```bash
# Reservoir: box + stats, volume, September, two scenarios
curl ".../api/data-in-depth/reservoir-storage?scenarios=s0011,s0020&include=box,statistics&units=volume&periods=sept"

# Reservoir: NOD/SOD aggregate exceedance curve, wet + above-normal years only
curl ".../api/data-in-depth/reservoir-storage?scenarios=s0020&subjects=NOD_Reservoirs,SOD_Reservoirs&include=exceedance&wyt=1,2"

# River flow: annual TAF stats for two gauges
curl ".../api/data-in-depth/river-flows?scenarios=s0011&subjects=SAC000,YUB002&include=statistics"

# Delta X2: April position, dry years only
curl ".../api/data-in-depth/delta-salinity?scenarios=s0011,s0020&periods=april&wyt=4,5"

# CWS: NOD/SOD aggregate percent-demand-met stats, two scenarios
curl ".../api/data-in-depth/cws?scenarios=s0011,s0020&subjects=NOD_CWS,SOD_CWS&measures=pct_demand_met&include=box,statistics"

# CWS: dry-year percent-demand-met for one system
curl ".../api/data-in-depth/cws?scenarios=s0020&subjects=MWD&measures=pct_demand_met&wyt=4,5"

# CWS: welfare-outcome measures for one demand unit
curl ".../api/data-in-depth/cws?scenarios=s0020&subjects=02_NU&measures=welfare_loss,shortage_total,shortage_pct"

# CWS: NOD/SOD aggregate shortage_pct stats, two scenarios
curl ".../api/data-in-depth/cws?scenarios=s0011,s0020&subjects=NOD_CWS,SOD_CWS&measures=shortage_pct&include=box,statistics"

# Ag: NOD/SOD aggregate shortage exceedance curve, two scenarios
curl ".../api/data-in-depth/ag?scenarios=s0011,s0020&subjects=NOD_Agriculture,SOD_Agriculture&measures=shortage&include=exceedance"

# Ag: all four measures for one demand unit, dry years only
curl ".../api/data-in-depth/ag?scenarios=s0020&subjects=02_NA&wyt=4,5"

# Ag: NOD/SOD aggregate revenue stats, two scenarios
curl ".../api/data-in-depth/ag?scenarios=s0011,s0020&subjects=NOD_Agriculture,SOD_Agriculture&measures=revenue&include=box,statistics"

# Groundwater: NOD/SOD aggregate storage volume stats, two scenarios (level has no aggregate)
curl ".../api/data-in-depth/groundwater-storage?scenarios=s0011,s0020&subjects=NOD_GroundwaterStorage,SOD_GroundwaterStorage&measures=volume&include=box,statistics"

# Groundwater: volume + level for one WBA, dry years only
curl ".../api/data-in-depth/groundwater-storage?scenarios=s0020&subjects=WBA2&wyt=4,5"
```

Response is nested `scenario → <subjects> → period → <series> → facets`, where
`<series>` is the **unit** (`TAF`/`PCT_CAP`/`km`) for reservoir/river/delta, or the
**measure** (`delivery`/`pct_demand_met` for CWS; `net_diversion`/`gw_pumping`/
`shortage` for ag; `volume`/`level` for groundwater) for CWS/ag/groundwater. The
subject-array key is `reservoirs`, `rivers`, or `subjects` (delta, CWS, ag, and
groundwater):

```jsonc
{
  "wyt_filter": [1, 2],                 // null when default (all years)
  "scenarios": [
    { "scenario": "s0020", "n_years": 41,
      "reservoirs": [                    // "rivers" / "subjects" on the other endpoints
        { "subject": "NOD_Reservoirs", "kind": "aggregate", "label": "North of Delta Reservoirs",
          "periods": { "sept": { "TAF": {
            "values":     [ { "water_year": 1922, "value": 8901.2 }, … ],
            "exceedance": [ { "water_year": 1999, "value": 11056.0, "percentile": 2.38 }, … ],
            "box":        { "min": …, "q1": …, "median": …, "q3": …, "max": …,
                            "whisker_low": …, "whisker_high": …, "outliers": [ … ] },
            "statistics": { "n": 41, "mean": …, "cv": … }
          } } } } ] } ] }
  ]
}
```

For CWS, ag, and groundwater the innermost split is the **measure** instead of
a unit — since the unit isn't the dict key in that case, each facet block also
echoes its own `"unit"`:

```jsonc
{ "scenario": "s0020", "n_years": 101,
  "subjects": [
    { "subject": "NOD_CWS", "kind": "aggregate", "label": "North of Delta Community Water Systems",
      "periods": { "annual": {
        "delivery": { "unit": "TAF", "values": […], "box": {…}, "statistics": { "n": 101, "mean": …, "cv": … } },
        "pct_demand_met": { "unit": "PCT_DEMAND_MET", "values": […], "box": {…}, "statistics": {…} },
        "welfare_loss": { "unit": "USD", "values": […], "box": {…}, "statistics": {…} },
        "shortage_total": { "unit": "TAF", "values": […], "box": {…}, "statistics": {…} },
        "shortage_pct": { "unit": "PCT_SHORTAGE", "values": […], "box": {…}, "statistics": {…} }
        // welfare_loss has no "exceedance" key here (or anywhere) - suppressed for now, see open_issues.md #10
      } } } ] }
```

```jsonc
{ "scenario": "s0020", "n_years": 100,
  "subjects": [
    { "subject": "02_NU", "kind": "entity", "label": "Anderson City of Anderson 4510001",
      "periods": { "annual": {
        "welfare_loss":    { "unit": "USD", "values": […], "statistics": {…} },
        "shortage_total":  { "unit": "TAF", "values": […], "statistics": {…} },
        "shortage_pct":    { "unit": "PCT_SHORTAGE", "values": […], "statistics": {…} }
      } } } ] }
```

```jsonc
{ "scenario": "s0020", "n_years": 100,
  "subjects": [
    { "subject": "02_NA", "kind": "entity", "label": "02_NA",
      "periods": { "annual": {
        "net_diversion": { "unit": "TAF", "values": […], "statistics": {…} },
        "gw_pumping":    { "unit": "TAF", "values": […], "statistics": {…} },
        "shortage":      { "unit": "TAF", "values": […], "statistics": {…} },
        "revenue":       { "unit": "USD", "values": […], "statistics": {…} }
      } } } ] }
```

```jsonc
{ "scenario": "s0020", "n_years": 101,
  "subjects": [
    { "subject": "NOD_GroundwaterStorage", "kind": "aggregate", "label": "North of Delta Groundwater Storage",
      "periods": { "annual": {
        "volume": { "unit": "TAF", "values": […], "box": {…}, "statistics": { "n": 101, "mean": …, "cv": … } }
        // no "level" key here - not aggregated (see open_issues.md #8)
      } } } ] }
```

(Reservoir/river/delta don't get this extra key — there the unit already *is*
the dict key, e.g. `"periods": { "sept": { "TAF": { "values": … } } }`.)

## Testing

```bash
# Health check
curl http://localhost:8000/api/health

# Sample queries
curl "http://localhost:8000/api/nodes?limit=5"
curl "http://localhost:8000/api/tiers/scenarios/s0020/tiers"
curl "http://localhost:8000/api/tiers/scenarios/s0020/locations?codes=RES_STOR,CWS_DEL"

# Reservoir statistics with filtering
curl "http://localhost:8000/api/statistics/scenarios/s0020/reservoir-percentiles?group=major"
curl "http://localhost:8000/api/statistics/scenarios/s0020/spill-monthly?reservoirs=SHSTA,OROVL"
```

## Deployment

Deployment is handled via GitHub Actions → ECR → ECS Fargate.

```bash
# Push to main triggers deployment when api/** changes (excluding api/**/*.md docs; also fires on changes to the workflow file)
git push origin main

# Manual ECS update (if needed)
aws ecs update-service --cluster coeqwal-api --service coeqwal-api-service --force-new-deployment --region us-west-2
```

### Manual Deployment (Troubleshooting)

If the API is running old code despite pushes to main, the ECR `:latest` image may be stale. The ECS task definition tracks `:latest`, so a forced deployment re-pulls whatever `:latest` currently points to - the fix is to rebuild `:latest` and force a new deployment from Cloud9:

```bash
cd ~/environment/coeqwal-backend
git pull origin main

# Verify version in code
grep "API_VERSION" api/coeqwal-api/main.py

# Login to ECR
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 533266975152.dkr.ecr.us-west-2.amazonaws.com

# Build fresh, production target
docker build --no-cache --target prod -f api/coeqwal-api/Dockerfile -t coeqwal-network-api:latest api/coeqwal-api

# Tag and push to ECR
docker tag coeqwal-network-api:latest 533266975152.dkr.ecr.us-west-2.amazonaws.com/coeqwal-network-api:latest
docker push 533266975152.dkr.ecr.us-west-2.amazonaws.com/coeqwal-network-api:latest

# Force new deployment
aws ecs update-service --cluster coeqwal-api --service coeqwal-api-service --force-new-deployment --region us-west-2
```

Wait 3-5 minutes, then confirm the rollout actually completed. Don't rely on the `version` field alone - it only changes if `API_VERSION` was bumped in this commit, so it can read "unchanged" even when a fresh image is live (or stale and you'd never know):
```bash
# The new rollout should settle to a single PRIMARY deployment with rolloutState COMPLETED
aws ecs describe-services --cluster coeqwal-api --services coeqwal-api-service --region us-west-2 --query 'services[0].deployments'

# Confirm the running :latest digest matches what you just pushed
aws ecr describe-images --repository-name coeqwal-network-api --region us-west-2 --image-ids imageTag=latest --query 'imageDetails[0].imageDigest'

# If API_VERSION was bumped, this should reflect the new value
curl https://api.coeqwal.org/   # check "version" field
```

### Checking Deployment Status

```bash
# Check ECS service events
aws ecs describe-services --cluster coeqwal-api --services coeqwal-api-service --region us-west-2 --query 'services[0].events[0:5]'

# Check running tasks
aws ecs list-tasks --cluster coeqwal-api --service-name coeqwal-api-service --region us-west-2

# Check recent logs
aws logs tail /ecs/coeqwal-api --since 10m --region us-west-2
```

## Architecture

```
Internet → Route 53 (api.coeqwal.org) → ALB → ECS Fargate → PostgreSQL RDS
```

For the full AWS architecture (RDS, ECS Fargate, ALB, Route 53, networking, cost, and a diagram), see `INFRASTRUCTURE.md` in the private [`coeqwal-private-docs`](https://github.com/berkeley-gif/coeqwal-private-docs) repo.

See [`database/schema/ERD.md`](../../database/schema/ERD.md) for the full database schema.
