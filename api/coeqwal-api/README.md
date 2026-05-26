# COEQWAL API

FastAPI backend for the COEQWAL project.

## Deployment

The API deploys via **GitHub Actions → ECR → ECS Fargate**. Push changes to `main` on GitHub. The CI pipeline auto-builds a new Docker image and deploys it to ECS when files under `api/` change. No manual steps needed unless CI is broken.

Note that we have `ruff` linting configured — run `ruff check .` before pushing to avoid failed builds. See "Linting" below.

If the auto-deploy gets stuck, see "Manual Deployment (Troubleshooting)" further down for how to rebuild and push the Docker image from Cloud9.

## Quick start

- **Base URL**: `https://api.coeqwal.org`
- **Interactive docs**: `https://api.coeqwal.org/docs`
- **Health check**: `https://api.coeqwal.org/api/health`

## Smoke tests

After a deploy, paste this into a shell to walk every endpoint family. It uses scenario `s0020` (override via `SMOKE_SCENARIO`) and targets prod (override via `COEQWAL_API_URL`). Each line prints `PASS 200 <path>` or `FAIL <code> <path>`, and the script exits non-zero if anything missed.

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
# Expect monthly_demand, monthly_sw_delivery, monthly_gw_pumping, monthly_shortage.
curl -sS "$BASE/api/statistics/scenarios/s0020/ag-demand-units/monthly?du_id=64_PA1" \
  | jq '.demand_units["64_PA1"] | keys'

# Env-flow channels list envelope uses `count`, not `total`.
curl -sS "$BASE/api/statistics/channels" | jq 'keys'
```

## How `coeqwal-website` consumes this API

The website does **not** call these endpoints with `fetch()` directly. All data access flows through the `@repo/data` package in [`coeqwal-website/packages/data`](../../../COEQWAL_repo/coeqwal-website/packages/data):

- `coeqwal/api.ts` holds endpoint URL builders.
- `coeqwal/fetchers.ts` wraps every endpoint in a typed `apiFetcher<T>` call.
- `coeqwal/hooks/*` exposes SWR hooks the website uses to read from the API. Most endpoints have a dedicated hook. A few have several hooks for different filter shapes (e.g. `useReservoirPercentiles`, `useAllReservoirPercentiles`, and `useGroupedReservoirPercentiles` all hit `/reservoir-percentiles` with different query params), and `useBatchStatistics` fans out to many endpoints in one call. Hook files are organized by domain (`useMiContractorStatistics.ts`, `useUrbanDemandUnitStatistics.ts`, `useAgStatistics.ts`, `useRefugeStatistics.ts`, etc).
- `cache/keys.ts` holds the matching cache-key constants.

When you change a URL or payload shape here, the matching website update is in those four files (types, fetchers, hooks, occasionally one component). Components never see raw endpoint URLs. Outside callers (notebooks, third-party tools, ad-hoc scripts) consume these endpoints directly.

## Local development

The supported environment for everything touching production data is Cloud9, where `$DATABASE_URL` and `$SUPERUSER_URL` are already configured. To exercise the API locally without RDS access, bring up a local Postgres with `docker compose up -d postgres` from the repo root and apply DDL from [`database/scripts/sql/.archive/`](../../database/scripts/sql/.archive/) manually. Then:

```bash
source ../../.venv/bin/activate
export DATABASE_URL="postgresql://coeqwal:coeqwal@localhost:5432/coeqwal_scenario"

uvicorn main:app --reload --port 8000

open http://localhost:8000/docs
```

To point at production RDS instead, replace `DATABASE_URL` with the production connection string. That path is only available from Cloud9 or VPN.

### Building the dev container

If you want to exercise the same Docker image CI builds, but with the dev-friendly defaults (root user, default asyncio loop, no access log), use the `dev` target of the multi-stage Dockerfile:

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

### Reservoir Statistics (`/api/statistics`)

All reservoir statistics endpoints support filtering by individual reservoirs or predefined groups.

**Filter Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `reservoirs` | string | Comma-separated reservoir short_codes (e.g., `SHSTA,OROVL,FOLSM`) |
| `group` | string | Predefined group: `major`, `cvp`, or `swp` |

> **Note:** Cannot use both `reservoirs` and `group` simultaneously. If neither is specified, defaults to the 8 major reservoirs.

**Examples:**

```bash
# Default (8 major reservoirs)
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/reservoir-percentiles"

# Filter by group
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/reservoir-percentiles?group=major"
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/reservoir-percentiles?group=cvp"
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/spill-monthly?group=swp"

# Filter by specific reservoirs
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/reservoir-percentiles?reservoirs=SHSTA,OROVL,FOLSM"
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/spill-monthly?reservoirs=SHSTA,TRNTY"
```

**Available Reservoir Groups:**

| Group | Description |
|-------|-------------|
| `major` | 8 major California reservoirs |
| `cvp` | Central Valley Project reservoirs |
| `swp` | State Water Project reservoirs |

**Major Reservoirs:** SHSTA, TRNTY, OROVL, FOLSM, MELON, MLRTN, SLUIS_CVP, SLUIS_SWP

**CVP Reservoirs:** SHSTA, TRNTY, FOLSM, MELON, MLRTN, SLUIS_CVP

**SWP Reservoirs:** OROVL, SLUIS_SWP

**Endpoints supporting these filters:**

- `GET /api/statistics/scenarios/{scenario_id}/reservoir-percentiles` - Monthly percentile bands
- `GET /api/statistics/scenarios/{scenario_id}/spill-monthly` - Monthly spill statistics

**Discovery endpoints:**

```bash
# List all reservoirs with statistics data
curl "https://api.coeqwal.org/api/statistics/reservoirs"

# List reservoir groups and their members
curl "https://api.coeqwal.org/api/statistics/reservoir-groups"

# List scenarios with percentile data
curl "https://api.coeqwal.org/api/statistics/scenarios"
```

### Network nodes & arcs (`/api/nodes`, `/api/arcs`)

**Filter Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `region` | string | Hydrologic region: `SAC`, `SJR`, `TUL`, `SF`, `SC`, `CC`, `NC` |
| `limit` | int | Maximum results (default: 1000, max: 10000) |

**Examples:**

```bash
# Get nodes in Sacramento region
curl "https://api.coeqwal.org/api/nodes?region=SAC&limit=500"

# Get arcs in San Joaquin region
curl "https://api.coeqwal.org/api/arcs?region=SJR&limit=500"
```

### Spatial Queries (`/api/nodes/spatial`)

**Filter Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bbox` | string | Bounding box: `minLng,minLat,maxLng,maxLat` |
| `zoom` | int | Map zoom level (1-20). Lower zooms show only major infrastructure |
| `limit` | int | Maximum nodes (default: 1000, max: 10000) |

**Examples:**

```bash
# Get nodes in bounding box (Sacramento Delta area)
curl "https://api.coeqwal.org/api/nodes/spatial?bbox=-122.5,37.5,-121.0,38.5&zoom=10"

# High zoom for detailed view
curl "https://api.coeqwal.org/api/nodes/spatial?bbox=-121.5,38.5,-121.0,39.0&zoom=14&limit=5000"
```

### Unfiltered nodes (`/api/nodes/unfiltered`)

**Filter Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bbox` | string | Bounding box: `minLng,minLat,maxLng,maxLat` |
| `source_filter` | string | Data source: `geopackage`, `network_schematic`, or `all` |
| `limit` | int | Maximum nodes (default: 10000, max: 50000) |

**Examples:**

```bash
# All nodes from geopackage source
curl "https://api.coeqwal.org/api/nodes/unfiltered?bbox=-122.5,37.5,-121.0,38.5&source_filter=geopackage"

# All nodes regardless of source
curl "https://api.coeqwal.org/api/nodes/unfiltered?bbox=-122.5,37.5,-121.0,38.5&source_filter=all"
```

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
curl "http://localhost:8000/api/statistics/scenarios/s0020/storage-monthly?reservoirs=SHSTA,OROVL"
```

## Deployment

Deployment is handled via GitHub Actions → ECR → ECS Fargate.

```bash
# Push to main triggers deployment (only when api/** files change)
git push origin main

# Manual ECS update (if needed)
aws ecs update-service --cluster coeqwal-api --service coeqwal-api-service --force-new-deployment --region us-west-2
```

### Manual Deployment (Troubleshooting)

If the API is running old code despite pushes to main, the ECR `:latest` image may be stale. Manually rebuild and push from Cloud9:

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

Wait 3-5 minutes, then verify:
```bash
curl https://api.coeqwal.org/
# Check "version" field matches expected version
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

**Performance:**
- Response time: 50-300ms for spatial queries
- Connection pool: 5-50 connections (auto-scaling)
- Concurrent users: 50+ supported

## Database

- **1,400+ network nodes** with PostGIS coordinates
- **1,000+ network arcs** (rivers, canals, pipelines)
- **8 scenarios** with tier outcomes
- **9 tier indicators** with location-level results
- **92 reservoirs** with monthly statistics data

See [COEQWAL_SCENARIOS_DB_ERD.md](../../database/schema/COEQWAL_SCENARIOS_DB_ERD.md) for full schema.
