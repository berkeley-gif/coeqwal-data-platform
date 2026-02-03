# COEQWAL API

FastAPI backend for the COEQWAL project.

## Quick start

- **Base URL**: `https://api.coeqwal.org`
- **Interactive docs**: `https://api.coeqwal.org/docs`
- **Health check**: `https://api.coeqwal.org/api/health`

## Local development

```bash
# Install dependencies
pip install -r requirements.txt

# Set database connection
export DATABASE_URL="postgresql://user:pass@host:5432/coeqwal"

# Run locally
uvicorn main:app --reload --port 8000

# View docs
open http://localhost:8000/docs
```

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
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/storage-monthly?group=swp"

# Filter by specific reservoirs
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/reservoir-percentiles?reservoirs=SHSTA,OROVL,FOLSM"
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/spill-monthly?reservoirs=SHSTA,TRNTY"
curl "https://api.coeqwal.org/api/statistics/scenarios/s0020/period-summary?reservoirs=MELON,MLRTN"
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
- `GET /api/statistics/scenarios/{scenario_id}/storage-monthly` - Monthly storage statistics
- `GET /api/statistics/scenarios/{scenario_id}/spill-monthly` - Monthly spill statistics  
- `GET /api/statistics/scenarios/{scenario_id}/period-summary` - Period-of-record summary

**Discovery endpoints:**

```bash
# List all reservoirs with statistics data
curl "https://api.coeqwal.org/api/statistics/reservoirs/all"

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
curl "http://localhost:8000/api/tier-map/s0020/RES_STOR"
curl "http://localhost:8000/api/tiers/scenarios/s0020/tiers"

# Reservoir statistics with filtering
curl "http://localhost:8000/api/statistics/scenarios/s0020/reservoir-percentiles?group=major"
curl "http://localhost:8000/api/statistics/scenarios/s0020/storage-monthly?reservoirs=SHSTA,OROVL"
```

## Deployment

Deployment is handled via GitHub Actions → ECR → ECS Fargate.

```bash
# Push to main triggers deployment
git push origin main

# Manual ECS update (if needed)
aws ecs update-service --cluster coeqwal-api --service coeqwal-api-service --force-new-deployment --region us-west-2
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
