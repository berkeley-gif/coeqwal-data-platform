# COEQWAL API

FastAPI backend for the COEQWAL project.

## Quick start

- **Base URL**: `https://api.coeqwal.org`
- **Interactive docs**: `https://api.coeqwal.org/docs`
- **Health check**: `https://api.coeqwal.org/api/health`

## API endpoints

### Scenarios
| Endpoint | Description |
|----------|-------------|
| `GET /api/scenarios` | List all water management scenarios |
| `GET /api/scenarios/{id}` | Get scenario details |
| `GET /api/scenarios/{id}/compare/{other_id}` | Compare two scenarios |

### Tier data (for charts)
| Endpoint | Description |
|----------|-------------|
| `GET /api/tiers/list` | List all tier indicators with metadata |
| `GET /api/tiers/definitions` | Get tier descriptions for tooltips |
| `GET /api/tiers/scenarios/{id}/tiers` | Get all tiers for a scenario |
| `GET /api/tiers/scenarios/{id}/tiers/{tier_code}` | Get specific tier data |

### Tier map (for map visualization)
| Endpoint | Description |
|----------|-------------|
| `GET /api/tier-map/scenarios` | List scenarios with tier map data |
| `GET /api/tier-map/tiers` | List available tier indicators |
| `GET /api/tier-map/summary/{scenario}` | Get tier summary for a scenario |
| `GET /api/tier-map/{scenario}/{tier}` | Get GeoJSON for map rendering |
| `GET /api/tier-map/{scenario}/{tier}/locations` | Get tier locations (no geometry) |

### Network data
| Endpoint | Description |
|----------|-------------|
| `GET /api/nodes` | Get CalSim3 network nodes (up to 10,000) |
| `GET /api/arcs` | Get network arcs with geometry (up to 10,000) |
| `GET /api/nodes/spatial?bbox=...&zoom=...` | Spatial query within bounding box |
| `GET /api/nodes/{id}/network` | Network traversal from a node |
| `GET /api/nodes/{id}/analysis` | Upstream/downstream connections |
| `GET /api/search?q=...` | Search nodes/arcs by name or code |

### Downloads
| Endpoint | Description |
|----------|-------------|
| `GET /scenario` | List downloadable scenario files |
| `GET /download?scenario=...&type=...` | Get presigned S3 download URL |

### System
| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Database connectivity check |
| `GET /docs` | Interactive Swagger documentation |
| `GET /redoc` | ReDoc documentation |

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

## Testing

```bash
# Health check
curl http://localhost:8000/api/health

# Sample queries
curl "http://localhost:8000/api/nodes?limit=5"
curl "http://localhost:8000/api/tier-map/s0020/RES_STOR"
curl "http://localhost:8000/api/tiers/scenarios/s0020/tiers"
```

## Deployment

Deployment is handled via GitHub Actions → ECR → ECS Fargate.

```bash
# Push to main triggers deployment
git push origin main

# Manual ECS update (if needed)
# ECS Console → Update service → Force new deployment
```

See [AWS_DEPLOYMENT_INSTRUCTIONS.md](../../AWS_DEPLOYMENT_INSTRUCTIONS.md) for detailed guides.

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

See [COEQWAL_SCENARIOS_DB_ERD.md](../../database/schema/COEQWAL_SCENARIOS_DB_ERD.md) for full schema.
