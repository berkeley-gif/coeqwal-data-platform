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
