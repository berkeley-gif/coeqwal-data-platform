# COEQWAL Backend

A comprehensive backend system for the Collaboratory for Equity in Water Allocation (COEQWAL) project, providing data APIs and infrastructure for California water management scenario presentation and analysis.

## Tech stack

### API layer
- **FastAPI** Async Python web framework with automatic OpenAPI documentation
- **Pydantic** Data validation and serialization
- **asyncpg** High-performance async PostgreSQL driver
- **Uvicorn** ASGI server

**Request flow:**
```
Request → Uvicorn → FastAPI → Pydantic (validates) → asyncpg (queries DB) → Response
```

### Database
- **PostgreSQL** Primary relational database
- **PostGIS** Spatial extensions for geospatial queries (bounding box, geometry)

### Cloud infrastructure (AWS)
- **ECS Fargate** Runs containerized API (Docker → ECR → ECS)
- **RDS PostgreSQL** Managed database with PostGIS
- **S3** Model output file storage
- **Route 53** DNS routing to api.coeqwal.org

### Data processing
- **boto3** AWS SDK for Python (ETL pipelines, database utilities)
- **Python scripts** ETL pipelines and data transformers

### Development tools
- **Docker** Containerization
- **Ruff** Python linting
- **Git** Version control

## API endpoints

**Production:** https://api.coeqwal.org | [Interactive docs](https://api.coeqwal.org/docs)

| Category | Endpoint | Description |
|----------|----------|-------------|
| **Scenarios** | `GET /api/scenarios` | List water management scenarios |
| | `GET /api/scenarios/{id}` | Get scenario details |
| **Tiers** | `GET /api/tiers/list` | List tier outcome indicators |
| | `GET /api/tiers/definitions` | Get tier descriptions (for tooltips) |
| | `GET /api/tiers/scenarios/{id}/tiers` | Get all tier data for a scenario |
| **Tier map** | `GET /api/tier-map/{scenario}/{tier}` | GeoJSON for map visualization |
| | `GET /api/tier-map/scenarios` | List scenarios with tier map data |
| | `GET /api/tier-map/tiers` | List available tier indicators |
| | `GET /api/tier-map/summary/{scenario}` | Get tier summary for a scenario |
| **Network** | `GET /api/nodes` | CalSim3 network nodes |
| | `GET /api/arcs` | Network arcs (rivers, canals) |
| | `GET /api/nodes/spatial?bbox=...` | Spatial query within bounding box |
| | `GET /api/nodes/{id}/network` | Network traversal from node |
| | `GET /api/search?q=...` | Search nodes/arcs by name |
| **Downloads** | `GET /api/scenario` | List downloadable scenario files |
| | `GET /api/download?scenario=...&type=...` | Get presigned S3 download URL |

## Quick start

```bash
# Navigate to API directory
cd api/coeqwal-api

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DATABASE_URL="postgresql://user:pass@host:5432/coeqwal"
export AWS_REGION="us-west-2"
export S3_BUCKET="coeqwal-model-run"

# Run locally
uvicorn main:app --reload --port 8000

# View API docs
open http://localhost:8000/docs
```

## Deployment

See [AWS_DEPLOYMENT_INSTRUCTIONS.md](./AWS_DEPLOYMENT_INSTRUCTIONS.md) for deployment guides including:
- CI/CD pipeline deployment
- Manual Docker + ECS deployment
- EC2 direct deployment
- Elastic Beanstalk deployment

## License

See [LICENSE](./LICENSE) for details.
