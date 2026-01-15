# COEQWAL Backend

A comprehensive backend system for the Collaboratory for Equity in Water Allocation (COEQWAL) project, providing data APIs and infrastructure for California water management scenario presentation and analysis.

## Tech stack

### API layer
- **FastAPI** — Async Python web framework with automatic OpenAPI documentation
- **Pydantic** — Data validation and serialization
- **asyncpg** — High-performance async PostgreSQL driver
- **Uvicorn** — ASGI server

**Request flow:**
```
Request → Uvicorn → FastAPI → Pydantic (validates) → asyncpg (queries DB) → Response
```

### Database
- **PostgreSQL** — Primary relational database
- **PostGIS** — Spatial extensions for geospatial queries (bounding box, geometry)

### Cloud infrastructure (AWS)
- **ECS / Fargate** — Containerized API deployment
- **ECR** — Docker image registry
- **S3** — Model output file storage
- **Lambda** — Presigned URL generation for downloads
- **API Gateway** — Lambda endpoint management
- **RDS** — Managed PostgreSQL hosting

### Data processing
- **boto3** — AWS SDK for Python (ETL pipelines, database utilities)
- **Python scripts** — ETL pipelines and data transformers

### Development tools
- **Docker** — Containerization
- **Ruff** — Python linting
- **Git** — Version control

## Project structure

```
coeqwal-backend/
├── api/coeqwal-api/       # FastAPI application
│   ├── main.py            # App entry point & core endpoints
│   ├── routes/            # API endpoint modules
│   │   ├── tier_endpoints.py
│   │   ├── tier_map_endpoints.py
│   │   ├── scenario_endpoints.py
│   │   ├── download_endpoints.py
│   │   ├── nodes_spatial.py
│   │   └── network_traversal.py
│   ├── config.py
│   └── requirements.txt
├── database/
│   ├── schema/            # ERD and table designs
│   ├── scripts/           # SQL migrations and loaders
│   ├── seed_tables/       # CSV seed data
│   └── utils/             # Database utilities
├── etl/                   # Data pipelines
│   ├── coeqwal-etl/       # Main ETL package
│   ├── lambda-trigger/    # S3 event triggers
│   └── tier_data/         # Tier data loaders
├── scripts/               # Utility scripts
│   └── coeqwalPresignDownloadLambda  # Lambda function code
└── data/                  # Raw and processed data files
```

## API endpoints

| Category | Endpoint | Description |
|----------|----------|-------------|
| **Scenarios** | `GET /api/scenarios` | List water management scenarios |
| **Tiers** | `GET /api/tiers/list` | List tier outcome indicators |
| | `GET /api/tiers/scenarios/{id}/tiers` | Get tier data for a scenario |
| **Tier map** | `GET /api/tier-map/{scenario}/{tier}` | GeoJSON for map visualization |
| **Network** | `GET /api/nodes` | CalSim3 network nodes |
| | `GET /api/arcs` | Network arcs (rivers, canals) |
| | `GET /api/nodes/spatial?bbox=...` | Spatial query within bounding box |
| | `GET /api/nodes/{id}/network` | Network traversal from node |
| **Downloads** | `GET /scenario` | List downloadable scenario files |
| | `GET /download?scenario=...&type=...` | Get presigned S3 download URL |

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

## Documentation

- [API documentation](./api/coeqwal-api/README.md)
- [Database ERD](./database/schema/COEQWAL_SCENARIOS_DB_ERD.md)
- [Tier map API guide](./api/coeqwal-api/TIER_MAP_API_DOCUMENTATION.md)
- [Frontend integration](./api/coeqwal-api/FRONTEND_API_DOCUMENTATION.md)

## License

See [LICENSE](./LICENSE) for details.
