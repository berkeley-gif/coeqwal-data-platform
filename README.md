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

## API

**Production:** https://api.coeqwal.org

**Interactive docs:** https://api.coeqwal.org/docs

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

## License

See [LICENSE](./LICENSE) for details.
