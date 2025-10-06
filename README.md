# COEQWAL Backend

A comprehensive backend system for COEQWAL scenario data and analytics.

## Repository Structure

```
coeqwal-backend/
├── database/
│ ├── schema/ # ERD and table definitions
│ ├── seed_tables/ # Initial data for lookup tables
│ └── utils/ # Currently db audit lambdas
├── etl/
│ ├── coeqwal-etl/ # DSS extraction and validation
│ └── lambda-trigger/ # AWS lambda trigger on model-run upload
├── 🌐 api/
│ └── coeqwal-api/ # FastAPI and web services
├── config/
│ └── environments/ # Centralized config management (underused currently)
```

## Quick start

### Prerequisites
- Docker Desktop
- AWS CLI configured
- PostgreSQL RDS instance (AWS)

### Architecture
This system runs primarily in **Docker containers** and **AWS services**:
- **ETL**: AWS Batch jobs using Docker images
- **API**: Can run locally or in containers
- **Database**: PostgreSQL RDS on AWS
- **Triggers**: AWS Lambda functions

### Key Components
```bash
# ETL
cd etl/coeqwal-etl/
docker build -t coeqwal-etl .

# Lambda Audit
cd database/utils/db_audit_lambda/
docker build --platform linux/amd64 -t lambda-layer-builder .

# Database connection
export DATABASE_URL="postgresql://..."
aws lambda invoke --function-name coeqwal-database-audit response.json

# API (Local development)
cd api/coeqwal-api/
pip install -r requirements.txt
export DATABASE_URL="postgresql://..."
uvicorn main:app --reload
```
