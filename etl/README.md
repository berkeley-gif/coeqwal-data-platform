# ETL (Extract, Transform, Load) Framework

Automated DSS file processing pipeline that extracts CSV data from CalSim model runs and validates against reference data.

## Architecture

### AWS-Native pipeline
- **Trigger**: S3 upload of DSS ZIP files
- **Processing**: AWS Batch jobs using Docker containers
- **Validation**: Automatic comparison against reference csvs
- **Storage**: Results saved to S3 with reports

Docker + AWS Batch gives us:
- Consistent environment (Linux + heclib.a)
- Scalable processing (multiple concurrent jobs)
- Cost-effective (pay only for processing time)
- Automated workflow (S3 upload → automatic processing)

### Components

#### `coeqwal-etl/` - Main ETL container
Docker-based DSS extraction using `pydsstools`:
- **Input**: DSS files from CalSim model runs
- **Output**: CSV time series data
- **Validation**: Compares against reference data with configurable tolerances
- **Platform**: Linux containers (AWS Batch compatible)

```bash
# Build ETL container
cd coeqwal-etl/
docker build -t coeqwal-etl .
```

#### `lambda-trigger/` S3 Event Handler
AWS Lambda function that triggers ETL jobs:
- **Trigger**: S3 ObjectCreated events on DSS ZIP uploads
- **Action**: Submits AWS Batch job with validation parameters
- **Monitoring**: Updates DynamoDB with job status

## Workflow

### 1. Upload DSS files
```
s3://coeqwal-model-run/ready/
```

### 2. Automatic processing
- Lambda detects upload
- Submits Batch job
- Docker container extracts CSV data
- Validates against reference CSV (if provided)

### 3. Results storage
```
s3://coeqwal-model-run/scenario/{scenario_short_code}/
├── csv/                    # Extracted CSV files
├── validation/             # Validation reports (JSON + CSV)
└── {scenario_short_code}_manifest.json     # Processing summary
```

## Validation framework

### Testing
- **Tolerances**: 1e-6 absolute and relative
- **Scope**: Compares all common variables between reference and extracted data
- **Reporting**: Detailed mismatch analysis with exact differences
- **Status**: PASS/FAIL with comprehensive summaries

## Technical details

### DSS library
Uses `pydsstools` with `heclib.a` (Linux static library)