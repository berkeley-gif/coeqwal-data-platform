# ETL (Extract, Transform, Load) Framework

Automated DSS file processing pipeline that extracts CSV data from CalSim model runs and validates against reference data.

---

## AWS production deployment

### Architecture
**AWS-Native pipeline** for automated processing:
- **Trigger**: S3 upload of DSS ZIP files
- **Processing**: AWS Batch jobs using Docker containers
- **Validation**: Automatic comparison against reference CSVs (uploaded Trend Report)
- **Storage**: Results saved to S3 with reports

### Benefits of Docker + AWS Batch:
- **Consistent environment** (Linux + heclib.a)
- **Scalable processing** (multiple concurrent jobs)
- **Cost-effective** (pay only for processing time, no Windows licensing)
- **Automated workflow** (S3 upload triggers automatic processing)

### Components

#### `coeqwal-etl/` - Main ETL container
Docker-based DSS extraction using `pydsstools`:
- **Input**: DSS files from CalSim model runs
- **Output**: CSV time series data
- **Validation**: Compares against reference data with configurable tolerances
- **Platform**: Linux containers (AWS Batch compatible)

#### `lambda-trigger/` - S3 Event Handler
AWS Lambda function that triggers ETL jobs:
- **Trigger**: S3 ObjectCreated events on DSS ZIP uploads
- **Action**: Submits AWS Batch job with validation parameters
- **Monitoring**: Updates DynamoDB with job status

### AWS workflow

#### 1. Upload DSS files
```
s3://coeqwal-model-run/ready/
```

#### 2. Automatic processing
- Lambda detects upload
- Submits Batch job
- Docker container extracts CSV data
- Validates against reference CSV (if provided)

#### 3. Results storage
```
s3://coeqwal-model-run/scenario/{scenario_short_code}/
├── csv/                    # Extracted CSV files
├── validation/             # Validation reports (JSON + CSV)
└── {scenario_short_code}_manifest.json     # Processing summary
```

---

## Local development & processing

### Prerequisites
- Docker installed and running on your machine
- DSS files available locally

### Step-by-step instructions

#### 1. Build the Docker container
```bash
cd etl/coeqwal-etl/
docker build -t coeqwal-dss .
```

#### 2. Prepare your local directories
```bash
# Create directories for input and output
mkdir -p ./dss_processing/input
mkdir -p ./dss_processing/output

# Copy your DSS files to input directory
cp /path/to/your/file.dss ~/dss_processing/input/
```

#### 3. Run DSS to CSV conversion
```bash
# Basic conversion (CalSim output)
docker run -v ~/dss_processing/input:/input -v ~/dss_processing/output:/output --entrypoint python coeqwal-dss /app/python-code/dss_to_csv.py --dss /input/your_file.dss --csv /output/result.csv --type calsim_output

# SV input conversion
docker run -v ~/dss_processing/input:/input -v ~/dss_processing/output:/output --entrypoint python coeqwal-dss /app/python-code/dss_to_csv.py --dss /input/your_sv_file.dss --csv /output/sv_result.csv --type sv_input
```

#### 4. Validation (optional)
```bash
# Compare your extracted CSV against a reference
docker run --platform linux/amd64 -v ./dss_processing:/data --entrypoint python coeqwal-dss /app/python-code/validate_csvs.py --ref /data/output/coeqwal_s0011_adjBL_wTUCP_DV_v0.0.csv --file /data/output/result.csv --abs-tol 1e-6 --rel-tol 1e-6 --verbose --out-csv /data/output/detailed_mismatches.csv --out-json /data/output/validation_summary.json
```
---

## 📊 Validation framework

### Tolerance parameters
- **Absolute tolerance (`abs_tol`)**: Maximum allowed absolute difference between values
  - Example: `abs_tol=1e-6` means values must be within ±0.000001 units
  - Used for values close to zero where relative comparison isn't meaningful
  
- **Relative tolerance (`rel_tol`)**: Maximum allowed relative difference as a fraction
  - Example: `rel_tol=1e-6` means values must be within 0.0001% of each other
  - Used for larger values where proportional differences matter more

### Validation logic
Values are considered equal if:
```python
# Both are NaN OR within tolerances
np.isclose(value1, value2, atol=abs_tol, rtol=rel_tol, equal_nan=True)
```

### Testing details
- **Default tolerances**: 1e-6 absolute and relative
- **Scope**: Compares all common variables between reference and extracted data
- **Reporting**: Detailed mismatch analysis with exact differences
- **Status**: PASS/FAIL with comprehensive summaries

---

## 🔧 Technical details

### DSS library
Uses `pydsstools` with `heclib.a` (Linux static library) for reading HEC-DSS files.

### Supported DSS types
- **CalSim Output**: Time series model results (typically monthly data)
- **SV Input**: Scenario variables and boundary conditions