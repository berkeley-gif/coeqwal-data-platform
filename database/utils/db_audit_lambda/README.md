# Database audit Lambda function

PostgreSQL database audit that runs on AWS Lambda and saves detailed reports to S3

## This script audits:

**Database structure**
- All tables with record counts
- Column definitions and data types
- Schema organization

**Versioning system**
- `version_family`, `version`, `domain_family_map`, `developer` tables
- Active version families and configurations
- Version history and relationships

**Audit fields**
- Tables with `created_by`, `created_at`, `updated_by`, `updated_at`
- Who has been creating records
- Date ranges of record creation

**Seed table upload progress**
- Which seed tables have been loaded
- Record counts to verify successful uploads
- Missing or incomplete tables

## 🚀 Setup process

### 1. Build dependencies layer

```bash
cd database/utils/db_audit_lambda/

# Build layer with Docker (creates lambda-layer.zip)
docker build --platform linux/amd64 -t lambda-layer-builder .
docker run --rm -v "$(pwd)":/output lambda-layer-builder

# Check layer file
ls -la lambda-layer.zip
du -h lambda-layer.zip  # Should be ~40MB
```

### 2. Create Lambda layer in AWS Console

1. **AWS Console** → **Lambda** → **Layers** → **Create layer**
2. **Name**: `coeqwal-db-audit-dependencies`
3. **Upload**: `lambda-layer.zip`
4. **Compatible runtimes**: Python 3.10
5. **Compatible architectures**: x86_64

### 3. Create Lambda function in AWS Console

1. **Lambda** → **Create function**
2. **Name**: `coeqwal-database-audit`
3. **Runtime**: Python 3.10
4. **Architecture**: x86_64
5. **Execution role**: Use existing role `coeqwal-database-audit-role`

### 4. Configure Lambda function

1. **Code**: Copy/paste content from `db_audit_lambda.py`
2. **Handler**: `db_audit_lambda.lambda_handler`
3. **Timeout**: 5 minutes
4. **Memory**: 512 MB
5. **Environment variables**:
   - `DATABASE_URL`: PostgreSQL connection string
   - `S3_BUCKET`: `coeqwal-model-run`
6. **VPC**: Same VPC as database (`vpc-0ea4c7c730a13c52d`)
7. **Subnets**: Database subnets (private1 + private2)
8. **Security group**: `sg-04667c3a432b7e844` (default)
9. **Layer**: Add your `coeqwal-db-audit-dependencies` layer

### 5. Update database security group

**EC2 Console** → **Security Groups** → `coeqwal-pg-sg`:
- **Add inbound rule**: PostgreSQL (5432) from `sg-04667c3a432b7e844`

### 6. Test the function

```bash
# In AWS CloudShell
aws lambda invoke --function-name coeqwal-database-audit response.json
cat response.json
```

## 📊 Output and analysis

The audit generates two comprehensive files in S3:

### 1. Detailed JSON Report (`audit_YYYYMMDD_HHMMSS.json`)
Complete database structure with:
- Every table's columns, data types, constraints
- Record counts and sample data
- Versioning system status
- Developer tracking information

### 2. Tables Summary CSV (`tables_summary_YYYYMMDD_HHMMSS.csv`)
Spreadsheet-friendly overview:
- Table-by-table record counts
- Audit field status (created_by, created_at, etc.)
- Easy sorting and filtering

### Reports

```bash
# In AWS CloudShell or with AWS CLI configured
aws s3 cp s3://coeqwal-model-run/database_audits/audit_20250922_215626.json audit_detailed.json
aws s3 cp s3://coeqwal-model-run/database_audits/tables_summary_20250922_215626.csv tables_summary.csv

# Quick analysis commands
head -20 tables_summary.csv  # Table overview
cat audit_detailed.json | python3 -m json.tool | head -50  # Detailed structure

# Show tables with most records
cat tables_summary.csv | sort -t',' -k3 -nr | head -10

# Show version families status
cat audit_detailed.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
for vf in data['versioning_system']['version_families']:
    print(f\"{vf['short_code']}: {vf['label']} ({'ACTIVE' if vf['is_active'] else 'INACTIVE'})\")
"
```

## 💰 Cost

AWS Lambda pricing (us-west-2):
- **Requests**: $0.20 per 1M requests  
- **Duration**: $0.0000166667 per GB-second

**Estimated cost per audit run**: ~$0.01 (1 penny)

## 🔧 Manual invocation

```bash
# Basic invocation (in AWS CloudShell)
aws lambda invoke --function-name coeqwal-database-audit response.json
cat response.json

# With region specified (if using local AWS CLI)
aws lambda invoke --function-name coeqwal-database-audit --region us-west-2 response.json
```

## 🔍 Troubleshooting

### Import errors (psycopg2, pandas)
- **Check layer**: Ensure `coeqwal-db-audit-dependencies` layer is attached
- **Check runtime**: Lambda must use Python 3.10 to match layer
- **Check architecture**: Both Lambda and layer must be x86_64

### Database connection timeout
- **Check VPC**: Lambda must be in same VPC as database
- **Check security group**: Database SG must allow Lambda SG on port 5432
- **Check environment variables**: DATABASE_URL must be correct

### Function timeout
- **Increase timeout**: Configuration → General → Timeout → 5 minutes
- **Increase memory**: 512 MB for better performance

### Decimal serialization errors
The code handles PostgreSQL decimal types automatically.

## 📈 Example audit results


```bash
# Get the comprehensive analysis
aws s3 cp s3://coeqwal-model-run/database_audits/audit_20250922_215626.json .
aws s3 cp s3://coeqwal-model-run/database_audits/tables_summary_20250922_215626.csv .
```

The detailed JSON contains complete information about:
- Which seed tables have been successfully loaded
- Record counts for verification
- Column structures and data types
- Versioning system status
- Developer tracking functionality

