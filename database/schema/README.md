# Database schema documentation

## Overview

This directory contains the Entity-Relationship Diagram (ERD) and schema documentation for the COEQWAL Scenarios Database.

## Files

| File | Purpose |
|------|---------|
| `COEQWAL_SCENARIOS_DB_ERD.md` | Primary ERD documentation |
| `network_entity_erd.txt` (in `.archive/`) | Complete DBML ERD with all layers |
| `ERD_MISSING_SECTIONS.md` (in `.archive/`) | Documentation for versioning/lookup sections |

## ERD verification

To verify the ERD matches the production database:

```bash
# 1. Run the database audit Lambda (generates JSON)
aws lambda invoke --function-name coeqwal-database-audit --region us-west-2 response.json

# 2. Download the audit file
aws s3 cp s3://coeqwal-model-run/database_audits/audit_YYYYMMDD_HHMMSS.json ./audits/

# 3. Verify existing ERD against audit
python ../audit/verify_erd_against_audit.py COEQWAL_SCENARIOS_DB_ERD.md ../../audits/latest.json

# 4. Generate new ERD from audit (if updates needed)
python ../audit/generate_erd_from_audit.py ../../audits/latest.json GENERATED_ERD.md
```

## Updating the ERD

When adding new tables:
1. Add the SQL script to `../scripts/sql/` with appropriate numbering
2. Document the table in `COEQWAL_SCENARIOS_DB_ERD.md`
3. Run ERD verification to confirm consistency
4. Update the Schema Implementation Status in `../README.md`