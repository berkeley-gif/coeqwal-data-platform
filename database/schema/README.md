# Database schema documentation

## Overview

This directory contains the Entity-Relationship Diagram (ERD) and schema documentation for the COEQWAL Scenarios Database.

## ERD verification

To verify the ERD matches the production database, run all commands from `database/schema/` (this directory):

```bash
cd /path/to/coeqwal-backend/database/schema

# 1. Run the database audit Lambda (generates JSON)
aws lambda invoke --function-name coeqwal-database-audit --region us-west-2 response.json

# 2. Download the audit file
aws s3 cp s3://coeqwal-model-run/database_audits/audit_YYYYMMDD_HHMMSS.json ../../audits/

# 3. Verify existing ERD against audit
python ../audit/verify_erd_against_audit.py COEQWAL_SCENARIOS_DB_ERD.md ../../audits/latest.json

# 4. Generate new ERD from audit (if updates needed)
python ../audit/generate_erd_from_audit.py ../../audits/latest.json GENERATED_ERD.md
```

## Entity model: MI contractors vs urban demand units

**MI (M&I contractors):** These are the 16 named SWP contractor agencies (MWD, Alameda County, Kern County, Santa Clara Valley, etc.) plus an MWD aggregate. CalSim tracks them explicitly by contractor name using `D_*_PMI` delivery arcs and `PERDV_SWP_*` allocation fractions. These are institutional entities -- water agencies that have Table A contracts with the State Water Project.

**DU_urban (urban demand units):** These are 81 geographic demand units (DUs) defined by CalSim's spatial grid. Each DU is a geographic zone like `10_SBA1`, `26S_SJR1`, etc. Variable mappings come from the `du_urban_variable` database table. These represent where water goes spatially, not which agency holds the contract.

An SWP contractor like MWD serves multiple DUs. A single DU might receive water from multiple projects. The two modules overlap in coverage but answer different questions: MI tells you "how much did Kern County Water Agency get?", DU_urban tells you "how much water reached geographic zone 26S?"

## Updating the ERD

When adding new tables:
1. Add the SQL script to `../scripts/sql/` with appropriate numbering
2. Document the table in `COEQWAL_SCENARIOS_DB_ERD.md`
3. Run ERD verification to confirm consistency
4. Update the Schema Implementation Status in `../README.md`