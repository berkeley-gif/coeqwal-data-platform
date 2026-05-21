"""COEQWAL ETL package.

Subpackages:
- common: shared constants, S3 path builders, DB helpers, structured logging
- ingestion: Google Drive to S3 ingestion (gdrive_bulk_download and friends)
- statistics: CalSim CSV to PostgreSQL statistics modules
- tier_data: team-delivered tier outcome CSVs to PostgreSQL
- batch-container: AWS Batch DSS-to-CSV extraction
- lambda: S3 PUT trigger for AWS Batch jobs
- verification: end-to-end accuracy checks

See etl/README.md for the operator runbook.
"""
