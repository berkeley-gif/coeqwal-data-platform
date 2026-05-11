# COEQWAL Data Platform

A comprehensive data platform for the Collaboratory for Equity in Water Allocation (COEQWAL) project, providing multi-level data schema, PostgreSQL database with PostGIS extension, data APIs, and upload and verification infrastructure for California water management scenario presentation, analysis, and review.

## Backend at a glance

- **PostgreSQL + PostGIS on RDS**, FastAPI in front (`api/coeqwal-api/`), ETL pipelines under `etl/`, and audit & verification under `database/audit/` and `etl/statistics/verify_*`.
- The schema is a strict layered architecture (see `database/README.md` for full details):
  - **Layers 00–08** = foundational/reference data (versioning, lookups, network, **entities**, variables, assumptions/operations, scenarios, hydroclimate, themes).
  - **Layers 10+** = derived results (tier results, monthly stats, period summaries).
- Source-of-truth ERD: `database/schema/COEQWAL_SCENARIOS_DB_ERD.md` (~96 tables, ~402k rows). The latest monthly audit is in `audits/monthly_<timestamp>/report.md`.
- Audit chain (per `database/audit/README.md`):
  - `database/audit/run_monthly_audit.py` → schema + content + health + cost.
  - `database/audit/verify_erd_against_audit.py` → ERD doc vs. live DB schema.
  - `database/scripts/sql/<layer>/09_verify_level<NN>.sql` → per-layer structural checks.
  - `etl/statistics/verify_all_sections.py` → CSV→DB statistics accuracy (Layer 2).
  - `etl/statistics/verify_api.py` → DB→API accuracy (Layer 3).
- Standard data shape (used for every domain — reservoirs, channels, ag DUs, refuges, MI contractors, CWS — community water systems):
  1. **Entity table** in layer `03_entity/` with `id` PK, `short_code` UNIQUE, FKs to lookup tables (`hydrologic_region_id`, `source_id`, `model_source_id`, …), and the standard audit columns (`is_active`, `created_at`, `created_by`, `updated_at`, `updated_by`) populated by the `set_audit_fields()` trigger.
  2. **Variable mapping** in layer `04_variable/` (e.g. `du_urban_variable`, `channel_variable`) holding the CalSim variable names per entity.
  3. **Optional multi-arc / sub-entity tables** (`du_urban_delivery_arc`, `mi_contractor_delivery_arc`) for entities that need to sum multiple CalSim arcs.
  4. **Group/membership tables** (`*_group` + `*_group_member`) for analytical filtering.
  5. **Statistics tables** in layer 10+ (`*_monthly`, `*_period_summary`) keyed by `scenario_short_code` + `<entity>_id`.
  6. Standards documented in `database/CHECKLIST_TABLE_STANDARDS.md`: snake_case, FK IDs (never text), audit trigger applied, row in `domain_family_map` for versioning. Every new table also needs an SQL script under `database/scripts/sql/<layer>/` and a seed CSV under `database/seed_tables/<layer>/`.

## Tech stack

### API layer
- **FastAPI** Async Python web framework with automatic OpenAPI documentation
- **Pydantic** Data validation and serialization
- **asyncpg** High-performance async PostgreSQL driver
- **Uvicorn** ASGI server

**Request flow:**
```
Request to Uvicorn to FastAPI to Pydantic (validates) to asyncpg (queries DB) to Response
```

### Database
- **PostgreSQL** Primary relational database
- **PostGIS** Spatial extensions for geospatial queries (bounding box, geometry)

### Cloud infrastructure (AWS)
- **ECS Fargate** Runs containerized API (Docker to ECR to ECS)
- **RDS PostgreSQL** Managed database with PostGIS
- **S3** Model output file storage
- **Route 53** DNS routing to api.coeqwal.org

### Data processing
- **boto3** AWS SDK for Python (ETL pipelines, database utilities)
- **Python scripts** ETL pipelines and data transformers

### Development tools
- **Docker** Containerization
- **Ruff** Python linting

## Data integrity

The platform has a layered audit and verification strategy. Each layer answers a different question:

| Layer | Question | Tools |
|-------|----------|-------|
| **All (monthly)** | Full audit: content + verification + health + cost | `python database/audit/run_monthly_audit.py` |
| **Schema structure** | Is the DB shaped correctly? | `database/run_audit.sh`, `verify_erd_against_audit.py`, per-layer `09_verify_level*.sql` |
| **Reference data content** | Are layers 00–08 correct? | `database/scripts/export_layer_tables.py` + diff vs `database/seed_tables/` |
| **ETL statistics accuracy** | Are computed results correct? | `etl/statistics/verify_all_sections.py` (CSVtoDB), `etl/statistics/verify_api.py` (DBtoAPI) |
| **Public status** | Is verification status visible? | `GET /api/verification/status` + frontend `/verification` page |

See `database/audit/README.md` for full audit documentation and usage guide.

## API

**Production:** https://api.coeqwal.org/api

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

## AWS Management

### Reclaiming disk space on the EC2 instance

If the AWS instance is running low on disk space (common after OS updates, package installs, or prolonged operation), use the following steps.

**Check current disk usage:**
```bash
df -h
```
Shows disk usage for all mounted filesystems in human-readable units (GB/MB). The `/` (root) filesystem is the one most likely to fill up. Look for `Use%` approaching 100%.

**Clean the DNF package cache:**
```bash
sudo dnf clean all
```
DNF (the package manager on Amazon Linux 2023 / RHEL-based systems) caches downloaded packages and metadata on disk after installation. Over time this cache can grow significantly. `dnf clean all` removes all cached package data, repo metadata, and headers. This is safe to run at any time — packages are re-downloaded from the repo on the next `dnf` operation.

Run `df -h` again after to confirm space was reclaimed.

**If `dnf clean all` doesn't reclaim much:**

Manually remove the DNF cache directory and check root disk usage:
```bash
sudo rm -rf /var/cache/dnf/*
df -h /
```

**Trim the system journal:**

`systemd` accumulates log journal files under `/var/log/journal/`. Check how much space the journal is using, then vacuum it down to 50 MB:
```bash
sudo journalctl --disk-usage
sudo journalctl --vacuum-size=50M
df -h /
```
`--disk-usage` reports total journal size. `--vacuum-size=50M` deletes the oldest journal files until the total size is at or below 50 MB. This is safe — it only removes old log history, not active logs.

**If space is still low — check Docker (~1.2 GB):**

Docker can accumulate significant disk usage from stopped containers, dangling images, unused volumes, and build cache. First, see a breakdown of what Docker is holding:
```bash
docker system df
```
This shows how much space is used by images, containers, volumes, and build cache, and how much is "reclaimable" (unused).

Safely remove everything unused:
```bash
docker container prune -f   # removes all stopped containers
docker image prune -a -f    # removes all images not used by a running container
docker volume prune -f      # removes all volumes not attached to a container
df -h /
```
These commands only delete objects that are not currently in use. Running containers, their images, and attached volumes are untouched. The `-f` flag skips the confirmation prompt.

## License

See [LICENSE](./LICENSE) for details.
