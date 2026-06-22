# COEQWAL Data Platform

A comprehensive data platform for the Collaboratory for Equity in Water Allocation (COEQWAL) project for California water management scenario presentation, analysis, and review.

#### Code

The data platform code repository and documentation is organized into four sections(`data`, `etl`, `database`, `api`). Each section has its own series of (often nested) READMEs for details. 

#### AWS infrastructure

The AWS infrastructure reference, `INFRASTRUCTURE.md`, lives in the private `coeqwal-private-docs` repository (https://github.com/berkeley-gif/coeqwal-private-docs). A schematic of that infrastructure can be found at https://dev.coeqwal.org/aws_architecture.html.

## [Data](data/README.md)

Where to find the source data the backend depends on. Most of it lives outside this repo:

- **Scenario model runs and trend reports** on the COEQWAL shared Google Drive (the SV input / DV output the ETL extracts, plus the reference CSVs it validates against).
- **Content listings** (tiers, outcomes) in the COEQWAL Platform Content Summary Google Sheet.
- **GIS sources** like the CalSim Geopackage (nodes, arcs, demand units, water budget areas, watersheds) are in the project Research Teams > Data Platform > GIS drive. We are experimenting with getting a [Kart repo](https://github.com/berkeley-gif/coeqwal-gis-kart) up and running.
- **Tracked reference spreadsheets and PDFs** under [`data/reference/`](data/reference/) (CWS / M&I / crosswalk xlsx, the CalSim 3 network schematic).
- **Authoritative attribute and reference data** (lookups, entities, scenarios, hydroclimates, themes, tier definitions, version metadata) in [`database/seed_tables/`](database/seed_tables/), organized by schema layer.

The most important single reference is the CalSim 3 manual PDF, too large for the repo (>100 MB) but downloadable from [DWR](https://water.ca.gov/Library/Modeling-and-Analysis/Central-Valley-models-and-tools/CalSim-3).

See [`data/README.md`](data/README.md) for the full inventory with locations and links.

## [Database](database/README.md)

PostgreSQL + PostGIS on RDS. A highly-normalized schema of ~96 tables organized conceptually into layers.

**Source-of-truth artifacts:**

- ERD: [`database/schema/ERD.md`](database/schema/ERD.md)
- Latest monthly audit: `audits/monthly_<timestamp>/report.md` (gitignored, regenerable)

**Layers:** foundational data (00-09) separated from derived results (10+). Layers tend to depend on/reference layers with a lower number.

- **00 VERSIONING** - `version_family`, `version`, `developer`, `domain_family_map`
- **01 LOOKUP** - `hydrologic_region`, `source`, `model_source`, `unit`, `spatial_scale`, `temporal_scale`, `statistic_category`, `statistic_type`, `geometry_type`, `network_type`, `network_subtype`, `network_entity_type`, `watershed`, `env_flow_season`
- **02 NETWORK** - `network`, `network_arc`, `network_node`, `network_gis`
- **03 ENTITY** - `reservoir`, `reservoir_entity`, `reservoir_group`, `reservoir_group_member`, `channel_entity`, `du_agriculture_entity`, `du_refuge_entity`, `du_urban_entity`, `du_urban_delivery_arc`, `du_urban_group`, `du_urban_group_member`, `mi_contractor`, `mi_contractor_delivery_arc`, `mi_contractor_group`, `mi_contractor_group_member`, `ag_aggregate_entity`, `cws_aggregate_entity`, `compliance_station`, `wba`
- **04 VARIABLE** - `calsim_model_variable_type`, `derived_variable_type`, `variable_type`, `channel_variable`, `du_urban_variable`
- **05 ASSUMPTIONS + OPS** - `assumption_category`, `assumption_definition`, `operation_category`, `operation_definition`
- **06 SCENARIO** - `scenario`, `scenario_hydroclimate_sibling`, `scenario_author`, `scenario_key_assumption_link`, `scenario_key_operation_link`, `scenario_tag`, `scenario_tag_link`
- **07 HYDROCLIMATE** - `hydroclimate`, `slr`
- **08 THEME** - `theme`, `theme_scenario_link`
- **09 TIER** - `tier_definition`, `tier_location`
- **10 TIER RESULTS** - `tier_result`, `tier_location_result`
- **11 PER-SCENARIO STATISTICS** - `reservoir_storage_monthly`, `reservoir_spill_monthly`, `reservoir_monthly_percentile`, `reservoir_period_summary`, `delta_monthly`, `delta_period_summary`, `du_delivery_monthly`, `du_shortage_monthly`, `du_period_summary`, `mi_delivery_monthly`, `mi_shortage_monthly`, `mi_contractor_period_summary`, `cws_aggregate_monthly`, `cws_aggregate_period_summary`, `ag_du_demand_monthly`, `ag_du_sw_delivery_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_shortage_monthly`, `ag_du_period_summary`, `ag_aggregate_monthly`, `ag_aggregate_period_summary`, `refuge_du_delivery_monthly`, `refuge_du_shortage_monthly`, `refuge_du_period_summary`, `env_flow_channel_monthly`, `env_flow_channel_seasonal`, `env_flow_channel_period_summary`
- **12 CROSS-SCENARIO ANALYSIS** - `sensitivity_climate`, `sensitivity_operational`

See [`database/README.md`](database/README.md) and the ERD, [`database/schema/ERD.md`](database/schema/ERD.md), for the full schema reference.

**Audits:** four concerns (schema structure, reference-data content, ETL statistics accuracy, plus health and cost), rolled up by `python database/audit/run_monthly_audit.py` into `audits/monthly_<ts>/report.md`. See [`database/README.md` Audit and verification](database/README.md#audit-and-verification) for the full chain and what to read after a run.

## [ETL](etl/README.md)

<!-- ACTIVE_SCENARIOS:BEGIN -->

**Active scenarios (72)**: s0011, s0020, s0021, s0023, s0024, s0025, s0026, s0027, s0028, s0030, s0031, s0032, s0033, s0035, s0036, s0037, s0039, s0040, s0041, s0042, s0044, s0045, s0046, s0047, s0048, s0049, s0050, s0051, s0056, s0057, s0058, s0059, s0060, s0062, s0063, s0065, s0067, s0068, s0069, s0071, s0072, s0073, s0074, s0075, s0076, s0077, s0078, s0079, s0080, s0081, s0082, s0083, s0084, s0085, s0087, s0088, s0089, s0091, s0092, s0093, s0094, s0095, s0096, s0097, s0098, s0099, s0100, s0101, s0102, s0103, s0104, s0105

_Last refreshed 2026-05-22 from `https://api.coeqwal.org/api/scenarios`. Regenerate with `python etl/ingestion/tools/refresh_active_scenarios.py`._

<!-- ACTIVE_SCENARIOS:END -->

### Confirm live scenario coverage

The block above is regenerated by hand. To check the public API right now, without a local DB connection:

```bash
curl -s https://api.coeqwal.org/api/scenarios \
  | python3 -c "import json,sys; d=json.load(sys.stdin); codes=sorted(s['short_code'] for s in d); print(f'{len(codes)} active scenarios on the public API, range {codes[0]} to {codes[-1]}.')"
```

Prints the live count and the lowest / highest `short_code` from the production API.

**Development status.** The ETL is still under iterative development, but provides a fully-functioning scenario and tier loading process. Development 
process has been to iteratively improve the process with each successive batch loaded. Next steps are to unify the pipeline steps ([`etl/run_full_pipeline.py`](etl/run_full_pipeline.py), in progress), unify and streamline the verification, and check the (currently rough) calculations. It is important to know that I typically only had time to do a quick first pass on the calculations, copied from the Water Allocation Modeling Team's Jupyter notebook as best I could given time constraints. Future iterations can harden these. I also accepted inefficiencies when working alone that could be improved for a more developer-friendly process.

The ETL has two parallel pipelines: the **scenario model-run pipeline** and the **tier data pipeline**. The full developer runbook (every command, the signal that each step worked, and troubleshooting) lives in [`etl/README.md`](etl/README.md). This section is the conceptual map.

### Scenario model-run pipeline

Handles CalSim scenario model run data end to end:

> `scan` → `download` → `promote` → `AWS Batch` → `audit` → `run_all (statistics)` → `verify` → `activate`

It moves in two stages:

- **Stage 1** (`scan` through the post-Batch `audit`) pulls each scenario's model run ZIP from the COEQWAL shared Google Drive, stages it to S3, and extracts the SV input and DV output DSS files to CSV.
- **Stage 2** (`run_all` → `activate`) reads those CSVs back from S3, computes derived statistics, and writes them to PostgreSQL. An activation step flips the scenarios' `is_active` record in the database, signalling to the API to deliver it.

Today each arrow is a manual step the developer runs in sequence from Cloud9.

**Stage 1 products** land in S3 (under `s3://coeqwal-model-run/scenario/<id>/`):

- The original CalSim ZIP (`run/`)
- The extracted DV and SV CSVs (`csv/`)
- The trend report reference CSV when present (`verify/`)
- Two JSON receipts at the scenario root: `ingest_record.json` (ingestion contract: declared basenames, SHA-256 hashes, provenance) and `extract_record.json` (Batch run record with the validation result inlined)

**Stage 2 products** land in PostgreSQL as statistics rows: reservoir storage, urban delivery, ag demand and shortage, MI contractor reliability, environmental flow alteration, refuge delivery, delta salinity / NDO / X2.

**Website**: Stage 1's S3 products feed Get Data (download links served by the `coeqwalPresignDownload` Lambda), and Stage 2's statistics feed Data in Depth (via the FastAPI service).

Two pieces of automation are designed and on the way to production-ready:

- A unified driver ([`etl/run_full_pipeline.py`](etl/run_full_pipeline.py)) that wraps every stage into one resumable command. Its current state and caveats are documented in the [ETL runbook](etl/README.md#experimental-orchestrator).
- An EventBridge `Batch -> run_all` trigger so statistics fire automatically on extraction completion.

Both are tracked in [`docs/TEAM_RUNBOOK.md`](docs/TEAM_RUNBOOK.md).

### Tier data pipeline

Team-delivered tier outcome CSVs loaded straight into PostgreSQL, independent of the scenario model-run pipeline. The developer drops correctly-formatted CSVs into [`etl/tier_data/staging/`](etl/tier_data/staging/), the loader generates an idempotent UPSERT, and `psql` applies it to the `tier_result` and `tier_location_result` tables. No S3 or Batch involvement. See the [tier-data runbook](etl/README.md#how-to-load-tier-data-into-the-database) for the commands.

**Products**: Tier result rows in the database. **Website**: Tier visualization tools.

### Verifications and audits

Every stage of both pipelines leaves a receipt, and each pipeline has verifiers that answer a specific question:

1. **Did the DSS file convert to CSV correctly?** The Batch container runs `validate_csvs.py` against the modeling team's trend report reference CSV. Automatic, on every Batch run.
2. **Did the statistics load into Postgres correctly?** `verify_all_sections.py` recomputes the headline statistics from the reference CSVs and compares against the database (developer diagnostic, experimental).
3. **Does the public API return those same numbers?** `verify_api.py` compares `api.coeqwal.org` responses against direct database queries.
4. **For tier data, does the database match what the team handed us?** `verify_tiers.py` compares `tier_result` rows against the staging CSVs the team delivered.

What to read after each run, what each console summary and status value means, and which artifact to open when something is flagged all live in [`etl/verification/README.md`](etl/verification/README.md). The end-to-end artifact map (what lands where, and which receipts are in S3, on local disk, or tracked in git) is in the [ETL runbook](etl/README.md#pipeline-paper-trail-where-every-artifact-lands).

## [API](api/coeqwal-api/README.md)

The public-facing surface at `https://api.coeqwal.org`. Two pieces:

| Piece | What it does | Source |
|---|---|---|
| FastAPI service on ECS Fargate | Serves the rest of the API. Statistics, tiers, verification status, etc. Async Python with automatic OpenAPI docs at `/docs` | [`api/coeqwal-api/`](api/coeqwal-api/) |
| `coeqwalPresignDownload` Lambda | Lists scenarios in S3 and presigns download URLs. Backs `GET /scenario` and `GET /download` through API Gateway v2 | [`api/lambda/coeqwalPresignDownload/`](api/lambda/coeqwalPresignDownload/) |

**Endpoints:**

- Production: <https://api.coeqwal.org/api>
- Interactive docs: <https://api.coeqwal.org/docs>

**Request flow (FastAPI service):**

```
Request -> Uvicorn -> FastAPI -> Pydantic (validates) -> asyncpg (queries DB) -> Response
```

Tech: FastAPI, Pydantic, `asyncpg`, Uvicorn. Containerized and deployed to ECS Fargate via GitHub Actions.

See [`api/coeqwal-api/README.md`](api/coeqwal-api/README.md) for endpoints, filtering options, and local development. See [`api/lambda/coeqwalPresignDownload/README.md`](api/lambda/coeqwalPresignDownload/README.md) for the download Lambda.

## AWS infrastructure

What runs where, at a glance:

| Service | Role |
|---|---|
| RDS PostgreSQL | Managed Postgres with PostGIS. Production database |
| ECS Fargate | Runs the FastAPI container (Docker -> ECR -> ECS) |
| S3 (`coeqwal-model-run`) | Model run ZIPs and extracted CSVs |
| Lambda (`coeqwalEtlTrigger`) | S3 PUT -> Batch SubmitJob for the extraction pipeline |
| Lambda (`coeqwalPresignDownload`) | Presigned-URL downloads for the website |
| Lambda (`coeqwal-database-audit`) | Scheduled DB audit |
| AWS Batch on Fargate Spot | Runs the DSS-to-CSV extraction container (`coeqwal-dss-queue`, job def `coeqwal-dss-jobdef:3`) |
| ECR | Docker image registry (`coeqwal-etl:latest`) |
| API Gateway (HTTP API v2) | Routes `GET /scenario` and `GET /download` to the presign Lambda |
| Route 53 | DNS routing to `api.coeqwal.org` |
| Cloud9 (`coeqwal-db-admin`) | Dev environment with the credentials and connection strings for running ETL and DB ops |
| GitHub Actions | CI for the FastAPI service (`api/coeqwal-api/`) and the ETL container (`etl/batch-container/`) |

Detailed runbook (AWS account IDs, ARNs, cost levers, IAM policies, deprecation notes) lives in `INFRASTRUCTURE.md` in the private `coeqwal-private-docs` repository (https://github.com/berkeley-gif/coeqwal-private-docs).

### Reclaiming disk space on the Cloud9 / EC2 instance

If the EC2 backing your Cloud9 instance is running low on disk space (common after OS updates, package installs, or prolonged operation):

**Check current disk usage:**

```bash
df -h
```

Shows disk usage for all mounted filesystems in human-readable units (GB/MB). The `/` (root) filesystem is the one most likely to fill up. Look for `Use%` approaching 100%.

**Clean the DNF package cache:**

```bash
sudo dnf clean all
```

DNF (the package manager on Amazon Linux 2023 / RHEL-based systems) caches downloaded packages and metadata on disk after installation. Over time this cache can grow significantly. `dnf clean all` removes all cached package data, repo metadata, and headers. Safe to run at any time. Packages are re-downloaded from the repo on the next `dnf` operation.

Run `df -h` again to confirm space was reclaimed.

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

`--disk-usage` reports total journal size. `--vacuum-size=50M` deletes the oldest journal files until the total size is at or below 50 MB. Safe to run. It only removes old log history, not active logs.

**If space is still low, check Docker (~1.2 GB):**

Docker can accumulate disk usage from stopped containers, dangling images, unused volumes, and build cache. See a breakdown of what Docker is holding:

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

## Open threads and roadmap

When picking up a partially-finished thread (geometry refactor, gw/sw
reconciliation, TAIESM1 ingest, master crosswalk reconciliation,
end-to-end pipeline automation, etc.), start at the team runbook. It indexes each open thread with current
state, files touched, and a "Next steps" block.

- [`docs/TEAM_RUNBOOK.md`](docs/TEAM_RUNBOOK.md) - active threads, rolled-back roadmap entries (R1, R2), and conventions for picking work back up
- [`docs/statistics_roadmap.md`](docs/statistics_roadmap.md) - statistics ETL roadmap (connection unification, atomic transactions, verification streamlining, reference-directory clarity)

## License

See [LICENSE](./LICENSE) for details.
