---
tags: [project, portfolio, orchestration, dagster, airflow, cloud-run, elt]
status: draft
created: 2026-03-14
updated: 2026-03-14
---

# Project 02: Orchestrated ELT Pipeline

Orchestration layer for the insurance claims data warehouse built in [[01-claims-warehouse]]. Demonstrates two orchestration approaches: **Dagster** for local development and **Cloud Run + Cloud Scheduler** for cost-effective cloud deployment. Includes a reference Airflow DAG to show production orchestration patterns.

## What It Demonstrates

- **Dagster orchestration** -- software-defined assets, IO managers, type-checked pipelines
- **Cloud Run deployment** -- serverless container execution triggered by cron or events
- **Airflow DAG design** -- TaskGroups, BranchPythonOperator, retries, SLAs (reference only)
- **Cost-effective architecture** -- achieving daily pipeline orchestration for ~$0.10/month instead of $400+/month with Cloud Composer
- **CI/CD pipeline** -- GitHub Actions for lint, test, build, and deploy with manual approval gates
- **Docker containerization** -- multi-stage build, non-root user, structured logging

## Tech Stack

| Component | Tool | Why |
|-----------|------|-----|
| Local orchestrator | Dagster | Software-defined assets, type safety, free UI |
| Cloud orchestrator | Cloud Scheduler + Cloud Run | Serverless, pay-per-use, ~$0.10/month |
| Reference orchestrator | Airflow DAG | Interview readiness, industry standard |
| Container runtime | Docker | Reproducible builds, Cloud Run requirement |
| Local development | docker-compose | Multi-service local environment |
| CI/CD | GitHub Actions | Free for public repos, native GCP integration |
| Container registry | Artifact Registry | GCP-native, vulnerability scanning |
| Event triggers | Eventarc | React to GCS file uploads |

## Architecture

### Local Development

```mermaid
graph LR
    subgraph "Local ($0/month)"
        DEV[Developer] --> |dagster dev| DAGSTER[Dagster UI<br/>:3000]
        DAGSTER --> |materialize| ASSETS[Software-Defined<br/>Assets]
        ASSETS --> |DuckDB| DUCK[(DuckDB<br/>In-Memory)]
        SQL[SQL Files<br/>Project 01] -.-> |volume mount| ASSETS
    end
```

### Cloud Deployment

```mermaid
graph LR
    subgraph "GCP (~$0.10/month)"
        SCHED[Cloud Scheduler<br/>06:00 UTC daily] --> |POST /run| CR[Cloud Run<br/>claims-elt-pipeline]
        GCS[GCS Bucket<br/>claims-raw/] --> |Eventarc| CR
        CR --> |DuckDB or BigQuery| DW[(Data Warehouse)]
        CR --> |structured JSON| LOGS[Cloud Logging]
    end
```

### CI/CD Pipeline

```mermaid
graph LR
    subgraph "GitHub Actions"
        PR[Pull Request] --> LINT[ruff check]
        PR --> TEST[pytest]
        PUSH[Push to main] --> LINT
        PUSH --> TEST
        TEST --> BUILD[Docker Build]
        BUILD --> |push| AR[Artifact Registry]
        AR --> |manual approval| DEPLOY[Cloud Run Deploy]
    end
```

### Full System View

```mermaid
graph TB
    subgraph "Source Data"
        GEN[Data Generator<br/>Faker + NumPy]
        GCS_SRC[GCS Uploads<br/>from source systems]
    end

    subgraph "Orchestration"
        DAGSTER_LOCAL[Dagster<br/>Local Dev]
        CR_CLOUD[Cloud Run<br/>Production]
        AIRFLOW_REF[Airflow DAG<br/>Reference Only]
    end

    subgraph "Pipeline Layers"
        STG[Staging<br/>5 tables]
        INT[Intermediate<br/>3 tables]
        MART[Marts<br/>6 tables]
        RPT[Reports<br/>2 tables]
        QC[Quality Checks]
    end

    subgraph "Warehouse"
        DUCK[(DuckDB<br/>Local)]
        BQ[(BigQuery<br/>Cloud)]
    end

    GEN --> DAGSTER_LOCAL
    GCS_SRC --> CR_CLOUD
    DAGSTER_LOCAL --> STG
    CR_CLOUD --> STG
    STG --> INT --> MART --> RPT --> QC

    DAGSTER_LOCAL -.-> DUCK
    CR_CLOUD -.-> BQ
```

## How to Run Locally

### Option 1: Dagster (recommended for development)

```bash
cd projects/02-orchestrated-elt

# Set up Python environment
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Start Dagster UI
dagster dev

# Open http://localhost:3000 and materialize assets
```

### Option 2: Docker Compose

```bash
cd projects/02-orchestrated-elt

# Run the pipeline once
docker compose up pipeline

# Or start Dagster UI in Docker
docker compose up dagster-ui
# Open http://localhost:3000

# Shell into the container for debugging
docker compose run pipeline bash
```

### Option 3: Direct Python execution

```bash
cd projects/02-orchestrated-elt
python -m pipeline
```

## How to Deploy to GCP

### Prerequisites

```bash
# Install and configure gcloud CLI
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
export GCP_PROJECT_ID=YOUR_PROJECT_ID
```

### Deploy

```bash
# Full deployment: setup + build + deploy + schedule
bash cloud_run/deploy.sh

# Or step by step:
bash cloud_run/deploy.sh setup      # Enable APIs, create SA
bash cloud_run/deploy.sh build      # Build and push container
bash cloud_run/deploy.sh deploy     # Deploy to Cloud Run
bash cloud_run/deploy.sh schedule   # Create daily cron trigger

# Optional: trigger on GCS file uploads
bash cloud_run/deploy.sh trigger
```

### Verify deployment

```bash
# Check service health
SERVICE_URL=$(gcloud run services describe claims-elt-pipeline \
    --region=us-central1 --format="value(status.url)")
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    "${SERVICE_URL}/health"

# Trigger a manual run
curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{"seed": 42}' \
    "${SERVICE_URL}/run"
```

## Cost Comparison

| Approach | Monthly Cost | Setup Complexity | Best For |
|----------|-------------|-----------------|----------|
| **Cloud Scheduler + Cloud Run** | ~$0.10 | Low | Simple/linear DAGs, cost-sensitive |
| Cloud Composer (Airflow) | ~$400+ | Medium | Complex DAGs with many dependencies |
| Self-managed Airflow (GCE) | ~$30 | High | Need Airflow features, budget-conscious |
| Dagster Cloud | ~$100+ | Low | Teams using Dagster, need managed service |
| Cloud Workflows | ~$0.01 | Low | Simple step sequences, GCP-native |

**Why Cloud Run over Composer for this project:**

The claims ELT pipeline is a linear sequence (generate -> staging -> intermediate -> marts -> reports -> quality checks). It has no fan-out parallelism, no complex branching, and no cross-DAG dependencies. Cloud Scheduler + Cloud Run handles this pattern at 1/4000th the cost of Composer.

Cloud Composer becomes worthwhile when you have:
- 10+ DAGs with cross-dependencies
- Complex retry/backfill requirements
- Need for Airflow's rich operator ecosystem (Spark, EMR, etc.)
- A team that already knows Airflow

## Project Structure

```
02-orchestrated-elt/
├── README.md
├── pyproject.toml
├── Dockerfile                          # Container for Cloud Run
├── docker-compose.yml                  # Local dev environment
├── src/
│   ├── __init__.py
│   ├── dagster_pipeline/               # Dagster orchestration
│   │   └── __init__.py                 #   (assets, IO managers, definitions)
│   ├── pipeline/                       # Core pipeline logic
│   │   └── __init__.py                 #   (shared between Dagster and Cloud Run)
│   └── airflow_dags/                   # Reference Airflow DAG
│       └── claims_etl_dag.py           #   TaskGroups, branching, production patterns
├── cloud_run/
│   ├── entrypoint.py                   # HTTP handler (POST /run, GET /health)
│   └── deploy.sh                       # GCP deployment script
└── tests/
    └── __init__.py
```

## Airflow DAG Reference

The Airflow DAG in `src/airflow_dags/claims_etl_dag.py` is a reference implementation -- it is NOT deployed to Cloud Composer. It demonstrates:

- **TaskGroups** for visual organization of pipeline layers
- **BranchPythonOperator** for quality check branching
- **Production defaults**: retries=2, retry_delay=5min, SLA=30min, email_on_failure
- **Proper scheduling**: daily at 06:00 UTC with catchup=False
- **Tags and documentation** for DAG discoverability

This DAG exists to show interviewers that the author understands Airflow deeply, while choosing a more cost-effective deployment strategy.

## Related Docs

- [[01-claims-warehouse]] -- The data warehouse this project orchestrates
- [[data-modeling-overview]] -- Dimensional modeling concepts
- [[airflow-guide]] -- Airflow concepts and patterns
- [[cloud-run-guide]] -- Cloud Run deployment patterns
- [[cost-optimization]] -- GCP cost management strategies
- [[ci-cd-patterns]] -- CI/CD pipeline design patterns
