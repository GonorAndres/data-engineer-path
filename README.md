# Data Engineering Portfolio & Knowledge Base

A complete data engineering platform built around insurance claims data, demonstrating end-to-end skills from dimensional modeling to infrastructure-as-code. Built by an actuarial sciences graduate targeting DE roles in Mexico's fintech and insurance sector.

## Platform Architecture

```mermaid
graph TB
    subgraph "Project 1: Claims Warehouse"
        GEN[Data Generator<br/>Faker + NumPy] --> GCS[GCS Bucket]
        GCS --> BQ_RAW[BigQuery Raw]
        BQ_RAW --> |Dataform| BQ_STG[Staging]
        BQ_STG --> BQ_INT[Intermediate]
        BQ_INT --> BQ_MART[Analytics<br/>fct_claims, dim_*]
        BQ_MART --> TRIANGLE[Loss Triangle]
        BQ_MART --> DASH[Looker Studio]
    end

    subgraph "Project 2: Orchestration"
        DAGSTER[Dagster<br/>Local Dev] --> |schedules| GEN
        SCHED[Cloud Scheduler] --> |triggers| CR_ELT[Cloud Run<br/>ELT Pipeline]
        CR_ELT --> BQ_RAW
        GHA[GitHub Actions] --> |deploys| CR_ELT
    end

    subgraph "Project 3: Streaming"
        SIM[Claims Simulator] --> PUBSUB[Pub/Sub Topic]
        PUBSUB --> CR_SUB[Cloud Run<br/>Subscriber]
        PUBSUB --> BEAM[Dataflow<br/>Batch Only]
        PUBSUB --> DLQ[Dead Letter Queue]
        CR_SUB --> BQ_RAW
        BEAM --> BQ_MART
    end

    subgraph "Project 4: Infrastructure"
        TF[Terraform] --> |manages all| GCS
        TF --> BQ_RAW
        TF --> PUBSUB
        TF --> CR_ELT
        TF --> CR_SUB
        TF --> SCHED
    end
```

## Projects

| # | Project | What It Demonstrates | Stack |
|---|---------|---------------------|-------|
| 1 | [Insurance Claims Warehouse](projects/01-claims-warehouse/) | Star schema, loss triangles, ELT, data quality | DuckDB, BigQuery, Dataform |
| 2 | [Orchestrated ELT](projects/02-orchestrated-elt/) | Orchestration patterns, CI/CD, containerization | Dagster, Airflow, Cloud Run, GitHub Actions |
| 3 | [Streaming Claims Intake](projects/03-streaming-claims-intake/) | Event-driven architecture, messaging, Beam | Pub/Sub, Cloud Run, Apache Beam |
| 4 | [Data Platform Terraform](projects/04-data-platform-terraform/) | Infrastructure as Code, modules, state management | Terraform, GCP |

These are not 4 isolated projects -- they form one integrated insurance data platform where each project builds on the previous ones.

## Knowledge Base

The `docs/` folder is an Obsidian vault with decision-oriented documentation:

- **Fundamentals**: Data modeling, SQL patterns, ETL/ELT, orchestration, loss triangles
- **Tools**: BigQuery, Dataform, DuckDB, Dagster, Pub/Sub, Dataflow, GCS
- **Decisions**: When to use batch vs stream, warehouse selection, orchestrator selection
- **Architecture**: Cost-effective orchestration, event-driven patterns, reference architecture

Open `docs/` in Obsidian to explore the knowledge graph, or start at [docs/INDEX.md](docs/INDEX.md).

## Quick Start

```bash
# Project 1: Run the claims warehouse locally ($0)
cd projects/01-claims-warehouse
python3 -m venv .venv && source .venv/bin/activate
pip install duckdb faker numpy polars pyarrow pytest
cd src && python3 main.py

# Project 2: Start Dagster UI ($0)
cd projects/02-orchestrated-elt
python3 -m venv .venv && source .venv/bin/activate
pip install dagster dagster-webserver duckdb faker numpy
dagster dev
```

## Cost Summary

The entire platform was built for ~$75-135 on GCP trial credits:

| Component | Monthly Cost | Alternative Cost |
|-----------|-------------|-----------------|
| Cloud Scheduler + Cloud Run | ~$0.10/month | Cloud Composer: ~$400/month |
| BigQuery (on-demand) | ~$5/month | Already included |
| Dataflow (batch only, 2-3 runs) | ~$5-20 total | Streaming: $1-2k/month |
| Terraform | Free | Free |

## Tech Stack

- **Languages**: Python 3.12, SQL (BigQuery dialect), HCL (Terraform)
- **Local**: DuckDB, Dagster, Apache Beam Direct Runner, Pub/Sub Emulator
- **GCP**: BigQuery, Dataform, GCS, Pub/Sub, Cloud Run, Cloud Scheduler, Eventarc
- **CI/CD**: GitHub Actions, Docker, Artifact Registry
- **Testing**: pytest (68+ tests across all projects)
