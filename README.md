# Data Engineering Portfolio & Knowledge Base

[![CI/CD Pipeline](https://github.com/GonorAndres/data-engineer-path/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/GonorAndres/data-engineer-path/actions/workflows/ci-cd.yml)

A complete data engineering platform built around insurance claims data, demonstrating end-to-end skills from dimensional modeling to infrastructure-as-code. Built by an actuarial sciences graduate targeting DE roles in Mexico's fintech and insurance sector.

## See it running

**[data-engineer.gonor.me](https://data-engineer.gonor.me)** is the dashboard at the end of
this platform. Every figure on it comes from a live BigQuery query against tables Dataform
built -- there are no fixtures behind it.

[![Claims Analytics dashboard](projects/01-claims-warehouse/docs/screenshots/overview.png)](https://data-engineer.gonor.me)

608 synthetic claims across five lines of business and all 32 Mexican states, carried from
CSV in GCS through staging, intermediate, analytics and report layers in BigQuery.
[Methodology](https://data-engineer.gonor.me/methodology) walks through the whole platform
from inside the app, in English and Spanish: the six projects, how an actuarial question
becomes a table, the layering rule, what governance means here, and what it deliberately
does not do.

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
        BQ_MART --> DASH[FastAPI Dashboard<br/>Cloud Run]
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

### Data Lineage

How data flows through the platform, from generation to analytics:

```mermaid
graph LR
    subgraph "Sources"
        GEN[Faker + NumPy<br/>Synthetic Claims]
        SIM[Claims Simulator<br/>Event Stream]
    end

    subgraph "Ingestion"
        GCS[GCS Bucket<br/>CSV files]
        PS[Pub/Sub<br/>JSON events]
    end

    subgraph "Processing"
        CR_ELT[Cloud Run<br/>ELT Pipeline]
        CR_SUB[Cloud Run<br/>Subscriber]
        BEAM[Dataflow<br/>Beam Batch]
    end

    subgraph "Warehouse Layers"
        RAW[(raw)]
        STG[(staging)]
        INT[(intermediate)]
        MART[(analytics)]
        RPT[(reports)]
    end

    subgraph "Outputs"
        TRI[Loss Triangle]
        FREQ[Frequency Analysis]
        HOURLY[Hourly Summaries]
    end

    GEN --> GCS --> CR_ELT --> RAW
    SIM --> PS --> CR_SUB --> RAW
    PS --> BEAM --> HOURLY
    RAW --> STG --> INT --> MART --> RPT
    RPT --> TRI & FREQ

    style GEN fill:#e1f5fe
    style SIM fill:#e1f5fe
    style RAW fill:#fff3e0
    style MART fill:#e8f5e9
    style RPT fill:#f3e5f5
```

## Projects

| # | Project | What It Demonstrates | Stack |
|---|---------|---------------------|-------|
| 1 | [Insurance Claims Warehouse](projects/01-claims-warehouse/) | Star schema, loss triangles, ELT, data quality | DuckDB, BigQuery, Dataform |
| 2 | [Orchestrated ELT](projects/02-orchestrated-elt/) | Orchestration patterns, CI/CD, containerization | Dagster, Airflow, Cloud Run, GitHub Actions |
| 3 | [Streaming Claims Intake](projects/03-streaming-claims-intake/) | Event-driven architecture, messaging, Beam | Pub/Sub, Cloud Run, Apache Beam |
| 4 | [Data Platform Terraform](projects/04-data-platform-terraform/) | Infrastructure as Code, modules, state management | Terraform, GCP |
| 5 | [Streaming Claims Pipeline](projects/05-streaming-claims-pipeline/) | Streaming semantics, watermarks, triggers, exactly-once | Pub/Sub, Apache Beam, Dataflow |
| 6 | [Pricing ML Feature Pipeline](projects/06-pricing-ml-pipeline/) | Feature engineering, GLM pricing, actuarial modeling | DuckDB, statsmodels, BigQuery ML |

These are not 6 isolated projects -- they form one integrated insurance data platform where each project builds on the previous ones.

## dbt + DuckDB / Snowflake

New here? [The Claims Pipeline, Explained End to End](docs/architecture/claims-pipeline-explained.md) walks through every stage, why it exists, which engine runs it today, and the real insurance questions it answers.

[`dbt_claims/`](dbt_claims/) is the P01 warehouse ported from Dataform to dbt: same models, same names, same numbers (reconciled against BigQuery: 608 claims, 26,944,733.19 paid, 738 exposure rows). It runs on DuckDB, a database that lives in a single local file, so it needs no cloud account; the same project targets Snowflake by switching the dbt profile.

```bash
cd dbt_claims && pip install -r requirements.txt
dbt build --profiles-dir .            # seeds, snapshot, models, tests on DuckDB
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

- **Layers:** seeds (raw CSVs) -> `stg_` -> `int_` -> marts (`fct_claims`, `fct_claim_payments`, `dim_policy`, `dim_policyholder`, `dim_coverage`, `dim_state`, `dim_date`) -> reports (`rpt_loss_triangle`, `rpt_claim_frequency`) -> `kpi_portfolio`.
- **History:** `snap_policies` is a dbt snapshot (SCD Type 2: a changed row is closed and a new version added). With the static sample data it holds one version per policy.
- **KPIs:** claim frequency, severity, loss ratio and pure premium are each defined once in `macros/kpis.sql` and reused at every grain.
- **Tests:** 88 -- unique/not-null keys, fact-to-dimension relationships, accepted values for line of business and state, non-negative paid amounts, development year >= 0, and the three Dataform assertions. Source freshness is configured as warn-only, since the sample data never changes.
- **Engine portability:** dbt's cross-database macros first; the remaining differences live in `adapter.dispatch` macros (one implementation per engine).
- **CI:** [`dbt.yml`](.github/workflows/dbt.yml) lints with sqlfluff, runs `dbt build` on DuckDB on every PR and push to `main`, checks the Airflow DAG parses, and publishes the docs site from `main`. [`dbt-snowflake.yml`](.github/workflows/dbt-snowflake.yml) is a manual run that skips unless the `SNOWFLAKE_*` secrets exist.
- **Orchestration:** [`airflow/dags/claims_dbt.py`](airflow/dags/claims_dbt.py) runs seed -> snapshot -> run -> test with BashOperator. It is the open-source counterpart to the Dagster pipeline in P02, which stays as it is.
- **Docs and lineage:** https://gonorandres.github.io/data-engineer-path/
- **Snowflake status:** the profile parses, but the project has not been run against a real Snowflake account yet.

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
- **ML/Stats**: statsmodels (GLM), scikit-learn (evaluation)
- **Testing**: pytest (185+ tests across all projects)

## Test Summary

| Project | Framework | Tests | Coverage Areas |
|---------|-----------|-------|----------------|
| 01 Claims Warehouse | pytest | 52 | Data generator distributions, SQL transform correctness, schema validation |
| 02 Orchestrated ELT | pytest | 16 | Dagster asset materialization, pipeline orchestration |
| 03 Streaming Intake | pytest | 45 | Simulator generation, subscriber validation, Beam windowing |
| 05 Streaming Pipeline | pytest | 42 | Streaming transforms, windowing, triggers, late data, deduplication |
| 06 Pricing ML Pipeline | pytest | 30 | Feature engineering, GLM training, evaluation metrics, pricing adequacy |
| **Total** | | **185** | |

```bash
# Run all tests
cd projects/01-claims-warehouse && python -m pytest tests/ -v
cd projects/02-orchestrated-elt && python -m pytest tests/ -v
cd projects/03-streaming-claims-intake && python -m pytest tests/ -v
cd projects/05-streaming-claims-pipeline && python -m pytest tests/ -v
cd projects/06-pricing-ml-pipeline && python -m pytest tests/ -v
```
